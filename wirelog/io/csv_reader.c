/*
 * csv_reader.c - wirelog CSV Reader
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Minimal CSV parser for loading EDB facts from .input directives.
 */

#include "csv_reader.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int
wl_csv_parse_line(const char *line, char delimiter, int64_t *values,
    uint32_t max_cols, uint32_t *count)
{
    if (!line || !values || !count)
        return -1;

    *count = 0;

    /* Skip leading whitespace (but not the delimiter itself) */
    while (*line && *line != delimiter && isspace((unsigned char)*line))
        line++;

    /* Empty line */
    if (*line == '\0')
        return 0;

    const char *p = line;
    while (*p) {
        /* Skip whitespace before value (but not the delimiter) */
        while (*p && *p != delimiter && isspace((unsigned char)*p))
            p++;

        if (*p == '\0') {
            if (*count == 0)
                break;
            return -1; /* trailing delimiter or empty integer field */
        }

        if (*count >= max_cols)
            return -2;

        char *end;
        errno = 0;
        int64_t val = strtoll(p, &end, 10);

        if (end == p)
            return -1; /* not a valid integer */
        if (errno == ERANGE)
            return -1; /* do not silently clamp out-of-range values */

        values[*count] = val;
        (*count)++;

        /* Skip whitespace after value (but not the delimiter) */
        while (*end && *end != delimiter && isspace((unsigned char)*end))
            end++;

        if (*end == delimiter) {
            p = end + 1;
        } else if (*end == '\0' || *end == '\n' || *end == '\r') {
            break;
        } else {
            return -1; /* unexpected character */
        }
    }

    return 0;
}

/* Initial capacity for row buffer */
#define CSV_INITIAL_CAPACITY 64
/*
 * Width cap for the integer-only reader below.  wl_csv_read_file()
 * auto-detects its column count from the file's first line, so it has no
 * expected width to size a buffer from and parses into a fixed frame
 * array; wl_csv_parse_line() bounds the writes with -2.  The
 * string-aware reader is told its width by the caller and sizes its row
 * buffer from it, so it is not subject to this cap (#997).
 */
#define CSV_MAX_COLS 256

/* ======================================================================== */
/* Growable line reader (#953)                                              */
/* ======================================================================== */

/*
 * The readers used to pull each line through a fixed `char line[4096]`
 * with fgets(), which delivers at most 4095 content bytes.  A longer
 * physical line was therefore handed to the parser in pieces and every
 * piece was turned into its own tuple: truncation that fabricated rows,
 * silently, with a zero exit status.  The width check added for #977
 * cannot catch it, because a split line can present the declared number
 * of fields in each of its chunks.
 *
 * This reader assembles whole lines instead.  getline() would do it in
 * one call but is POSIX-2008 and absent on MSVC, which CI builds with,
 * so the framing is hand-rolled from fread() plus memchr().  Working in
 * explicit byte counts rather than strlen() also makes an embedded NUL
 * visible instead of silently ending the line early.
 *
 * Note that assembling whole lines *removes* the accidental protection
 * fragmentation used to give the wide-relation write path: a long line
 * that used to arrive in 4095-byte pieces now arrives entire, so the
 * per-write capacity bound added for #997 is what keeps a 300-column
 * relation from overflowing its row buffer.  That bound must survive any
 * further consolidation of these parsers -- see csv_parse_line_via_ctx().
 */
typedef struct {
    FILE *f;
    char *chunk; /* staging buffer, WL_CSV_READ_CHUNK bytes */
    size_t chunk_len;
    size_t chunk_pos;
    char *line; /* growable spill buffer for chunk-straddling lines */
    size_t line_len;
    size_t line_cap;
    int eof;
} csv_line_reader_t;

static int
csv_reader_init(csv_line_reader_t *r, FILE *f)
{
    r->f = f;
    r->chunk_len = 0;
    r->chunk_pos = 0;
    r->line = NULL;
    r->line_len = 0;
    r->line_cap = 0;
    r->eof = 0;
    r->chunk = (char *)malloc(WL_CSV_READ_CHUNK);
    return r->chunk ? WL_CSV_OK : WL_CSV_ERR_MEMORY;
}

static void
csv_reader_dispose(csv_line_reader_t *r)
{
    free(r->chunk);
    free(r->line);
    r->chunk = NULL;
    r->line = NULL;
}

/*
 * Append @n bytes to the spill buffer, growing it within the ceiling.
 *
 * What actually bounds the allocation is the clamp on `cap` below, not the
 * order of the length check -- a point worth stating, because the obvious
 * reading is wrong.  Moving the length check after the growth block rejects
 * exactly the same inputs and peaks at the same size: this function is only
 * ever called with n <= WL_CSV_READ_CHUNK, and WL_CSV_MAX_LINE is an exact
 * multiple of that, so an over-long line reaches the clamped capacity and is
 * refused on the following append either way.
 *
 * The clamp is what keeps a 20 MiB line from allocating 20 MiB.  It is safe
 * only while `cap >= need` still holds after clamping, so that is asserted
 * directly rather than left to the check above -- clamping to a capacity
 * smaller than the write that follows would be a heap overflow, and nothing
 * else in this function would catch it.
 */
static int
csv_line_append(csv_line_reader_t *r, const char *src, size_t n)
{
    if (n > WL_CSV_MAX_LINE - r->line_len)
        return WL_CSV_ERR_LINE_TOO_LONG;

    size_t need = r->line_len + n + 1; /* +1 for the NUL terminator */
    if (need > r->line_cap) {
        size_t cap = r->line_cap ? r->line_cap : WL_CSV_READ_CHUNK;
        while (cap < need)
            cap *= 2;
        if (cap > WL_CSV_MAX_LINE + 1)
            cap = WL_CSV_MAX_LINE + 1;
        if (cap < need)
            return WL_CSV_ERR_LINE_TOO_LONG;
        char *tmp = (char *)realloc(r->line, cap);
        if (!tmp)
            return WL_CSV_ERR_MEMORY;
        r->line = tmp;
        r->line_cap = cap;
    }

    memcpy(r->line + r->line_len, src, n);
    r->line_len += n;
    return WL_CSV_OK;
}

/*
 * Strip the trailing line terminator from @buf[0..*len), reject an
 * embedded NUL, and NUL-terminate in place.  @buf must have one spare
 * writable byte at *len (the consumed newline, or the spill buffer's
 * reserved slot).
 */
static int
csv_finish_line(char *buf, size_t *len)
{
    size_t n = *len;
    while (n > 0 && (buf[n - 1] == '\n' || buf[n - 1] == '\r'))
        n--;

    if (memchr(buf, '\0', n) != NULL)
        return WL_CSV_ERR_EMBEDDED_NUL;

    buf[n] = '\0';
    *len = n;
    return WL_CSV_OK;
}

/*
 * Fetch the next line, with its terminator stripped.
 *
 * On success *out_line points either into the staging chunk or into the
 * spill buffer; either way it is valid only until the next call.  The
 * buffer is reader-owned in both cases, which is what makes writing the
 * terminating NUL into it legal -- the parsers still take a
 * `const char *`, because wl_csv_parse_line_ex() is called with string
 * literals from the tests.
 *
 * Returns 1 when a line was produced, 0 at end of input, or a negative
 * WL_CSV_ERR_* code.
 */
static int
csv_next_line(csv_line_reader_t *r, char **out_line, size_t *out_len)
{
    int spilled = 0;

    r->line_len = 0;

    for (;;) {
        if (r->chunk_pos == r->chunk_len) {
            if (r->eof)
                break;
            r->chunk_len = fread(r->chunk, 1, WL_CSV_READ_CHUNK, r->f);
            r->chunk_pos = 0;
            if (r->chunk_len == 0) {
                /*
                 * fgets() reported EOF and a read error the same way, so
                 * a failed read used to end the load quietly with
                 * whatever had been parsed so far.  Silently keeping a
                 * prefix of a file is the same failure mode as silently
                 * keeping a prefix of a line; report it instead.
                 */
                if (ferror(r->f))
                    return WL_CSV_ERR_ARGS;
                r->eof = 1;
                break;
            }
        }

        char *base = r->chunk + r->chunk_pos;
        size_t avail = r->chunk_len - r->chunk_pos;
        char *nl = (char *)memchr(base, '\n', avail);

        if (nl && !spilled) {
            /*
             * Whole line inside the chunk: parse it in place, no copy.
             * The newline is consumed, so overwriting it with the NUL
             * terminator cannot lose a byte we still need.
             */
            size_t n = (size_t)(nl - base) + 1;
            r->chunk_pos += n;
            int rc = csv_finish_line(base, &n);
            if (rc != WL_CSV_OK)
                return rc;
            *out_line = base;
            *out_len = n;
            return 1;
        }

        size_t take = nl ? (size_t)(nl - base) + 1 : avail;
        int rc = csv_line_append(r, base, take);
        if (rc != WL_CSV_OK)
            return rc;
        spilled = 1;
        r->chunk_pos += take;
        if (nl)
            break;
    }

    if (r->line_len == 0)
        return 0; /* end of input */

    size_t n = r->line_len;
    int rc = csv_finish_line(r->line, &n);
    if (rc != WL_CSV_OK)
        return rc;
    *out_line = r->line;
    *out_len = n;
    return 1;
}

int
wl_csv_read_file(const char *path, char delimiter, int64_t **data,
    uint32_t *nrows, uint32_t *ncols)
{
    if (!path || !data || !nrows || !ncols)
        return -1;

    FILE *f = fopen(path, "r");
    if (!f)
        return -1;

    *data = NULL;
    *nrows = 0;
    *ncols = 0;

    uint32_t capacity = CSV_INITIAL_CAPACITY;
    uint32_t cols_expected = 0;
    int64_t *buf = NULL;

    csv_line_reader_t reader;
    int rc = csv_reader_init(&reader, f);
    if (rc != WL_CSV_OK) {
        fclose(f);
        return rc;
    }

    char *line = NULL;
    size_t len = 0;
    int lrc;

    while ((lrc = csv_next_line(&reader, &line, &len)) == 1) {
        /* Skip empty lines */
        if (len == 0)
            continue;

        int64_t row_values[CSV_MAX_COLS];
        uint32_t col_count = 0;
        rc = wl_csv_parse_line(line, delimiter, row_values, CSV_MAX_COLS,
                &col_count);
        if (rc != 0 || col_count == 0) {
            lrc = WL_CSV_ERR_PARSE;
            break;
        }

        /* First row determines column count */
        if (*nrows == 0) {
            cols_expected = col_count;
            buf = (int64_t *)malloc((size_t)capacity * cols_expected
                    * sizeof(int64_t));
            if (!buf) {
                lrc = WL_CSV_ERR_MEMORY;
                break;
            }
        } else if (col_count != cols_expected) {
            lrc = WL_CSV_ERR_PARSE; /* inconsistent column count */
            break;
        }

        /* Grow buffer if needed */
        if (*nrows >= capacity) {
            capacity *= 2;
            int64_t *tmp = (int64_t *)realloc(
                buf, (size_t)capacity * cols_expected * sizeof(int64_t));
            if (!tmp) {
                lrc = WL_CSV_ERR_MEMORY;
                break;
            }
            buf = tmp;
        }

        /* Copy row into flat array */
        memcpy(&buf[(size_t)*nrows * cols_expected], row_values,
            (size_t)cols_expected * sizeof(int64_t));
        (*nrows)++;
    }

    csv_reader_dispose(&reader);
    fclose(f);

    if (lrc < 0) {
        free(buf);
        *nrows = 0;
        return lrc;
    }

    *data = buf;
    *ncols = cols_expected;
    return 0;
}

/* ======================================================================== */
/* Callback-based line parser (internal)                                    */
/* ======================================================================== */

/*
 * Growable scratch for NUL-terminating one string field at a time.
 *
 * This replaces a `char strbuf[4096]` that silently clipped every string
 * field at 4095 bytes.  It was unreachable through the file readers --
 * fgets() could never hand them a chunk longer than the guard admitted --
 * but it is reachable through wl_csv_parse_line_ex(), and growing only
 * the line buffer would have converted #953's row fabrication into field
 * truncation (#953).
 *
 * One scratch is owned per *file* and reused for every field, so a load
 * costs no allocations per line or per field once it has warmed up.
 */
typedef struct {
    char *buf;
    size_t cap;
} csv_scratch_t;

static void
csv_scratch_dispose(csv_scratch_t *s)
{
    free(s->buf);
    s->buf = NULL;
    s->cap = 0;
}

static int
csv_scratch_reserve(csv_scratch_t *s, size_t need)
{
    if (need <= s->cap)
        return WL_CSV_OK;

    size_t cap = s->cap ? s->cap : 256;
    while (cap < need) {
        if (cap > (size_t)-1 / 2)
            return WL_CSV_ERR_MEMORY;
        cap *= 2;
    }

    char *tmp = (char *)realloc(s->buf, cap);
    if (!tmp)
        return WL_CSV_ERR_MEMORY;
    s->buf = tmp;
    s->cap = cap;
    return WL_CSV_OK;
}

/*
 * The single implementation of the string-aware line parser.
 * wl_csv_parse_line_ex() below is a thin wrapper over it; the two used to
 * be near-identical copies, and the capacity check added for #997 had to
 * exist in both or neither, so they are now one body.
 *
 * @max_cols is the capacity of @values.  Writing @num_cols cells into a
 * buffer that does not hold them is what #997 was: the caller's declared
 * width was trusted with no bound, overflowing a fixed 256-entry frame
 * array.  The bound lives here, at the write site, mirroring
 * wl_csv_parse_line().  It matters more, not less, now that #953 hands
 * this function whole lines: the old fgets() fragmentation used to keep
 * a long wide line under the write count by accident.  Do not remove it.
 *
 * @line is not modified; each string field is delimited by scanning and
 * then copied into @scratch in one memcpy rather than a bounds-checked
 * byte loop.
 */
static int
csv_parse_line_via_ctx(const char *line, char delimiter,
    const wirelog_column_type_t *col_types, uint32_t num_cols,
    int64_t *values, uint32_t max_cols, uint32_t *count,
    int64_t (*intern_cb)(void *opaque, const char *str),
    void *opaque, csv_scratch_t *scratch)
{
    if (!line || !col_types || !values || !count || num_cols == 0 || !intern_cb
        || !scratch)
        return -1;

    if (num_cols > max_cols)
        return -2; /* too many columns for the output buffer */

    *count = 0;
    const char *p = line;

    for (uint32_t col = 0; col < num_cols; col++) {
        /* Skip leading whitespace */
        while (*p && *p != delimiter && *p != '"' && isspace((unsigned char)*p))
            p++;

        if (*p == '\0') {
            /* A trailing delimiter denotes an empty final string field. */
            if (col == num_cols - 1 && col > 0 && p[-1] == delimiter
                && col_types[col] == WIRELOG_TYPE_STRING) {
                values[col] = intern_cb(opaque, "");
                if (values[col] < 0)
                    return -1;
                (*count)++;
                break;
            }
            return -2; /* too few columns */
        }

        if (col_types[col] == WIRELOG_TYPE_STRING) {
            /* Parse string field: quoted or unquoted */
            const char *start;
            size_t slen;

            if (*p == '"') {
                /* Quoted field: read until closing quote */
                p++; /* skip opening quote */
                start = p;
                while (*p && *p != '"')
                    p++;
                if (*p != '"')
                    return -1; /* unterminated quoted field */
                slen = (size_t)(p - start);
                p++; /* skip closing quote */
                while (*p && *p != delimiter && *p != '\n' && *p != '\r'
                    && isspace((unsigned char)*p))
                    p++;
                if (*p && *p != delimiter && *p != '\n' && *p != '\r')
                    return -1; /* junk after closing quote */
            } else {
                /* Unquoted field: read until delimiter or end */
                start = p;
                while (*p && *p != delimiter && *p != '\n' && *p != '\r')
                    p++;
                slen = (size_t)(p - start);
                /* Trim trailing whitespace */
                while (slen > 0 && isspace((unsigned char)start[slen - 1]))
                    slen--;
            }

            int rc = csv_scratch_reserve(scratch, slen + 1);
            if (rc != WL_CSV_OK)
                return rc;
            memcpy(scratch->buf, start, slen);
            scratch->buf[slen] = '\0';

            values[col] = intern_cb(opaque, scratch->buf);
            if (values[col] < 0)
                return -1;
        } else {
            /* Parse integer field */
            char *end;
            errno = 0;
            values[col] = strtoll(p, &end, 10);
            if (end == p)
                return -1; /* not a valid integer */
            if (errno == ERANGE)
                return -1; /* do not silently clamp out-of-range values */
            p = end;

            /* Skip trailing whitespace */
            while (*p && *p != delimiter && isspace((unsigned char)*p))
                p++;
        }

        (*count)++;

        /* Skip delimiter between fields */
        if (col < num_cols - 1) {
            if (*p == delimiter)
                p++;
            else
                return -2; /* missing field separator */
        } else if (col_types[col] != WIRELOG_TYPE_STRING) {
            /* The string reader historically ignores fields beyond the
            * declared width; preserve that adapter behavior (#982). */
            while (*p && (*p == '\n' || *p == '\r'
                || isspace((unsigned char)*p)))
                p++;
            if (*p != '\0')
                return -2; /* extra field after the declared width */
        }
    }

    return 0;
}

/* Adapts the intern-table API to the callback the parser expects. */
static int64_t
intern_trampoline(void *opaque, const char *str)
{
    return wl_intern_put((wl_intern_t *)opaque, str);
}

/* ======================================================================== */
/* Extended Line Parser (mixed int/string)                                  */
/* ======================================================================== */

int
wl_csv_parse_line_ex(const char *line, char delimiter,
    const wirelog_column_type_t *col_types, uint32_t num_cols,
    int64_t *values, uint32_t max_cols, uint32_t *count, wl_intern_t *intern)
{
    /*
     * A NULL @intern is only an error once a STRING column is actually
     * reached: wl_intern_put(NULL, ...) returns -1, which the parser
     * reports as -1.  All-integer column sets stay legal without one.
     *
     * The direct entry point is not the bulk-load path, so it owns its
     * field scratch for the duration of the call; callers reading a file
     * reuse one scratch across every line instead.
     */
    csv_scratch_t scratch = { NULL, 0 };
    int rc = csv_parse_line_via_ctx(line, delimiter, col_types, num_cols,
            values, max_cols, count, intern_trampoline, intern, &scratch);
    csv_scratch_dispose(&scratch);
    return rc;
}

/* ======================================================================== */
/* Callback-based File Reader (#455)                                        */
/* ======================================================================== */

int
wl_csv_read_file_via_ctx(
    const char *filepath,
    char delimiter,
    const wirelog_column_type_t *col_types,
    uint32_t num_cols,
    int64_t **out_data,
    uint32_t *out_nrows,
    uint32_t *out_ncols,
    int64_t (*intern_cb)(void *opaque, const char *str),
    void *opaque)
{
    if (!filepath || !col_types || !out_data || !out_nrows || !out_ncols
        || num_cols == 0 || !intern_cb)
        return -1;

    FILE *f = fopen(filepath, "r");
    if (!f)
        return -1;

    *out_data = NULL;
    *out_nrows = 0;
    *out_ncols = num_cols;

    uint32_t capacity = CSV_INITIAL_CAPACITY;
    int64_t *buf
        = (int64_t *)malloc((size_t)capacity * num_cols * sizeof(int64_t));
    if (!buf) {
        fclose(f);
        return -3;
    }

    /*
     * One row buffer for the whole file, sized to the caller's real
     * width.  It used to be a fixed int64_t[256] declared inside the
     * loop, which the parser wrote past for any relation wider than that
     * (#997).  Allocating per line instead would be visible on the DOOP
     * load path, which streams ~760 MB through here.
     */
    int64_t *row_values = (int64_t *)malloc((size_t)num_cols
            * sizeof(int64_t));
    if (!row_values) {
        free(buf);
        fclose(f);
        return -3;
    }

    /*
     * One line reader and one field scratch for the whole file, both
     * reused for every line (#953).  A DOOP relation is ~885k lines x 2
     * fields; allocating per line or per field would cost 1.77M
     * allocations on one input.
     */
    csv_line_reader_t reader;
    int rc = csv_reader_init(&reader, f);
    if (rc != WL_CSV_OK) {
        free(row_values);
        free(buf);
        fclose(f);
        return rc;
    }
    csv_scratch_t scratch = { NULL, 0 };

    char *line = NULL;
    size_t len = 0;
    int lrc;

    while ((lrc = csv_next_line(&reader, &line, &len)) == 1) {
        /* Skip empty lines */
        if (len == 0)
            continue;

        uint32_t col_count = 0;
        /*
         * row_values holds exactly num_cols cells, so that is also the
         * capacity handed to the parser -- the #997 bound, kept intact
         * across the #953 rewrite of the line framing above.
         */
        rc = csv_parse_line_via_ctx(line, delimiter, col_types, num_cols,
                row_values, num_cols, &col_count,
                intern_cb, opaque, &scratch);
        if (rc != 0 || col_count != num_cols) {
            lrc = (rc == WL_CSV_ERR_MEMORY) ? rc : WL_CSV_ERR_PARSE;
            break;
        }

        /* Grow buffer if needed */
        if (*out_nrows >= capacity) {
            capacity *= 2;
            int64_t *tmp = (int64_t *)realloc(buf, (size_t)capacity * num_cols
                    * sizeof(int64_t));
            if (!tmp) {
                lrc = WL_CSV_ERR_MEMORY;
                break;
            }
            buf = tmp;
        }

        /* Copy row into flat array */
        memcpy(&buf[(size_t)*out_nrows * num_cols], row_values,
            (size_t)num_cols * sizeof(int64_t));
        (*out_nrows)++;
    }

    csv_scratch_dispose(&scratch);
    csv_reader_dispose(&reader);
    free(row_values);
    fclose(f);

    if (lrc < 0) {
        free(buf);
        *out_nrows = 0;
        return lrc;
    }

    *out_data = buf;
    return 0;
}

/* ======================================================================== */
/* Extended File Reader -- thin wrapper over via_ctx                         */
/* ======================================================================== */

int
wl_csv_read_file_ex(const char *path, char delimiter,
    const wirelog_column_type_t *col_types, uint32_t num_cols,
    int64_t **data, uint32_t *nrows, uint32_t *ncols,
    wl_intern_t *intern)
{
    if (!intern)
        return -1;
    return wl_csv_read_file_via_ctx(path, delimiter, col_types, num_cols,
               data, nrows, ncols,
               intern_trampoline, intern);
}
