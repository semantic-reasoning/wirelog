/*
 * backend/columnar_nanoarrow.c - wirelog Nanoarrow Columnar Backend
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "columnar_nanoarrow.h"
#include "memory.h"
#include "../session.h"
#include "../wirelog-internal.h"
#include "arena/arena.h"

#include "nanoarrow/nanoarrow.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Relation Storage                                                          */
/* ======================================================================== */

#define COL_REL_INIT_CAP 64u
#define COL_STACK_MAX 32u
#define COL_FILTER_STACK 64u
#define MAX_ITERATIONS 4096u

/*
 * col_rel_t: in-memory columnar relation.
 *
 * Tuples are stored row-major: data[row * ncols + col].
 * Column names enable JOIN key resolution (variable name -> position).
 * The ArrowSchema provides Arrow-compatible type metadata.
 */
typedef struct {
    char *name;                /* owned, null-terminated                */
    uint32_t ncols;            /* columns per tuple (0 = unset)         */
    int64_t *data;             /* owned, row-major int64 buffer         */
    uint32_t nrows;            /* current row count                     */
    uint32_t capacity;         /* allocated row capacity                */
    char **col_names;          /* owned array of ncols owned strings    */
    struct ArrowSchema schema; /* owned Arrow schema (lazy-inited)      */
    bool schema_ok;            /* true after schema is initialised      */
} col_rel_t;

/* ---- lifecycle ---------------------------------------------------------- */

static void
col_rel_free_contents(col_rel_t *r)
{
    if (!r)
        return;
    free(r->name);
    free(r->data);
    if (r->col_names) {
        for (uint32_t i = 0; i < r->ncols; i++)
            free(r->col_names[i]);
        free(r->col_names);
    }
    if (r->schema_ok)
        ArrowSchemaRelease(&r->schema);
    memset(r, 0, sizeof(*r));
}

/*
 * col_rel_set_schema:
 * Initialise ncols, col_names[], data buffer, and ArrowSchema.
 * Called lazily on first insert (EDB) or when relation is first produced.
 * Returns 0 on success, ENOMEM/EINVAL on failure.
 */
static int
col_rel_set_schema(col_rel_t *r, uint32_t ncols, const char *const *col_names)
{
    if (r->ncols != 0)
        return 0; /* already initialised */

    r->ncols = ncols;

    if (ncols > 0) {
        r->capacity = COL_REL_INIT_CAP;
        r->data = (int64_t *)malloc(sizeof(int64_t) * r->capacity * ncols);
        if (!r->data)
            return ENOMEM;

        r->col_names = (char **)calloc(ncols, sizeof(char *));
        if (!r->col_names) {
            free(r->data);
            r->data = NULL;
            return ENOMEM;
        }
        for (uint32_t i = 0; i < ncols; i++) {
            if (col_names && col_names[i]) {
                r->col_names[i] = wl_strdup(col_names[i]);
            } else {
                char buf[32];
                snprintf(buf, sizeof(buf), "col%u", i);
                r->col_names[i] = wl_strdup(buf);
            }
            if (!r->col_names[i]) {
                for (uint32_t j = 0; j < i; j++)
                    free(r->col_names[j]);
                free(r->col_names);
                free(r->data);
                r->col_names = NULL;
                r->data = NULL;
                return ENOMEM;
            }
        }
    }

    /* Arrow schema: struct<col0:i64, col1:i64, ...> */
    ArrowSchemaInit(&r->schema);
    if (ArrowSchemaSetTypeStruct(&r->schema, (int64_t)ncols) != NANOARROW_OK) {
        /* cleanup names/data done by caller via col_rel_free_contents */
        return EINVAL;
    }
    for (uint32_t i = 0; i < ncols; i++) {
        if (ArrowSchemaInitFromType(r->schema.children[i], NANOARROW_TYPE_INT64)
            != NANOARROW_OK) {
            ArrowSchemaRelease(&r->schema);
            return EINVAL;
        }
        const char *cname
            = (r->col_names && r->col_names[i]) ? r->col_names[i] : "";
        ArrowSchemaSetName(r->schema.children[i], cname);
    }
    r->schema_ok = true;
    return 0;
}

static int
col_rel_alloc(col_rel_t **out, const char *name)
{
    col_rel_t *r = (col_rel_t *)calloc(1, sizeof(col_rel_t));
    if (!r)
        return ENOMEM;
    r->name = wl_strdup(name);
    if (!r->name) {
        free(r);
        return ENOMEM;
    }
    *out = r;
    return 0;
}

static int
col_rel_append_row(col_rel_t *r, const int64_t *row)
{
    if (r->nrows >= r->capacity) {
        uint32_t new_cap = r->capacity ? r->capacity * 2 : COL_REL_INIT_CAP;
        if (new_cap <= r->capacity) /* overflow guard */
            return ENOMEM;
        int64_t *nd = (int64_t *)realloc(
            r->data, sizeof(int64_t) * (size_t)new_cap * r->ncols);
        if (!nd)
            return ENOMEM;
        r->data = nd;
        r->capacity = new_cap;
    }
    memcpy(r->data + (size_t)r->nrows * r->ncols, row,
           sizeof(int64_t) * r->ncols);
    r->nrows++;
    return 0;
}

/* Copy all rows from src into dst (must have same ncols). */
static int
col_rel_append_all(col_rel_t *dst, const col_rel_t *src)
{
    for (uint32_t i = 0; i < src->nrows; i++) {
        int rc = col_rel_append_row(dst, src->data + (size_t)i * src->ncols);
        if (rc != 0)
            return rc;
    }
    return 0;
}

/* ---- column name lookup ------------------------------------------------- */

static int
col_rel_col_idx(const col_rel_t *r, const char *name)
{
    if (!r->col_names || !name)
        return -1;
    for (uint32_t i = 0; i < r->ncols; i++) {
        if (r->col_names[i] && strcmp(r->col_names[i], name) == 0)
            return (int)i;
    }
    /* fallback: "col<N>" convention */
    if (name[0] == 'c' && name[1] == 'o' && name[2] == 'l') {
        char *end;
        long v = strtol(name + 3, &end, 10);
        if (*end == '\0' && v >= 0 && (uint32_t)v < r->ncols)
            return (int)v;
    }
    return -1;
}

/* ======================================================================== */
/* Session                                                                   */
/* ======================================================================== */

/*
 * wl_col_session_t: Columnar backend session state
 *
 * Memory layout (C11 §6.7.2.1 ¶15 - pointer compatibility):
 *   Offset 0: wl_session_t base
 *     └─ Contains: backend pointer (vtable dispatch, set by session.c:38)
 *   Offset sizeof(wl_session_t): columnar-specific fields
 *     ├─ const wl_plan_t *plan
 *     ├─ col_rel_t **rels
 *     ├─ uint32_t nrels
 *     ├─ uint32_t rel_cap
 *     ├─ wl_on_delta_fn delta_cb
 *     └─ void *delta_data
 *
 * Memory ownership:
 *   - base.backend: set by session.c:38 after col_session_create returns
 *   - plan: borrowed (lifetime: caller, must outlive session)
 *   - rels[]: owned malloc array, grown on demand via session_add_rel()
 *   - rels[i]: owned col_rel_t* (each allocated separately, freed on destroy)
 *   - rels[i]->data: owned int64_t[] row-major buffer (persistent across steps)
 *   - rels[i]->col_names: owned char*[] (set on first insert or eval output)
 *
 * Casting contract:
 *   - (wl_col_session_t *)session is safe because base is the first field
 *   - session.c:38 writes to (*out)->backend which aliases &base.backend
 *   - All col_session_* vtable functions MUST cast via COL_SESSION() macro
 *
 * Phase 2A vs 2B boundary:
 *   - Phase 2A (current): full re-evaluation each step; set-diff for deltas
 *   - Phase 2B (future): semi-naive delta propagation (split rels: R + ΔR)
 *   - DO NOT optimize the iteration loop here; marked for Phase 2B rewrite
 *
 * Thread safety: NOT thread-safe. Each worker thread must own its session.
 *
 * @see backend_dd.c:35-44 for the embedding pattern reference (wl_dd_session_t)
 * @see session.h:38-40 for canonical wl_session_t definition
 * @see exec_plan.h for wl_plan_t backend-agnostic plan types
 */
typedef struct {
    wl_session_t base;       /* MUST be first field (vtable dispatch)  */
    const wl_plan_t *plan;   /* borrowed, lifetime: caller             */
    col_rel_t **rels;        /* owned array of owned col_rel_t*        */
    uint32_t nrels;          /* current number of registered relations */
    uint32_t rel_cap;        /* allocated capacity of rels[]           */
    wl_on_delta_fn delta_cb; /* delta callback (NULL = disabled)       */
    void *delta_data;        /* opaque user context for delta_cb       */
    wl_arena_t *eval_arena;  /* arena for per-iteration temporaries    */
} wl_col_session_t;

/*
 * COL_SESSION: Cast wl_session_t* to wl_col_session_t*
 *
 * Safe because wl_session_t base is the first member of wl_col_session_t
 * (C11 §6.7.2.1 ¶15 guarantees address equality of struct and first member).
 */
#define COL_SESSION(s) ((wl_col_session_t *)(s))

static col_rel_t *
session_find_rel(wl_col_session_t *sess, const char *name)
{
    if (!name)
        return NULL;
    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && strcmp(sess->rels[i]->name, name) == 0)
            return sess->rels[i];
    }
    return NULL;
}

static int
session_add_rel(wl_col_session_t *sess, col_rel_t *r)
{
    if (sess->nrels >= sess->rel_cap) {
        uint32_t nc = sess->rel_cap ? sess->rel_cap * 2 : 16;
        col_rel_t **nr
            = (col_rel_t **)realloc(sess->rels, sizeof(col_rel_t *) * nc);
        if (!nr)
            return ENOMEM;
        sess->rels = nr;
        sess->rel_cap = nc;
    }
    sess->rels[sess->nrels++] = r;
    return 0;
}

static void
session_remove_rel(wl_col_session_t *sess, const char *name)
{
    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && strcmp(sess->rels[i]->name, name) == 0) {
            col_rel_free_contents(sess->rels[i]);
            free(sess->rels[i]);
            sess->rels[i] = NULL;
            return;
        }
    }
}

/* ======================================================================== */
/* Postfix Filter Expression Evaluator                                       */
/* ======================================================================== */

typedef struct {
    int64_t vals[COL_FILTER_STACK];
    uint32_t top;
} filt_stack_t;

static inline void
filt_push(filt_stack_t *s, int64_t v)
{
    if (s->top < COL_FILTER_STACK)
        s->vals[s->top++] = v;
}

static inline int64_t
filt_pop(filt_stack_t *s)
{
    return s->top != 0 ? s->vals[--s->top] : 0;
}

/*
 * col_eval_filter_row:
 * Evaluate the postfix expression buffer against a single row.
 * Variable names are assumed to be "col<N>" (rewritten by plan compiler).
 * Returns non-zero if the row passes the predicate, 0 if filtered out.
 */
static int
col_eval_filter_row(const uint8_t *buf, uint32_t size, const int64_t *row,
                    uint32_t ncols)
{
    filt_stack_t s;
    s.top = 0;

    uint32_t i = 0;
    while (i < size) {
        uint8_t tag = buf[i++];
        switch ((wl_plan_expr_tag_t)tag) {

        case WL_PLAN_EXPR_VAR: {
            if (i + 2 > size)
                goto bad;
            uint16_t nlen;
            memcpy(&nlen, buf + i, 2);
            i += 2;
            if (i + nlen > size)
                goto bad;
            /* variable name is "colN" */
            long col = 0;
            if (nlen > 3 && buf[i] == 'c' && buf[i + 1] == 'o'
                && buf[i + 2] == 'l') {
                char tmp[16] = { 0 };
                uint32_t cplen = (nlen - 3 < 15) ? nlen - 3 : 15;
                memcpy(tmp, buf + i + 3, cplen);
                col = strtol(tmp, NULL, 10);
            }
            i += nlen;
            filt_push(&s, (col >= 0 && (uint32_t)col < ncols) ? row[col] : 0);
            break;
        }

        case WL_PLAN_EXPR_CONST_INT: {
            if (i + 8 > size)
                goto bad;
            int64_t v;
            memcpy(&v, buf + i, 8);
            i += 8;
            filt_push(&s, v);
            break;
        }

        case WL_PLAN_EXPR_BOOL: {
            if (i + 1 > size)
                goto bad;
            filt_push(&s, buf[i++] ? 1 : 0);
            break;
        }

        case WL_PLAN_EXPR_CONST_STR: {
            if (i + 2 > size)
                goto bad;
            uint16_t slen;
            memcpy(&slen, buf + i, 2);
            i += 2;
            i += slen; /* skip string data, push 0 placeholder */
            filt_push(&s, 0);
            break;
        }

        /* Arithmetic */
        case WL_PLAN_EXPR_ARITH_ADD: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a + b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SUB: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a - b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_MUL: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a * b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_DIV: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, b != 0 ? a / b : 0);
            break;
        }
        case WL_PLAN_EXPR_ARITH_MOD: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, b != 0 ? a % b : 0);
            break;
        }

        /* Comparisons */
        case WL_PLAN_EXPR_CMP_EQ: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a == b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_NEQ: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a != b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_LT: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a < b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_GT: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a > b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_LTE: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a <= b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_GTE: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a >= b ? 1 : 0);
            break;
        }

        /* Aggregates: not valid in row-level filter, skip */
        case WL_PLAN_EXPR_AGG_COUNT:
        case WL_PLAN_EXPR_AGG_SUM:
        case WL_PLAN_EXPR_AGG_MIN:
        case WL_PLAN_EXPR_AGG_MAX:
            break;

        default:
            goto bad;
        }
    }
    return s.top > 0 ? (int)(s.vals[s.top - 1] != 0) : 0;

bad:
    return 1; /* parse error: pass row through */
}

/* ======================================================================== */
/* Eval Stack                                                                */
/* ======================================================================== */

/*
 * eval_entry_t: one entry on the operator evaluation stack.
 *
 * @rel:   heap-allocated result relation (owned; freed on pop)
 * @owned: true if this entry owns @rel (must free on pop)
 */
typedef struct {
    col_rel_t *rel;
    bool owned;
    bool is_delta; /* true when rel is a delta (ΔR) relation, not the full */
} eval_entry_t;

typedef struct {
    eval_entry_t items[COL_STACK_MAX];
    uint32_t top;
} eval_stack_t;

static void
eval_stack_init(eval_stack_t *s)
{
    memset(s, 0, sizeof(*s));
}

static int
eval_stack_push(eval_stack_t *s, col_rel_t *r, bool owned)
{
    if (s->top >= COL_STACK_MAX)
        return ENOBUFS;
    s->items[s->top].rel = r;
    s->items[s->top].owned = owned;
    s->items[s->top].is_delta = false;
    s->top++;
    return 0;
}

/* Push with explicit delta flag (used by VARIABLE and JOIN to tag delta results). */
static int
eval_stack_push_delta(eval_stack_t *s, col_rel_t *r, bool owned, bool is_delta)
{
    int rc = eval_stack_push(s, r, owned);
    if (rc == 0)
        s->items[s->top - 1].is_delta = is_delta;
    return rc;
}

static eval_entry_t
eval_stack_pop(eval_stack_t *s)
{
    eval_entry_t e = { NULL, false, false };
    if (s->top > 0)
        e = s->items[--s->top];
    return e;
}

static void
eval_stack_drain(eval_stack_t *s)
{
    while (s->top > 0) {
        eval_entry_t e = eval_stack_pop(s);
        if (e.owned) {
            col_rel_free_contents(e.rel);
            free(e.rel);
        }
    }
}

/* ======================================================================== */
/* Operator Implementations                                                  */
/* ======================================================================== */

/* Helper: create a new owned relation with given ncols and auto-named cols. */
static col_rel_t *
col_rel_new_auto(const char *name, uint32_t ncols)
{
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, name) != 0)
        return NULL;
    if (col_rel_set_schema(r, ncols, NULL) != 0) {
        col_rel_free_contents(r);
        free(r);
        return NULL;
    }
    return r;
}

/* Helper: create owned relation copying col_names from src. */
static col_rel_t *
col_rel_new_like(const char *name, const col_rel_t *src)
{
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, name) != 0)
        return NULL;
    if (col_rel_set_schema(r, src->ncols, (const char *const *)src->col_names)
        != 0) {
        col_rel_free_contents(r);
        free(r);
        return NULL;
    }
    return r;
}

/* --- VARIABLE ------------------------------------------------------------ */

static int
col_op_variable(const wl_plan_op_t *op, eval_stack_t *stack,
                wl_col_session_t *sess)
{
    if (!op->relation_name)
        return ENOENT;
    col_rel_t *full_rel = session_find_rel(sess, op->relation_name);
    if (!full_rel)
        return ENOENT;
    /* Prefer delta only when it is a genuine strict subset of the full
     * relation (nrows < full.nrows). On the first iteration after a
     * relation is populated its delta equals the full, so we use full
     * to avoid doubling the work vs. naive evaluation. */
    char dname[256];
    snprintf(dname, sizeof(dname), "$d$%s", op->relation_name);
    col_rel_t *delta = session_find_rel(sess, dname);
    bool use_delta
        = (delta && delta->nrows > 0 && delta->nrows < full_rel->nrows);
    col_rel_t *rel = use_delta ? delta : full_rel;
    /* push borrowed reference - session owns the relation */
    return eval_stack_push_delta(stack, rel, false, use_delta);
}

/* --- MAP ----------------------------------------------------------------- */

static int
col_op_map(const wl_plan_op_t *op, eval_stack_t *stack)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    uint32_t pc = op->project_count;
    col_rel_t *out = col_rel_new_auto("$map", pc);
    if (!out) {
        if (e.owned) {
            col_rel_free_contents(e.rel);
            free(e.rel);
        }
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * pc);
    if (!tmp) {
        col_rel_free_contents(out);
        free(out);
        if (e.owned) {
            col_rel_free_contents(e.rel);
            free(e.rel);
        }
        return ENOMEM;
    }

    for (uint32_t r = 0; r < e.rel->nrows; r++) {
        const int64_t *row = e.rel->data + (size_t)r * e.rel->ncols;
        for (uint32_t c = 0; c < pc; c++) {
            uint32_t src = op->project_indices[c];
            tmp[c] = (src < e.rel->ncols) ? row[src] : 0;
        }
        int rc = col_rel_append_row(out, tmp);
        if (rc != 0) {
            free(tmp);
            col_rel_free_contents(out);
            free(out);
            if (e.owned) {
                col_rel_free_contents(e.rel);
                free(e.rel);
            }
            return rc;
        }
    }
    free(tmp);

    if (e.owned) {
        col_rel_free_contents(e.rel);
        free(e.rel);
    }
    return eval_stack_push(stack, out, true);
}

/* --- FILTER -------------------------------------------------------------- */

static int
col_op_filter(const wl_plan_op_t *op, eval_stack_t *stack)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    col_rel_t *out = col_rel_new_like("$filter", e.rel);
    if (!out) {
        if (e.owned) {
            col_rel_free_contents(e.rel);
            free(e.rel);
        }
        return ENOMEM;
    }

    const uint8_t *buf = op->filter_expr.data;
    uint32_t bsz = op->filter_expr.size;

    for (uint32_t r = 0; r < e.rel->nrows; r++) {
        const int64_t *row = e.rel->data + (size_t)r * e.rel->ncols;
        int pass = (!buf || bsz == 0)
                       ? 1
                       : col_eval_filter_row(buf, bsz, row, e.rel->ncols);
        if (pass) {
            int rc = col_rel_append_row(out, row);
            if (rc != 0) {
                col_rel_free_contents(out);
                free(out);
                if (e.owned) {
                    col_rel_free_contents(e.rel);
                    free(e.rel);
                }
                return rc;
            }
        }
    }

    if (e.owned) {
        col_rel_free_contents(e.rel);
        free(e.rel);
    }
    return eval_stack_push(stack, out, true);
}

/* --- Hash join helpers --------------------------------------------------- */

static uint32_t
next_pow2(uint32_t n)
{
    if (n < 16)
        return 16;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    return n + 1;
}

static uint32_t
hash_int64_keys(const int64_t *row, const uint32_t *key_cols, uint32_t kc)
{
    uint32_t h = 2166136261u; /* FNV-1a offset basis */
    for (uint32_t i = 0; i < kc; i++) {
        uint64_t v = (uint64_t)row[key_cols[i]];
        h ^= (uint32_t)(v & 0xffffffff);
        h *= 16777619u;
        h ^= (uint32_t)(v >> 32);
        h *= 16777619u;
    }
    return h;
}

/* --- JOIN ---------------------------------------------------------------- */

static int
col_op_join(const wl_plan_op_t *op, eval_stack_t *stack, wl_col_session_t *sess)
{
    eval_entry_t left_e = eval_stack_pop(stack);
    if (!left_e.rel)
        return EINVAL;

    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right) {
        if (left_e.owned) {
            col_rel_free_contents(left_e.rel);
            free(left_e.rel);
        }
        return ENOENT;
    }

    /* Pass 1 (right-delta): substitute delta of right when left is a full
     * relation. This covers the A_full × ΔB variant of semi-naive:
     * when left is NOT a delta (full relation), substitute the delta of
     * right if it exists and is strictly smaller than the full right. */
    bool used_right_delta = false;
    if (!left_e.is_delta && op->right_relation) {
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        if (rdelta && rdelta->nrows > 0 && rdelta->nrows < right->nrows) {
            right = rdelta;
            used_right_delta = true;
        }
    }

    uint32_t kc = op->key_count;
    col_rel_t *left = left_e.rel;

    /* Resolve key column positions */
    uint32_t *lk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    uint32_t *rk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    if (!lk || !rk) {
        free(lk);
        free(rk);
        if (left_e.owned) {
            col_rel_free_contents(left);
            free(left);
        }
        return ENOMEM;
    }
    for (uint32_t k = 0; k < kc; k++) {
        int li = col_rel_col_idx(left, op->left_keys ? op->left_keys[k] : NULL);
        int ri
            = col_rel_col_idx(right, op->right_keys ? op->right_keys[k] : NULL);
        lk[k] = (li >= 0) ? (uint32_t)li : 0;
        rk[k] = (ri >= 0) ? (uint32_t)ri : 0;
    }

    /* Output: all left cols + all right cols (including key duplication).
     * Downstream MAP will project the desired output columns. */
    uint32_t ocols = left->ncols + right->ncols;
    col_rel_t *out = col_rel_new_auto("$join", ocols);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned) {
            col_rel_free_contents(left);
            free(left);
        }
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_free_contents(out);
        free(out);
        free(lk);
        free(rk);
        if (left_e.owned) {
            col_rel_free_contents(left);
            free(left);
        }
        return ENOMEM;
    }

    /* Hash join: build hash table from right relation, probe with left. */
    uint32_t nbuckets = next_pow2(right->nrows > 0 ? right->nrows * 2 : 1);
    uint32_t *ht_head = (uint32_t *)calloc(nbuckets, sizeof(uint32_t));
    uint32_t *ht_next
        = (uint32_t *)malloc((right->nrows + 1) * sizeof(uint32_t));
    if (!ht_head || !ht_next) {
        free(ht_head);
        free(ht_next);
        free(tmp);
        col_rel_free_contents(out);
        free(out);
        free(lk);
        free(rk);
        if (left_e.owned) {
            col_rel_free_contents(left);
            free(left);
        }
        return ENOMEM;
    }
    for (uint32_t rr = 0; rr < right->nrows; rr++) {
        const int64_t *rrow = right->data + (size_t)rr * right->ncols;
        uint32_t h = hash_int64_keys(rrow, rk, kc) & (nbuckets - 1);
        ht_next[rr] = ht_head[h];
        ht_head[h] = rr + 1; /* 1-based index; 0 = end of chain */
    }

    int join_rc = 0;
    for (uint32_t lr = 0; lr < left->nrows && join_rc == 0; lr++) {
        const int64_t *lrow = left->data + (size_t)lr * left->ncols;
        uint32_t h = hash_int64_keys(lrow, lk, kc) & (nbuckets - 1);
        for (uint32_t e = ht_head[h]; e != 0; e = ht_next[e - 1]) {
            uint32_t rr = e - 1;
            const int64_t *rrow = right->data + (size_t)rr * right->ncols;
            bool match = true;
            for (uint32_t k = 0; k < kc && match; k++)
                match = (lrow[lk[k]] == rrow[rk[k]]);
            if (!match)
                continue;
            memcpy(tmp, lrow, sizeof(int64_t) * left->ncols);
            memcpy(tmp + left->ncols, rrow, sizeof(int64_t) * right->ncols);
            join_rc = col_rel_append_row(out, tmp);
            if (join_rc != 0)
                break;
        }
    }
    free(ht_head);
    free(ht_next);
    if (join_rc != 0) {
        free(tmp);
        col_rel_free_contents(out);
        free(out);
        free(lk);
        free(rk);
        if (left_e.owned) {
            col_rel_free_contents(left);
            free(left);
        }
        return join_rc;
    }

    free(tmp);
    free(lk);
    free(rk);
    if (left_e.owned) {
        col_rel_free_contents(left);
        free(left);
    }
    /* Propagate delta flag: result is a delta if left was delta OR we used
     * right-delta. This ensures subsequent JOINs in the same rule plan know
     * whether to apply right-delta (they should NOT if we already used one). */
    bool result_is_delta = left_e.is_delta || used_right_delta;
    return eval_stack_push_delta(stack, out, true, result_is_delta);
}

/* --- ANTIJOIN ------------------------------------------------------------ */

static int
col_op_antijoin(const wl_plan_op_t *op, eval_stack_t *stack,
                wl_col_session_t *sess)
{
    eval_entry_t left_e = eval_stack_pop(stack);
    if (!left_e.rel)
        return EINVAL;

    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right) {
        /* If right relation doesn't exist, antijoin keeps all left rows */
        return eval_stack_push(stack, left_e.rel, left_e.owned);
    }

    col_rel_t *left = left_e.rel;
    uint32_t kc = op->key_count;

    uint32_t *lk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    uint32_t *rk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    if (!lk || !rk) {
        free(lk);
        free(rk);
        if (left_e.owned) {
            col_rel_free_contents(left);
            free(left);
        }
        return ENOMEM;
    }
    for (uint32_t k = 0; k < kc; k++) {
        int li = col_rel_col_idx(left, op->left_keys ? op->left_keys[k] : NULL);
        int ri
            = col_rel_col_idx(right, op->right_keys ? op->right_keys[k] : NULL);
        lk[k] = (li >= 0) ? (uint32_t)li : 0;
        rk[k] = (ri >= 0) ? (uint32_t)ri : 0;
    }

    col_rel_t *out = col_rel_new_like("$antijoin", left);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned) {
            col_rel_free_contents(left);
            free(left);
        }
        return ENOMEM;
    }

    /* Hash antijoin: build hash set from right, iterate left. */
    uint32_t aj_nbuckets = next_pow2(right->nrows > 0 ? right->nrows * 2 : 1);
    uint32_t *aj_head = (uint32_t *)calloc(aj_nbuckets, sizeof(uint32_t));
    uint32_t *aj_next
        = (uint32_t *)malloc((right->nrows + 1) * sizeof(uint32_t));
    if (!aj_head || !aj_next) {
        free(aj_head);
        free(aj_next);
        col_rel_free_contents(out);
        free(out);
        free(lk);
        free(rk);
        if (left_e.owned) {
            col_rel_free_contents(left);
            free(left);
        }
        return ENOMEM;
    }
    for (uint32_t rr = 0; rr < right->nrows; rr++) {
        const int64_t *rrow = right->data + (size_t)rr * right->ncols;
        uint32_t h = hash_int64_keys(rrow, rk, kc) & (aj_nbuckets - 1);
        aj_next[rr] = aj_head[h];
        aj_head[h] = rr + 1;
    }
    int aj_rc = 0;
    for (uint32_t lr = 0; lr < left->nrows && aj_rc == 0; lr++) {
        const int64_t *lrow = left->data + (size_t)lr * left->ncols;
        uint32_t h = hash_int64_keys(lrow, lk, kc) & (aj_nbuckets - 1);
        bool found = false;
        for (uint32_t e = aj_head[h]; e != 0 && !found; e = aj_next[e - 1]) {
            uint32_t rr = e - 1;
            const int64_t *rrow = right->data + (size_t)rr * right->ncols;
            bool match = true;
            for (uint32_t k = 0; k < kc && match; k++)
                match = (lrow[lk[k]] == rrow[rk[k]]);
            if (match)
                found = true;
        }
        if (!found)
            aj_rc = col_rel_append_row(out, lrow);
    }
    free(aj_head);
    free(aj_next);
    if (aj_rc != 0) {
        col_rel_free_contents(out);
        free(out);
        free(lk);
        free(rk);
        if (left_e.owned) {
            col_rel_free_contents(left);
            free(left);
        }
        return aj_rc;
    }

    free(lk);
    free(rk);
    if (left_e.owned) {
        col_rel_free_contents(left);
        free(left);
    }
    return eval_stack_push(stack, out, true);
}

/* --- CONCAT -------------------------------------------------------------- */

static int
col_op_concat(eval_stack_t *stack)
{
    if (stack->top < 2)
        return EINVAL;

    eval_entry_t b_e = eval_stack_pop(stack);
    eval_entry_t a_e = eval_stack_pop(stack);
    col_rel_t *a = a_e.rel;
    col_rel_t *b = b_e.rel;

    if (!a || !b || a->ncols != b->ncols) {
        if (a_e.owned) {
            col_rel_free_contents(a);
            free(a);
        }
        if (b_e.owned) {
            col_rel_free_contents(b);
            free(b);
        }
        return EINVAL;
    }

    col_rel_t *out = col_rel_new_like("$concat", a);
    if (!out) {
        if (a_e.owned) {
            col_rel_free_contents(a);
            free(a);
        }
        if (b_e.owned) {
            col_rel_free_contents(b);
            free(b);
        }
        return ENOMEM;
    }

    int rc = col_rel_append_all(out, a);
    if (rc == 0)
        rc = col_rel_append_all(out, b);

    if (a_e.owned) {
        col_rel_free_contents(a);
        free(a);
    }
    if (b_e.owned) {
        col_rel_free_contents(b);
        free(b);
    }

    if (rc != 0) {
        col_rel_free_contents(out);
        free(out);
        return rc;
    }
    return eval_stack_push(stack, out, true);
}

/* --- CONSOLIDATE --------------------------------------------------------- */

/* Comparison for qsort: lexicographic int64 row order. */
static uint32_t g_consolidate_ncols; /* qsort context (single-threaded) */

static int
row_cmp(const void *a, const void *b)
{
    const int64_t *ra = (const int64_t *)a;
    const int64_t *rb = (const int64_t *)b;
    for (uint32_t c = 0; c < g_consolidate_ncols; c++) {
        if (ra[c] < rb[c])
            return -1;
        if (ra[c] > rb[c])
            return 1;
    }
    return 0;
}

static int
col_op_consolidate(eval_stack_t *stack)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    col_rel_t *in = e.rel;
    uint32_t nc = in->ncols;
    uint32_t nr = in->nrows;

    if (nr <= 1) {
        /* Nothing to deduplicate */
        return eval_stack_push(stack, in, e.owned);
    }

    /* Sort in-place if we own the relation, otherwise copy first */
    col_rel_t *work = in;
    bool work_owned = e.owned;
    if (!work_owned) {
        work = col_rel_new_like("$consol", in);
        if (!work)
            return ENOMEM;
        if (col_rel_append_all(work, in) != 0) {
            col_rel_free_contents(work);
            free(work);
            return ENOMEM;
        }
        work_owned = true;
    }

    g_consolidate_ncols = nc;
    qsort(work->data, nr, sizeof(int64_t) * nc, row_cmp);

    /* Compact: keep only unique rows */
    uint32_t out_r = 1; /* first row always kept */
    for (uint32_t r = 1; r < nr; r++) {
        const int64_t *prev = work->data + (size_t)(r - 1) * nc;
        const int64_t *cur = work->data + (size_t)r * nc;
        if (memcmp(prev, cur, sizeof(int64_t) * nc) != 0) {
            if (out_r != r)
                memcpy(work->data + (size_t)out_r * nc, cur,
                       sizeof(int64_t) * nc);
            out_r++;
        }
    }
    work->nrows = out_r;

    return eval_stack_push(stack, work, work_owned);
}

/*
 * col_op_consolidate_incremental:
 * Incremental sort+dedup for semi-naive evaluation.
 *
 * Precondition: rel->data[0..old_nrows) is already sorted+unique from
 * the previous iteration's consolidation. New rows appended during this
 * iteration live in [old_nrows..rel->nrows).
 *
 * Algorithm:
 *   1. Sort only the delta rows: O(D log D)
 *   2. Dedup within delta: O(D)
 *   3. Merge sorted old with sorted delta, skipping duplicates: O(N + D)
 *
 * Total: O(D log D + N) vs O(N log N) for full re-sort.
 */
static int
col_op_consolidate_incremental(col_rel_t *rel, uint32_t old_nrows)
{
    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;

    if (nr <= 1 || old_nrows >= nr)
        return 0; /* nothing new or trivially sorted */

    uint32_t delta_count = nr - old_nrows;
    int64_t *delta_start = rel->data + (size_t)old_nrows * nc;
    size_t row_bytes = (size_t)nc * sizeof(int64_t);

    /* Phase 1: sort only the new delta rows */
    g_consolidate_ncols = nc;
    qsort(delta_start, delta_count, row_bytes, row_cmp);

    /* Phase 1b: dedup within delta */
    uint32_t d_unique = 1;
    for (uint32_t i = 1; i < delta_count; i++) {
        if (memcmp(delta_start + (size_t)(i - 1) * nc,
                   delta_start + (size_t)i * nc, row_bytes)
            != 0) {
            if (d_unique != i)
                memcpy(delta_start + (size_t)d_unique * nc,
                       delta_start + (size_t)i * nc, row_bytes);
            d_unique++;
        }
    }

    /* Phase 2: merge sorted old [0..old_nrows) with sorted+unique delta.
     * Allocate temporary buffer for merge output. */
    size_t max_rows = (size_t)old_nrows + d_unique;
    int64_t *merged = (int64_t *)malloc(max_rows * nc * sizeof(int64_t));
    if (!merged)
        return ENOMEM;

    uint32_t oi = 0, di = 0, out = 0;
    while (oi < old_nrows && di < d_unique) {
        const int64_t *orow = rel->data + (size_t)oi * nc;
        const int64_t *drow = delta_start + (size_t)di * nc;
        int cmp = memcmp(orow, drow, row_bytes);
        if (cmp < 0) {
            memcpy(merged + (size_t)out * nc, orow, row_bytes);
            oi++;
            out++;
        } else if (cmp == 0) {
            memcpy(merged + (size_t)out * nc, orow, row_bytes);
            oi++;
            di++;
            out++; /* skip duplicate from delta */
        } else {
            memcpy(merged + (size_t)out * nc, drow, row_bytes);
            di++;
            out++;
        }
    }
    /* Copy remaining from old */
    if (oi < old_nrows) {
        uint32_t remaining = old_nrows - oi;
        memcpy(merged + (size_t)out * nc, rel->data + (size_t)oi * nc,
               (size_t)remaining * row_bytes);
        out += remaining;
    }
    /* Copy remaining from delta */
    if (di < d_unique) {
        uint32_t remaining = d_unique - di;
        memcpy(merged + (size_t)out * nc, delta_start + (size_t)di * nc,
               (size_t)remaining * row_bytes);
        out += remaining;
    }

    /* Swap buffer */
    free(rel->data);
    rel->data = merged;
    rel->nrows = out;
    rel->capacity = (uint32_t)max_rows;
    return 0;
}

/* --- SEMIJOIN ------------------------------------------------------------ */

static int
col_op_semijoin(const wl_plan_op_t *op, eval_stack_t *stack,
                wl_col_session_t *sess)
{
    eval_entry_t left_e = eval_stack_pop(stack);
    if (!left_e.rel)
        return EINVAL;

    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right)
        return eval_stack_push(stack, left_e.rel, left_e.owned);

    col_rel_t *left = left_e.rel;
    uint32_t kc = op->key_count;

    uint32_t *lk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    uint32_t *rk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    if (!lk || !rk) {
        free(lk);
        free(rk);
        if (left_e.owned) {
            col_rel_free_contents(left);
            free(left);
        }
        return ENOMEM;
    }
    for (uint32_t k = 0; k < kc; k++) {
        int li = col_rel_col_idx(left, op->left_keys ? op->left_keys[k] : NULL);
        int ri
            = col_rel_col_idx(right, op->right_keys ? op->right_keys[k] : NULL);
        lk[k] = (li >= 0) ? (uint32_t)li : 0;
        rk[k] = (ri >= 0) ? (uint32_t)ri : 0;
    }

    /* Output: project_indices selects output columns from left */
    uint32_t ocols = op->project_count ? op->project_count : left->ncols;
    col_rel_t *out = col_rel_new_auto("$semijoin", ocols);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned) {
            col_rel_free_contents(left);
            free(left);
        }
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_free_contents(out);
        free(out);
        free(lk);
        free(rk);
        if (left_e.owned) {
            col_rel_free_contents(left);
            free(left);
        }
        return ENOMEM;
    }

    for (uint32_t lr = 0; lr < left->nrows; lr++) {
        const int64_t *lrow = left->data + (size_t)lr * left->ncols;
        bool found = false;
        for (uint32_t rr = 0; rr < right->nrows && !found; rr++) {
            const int64_t *rrow = right->data + (size_t)rr * right->ncols;
            bool match = true;
            for (uint32_t k = 0; k < kc && match; k++)
                match = (lrow[lk[k]] == rrow[rk[k]]);
            if (match)
                found = true;
        }
        if (found) {
            if (op->project_count > 0 && op->project_indices) {
                for (uint32_t c = 0; c < ocols; c++) {
                    uint32_t si = op->project_indices[c];
                    tmp[c] = (si < left->ncols) ? lrow[si] : 0;
                }
            } else {
                memcpy(tmp, lrow, sizeof(int64_t) * left->ncols);
            }
            int rc = col_rel_append_row(out, tmp);
            if (rc != 0) {
                free(tmp);
                col_rel_free_contents(out);
                free(out);
                free(lk);
                free(rk);
                if (left_e.owned) {
                    col_rel_free_contents(left);
                    free(left);
                }
                return rc;
            }
        }
    }

    free(tmp);
    free(lk);
    free(rk);
    if (left_e.owned) {
        col_rel_free_contents(left);
        free(left);
    }
    return eval_stack_push(stack, out, true);
}

/* --- REDUCE (aggregate) -------------------------------------------------- */

static int
col_op_reduce(const wl_plan_op_t *op, eval_stack_t *stack)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    col_rel_t *in = e.rel;
    uint32_t gc = op->group_by_count;

    /* Output: group_by columns + 1 aggregate column */
    uint32_t ocols = gc + 1;
    col_rel_t *out = col_rel_new_auto("$reduce", ocols);
    if (!out) {
        if (e.owned) {
            col_rel_free_contents(in);
            free(in);
        }
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_free_contents(out);
        free(out);
        if (e.owned) {
            col_rel_free_contents(in);
            free(in);
        }
        return ENOMEM;
    }

    /* Sort by group key for group-by */
    /* (Simple O(n^2) implementation; sufficient for Phase 2A) */
    for (uint32_t r = 0; r < in->nrows; r++) {
        const int64_t *row = in->data + (size_t)r * in->ncols;

        /* Check if this group key already exists in output */
        bool found = false;
        for (uint32_t o = 0; o < out->nrows; o++) {
            int64_t *orow = out->data + (size_t)o * ocols;
            bool match = true;
            for (uint32_t k = 0; k < gc && match; k++) {
                uint32_t gi
                    = op->group_by_indices ? op->group_by_indices[k] : k;
                match = (row[gi < in->ncols ? gi : 0] == orow[k]);
            }
            if (match) {
                /* Update aggregate */
                int64_t val = (in->ncols > gc) ? row[gc] : 1;
                switch (op->agg_fn) {
                case WIRELOG_AGG_COUNT:
                    orow[gc]++;
                    break;
                case WIRELOG_AGG_SUM:
                    orow[gc] += val;
                    break;
                case WIRELOG_AGG_MIN:
                    if (val < orow[gc])
                        orow[gc] = val;
                    break;
                case WIRELOG_AGG_MAX:
                    if (val > orow[gc])
                        orow[gc] = val;
                    break;
                default:
                    break;
                }
                found = true;
                break;
            }
        }
        if (!found) {
            for (uint32_t k = 0; k < gc; k++) {
                uint32_t gi
                    = op->group_by_indices ? op->group_by_indices[k] : k;
                tmp[k] = row[gi < in->ncols ? gi : 0];
            }
            int64_t init_val = (in->ncols > gc) ? row[gc] : 1;
            tmp[gc] = (op->agg_fn == WIRELOG_AGG_COUNT) ? 1 : init_val;
            int rc = col_rel_append_row(out, tmp);
            if (rc != 0) {
                free(tmp);
                col_rel_free_contents(out);
                free(out);
                if (e.owned) {
                    col_rel_free_contents(in);
                    free(in);
                }
                return rc;
            }
        }
    }

    free(tmp);
    if (e.owned) {
        col_rel_free_contents(in);
        free(in);
    }
    return eval_stack_push(stack, out, true);
}

/* ======================================================================== */
/* Stratum Evaluator                                                         */
/* ======================================================================== */

/*
 * col_eval_relation_plan:
 * Evaluate all operators for one relation plan using the eval stack.
 * On success, the top of stack holds the result relation (owned).
 */
static int
col_eval_relation_plan(const wl_plan_relation_t *rplan, eval_stack_t *stack,
                       wl_col_session_t *sess)
{
    for (uint32_t i = 0; i < rplan->op_count; i++) {
        const wl_plan_op_t *op = &rplan->ops[i];
        int rc = 0;
        switch (op->op) {
        case WL_PLAN_OP_VARIABLE:
            rc = col_op_variable(op, stack, sess);
            break;
        case WL_PLAN_OP_MAP:
            rc = col_op_map(op, stack);
            break;
        case WL_PLAN_OP_FILTER:
            rc = col_op_filter(op, stack);
            break;
        case WL_PLAN_OP_JOIN:
            rc = col_op_join(op, stack, sess);
            break;
        case WL_PLAN_OP_ANTIJOIN:
            rc = col_op_antijoin(op, stack, sess);
            break;
        case WL_PLAN_OP_CONCAT:
            rc = col_op_concat(stack);
            break;
        case WL_PLAN_OP_CONSOLIDATE:
            rc = col_op_consolidate(stack);
            break;
        case WL_PLAN_OP_REDUCE:
            rc = col_op_reduce(op, stack);
            break;
        case WL_PLAN_OP_SEMIJOIN:
            rc = col_op_semijoin(op, stack, sess);
            break;
        default:
            break;
        }
        if (rc != 0)
            return rc;
    }
    return 0;
}

/*
 * col_eval_stratum:
 * Evaluate one stratum, writing results into session relations.
 * Non-recursive strata are evaluated once.
 * Recursive strata use semi-naive fixed-point iteration.
 *
 * Returns 0 on success, non-zero on error.
 */
static int
col_eval_stratum(const wl_plan_stratum_t *sp, wl_col_session_t *sess)
{
    if (!sp->is_recursive) {
        /* Non-recursive: evaluate each relation plan once */
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            const wl_plan_relation_t *rp = &sp->relations[ri];

            eval_stack_t stack;
            eval_stack_init(&stack);

            int rc = col_eval_relation_plan(rp, &stack, sess);
            if (rc != 0) {
                eval_stack_drain(&stack);
                return rc;
            }

            if (stack.top == 0)
                continue;

            eval_entry_t result = eval_stack_pop(&stack);
            eval_stack_drain(&stack); /* drain any leftover entries */

            col_rel_t *target = session_find_rel(sess, rp->name);
            if (!target) {
                /* First time: create and register the relation */
                if (result.owned) {
                    /* Rename the result relation */
                    free(result.rel->name);
                    result.rel->name = wl_strdup(rp->name);
                    if (!result.rel->name) {
                        col_rel_free_contents(result.rel);
                        free(result.rel);
                        return ENOMEM;
                    }
                    rc = session_add_rel(sess, result.rel);
                    if (rc != 0) {
                        col_rel_free_contents(result.rel);
                        free(result.rel);
                        return rc;
                    }
                    result.owned = false;
                } else {
                    col_rel_t *copy = col_rel_new_like(rp->name, result.rel);
                    if (!copy)
                        return ENOMEM;
                    if ((rc = col_rel_append_all(copy, result.rel)) != 0) {
                        col_rel_free_contents(copy);
                        free(copy);
                        return rc;
                    }
                    rc = session_add_rel(sess, copy);
                    if (rc != 0) {
                        col_rel_free_contents(copy);
                        free(copy);
                        return rc;
                    }
                }
            } else {
                /* Append new results to existing relation */
                rc = col_rel_append_all(target, result.rel);
                if (result.owned) {
                    col_rel_free_contents(result.rel);
                    free(result.rel);
                }
                if (rc != 0)
                    return rc;
            }
        }
        return 0;
    }

    /*
     * Recursive stratum: semi-naive fixed-point iteration.
     * Iterate until no new tuples are produced.
     *
     * Pre-register empty IDB relations so that VARIABLE ops can find
     * them on the first iteration (before any tuples are produced).
     */
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        col_rel_t *existing = session_find_rel(sess, sp->relations[ri].name);
        if (!existing) {
            col_rel_t *empty = NULL;
            int rc = col_rel_alloc(&empty, sp->relations[ri].name);
            if (rc != 0)
                return ENOMEM;
            rc = session_add_rel(sess, empty);
            if (rc != 0) {
                col_rel_free_contents(empty);
                free(empty);
                return rc;
            }
        }
    }

    /*
     * Semi-naive fixed-point iteration with delta tracking.
     * VARIABLE ops prefer "$d$relname" delta relations (rows added in the
     * previous iteration). JOIN right-side lookups always use the full
     * relation by name, giving delta (left) x full (right) join semantics.
     */
    uint32_t nrels = sp->relation_count;
    col_rel_t **delta_rels = (col_rel_t **)calloc(nrels, sizeof(col_rel_t *));
    if (!delta_rels)
        return ENOMEM;

    for (uint32_t iter = 0; iter < MAX_ITERATIONS; iter++) {
        /* Register delta relations from previous iteration into session */
        for (uint32_t ri = 0; ri < nrels; ri++) {
            if (!delta_rels[ri])
                continue;
            char dname[256];
            snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
            session_remove_rel(sess, dname);
            int rc = session_add_rel(sess, delta_rels[ri]);
            if (rc != 0) {
                col_rel_free_contents(delta_rels[ri]);
                free(delta_rels[ri]);
            }
            delta_rels[ri] = NULL; /* session now owns it */
        }

        /* Record per-relation row counts and save sorted snapshots for delta
         * computation after consolidation. old_data[ri] is a copy of the
         * pre-evaluation sorted+unique relation; delta = R_new - R_old. */
        uint32_t *snap = (uint32_t *)malloc(nrels * sizeof(uint32_t));
        int64_t **old_data = (int64_t **)calloc(nrels, sizeof(int64_t *));
        if (!snap || !old_data) {
            free(snap);
            free(old_data);
            free(delta_rels);
            return ENOMEM;
        }
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
            snap[ri] = r ? r->nrows : 0;
            if (snap[ri] > 0 && r && r->ncols > 0) {
                size_t bytes = (size_t)snap[ri] * r->ncols * sizeof(int64_t);
                old_data[ri] = (int64_t *)malloc(bytes);
                if (old_data[ri])
                    memcpy(old_data[ri], r->data, bytes);
            }
        }

        /* Single-pass semi-naive evaluation. VARIABLE prefers delta when it
         * is a strict subset of full (genuine new facts). JOIN propagates
         * the is_delta flag through results and applies right-delta when
         * left is full and a strictly-smaller right delta exists. */
        for (uint32_t ri = 0; ri < nrels; ri++) {
            const wl_plan_relation_t *rp = &sp->relations[ri];

            eval_stack_t stack;
            eval_stack_init(&stack);

            int rc = col_eval_relation_plan(rp, &stack, sess);
            if (rc != 0) {
                eval_stack_drain(&stack);
                free(snap);
                free(delta_rels);
                return rc;
            }

            if (stack.top == 0)
                continue;

            eval_entry_t result = eval_stack_pop(&stack);
            eval_stack_drain(&stack);

            col_rel_t *target = session_find_rel(sess, rp->name);
            if (!target) {
                col_rel_t *copy;
                if (result.owned) {
                    copy = result.rel;
                    free(copy->name);
                    copy->name = wl_strdup(rp->name);
                    if (!copy->name) {
                        col_rel_free_contents(copy);
                        free(copy);
                        free(snap);
                        free(delta_rels);
                        return ENOMEM;
                    }
                    result.owned = false;
                } else {
                    copy = col_rel_new_like(rp->name, result.rel);
                    if (!copy) {
                        free(snap);
                        free(delta_rels);
                        return ENOMEM;
                    }
                    if ((rc = col_rel_append_all(copy, result.rel)) != 0) {
                        col_rel_free_contents(copy);
                        free(copy);
                        free(snap);
                        free(delta_rels);
                        return rc;
                    }
                }
                rc = session_add_rel(sess, copy);
                if (rc != 0) {
                    col_rel_free_contents(copy);
                    free(copy);
                    free(snap);
                    free(delta_rels);
                    return rc;
                }
            } else {
                /* Adopt schema from result if target is still uninitialized */
                if (target->ncols == 0 && result.rel->ncols > 0) {
                    rc = col_rel_set_schema(
                        target, result.rel->ncols,
                        (const char *const *)result.rel->col_names);
                    if (rc != 0) {
                        if (result.owned) {
                            col_rel_free_contents(result.rel);
                            free(result.rel);
                        }
                        free(snap);
                        free(delta_rels);
                        return rc;
                    }
                }
                rc = col_rel_append_all(target, result.rel);
                if (result.owned) {
                    col_rel_free_contents(result.rel);
                    free(result.rel);
                }
                if (rc != 0) {
                    free(snap);
                    free(delta_rels);
                    return rc;
                }
            }
        }

        /* Remove delta relations from session (evaluation is complete) */
        for (uint32_t ri = 0; ri < nrels; ri++) {
            char dname[256];
            snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
            session_remove_rel(sess, dname);
        }

        /* Consolidate all IDB relations to remove duplicates.
         * Use incremental merge: sort only new rows O(D log D), then
         * merge with sorted old prefix O(N).  snap[ri] marks the boundary
         * between the already-sorted prefix and unsorted new rows. */
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
            if (!r || r->nrows == 0)
                continue;

            int rc2 = col_op_consolidate_incremental(r, snap[ri]);
            if (rc2 != 0) {
                for (uint32_t j = 0; j < nrels; j++)
                    free(old_data[j]);
                free(old_data);
                free(snap);
                free(delta_rels);
                return rc2;
            }
        }

        /* Compute proper delta: R_new - R_old via sorted merge walk.
         * Both old_data[ri] and r->data are sorted+unique (old from previous
         * consolidation, new from consolidation just done above).
         * This ensures delta contains only truly new tuples, preventing the
         * base-case re-evaluation from polluting delta with known rows. */
        bool any_new = false;
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
            if (!r || r->nrows == 0 || r->ncols == 0)
                continue;
            char dname[256];
            snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
            col_rel_t *delta = col_rel_new_like(dname, r);
            if (!delta)
                continue;
            uint32_t nc = r->ncols;
            size_t row_bytes = (size_t)nc * sizeof(int64_t);
            int64_t *old = old_data[ri];
            uint32_t old_n = snap[ri];
            if (old == NULL || old_n == 0) {
                /* First time: all consolidated rows are new */
                col_rel_append_all(delta, r); /* best-effort */
            } else {
                /* Merge walk: emit rows in R_new not present in R_old */
                uint32_t oi = 0, ni = 0;
                while (ni < r->nrows) {
                    const int64_t *nrow = r->data + (size_t)ni * nc;
                    if (oi < old_n) {
                        const int64_t *orow = old + (size_t)oi * nc;
                        int cmp = memcmp(nrow, orow, row_bytes);
                        if (cmp < 0) {
                            col_rel_append_row(delta, nrow);
                            ni++;
                        } else if (cmp == 0) {
                            ni++;
                            oi++;
                        } else {
                            oi++;
                        }
                    } else {
                        col_rel_append_row(delta, nrow);
                        ni++;
                    }
                }
            }
            if (delta->nrows > 0) {
                delta_rels[ri] = delta;
                any_new = true;
            } else {
                col_rel_free_contents(delta);
                free(delta);
            }
        }

        /* Free pre-evaluation snapshots */
        for (uint32_t ri = 0; ri < nrels; ri++)
            free(old_data[ri]);
        free(old_data);
        free(snap);

        if (!any_new)
            break;
    }

    /* Cleanup any remaining pending delta relations */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        if (delta_rels[ri]) {
            col_rel_free_contents(delta_rels[ri]);
            free(delta_rels[ri]);
        }
        char dname[256];
        snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
        session_remove_rel(sess, dname);
    }
    free(delta_rels);
    return 0;
}

/* ======================================================================== */
/* Vtable Functions                                                          */
/* ======================================================================== */

/*
 * col_session_create: Initialize a columnar backend session
 *
 * Implements wl_compute_backend_t.session_create vtable slot.
 *
 * @param plan:        Execution plan (borrowed, must outlive session)
 * @param num_workers: Ignored; columnar backend is single-threaded
 * @param out:         (out) Receives &sess->base on success
 *
 * Memory initialization order:
 *   1. Allocate wl_col_session_t (zero-initialized via calloc)
 *   2. Set sess->plan = plan (borrowed reference)
 *   3. Allocate rels[] with initial capacity 16
 *   4. Pre-register EDB relations from plan->edb_relations (ncols lazy-inited)
 *   5. Set *out = &sess->base  (session.c:38 then sets base.backend)
 *
 * @return 0 on success, EINVAL if plan/out is NULL, ENOMEM on alloc failure
 *
 * @see wl_session_create in session.c for vtable dispatch context
 * @see wl_col_session_t memory layout documentation above
 */
static int
col_session_create(const wl_plan_t *plan, uint32_t num_workers,
                   wl_session_t **out)
{
    (void)num_workers; /* columnar backend is single-threaded */

    if (!plan || !out)
        return EINVAL;

    wl_col_session_t *sess
        = (wl_col_session_t *)calloc(1, sizeof(wl_col_session_t));
    if (!sess)
        return ENOMEM;

    sess->plan = plan;
    sess->rel_cap = 16;
    sess->rels = (col_rel_t **)calloc(sess->rel_cap, sizeof(col_rel_t *));
    if (!sess->rels) {
        free(sess);
        return ENOMEM;
    }

    /* Allocate per-iteration arena (256MB for temporary evaluation data) */
    sess->eval_arena = wl_arena_create(256 * 1024 * 1024);
    if (!sess->eval_arena) {
        free(sess->rels);
        free(sess);
        return ENOMEM;
    }

    /* Pre-register EDB relations (ncols determined at first insert) */
    for (uint32_t i = 0; i < plan->edb_count; i++) {
        col_rel_t *r = NULL;
        int rc = col_rel_alloc(&r, plan->edb_relations[i]);
        if (rc != 0)
            goto oom;
        rc = session_add_rel(sess, r);
        if (rc != 0) {
            col_rel_free_contents(r);
            free(r);
            goto oom;
        }
    }

    *out = &sess->base;
    return 0;

oom:
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_free_contents(sess->rels[i]);
        free(sess->rels[i]);
    }
    free(sess->rels);
    free(sess);
    return ENOMEM;
}

/*
 * col_session_destroy: Free all resources owned by a columnar session
 *
 * Implements wl_compute_backend_t.session_destroy vtable slot.
 * NULL-safe. Frees rels[], each rels[i], and the session struct itself.
 * The plan is borrowed and NOT freed here.
 *
 * @param session: wl_session_t* (cast to wl_col_session_t* internally)
 */
static void
col_session_destroy(wl_session_t *session)
{
    if (!session)
        return;
    wl_col_session_t *sess = COL_SESSION(session);
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_free_contents(sess->rels[i]);
        free(sess->rels[i]);
    }
    free(sess->rels);
    if (sess->eval_arena)
        wl_arena_free(sess->eval_arena);
    free(sess);
}

static int
col_session_insert(wl_session_t *session, const char *relation,
                   const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    if (!session || !relation || !data)
        return EINVAL;

    col_rel_t *r = session_find_rel(COL_SESSION(session), relation);
    if (!r)
        return ENOENT;

    /* Lazy schema initialisation on first insert */
    if (r->ncols == 0) {
        int rc = col_rel_set_schema(r, num_cols, NULL);
        if (rc != 0)
            return rc;
    } else if (r->ncols != num_cols) {
        return EINVAL; /* column count mismatch */
    }

    for (uint32_t i = 0; i < num_rows; i++) {
        int rc = col_rel_append_row(r, data + (size_t)i * num_cols);
        if (rc != 0)
            return rc;
    }
    return 0;
}

static int
col_session_remove(wl_session_t *session, const char *relation,
                   const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    if (!session || !relation || !data)
        return EINVAL;

    col_rel_t *r = session_find_rel(COL_SESSION(session), relation);
    if (!r || r->ncols != num_cols)
        return ENOENT;

    /* Compact: remove matching rows */
    for (uint32_t di = 0; di < num_rows; di++) {
        const int64_t *del = data + (size_t)di * num_cols;
        uint32_t out_r = 0;
        for (uint32_t ri = 0; ri < r->nrows; ri++) {
            const int64_t *row = r->data + (size_t)ri * num_cols;
            if (memcmp(row, del, sizeof(int64_t) * num_cols) != 0) {
                if (out_r != ri)
                    memcpy(r->data + (size_t)out_r * num_cols, row,
                           sizeof(int64_t) * num_cols);
                out_r++;
            } else {
                /* Remove first matching row only */
                di = num_rows; /* break outer loop after this one */
                for (uint32_t rest = ri + 1; rest < r->nrows; rest++, out_r++)
                    memcpy(r->data + (size_t)out_r * num_cols,
                           r->data + (size_t)rest * num_cols,
                           sizeof(int64_t) * num_cols);
                r->nrows = out_r;
                goto next_del;
            }
        }
        r->nrows = out_r;
    next_del:;
    }
    return 0;
}

/*
 * col_row_in_sorted: Binary search for a row in a sorted int64 row buffer.
 *
 * @param sorted_data: Row-major int64 buffer sorted by memcmp row order
 * @param nrows:       Number of rows in sorted_data
 * @param ncols:       Columns per row
 * @param row:         The row to search for
 * @return true if found, false otherwise
 */
static bool
col_row_in_sorted(const int64_t *sorted_data, uint32_t nrows, uint32_t ncols,
                  const int64_t *row)
{
    if (!sorted_data || nrows == 0 || ncols == 0)
        return false;
    uint32_t lo = 0, hi = nrows;
    size_t row_bytes = sizeof(int64_t) * ncols;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        int cmp = memcmp(sorted_data + (size_t)mid * ncols, row, row_bytes);
        if (cmp == 0)
            return true;
        if (cmp < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return false;
}

/*
 * col_idb_consolidate: Sort + dedup one IDB relation in-place.
 *
 * Reuses the eval stack + col_op_consolidate operator so sort order
 * is consistent with the rest of the evaluation pipeline.
 */
static int
col_idb_consolidate(col_rel_t *r)
{
    eval_stack_t stk;
    eval_stack_init(&stk);
    int rc = eval_stack_push(&stk, r, false); /* borrowed */
    if (rc != 0)
        return rc;
    col_op_consolidate(&stk);
    if (stk.top > 0) {
        eval_entry_t ce = eval_stack_pop(&stk);
        if (ce.owned && ce.rel != r) {
            free(r->data);
            r->data = ce.rel->data;
            r->nrows = ce.rel->nrows;
            r->capacity = ce.rel->capacity;
            ce.rel->data = NULL;
            col_rel_free_contents(ce.rel);
            free(ce.rel);
        }
    }
    return 0;
}

/*
 * col_stratum_step_with_delta: Evaluate one stratum and fire delta callbacks.
 *
 * Phase 2A algorithm (full re-eval + set diff):
 *   1. Snapshot each IDB relation's current sorted rows (prev state)
 *   2. Run col_eval_stratum (appends newly derived rows)
 *   3. Consolidate each IDB relation (sort + dedup)
 *   4. Fire delta_cb(+1) for each row in new state not found in prev state
 *   5. Free snapshots
 *
 * TODO(Phase 2B): Replace step 2 with semi-naive ΔR propagation.
 */
static int
col_stratum_step_with_delta(const wl_plan_stratum_t *sp, wl_col_session_t *sess)
{
    uint32_t rc_cnt = sp->relation_count;

    /* Allocate snapshot arrays */
    int64_t **prev_data = (int64_t **)calloc(rc_cnt, sizeof(int64_t *));
    uint32_t *prev_nrows = (uint32_t *)calloc(rc_cnt, sizeof(uint32_t));
    uint32_t *prev_ncols = (uint32_t *)calloc(rc_cnt, sizeof(uint32_t));
    if (!prev_data || !prev_nrows || !prev_ncols) {
        free(prev_data);
        free(prev_nrows);
        free(prev_ncols);
        return ENOMEM;
    }

    /* Step 1: snapshot sorted prev state for each IDB relation */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->nrows == 0 || r->ncols == 0)
            continue;
        size_t sz = (size_t)r->nrows * r->ncols * sizeof(int64_t);
        prev_data[ri] = (int64_t *)malloc(sz);
        if (!prev_data[ri]) {
            for (uint32_t i = 0; i < ri; i++)
                free(prev_data[i]);
            free(prev_data);
            free(prev_nrows);
            free(prev_ncols);
            return ENOMEM;
        }
        memcpy(prev_data[ri], r->data, sz);
        prev_nrows[ri] = r->nrows;
        prev_ncols[ri] = r->ncols;
    }

    /* Step 2: evaluate stratum (appends new rows to IDB relations) */
    int rc = col_eval_stratum(sp, sess);
    if (rc != 0)
        goto cleanup;

    /* Steps 3-4: consolidate each IDB relation, fire callbacks for new rows */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->nrows == 0)
            continue;

        /* Consolidate: sort + dedup so binary search is valid */
        rc = col_idb_consolidate(r);
        if (rc != 0)
            goto cleanup;

        /* Fire delta_cb(+1) for rows not present in prev sorted state */
        uint32_t ncols = r->ncols;
        for (uint32_t row = 0; row < r->nrows; row++) {
            const int64_t *rowp = r->data + (size_t)row * ncols;
            if (!col_row_in_sorted(prev_data[ri], prev_nrows[ri], ncols,
                                   rowp)) {
                sess->delta_cb(r->name, rowp, ncols, +1, sess->delta_data);
            }
        }
    }

cleanup:
    for (uint32_t i = 0; i < rc_cnt; i++)
        free(prev_data[i]);
    free(prev_data);
    free(prev_nrows);
    free(prev_ncols);
    return rc;
}

/*
 * col_session_step: Advance the session by one evaluation epoch
 *
 * Implements wl_compute_backend_t.session_step vtable slot.
 *
 * Iterates all strata in plan order. For each stratum:
 *   - Fast path (no delta_cb): col_eval_stratum directly
 *   - Delta path: col_stratum_step_with_delta (snapshot + eval + set diff)
 * Arena is reset after each stratum to reclaim temporary evaluation data.
 *
 * TODO(Phase 2B): Replace set-diff delta with semi-naive ΔR propagation.
 *
 * @param session: wl_session_t* (cast to wl_col_session_t* internally)
 * @return 0 on success, non-zero on evaluation error
 */
static int
col_session_step(wl_session_t *session)
{
    wl_col_session_t *sess = COL_SESSION(session);
    const wl_plan_t *plan = sess->plan;

    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        int rc = sess->delta_cb ? col_stratum_step_with_delta(sp, sess)
                                : col_eval_stratum(sp, sess);
        if (rc != 0)
            return rc;
        /* Reset arena after stratum evaluation to free temporaries */
        if (sess->eval_arena)
            wl_arena_reset(sess->eval_arena);
    }
    return 0;
}

/*
 * col_session_set_delta_cb: Register a delta callback on this session
 *
 * Implements wl_compute_backend_t.session_set_delta_cb vtable slot.
 * The callback is invoked with diff=+1 for new tuples during col_session_step.
 *
 * TODO(Phase 2B): Also fire diff=-1 for retracted tuples when semi-naive
 * delta propagation tracks removed tuples explicitly.
 *
 * @param session:   wl_session_t* (cast to wl_col_session_t* internally)
 * @param callback:  Function invoked per output delta tuple (NULL to disable)
 * @param user_data: Opaque pointer passed through to callback
 */
static void
col_session_set_delta_cb(wl_session_t *session, wl_on_delta_fn callback,
                         void *user_data)
{
    if (!session)
        return;
    wl_col_session_t *sess = COL_SESSION(session);
    sess->delta_cb = callback;
    sess->delta_data = user_data;
}

/*
 * col_session_snapshot: Evaluate all strata and emit current IDB tuples
 *
 * Implements wl_compute_backend_t.session_snapshot vtable slot.
 *
 * Evaluation order:
 *   1. Execute all strata in plan order (col_eval_stratum per stratum)
 *   2. For each IDB relation in each stratum, invoke callback once per row
 *
 * Complexity: O(S * R * N) where S=strata, R=relations per stratum, N=rows
 *
 * TODO(Phase 2B): Snapshot should read from stable R (not recompute);
 * currently re-evaluates on every call which is O(input) per snapshot.
 *
 * @param session:   wl_session_t* (cast to wl_col_session_t* internally)
 * @param callback:  Invoked once per output tuple (relation, row, ncols)
 * @param user_data: Opaque pointer passed through to callback
 * @return 0 on success, EINVAL if session/callback NULL, non-zero on eval error
 */
static int
col_session_snapshot(wl_session_t *session, wl_on_tuple_fn callback,
                     void *user_data)
{
    if (!session || !callback)
        return EINVAL;

    wl_col_session_t *sess = COL_SESSION(session);
    const wl_plan_t *plan = sess->plan;

    /* Execute all strata in order */
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        int rc = col_eval_stratum(&plan->strata[si], sess);
        if (rc != 0)
            return rc;
    }

    /* Invoke callback for every tuple in every IDB relation */
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            const char *rname = sp->relations[ri].name;
            col_rel_t *r = session_find_rel(sess, rname);
            if (!r || r->nrows == 0)
                continue;
            for (uint32_t row = 0; row < r->nrows; row++) {
                callback(rname, r->data + (size_t)row * r->ncols, r->ncols,
                         user_data);
            }
        }
    }
    return 0;
}

/* ======================================================================== */
/* Vtable Singleton                                                          */
/* ======================================================================== */

static const wl_compute_backend_t col_backend = {
    .name = "columnar",
    .session_create = col_session_create,
    .session_destroy = col_session_destroy,
    .session_insert = col_session_insert,
    .session_remove = col_session_remove,
    .session_step = col_session_step,
    .session_set_delta_cb = col_session_set_delta_cb,
    .session_snapshot = col_session_snapshot,
};

const wl_compute_backend_t *
wl_backend_columnar(void)
{
    return &col_backend;
}
