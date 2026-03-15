/*
 * backend/columnar_nanoarrow.c - wirelog Nanoarrow Columnar Backend
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#define _GNU_SOURCE

#include "columnar_nanoarrow.h"
#include "delta_pool.h"
#include "memory.h"
#include "../session.h"
#include "../wirelog-internal.h"
#include "../workqueue.h"
#include "arena/arena.h"

#include "nanoarrow/nanoarrow.h"
#include <xxhash.h>

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#ifdef __SSE2__
#include <emmintrin.h>
#endif

#ifdef __ARM_NEON__
#include <arm_neon.h>
#endif

/* GCC/Clang extension not supported on MSVC */
#ifdef _MSC_VER
#define UNUSED
#include <windows.h> /* For GetTickCount64() in now_ns() */
#include <intrin.h>  /* For _BitScanForward64 */
/* MSVC: __builtin_ctzll not available; use _BitScanForward64 */
static inline int
ctzll(unsigned long long x)
{
    unsigned long index;
    if (_BitScanForward64(&index, x)) {
        return (int)index;
    }
    return 64; /* undefined for x=0, but we don't call it with x=0 */
}
#define __builtin_ctzll(x) ctzll(x)
#else
#define UNUSED __attribute__((unused))
#endif

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
    /* Sorted-prefix tracking for incremental consolidation (issue #94).
     * After consolidation, sorted_nrows == nrows (all rows sorted+unique).
     * Appending new rows leaves sorted_nrows unchanged: data[0..sorted_nrows)
     * is sorted, data[sorted_nrows..nrows) is an unsorted suffix.
     * Enables O(D log D + N) merge instead of O(N log N) full sort. */
    uint32_t sorted_nrows;
    /* Persistent merge buffer for consolidation (issue #94).
     * Reused across iterations to avoid per-consolidation malloc/free.
     * Grows via realloc as needed; freed only on relation destruction. */
    int64_t *merge_buf;
    uint32_t merge_buf_cap; /* capacity in rows */
    /* Base row count for delta propagation (issue #83).
     * After initial eval convergence, base_nrows == nrows. On incremental
     * insert, nrows grows but base_nrows stays fixed. Rows[base_nrows..nrows)
     * are the delta (new EDB facts not yet propagated). */
    uint32_t base_nrows;
    /* Timestamp tracking (optional): NULL when disabled.
     * When non-NULL, timestamps[i] records the provenance of data row i.
     * Capacity tracks with the data array (capacity entries allocated). */
    col_delta_timestamp_t *timestamps;
    /* Pool ownership flag (issue #123).
     * true  = struct was allocated from delta_pool; do not free() the struct.
     * false = struct was heap-allocated via calloc(); free() on destroy. */
    bool pool_owned;
} col_rel_t;

/**
 * col_materialized_join_t - Cached intermediate join result for CSE
 *
 * Stores the output of a multi-way join operation for reuse across
 * multiple semi-naive iterations. Used by Option 2 + Materialization
 * to avoid redundant computation of shared join prefixes.
 */
typedef struct {
    int64_t *data;         /* owned, row-major int64 buffer         */
    uint32_t nrows;        /* current row count                     */
    uint32_t ncols;        /* columns per tuple                     */
    uint32_t capacity;     /* allocated row capacity (for appending)*/
    uint32_t memory_limit; /* max bytes before eviction             */
    bool is_valid;         /* true if data is current for this iter */
} col_materialized_join_t;

/* ---- lifecycle ---------------------------------------------------------- */

static void
col_rel_free_contents(col_rel_t *r)
{
    if (!r)
        return;
    free(r->name);
    free(r->data);
    free(r->merge_buf);
    free(r->timestamps);
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
 * col_rel_destroy:
 * Free contents and, if heap-allocated (pool_owned == false), the struct
 * itself.  Pool-owned structs have their memory reclaimed on pool_reset();
 * calling free() on them would corrupt the pool allocator.
 */
static void
col_rel_destroy(col_rel_t *r)
{
    if (!r)
        return;
    bool from_pool = r->pool_owned;
    col_rel_free_contents(r); /* memset zeroes pool_owned */
    if (!from_pool)
        free(r);
    /* If pool_owned: struct memory freed on pool_reset(), skip free(). */
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
    r->pool_owned = false;
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
        /* Grow timestamps first (if tracking) so we can roll back cleanly
         * on a subsequent data realloc failure. */
        if (r->timestamps) {
            col_delta_timestamp_t *new_ts = (col_delta_timestamp_t *)realloc(
                r->timestamps, new_cap * sizeof(col_delta_timestamp_t));
            if (!new_ts)
                return ENOMEM;
            r->timestamps = new_ts;
        }
        int64_t *nd = (int64_t *)realloc(
            r->data, sizeof(int64_t) * (size_t)new_cap * r->ncols);
        if (!nd)
            return ENOMEM;
        r->data = nd;
        r->capacity = new_cap;
    }
    if (r->timestamps)
        memset(&r->timestamps[r->nrows], 0, sizeof(col_delta_timestamp_t));
    memcpy(r->data + (size_t)r->nrows * r->ncols, row,
           sizeof(int64_t) * r->ncols);
    r->nrows++;
    return 0;
}

/* Copy all rows from src into dst (must have same ncols).
 * If src has timestamps and dst has timestamp tracking enabled, the source
 * timestamps are propagated to the newly appended rows. */
static int
col_rel_append_all(col_rel_t *dst, const col_rel_t *src)
{
    uint32_t dst_base = dst->nrows;
    for (uint32_t i = 0; i < src->nrows; i++) {
        int rc = col_rel_append_row(dst, src->data + (size_t)i * src->ncols);
        if (rc != 0)
            return rc;
    }
    /* Overwrite the zero-initialized timestamps with src provenance. */
    if (src->timestamps && dst->timestamps)
        memcpy(&dst->timestamps[dst_base], src->timestamps,
               src->nrows * sizeof(col_delta_timestamp_t));
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
/* CSE Materialized Join Management (US-003)                               */
/* ======================================================================== */

/**
 * col_materialized_join_create - Allocate and initialize a materialized join.
 * Memory limit defaults to 10MB if not specified.
 */
static col_materialized_join_t *UNUSED
col_materialized_join_create(uint32_t ncols, uint32_t memory_limit)
{
    col_materialized_join_t *mj
        = (col_materialized_join_t *)calloc(1, sizeof(*mj));
    if (!mj)
        return NULL;

    mj->ncols = ncols;
    mj->memory_limit = (memory_limit > 0) ? memory_limit : (10 * 1024 * 1024);
    mj->capacity = 64; /* start small, grow on demand */
    mj->nrows = 0;
    mj->is_valid = false;

    if (ncols > 0) {
        mj->data = (int64_t *)malloc(sizeof(int64_t) * mj->capacity * ncols);
        if (!mj->data) {
            free(mj);
            return NULL;
        }
    }

    return mj;
}

/**
 * col_materialized_join_append - Add a row to materialized join.
 * Returns 0 on success, ENOMEM if materialized join would exceed memory limit.
 */
static int UNUSED
col_materialized_join_append(col_materialized_join_t *mj, const int64_t *row)
{
    if (!mj || !mj->data || mj->ncols == 0)
        return EINVAL;

    /* Check memory limit */
    uint32_t row_bytes = sizeof(int64_t) * mj->ncols;
    if ((size_t)mj->nrows * row_bytes + row_bytes > mj->memory_limit)
        return ENOMEM; /* exceeds memory limit */

    /* Grow capacity if needed */
    if (mj->nrows >= mj->capacity) {
        uint32_t new_cap = mj->capacity * 2;
        int64_t *new_data = (int64_t *)realloc(
            mj->data, sizeof(int64_t) * new_cap * mj->ncols);
        if (!new_data)
            return ENOMEM;
        mj->data = new_data;
        mj->capacity = new_cap;
    }

    memcpy(mj->data + (size_t)mj->nrows * mj->ncols, row, row_bytes);
    mj->nrows++;
    return 0;
}

/**
 * col_materialized_join_free - Free materialized join and release data.
 */
static void UNUSED
col_materialized_join_free(col_materialized_join_t *mj)
{
    if (!mj)
        return;
    free(mj->data);
    memset(mj, 0, sizeof(*mj));
    free(mj);
}

/**
 * col_materialized_join_invalidate - Mark as invalid (expires at end of iteration).
 */
static void UNUSED
col_materialized_join_invalidate(col_materialized_join_t *mj)
{
    if (mj)
        mj->is_valid = false;
}

/* ======================================================================== */
/* Materialization Cache (US-006)                                            */
/* ======================================================================== */

#define COL_MAT_CACHE_MAX 64u
#define COL_MAT_CACHE_LIMIT_BYTES (100ULL * 1024ULL * 1024ULL)

/**
 * col_mat_cache_key_content - Content-based hash of a col_rel_t.
 *
 * FNV-1a over first min(100, nrows) rows + shape (nrows, ncols).
 * Deterministic: same sorted data layout -> same hash.
 * Bounded cost: O(min(100, nrows)) independent of relation size.
 */
static uint64_t
col_mat_cache_key_content(const col_rel_t *rel)
{
    if (!rel || rel->nrows == 0)
        return 0;

    uint64_t hash = 14695981039346656037ULL; /* FNV-1a offset basis */
    uint32_t k = rel->nrows < 100 ? rel->nrows : 100;

    for (uint32_t i = 0; i < k; i++) {
        for (uint32_t j = 0; j < rel->ncols; j++) {
            uint64_t val = (uint64_t)rel->data[i * rel->ncols + j];
            hash ^= val;
            hash *= 1099511628211ULL; /* FNV-1a prime */
        }
    }

    /* Mix in shape to distinguish relations with identical prefixes */
    hash ^= (uint64_t)rel->nrows;
    hash *= 1099511628211ULL;
    hash ^= (uint64_t)rel->ncols;
    hash *= 1099511628211ULL;

    return hash;
}

/*
 * col_mat_entry_t: one entry in the materialization cache.
 * Keyed by content hashes of left and right input relations.
 * The cache owns result; freed when the entry is evicted or cache cleared.
 */
typedef struct {
    uint64_t left_hash;  /* content hash of left input relation  */
    uint64_t right_hash; /* content hash of right input relation */
    col_rel_t *result;   /* owned cached join result             */
    size_t mem_bytes;    /* bytes used by result->data           */
    uint64_t lru_clock;  /* logical time of last access          */
} col_mat_entry_t;

typedef struct {
    col_mat_entry_t entries[COL_MAT_CACHE_MAX];
    uint32_t count;
    size_t total_bytes;
    uint64_t clock;
    uint64_t hits;   /* cache hit counter  */
    uint64_t misses; /* cache miss counter */
} col_mat_cache_t;

static void
col_mat_cache_clear(col_mat_cache_t *cache)
{
    for (uint32_t i = 0; i < cache->count; i++)
        col_rel_destroy(cache->entries[i].result);
    cache->count = 0;
    cache->total_bytes = 0;
}

static col_rel_t *
col_mat_cache_lookup(col_mat_cache_t *cache, const col_rel_t *left,
                     const col_rel_t *right)
{
    uint64_t lh = col_mat_cache_key_content(left);
    uint64_t rh = col_mat_cache_key_content(right);
    for (uint32_t i = 0; i < cache->count; i++) {
        if (cache->entries[i].left_hash == lh
            && cache->entries[i].right_hash == rh) {
            cache->entries[i].lru_clock = ++cache->clock;
            cache->hits++;
            return cache->entries[i].result;
        }
    }
    cache->misses++;
    return NULL;
}

static void
col_mat_cache_insert(col_mat_cache_t *cache, const col_rel_t *left,
                     const col_rel_t *right, col_rel_t *result)
{
    size_t result_bytes
        = (result->nrows > 0 && result->ncols > 0)
              ? (size_t)result->nrows * result->ncols * sizeof(int64_t)
              : 0;

    /* Evict LRU entries until within memory limit */
    while (cache->count > 0
           && cache->total_bytes + result_bytes > COL_MAT_CACHE_LIMIT_BYTES) {
        uint32_t lru = 0;
        for (uint32_t i = 1; i < cache->count; i++) {
            if (cache->entries[i].lru_clock < cache->entries[lru].lru_clock)
                lru = i;
        }
        cache->total_bytes -= cache->entries[lru].mem_bytes;
        col_rel_destroy(cache->entries[lru].result);
        memmove(&cache->entries[lru], &cache->entries[lru + 1],
                (cache->count - lru - 1) * sizeof(col_mat_entry_t));
        cache->count--;
    }

    /* Evict oldest entry if array is full */
    if (cache->count >= COL_MAT_CACHE_MAX) {
        uint32_t lru = 0;
        for (uint32_t i = 1; i < cache->count; i++) {
            if (cache->entries[i].lru_clock < cache->entries[lru].lru_clock)
                lru = i;
        }
        cache->total_bytes -= cache->entries[lru].mem_bytes;
        col_rel_destroy(cache->entries[lru].result);
        memmove(&cache->entries[lru], &cache->entries[lru + 1],
                (cache->count - lru - 1) * sizeof(col_mat_entry_t));
        cache->count--;
    }

    col_mat_entry_t *e = &cache->entries[cache->count++];
    e->left_hash = col_mat_cache_key_content(left);
    e->right_hash = col_mat_cache_key_content(right);
    e->result = result;
    e->mem_bytes = result_bytes;
    e->lru_clock = ++cache->clock;
    cache->total_bytes += result_bytes;
}

/* ======================================================================== */
/* Arrangement Layer (Phase 3C)                                             */
/* ======================================================================== */

/*
 * arr_hash_key: FNV-1a hash over key columns of a single row.
 * nbuckets MUST be a power of 2.
 */
static uint32_t
arr_hash_key(const int64_t *row, const uint32_t *key_cols, uint32_t key_count,
             uint32_t nbuckets)
{
    uint64_t h = 14695981039346656037ULL; /* FNV-1a basis */
    for (uint32_t k = 0; k < key_count; k++) {
        uint64_t v = (uint64_t)row[key_cols[k]];
        for (int b = 0; b < 8; b++) {
            h ^= v & 0xFFu;
            h *= 1099511628211ULL;
            v >>= 8;
        }
    }
    return (uint32_t)(h & (uint64_t)(nbuckets - 1));
}

/* Round n up to the next power of 2; minimum 16. */
static uint32_t
arr_next_pow2(uint32_t n)
{
    if (n < 16u)
        return 16u;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    return n + 1u;
}

/*
 * arr_free_contents: free hash table arrays only.
 * key_cols is NOT freed here — it is always owned by the registry entry
 * (col_arr_entry_t.key_cols) and freed separately in col_session_destroy.
 */
static void
arr_free_contents(col_arrangement_t *arr)
{
    if (!arr)
        return;
    free(arr->ht_head);
    free(arr->ht_next);
    arr->ht_head = NULL;
    arr->ht_next = NULL;
    arr->nbuckets = 0;
    arr->ht_cap = 0;
    arr->indexed_rows = 0;
}

/* Full rebuild: index all nrows rows in rel into arr. */
static int
arr_build_full(col_arrangement_t *arr, const col_rel_t *rel)
{
    uint32_t nrows = rel->nrows;
    uint32_t nbuckets = arr_next_pow2(nrows > 0 ? nrows * 2u : 16u);

    /* Reallocate bucket heads if size changed. */
    if (nbuckets != arr->nbuckets) {
        uint32_t *head = (uint32_t *)malloc(nbuckets * sizeof(uint32_t));
        if (!head)
            return ENOMEM;
        free(arr->ht_head);
        arr->ht_head = head;
        arr->nbuckets = nbuckets;
    }
    memset(arr->ht_head, 0xFF, nbuckets * sizeof(uint32_t)); /* UINT32_MAX */

    /* Grow chain array if needed. */
    if (nrows > arr->ht_cap) {
        uint32_t new_cap = nrows > arr->ht_cap * 2u ? nrows : arr->ht_cap * 2u;
        if (new_cap < 16u)
            new_cap = 16u;
        uint32_t *nxt = (uint32_t *)malloc(new_cap * sizeof(uint32_t));
        if (!nxt)
            return ENOMEM;
        free(arr->ht_next);
        arr->ht_next = nxt;
        arr->ht_cap = new_cap;
    }

    uint32_t nc = rel->ncols;
    for (uint32_t row = 0; row < nrows; row++) {
        const int64_t *rp = rel->data + (size_t)row * nc;
        uint32_t bucket
            = arr_hash_key(rp, arr->key_cols, arr->key_count, nbuckets);
        arr->ht_next[row] = arr->ht_head[bucket];
        arr->ht_head[bucket] = row;
    }
    arr->indexed_rows = nrows;
    return 0;
}

/* Incremental update: index only rows [old_nrows..rel->nrows). */
static int
arr_update_incremental(col_arrangement_t *arr, const col_rel_t *rel,
                       uint32_t old_nrows)
{
    uint32_t nrows = rel->nrows;
    if (old_nrows >= nrows)
        return 0;

    /* If load factor would exceed 50%, full rebuild needed. */
    uint32_t needed = arr_next_pow2(nrows * 2u);
    if (needed != arr->nbuckets)
        return arr_build_full(arr, rel);

    /* Grow chain array if needed. */
    if (nrows > arr->ht_cap) {
        uint32_t new_cap = nrows * 2u < 16u ? 16u : nrows * 2u;
        uint32_t *nxt
            = (uint32_t *)realloc(arr->ht_next, new_cap * sizeof(uint32_t));
        if (!nxt)
            return ENOMEM;
        arr->ht_next = nxt;
        arr->ht_cap = new_cap;
    }

    uint32_t nc = rel->ncols;
    uint32_t nb = arr->nbuckets;
    for (uint32_t row = old_nrows; row < nrows; row++) {
        const int64_t *rp = rel->data + (size_t)row * nc;
        uint32_t bucket = arr_hash_key(rp, arr->key_cols, arr->key_count, nb);
        arr->ht_next[row] = arr->ht_head[bucket];
        arr->ht_head[bucket] = row;
    }
    arr->indexed_rows = nrows;
    return 0;
}

/*
 * col_arr_entry_t: one (rel_name, key_cols) → arrangement mapping.
 * Stored in the session's flat arrangement registry.
 */
typedef struct {
    char *rel_name;     /* owned copy of relation name    */
    uint32_t *key_cols; /* owned copy of key column array */
    uint32_t key_count;
    col_arrangement_t arr; /* embedded arrangement           */
} col_arr_entry_t;

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
/*
 * Phase 4: Per-Stratum Frontier Array Limit
 * Max strata supported for incremental evaluation. Most Datalog programs have
 * 2-10 strata; 32 is a practical upper bound. Increase if needed.
 */
#define MAX_STRATA 32

/*
 * Phase 4 (US-4-002): Per-Rule Frontier Array Limit
 * Max rules supported for rule-level incremental evaluation. Matches the
 * uint64_t bitmask width used for rule dependency tracking.
 */
#define MAX_RULES 64

typedef struct {
    wl_session_t base;         /* MUST be first field (vtable dispatch)  */
    const wl_plan_t *plan;     /* borrowed, lifetime: caller             */
    col_rel_t **rels;          /* owned array of owned col_rel_t*        */
    uint32_t nrels;            /* current number of registered relations */
    uint32_t rel_cap;          /* allocated capacity of rels[]           */
    wl_on_delta_fn delta_cb;   /* delta callback (NULL = disabled)       */
    void *delta_data;          /* opaque user context for delta_cb       */
    wl_arena_t *eval_arena;    /* arena for per-iteration temporaries    */
    col_mat_cache_t mat_cache; /* materialization cache (US-006)        */
    uint32_t total_iterations; /* fixed-point iterations in last eval   */
    wl_work_queue_t *wq;       /* reusable thread pool for K-fusion     */
    /* Profiling counters (3B-003): accumulated across all strata/iters  */
    uint64_t
        consolidation_ns; /* time in col_op_consolidate_incremental_delta */
    uint64_t kfusion_ns;  /* time in col_op_k_fusion                */
    /* Arrangement registry (Phase 3C): persistent hash indices           */
    col_arr_entry_t *arr_entries; /* owned flat array of arrangements     */
    uint32_t arr_count;           /* number of active arrangements        */
    uint32_t arr_cap;             /* allocated capacity                   */
    /* Delta arrangement cache (Phase 3C-001-Ext): per-iteration hash
     * indices for delta-substituted right relations.  Unlike arr_entries
     * (cross-iteration, session-global), these are:
     *   - Per-worker: zeroed in each K-fusion worker's session copy
     *   - Per-iteration: freed at start of each col_eval_stratum iter
     * This avoids races: workers write to their own copy, never shared. */
    col_arr_entry_t *darr_entries; /* owned flat array of delta arrs      */
    uint32_t darr_count;           /* number of active delta arrangements */
    uint32_t darr_cap;             /* allocated capacity                  */
    /* 2D Frontier Epoch Tracking (Issue #103): Incremental insertion epoch counter.
     * Incremented before each EDB insertion and stratum evaluation to distinguish
     * between different insertion epochs. Used with frontiers[] to track
     * (outer_epoch, iteration) pairs: an iteration is skipped only if it was
     * already processed in the SAME outer_epoch. This prevents incorrect skipping
     * when comparing across different insertion epochs. */
    uint32_t outer_epoch;
    /* Frontier tracking (Phase 4 / Issue #104): per-stratum 2D frontier array
     * for epoch-aware incremental re-evaluation. Each frontiers[i] tracks the
     * (outer_epoch, iteration) pair at which stratum i last converged.
     * An iteration is skipped only when it was already processed in the SAME
     * outer_epoch, preventing incorrect skipping across insertion epochs.
     * This enables skipping redundant iterations when frontier persists across
     * session_step calls (incremental evaluation). */
    col_frontier_2d_t
        frontiers[MAX_STRATA]; /* per-stratum 2D frontier tracking */
    /* Frontier tracking (Phase 4, US-4-002): per-rule frontier array for
     * rule-level incremental re-evaluation. Each rule_frontiers[i] tracks the
     * minimum (iteration, stratum) boundary fully processed for rule i.
     * Initialized to (0, 0) via calloc. Reset to UINT32_MAX for affected
     * rules on incremental insert (enables selective rule skipping in
     * US-4-004). MAX_RULES=64 matches uint64_t bitmask width. */
    col_frontier_2d_t
        rule_frontiers[MAX_RULES]; /* per-rule frontier tracking with
                                                     (outer_epoch, iteration) pairs */
    /* Per-phase K-fusion profiling (Phase 3A): breakdown of kfusion_ns into
     * four sub-phases accumulated across all col_op_k_fusion calls per eval.
     *   alloc_ns:    calloc of results/workers/worker_sess arrays
     *   dispatch_ns: submit loop + wl_workqueue_wait_all barrier
     *   merge_ns:    col_rel_merge_k + eval_stack_push
     *   cleanup_ns:  result/worker mat_cache/arr/darr free loops + free() */
    uint64_t kfusion_alloc_ns;
    uint64_t kfusion_dispatch_ns;
    uint64_t kfusion_merge_ns;
    uint64_t kfusion_cleanup_ns;
    /* Phase 4: tracks which relation was just inserted via
     * col_session_insert_incremental, enables affected-stratum skip
     * optimization. Borrowed pointer; lifetime: until next session_step.
     * NULL when no incremental insert preceded the current step (all strata
     * evaluated normally via affected_mask = UINT64_MAX). */
    const char *last_inserted_relation;
    /* Current fixed-point iteration counter within col_eval_stratum.
     * Set at the start of each iteration; operators use this to distinguish
     * iteration 0 (base case: FORCE_DELTA falls back to full relation) from
     * iteration > 0 (FORCE_DELTA with absent delta → empty short-circuit).
     * Only valid during col_eval_stratum execution. */
    uint32_t current_iteration;
    /* Delta-seeded incremental evaluation (issue #83).
     * When true, EDB delta relations have been pre-seeded into the session
     * before re-evaluation. FORCE_DELTA at iteration 0 pushes empty (not full)
     * for relations without a pre-seeded delta, enabling delta-only propagation
     * instead of full re-derivation. Cleared after eval completes. */
    bool delta_seeded;
    /* Multi-worker support (issue #99).
     * Stored from col_session_create num_workers parameter.
     * When > 1, sess->wq is created at session init for parallel K-fusion.
     * When == 1, K-fusion evaluates copies sequentially (no thread overhead). */
    uint32_t num_workers;
    /* Monotone property tracking (issue #105).
     * stratum_is_monotone[si] = true if stratum si only derives facts
     * (no deletion via negation/antijoin/subtraction). Used for DRedL-style
     * deletion phase skip optimization. Populated from plan->strata[si].is_monotone
     * during session_create. Conservative default: all false (no optimization). */
    bool stratum_is_monotone[MAX_STRATA];
    /* Retraction delta tracking (Issue #158): tracks which relation was just
     * removed via col_session_remove_incremental, enables delta retraction path.
     * Borrowed pointer; lifetime: until next session_step.
     * NULL when no incremental remove preceded the current step. */
    const char *last_removed_relation;
    /* Retraction-seeded incremental evaluation (Issue #158).
     * When true, EDB retraction delta relations have been pre-seeded into
     * the session before re-evaluation. Similar to delta_seeded but for
     * $r$<name> relations. Cleared after eval completes. */
    bool retraction_seeded;
    delta_pool_t *delta_pool; /* Pool allocator for operator temporaries */
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
    /* Pool-owned structs must be promoted to heap before storing in the
     * session, because col_session_destroy calls free() on each entry. */
    if (r->pool_owned) {
        col_rel_t *heap = (col_rel_t *)calloc(1, sizeof(col_rel_t));
        if (!heap)
            return ENOMEM;
        *heap = *r;
        heap->pool_owned = false;
        /* Zero out source slot so pool_reset doesn't double-free contents */
        memset(r, 0, sizeof(*r));
        r = heap;
    }
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
            col_rel_destroy(sess->rels[i]);
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
 * col_eval_expr_run:
 * Core postfix expression evaluator. Runs the bytecode against a row and
 * stores the top-of-stack value in *out_val.
 * Returns 0 on success, non-zero on malformed bytecode.
 */
static int
col_eval_expr_run(const uint8_t *buf, uint32_t size, const int64_t *row,
                  uint32_t ncols, int64_t *out_val)
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
        case WL_PLAN_EXPR_ARITH_BAND: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a & b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BOR: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a | b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BXOR: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a ^ b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BNOT: {
            int64_t a = filt_pop(&s);
            filt_push(&s, ~a);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SHL: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a << b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SHR: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a >> b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_HASH: {
            int64_t a = filt_pop(&s);
            filt_push(&s, (int64_t)XXH3_64bits(&a, sizeof(a)));
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

        /* Aggregates: not valid in row-level evaluation, skip */
        case WL_PLAN_EXPR_AGG_COUNT:
        case WL_PLAN_EXPR_AGG_SUM:
        case WL_PLAN_EXPR_AGG_MIN:
        case WL_PLAN_EXPR_AGG_MAX:
            break;

        default:
            goto bad;
        }
    }
    *out_val = s.top > 0 ? s.vals[s.top - 1] : 0;
    return 0;

bad:
    *out_val = 0;
    return 1;
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
    int64_t val;
    int err = col_eval_expr_run(buf, size, row, ncols, &val);
    return err ? 1 : (val != 0 ? 1 : 0); /* on error: pass row through */
}

/*
 * col_eval_expr_i64:
 * Evaluate the postfix expression buffer and return the computed int64 value.
 * Used by MAP operations to compute head argument expressions.
 * Returns 0 on empty expression or evaluation error.
 */
static int64_t
col_eval_expr_i64(const uint8_t *buf, uint32_t size, const int64_t *row,
                  uint32_t ncols)
{
    int64_t val;
    col_eval_expr_run(buf, size, row, ncols, &val);
    return val;
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
    uint32_t
        *seg_boundaries; /* Array of K+1 boundary row indices (K-way merge) */
    uint32_t seg_count;  /* Number of segments (0 = no segmentation) */
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
    s->items[s->top].seg_boundaries = NULL;
    s->items[s->top].seg_count = 0;
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
    eval_entry_t e = { NULL, false, false, NULL, 0 };
    if (s->top > 0)
        e = s->items[--s->top];
    return e;
}

static void
eval_stack_drain(eval_stack_t *s)
{
    while (s->top > 0) {
        eval_entry_t e = eval_stack_pop(s);
        if (e.seg_boundaries)
            free(e.seg_boundaries);
        if (e.owned)
            col_rel_destroy(e.rel);
    }
}

/* ======================================================================== */
/* Operator Implementations                                                  */
/* ======================================================================== */

/* Forward declarations for recursive evaluation */
static int
col_eval_relation_plan(const wl_plan_relation_t *rplan, eval_stack_t *stack,
                       wl_col_session_t *sess);

/* Forward declaration for empty-delta pre-scan skip (issue #85) */
static bool
has_empty_forced_delta(const wl_plan_relation_t *rp, wl_col_session_t *sess,
                       uint32_t iteration);

/* Forward declarations for delta arrangement cache (Phase 3C-001-Ext) */
static col_arrangement_t *
col_session_get_delta_arrangement(wl_col_session_t *cs, const char *rel_name,
                                  const col_rel_t *delta_rel,
                                  const uint32_t *key_cols, uint32_t key_count);
static void
col_session_free_delta_arrangements(wl_col_session_t *cs);

/* Forward declaration for Phase 4 affected-strata skip */
uint64_t
col_compute_affected_strata(wl_session_t *session,
                            const char *inserted_relation);

/* Helper: create a new owned relation with given ncols and auto-named cols. */
static col_rel_t *
col_rel_new_auto(const char *name, uint32_t ncols)
{
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, name) != 0)
        return NULL;
    if (col_rel_set_schema(r, ncols, NULL) != 0) {
        col_rel_destroy(r);
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
        col_rel_destroy(r);
        return NULL;
    }
    return r;
}

/* Pool-aware col_rel constructor wrappers.
 *
 * The struct slot is allocated from the pool slab (O(1), no free needed).
 * Data buffers and col_names are still heap-allocated so that realloc in
 * col_rel_append_row remains safe and col_names are available for column
 * lookup (col_rel_col_idx).  The pool_owned flag tells col_rel_destroy to
 * skip free() on the struct itself while still freeing heap-allocated
 * internals. */
static col_rel_t *
col_rel_pool_new_like(delta_pool_t *pool, const char *name,
                      const col_rel_t *like)
{
    if (!pool)
        return col_rel_new_like(name, like); /* Fallback to malloc */
    col_rel_t *r = (col_rel_t *)delta_pool_alloc_slot(pool);
    if (!r)
        return col_rel_new_like(name, like); /* Pool exhausted, fallback */
    r->pool_owned = true;
    r->name = wl_strdup(name);
    if (!r->name) {
        memset(r, 0, sizeof(*r));
        return col_rel_new_like(name, like);
    }
    r->ncols = like->ncols;
    r->capacity = COL_REL_INIT_CAP;
    if (like->ncols > 0) {
        r->data
            = (int64_t *)malloc(sizeof(int64_t) * r->capacity * like->ncols);
        if (!r->data) {
            free(r->name);
            memset(r, 0, sizeof(*r));
            return col_rel_new_like(name, like); /* Fallback */
        }
    }
    /* Copy col_names so col_rel_col_idx works for downstream operators */
    if (like->col_names && like->ncols > 0) {
        r->col_names = (char **)calloc(like->ncols, sizeof(char *));
        if (r->col_names) {
            for (uint32_t i = 0; i < like->ncols; i++) {
                if (like->col_names[i])
                    r->col_names[i] = wl_strdup(like->col_names[i]);
            }
        }
    }
    r->nrows = 0;
    return r;
}

static col_rel_t *
col_rel_pool_new_auto(delta_pool_t *pool, const char *name, uint32_t ncols)
{
    if (!pool)
        return col_rel_new_auto(name, ncols); /* Fallback */
    col_rel_t *r = (col_rel_t *)delta_pool_alloc_slot(pool);
    if (!r)
        return col_rel_new_auto(name, ncols); /* Pool exhausted */
    r->pool_owned = true;
    r->name = wl_strdup(name);
    if (!r->name) {
        memset(r, 0, sizeof(*r));
        return col_rel_new_auto(name, ncols);
    }
    r->ncols = ncols;
    r->capacity = COL_REL_INIT_CAP;
    if (ncols > 0) {
        r->data = (int64_t *)malloc(sizeof(int64_t) * r->capacity * ncols);
        if (!r->data) {
            free(r->name);
            memset(r, 0, sizeof(*r));
            return col_rel_new_auto(name, ncols); /* Fallback */
        }
        /* Auto-generate col_names (col0, col1, ...) matching set_schema */
        r->col_names = (char **)calloc(ncols, sizeof(char *));
        if (r->col_names) {
            for (uint32_t i = 0; i < ncols; i++) {
                char buf[32];
                snprintf(buf, sizeof(buf), "col%u", i);
                r->col_names[i] = wl_strdup(buf);
            }
        }
    }
    r->nrows = 0;
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

    /* Delta mode controls whether we use delta or full relation.
     * FORCE_FULL:  always use the full relation (no delta substitution).
     * FORCE_DELTA: always use the delta relation; if no delta exists or
     *              it is empty, push an empty relation so that the rule
     *              copy produces no output (correct semi-naive behavior).
     * AUTO:        heuristic -- prefer delta only when it is a genuine
     *              strict subset of the full relation (nrows < full). */
    char dname[256];
    snprintf(dname, sizeof(dname), "$d$%s", op->relation_name);
    col_rel_t *delta = session_find_rel(sess, dname);

    if (op->delta_mode == WL_DELTA_FORCE_FULL) {
        return eval_stack_push_delta(stack, full_rel, false, false);
    }
    if (op->delta_mode == WL_DELTA_FORCE_DELTA) {
        if (delta && delta->nrows > 0) {
            return eval_stack_push_delta(stack, delta, false, true);
        }
        if (sess->current_iteration == 0) {
            if (sess->delta_seeded) {
                /* Issue #83: delta-seeded incremental re-eval. No pre-seeded
                 * delta means this relation has no new facts. Push empty so
                 * only rules with actual deltas produce output. */
                col_rel_t *empty = col_rel_pool_new_like(
                    sess->delta_pool, "$empty_delta", full_rel);
                if (!empty)
                    return ENOMEM;
                int push_rc = eval_stack_push_delta(stack, empty, true, true);
                if (push_rc != 0)
                    col_rel_destroy(empty);
                return push_rc;
            }
            /* Base-case iteration: no deltas exist yet, fall back to full
             * relation so EDB-grounded rules can still fire on iter 0. */
            return eval_stack_push_delta(stack, full_rel, false, false);
        }
        /* Iteration > 0: delta absent or empty means the relation has
         * converged.  Push an empty relation so this rule copy produces
         * no output (correct semi-naive semantics, issue #85). */
        col_rel_t *empty
            = col_rel_pool_new_like(sess->delta_pool, "$empty_delta", full_rel);
        if (!empty)
            return ENOMEM;
        int push_rc = eval_stack_push_delta(stack, empty, true, true);
        if (push_rc != 0)
            col_rel_destroy(empty);
        return push_rc;
    }

    /* WL_DELTA_AUTO: original heuristic */
    bool use_delta
        = (delta && delta->nrows > 0 && delta->nrows < full_rel->nrows);
    col_rel_t *rel = use_delta ? delta : full_rel;
    /* push borrowed reference - session owns the relation */
    return eval_stack_push_delta(stack, rel, false, use_delta);
}

/* --- MAP ----------------------------------------------------------------- */

static int
col_op_map(const wl_plan_op_t *op, eval_stack_t *stack, wl_col_session_t *sess)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    uint32_t pc = op->project_count;
    col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, "$map", pc);
    if (!out) {
        if (e.owned)
            col_rel_destroy(e.rel);
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * pc);
    if (!tmp) {
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(e.rel);
        return ENOMEM;
    }

    for (uint32_t r = 0; r < e.rel->nrows; r++) {
        const int64_t *row = e.rel->data + (size_t)r * e.rel->ncols;
        for (uint32_t c = 0; c < pc; c++) {
            if (op->map_exprs && c < op->map_expr_count && op->map_exprs[c].data
                && op->map_exprs[c].size > 0) {
                tmp[c] = col_eval_expr_i64(op->map_exprs[c].data,
                                           op->map_exprs[c].size, row,
                                           e.rel->ncols);
            } else {
                uint32_t src = op->project_indices ? op->project_indices[c] : c;
                tmp[c] = (src < e.rel->ncols) ? row[src] : 0;
            }
        }
        int rc = col_rel_append_row(out, tmp);
        if (rc != 0) {
            free(tmp);
            col_rel_destroy(out);
            if (e.owned)
                col_rel_destroy(e.rel);
            return rc;
        }
    }
    free(tmp);

    if (e.owned)
        col_rel_destroy(e.rel);
    return eval_stack_push(stack, out, true);
}

/* --- FILTER -------------------------------------------------------------- */

static int
col_op_filter(const wl_plan_op_t *op, eval_stack_t *stack,
              wl_col_session_t *sess)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    col_rel_t *out = col_rel_pool_new_like(sess->delta_pool, "$filter", e.rel);
    if (!out) {
        if (e.owned)
            col_rel_destroy(e.rel);
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
                col_rel_destroy(out);
                if (e.owned)
                    col_rel_destroy(e.rel);
                return rc;
            }
        }
    }

    if (e.owned)
        col_rel_destroy(e.rel);
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
        if (left_e.owned)
            col_rel_destroy(left_e.rel);
        return ENOENT;
    }

    /* Right-side delta substitution controlled by delta_mode:
     * FORCE_DELTA: always substitute delta of right if available; if no
     *              delta exists, short-circuit with an empty result (this
     *              rule copy produces no tuples from this permutation).
     * FORCE_FULL:  never substitute delta; always use full right.
     * AUTO:        heuristic -- substitute delta when left is not already
     *              a delta and right-delta is strictly smaller than full. */
    bool used_right_delta = false;
    if (op->delta_mode == WL_DELTA_FORCE_DELTA && op->right_relation) {
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        if (rdelta && rdelta->nrows > 0) {
            right = rdelta;
            used_right_delta = true;
        } else if (sess->current_iteration > 0 || sess->delta_seeded) {
            /* Iteration > 0 or delta-seeded iter 0 (issue #83):
             * FORCE_DELTA required but delta absent/empty. Short-circuit to
             * empty result — this rule copy produces no tuples from this
             * permutation (correct semi-naive, issue #85). */
            uint32_t ocols = left_e.rel->ncols + right->ncols;
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            col_rel_t *empty = col_rel_new_auto("$join_empty", ocols);
            if (!empty)
                return ENOMEM;
            int push_rc = eval_stack_push(stack, empty, true);
            if (push_rc != 0)
                col_rel_destroy(empty);
            return push_rc;
        }
        /* else: iteration 0 — no deltas yet, fall through to full right */
    } else if (op->delta_mode != WL_DELTA_FORCE_FULL && !left_e.is_delta
               && op->right_relation) {
        /* AUTO: original heuristic */
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        if (rdelta && rdelta->nrows > 0 && rdelta->nrows < right->nrows) {
            right = rdelta;
            used_right_delta = true;
        }
    }

    /* Materialization cache: reuse previous join result when available.
     * Works with both stable (borrowed) and worker-owned relations since
     * the cache key is based on content hash, not ownership. This enables
     * cache reuse in K-fusion worker sessions, eliminating redundant joins. */
    if (op->materialized) {
        col_rel_t *cached
            = col_mat_cache_lookup(&sess->mat_cache, left_e.rel, right);
        if (cached) {
            return eval_stack_push_delta(stack, cached, false,
                                         left_e.is_delta || used_right_delta);
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
        if (left_e.owned)
            col_rel_destroy(left);
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
    col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, "$join", ocols);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }

    /* Hash join: use persistent arrangement for the full right relation;
     * fall back to an ephemeral hash table for delta substitution or when
     * the arrangement cannot be allocated. */
    col_arrangement_t *arr = NULL;
    uint32_t nbuckets_ep = 0;
    uint32_t *ht_head_ep = NULL;
    uint32_t *ht_next_ep = NULL;

    if (!used_right_delta && op->right_relation && kc > 0)
        arr = col_session_get_arrangement(&sess->base, op->right_relation, rk,
                                          kc);
    else if (used_right_delta && op->right_relation && kc > 0)
        arr = col_session_get_delta_arrangement(sess, op->right_relation, right,
                                                rk, kc);

    if (!arr) {
        /* Ephemeral hash table (delta path or arrangement unavailable). */
        nbuckets_ep = next_pow2(right->nrows > 0 ? right->nrows * 2 : 1);
        ht_head_ep = (uint32_t *)calloc(nbuckets_ep, sizeof(uint32_t));
        ht_next_ep = (uint32_t *)malloc((right->nrows > 0 ? right->nrows : 1)
                                        * sizeof(uint32_t));
        if (!ht_head_ep || !ht_next_ep) {
            free(ht_head_ep);
            free(ht_next_ep);
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (left_e.owned)
                col_rel_destroy(left);
            return ENOMEM;
        }
        for (uint32_t rr = 0; rr < right->nrows; rr++) {
            const int64_t *rrow = right->data + (size_t)rr * right->ncols;
            uint32_t h = hash_int64_keys(rrow, rk, kc) & (nbuckets_ep - 1);
            ht_next_ep[rr] = ht_head_ep[h];
            ht_head_ep[h] = rr + 1; /* 1-based; 0 = end of chain */
        }
    }

    /* key_row scratch buffer for arrangement probe: values placed at rk[]
     * positions so col_arrangement_find_first() matches correctly. */
    int64_t *key_row = NULL;
    if (arr) {
        key_row = (int64_t *)malloc(sizeof(int64_t)
                                    * (right->ncols > 0 ? right->ncols : 1));
        if (!key_row) {
            free(ht_head_ep);
            free(ht_next_ep);
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (left_e.owned)
                col_rel_destroy(left);
            return ENOMEM;
        }
    }

    int join_rc = 0;
    for (uint32_t lr = 0; lr < left->nrows && join_rc == 0; lr++) {
        const int64_t *lrow = left->data + (size_t)lr * left->ncols;

        if (arr) {
            /* Arrangement probe: fill key_row at right-side positions. */
            for (uint32_t k = 0; k < kc; k++)
                key_row[rk[k]] = lrow[lk[k]];
            uint32_t rr = col_arrangement_find_first(arr, right->data,
                                                     right->ncols, key_row);
            while (rr != UINT32_MAX && join_rc == 0) {
                const int64_t *rrow = right->data + (size_t)rr * right->ncols;
                /* Verify key match: find_next may return collision rows. */
                bool match = true;
                for (uint32_t k = 0; k < kc && match; k++)
                    match = (lrow[lk[k]] == rrow[rk[k]]);
                if (match) {
                    memcpy(tmp, lrow, sizeof(int64_t) * left->ncols);
                    memcpy(tmp + left->ncols, rrow,
                           sizeof(int64_t) * right->ncols);
                    join_rc = col_rel_append_row(out, tmp);
                }
                rr = col_arrangement_find_next(arr, rr);
            }
        } else {
            /* Ephemeral hash probe. */
            uint32_t h = hash_int64_keys(lrow, lk, kc) & (nbuckets_ep - 1);
            for (uint32_t e = ht_head_ep[h]; e != 0; e = ht_next_ep[e - 1]) {
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
    }
    free(key_row);
    free(ht_head_ep);
    free(ht_next_ep);
    if (join_rc != 0) {
        free(tmp);
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return join_rc;
    }

    free(tmp);
    free(lk);
    free(rk);
    if (left_e.owned)
        col_rel_destroy(left);
    /* Propagate delta flag: result is a delta if left was delta OR we used
     * right-delta. This ensures subsequent JOINs in the same rule plan know
     * whether to apply right-delta (they should NOT if we already used one). */
    bool result_is_delta = left_e.is_delta || used_right_delta;

    /* Populate materialization cache when hint is set.
     * Works with both stable and worker-owned relations.
     * Cache takes ownership of out; we push a borrowed reference.
     * This enables K-fusion workers to cache and reuse intermediate joins,
     * reducing redundant computation across the K worker copies. */
    if (op->materialized) {
        col_mat_cache_insert(&sess->mat_cache, left, right, out);
        return eval_stack_push_delta(stack, out, false, result_is_delta);
    }
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
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    for (uint32_t k = 0; k < kc; k++) {
        int li = col_rel_col_idx(left, op->left_keys ? op->left_keys[k] : NULL);
        int ri
            = col_rel_col_idx(right, op->right_keys ? op->right_keys[k] : NULL);
        lk[k] = (li >= 0) ? (uint32_t)li : 0;
        rk[k] = (ri >= 0) ? (uint32_t)ri : 0;
    }

    col_rel_t *out = col_rel_pool_new_like(sess->delta_pool, "$antijoin", left);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
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
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
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
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return aj_rc;
    }

    free(lk);
    free(rk);
    if (left_e.owned)
        col_rel_destroy(left);
    return eval_stack_push(stack, out, true);
}

/* --- CONCAT -------------------------------------------------------------- */

static int
col_op_concat(eval_stack_t *stack, wl_col_session_t *sess)
{
    if (stack->top < 2)
        return 0; /* single-item passthrough for K-copy boundary marker */

    eval_entry_t b_e = eval_stack_pop(stack);
    eval_entry_t a_e = eval_stack_pop(stack);
    col_rel_t *a = a_e.rel;
    col_rel_t *b = b_e.rel;

    if (!a || !b || a->ncols != b->ncols) {
        if (a_e.owned)
            col_rel_destroy(a);
        if (b_e.owned)
            col_rel_destroy(b);
        return EINVAL;
    }

    col_rel_t *out = col_rel_pool_new_like(sess->delta_pool, "$concat", a);
    if (!out) {
        if (a_e.owned)
            col_rel_destroy(a);
        if (b_e.owned)
            col_rel_destroy(b);
        return ENOMEM;
    }

    int rc = col_rel_append_all(out, a);
    if (rc == 0)
        rc = col_rel_append_all(out, b);

    if (rc != 0) {
        if (a_e.seg_boundaries)
            free(a_e.seg_boundaries);
        if (b_e.seg_boundaries)
            free(b_e.seg_boundaries);
        if (a_e.owned)
            col_rel_destroy(a);
        if (b_e.owned)
            col_rel_destroy(b);
        col_rel_destroy(out);
        return rc;
    }

    /* Track segment boundaries for K-way merge optimization. */
    uint32_t left_segs = a_e.seg_count > 0 ? a_e.seg_count : 1;
    uint32_t right_segs = b_e.seg_count > 0 ? b_e.seg_count : 1;
    uint32_t total_segs = left_segs + right_segs;

    uint32_t *out_boundaries
        = (uint32_t *)malloc((total_segs + 1) * sizeof(uint32_t));
    if (!out_boundaries) {
        if (a_e.seg_boundaries)
            free(a_e.seg_boundaries);
        if (b_e.seg_boundaries)
            free(b_e.seg_boundaries);
        if (a_e.owned)
            col_rel_destroy(a);
        if (b_e.owned)
            col_rel_destroy(b);
        col_rel_destroy(out);
        return ENOMEM;
    }

    /* Copy left boundaries */
    if (a_e.seg_boundaries != NULL) {
        memcpy(out_boundaries, a_e.seg_boundaries,
               (left_segs + 1) * sizeof(uint32_t));
    } else {
        out_boundaries[0] = 0;
        out_boundaries[1] = a->nrows;
    }

    /* Adjust and append right boundaries */
    uint32_t right_offset = a->nrows;
    if (b_e.seg_boundaries != NULL) {
        for (uint32_t i = 0; i <= right_segs; i++)
            out_boundaries[left_segs + i]
                = b_e.seg_boundaries[i] + right_offset;
    } else {
        out_boundaries[left_segs] = right_offset;
        out_boundaries[left_segs + 1] = out->nrows;
    }

    /* Clean up input boundaries */
    if (a_e.seg_boundaries)
        free(a_e.seg_boundaries);
    if (b_e.seg_boundaries)
        free(b_e.seg_boundaries);

    if (a_e.owned)
        col_rel_destroy(a);
    if (b_e.owned)
        col_rel_destroy(b);

    rc = eval_stack_push(stack, out, true);
    if (rc != 0) {
        free(out_boundaries);
        col_rel_destroy(out);
        return rc;
    }

    /* Attach boundary metadata to the pushed entry */
    stack->items[stack->top - 1].seg_boundaries = out_boundaries;
    stack->items[stack->top - 1].seg_count = total_segs;
    return 0;
}

/* --- CONSOLIDATE --------------------------------------------------------- */

/* Comparison for qsort_r: lexicographic int64 row order.
 * Note: BSD qsort_r has signature: qsort_r(base, nmemb, size, ctx, comparator)
 *       GNU qsort_r has signature: qsort_r(base, nmemb, size, comparator, arg)
 * We use the BSD signature here and conditionally adapt for GNU systems.
 */
#ifdef __GLIBC__
/* GNU glibc qsort_r: comparator first, context last */
static int
row_cmp_fn(const void *a, const void *b, void *ctx)
{
    const uint32_t ncols = *(const uint32_t *)ctx;
    const int64_t *ra = (const int64_t *)a;
    const int64_t *rb = (const int64_t *)b;
    for (uint32_t c = 0; c < ncols; c++) {
        if (ra[c] < rb[c])
            return -1;
        if (ra[c] > rb[c])
            return 1;
    }
    return 0;
}
#define QSORT_R_CALL(base, nmemb, size, ctx, fn) \
    qsort_r(base, nmemb, size, fn, ctx)
#elif defined(_MSC_VER)
/* MSVC qsort_s: context first, comparator last (same signature as BSD qsort_r) */
static int __cdecl row_cmp_fn(void *ctx, const void *a, const void *b)
{
    const uint32_t ncols = *(const uint32_t *)ctx;
    const int64_t *ra = (const int64_t *)a;
    const int64_t *rb = (const int64_t *)b;
    for (uint32_t c = 0; c < ncols; c++) {
        if (ra[c] < rb[c])
            return -1;
        if (ra[c] > rb[c])
            return 1;
    }
    return 0;
}
#define QSORT_R_CALL(base, nmemb, size, ctx, fn) \
    qsort_s(base, nmemb, size, fn, ctx)
#else
/* BSD qsort_r: context first, comparator last */
static int
row_cmp_fn(void *ctx, const void *a, const void *b)
{
    const uint32_t ncols = *(const uint32_t *)ctx;
    const int64_t *ra = (const int64_t *)a;
    const int64_t *rb = (const int64_t *)b;
    for (uint32_t c = 0; c < ncols; c++) {
        if (ra[c] < rb[c])
            return -1;
        if (ra[c] > rb[c])
            return 1;
    }
    return 0;
}
#define QSORT_R_CALL(base, nmemb, size, ctx, fn) \
    qsort_r(base, nmemb, size, ctx, fn)
#endif

/* Lexicographic int64_t row comparison for K-way merge.
 * Equivalent to row_cmp_lex / row_cmp_optimized but available before
 * the SIMD dispatcher is defined. */
static inline int
kway_row_cmp(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    for (uint32_t c = 0; c < ncols; c++) {
        if (a[c] < b[c])
            return -1;
        if (a[c] > b[c])
            return 1;
    }
    return 0;
}

/*
 * col_op_consolidate_kway_merge - K-way merge with per-segment sort and dedup.
 *
 * Sorts each segment in-place, then merges K sorted segments using a min-heap.
 * Deduplicates on-the-fly during merge. Writes merged result back into rel.
 *
 * For K=1: just sort + dedup in-place.
 * For K=2: optimized 2-way merge (no heap overhead).
 * For K>=3: min-heap merge with O(M log K) comparisons.
 *
 * @rel            Relation containing K concatenated segments.
 * @seg_boundaries Array of (seg_count+1) offsets [s0, s1, ..., sK].
 * @seg_count      Number of segments K.
 * @return         0 on success, ENOMEM on allocation failure.
 */
int
col_op_consolidate_kway_merge(col_rel_t *rel, const uint32_t *seg_boundaries,
                              uint32_t seg_count)
{
    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;
    size_t row_bytes = (size_t)nc * sizeof(int64_t);

    if (nr <= 1)
        return 0;

    /* Sort each segment in-place */
    for (uint32_t s = 0; s < seg_count; s++) {
        uint32_t start = seg_boundaries[s];
        uint32_t end = seg_boundaries[s + 1];
        if (end > start + 1) {
            QSORT_R_CALL(rel->data + (size_t)start * nc, end - start, row_bytes,
                         &nc, row_cmp_fn);
        }
    }

    /* K=1: segments already sorted, just dedup in-place */
    if (seg_count == 1) {
        uint32_t out_r = 1;
        for (uint32_t r = 1; r < nr; r++) {
            const int64_t *prev = rel->data + (size_t)(r - 1) * nc;
            const int64_t *cur = rel->data + (size_t)r * nc;
            if (memcmp(prev, cur, row_bytes) != 0) {
                if (out_r != r)
                    memcpy(rel->data + (size_t)out_r * nc, cur, row_bytes);
                out_r++;
            }
        }
        rel->nrows = out_r;
        return 0;
    }

    /* Allocate merge output buffer */
    int64_t *merged = (int64_t *)malloc((size_t)nr * nc * sizeof(int64_t));
    if (!merged)
        return ENOMEM;

    if (seg_count == 2) {
        /* Optimized 2-way merge (no heap) */
        uint32_t mid = seg_boundaries[1];
        uint32_t i = seg_boundaries[0], j = mid;
        uint32_t i_end = mid, j_end = seg_boundaries[2];
        uint32_t out = 0;
        int64_t *last_row = NULL;

        while (i < i_end && j < j_end) {
            int64_t *row_i = rel->data + (size_t)i * nc;
            int64_t *row_j = rel->data + (size_t)j * nc;
            int cmp = kway_row_cmp(row_i, row_j, nc);
            int64_t *row_to_add;

            if (cmp <= 0) {
                row_to_add = row_i;
                i++;
                if (cmp == 0)
                    j++; /* skip duplicate */
            } else {
                row_to_add = row_j;
                j++;
            }

            if (last_row == NULL
                || memcmp(last_row, row_to_add, row_bytes) != 0) {
                memcpy(merged + (size_t)out * nc, row_to_add, row_bytes);
                last_row = merged + (size_t)out * nc;
                out++;
            }
        }

        while (i < i_end) {
            int64_t *row = rel->data + (size_t)i * nc;
            if (last_row == NULL || memcmp(last_row, row, row_bytes) != 0) {
                memcpy(merged + (size_t)out * nc, row, row_bytes);
                last_row = merged + (size_t)out * nc;
                out++;
            }
            i++;
        }

        while (j < j_end) {
            int64_t *row = rel->data + (size_t)j * nc;
            if (last_row == NULL || memcmp(last_row, row, row_bytes) != 0) {
                memcpy(merged + (size_t)out * nc, row, row_bytes);
                last_row = merged + (size_t)out * nc;
                out++;
            }
            j++;
        }

        memcpy(rel->data, merged, (size_t)out * nc * sizeof(int64_t));
        rel->nrows = out;
        free(merged);
        return 0;
    }

    /* General K-way merge (K >= 3) using min-heap.
     *
     * Heap entries: (segment_index, current_row_pointer).
     * Heap property: parent row <= child rows (lexicographic).
     */
    typedef struct {
        uint32_t seg;    /* segment index */
        uint32_t cursor; /* current row index within rel->data */
        uint32_t end;    /* one-past-end row index for this segment */
    } heap_entry_t;

    /* Build initial heap from non-empty segments */
    heap_entry_t *heap
        = (heap_entry_t *)malloc(seg_count * sizeof(heap_entry_t));
    if (!heap) {
        free(merged);
        return ENOMEM;
    }

    uint32_t heap_size = 0;
    for (uint32_t s = 0; s < seg_count; s++) {
        if (seg_boundaries[s] < seg_boundaries[s + 1]) {
            heap[heap_size].seg = s;
            heap[heap_size].cursor = seg_boundaries[s];
            heap[heap_size].end = seg_boundaries[s + 1];
            heap_size++;
        }
    }

    /* Sift-down helper (inline macro for performance) */
#define HEAP_ROW(idx) (rel->data + (size_t)heap[(idx)].cursor * nc)
#define HEAP_SIFT_DOWN(start, size)                                      \
    do {                                                                 \
        uint32_t _p = (start);                                           \
        while (2 * _p + 1 < (size)) {                                    \
            uint32_t _c = 2 * _p + 1;                                    \
            if (_c + 1 < (size)                                          \
                && kway_row_cmp(HEAP_ROW(_c + 1), HEAP_ROW(_c), nc) < 0) \
                _c++;                                                    \
            if (kway_row_cmp(HEAP_ROW(_p), HEAP_ROW(_c), nc) <= 0)       \
                break;                                                   \
            heap_entry_t _tmp = heap[_p];                                \
            heap[_p] = heap[_c];                                         \
            heap[_c] = _tmp;                                             \
            _p = _c;                                                     \
        }                                                                \
    } while (0)

    /* Build min-heap (heapify) */
    if (heap_size > 1) {
        for (int32_t i = (int32_t)(heap_size / 2) - 1; i >= 0; i--)
            HEAP_SIFT_DOWN((uint32_t)i, heap_size);
    }

    /* Extract-min loop with dedup */
    uint32_t out = 0;
    int64_t *last_row = NULL;

    while (heap_size > 0) {
        int64_t *min_row = HEAP_ROW(0);

        /* Dedup: skip if same as last emitted row */
        if (last_row == NULL || memcmp(last_row, min_row, row_bytes) != 0) {
            memcpy(merged + (size_t)out * nc, min_row, row_bytes);
            last_row = merged + (size_t)out * nc;
            out++;
        }

        /* Advance cursor of min segment */
        heap[0].cursor++;
        if (heap[0].cursor >= heap[0].end) {
            /* Segment exhausted: replace root with last element */
            heap[0] = heap[heap_size - 1];
            heap_size--;
        }
        if (heap_size > 0)
            HEAP_SIFT_DOWN(0, heap_size);
    }

#undef HEAP_ROW
#undef HEAP_SIFT_DOWN

    memcpy(rel->data, merged, (size_t)out * nc * sizeof(int64_t));
    rel->nrows = out;
    free(merged);
    free(heap);
    return 0;
}

static int
col_op_consolidate(eval_stack_t *stack, wl_col_session_t *sess)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    col_rel_t *in = e.rel;
    uint32_t nc = in->ncols;
    uint32_t nr = in->nrows;

    if (nr <= 1) {
        /* Nothing to deduplicate */
        if (e.seg_boundaries)
            free(e.seg_boundaries);
        in->sorted_nrows = nr;
        return eval_stack_push(stack, in, e.owned);
    }

    /* Sort in-place if we own the relation, otherwise copy first */
    col_rel_t *work = in;
    bool work_owned = e.owned;
    if (!work_owned) {
        work = col_rel_pool_new_like(sess->delta_pool, "$consol", in);
        if (!work) {
            if (e.seg_boundaries)
                free(e.seg_boundaries);
            return ENOMEM;
        }
        if (col_rel_append_all(work, in) != 0) {
            col_rel_destroy(work);
            if (e.seg_boundaries)
                free(e.seg_boundaries);
            return ENOMEM;
        }
        work_owned = true;
    }

    /* Dispatch: K-way merge when segment boundaries are available */
    uint32_t k = e.seg_count > 0 ? e.seg_count : 1;
    if (k >= 2 && e.seg_boundaries != NULL) {
        int rc = col_op_consolidate_kway_merge(work, e.seg_boundaries, k);
        free(e.seg_boundaries);
        if (rc != 0) {
            if (work_owned)
                col_rel_destroy(work);
            return rc;
        }
        work->sorted_nrows = work->nrows;
        return eval_stack_push(stack, work, work_owned);
    }

    if (e.seg_boundaries)
        free(e.seg_boundaries);

    size_t row_bytes = (size_t)nc * sizeof(int64_t);

    /* Issue #94: Incremental merge when a sorted prefix exists.
     * data[0..sorted_nrows) is already sorted+unique from a prior
     * consolidation.  Sort only the unsorted suffix and merge. */
    uint32_t sn = work->sorted_nrows;
    if (sn > 0 && sn < nr) {
        uint32_t delta_count = nr - sn;
        int64_t *delta_start = work->data + (size_t)sn * nc;

        /* Phase 1: sort only the unsorted suffix */
        QSORT_R_CALL(delta_start, delta_count, row_bytes, &nc, row_cmp_fn);

        /* Phase 1b: dedup within suffix */
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

        /* Phase 2: merge sorted prefix with sorted suffix */
        uint32_t max_rows = sn + d_unique;

        /* Reuse persistent merge buffer when possible */
        int64_t *merged;
        bool used_merge_buf = false;
        if (work->merge_buf && work->merge_buf_cap >= max_rows) {
            merged = work->merge_buf;
            used_merge_buf = true;
        } else {
            /* Grow persistent buffer */
            uint32_t new_cap = max_rows > work->merge_buf_cap * 2
                                   ? max_rows
                                   : work->merge_buf_cap * 2;
            if (new_cap < max_rows)
                new_cap = max_rows;
            int64_t *nb = (int64_t *)realloc(
                work->merge_buf, (size_t)new_cap * nc * sizeof(int64_t));
            if (!nb) {
                if (work_owned && work != in)
                    col_rel_destroy(work);
                return ENOMEM;
            }
            work->merge_buf = nb;
            work->merge_buf_cap = new_cap;
            merged = nb;
            used_merge_buf = true;
        }

        uint32_t oi = 0, di = 0, out = 0;
        while (oi < sn && di < d_unique) {
            const int64_t *orow = work->data + (size_t)oi * nc;
            const int64_t *drow = delta_start + (size_t)di * nc;
            int cmp = memcmp(orow, drow, row_bytes);
            if (cmp < 0) {
                memcpy(merged + (size_t)out * nc, orow, row_bytes);
                oi++;
            } else if (cmp == 0) {
                memcpy(merged + (size_t)out * nc, orow, row_bytes);
                oi++;
                di++;
            } else {
                memcpy(merged + (size_t)out * nc, drow, row_bytes);
                di++;
            }
            out++;
        }
        while (oi < sn) {
            memcpy(merged + (size_t)out * nc, work->data + (size_t)oi * nc,
                   row_bytes);
            oi++;
            out++;
        }
        while (di < d_unique) {
            memcpy(merged + (size_t)out * nc, delta_start + (size_t)di * nc,
                   row_bytes);
            di++;
            out++;
        }

        /* Swap: merged data becomes work->data */
        if (used_merge_buf) {
            /* merge_buf holds the result; allocate new data, copy back */
            if (work->capacity < out) {
                int64_t *nd = (int64_t *)realloc(
                    work->data, (size_t)out * nc * sizeof(int64_t));
                if (!nd) {
                    if (work_owned && work != in)
                        col_rel_destroy(work);
                    return ENOMEM;
                }
                work->data = nd;
                work->capacity = out;
            }
            memcpy(work->data, merged, (size_t)out * nc * sizeof(int64_t));
        }
        work->nrows = out;
        work->sorted_nrows = out;
        return eval_stack_push(stack, work, work_owned);
    }

    /* Fallback: standard qsort + dedup (sorted_nrows == 0 or full re-sort) */
    QSORT_R_CALL(work->data, nr, row_bytes, &nc, row_cmp_fn);

    /* Compact: keep only unique rows */
    uint32_t out_r = 1; /* first row always kept */
    for (uint32_t r = 1; r < nr; r++) {
        const int64_t *prev = work->data + (size_t)(r - 1) * nc;
        const int64_t *cur = work->data + (size_t)r * nc;
        if (memcmp(prev, cur, row_bytes) != 0) {
            if (out_r != r)
                memcpy(work->data + (size_t)out_r * nc, cur, row_bytes);
            out_r++;
        }
    }
    work->nrows = out_r;
    work->sorted_nrows = out_r;

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
static int UNUSED
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
    QSORT_R_CALL(delta_start, delta_count, row_bytes, &nc, row_cmp_fn);

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

/* Helper: lexicographic int64_t row comparison (-1/0/+1).
 * Compares rows a and b with ncols columns using int64_t values (not bytes).
 * Required for correct little-endian int64_t comparisons.
 */
static int UNUSED
row_cmp_lex(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    for (uint32_t c = 0; c < ncols; c++) {
        if (a[c] < b[c])
            return -1;
        if (a[c] > b[c])
            return 1;
    }
    return 0;
}

#ifdef __AVX2__
/* row_cmp_simd_avx2 - AVX2-accelerated lexicographic int64_t row comparison.
 *
 * Compares rows a and b (each ncols int64_t values) and returns -1, 0, or +1,
 * identical in semantics to row_cmp_lex().  Processes 4 elements per SIMD
 * iteration then falls back to scalar for the remainder.
 *
 * No alignment assumptions: unaligned loads (_mm256_loadu_si256) are used.
 */
static inline int
row_cmp_simd_avx2(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    uint32_t i = 0;

    /* Process 4 int64_t elements per iteration (256-bit vectors). */
    for (; i + 4 <= ncols; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i *)(a + i));
        __m256i vb = _mm256_loadu_si256((const __m256i *)(b + i));

        /* eq_mask: 0xFFFFFFFFFFFFFFFF for equal lanes, 0 otherwise. */
        __m256i eq_mask = _mm256_cmpeq_epi64(va, vb);

        /* Collapse equality mask to 4-bit scalar (one bit per byte-group of 8).
         * movemask gives one bit per byte; equal lane -> 8 bits set -> 0xFF.
         * We check for a fully-equal lane by looking at 8-bit groups. */
        int eq_bits = _mm256_movemask_epi8(eq_mask); /* 32 bits, 8 per lane */

        if (eq_bits == (int)0xFFFFFFFF) {
            /* All 4 lanes are equal; continue to next chunk. */
            continue;
        }

        /* At least one lane differs.  Find the lowest-index differing lane.
         * eq_bits has 8 consecutive bits set for an equal lane.
         * Lane k occupies bits [8k .. 8k+7].  A differing lane has at least
         * one of those bits clear, so (~eq_bits) has a set bit in that range.
         */
        int neq = ~eq_bits;
        /* ctz gives the position of the first differing byte; divide by 8
         * gives the lane index within this 4-element chunk. */
        int lane = __builtin_ctz((unsigned int)neq) / 8;
        int64_t av = a[i + (uint32_t)lane];
        int64_t bv = b[i + (uint32_t)lane];
        return (av < bv) ? -1 : 1;
    }

    /* Scalar fallback for the remaining ncols % 4 elements. */
    for (; i < ncols; i++) {
        if (a[i] < b[i])
            return -1;
        if (a[i] > b[i])
            return 1;
    }
    return 0;
}
#endif /* __AVX2__ */

#ifdef __ARM_NEON__
/* row_cmp_simd_neon - NEON-accelerated lexicographic int64_t row comparison.
 *
 * Compares rows a and b (each ncols int64_t values) and returns -1, 0, or +1,
 * identical in semantics to row_cmp_lex().  Processes 2 elements per SIMD
 * iteration then falls back to scalar for the remainder.
 *
 * No alignment assumptions: unaligned loads (vld1q_s64) are used.
 */
static inline int
row_cmp_simd_neon(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    uint32_t i = 0;

    /* Process 2 int64_t elements per iteration (128-bit vectors). */
    for (; i + 2 <= ncols; i += 2) {
        int64x2_t va = vld1q_s64(a + i);
        int64x2_t vb = vld1q_s64(b + i);

        /* eq_mask: all-ones (0xFFFFFFFFFFFFFFFF) for equal lanes, 0 otherwise. */
        uint64x2_t eq_mask = vceqq_s64(va, vb);

        /* Optimized lane extraction: check lane 0 first, avoid ternary operator.
         * This improves instruction scheduling and reduces branch prediction stalls. */
        uint64_t eq0 = vgetq_lane_u64(eq_mask, 0);
        if (!eq0) {
            /* Lane 0 differs; extract and compare. */
            int64_t av = vgetq_lane_s64(va, 0);
            int64_t bv = vgetq_lane_s64(vb, 0);
            return (av < bv) ? -1 : 1;
        }

        /* Lane 0 is equal; check lane 1. */
        uint64_t eq1 = vgetq_lane_u64(eq_mask, 1);
        if (eq1) {
            /* Both lanes equal; continue to next pair. */
            continue;
        }

        /* Lane 1 differs; extract and compare. */
        int64_t av = vgetq_lane_s64(va, 1);
        int64_t bv = vgetq_lane_s64(vb, 1);
        return (av < bv) ? -1 : 1;
    }

    /* Scalar fallback for the remaining ncols % 2 element. */
    if (i < ncols) {
        if (a[i] < b[i])
            return -1;
        if (a[i] > b[i])
            return 1;
    }
    return 0;
}
#endif /* __ARM_NEON__ */

/* Dispatcher: Select best row comparison at compile time.
 * Automatically chooses AVX2, NEON, or scalar fallback.
 */
#ifdef __AVX2__
#define row_cmp_optimized row_cmp_simd_avx2
#elif defined(__ARM_NEON__)
#define row_cmp_optimized row_cmp_simd_neon
#else
#define row_cmp_optimized row_cmp_lex
#endif

/* ---- profiling helper --------------------------------------------------- */

/*
 * now_ns: return monotonic time in nanoseconds.
 * Uses clock_gettime(CLOCK_MONOTONIC) on POSIX systems,
 * GetTickCount64() on Windows MSVC.
 * Returns 0 on platforms where time function is unavailable (non-fatal).
 */
static uint64_t
now_ns(void)
{
#ifdef _MSC_VER
    /* Windows MSVC: use GetTickCount64() in milliseconds, convert to nanoseconds.
     * Returns milliseconds since system startup (monotonic). */
    return GetTickCount64() * 1000000ULL;
#else
    /* POSIX: use clock_gettime with CLOCK_MONOTONIC. */
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        return 0;
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
#endif
}

/*
 * col_op_consolidate_incremental_delta - Incremental consolidation with delta output
 *
 * PURPOSE:
 *   Merge pre-sorted old data with newly appended delta rows, while simultaneously
 *   emitting the set of truly-new rows (R_new - R_old) as a byproduct.
 *   This eliminates separate post-iteration merge walk needed for delta computation.
 *
 * PRECONDITIONS:
 *   - rel->data[0..old_nrows) is already sorted and unique (invariant)
 *   - rel->data[old_nrows..rel->nrows) contains newly appended delta rows (unsorted)
 *   - old_nrows <= rel->nrows
 *
 * POSTCONDITIONS:
 *   - rel->data[0..rel->nrows) is sorted and unique (new invariant)
 *   - delta_out->data contains exactly R_new - R_old (truly new rows)
 *   - delta_out->data is sorted in same order as rel->data
 *   - rel->nrows reflects final merged count
 *
 * MEMORY OWNERSHIP:
 *   - Caller allocates col_rel_t *delta_out (structure only)
 *   - Function allocates and owns delta_out->data (int64_t array)
 *   - Caller responsible for freeing delta_out->data via col_rel_free_contents()
 *   - If delta_out == NULL, new rows not collected (merge only)
 *
 * ERROR HANDLING:
 *   - Returns 0 on success
 *   - Returns ENOMEM if malloc fails
 *   - On error, rel and delta_out states are undefined; caller should not use
 *
 * ALGORITHM COMPLEXITY:
 *   - Time: O(D log D + N + D) where D = new delta rows, N = old rows
 *   - Space: O(N + D) for merge buffer + delta_out buffer
 *   - Dominant term: O(N) when D << N (typical in late iterations)
 */
int
col_op_consolidate_incremental_delta(col_rel_t *rel, uint32_t old_nrows,
                                     col_rel_t *delta_out)
{
    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;

    if (nr <= 1 || old_nrows >= nr)
        return 0; /* nothing new */

    uint32_t delta_count = nr - old_nrows;
    int64_t *delta_start = rel->data + (size_t)old_nrows * nc;
    size_t row_bytes = (size_t)nc * sizeof(int64_t);

    /* Phase 1: sort only the new delta rows */
    QSORT_R_CALL(delta_start, delta_count, row_bytes, &nc, row_cmp_fn);

    /* Phase 1b: dedup within delta */
    uint32_t d_unique = 1;
    for (uint32_t i = 1; i < delta_count; i++) {
        if (row_cmp_optimized(delta_start + (size_t)(i - 1) * nc,
                              delta_start + (size_t)i * nc, nc)
            != 0) {
            if (d_unique != i)
                memcpy(delta_start + (size_t)d_unique * nc,
                       delta_start + (size_t)i * nc, row_bytes);
            d_unique++;
        }
    }

    /* Phase 2: merge sorted old [0..old_nrows) with sorted+unique delta.
     * Rows present in delta but not in old are emitted into delta_out.
     *
     * Issue #94: Reuse persistent merge buffer to avoid per-call malloc/free.
     * The merge buffer lives in rel->merge_buf and grows via realloc. */
    uint32_t max_rows = old_nrows + d_unique;

    if (rel->merge_buf_cap < max_rows) {
        uint32_t new_cap = max_rows > rel->merge_buf_cap * 2
                               ? max_rows
                               : rel->merge_buf_cap * 2;
        if (new_cap < max_rows)
            new_cap = max_rows;
        int64_t *nb = (int64_t *)realloc(rel->merge_buf, (size_t)new_cap * nc
                                                             * sizeof(int64_t));
        if (!nb)
            return ENOMEM;
        rel->merge_buf = nb;
        rel->merge_buf_cap = new_cap;
    }
    int64_t *merged = rel->merge_buf;

    uint32_t oi = 0, di = 0, out = 0;
    const int64_t *o_ptr = rel->data;
    const int64_t *d_ptr = delta_start;
    int64_t *merged_ptr = merged;
    while (oi < old_nrows && di < d_unique) {
        int cmp = row_cmp_optimized(o_ptr, d_ptr, nc);
        const int64_t *row_to_copy = (cmp < 0) ? o_ptr : d_ptr;
        memcpy(merged_ptr, row_to_copy, row_bytes);

        if (cmp == 0) {
            /* duplicate: skip delta row */
            d_ptr += nc;
            di++;
        }
        if (cmp <= 0) {
            o_ptr += nc;
            oi++;
        } else {
            /* delta row not in old: new fact */
            if (delta_out)
                col_rel_append_row(delta_out, d_ptr);
            d_ptr += nc;
            di++;
        }
        merged_ptr += nc;
        out++;
    }
    /* Remaining old rows */
    if (oi < old_nrows) {
        uint32_t remaining = old_nrows - oi;
        memcpy(merged + (size_t)out * nc, rel->data + (size_t)oi * nc,
               (size_t)remaining * row_bytes);
        out += remaining;
    }
    /* Remaining delta rows: all new */
    if (di < d_unique) {
        if (delta_out) {
            for (uint32_t k = di; k < d_unique; k++)
                col_rel_append_row(delta_out, delta_start + (size_t)k * nc);
        }
        uint32_t remaining = d_unique - di;
        memcpy(merged + (size_t)out * nc, delta_start + (size_t)di * nc,
               (size_t)remaining * row_bytes);
        out += remaining;
    }

    /* Copy merged result back into rel->data (merge_buf is persistent,
     * cannot be swapped into data without losing merge_buf for next call). */
    if (rel->capacity < out) {
        int64_t *nd
            = (int64_t *)realloc(rel->data, (size_t)out * nc * sizeof(int64_t));
        if (!nd)
            return ENOMEM;
        rel->data = nd;
        rel->capacity = out;
    }
    memcpy(rel->data, merged, (size_t)out * nc * sizeof(int64_t));
    rel->nrows = out;
    rel->sorted_nrows = out;

    /* Phase 4: Update timestamp array to match consolidated data.
     * After merge, timestamps for old rows are still valid, but new rows
     * from delta have no timestamp information. Mark timestamps as invalid
     * by deallocating (frontier computation will see NULL and return (0,0)). */
    if (rel->timestamps) {
        free(rel->timestamps);
        rel->timestamps = NULL;
    }
    return 0;
}

/* --- K-FUSION ------------------------------------------------------------ */

/**
 * col_rel_merge_k:
 * Merge K sorted relations into a single deduplicated relation.
 * Uses the same min-heap merging strategy as col_op_consolidate_kway_merge.
 *
 * @relations: Array of K col_rel_t pointers (caller-owned, each sorted)
 * @k:         Number of relations to merge
 *
 * Returns: Newly allocated merged relation (caller must free).
 *          Returns NULL on allocation failure.
 *
 * The output relation name is "<merged-k>" and contains all rows from
 * the K input relations with duplicates removed.
 */
static col_rel_t *UNUSED
col_rel_merge_k(col_rel_t **relations, uint32_t k)
{
    if (k == 0)
        return NULL;

    /* All K relations must have the same schema */
    uint32_t nc = relations[0]->ncols;
    uint32_t total_rows = 0;
    for (uint32_t i = 0; i < k; i++) {
        if (relations[i]->ncols != nc)
            return NULL; /* Schema mismatch */
        total_rows += relations[i]->nrows;
    }

    if (total_rows == 0) {
        /* Create empty result with correct schema */
        return col_rel_new_like("<merged-k>", relations[0]);
    }

    /* Create output relation with capacity for all rows */
    col_rel_t *out = col_rel_new_like("<merged-k>", relations[0]);
    if (!out)
        return NULL;

    /* K=1: Copy with dedup using append (handles dynamic growth) */
    if (k == 1) {
        col_rel_t *src = relations[0];
        const int64_t *last_row = NULL;
        for (uint32_t r = 0; r < src->nrows; r++) {
            const int64_t *row = src->data + (size_t)r * nc;
            if (last_row == NULL || kway_row_cmp(last_row, row, nc) != 0) {
                if (col_rel_append_row(out, row) != 0) {
                    col_rel_destroy(out);
                    return NULL;
                }
                last_row = out->data + (size_t)(out->nrows - 1) * nc;
            }
        }
        return out;
    }

    /* K=2: Optimized 2-pointer merge using append */
    if (k == 2) {
        col_rel_t *left = relations[0];
        col_rel_t *right = relations[1];
        uint32_t li = 0, ri = 0;
        const int64_t *last_row = NULL;

        while (li < left->nrows && ri < right->nrows) {
            const int64_t *lrow = left->data + (size_t)li * nc;
            const int64_t *rrow = right->data + (size_t)ri * nc;
            int cmp = kway_row_cmp(lrow, rrow, nc);

            const int64_t *row_to_add = NULL;
            if (cmp < 0) {
                row_to_add = lrow;
                li++;
            } else if (cmp > 0) {
                row_to_add = rrow;
                ri++;
            } else {
                /* Equal rows: add once, skip both */
                row_to_add = lrow;
                li++;
                ri++;
            }

            if (last_row == NULL
                || kway_row_cmp(last_row, row_to_add, nc) != 0) {
                if (col_rel_append_row(out, row_to_add) != 0) {
                    col_rel_destroy(out);
                    return NULL;
                }
                last_row = out->data + (size_t)(out->nrows - 1) * nc;
            }
        }

        /* Drain remaining rows from left */
        while (li < left->nrows) {
            const int64_t *row = left->data + (size_t)li * nc;
            if (last_row == NULL || kway_row_cmp(last_row, row, nc) != 0) {
                if (col_rel_append_row(out, row) != 0) {
                    col_rel_destroy(out);
                    return NULL;
                }
                last_row = out->data + (size_t)(out->nrows - 1) * nc;
            }
            li++;
        }

        /* Drain remaining rows from right */
        while (ri < right->nrows) {
            const int64_t *row = right->data + (size_t)ri * nc;
            if (last_row == NULL || kway_row_cmp(last_row, row, nc) != 0) {
                if (col_rel_append_row(out, row) != 0) {
                    col_rel_destroy(out);
                    return NULL;
                }
                last_row = out->data + (size_t)(out->nrows - 1) * nc;
            }
            ri++;
        }

        return out;
    }

    /* K >= 3: Pairwise merge fallback */
    col_rel_t *temp = relations[0];
    for (uint32_t i = 1; i < k; i++) {
        col_rel_t *pair[2] = { temp, relations[i] };
        col_rel_t *merged = col_rel_merge_k(pair, 2);
        if (!merged) {
            col_rel_destroy(out);
            if (i > 1)
                col_rel_destroy(temp);
            return NULL;
        }
        if (i > 1)
            col_rel_destroy(temp);
        temp = merged;
    }

    /* Move final result into output using append */
    {
        const int64_t *last_row = NULL;
        for (uint32_t r = 0; r < temp->nrows; r++) {
            const int64_t *row = temp->data + (size_t)r * nc;
            if (last_row == NULL || kway_row_cmp(last_row, row, nc) != 0) {
                if (col_rel_append_row(out, row) != 0) {
                    col_rel_destroy(out);
                    col_rel_destroy(temp);
                    return NULL;
                }
                last_row = out->data + (size_t)(out->nrows - 1) * nc;
            }
        }
        col_rel_destroy(temp);
    }

    return out;
}

/**
 * Worker task context for K-fusion evaluation.
 * plan_data is embedded (not a pointer) so its lifetime matches the worker array.
 * sess points to an isolated session wrapper with a per-worker mat_cache so
 * concurrent col_op_join calls do not share the non-thread-safe cache.
 */
typedef struct {
    wl_plan_relation_t plan_data; /* Embedded plan (stable lifetime) */
    eval_stack_t stack;           /* Output stack (initialized by worker) */
    wl_col_session_t
        *sess;    /* Per-worker session wrapper (isolated mat_cache) */
    int rc;       /* Return code from evaluation */
    bool skipped; /* true if skipped due to empty forced delta (#85) */
} col_op_k_fusion_worker_t;

/**
 * Worker thread function for K-fusion parallel evaluation.
 * Evaluates a single relation plan and collects result in context.
 */
static void
col_op_k_fusion_worker(void *ctx)
{
    col_op_k_fusion_worker_t *wc = (col_op_k_fusion_worker_t *)ctx;
    eval_stack_init(&wc->stack);
    wc->rc = col_eval_relation_plan(&wc->plan_data, &wc->stack, wc->sess);
}

/**
 * K-Fusion operator: evaluate K copies of a relation plan via workqueue,
 * merge results with deduplication, and push result onto stack.
 *
 * Each of the K operator sequences in opaque_data is submitted as a
 * separate worker task to the workqueue. The K workers evaluate in
 * parallel (or sequentially on single-threaded systems).
 * Results are merged via col_rel_merge_k() after all workers complete.
 */
static int
col_op_k_fusion(const wl_plan_op_t *op, eval_stack_t *stack,
                wl_col_session_t *sess)
{
    if (!op->opaque_data)
        return EINVAL;

    wl_plan_op_k_fusion_t *meta = (wl_plan_op_k_fusion_t *)op->opaque_data;
    uint32_t k = meta->k;
    if (k == 0)
        return EINVAL;

    uint64_t _phase_t0 = now_ns();
    col_rel_t **results = (col_rel_t **)calloc(k, sizeof(col_rel_t *));
    col_op_k_fusion_worker_t *workers = (col_op_k_fusion_worker_t *)calloc(
        k, sizeof(col_op_k_fusion_worker_t));
    /* Per-worker session wrappers: shallow copy of sess with an isolated
     * mat_cache so concurrent col_op_join calls do not race on the cache. */
    wl_col_session_t *worker_sess
        = (wl_col_session_t *)calloc(k, sizeof(wl_col_session_t));
    COL_SESSION(sess)->kfusion_alloc_ns += now_ns() - _phase_t0;
    if (!results || !workers || !worker_sess) {
        free(results);
        free(workers);
        free(worker_sess);
        return ENOMEM;
    }

    /* Use session-level workqueue created at col_session_create (issue #99).
     * When num_workers=1 (wq==NULL), K copies are evaluated sequentially
     * below with no thread overhead. */
    wl_work_queue_t *wq = sess->wq; /* NULL when num_workers=1 */

    int rc = 0;

    /* Snapshot the current mat_cache entry count.  Each worker inherits the
     * existing cache entries for lookup (read-only for shared entries).
     * At cleanup, only entries added by the worker (index >= base_count)
     * are freed, avoiding double-free of the shared result pointers. */
    uint32_t base_count = sess->mat_cache.count;

    /* Initialise per-worker session wrappers and submit all K tasks in one
     * batch so workers execute in parallel. */
    _phase_t0 = now_ns();
    for (uint32_t d = 0; d < k; d++) {
        /* Shallow copy shares rels[], plan, etc. (read-only during K-fusion).
         * mat_cache is copied by value so workers can look up existing entries
         * and add new ones without affecting the original session's cache.
         * arr_* and darr_* are zeroed: each worker builds its own arrangement
         * cache independently (no sharing, no races). Lock-free: no mutex needed
         * because each worker owns its isolated cache. */
        worker_sess[d] = *sess;
        worker_sess[d].wq = NULL; /* prevent nested K-fusion from workers */
        worker_sess[d].arr_entries = NULL;
        worker_sess[d].arr_count = 0;
        worker_sess[d].arr_cap = 0;
        worker_sess[d].darr_entries = NULL;
        worker_sess[d].darr_count = 0;
        worker_sess[d].darr_cap = 0;
        worker_sess[d].delta_pool
            = delta_pool_create(128, sizeof(col_rel_t), 32 * 1024 * 1024);

        workers[d].plan_data.name = "<k_fusion_copy>";
        workers[d].plan_data.ops = meta->k_ops[d];
        workers[d].plan_data.op_count = meta->k_op_counts[d];
        workers[d].sess = &worker_sess[d];
        workers[d].rc = 0;

        /* Per-copy empty-delta skip (issue #85): if this copy's sub-plan
         * has a FORCE_DELTA op referencing an empty/absent delta on
         * iteration > 0, skip dispatching — the copy would produce 0 rows. */
        if (has_empty_forced_delta(&workers[d].plan_data, sess,
                                   sess->current_iteration)) {
            workers[d].rc = 0; /* mark as succeeded with no output */
            workers[d].skipped = true;
            continue;
        }

        if (wq) {
            /* Parallel path: submit to session workqueue (issue #99) */
            if (wl_workqueue_submit(wq, col_op_k_fusion_worker, &workers[d])
                != 0) {
                rc = ENOMEM;
                wl_workqueue_drain(wq);
                goto cleanup_wq;
            }
        } else {
            /* Sequential fallback: execute directly (num_workers=1) */
            col_op_k_fusion_worker(&workers[d]);
        }
    }

    /* Barrier: wait for all parallel workers to complete.
     * Skipped when wq is NULL (sequential path already finished). */
    if (wq && wl_workqueue_wait_all(wq) != 0) {
        rc = -1;
        goto cleanup_wq;
    }
    COL_SESSION(sess)->kfusion_dispatch_ns += now_ns() - _phase_t0;

    /* Collect results from each worker's eval_stack */
    _phase_t0 = now_ns();
    for (uint32_t d = 0; d < k; d++) {
        /* Skipped workers (empty forced delta) contribute an empty result */
        if (workers[d].skipped) {
            results[d] = NULL; /* NULL = no rows from this copy */
            continue;
        }

        if (workers[d].rc != 0) {
            rc = workers[d].rc;
            eval_stack_drain(&workers[d].stack);
            goto cleanup_results;
        }

        eval_entry_t e = eval_stack_pop(&workers[d].stack);
        if (!e.rel) {
            rc = EINVAL;
            eval_stack_drain(&workers[d].stack);
            goto cleanup_results;
        }

        /* If not owned, make a copy we can hand to merge */
        if (!e.owned) {
            col_rel_t *copy = col_rel_pool_new_like(worker_sess[d].delta_pool,
                                                    "<k_fusion_copy>", e.rel);
            if (!copy) {
                rc = ENOMEM;
                eval_stack_drain(&workers[d].stack);
                goto cleanup_results;
            }
            size_t row_bytes = (size_t)e.rel->ncols * sizeof(int64_t);
            memcpy(copy->data, e.rel->data, (size_t)e.rel->nrows * row_bytes);
            copy->nrows = e.rel->nrows;
            results[d] = copy;
        } else {
            results[d] = e.rel;
        }
        eval_stack_drain(&workers[d].stack);
    }

    /* Merge K results with deduplication.
     * Workers ran WL_PLAN_OP_CONSOLIDATE as the last plan op, so each
     * result is already sorted+deduped — no qsort needed here.
     * Skipped copies (empty forced delta) have NULL results — compact
     * them out before merging. */
    {
        /* Compact non-NULL results (skipped copies have NULL). Use the
         * existing results array as backing — we build compact in-place. */
        col_rel_t **compact = (col_rel_t **)malloc(k * sizeof(col_rel_t *));
        if (!compact) {
            rc = ENOMEM;
            goto cleanup_results;
        }
        uint32_t n_results = 0;
        for (uint32_t d = 0; d < k; d++) {
            if (results[d])
                compact[n_results++] = results[d];
        }

        col_rel_t *merged;
        if (n_results == 0) {
            /* All copies skipped: produce empty output.  Derive column
             * count from the K-fusion target relation (op->relation_name)
             * so the empty result has a matching schema. */
            uint32_t ncols = 0;
            if (op->relation_name) {
                col_rel_t *target = session_find_rel(sess, op->relation_name);
                if (target)
                    ncols = target->ncols;
            }
            merged = col_rel_new_auto("$kfusion_empty", ncols);
        } else {
            merged = col_rel_merge_k(compact, n_results);
        }
        free(compact);
        if (!merged) {
            rc = ENOMEM;
            goto cleanup_results;
        }
        rc = eval_stack_push(stack, merged, true);
        if (rc != 0)
            col_rel_destroy(merged);
    }
    COL_SESSION(sess)->kfusion_merge_ns += now_ns() - _phase_t0;

cleanup_results:
    _phase_t0 = now_ns();
    for (uint32_t d = 0; d < k; d++) {
        if (results[d])
            col_rel_destroy(results[d]);
    }

cleanup_wq:
    /* On early-exit paths (submit failure, wait failure) _phase_t0 may hold
     * a stale dispatch value; reset it here so cleanup timing is correct. */
    _phase_t0 = now_ns();
    /* wq is session-owned and reused across iterations — do not destroy here.
     * Only free mat_cache entries the worker added (index >= base_count).
     * Entries 0..base_count-1 share result pointers with the original
     * session's mat_cache and must not be double-freed here.
     * Free each worker's private arrangement caches (arr_* and darr_*).
     * Lock-free design: no synchronization needed because each worker owns
     * its isolated cache — no races at cleanup time. */
    for (uint32_t d = 0; d < k; d++) {
        col_mat_cache_t *wc = &worker_sess[d].mat_cache;
        for (uint32_t i = base_count; i < wc->count; i++)
            col_rel_destroy(wc->entries[i].result);
        /* Free worker's private full-arrangement cache (arr_*). */
        for (uint32_t i = 0; i < worker_sess[d].arr_count; i++) {
            col_arr_entry_t *e = &worker_sess[d].arr_entries[i];
            free(e->rel_name);
            free(e->key_cols);
            arr_free_contents(&e->arr);
        }
        free(worker_sess[d].arr_entries);
        /* Free worker's private delta-arrangement cache (darr_*). */
        col_session_free_delta_arrangements(&worker_sess[d]);
        delta_pool_destroy(worker_sess[d].delta_pool);
    }
    free(worker_sess);
    free(results);
    free(workers);
    COL_SESSION(sess)->kfusion_cleanup_ns += now_ns() - _phase_t0;
    return rc;
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
        if (left_e.owned)
            col_rel_destroy(left);
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
    col_rel_t *out
        = col_rel_pool_new_auto(sess->delta_pool, "$semijoin", ocols);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
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
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (left_e.owned)
                    col_rel_destroy(left);
                return rc;
            }
        }
    }

    free(tmp);
    free(lk);
    free(rk);
    if (left_e.owned)
        col_rel_destroy(left);
    return eval_stack_push(stack, out, true);
}

/* --- REDUCE (aggregate) -------------------------------------------------- */

static int
col_op_reduce(const wl_plan_op_t *op, eval_stack_t *stack,
              wl_col_session_t *sess)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    col_rel_t *in = e.rel;
    uint32_t gc = op->group_by_count;

    /* Output: group_by columns + 1 aggregate column */
    uint32_t ocols = gc + 1;
    col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, "$reduce", ocols);
    if (!out) {
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(in);
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
                col_rel_destroy(out);
                if (e.owned)
                    col_rel_destroy(in);
                return rc;
            }
        }
    }

    free(tmp);
    if (e.owned)
        col_rel_destroy(in);
    return eval_stack_push(stack, out, true);
}

/* --- REDUCE WEIGHTED (Z-set / Mobius COUNT) ------------------------------ */

/*
 * col_op_reduce_weighted:
 * Global COUNT aggregation using Z-set (signed multiplicity) semantics.
 * Output: one row whose data value = sum of input multiplicities, and whose
 * timestamp.multiplicity = the same sum.
 *
 * src - input relation; src->timestamps[i].multiplicity carries each row's
 *       signed weight.
 * dst - output relation (caller-allocated, empty on entry, ncols >= 1).
 *
 * Returns 0 on success, EINVAL / ENOMEM on error.
 */
int
col_op_reduce_weighted(const col_rel_t *src, col_rel_t *dst)
{
    if (!src || !dst)
        return EINVAL;

    /* Sum all input multiplicities. */
    int64_t total = 0;
    if (src->timestamps) {
        for (uint32_t i = 0; i < src->nrows; i++)
            total += src->timestamps[i].multiplicity;
    } else {
        /* No timestamp tracking: treat each row as multiplicity 1. */
        total = (int64_t)src->nrows;
    }

    /* Allocate timestamp tracking on dst if not already present. */
    if (!dst->timestamps) {
        dst->timestamps
            = (col_delta_timestamp_t *)calloc(1, sizeof(col_delta_timestamp_t));
        if (!dst->timestamps)
            return ENOMEM;
        dst->capacity = (dst->capacity == 0) ? 1 : dst->capacity;
    }

    /* Allocate data buffer for one output row if not already present. */
    if (!dst->data) {
        uint32_t ncols = dst->ncols ? dst->ncols : 1;
        dst->data = (int64_t *)calloc(ncols, sizeof(int64_t));
        if (!dst->data)
            return ENOMEM;
        dst->capacity = 1;
    }

    /* Write the single aggregate row. */
    dst->data[0] = total;
    dst->nrows = 1;

    /* Set output row multiplicity. */
    memset(&dst->timestamps[0], 0, sizeof(col_delta_timestamp_t));
    dst->timestamps[0].multiplicity = total;

    return 0;
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

        /* Phase 3C NOTE: Weighted operation cases (WL_PLAN_OP_JOIN_WEIGHTED,
         * WL_PLAN_OP_REDUCE_WEIGHTED) are not yet present. These functions
         * exist and are tested independently (col_op_join_weighted,
         * col_op_reduce_weighted in columnar_nanoarrow.c). Integration into
         * this switch will occur when the plan generator emits weighted opcodes
         * for Z-set multiplicity evaluation. For now, col_op_join and
         * col_op_reduce dispatch to their base (non-weighted) versions. */
        switch (op->op) {
        case WL_PLAN_OP_VARIABLE:
            rc = col_op_variable(op, stack, sess);
            break;
        case WL_PLAN_OP_MAP:
            rc = col_op_map(op, stack, sess);
            break;
        case WL_PLAN_OP_FILTER:
            rc = col_op_filter(op, stack, sess);
            break;
        case WL_PLAN_OP_JOIN:
            rc = col_op_join(op, stack, sess);
            break;
        case WL_PLAN_OP_ANTIJOIN:
            rc = col_op_antijoin(op, stack, sess);
            break;
        case WL_PLAN_OP_CONCAT:
            rc = col_op_concat(stack, sess);
            break;
        case WL_PLAN_OP_CONSOLIDATE:
            rc = col_op_consolidate(stack, sess);
            break;
        case WL_PLAN_OP_REDUCE:
            rc = col_op_reduce(op, stack, sess);
            break;
        case WL_PLAN_OP_SEMIJOIN:
            rc = col_op_semijoin(op, stack, sess);
            break;
        case WL_PLAN_OP_K_FUSION: {
            uint64_t t0 = now_ns();
            rc = col_op_k_fusion(op, stack, sess);
            COL_SESSION(sess)->kfusion_ns += now_ns() - t0;
            break;
        }
        default:
            break;
        }
        if (rc != 0)
            return rc;
    }
    return 0;
}

/*
 * has_empty_forced_delta:
 * Check if a relation plan would produce empty output because it contains
 * a FORCE_DELTA op whose delta relation is empty or absent.
 *
 * On iteration 0, no deltas exist yet; FORCE_DELTA ops fall back to the
 * full relation (base-case seeding), so we always return false.
 *
 * On iteration > 0, if any FORCE_DELTA VARIABLE or JOIN op references a
 * delta that is empty/absent, the entire plan would produce 0 rows, so
 * we can safely skip evaluation.
 *
 * Returns true if the plan can be skipped (empty forced-delta found).
 */
static bool
has_empty_forced_delta(const wl_plan_relation_t *rp, wl_col_session_t *sess,
                       uint32_t iteration)
{
    if (iteration == 0 && !sess->delta_seeded)
        return false; /* Base case: no deltas exist yet (non-incremental) */

    for (uint32_t oi = 0; oi < rp->op_count; oi++) {
        const wl_plan_op_t *op = &rp->ops[oi];
        if (op->delta_mode != WL_DELTA_FORCE_DELTA)
            continue;

        const char *rel_name = NULL;
        if (op->op == WL_PLAN_OP_VARIABLE)
            rel_name = op->relation_name;
        else if (op->op == WL_PLAN_OP_JOIN || op->op == WL_PLAN_OP_SEMIJOIN)
            rel_name = op->right_relation;

        if (rel_name) {
            char dname[256];
            snprintf(dname, sizeof(dname), "$d$%s", rel_name);
            col_rel_t *d = session_find_rel(sess, dname);
            if (!d || d->nrows == 0)
                return true; /* Found empty forced-delta */
        }
    }
    return false;
}

/* Forward declaration of col_frontier_compute (defined later) */
static col_frontier_t
col_frontier_compute(const col_rel_t *rel);

/*
 * col_eval_stratum:
 * Evaluate one stratum, writing results into session relations.
 * Non-recursive strata are evaluated once.
 * Recursive strata use semi-naive fixed-point iteration.
 *
 * Returns 0 on success, non-zero on error.
 */
static int
col_eval_stratum(const wl_plan_stratum_t *sp, wl_col_session_t *sess,
                 uint32_t stratum_idx)
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
                        col_rel_destroy(result.rel);
                        return ENOMEM;
                    }
                    rc = session_add_rel(sess, result.rel);
                    if (rc != 0) {
                        col_rel_destroy(result.rel);
                        return rc;
                    }
                    result.owned = false;
                } else {
                    col_rel_t *copy = col_rel_new_like(rp->name, result.rel);
                    if (!copy)
                        return ENOMEM;
                    if ((rc = col_rel_append_all(copy, result.rel)) != 0) {
                        col_rel_destroy(copy);
                        return rc;
                    }
                    rc = session_add_rel(sess, copy);
                    if (rc != 0) {
                        col_rel_destroy(copy);
                        return rc;
                    }
                }
            } else {
                /* Append new results to existing relation */
                rc = col_rel_append_all(target, result.rel);
                if (result.owned)
                    col_rel_destroy(result.rel);
                if (rc != 0)
                    return rc;
            }
        }
        col_mat_cache_clear(&sess->mat_cache);
        delta_pool_reset(sess->delta_pool);

        /* Non-recursive stratum frontier: record convergence epoch and iteration.
         * Non-recursive strata always converge at iteration UINT32_MAX (no loop),
         * so store (outer_epoch, UINT32_MAX) to enable epoch-aware skip on next
         * incremental call. Always update both fields so same-epoch skip logic
         * fires correctly when the frontier persists across session_step calls. */
        if (stratum_idx < MAX_STRATA) {
            col_frontier_2d_t f2d = { sess->outer_epoch, UINT32_MAX };
            sess->frontiers[stratum_idx] = f2d;
        }

        /* Non-recursive rule frontiers: mark each rule fully evaluated.
         * UINT32_MAX sentinel matches stratum frontier convention. */
        if (sess->plan) {
            uint32_t rule_base = 0;
            for (uint32_t si = 0; si < stratum_idx; si++)
                rule_base += sess->plan->strata[si].relation_count;
            for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
                uint32_t rule_idx = rule_base + ri;
                if (rule_idx < MAX_RULES) {
                    /* Issue #106: Conservative approach - always reset rule frontiers
                     * for affected strata to (current_epoch, UINT32_MAX) sentinel.
                     * UINT32_MAX prevents premature skip during re-evaluation. */
                    sess->rule_frontiers[rule_idx].outer_epoch
                        = sess->outer_epoch;
                    sess->rule_frontiers[rule_idx].iteration = UINT32_MAX;
                }
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
                col_rel_destroy(empty);
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

    /* Sort pre-existing data in each IDB relation before iterating.
     * Handles the EDB+IDB case: when base facts are pre-loaded into a
     * relation that also appears as an IDB in a recursive rule, the loaded
     * facts may be in insertion order (unsorted).
     * col_op_consolidate_incremental_delta requires rel->data[0..snap) to be
     * sorted; an unsorted prefix causes the 2-pointer merge to miss duplicates,
     * producing spurious output rows. */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (r && r->nrows > 1) {
            uint32_t nc = r->ncols;
            size_t row_bytes = (size_t)nc * sizeof(int64_t);
            QSORT_R_CALL(r->data, r->nrows, row_bytes, &nc, row_cmp_fn);
            r->sorted_nrows = r->nrows;
        }
    }

    /* Initialize recursive stratum frontier to UINT32_MAX (not set sentinel)
     * only on the first evaluation (frontier==0 from calloc). If a prior
     * col_session_insert_incremental call preserved a real convergence frontier,
     * keep it so the per-iteration skip condition fires for iterations beyond
     * the prior convergence point. */
    if (stratum_idx < MAX_STRATA
        && sess->frontiers[stratum_idx].iteration == 0) {
        sess->frontiers[stratum_idx].iteration = UINT32_MAX;
    }

    /* Phase 4 (US-4-004): Compute the base global rule index for this stratum.
     * Rule indices are assigned by enumerating strata in order and relations
     * within each stratum, matching col_compute_affected_rules convention. */
    uint32_t rule_id_base = 0;
    if (sess->plan) {
        for (uint32_t si = 0;
             si < stratum_idx && si < sess->plan->stratum_count; si++) {
            rule_id_base += sess->plan->strata[si].relation_count;
        }
    }
    /* Clamp so rule_id_base + ri never exceeds MAX_RULES - 1 */
    if (rule_id_base >= MAX_RULES)
        rule_id_base = MAX_RULES;

    uint32_t iter;
    col_frontier_t strat_frontier = { 0, 0 };
    for (iter = 0; iter < MAX_ITERATIONS; iter++) {
        /* Publish current iteration so operators can distinguish base case
         * (iter 0: FORCE_DELTA falls back to full) from delta case
         * (iter > 0: FORCE_DELTA with absent delta → empty result). */
        sess->current_iteration = iter;

        /* Phase 3D-Ext-002 (DORMANT): Fine-grained frontier skip infrastructure.
         *
         * STATUS: This optimization is currently unreachable in the single-call evaluation
         * model (session_step, session_snapshot). Designed for future incremental re-evaluation
         * where the frontier persists across multiple calls without being reset.
         *
         * WHY UNREACHABLE (Phase 4): Non-recursive strata reset their frontier to (0, stratum_idx)
         * before recursive strata evaluate. Since each stratum initializes its frontier to iteration=0,
         * the per-stratum skip condition `iter > frontiers[stratum_idx].iteration` is always false
         * when evaluating the first session_step call. Becomes active only when frontier persists
         * across multiple session_step calls (incremental re-evaluation, Phase 4+).
         *
         * INTENDED SEMANTICS (when frontier persists across calls):
         * Skip iteration only if: iter > frontier.iteration AND frontier.stratum < current_stratum.
         * Both conditions must be true to skip safely. This prevents:
         * - Skipping iterations before frontier (data loss)
         * - Skipping stratum 0 (recursion entry point)
         * - Premature termination of recursive stratum evaluation
         *
         * LATENT CORRECTNESS BUG (if activated without semantic fix):
         * Cross-stratum iteration counter mismatch. Variable 'iter' is this stratum's local
         * fixed-point counter. Variable 'frontier.iteration' is from previous stratum's
         * convergence point. These are semantically unrelated values that happen to share
         * the name "iteration". Comparing them across strata is incorrect and could skip
         * needed work in multi-recursive-stratum programs, producing incomplete results.
         *
         * PHASE 4+ WORK (Incremental Evaluation):
         * Before activating this skip, implement one of:
         * 1. Per-stratum frontier tracking (store in array, indexed by stratum_idx)
         * 2. Same-stratum comparison only (only skip if frontier.stratum == stratum_idx)
         * 3. Change continue to break if stratum has converged beyond this point
         * See docs/3d-ext-incremental-eval-roadmap.md for design options.
         *
         * ARCHITECT REVIEW: Conditional approval (a2a42d1fa88a8d650).
         * Dormant status verified. Recommendation: document before activation.
         * See progress.txt Phase 3D-Ext section for full architect findings.
         */
        /* Phase 4: Per-stratum frontier skip condition (DORMANT in this context).
         * When frontier is reset to UINT32_MAX for affected strata, this skip
         * condition is ineffective (iter > UINT32_MAX is always false).
         * Skip logic only activates when frontier persists across multiple
         * incremental snapshots with small delta facts.
         *
         * ENABLED: for unaffected strata, skip iterations beyond frontier.
         * Affected strata (frontier=UINT32_MAX) naturally re-evaluate all iterations. */
        /* US-104-002: 2D frontier skip condition (epoch-aware).
         * Skip only when BOTH conditions hold:
         *   1. Same insertion epoch: frontier was set in this outer_epoch, so
         *      the convergence point is still valid for the current data set.
         *   2. Iteration beyond convergence: iter > frontier.iteration means
         *      this stratum already converged at a lower iteration count.
         * When epochs differ (new insertion cycle), outer_epoch mismatch means
         * the frontier is stale — do NOT skip, always re-evaluate from iter 0. */
        if (stratum_idx < MAX_STRATA) {
            bool same_epoch = (sess->outer_epoch
                               == sess->frontiers[stratum_idx].outer_epoch);
            bool beyond_convergence
                = (iter > sess->frontiers[stratum_idx].iteration);
            if (same_epoch && beyond_convergence) {
                continue; /* Skip: already processed at this iter in same epoch */
            }
        }

        /* Clear per-iteration delta arrangement cache (sequential eval path).
         * K-fusion workers manage their own darr caches independently. */
        col_session_free_delta_arrangements(sess);

        /* Register delta relations from previous iteration into session */
        for (uint32_t ri = 0; ri < nrels; ri++) {
            if (!delta_rels[ri])
                continue;
            char dname[256];
            snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
            session_remove_rel(sess, dname);
            int rc = session_add_rel(sess, delta_rels[ri]);
            if (rc != 0)
                col_rel_destroy(delta_rels[ri]);
            delta_rels[ri] = NULL; /* session now owns it */
        }

        /* Record per-relation row counts (O(1) snapshot — no data copy). */
        uint32_t *snap = (uint32_t *)malloc(nrels * sizeof(uint32_t));
        if (!snap) {
            free(delta_rels);
            return ENOMEM;
        }
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
            snap[ri] = r ? r->nrows : 0; /* O(1) count only, no copy */
        }

        /* Phase 3D: Frontier skip with multiplicities (US-3D-002)
         * Skip iteration if all delta relations have zero net multiplicity.
         * This optimizes away iterations where no new facts can be derived.
         * Condition: sum of all multiplicities in all delta relations == 0. */
        if (iter > 0) { /* Only skip from iteration 1 onward */
            bool all_deltas_net_zero = true;
            for (uint32_t ri = 0; ri < nrels; ri++) {
                char dname[256];
                snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
                col_rel_t *delta = session_find_rel(sess, dname);
                if (!delta || delta->nrows == 0) {
                    /* Empty delta: net multiplicity is zero */
                    continue;
                }
                /* Compute net multiplicity for this delta relation */
                int64_t net_mult = 0;
                for (uint32_t row = 0; row < delta->nrows; row++) {
                    net_mult += delta->timestamps[row].multiplicity;
                }
                if (net_mult != 0) {
                    all_deltas_net_zero = false;
                    break;
                }
            }
            if (all_deltas_net_zero) {
                /* All deltas have zero net multiplicity: skip evaluation */
                free(snap);
                continue;
            }
        }

        /* Stratum-level early exit: if all rules have empty forced deltas,
         * the iteration will produce no new facts. Skip it. (Issue #81) */
        if (iter > 0) {
            bool all_rules_empty = true;
            for (uint32_t ri = 0; ri < nrels; ri++) {
                if (!has_empty_forced_delta(&sp->relations[ri], sess, iter)) {
                    all_rules_empty = false;
                    break;
                }
            }
            if (all_rules_empty) {
                free(snap);
                continue;
            }
        }

        /* Single-pass semi-naive evaluation. VARIABLE prefers delta when it
         * is a strict subset of full (genuine new facts). JOIN propagates
         * the is_delta flag through results and applies right-delta when
         * left is full and a strictly-smaller right delta exists. */
        for (uint32_t ri = 0; ri < nrels; ri++) {
            const wl_plan_relation_t *rp = &sp->relations[ri];

            /* Issue #106 (US-106-003): Rule-level frontier skip with epoch gating.
             * Skip rule evaluation only when BOTH conditions hold:
             * 1. Same outer_epoch (prevents cross-epoch incorrect skips)
             * 2. Iteration > convergence point (rule already processed in this epoch)
             * Across epoch boundaries, outer_epoch mismatch => skip condition false => re-eval. */
            uint32_t rule_id = rule_id_base + ri;
            if (rule_id < MAX_RULES
                && sess->rule_frontiers[rule_id].outer_epoch
                       == sess->outer_epoch
                && iter > sess->rule_frontiers[rule_id].iteration) {
                continue;
            }

            /* Pre-scan skip: if a FORCE_DELTA op references an empty or
             * absent delta (iteration > 0), the plan would produce 0 rows.
             * Skip evaluation entirely to avoid unnecessary work. */
            if (has_empty_forced_delta(rp, sess, iter)) {
                continue;
            }

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

            /* Post-eval skip: if evaluation produced 0 rows, skip the
             * append + consolidate path.  This is a safety net for cases
             * not caught by the pre-scan (e.g. filters that eliminate
             * all rows). */
            if (result.rel && result.rel->nrows == 0) {
                if (result.owned)
                    col_rel_destroy(result.rel);
                continue;
            }

            col_rel_t *target = session_find_rel(sess, rp->name);
            if (!target) {
                col_rel_t *copy;
                if (result.owned) {
                    copy = result.rel;
                    free(copy->name);
                    copy->name = wl_strdup(rp->name);
                    if (!copy->name) {
                        col_rel_destroy(copy);
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
                        col_rel_destroy(copy);
                        free(snap);
                        free(delta_rels);
                        return rc;
                    }
                }
                rc = session_add_rel(sess, copy);
                if (rc != 0) {
                    col_rel_destroy(copy);
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
                        if (result.owned)
                            col_rel_destroy(result.rel);
                        free(snap);
                        free(delta_rels);
                        return rc;
                    }
                }
                rc = col_rel_append_all(target, result.rel);
                if (result.owned)
                    col_rel_destroy(result.rel);
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

        /* Phase 4: Frontier is computed incrementally as deltas are created
         * because delta_rels[ri] may be set to NULL in the next iteration's
         * registration loop. strat_frontier is declared before the iteration loop. */

        /* Consolidate all IDB relations to remove duplicates and compute delta
         * as a byproduct.  snap[ri] marks the boundary between the already-
         * sorted prefix and unsorted new rows appended this iteration. */
        bool any_new = false;
        for (uint32_t ri = 0; ri < nrels; ri++) {
            col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
            if (!r || snap[ri] >= r->nrows) {
                continue; /* no new rows for this relation */
            }

            char dname[256];
            snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
            col_rel_t *delta = col_rel_new_like(dname, r);
            if (!delta) {
                free(snap);
                free(delta_rels);
                return ENOMEM;
            }

            /* Consolidate WITH delta output (no separate merge walk) */
            uint32_t cons_old = snap[ri];
            uint32_t cons_new = r->nrows - cons_old; /* delta count D */
            uint64_t cons_t0 = now_ns();
            int rc2 = col_op_consolidate_incremental_delta(r, snap[ri], delta);
            uint64_t cons_elapsed = now_ns() - cons_t0;
            sess->consolidation_ns += cons_elapsed;
            /* Invalidate arrangements for this relation (data changed). */
            col_session_invalidate_arrangements(&sess->base,
                                                sp->relations[ri].name);
            /* Per-call trace: WL_CONSOLIDATION_LOG=1 prints N/D/time per call */
            if (getenv("WL_CONSOLIDATION_LOG")) {
                fprintf(stderr,
                        "CONS iter=%u stratum=%u rel=%s N=%u D=%u "
                        "time_us=%.1f ratio=%.4f\n",
                        iter, stratum_idx, sp->relations[ri].name, cons_old,
                        cons_new, (double)cons_elapsed / 1000.0,
                        cons_old > 0 ? (double)cons_new / (double)cons_old
                                     : 0.0);
            }
            if (rc2 != 0) {
                col_rel_destroy(delta);
                free(snap);
                free(delta_rels);
                return rc2;
            }

            if (delta->nrows > 0) {
                /* Stamp each new row with its provenance (iteration, stratum).
                 * worker=0 indicates the sequential (non-K-fusion) path. */
                delta->timestamps = (col_delta_timestamp_t *)calloc(
                    delta->nrows, sizeof(col_delta_timestamp_t));
                if (!delta->timestamps) {
                    col_rel_destroy(delta);
                    free(snap);
                    free(delta_rels);
                    return ENOMEM;
                }
                for (uint32_t ti = 0; ti < delta->nrows; ti++) {
                    delta->timestamps[ti].iteration = iter;
                    delta->timestamps[ti].stratum = stratum_idx;
                    /* worker left zero: sequential evaluation path */
                    delta->timestamps[ti].multiplicity = 1;
                }

                /* Phase 4: Enable timestamp tracking on target relation to preserve
                 * provenance through consolidation. This enables frontier computation
                 * to determine which iterations have converged. */
                if (!r->timestamps && r->capacity > 0) {
                    r->timestamps = (col_delta_timestamp_t *)calloc(
                        r->capacity, sizeof(col_delta_timestamp_t));
                    if (!r->timestamps) {
                        free(delta->timestamps);
                        col_rel_destroy(delta);
                        free(snap);
                        free(delta_rels);
                        return ENOMEM;
                    }
                }

                delta_rels[ri] = delta;
                any_new = true;

                /* Phase 4: Compute frontier from this delta immediately.
                 * This must happen before the next iteration's registration loop
                 * sets delta_rels[ri] = NULL.
                 * frontier = MAXIMUM iteration that produced facts. Enables skip
                 * optimization in next session_step: skip iterations <= frontier. */
                col_frontier_t rel_frontier = col_frontier_compute(delta);
                if (rel_frontier.iteration > strat_frontier.iteration
                    || (rel_frontier.iteration == strat_frontier.iteration
                        && rel_frontier.stratum > strat_frontier.stratum)) {
                    strat_frontier = rel_frontier;
                }
            } else {
                col_rel_destroy(delta);
            }
        }

        free(snap);

        delta_pool_reset(sess->delta_pool);
        /* Clear materialization cache between iterations (relation data changed) */
        col_mat_cache_clear(&sess->mat_cache);

        if (!any_new) {
            sess->total_iterations = iter;
            break;
        }
    }
    sess->total_iterations = iter;

    /* Issue #106 (US-106-005): Record per-rule frontier at convergence with epoch.
     * When stratum converges at iteration I, record (outer_epoch, I) for each rule.
     * On next incremental snapshot, skip condition checks epoch match AND iter > I.
     * This preserves fine-grained rule convergence across insertions. */
    for (uint32_t ri = 0; ri < nrels && rule_id_base + ri < MAX_RULES; ri++) {
        uint32_t rule_id = rule_id_base + ri;
        sess->rule_frontiers[rule_id].outer_epoch = sess->outer_epoch;
        sess->rule_frontiers[rule_id].iteration = iter;
    }

    /* Phase 4: Update per-stratum frontier after recursive stratum evaluation.
     * frontier was computed incrementally during consolidation, so just
     * store it in the session. Each stratum independently tracks its
     * convergence frontier for the skip optimization in the next session_step. */
    if (strat_frontier.iteration != UINT32_MAX && stratum_idx < MAX_STRATA) {
        /* Set this stratum's 2D frontier to the convergence point.
         * outer_epoch tracks the insertion epoch; iteration tracks convergence.
         * This enables skipping iterations if frontier persists across
         * session_step calls (incremental evaluation). */
        col_frontier_2d_t f2d = { sess->outer_epoch, strat_frontier.iteration };
        sess->frontiers[stratum_idx] = f2d;
    }

    /* Cleanup all delta relations after frontier has been computed */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        if (delta_rels[ri])
            col_rel_destroy(delta_rels[ri]);
        char dname[256];
        snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
        session_remove_rel(sess, dname);
    }
    free(delta_rels);
    delta_pool_reset(sess->delta_pool);
    col_mat_cache_clear(&sess->mat_cache);

    return 0;
}

/* ======================================================================== */
/* Public Accessors                                                          */
/* ======================================================================== */

/*
 * col_session_get_iteration_count:
 *
 * Return the number of fixed-point iterations performed during the last
 * call to col_eval_stratum.  Returns 0 if no evaluation has occurred yet.
 *
 * @param sess  A wl_session_t* backed by the columnar backend.
 */
uint32_t
col_session_get_iteration_count(wl_session_t *sess)
{
    return COL_SESSION(sess)->total_iterations;
}

/*
 * col_session_get_cache_stats:
 *
 * Return CSE materialization cache hit and miss counts accumulated during
 * the last evaluation.  Both out-parameters are optional (NULL-safe).
 *
 * @param sess    A wl_session_t* backed by the columnar backend.
 * @param out_hits    Set to the number of cache hits (may be NULL).
 * @param out_misses  Set to the number of cache misses (may be NULL).
 */
void
col_session_get_cache_stats(wl_session_t *sess, uint64_t *out_hits,
                            uint64_t *out_misses)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_hits)
        *out_hits = cs->mat_cache.hits;
    if (out_misses)
        *out_misses = cs->mat_cache.misses;
}

/*
 * col_session_get_perf_stats:
 *
 * Return accumulated profiling counters (in nanoseconds) from the last
 * wl_session_snapshot() call.  Counters are reset at the start of each
 * evaluation pass.  Both out-parameters are optional (NULL-safe).
 *
 * @param sess             A wl_session_t* backed by the columnar backend.
 * @param out_consolidation_ns  Time spent in incremental consolidation.
 * @param out_kfusion_ns        Time spent in K-fusion dispatch.
 */
void
col_session_get_perf_stats(wl_session_t *sess, uint64_t *out_consolidation_ns,
                           uint64_t *out_kfusion_ns,
                           uint64_t *out_kfusion_alloc_ns,
                           uint64_t *out_kfusion_dispatch_ns,
                           uint64_t *out_kfusion_merge_ns,
                           uint64_t *out_kfusion_cleanup_ns)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_consolidation_ns)
        *out_consolidation_ns = cs->consolidation_ns;
    if (out_kfusion_ns)
        *out_kfusion_ns = cs->kfusion_ns;
    if (out_kfusion_alloc_ns)
        *out_kfusion_alloc_ns = cs->kfusion_alloc_ns;
    if (out_kfusion_dispatch_ns)
        *out_kfusion_dispatch_ns = cs->kfusion_dispatch_ns;
    if (out_kfusion_merge_ns)
        *out_kfusion_merge_ns = cs->kfusion_merge_ns;
    if (out_kfusion_cleanup_ns)
        *out_kfusion_cleanup_ns = cs->kfusion_cleanup_ns;
}

/*
 * col_frontier_compute:
 *
 * Compute the minimum (iteration, stratum) pair from a relation's timestamps.
 * This represents the frontier: the lowest point that has been fully processed.
 *
 * Returns (0, 0) if:
 *   - The relation is NULL or has no rows
 *   - The relation has no timestamp tracking (timestamps == NULL)
 *   - Timestamp array is empty
 *
 * Otherwise returns min(iteration, stratum) across all rows.
 *
 * @param rel  col_rel_t* to compute frontier from (NULL-safe)
 * @return col_frontier_t with min (iteration, stratum) or (0, 0) if no data
 */
static col_frontier_t
col_frontier_compute(const col_rel_t *rel)
{
    col_frontier_t f = { 0, 0 };

    /* Handle NULL or empty relation */
    if (!rel || rel->nrows == 0 || !rel->timestamps)
        return f;

    /* Initialize frontier to first row's timestamp */
    f.iteration = rel->timestamps[0].iteration;
    f.stratum = rel->timestamps[0].stratum;

    /* Find minimum (iteration, stratum) */
    for (uint32_t i = 1; i < rel->nrows; i++) {
        const col_delta_timestamp_t *ts = &rel->timestamps[i];
        if (ts->iteration < f.iteration
            || (ts->iteration == f.iteration && ts->stratum < f.stratum)) {
            f.iteration = ts->iteration;
            f.stratum = ts->stratum;
        }
    }

    return f;
}

/*
 * col_session_cleanup_old_data:
 *
 * Remove data that is entirely before the frontier (iteration, stratum).
 * Only rows with timestamps <= frontier are removed.
 *
 * This function performs selective cleanup of old delta rows to reduce
 * memory usage. Timestamps before the frontier have already been processed
 * and will not be needed again during evaluation.
 *
 * @param sess     wl_session_t* backed by columnar backend
 * @param frontier Minimum (iteration, stratum) to preserve; data before this
 * can be freed
 */
static void
col_session_cleanup_old_data(wl_session_t *sess, col_frontier_t frontier)
{
    if (!sess)
        return;

    wl_col_session_t *cs = COL_SESSION(sess);

    /* Scan all relations, remove rows with timestamps <= frontier */
    for (uint32_t ri = 0; ri < cs->nrels; ri++) {
        col_rel_t *rel = cs->rels[ri];
        if (!rel || !rel->timestamps || rel->nrows == 0)
            continue;

        /* Find first row that is after frontier */
        uint32_t keep_from = 0;
        for (uint32_t row = 0; row < rel->nrows; row++) {
            const col_delta_timestamp_t *ts = &rel->timestamps[row];
            if (ts->iteration > frontier.iteration
                || (ts->iteration == frontier.iteration
                    && ts->stratum > frontier.stratum)) {
                keep_from = row;
                break;
            }
        }

        /* If all rows are at or before frontier, clear entire relation */
        if (keep_from == rel->nrows) {
            free(rel->timestamps);
            rel->timestamps = NULL;
            rel->nrows = 0;
            rel->capacity = 0;
            free(rel->data);
            rel->data = NULL;
            /* Invalidate arrangements (data changed) */
            col_session_invalidate_arrangements(sess, rel->name);
        } else if (keep_from > 0) {
            /* Shift rows forward and update timestamps */
            uint32_t new_nrows = rel->nrows - keep_from;
            size_t row_bytes = (size_t)rel->ncols * sizeof(int64_t);

            memmove(rel->data, rel->data + (size_t)keep_from * rel->ncols,
                    new_nrows * row_bytes);
            memmove(rel->timestamps, rel->timestamps + keep_from,
                    new_nrows * sizeof(col_delta_timestamp_t));

            rel->nrows = new_nrows;
            /* Invalidate arrangements (data changed) */
            col_session_invalidate_arrangements(sess, rel->name);
        }
    }
}

/* ======================================================================== */
/* Arrangement Accessors (Phase 3C)                                         */
/* ======================================================================== */

col_arrangement_t *
col_session_get_arrangement(wl_session_t *sess, const char *rel_name,
                            const uint32_t *key_cols, uint32_t key_count)
{
    if (!sess || !rel_name || !key_cols || key_count == 0)
        return NULL;

    wl_col_session_t *cs = COL_SESSION(sess);

    /* Look up the relation. */
    col_rel_t *rel = NULL;
    for (uint32_t i = 0; i < cs->nrels; i++) {
        if (cs->rels[i] && cs->rels[i]->name
            && strcmp(cs->rels[i]->name, rel_name) == 0) {
            rel = cs->rels[i];
            break;
        }
    }
    if (!rel)
        return NULL;

    /* Search registry for matching (rel_name, key_cols) entry. */
    for (uint32_t i = 0; i < cs->arr_count; i++) {
        col_arr_entry_t *e = &cs->arr_entries[i];
        if (e->key_count != key_count)
            continue;
        if (strcmp(e->rel_name, rel_name) != 0)
            continue;
        bool match = true;
        for (uint32_t k = 0; k < key_count; k++) {
            if (e->key_cols[k] != key_cols[k]) {
                match = false;
                break;
            }
        }
        if (!match)
            continue;

        /* Found: update if stale. */
        if (e->arr.indexed_rows == 0 && rel->nrows > 0) {
            if (arr_build_full(&e->arr, rel) != 0)
                return NULL;
        } else if (e->arr.indexed_rows < rel->nrows) {
            uint32_t old = e->arr.indexed_rows;
            if (arr_update_incremental(&e->arr, rel, old) != 0)
                return NULL;
        }
        return &e->arr;
    }

    /* Not found: grow registry and create new entry. */
    if (cs->arr_count >= cs->arr_cap) {
        uint32_t new_cap = cs->arr_cap ? cs->arr_cap * 2u : 8u;
        col_arr_entry_t *ne = (col_arr_entry_t *)realloc(
            cs->arr_entries, new_cap * sizeof(col_arr_entry_t));
        if (!ne)
            return NULL;
        cs->arr_entries = ne;
        cs->arr_cap = new_cap;
    }

    col_arr_entry_t *e = &cs->arr_entries[cs->arr_count];
    memset(e, 0, sizeof(*e));

    e->rel_name = wl_strdup(rel_name);
    if (!e->rel_name)
        return NULL;

    e->key_cols = (uint32_t *)malloc(key_count * sizeof(uint32_t));
    if (!e->key_cols) {
        free(e->rel_name);
        e->rel_name = NULL;
        return NULL;
    }
    memcpy(e->key_cols, key_cols, key_count * sizeof(uint32_t));
    e->key_count = key_count;
    e->arr.key_cols = e->key_cols; /* shared view; key_cols owned by entry */
    e->arr.key_count = key_count;
    cs->arr_count++;

    /* Initial build. */
    if (rel->nrows > 0 && arr_build_full(&e->arr, rel) != 0) {
        /* Roll back the entry we just added. */
        cs->arr_count--;
        free(e->rel_name);
        free(e->key_cols);
        memset(e, 0, sizeof(*e));
        return NULL;
    }
    return &e->arr;
}

uint32_t
col_arrangement_find_first(const col_arrangement_t *arr,
                           const int64_t *rel_data, uint32_t rel_ncols,
                           const int64_t *key_row)
{
    if (!arr || !rel_data || !key_row || arr->nbuckets == 0)
        return UINT32_MAX;

    uint32_t bucket
        = arr_hash_key(key_row, arr->key_cols, arr->key_count, arr->nbuckets);
    uint32_t row = arr->ht_head[bucket];
    while (row != UINT32_MAX) {
        const int64_t *rp = rel_data + (size_t)row * rel_ncols;
        bool match = true;
        for (uint32_t k = 0; k < arr->key_count; k++) {
            if (rp[arr->key_cols[k]] != key_row[arr->key_cols[k]]) {
                match = false;
                break;
            }
        }
        if (match)
            return row;
        row = arr->ht_next[row];
    }
    return UINT32_MAX;
}

uint32_t
col_arrangement_find_next(const col_arrangement_t *arr, uint32_t row_idx)
{
    if (!arr || row_idx >= arr->ht_cap)
        return UINT32_MAX;
    return arr->ht_next[row_idx];
}

void
col_session_invalidate_arrangements(wl_session_t *sess, const char *rel_name)
{
    if (!sess || !rel_name)
        return;
    wl_col_session_t *cs = COL_SESSION(sess);
    for (uint32_t i = 0; i < cs->arr_count; i++) {
        if (strcmp(cs->arr_entries[i].rel_name, rel_name) == 0)
            cs->arr_entries[i].arr.indexed_rows = 0; /* force full rebuild */
    }
}

/* ======================================================================== */
/* Delta Arrangement Cache (Phase 3C-001-Ext)                               */
/* ======================================================================== */

/*
 * col_session_free_delta_arrangements:
 *
 * Free all entries in the delta arrangement cache and reset the cache.
 * Called at the start of each semi-naive iteration (sequential path) and
 * by col_op_k_fusion after each dispatch to clean up worker copies.
 *
 * Safe to call on a zeroed session (darr_count == 0, darr_entries == NULL).
 */
static void
col_session_free_delta_arrangements(wl_col_session_t *cs)
{
    for (uint32_t i = 0; i < cs->darr_count; i++) {
        col_arr_entry_t *e = &cs->darr_entries[i];
        free(e->rel_name);
        free(e->key_cols);
        arr_free_contents(&e->arr);
    }
    free(cs->darr_entries);
    cs->darr_entries = NULL;
    cs->darr_count = 0;
    cs->darr_cap = 0;
}

/*
 * col_session_get_delta_arrangement:
 *
 * Return (or lazily create) a delta arrangement for `delta_rel` keyed on
 * `key_cols[0..key_count)`.  Stored in cs->darr_entries (the per-worker
 * delta cache) and keyed by (rel_name, key_cols[]).
 *
 * Unlike full arrangements (which persist across iterations for EDB
 * relations), delta arrangements are rebuilt from scratch if stale
 * (indexed_rows != delta_rel->nrows).  This handles the case where the
 * same delta relation grows between iterations.
 *
 * Returns NULL on allocation failure or if key_count == 0.
 */
static col_arrangement_t *
col_session_get_delta_arrangement(wl_col_session_t *cs, const char *rel_name,
                                  const col_rel_t *delta_rel,
                                  const uint32_t *key_cols, uint32_t key_count)
{
    if (!cs || !rel_name || !delta_rel || !key_cols || key_count == 0)
        return NULL;

    /* Search existing cache entries. */
    for (uint32_t i = 0; i < cs->darr_count; i++) {
        col_arr_entry_t *e = &cs->darr_entries[i];
        if (e->key_count != key_count)
            continue;
        if (strcmp(e->rel_name, rel_name) != 0)
            continue;
        bool match = true;
        for (uint32_t k = 0; k < key_count; k++) {
            if (e->key_cols[k] != key_cols[k]) {
                match = false;
                break;
            }
        }
        if (!match)
            continue;
        /* Found: rebuild if stale (delta changed size). */
        if (e->arr.indexed_rows != delta_rel->nrows) {
            arr_free_contents(&e->arr);
            if (delta_rel->nrows > 0 && arr_build_full(&e->arr, delta_rel) != 0)
                return NULL;
        }
        return &e->arr;
    }

    /* Not found: grow cache and create new entry. */
    if (cs->darr_count >= cs->darr_cap) {
        uint32_t new_cap = cs->darr_cap ? cs->darr_cap * 2u : 4u;
        col_arr_entry_t *ne = (col_arr_entry_t *)realloc(
            cs->darr_entries, new_cap * sizeof(col_arr_entry_t));
        if (!ne)
            return NULL;
        cs->darr_entries = ne;
        cs->darr_cap = new_cap;
    }

    col_arr_entry_t *e = &cs->darr_entries[cs->darr_count];
    memset(e, 0, sizeof(*e));

    e->rel_name = wl_strdup(rel_name);
    if (!e->rel_name)
        return NULL;

    e->key_cols = (uint32_t *)malloc(key_count * sizeof(uint32_t));
    if (!e->key_cols) {
        free(e->rel_name);
        e->rel_name = NULL;
        return NULL;
    }
    memcpy(e->key_cols, key_cols, key_count * sizeof(uint32_t));
    e->key_count = key_count;
    e->arr.key_cols = e->key_cols; /* shared view; owned by entry */
    e->arr.key_count = key_count;
    cs->darr_count++;

    /* Initial build. */
    if (delta_rel->nrows > 0 && arr_build_full(&e->arr, delta_rel) != 0) {
        cs->darr_count--;
        free(e->rel_name);
        free(e->key_cols);
        memset(e, 0, sizeof(*e));
        return NULL;
    }
    return &e->arr;
}

/*
 * col_session_get_darr_count:
 *
 * Return the number of delta arrangement cache entries in the session.
 * Expected to be 0 on the main session after K-fusion evaluation
 * (delta caches are per-worker and freed after each dispatch).
 *
 * Used by tests to verify per-worker isolation invariant.
 */
uint32_t
col_session_get_darr_count(wl_session_t *sess)
{
    if (!sess)
        return 0;
    return COL_SESSION(sess)->darr_count;
}

/*
 * col_session_get_frontier:
 *
 * Copy frontiers[stratum_idx] into *out_frontier.
 * Returns EINVAL for NULL args or out-of-range stratum_idx.
 */
int
col_session_get_frontier(wl_session_t *session, uint32_t stratum_idx,
                         col_frontier_2d_t *out_frontier)
{
    if (!session || !out_frontier || stratum_idx >= MAX_STRATA)
        return EINVAL;
    *out_frontier = COL_SESSION(session)->frontiers[stratum_idx];
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
 * @param num_workers: Thread pool size for parallel K-fusion. When > 1,
 *                     creates a workqueue at session init. When 1, K-fusion
 *                     evaluates copies sequentially (no thread overhead).
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
    if (!plan || !out)
        return EINVAL;

    wl_col_session_t *sess
        = (wl_col_session_t *)calloc(1, sizeof(wl_col_session_t));
    if (!sess)
        return ENOMEM;

    sess->plan = plan;
    sess->num_workers = num_workers > 0 ? num_workers : 1;
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

    /* Create workqueue for parallel K-fusion when num_workers > 1.
     * Single-threaded mode (num_workers=1) leaves wq=NULL; K-fusion
     * evaluates copies sequentially with no thread overhead. (Issue #99) */
    if (sess->num_workers > 1) {
        sess->wq = wl_workqueue_create(sess->num_workers);
        if (!sess->wq) {
            wl_arena_free(sess->eval_arena);
            free(sess->rels);
            free(sess);
            return ENOMEM;
        }
    }

    /* Create delta pool for per-iteration temporaries.
     * Slab: 256 relations (cover ~20 rules x 5 ops + headroom)
     * Arena: 64MB initial (for row data buffers) */
    sess->delta_pool
        = delta_pool_create(256, sizeof(col_rel_t), 64 * 1024 * 1024);
    if (!sess->delta_pool) {
        /* Non-fatal: pool allocation failed, fall back to malloc */
    }

    /* Pre-register EDB relations (ncols determined at first insert) */
    for (uint32_t i = 0; i < plan->edb_count; i++) {
        col_rel_t *r = NULL;
        int rc = col_rel_alloc(&r, plan->edb_relations[i]);
        if (rc != 0)
            goto oom;
        rc = session_add_rel(sess, r);
        if (rc != 0) {
            col_rel_destroy(r);
            goto oom;
        }
    }

    /* Issue #105: Populate stratum_is_monotone from plan.
     * Copy monotone property from each stratum in the plan.
     * Conservative default (all false from calloc) is already set,
     * so only copy if strata exist. */
    for (uint32_t si = 0; si < plan->stratum_count && si < MAX_STRATA; si++) {
        sess->stratum_is_monotone[si] = plan->strata[si].is_monotone;
    }

    /* Issue #103: Initialize 2D frontier epoch tracking.
     * outer_epoch is initialized to 0 by calloc (line 4673) and incremented
     * before each EDB insertion via col_session_insert_incremental. This
     * distinguishes different insertion epochs for 2D frontier (epoch, iteration)
     * pairs to prevent incorrect skip-condition evaluation across epochs. */
    /* outer_epoch = 0; */ /* Already zeroed by calloc */

    *out = &sess->base;
    return 0;

oom:
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_free_contents(sess->rels[i]);
        free(sess->rels[i]);
    }
    free(sess->rels);
    wl_workqueue_destroy(sess->wq); /* NULL-safe */
    wl_arena_free(sess->eval_arena);
    delta_pool_destroy(sess->delta_pool); /* NULL-safe */
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
    col_mat_cache_clear(&sess->mat_cache);
    wl_workqueue_destroy(sess->wq);
    /* Free arrangement registry (Phase 3C) */
    for (uint32_t i = 0; i < sess->arr_count; i++) {
        free(sess->arr_entries[i].rel_name);
        free(sess->arr_entries[i].key_cols);
        arr_free_contents(&sess->arr_entries[i].arr);
    }
    free(sess->arr_entries);
    col_session_free_delta_arrangements(sess);
    delta_pool_destroy(sess->delta_pool);
    free(sess);
}

int
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

    /* Enable incremental re-evaluation mode: mark this relation as the
     * insertion point for affected stratum detection. This activates
     * frontier persistence in col_session_snapshot, enabling the per-iteration
     * skip condition to reduce iterations on subsequent snapshots. */
    COL_SESSION(session)->last_inserted_relation = relation;

    return 0;
}

/*
 * col_session_insert_incremental: Append facts to a session WITHOUT resetting
 * the per-stratum frontier.
 *
 * Unlike col_session_insert(), this function preserves frontier[] state so
 * that a subsequent col_session_step() call can perform incremental
 * re-evaluation: only strata whose frontier has not yet converged past the
 * current iteration are evaluated.
 *
 * Facts are appended to the existing relation; existing rows are kept.
 * Schema is lazily initialised on the first call (same as col_session_insert).
 *
 * @param session:  Active wl_session_t created by col_session_create
 * @param relation: Name of the EDB relation to append to
 * @param data:     Row-major int64_t array, num_rows * num_cols elements
 * @param num_rows: Number of rows to append (0 is a no-op and returns 0)
 * @param num_cols: Number of columns per row
 * @return 0 on success, EINVAL on bad args, ENOENT if relation unknown,
 *         ENOMEM on allocation failure
 */
int
col_session_insert_incremental(wl_session_t *session, const char *relation,
                               const int64_t *data, uint32_t num_rows,
                               uint32_t num_cols)
{
    if (!session || !relation || !data)
        return EINVAL;

    if (num_rows == 0)
        return 0; /* true no-op */

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

    /* Append rows; frontier[] is intentionally NOT modified */
    for (uint32_t i = 0; i < num_rows; i++) {
        int rc = col_rel_append_row(r, data + (size_t)i * num_cols);
        if (rc != 0)
            return rc;
    }

    /* Invalidate arrangement caches for the modified relation so subsequent
     * re-evaluation rebuilds hash indices with the new rows (issue #92). */
    col_session_invalidate_arrangements(session, relation);

    /* Issue #103: Increment outer_epoch to mark a new insertion epoch.
     * This epoch counter distinguishes different insertion phases for 2D frontier
     * tracking: (outer_epoch, iteration) pairs ensure iterations are skipped only
     * within the same epoch. Wrapping at UINT32_MAX is acceptable (continues
     * distinguishing epochs across multiple insertions). */
    wl_col_session_t *sess = COL_SESSION(session);
    sess->outer_epoch++;

    /* Record the inserted relation so col_session_step can skip unaffected
     * strata (Phase 4 affected-stratum skip optimization). */
    sess->last_inserted_relation = relation;
    return 0;
}

/* Forward declaration for col_session_remove_incremental */
static int
col_session_remove_incremental(wl_session_t *session, const char *relation,
                               const int64_t *data, uint32_t num_rows,
                               uint32_t num_cols);

static int
col_session_remove(wl_session_t *session, const char *relation,
                   const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    if (!session || !relation || !data)
        return EINVAL;

    wl_col_session_t *sess = COL_SESSION(session);
    if (sess->delta_cb != NULL)
        return col_session_remove_incremental(session, relation, data, num_rows,
                                              num_cols);

    col_rel_t *r = session_find_rel(sess, relation);
    if (!r)
        return ENOENT;
    if (r->ncols == 0)
        return 0; /* uninitialized schema = nothing to remove */
    if (r->ncols != num_cols)
        return EINVAL;

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
 * col_session_remove_incremental: Remove rows and pre-seed retraction deltas
 *
 * (Issue #158) Semi-naive delta retraction for non-recursive strata.
 * When a delta callback is registered, this function:
 *   1. Creates $r$<name> relation from removed rows
 *   2. Registers it as a session relation (for VARIABLE ops to consume)
 *   3. Removes rows from the EDB using existing compact logic
 *   4. Records the removal for affected-stratum calculation
 *
 * The $r$<name> relation is used during the next session_step to seed
 * the retraction evaluation, enabling delta-only propagation.
 */
static int
col_session_remove_incremental(wl_session_t *session, const char *relation,
                               const int64_t *data, uint32_t num_rows,
                               uint32_t num_cols)
{
    if (!session || !relation || !data)
        return EINVAL;

    wl_col_session_t *sess = COL_SESSION(session);

    /* Find EDB relation */
    col_rel_t *r = session_find_rel(sess, relation);
    if (!r)
        return ENOENT;
    if (r->ncols == 0)
        return 0; /* uninitialized schema = nothing to remove */
    if (r->ncols != num_cols)
        return EINVAL;

    /* Allocate $r$<name> delta relation to collect removed rows */
    char rname[256];
    snprintf(rname, sizeof(rname), "$r$%s", relation);

    col_rel_t *rdelta = col_rel_new_auto(rname, num_cols);
    if (!rdelta)
        return ENOMEM;

    /* Append each removed row to the delta relation.
     * We need to track which rows are actually being removed from the EDB,
     * then add them to rdelta. */
    int rc = 0;
    for (uint32_t di = 0; di < num_rows; di++) {
        const int64_t *del = data + (size_t)di * num_cols;
        /* Check if this row exists in EDB; if so, append to rdelta */
        for (uint32_t ri = 0; ri < r->nrows; ri++) {
            const int64_t *row = r->data + (size_t)ri * num_cols;
            if (memcmp(row, del, sizeof(int64_t) * num_cols) == 0) {
                /* Found matching row; add to retraction delta */
                rc = col_rel_append_row(rdelta, del);
                if (rc != 0) {
                    col_rel_destroy(rdelta);
                    return rc;
                }
                break; /* Only one copy per removal request */
            }
        }
    }

    /* Register $r$<name> in session (replacing any prior) */
    session_remove_rel(sess, rname);
    rc = session_add_rel(sess, rdelta);
    if (rc != 0) {
        col_rel_destroy(rdelta);
        return rc;
    }

    /* Remove rows from the EDB using existing compact logic */
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
                goto next_del_incr;
            }
        }
        r->nrows = out_r;
    next_del_incr:;
    }

    /* Clamp base_nrows to current row count */
    if (r->base_nrows > r->nrows)
        r->base_nrows = r->nrows;

    /* Mark removal for affected-stratum calculation */
    sess->last_removed_relation = relation;
    sess->outer_epoch++;

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
col_idb_consolidate(col_rel_t *r, wl_col_session_t *sess)
{
    eval_stack_t stk;
    eval_stack_init(&stk);
    int rc = eval_stack_push(&stk, r, false); /* borrowed */
    if (rc != 0)
        return rc;
    col_op_consolidate(&stk, sess);
    if (stk.top > 0) {
        eval_entry_t ce = eval_stack_pop(&stk);
        if (ce.owned && ce.rel != r) {
            free(r->data);
            r->data = ce.rel->data;
            r->nrows = ce.rel->nrows;
            r->capacity = ce.rel->capacity;
            ce.rel->data = NULL;
            col_rel_destroy(ce.rel);
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
col_stratum_step_with_delta(const wl_plan_stratum_t *sp, wl_col_session_t *sess,
                            uint32_t stratum_idx)
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
        if (!r || r->ncols == 0)
            continue;
        /* Snapshot even if empty: needed to detect retractions via set-diff */
        if (r->nrows > 0) {
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
        } else {
            /* Relation is empty, but mark it so we remember it existed */
            prev_nrows[ri] = 0;
            prev_ncols[ri] = r->ncols;
        }
    }

    /* Step 1b: Clear IDB relations before re-evaluation to enable retraction
     * detection */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r)
            continue;
        r->nrows = 0; /* Clear the relation for fresh derivation */
    }

    /* Step 2: evaluate stratum (appends new rows to IDB relations) */
    int rc = col_eval_stratum(sp, sess, stratum_idx);
    if (rc != 0)
        goto cleanup;

    /* Steps 3-4: consolidate each IDB relation, fire callbacks for new rows */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r)
            continue;

        /* Consolidate: sort + dedup so binary search is valid */
        rc = col_idb_consolidate(r, sess);
        if (rc != 0)
            goto cleanup;

        uint32_t ncols = r->ncols;

        /* Fire delta_cb(+1) for rows not present in prev sorted state */
        if (r->nrows > 0) {
            for (uint32_t row = 0; row < r->nrows; row++) {
                const int64_t *rowp = r->data + (size_t)row * ncols;
                if (!col_row_in_sorted(prev_data[ri], prev_nrows[ri], ncols,
                                       rowp)) {
                    sess->delta_cb(r->name, rowp, ncols, +1, sess->delta_data);
                }
            }
        }

        /* Fire delta_cb(-1) for rows present in prev sorted state but not in new
         */
        if (prev_nrows[ri] > 0) {
            for (uint32_t row = 0; row < prev_nrows[ri]; row++) {
                const int64_t *rowp
                    = prev_data[ri] + (size_t)row * prev_ncols[ri];
                if (!col_row_in_sorted(r->data, r->nrows, prev_ncols[ri],
                                       rowp)) {
                    sess->delta_cb(r->name, rowp, prev_ncols[ri], -1,
                                   sess->delta_data);
                }
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

/* Forward declarations for helper functions used in col_session_step */
static bool
stratum_has_preseeded_delta(const wl_plan_stratum_t *sp,
                            wl_col_session_t *sess);
static uint32_t
rule_index_to_stratum_index(const wl_plan_t *plan, uint32_t rule_id);

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

    /* Compute affected strata bitmask (Phase 4 incremental skip).
     * When last_inserted_relation is set (incremental path), only evaluate
     * strata that transitively depend on the inserted relation.  When NULL
     * (regular step), UINT64_MAX means all strata are evaluated. */
    uint64_t affected_mask = UINT64_MAX;
    if (sess->last_inserted_relation != NULL) {
        affected_mask = col_compute_affected_strata(
            session, sess->last_inserted_relation);
    }

    /* Issue #106 (US-106-004): Reset rule frontiers with stratum context awareness.
     * col_session_step is for delta callback mode (no pre-seeded deltas).
     * Always reset affected rules to force re-evaluation.
     * Selective reset based on pre-seeded delta is only in col_session_snapshot.
     *
     * @see col_session_snapshot for selective rule frontier reset (Issue #107) */
    if (affected_mask == UINT64_MAX) {
        /* Full evaluation (non-incremental): reset all rules to (current_epoch, UINT32_MAX)
         * sentinel. Prevents premature skip across different evaluation contexts. */
        for (uint32_t ri = 0; ri < MAX_RULES; ri++) {
            sess->rule_frontiers[ri].outer_epoch = sess->outer_epoch;
            sess->rule_frontiers[ri].iteration = UINT32_MAX;
        }
    } else {
        /* Incremental (delta callback mode): reset affected rules to (current_epoch, UINT32_MAX).
         * No pre-seeded deltas in this path, so reset unconditionally. */
        for (uint32_t si = 0; si < plan->stratum_count; si++) {
            if ((affected_mask & ((uint64_t)1 << si)) != 0) {
                uint32_t rule_base = 0;
                for (uint32_t j = 0; j < si; j++)
                    rule_base += plan->strata[j].relation_count;
                for (uint32_t ri = 0; ri < plan->strata[si].relation_count;
                     ri++) {
                    uint32_t rule_id = rule_base + ri;
                    if (rule_id < MAX_RULES) {
                        sess->rule_frontiers[rule_id].outer_epoch
                            = sess->outer_epoch;
                        sess->rule_frontiers[rule_id].iteration = UINT32_MAX;
                    }
                }
            }
        }
    }

    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        /* Skip strata not affected by the last incremental insertion */
        if ((affected_mask & ((uint64_t)1 << si)) == 0)
            continue;

        const wl_plan_stratum_t *sp = &plan->strata[si];
        int rc = sess->delta_cb ? col_stratum_step_with_delta(sp, sess, si)
                                : col_eval_stratum(sp, sess, si);
        if (rc != 0)
            return rc;
        /* Reset arena after stratum evaluation to free temporaries */
        if (sess->eval_arena)
            wl_arena_reset(sess->eval_arena);
    }

    /* Reset after successful eval so next plain session_step runs all strata */
    sess->last_inserted_relation = NULL;
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
 * stratum_has_preseeded_delta: Check if stratum has pre-seeded EDB delta relations
 *
 * Returns true if any relation in the stratum has a pre-seeded EDB delta ($d$<name>)
 * with nrows > 0 already registered in the session.
 *
 * Pre-seeded deltas are created at lines 5213-5235 in col_session_snapshot before
 * frontier reset. This function checks the deltas are available (nrows > 0).
 *
 * Issue #102: Selective frontier reset. Only reset frontier if stratum has
 * pre-seeded delta, preserving frontier skip for transitively-affected IDB strata.
 *
 * @param sp: wl_plan_stratum_t* for the stratum to check
 * @param sess: wl_col_session_t* session containing the relations
 * @return true if any pre-seeded delta exists with nrows > 0, false otherwise
 */
static bool
stratum_has_preseeded_delta(const wl_plan_stratum_t *sp, wl_col_session_t *sess)
{
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        char dname[256];
        snprintf(dname, sizeof(dname), "$d$%s", sp->relations[ri].name);
        col_rel_t *delta = session_find_rel(sess, dname);
        if (delta && delta->nrows > 0)
            return true;
    }
    return false;
}

/*
 * rule_index_to_stratum_index: Map a flat rule index to its stratum index
 *
 * Rules are laid out contiguously across strata in plan order. Stratum 0
 * owns rule indices [0, relation_count_0), stratum 1 owns
 * [relation_count_0, relation_count_0 + relation_count_1), and so on.
 * This function walks the strata, accumulating a running rule offset, and
 * returns the stratum whose window contains rule_id.
 *
 * Issue #107: Selective rule frontier reset uses this mapping to check
 * if a rule's stratum has pre-seeded delta before resetting the rule's frontier.
 *
 * @param plan:    Execution plan containing the strata array.
 * @param rule_id: Flat (zero-based) rule index to look up.
 * @return Stratum index that owns rule_id, or UINT32_MAX if rule_id is
 *         out of range (>= total rule count across all strata).
 */
static uint32_t
rule_index_to_stratum_index(const wl_plan_t *plan, uint32_t rule_id)
{
    uint32_t offset = 0;
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        uint32_t next_offset = offset + plan->strata[si].relation_count;
        if (rule_id < next_offset)
            return si;
        offset = next_offset;
    }
    return UINT32_MAX;
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

    /* Reset profiling counters for this evaluation pass */
    sess->consolidation_ns = 0;
    sess->kfusion_ns = 0;
    sess->kfusion_alloc_ns = 0;
    sess->kfusion_dispatch_ns = 0;
    sess->kfusion_merge_ns = 0;
    sess->kfusion_cleanup_ns = 0;

    /* Phase 4 incremental skip: when last_inserted_relation is set, only
     * re-evaluate strata that transitively depend on the inserted relation.
     * On the first snapshot (total_iterations == 0), always evaluate all strata
     * to establish the baseline. */
    uint64_t affected_mask = UINT64_MAX;
    if (sess->last_inserted_relation != NULL && sess->total_iterations > 0) {
        affected_mask = col_compute_affected_strata(
            session, sess->last_inserted_relation);

        /* Issue #83: Pre-seed EDB delta relations for delta-only propagation.
         * For each relation with nrows > base_nrows, create a $d$<name> delta
         * containing only the new rows. This allows FORCE_DELTA at iteration 0
         * to use the delta instead of the full relation, avoiding full
         * re-derivation of existing IDB tuples. */
        for (uint32_t i = 0; i < sess->nrels; i++) {
            col_rel_t *r = sess->rels[i];
            if (!r || r->base_nrows == 0 || r->nrows <= r->base_nrows)
                continue;
            /* Create delta relation with rows[base_nrows..nrows) */
            char dname[256];
            snprintf(dname, sizeof(dname), "$d$%s", r->name);
            uint32_t delta_nrows = r->nrows - r->base_nrows;
            col_rel_t *delta = col_rel_new_auto(dname, r->ncols);
            if (!delta)
                continue; /* best-effort; falls back to full eval */
            for (uint32_t row = 0; row < delta_nrows; row++) {
                col_rel_append_row(
                    delta, r->data + (size_t)(r->base_nrows + row) * r->ncols);
            }
            session_remove_rel(sess, dname);
            session_add_rel(sess, delta);
        }
        sess->delta_seeded = true;
    }

    /* For affected strata, selectively reset the per-stratum frontier to UINT32_MAX
     * (not-set sentinel) based on pre-seeded EDB delta presence.
     * UINT32_MAX ensures `iter > UINT32_MAX` is always false, forcing full
     * re-evaluation of strata with pre-seeded deltas.
     *
     * Issue #107: Selective frontier reset based on pre-seeded delta check.
     * Reset frontiers ONLY for strata that have pre-seeded EDB deltas.
     * Preserve frontiers for transitively-affected strata (no direct EDB delta).
     *
     * Safety: Transitively-affected strata receive new facts from upstream via
     * delta propagation, but convergence still occurs within previous frontier
     * bounds. Semi-naive evaluation processes deltas incrementally: iteration i
     * only derives from deltas at iteration i-1. If a stratum converged at
     * iteration F (no new facts at F+1+), subsequent upstream facts flow through
     * iterations 0..F, unlikely to require F+1+ unless graph topology changes.
     * Test coverage (test_delta_propagation test 3) validates correctness for
     * cyclic multi-iteration patterns. CSPA benchmark confirms safety. */
    if (affected_mask != UINT64_MAX) {
        for (uint32_t si = 0; si < plan->stratum_count && si < MAX_STRATA;
             si++) {
            if ((affected_mask & ((uint64_t)1 << si)) != 0) {
                /* Issue #107: Selective rule frontier reset based on pre-seeded delta presence.
                 * Reset frontier for strata that have pre-seeded EDB deltas.
                 * Preserve frontier for transitively-affected strata (no direct EDB delta).
                 *
                 * Safety: Transitively-affected strata receive new facts from upstream
                 * strata, but in the presence of pre-seeded deltas, fact propagation
                 * still converges within previous frontier bounds. The pre-seeded delta
                 * check already limits EDB propagation (delta from [base_nrows, nrows)).
                 *
                 * Test coverage: test_delta_propagation validates cyclic correctness. */
                if (stratum_has_preseeded_delta(&plan->strata[si], sess)) {
                    sess->frontiers[si].outer_epoch = sess->outer_epoch;
                    sess->frontiers[si].iteration = UINT32_MAX;
                }
                /* Else: stratum affected but no pre-seeded delta → KEEP frontier */
            }
        }
        /* Phase 4 (US-4-004) + Issue #107: Selective rule frontier reset.
         * Use col_compute_affected_rules bitmask to identify rules needing
         * re-evaluation. For each affected rule, check if its stratum has
         * pre-seeded EDB delta before resetting the frontier.
         *
         * Reset when:
         *   1. Rule is affected (bit set in affected_rules)
         *   2. Rule's stratum is affected (bit set in affected_mask)
         *   3. Stratum HAS pre-seeded EDB delta
         *
         * Preserve when:
         *   1. Rule's stratum affected but NO pre-seeded delta (transitively affected only)
         *   2. Frontier skip can still fire for iterations beyond previous convergence point
         *
         * Performance: Frontier skip on transitively-affected strata reduces iterations
         * for IDB-only derivations, improving speedup from frontier skip optimization. */
        uint64_t affected_rules
            = col_compute_affected_rules(session, sess->last_inserted_relation);
        for (uint32_t ri = 0; ri < MAX_RULES; ri++) {
            if ((affected_rules & ((uint64_t)1 << ri)) == 0)
                continue;
            uint32_t si = rule_index_to_stratum_index(plan, ri);
            if (si == UINT32_MAX)
                continue;
            if ((affected_mask & ((uint64_t)1 << si)) == 0)
                continue;
            if (stratum_has_preseeded_delta(&plan->strata[si], sess)) {
                sess->rule_frontiers[ri].outer_epoch = sess->outer_epoch;
                sess->rule_frontiers[ri].iteration = UINT32_MAX;
            }
            /* Else: rule's stratum affected but no pre-seeded delta → KEEP frontier */
        }
    } else {
        /* Full re-evaluation (non-incremental call): reset ALL rule frontiers
         * to (current_epoch, UINT32_MAX) sentinel. Prevents premature skip. */
        for (uint32_t ri = 0; ri < MAX_RULES; ri++) {
            sess->rule_frontiers[ri].outer_epoch = sess->outer_epoch;
            sess->rule_frontiers[ri].iteration = UINT32_MAX;
        }
    }

    /* Execute strata in order, skipping unaffected ones */
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        if ((affected_mask & ((uint64_t)1 << si)) == 0)
            continue;
        int rc = col_eval_stratum(&plan->strata[si], sess, si);
        if (rc != 0)
            return rc;
        if (sess->eval_arena)
            wl_arena_reset(sess->eval_arena);
    }

    /* Reset after successful eval so next plain snapshot runs all strata */
    sess->last_inserted_relation = NULL;
    sess->delta_seeded = false;

    /* Issue #83: Update base_nrows for all relations after convergence.
     * This marks the current state as "stable" so the next incremental
     * insert can compute the delta as rows[base_nrows..nrows). */
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_t *r = sess->rels[i];
        if (r)
            r->base_nrows = r->nrows;
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
/* Affected Strata Detection (Phase 4)                                      */
/* ======================================================================== */

/*
 * bitmask_or_simd - Union two 64-bit bitmasks using SIMD when available.
 *
 * For >8 strata this avoids sequential OR chains by operating on two 64-bit
 * words packed into a 128-bit vector in a single instruction.
 *
 * When SIMD is unavailable the scalar fallback is a single OR, which the
 * compiler will inline. The result is always written back to *dst.
 */
static inline uint64_t
bitmask_or_simd(uint64_t dst, uint64_t src)
{
#if defined(__ARM_NEON__)
    /* Pack both masks into a 128-bit vector and OR them in one shot. */
    uint64x2_t vd = vcombine_u64(vcreate_u64(dst), vcreate_u64(0));
    uint64x2_t vs = vcombine_u64(vcreate_u64(src), vcreate_u64(0));
    uint64x2_t vr = vorrq_u64(vd, vs);
    return vgetq_lane_u64(vr, 0);
#elif defined(__SSE2__)
    __m128i vd = _mm_set_epi64x(0, (int64_t)dst);
    __m128i vs = _mm_set_epi64x(0, (int64_t)src);
    __m128i vr = _mm_or_si128(vd, vs);
    return (uint64_t)_mm_cvtsi128_si64(vr);
#else
    return dst | src;
#endif
}

/*
 * stratum_references_relation - Return true if any VARIABLE op in stratum sp
 * references the relation named `rel`, or if any JOIN/ANTIJOIN/SEMIJOIN op
 * has right_relation matching `rel`.
 */
static bool
stratum_references_relation(const wl_plan_stratum_t *sp, const char *rel)
{
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *pr = &sp->relations[ri];
        for (uint32_t oi = 0; oi < pr->op_count; oi++) {
            if (pr->ops[oi].op == WL_PLAN_OP_VARIABLE
                && pr->ops[oi].relation_name != NULL
                && strcmp(pr->ops[oi].relation_name, rel) == 0) {
                return true;
            }
            if ((pr->ops[oi].op == WL_PLAN_OP_JOIN
                 || pr->ops[oi].op == WL_PLAN_OP_ANTIJOIN
                 || pr->ops[oi].op == WL_PLAN_OP_SEMIJOIN)
                && pr->ops[oi].right_relation != NULL
                && strcmp(pr->ops[oi].right_relation, rel) == 0) {
                return true;
            }
        }
    }
    return false;
}

/*
 * col_compute_affected_strata - Identify strata needing re-evaluation.
 *
 * Walks the stratum dependency graph rooted at `inserted_relation` and
 * returns a bitmask where bit i is set if stratum i directly or transitively
 * depends on the newly inserted relation.
 *
 * Algorithm (iterative worklist BFS over stratum indices):
 *   1. Seed: mark every stratum that directly references inserted_relation.
 *   2. Propagate: for each newly-marked stratum, find all strata that
 *      reference any relation produced by the marked stratum, and mark them.
 *   3. Repeat until no new strata are marked.
 *
 * "Produced by stratum s" = every relation listed in plan->strata[s].relations.
 *
 * SIMD: bitmask union uses bitmask_or_simd() which compiles to a single
 * vorrq_u64 (NEON) or _mm_or_si128 (SSE2) instruction when those ISAs are
 * available. The scalar path is a plain | operator.
 *
 * Supports up to 64 strata (one uint64_t bitmask). Plans with more strata
 * will have bits beyond 63 silently ignored (conservative: returns 0 for
 * those strata, which causes them to be re-evaluated unconditionally by the
 * caller's fallback).
 *
 * @param session          Active session (must have a plan attached).
 * @param inserted_relation Name of the EDB relation receiving new facts.
 * @return Bitmask of affected stratum indices; 0 on invalid input.
 */
uint64_t
col_compute_affected_strata(wl_session_t *session,
                            const char *inserted_relation)
{
    if (!session || !inserted_relation)
        return 0;

    wl_col_session_t *sess = COL_SESSION(session);
    const wl_plan_t *plan = sess->plan;
    if (!plan || plan->stratum_count == 0)
        return 0;

    uint32_t nstrata = plan->stratum_count;
    if (nstrata > 64)
        nstrata = 64; /* clamp to bitmask width */

    /* Issue #93: Removed EDB fast path that returned full_mask for any EDB
     * insertion. The BFS below correctly computes targeted affected strata
     * for both EDB and IDB relations, enabling frontier skip optimization. */

    uint64_t affected = 0;

    /* --- Pass 1: seed with strata directly referencing inserted_relation --- */
    for (uint32_t si = 0; si < nstrata; si++) {
        if (stratum_references_relation(&plan->strata[si], inserted_relation)) {
            affected = bitmask_or_simd(affected, (uint64_t)1 << si);
        }
    }

    /* Early exit: if all strata are already marked, skip transitive closure.
     * This occurs when inserted_relation is a base fact (referenced by many/all
     * strata), avoiding O(nstrata^2) work in Pass 2. */
    uint64_t full_mask = (nstrata == 64) ? ~0ULL : ((1ULL << nstrata) - 1);
    if (affected == full_mask)
        return affected;

    /* --- Pass 2: transitive propagation via fixed-point iteration ---------- */
    /*
     * For every newly-marked stratum, find all strata that reference any
     * relation produced by that stratum. We loop until the bitmask stabilises.
     */
    uint64_t prev = 0;
    while (prev != affected) {
        prev = affected;
        /* Iterate over all currently marked strata. */
        uint64_t pending = affected;
        while (pending) {
            /* Extract lowest set bit index. */
            uint32_t si = (uint32_t)__builtin_ctzll(pending);
            pending &= pending - 1; /* clear lowest set bit */

            if (si >= nstrata)
                break;

            const wl_plan_stratum_t *src_sp = &plan->strata[si];
            /* For each relation produced by stratum si ... */
            for (uint32_t ri = 0; ri < src_sp->relation_count; ri++) {
                const char *produced = src_sp->relations[ri].name;
                if (!produced)
                    continue;
                /* ... mark any stratum that references it. */
                for (uint32_t sj = 0; sj < nstrata; sj++) {
                    if (affected & ((uint64_t)1 << sj))
                        continue; /* already marked */
                    if (stratum_references_relation(&plan->strata[sj],
                                                    produced)) {
                        affected = bitmask_or_simd(affected, (uint64_t)1 << sj);
                    }
                }
            }
        }
    }

    return affected;
}

/* ======================================================================== */
/* Affected Rule Detection (Phase 4, US-4-003)                              */
/* ======================================================================== */

/*
 * rule_references_relation - Return true if any VARIABLE op in relation pr
 * references the relation named `rel`.
 */
static bool
rule_references_relation(const wl_plan_relation_t *pr, const char *rel)
{
    for (uint32_t oi = 0; oi < pr->op_count; oi++) {
        /* Check VARIABLE ops (left child of joins) */
        if (pr->ops[oi].op == WL_PLAN_OP_VARIABLE
            && pr->ops[oi].relation_name != NULL
            && strcmp(pr->ops[oi].relation_name, rel) == 0) {
            return true;
        }
        /* Check JOIN/ANTIJOIN/SEMIJOIN right_relation (right child of joins) */
        if ((pr->ops[oi].op == WL_PLAN_OP_JOIN
             || pr->ops[oi].op == WL_PLAN_OP_ANTIJOIN
             || pr->ops[oi].op == WL_PLAN_OP_SEMIJOIN)
            && pr->ops[oi].right_relation != NULL
            && strcmp(pr->ops[oi].right_relation, rel) == 0) {
            return true;
        }
    }
    return false;
}

/*
 * col_compute_affected_rules - Identify rules needing re-evaluation.
 *
 * Rules are enumerated globally across all strata in declaration order:
 * stratum 0 relations first (by relation index), then stratum 1, etc.
 * Rule index i corresponds to the i-th relation in this traversal.
 *
 * Algorithm (same iterative fixed-point BFS as col_compute_affected_strata):
 *   1. Seed: mark every rule whose VARIABLE body references inserted_relation.
 *   2. Propagate: for each newly-marked rule, find all rules whose body
 *      references the head relation produced by the marked rule, and mark them.
 *   3. Repeat until the bitmask stabilises.
 *
 * SIMD: bitmask union uses bitmask_or_simd() (same helper as strata version).
 *
 * Supports up to 64 rules (one uint64_t bitmask). Plans with more rules will
 * have bits beyond 63 silently ignored (conservative: those rules are always
 * re-evaluated by the caller's fallback via UINT64_MAX mask).
 *
 * @param session           Active session (must have a plan attached).
 * @param inserted_relation Name of the EDB relation receiving new facts.
 * @return Bitmask of affected rule indices; 0 on invalid input.
 */
uint64_t
col_compute_affected_rules(wl_session_t *session, const char *inserted_relation)
{
    if (!session || !inserted_relation)
        return 0;

    wl_col_session_t *sess = COL_SESSION(session);
    const wl_plan_t *plan = sess->plan;
    if (!plan || plan->stratum_count == 0)
        return 0;

    /*
     * Build a flat enumeration of (stratum_idx, relation_idx) pairs so each
     * rule has a stable global index.  We clamp to MAX_RULES (64) to stay
     * within the uint64_t bitmask.
     */
    uint32_t nrules = 0;
    for (uint32_t si = 0; si < plan->stratum_count && nrules < MAX_RULES;
         si++) {
        uint32_t rc = plan->strata[si].relation_count;
        if (nrules + rc > MAX_RULES)
            rc = MAX_RULES - nrules;
        nrules += rc;
    }

    if (nrules == 0)
        return 0;

    /*
     * Store (stratum_idx, relation_idx) for each global rule index so we can
     * look up the rule's head name and body ops later.
     */
    uint32_t rule_si[MAX_RULES]; /* stratum index for rule i  */
    uint32_t rule_ri[MAX_RULES]; /* relation index for rule i */
    {
        uint32_t idx = 0;
        for (uint32_t si = 0; si < plan->stratum_count && idx < MAX_RULES;
             si++) {
            for (uint32_t ri = 0;
                 ri < plan->strata[si].relation_count && idx < MAX_RULES;
                 ri++) {
                rule_si[idx] = si;
                rule_ri[idx] = ri;
                idx++;
            }
        }
    }

    uint64_t affected = 0;

    /* --- Pass 1: seed rules that directly reference inserted_relation --- */
    for (uint32_t i = 0; i < nrules; i++) {
        const wl_plan_relation_t *pr
            = &plan->strata[rule_si[i]].relations[rule_ri[i]];
        if (rule_references_relation(pr, inserted_relation)) {
            affected = bitmask_or_simd(affected, (uint64_t)1 << i);
        }
    }

    /* --- Pass 2: transitive propagation via fixed-point iteration --------- */
    /*
     * For every newly-marked rule, find all rules whose body references the
     * head relation produced by the marked rule. Loop until stable.
     */
    uint64_t prev = 0;
    while (prev != affected) {
        prev = affected;
        uint64_t pending = affected;
        while (pending) {
            uint32_t i = (uint32_t)__builtin_ctzll(pending);
            pending &= pending - 1; /* clear lowest set bit */

            if (i >= nrules)
                break;

            /* Head relation name produced by rule i */
            const char *head
                = plan->strata[rule_si[i]].relations[rule_ri[i]].name;
            if (!head)
                continue;

            /* Mark any rule whose body references this head */
            for (uint32_t j = 0; j < nrules; j++) {
                if (affected & ((uint64_t)1 << j))
                    continue; /* already marked */
                const wl_plan_relation_t *pr
                    = &plan->strata[rule_si[j]].relations[rule_ri[j]];
                if (rule_references_relation(pr, head)) {
                    affected = bitmask_or_simd(affected, (uint64_t)1 << j);
                }
            }
        }
    }

    return affected;
}

/* ======================================================================== */
/* Mobius / Z-set Weighted JOIN                                              */
/* ======================================================================== */

/*
 * col_op_join_weighted - equi-join with multiplicity multiplication.
 *
 * Joins lhs and rhs on column index key_col (present in both).  For each
 * matching pair the output row is appended to dst and its timestamp
 * multiplicity is set to lhs_mult * rhs_mult.
 *
 * Output layout: all lhs columns followed by all rhs columns (key column
 * is duplicated; callers may project as needed).  dst->ncols is initialised
 * by this function; dst must be caller-allocated with ncols==0 on entry.
 *
 * Returns 0 on success, non-zero (ENOMEM / EINVAL) on error.
 */
int
col_op_join_weighted(const col_rel_t *lhs, const col_rel_t *rhs,
                     uint32_t key_col, col_rel_t *dst)
{
    if (!lhs || !rhs || !dst)
        return EINVAL;
    if (key_col >= lhs->ncols || key_col >= rhs->ncols)
        return EINVAL;

    uint32_t ocols = lhs->ncols + rhs->ncols;
    dst->ncols = ocols;

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols > 0 ? ocols : 1));
    if (!tmp)
        return ENOMEM;

    int rc = 0;
    for (uint32_t li = 0; li < lhs->nrows && rc == 0; li++) {
        const int64_t *lrow = lhs->data + (size_t)li * lhs->ncols;
        int64_t lmult = lhs->timestamps ? lhs->timestamps[li].multiplicity : 1;

        for (uint32_t ri = 0; ri < rhs->nrows && rc == 0; ri++) {
            const int64_t *rrow = rhs->data + (size_t)ri * rhs->ncols;

            if (lrow[key_col] != rrow[key_col])
                continue;

            int64_t rmult
                = rhs->timestamps ? rhs->timestamps[ri].multiplicity : 1;

            memcpy(tmp, lrow, sizeof(int64_t) * lhs->ncols);
            memcpy(tmp + lhs->ncols, rrow, sizeof(int64_t) * rhs->ncols);

            /* Grow dst manually to keep data and timestamps in sync. */
            if (dst->nrows >= dst->capacity) {
                uint32_t new_cap = dst->capacity ? dst->capacity * 2 : 16;
                int64_t *nd = (int64_t *)realloc(
                    dst->data, sizeof(int64_t) * (size_t)new_cap * ocols);
                if (!nd) {
                    rc = ENOMEM;
                    break;
                }
                dst->data = nd;
                col_delta_timestamp_t *nt = (col_delta_timestamp_t *)realloc(
                    dst->timestamps,
                    (size_t)new_cap * sizeof(col_delta_timestamp_t));
                if (!nt) {
                    rc = ENOMEM;
                    break;
                }
                dst->timestamps = nt;
                dst->capacity = new_cap;
            }
            memcpy(dst->data + (size_t)dst->nrows * ocols, tmp,
                   sizeof(int64_t) * ocols);
            memset(&dst->timestamps[dst->nrows], 0,
                   sizeof(col_delta_timestamp_t));
            dst->timestamps[dst->nrows].multiplicity = lmult * rmult;
            dst->nrows++;
        }
    }

    free(tmp);
    return rc;
}

/* ======================================================================== */
/* Mobius / Z-set Delta Formula                                             */
/* ======================================================================== */

/*
 * col_compute_delta_mobius:
 * Compute the Mobius delta between prev_collection and curr_collection.
 *
 * For each unique key (column 0) in the union of both relations:
 *   - key only in curr:  delta_mult = curr_mult
 *   - key only in prev:  delta_mult = -prev_mult
 *   - key in both:       delta_mult = curr_mult - prev_mult (skipped if 0)
 *
 * Both input relations must have timestamps != NULL.
 * out_delta must be caller-allocated, empty (nrows==0) on entry.
 *
 * Returns 0 on success, EINVAL on bad arguments, ENOMEM on allocation failure.
 */
int
col_compute_delta_mobius(const col_rel_t *prev_collection,
                         const col_rel_t *curr_collection, col_rel_t *out_delta)
{
    if (!prev_collection || !curr_collection || !out_delta)
        return EINVAL;
    if (prev_collection->ncols == 0 || curr_collection->ncols == 0)
        return EINVAL;
    if (prev_collection->ncols != curr_collection->ncols)
        return EINVAL;

    uint32_t ncols = prev_collection->ncols;
    out_delta->ncols = ncols;

    /* Helper lambda (via inline block) to append a row+mult to out_delta. */
#define DELTA_APPEND(row_ptr, mult_val)                                       \
    do {                                                                      \
        if (out_delta->nrows >= out_delta->capacity) {                        \
            uint32_t new_cap                                                  \
                = out_delta->capacity ? out_delta->capacity * 2 : 16;         \
            int64_t *nd = (int64_t *)realloc(                                 \
                out_delta->data, sizeof(int64_t) * (size_t)new_cap * ncols);  \
            if (!nd)                                                          \
                return ENOMEM;                                                \
            out_delta->data = nd;                                             \
            col_delta_timestamp_t *nt = (col_delta_timestamp_t *)realloc(     \
                out_delta->timestamps,                                        \
                (size_t)new_cap * sizeof(col_delta_timestamp_t));             \
            if (!nt)                                                          \
                return ENOMEM;                                                \
            out_delta->timestamps = nt;                                       \
            out_delta->capacity = new_cap;                                    \
        }                                                                     \
        memcpy(out_delta->data + (size_t)out_delta->nrows * ncols, (row_ptr), \
               sizeof(int64_t) * ncols);                                      \
        col_delta_timestamp_t ts_;                                            \
        memset(&ts_, 0, sizeof(ts_));                                         \
        ts_.multiplicity = (mult_val);                                        \
        out_delta->timestamps[out_delta->nrows] = ts_;                        \
        out_delta->nrows++;                                                   \
    } while (0)

    /* Pass 1: iterate over curr; for each key look up in prev. */
    for (uint32_t ci = 0; ci < curr_collection->nrows; ci++) {
        const int64_t *crow = curr_collection->data + (size_t)ci * ncols;
        int64_t cmult = curr_collection->timestamps
                            ? curr_collection->timestamps[ci].multiplicity
                            : 1;

        /* Search prev for matching key (column 0). */
        int64_t pmult = 0;
        bool found_in_prev = false;
        for (uint32_t pi = 0; pi < prev_collection->nrows; pi++) {
            const int64_t *prow = prev_collection->data + (size_t)pi * ncols;
            if (prow[0] == crow[0]) {
                pmult = prev_collection->timestamps
                            ? prev_collection->timestamps[pi].multiplicity
                            : 1;
                found_in_prev = true;
                break;
            }
        }

        int64_t delta_mult = found_in_prev ? (cmult - pmult) : cmult;
        if (delta_mult != 0) {
            DELTA_APPEND(crow, delta_mult);
        }
    }

    /* Pass 2: iterate over prev; emit -prev_mult for keys absent in curr. */
    for (uint32_t pi = 0; pi < prev_collection->nrows; pi++) {
        const int64_t *prow = prev_collection->data + (size_t)pi * ncols;
        int64_t pmult = prev_collection->timestamps
                            ? prev_collection->timestamps[pi].multiplicity
                            : 1;

        bool found_in_curr = false;
        for (uint32_t ci = 0; ci < curr_collection->nrows; ci++) {
            const int64_t *crow = curr_collection->data + (size_t)ci * ncols;
            if (crow[0] == prow[0]) {
                found_in_curr = true;
                break;
            }
        }

        if (!found_in_curr) {
            int64_t delta_mult = -pmult;
            if (delta_mult != 0) {
                DELTA_APPEND(prow, delta_mult);
            }
        }
    }

#undef DELTA_APPEND

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
