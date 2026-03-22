/*
 * columnar/session_hash.c - Relation name hash table for O(1) lookup
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Hash-based lookup for session_find_rel to replace O(N) strcmp loop.
 * Uses FNV-1a hashing (pattern from arrangement.c) with chaining.
 * Issue #281: Performance optimization for evaluator hotpath.
 */

#include "columnar/internal.h"

#include "../wirelog-internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* FNV-1a Hash for Relation Name Strings                                   */
/* ======================================================================== */

/*
 * session_rel_hash_string: FNV-1a hash over null-terminated relation name.
 * nbuckets MUST be a power of 2.
 */
static uint32_t
session_rel_hash_string(const char *name, uint32_t nbuckets)
{
    uint64_t h = 14695981039346656037ULL; /* FNV-1a basis */
    for (const unsigned char *p = (const unsigned char *)name; *p; p++) {
        h ^= *p;
        h *= 1099511628211ULL; /* FNV-1a prime */
    }
    return (uint32_t)(h & (uint64_t)(nbuckets - 1));
}

/* Round n up to the next power of 2; minimum 16. */
static uint32_t
session_rel_next_pow2(uint32_t n)
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

/* ======================================================================== */
/* Hash Table Build and Maintenance                                        */
/* ======================================================================== */

/*
 * session_rel_build_hash: Full rebuild of hash table from rels[] array.
 * Returns 0 on success, ENOMEM on allocation failure.
 * May reallocate bucket heads if size changed, or chain array if needed.
 */
int
session_rel_build_hash(wl_col_session_t *sess)
{
    uint32_t nrels = sess->nrels;
    uint32_t nbuckets = session_rel_next_pow2(nrels > 0 ? nrels * 2u : 16u);

    /* Reallocate bucket heads if size changed. */
    if (nbuckets != sess->rel_hash_nbuckets) {
        uint32_t *head
            = (uint32_t *)malloc(nbuckets * sizeof(uint32_t));
        if (!head)
            return ENOMEM;
        free(sess->rel_hash_head);
        sess->rel_hash_head = head;
        sess->rel_hash_nbuckets = nbuckets;
    }
    memset(sess->rel_hash_head, 0xFF, nbuckets * sizeof(uint32_t));

    /* Allocate chain array with explicit capacity tracking.
     * Allocate nrels capacity (all relation indices from 0..nrels-1).
     * This tracks actual capacity separately from bucket count. */
    if (nrels > sess->rel_hash_chain_cap) {
        uint32_t new_cap = nrels;
        uint32_t *nxt = (uint32_t *)malloc(new_cap * sizeof(uint32_t));
        if (!nxt)
            return ENOMEM;
        free(sess->rel_hash_next);
        sess->rel_hash_next = nxt;
        sess->rel_hash_chain_cap = new_cap;
    }

    /* Index all non-NULL relations. */
    for (uint32_t i = 0; i < nrels; i++) {
        if (!sess->rels[i])
            continue; /* Skip holes from session_remove_rel */
        uint32_t bucket
            = session_rel_hash_string(sess->rels[i]->name, nbuckets);
        sess->rel_hash_next[i] = sess->rel_hash_head[bucket];
        sess->rel_hash_head[bucket] = i;
    }
    return 0;
}

/*
 * session_rel_hash_lookup: Chain walk to find relation by name.
 * Returns pointer to col_rel_t on match, NULL if not found.
 * If hash table is uninitialized (nbuckets == 0), returns NULL
 * and caller should rebuild hash before retrying.
 */
col_rel_t *
session_rel_hash_lookup(wl_col_session_t *sess, const char *name)
{
    if (!name || sess->rel_hash_nbuckets == 0)
        return NULL;

    uint32_t bucket = session_rel_hash_string(name, sess->rel_hash_nbuckets);
    uint32_t idx = sess->rel_hash_head[bucket];

    while (idx != UINT32_MAX) {
        if (sess->rels[idx] && strcmp(sess->rels[idx]->name, name) == 0)
            return sess->rels[idx];
        idx = sess->rel_hash_next[idx];
    }
    return NULL;
}

/*
 * session_rel_hash_insert: Add a new relation to the hash table.
 * Called after session_add_rel appends to rels[] array.
 * Rebuilds entire hash if load factor would exceed 50%.
 * Returns 0 on success, ENOMEM on failure.
 */
int
session_rel_hash_insert(wl_col_session_t *sess, uint32_t idx)
{
    if (idx >= sess->nrels || !sess->rels[idx])
        return 0; /* Safety check: nothing to insert */

    /* Lazy initialization: build hash if not yet created. */
    if (sess->rel_hash_nbuckets == 0) {
        return session_rel_build_hash(sess);
    }

    /* Rebuild if load factor would exceed 50% or chain array capacity insufficient. */
    uint32_t needed = session_rel_next_pow2(sess->nrels * 2u);
    if (needed != sess->rel_hash_nbuckets || idx >= sess->rel_hash_chain_cap) {
        return session_rel_build_hash(sess);
    }

    /* Insert into hash chain. Chain array has sufficient capacity. */
    uint32_t bucket
        = session_rel_hash_string(sess->rels[idx]->name,
            sess->rel_hash_nbuckets);
    sess->rel_hash_next[idx] = sess->rel_hash_head[bucket];
    sess->rel_hash_head[bucket] = idx;

    return 0;
}

/*
 * session_rel_hash_remove: Remove entry from hash chain after session_remove_rel.
 * Properly unlinks the entry from its hash bucket chain.
 * Returns 0 always (no allocation).
 */
int
session_rel_hash_remove(wl_col_session_t *sess, uint32_t idx)
{
    if (idx >= sess->nrels || sess->rel_hash_nbuckets == 0)
        return 0; /* Nothing to remove if hash not built */

    if (!sess->rels[idx])
        return 0; /* Already NULL, nothing to remove */

    /* After session_remove_rel nullifies rels[idx], we can't retrieve the name
     * to unlink from the chain. Rebuild the hash to compact out the removed entry. */
    return session_rel_build_hash(sess);
}

/*
 * session_rel_free_hash: Free hash table arrays on session destroy.
 */
void
session_rel_free_hash(wl_col_session_t *sess)
{
    if (!sess)
        return;
    free(sess->rel_hash_head);
    free(sess->rel_hash_next);
    sess->rel_hash_head = NULL;
    sess->rel_hash_next = NULL;
    sess->rel_hash_nbuckets = 0;
}
