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

#include <assert.h>
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

/* Round n up to the next power of 2; minimum 16. Returns 0 on overflow. */
static uint32_t
session_rel_next_pow2(uint32_t n)
{
    if (n < 16u)
        return 16u;
    if (n > UINT32_MAX / 2u + 1u)
        return 0;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    return n + 1u;
}

static bool
session_reservation_reserved(
    const wl_columnar_memory_reservation_t *reservation)
{
    return reservation->identity == reservation
           && atomic_load_explicit(&reservation->state,
               memory_order_acquire)
           == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED;
}

static void
session_reservation_release(wl_columnar_memory_reservation_t *reservation)
{
    if (session_reservation_reserved(reservation)
        && !wl_columnar_memory_rollback(reservation))
        abort();
}

static void
session_reservation_move_if_reserved(
    wl_columnar_memory_reservation_t *destination,
    wl_columnar_memory_reservation_t *source)
{
    if (!session_reservation_reserved(source))
        return;
    if (destination->identity != destination)
        wl_columnar_memory_reservation_init(destination);
    if (!wl_columnar_memory_reservation_move(destination, source))
        abort();
}

/* ======================================================================== */
/* Hash Table Build and Maintenance                                        */
/* ======================================================================== */

/*
 * session_rel_build_hash: Full rebuild of hash table from rels[] array.
 * Replaces both arrays together. Admission or allocation failure preserves
 * the previous hash until the caller chooses linear lookup.
 */
int
session_rel_build_hash(wl_col_session_t *sess)
{
    uint32_t nrels = sess->nrels;
    if (nrels > UINT32_MAX / 2u)
        return ENOMEM;
    uint32_t nbuckets = session_rel_next_pow2(nrels > 0 ? nrels * 2u : 16u);
    if (nbuckets == 0)
        return ENOMEM;

    uint64_t head_bytes, next_bytes, total_bytes;
    if (!wl_columnar_memory_size_mul(nbuckets, sizeof(uint32_t),
        &head_bytes)
        || !wl_columnar_memory_size_mul(nrels, sizeof(uint32_t),
        &next_bytes)
        || !wl_columnar_memory_size_add(head_bytes, next_bytes,
        &total_bytes)
        || head_bytes > SIZE_MAX || next_bytes > SIZE_MAX)
        return EOVERFLOW;
    wl_columnar_memory_reservation_t pending;
    wl_columnar_memory_reservation_init(&pending);
    if (sess->memory_governor) {
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(sess->memory_governor),
                total_bytes, &pending);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
        }
    }
    uint32_t *head = (uint32_t *)malloc((size_t)head_bytes);
    uint32_t *next = nrels ? (uint32_t *)malloc((size_t)next_bytes) : NULL;
    if (!head || (nrels && !next)) {
        free(head);
        free(next);
        session_reservation_release(&pending);
        return ENOMEM;
    }
    memset(head, 0xFF, (size_t)head_bytes);

    /* Index all non-NULL relations. */
    for (uint32_t i = 0; i < nrels; i++) {
        if (!sess->rels[i])
            continue; /* Skip holes from session_remove_rel */
        uint32_t bucket
            = session_rel_hash_string(sess->rels[i]->name, nbuckets);
        next[i] = head[bucket];
        head[bucket] = i;
    }
    free(sess->rel_hash_head);
    free(sess->rel_hash_next);
    session_reservation_release(&sess->rel_hash_reservation);
    session_reservation_move_if_reserved(&sess->rel_hash_reservation,
        &pending);
    sess->rel_hash_head = head;
    sess->rel_hash_next = next;
    sess->rel_hash_nbuckets = nbuckets;
    sess->rel_hash_chain_cap = nrels;
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
    if (sess->nrels > UINT32_MAX / 2u)
        return ENOMEM;
    uint32_t needed = session_rel_next_pow2(sess->nrels * 2u);
    if (needed == 0)
        return ENOMEM;
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
    session_reservation_release(&sess->rel_hash_reservation);
    sess->rel_hash_head = NULL;
    sess->rel_hash_next = NULL;
    sess->rel_hash_nbuckets = 0;
    sess->rel_hash_chain_cap = 0;
}

#ifdef WL_TEST_OWNER_PUBLICATION
static wl_col_session_t *wl_columnar_session_hash_fail_image_session;
static bool wl_columnar_session_hash_fail_image_hit;

void
wl_columnar_session_hash_test_fail_registry_image_prepare(
    wl_col_session_t *session)
{
    wl_columnar_session_hash_fail_image_session = session;
    wl_columnar_session_hash_fail_image_hit = false;
}

bool
wl_columnar_session_hash_test_registry_image_prepare_failure_hit(void)
{
    return wl_columnar_session_hash_fail_image_hit;
}
#endif

static int
session_registry_image_admit(wl_col_session_t *session, uint64_t bytes,
    wl_columnar_memory_reservation_t *reservation)
{
    if (!session->memory_governor)
        return 0;
    wl_columnar_memory_admission_status_t status
        = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(session->memory_governor),
            bytes, reservation);
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
        || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return 0;
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
        session->memory_budget_denied = true;
    return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
        : EINVAL;
}

void
wl_columnar_session_hash_registry_image_discard(
    wl_columnar_session_hash_registry_image_t *image)
{
    if (!image)
        return;
    free((void *)image->rels);
    session_reservation_release(&image->rels_reservation);
    free(image->hash_head);
    free(image->hash_next);
    session_reservation_release(&image->hash_reservation);
    free((void *)image->expected_rel_contents);
    session_reservation_release(&image->expected_rels_reservation);
    memset(image, 0, sizeof(*image));
}

int
wl_columnar_session_hash_registry_image_prepare(wl_col_session_t *session,
    col_rel_t *const *additions, uint32_t addition_count,
    wl_columnar_session_hash_registry_image_t *image)
{
    size_t bytes;
    uint32_t needed, append_count = 0, holes = 0, buckets, cap;

    if (!session || !additions || addition_count == 0 || !image)
        return EINVAL;
    memset(image, 0, sizeof(*image));
    wl_columnar_memory_reservation_init(&image->hash_reservation);
    wl_columnar_memory_reservation_init(&image->rels_reservation);
    wl_columnar_memory_reservation_init(
        &image->expected_rels_reservation);
    if (session->nrels > 0) {
        if (!session->rels) {
            return EINVAL;
        }
        for (uint32_t i = 0; i < session->nrels; i++)
            if (!session->rels[i])
                holes++;
    }
    append_count = addition_count > holes ? addition_count - holes : 0;
    if (append_count > UINT32_MAX - session->nrels)
        return EOVERFLOW;
    needed = session->nrels + append_count;
    cap = session->rel_cap > needed ? session->rel_cap : needed;
    bytes = (size_t)cap * sizeof(*image->rels);
    if (cap != 0 && bytes / cap != sizeof(*image->rels))
        return EOVERFLOW;
    int admit_rc = session_registry_image_admit(session, bytes,
            &image->rels_reservation);
    if (admit_rc != 0) {
        wl_columnar_session_hash_registry_image_discard(image);
        return admit_rc;
    }
    image->rels = (col_rel_t **)malloc(bytes);
    if (!image->rels) {
        wl_columnar_session_hash_registry_image_discard(image);
        return ENOMEM;
    }
    if (session->nrels > 0) {
        uint64_t expected_bytes;
        if (!wl_columnar_memory_size_mul(session->nrels,
            sizeof(*image->expected_rel_contents), &expected_bytes)
            || expected_bytes > SIZE_MAX) {
            wl_columnar_session_hash_registry_image_discard(image);
            return EOVERFLOW;
        }
        admit_rc = session_registry_image_admit(session, expected_bytes,
                &image->expected_rels_reservation);
        if (admit_rc != 0) {
            wl_columnar_session_hash_registry_image_discard(image);
            return admit_rc;
        }
        image->expected_rel_contents = (col_rel_t **)malloc(
            (size_t)expected_bytes);
        if (!image->expected_rel_contents) {
            wl_columnar_session_hash_registry_image_discard(image);
            return ENOMEM;
        }
        memcpy((void *)image->expected_rel_contents,
            (const void *)session->rels,
            (size_t)session->nrels * sizeof(*image->expected_rel_contents));
        memcpy((void *)image->rels, (const void *)session->rels,
            (size_t)session->nrels * sizeof(*image->rels));
    }
#ifdef WL_TEST_OWNER_PUBLICATION
    if (wl_columnar_session_hash_fail_image_session == session) {
        wl_columnar_session_hash_fail_image_session = NULL;
        wl_columnar_session_hash_fail_image_hit = true;
        wl_columnar_session_hash_registry_image_discard(image);
        return ENOMEM;
    }
#endif
    image->nrels = session->nrels;
    for (uint32_t i = 0; i < addition_count; i++) {
        col_rel_t *candidate = additions[i];
        if (!candidate || !candidate->name || !candidate->name[0]) {
            wl_columnar_session_hash_registry_image_discard(image);
            return EINVAL;
        }
        for (uint32_t j = 0; j < session->nrels; j++) {
            col_rel_t *existing = session->rels[j];
            if (existing && existing->name
                && strcmp(existing->name, candidate->name) == 0) {
                wl_columnar_session_hash_registry_image_discard(image);
                return EEXIST;
            }
        }
        for (uint32_t j = 0; j < i; j++) {
            if (strcmp(additions[j]->name, candidate->name) == 0) {
                wl_columnar_session_hash_registry_image_discard(image);
                return EEXIST;
            }
        }
        uint32_t slot = 0;
        while (slot < image->nrels && image->rels[slot])
            slot++;
        if (slot >= needed || slot >= cap) {
            wl_columnar_session_hash_registry_image_discard(image);
            return EOVERFLOW;
        }
        if (slot == image->nrels)
            image->nrels++;
        image->rels[slot] = candidate;
    }
    image->rel_cap = cap;
    if (needed > UINT32_MAX / 2u) {
        wl_columnar_session_hash_registry_image_discard(image);
        return EOVERFLOW;
    }
    buckets = session_rel_next_pow2(needed ? needed * 2u : 16u);
    if (!buckets) {
        wl_columnar_session_hash_registry_image_discard(image);
        return EOVERFLOW;
    }
    size_t head_bytes = (size_t)buckets * sizeof(*image->hash_head);
    size_t next_bytes = (size_t)needed * sizeof(*image->hash_next);
    if (head_bytes / buckets != sizeof(*image->hash_head)
        || next_bytes / needed != sizeof(*image->hash_next)) {
        wl_columnar_session_hash_registry_image_discard(image);
        return EOVERFLOW;
    }
    uint64_t hash_bytes;
    if (!wl_columnar_memory_size_add(head_bytes, next_bytes,
        &hash_bytes)) {
        wl_columnar_session_hash_registry_image_discard(image);
        return EOVERFLOW;
    }
    if (session->memory_governor) {
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(
                    session->memory_governor), hash_bytes,
                &image->hash_reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                session->memory_budget_denied = true;
            wl_columnar_session_hash_registry_image_discard(image);
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
        }
    }
    image->hash_head = malloc(head_bytes);
    image->hash_next = malloc(next_bytes);
    if (!image->hash_head || !image->hash_next) {
        wl_columnar_session_hash_registry_image_discard(image);
        return ENOMEM;
    }
    memset(image->hash_head, 0xFF,
        (size_t)buckets * sizeof(*image->hash_head));
    for (uint32_t i = 0; i < needed; i++) {
        col_rel_t *relation = image->rels[i];
        if (!relation)
            continue;
        if (!relation->name) {
            wl_columnar_session_hash_registry_image_discard(image);
            return EINVAL;
        }
        uint32_t bucket = session_rel_hash_string(relation->name, buckets);
        image->hash_next[i] = image->hash_head[bucket];
        image->hash_head[bucket] = i;
    }
    image->hash_nbuckets = buckets;
    image->hash_chain_cap = needed;
    image->session = session;
    image->expected_rels = session->rels;
    image->expected_nrels = session->nrels;
    image->expected_rel_cap = session->rel_cap;
    image->expected_hash_head = session->rel_hash_head;
    image->expected_hash_next = session->rel_hash_next;
    image->expected_hash_nbuckets = session->rel_hash_nbuckets;
    image->expected_hash_chain_cap = session->rel_hash_chain_cap;
    return 0;
}

int
wl_columnar_session_hash_registry_image_validate(
    const wl_columnar_session_hash_registry_image_t *image)
{
    const wl_col_session_t *session;

    if (!image || !image->session || !image->rels || !image->hash_head
        || !image->hash_next)
        return EINVAL;
    session = image->session;
    if (session->rels != image->expected_rels
        || session->nrels != image->expected_nrels
        || session->rel_cap != image->expected_rel_cap
        || session->rel_hash_head != image->expected_hash_head
        || session->rel_hash_next != image->expected_hash_next
        || session->rel_hash_nbuckets != image->expected_hash_nbuckets
        || session->rel_hash_chain_cap != image->expected_hash_chain_cap)
        return EBUSY;
    for (uint32_t i = 0; i < image->expected_nrels; i++)
        if (session->rels[i] != image->expected_rel_contents[i])
            return EBUSY;
    return 0;
}

void
wl_columnar_session_hash_registry_image_publish(
    wl_columnar_session_hash_registry_image_t *image)
{
    wl_col_session_t *session;
    col_rel_t **old_rels;
    uint32_t *old_head, *old_next;

    assert(wl_columnar_session_hash_registry_image_validate(image) == 0);
    session = image->session;
    old_rels = session->rels;
    old_head = session->rel_hash_head;
    old_next = session->rel_hash_next;
    session->rels = image->rels;
    session->nrels = image->nrels;
    session->rel_cap = image->rel_cap;
    session->rel_hash_head = image->hash_head;
    session->rel_hash_next = image->hash_next;
    session->rel_hash_nbuckets = image->hash_nbuckets;
    session->rel_hash_chain_cap = image->hash_chain_cap;
    image->rels = NULL;
    image->hash_head = NULL;
    image->hash_next = NULL;
    free((void *)old_rels);
    session_reservation_release(&session->rels_reservation);
    session_reservation_move_if_reserved(&session->rels_reservation,
        &image->rels_reservation);
    free(old_head);
    free(old_next);
    session_reservation_release(&session->rel_hash_reservation);
    session_reservation_move_if_reserved(
        &session->rel_hash_reservation, &image->hash_reservation);
    free((void *)image->expected_rel_contents);
    session_reservation_release(&image->expected_rels_reservation);
    memset(image, 0, sizeof(*image));
}

/* Bounded fresh-head publication. These private cells are called only by the
 * original framed parent after its fresh HOST admission. All forward work is
 * charged to its saved original operation; retirement never charges it. */
#ifdef WL_SESSION_TEST_HOOKS
int wl_columnar_session_hash_test_registry_allocation = -1;
int (*wl_columnar_session_hash_test_registry_move)(
    wl_columnar_eval_serial_framed_attempt_t *, unsigned, bool);
#endif

static int
wl_columnar_session_hash_registry_charge(
    wl_columnar_eval_serial_framed_attempt_t *parent, uint64_t work)
{
    if (wl_evaluation_control_active_attempt_id(parent->control)
        != parent->attempt_id) return EINVAL;
    int rc = wl_evaluation_control_charge(parent->control, work);
    if (rc) return rc;
    return wl_evaluation_control_active_attempt_id(parent->control)
           == parent->attempt_id ? 0 : EINVAL;
}

static int
wl_columnar_session_hash_registry_tail(
    wl_columnar_eval_serial_framed_attempt_t *parent)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_serial_test_framed_checkpoint)
        wl_columnar_eval_serial_test_framed_checkpoint(parent);
#endif
    return wl_columnar_session_hash_registry_charge(parent, 0);
}

static int
wl_columnar_session_hash_registry_owner(
    wl_columnar_eval_serial_framed_attempt_t *parent)
{
    if (!parent || !wl_columnar_eval_serial_framed_owner_valid(parent,
        parent->frame)) return EINVAL;
    wl_columnar_eval_serial_framed_attempt_t *original = NULL;
    int rc = wl_columnar_eval_stack_framed_context(parent->frame,
            parent->session, &original);
    return rc ? rc : original == parent ? 0 : EINVAL;
}

/* Positive RESERVED credits and genuinely canonical inert tokens only. An
 * old RELEASED payload cannot be restored by reservation_move and is rejected.
 * A noGov all-zero token is read but never initialized or otherwise changed. */
static int
wl_columnar_session_hash_registry_token(
    const wl_columnar_memory_reservation_t *token,
    wl_columnar_memory_governor_t *governor, uint64_t bytes, bool *live)
{
    uint64_t state = atomic_load_explicit(&token->state, memory_order_acquire);
    uint64_t owner = atomic_load_explicit(&token->owner_bits,
            memory_order_acquire);
    *live = false;
    if (owner || token->replacement_bytes || token->transition_kind)
        return ENOTSUP;
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY) {
        if (token->governor || token->bytes
            || (token->identity != token && (governor || token->identity)))
            return ENOTSUP;
        return 0;
    }
    if (state != WL_COLUMNAR_MEMORY_RESERVATION_RESERVED || !governor
        || !bytes || token->identity != token || token->governor != governor
        || token->bytes != bytes) return ENOTSUP;
    *live = true;
    return 0;
}

static int
wl_columnar_session_hash_registry_tuple(
    const wl_columnar_session_hash_registry_image_t *image)
{
    const wl_col_session_t *session = image->session;
    return session->rels == image->expected_rels
           && session->nrels == image->expected_nrels
           && session->rel_cap == image->expected_rel_cap
           && session->rel_hash_head == image->expected_hash_head
           && session->rel_hash_next == image->expected_hash_next
           && session->rel_hash_nbuckets == image->expected_hash_nbuckets
           && session->rel_hash_chain_cap == image->expected_hash_chain_cap
           ? 0 : EBUSY;
}

static void
wl_columnar_session_hash_registry_fail(
    wl_columnar_eval_serial_framed_attempt_t *parent, int result)
{
    if (!parent->primary_result) parent->primary_result = result;
    if (result == ENOSPC) parent->session->memory_budget_denied = true;
    parent->publication.phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_REVERSE;
    parent->publication.move = 4;
}

static void *
wl_columnar_session_hash_registry_allocate(size_t bytes)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_session_hash_test_registry_allocation == 0) return NULL;
    if (wl_columnar_session_hash_test_registry_allocation > 0)
        --wl_columnar_session_hash_test_registry_allocation;
#endif
    return malloc(bytes);
}

int
wl_columnar_session_hash_registry_attempt_begin(
    wl_columnar_eval_serial_framed_attempt_t *parent)
{
    int rc = wl_columnar_session_hash_registry_owner(parent);
    if (rc) return rc;
    wl_columnar_session_hash_registry_attempt_t *attempt = &parent->publication;
    wl_col_session_t *session = parent->session;
    if (parent->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY
        || attempt->identity || !parent->candidate_owned || !parent->candidate
        || parent->primary_result) return EBUSY;
    /* Separate the authenticated owner prefix from the complete read-only
     * registry/token preflight. No B owner exists on refusal. */
    rc = wl_columnar_session_hash_registry_charge(parent, 256);
    if (rc) return rc;
    if (!parent->candidate->name || !parent->candidate->name[0]) return EINVAL;
    if (session->nrels > session->rel_cap
        || (!!session->rels != !!session->rel_cap)
        || (!!session->rel_hash_head != !!session->rel_hash_nbuckets)
        || (!!session->rel_hash_next != !!session->rel_hash_chain_cap)
        || (session->rel_hash_nbuckets
        && ((session->rel_hash_nbuckets & (session->rel_hash_nbuckets-1))
        || session->rel_hash_nbuckets < 16
        || session->rel_hash_chain_cap < session->nrels))
        || (!session->rel_hash_nbuckets && session->rel_hash_chain_cap))
        return EINVAL;
    uint64_t rel_bytes, head_bytes, next_bytes, hash_bytes;
    if (!wl_columnar_memory_size_mul(session->rel_cap,
        sizeof(*session->rels), &rel_bytes)
        || !wl_columnar_memory_size_mul(session->rel_hash_nbuckets,
        sizeof(uint32_t), &head_bytes)
        || !wl_columnar_memory_size_mul(session->rel_hash_chain_cap,
        sizeof(uint32_t), &next_bytes)
        || !wl_columnar_memory_size_add(head_bytes, next_bytes, &hash_bytes))
        return EOVERFLOW;
    wl_columnar_memory_governor_t *governor = session->memory_governor
        ? wl_columnar_memory_governor_ref_get(session->memory_governor) : NULL;
    bool live;
    rc = wl_columnar_session_hash_registry_token(&session->rels_reservation,
            governor, rel_bytes, &live);
    if (rc) return rc;
    rc = wl_columnar_session_hash_registry_token(&session->rel_hash_reservation,
            governor, hash_bytes, &live);
    if (rc) return rc;
    /* Independent first capacities, in addition to A98:
     * physical4, private credit3, old physical3/credit2, reverse9,
     * proof6, parent bridge1, cutoff1, ACK bridge1, seal1 =31.
     * Token initialization is activation current work, not future ownership.
     * 124*192 separates15872 current and7936 future before B ownership. */
    for (unsigned i = 0; i < 124; ++i) {
        rc = wl_columnar_session_hash_registry_charge(parent, 192);
        if (rc) return rc;
    }
    rc = wl_columnar_session_hash_registry_charge(parent, 256);
    if (rc) return rc;
    wl_columnar_memory_reservation_init(&attempt->image.rels_reservation);
    wl_columnar_memory_reservation_init(&attempt->image.hash_reservation);
    wl_columnar_memory_reservation_init(
        &attempt->image.expected_rels_reservation);
    wl_columnar_memory_reservation_init(&attempt->retired_rels);
    wl_columnar_memory_reservation_init(&attempt->retired_hash);
    attempt->parent = parent;
    attempt->image.session = session;
    attempt->image.expected_rels = session->rels;
    attempt->image.expected_nrels = session->nrels;
    attempt->image.expected_rel_cap = session->rel_cap;
    attempt->image.expected_hash_head = session->rel_hash_head;
    attempt->image.expected_hash_next = session->rel_hash_next;
    attempt->image.expected_hash_nbuckets = session->rel_hash_nbuckets;
    attempt->image.expected_hash_chain_cap = session->rel_hash_chain_cap;
    attempt->old_rel_bytes = rel_bytes; attempt->old_hash_bytes = hash_bytes;
    attempt->first_hole = UINT32_MAX;
    attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_HOLE;
    attempt->identity = attempt;
    return 0;
}

static int
wl_columnar_session_hash_registry_admit(
    wl_columnar_eval_serial_framed_attempt_t *parent, uint64_t bytes,
    wl_columnar_memory_reservation_t *token, bool *owned)
{
    if (!bytes || !parent->session->memory_governor) return 0;
    wl_columnar_memory_admission_status_t status;
    int rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
        wl_columnar_memory_governor_ref_get(parent->session->memory_governor),
        bytes, token, &status, parent->control, parent->attempt_id);
    if (rc) return rc; /* Nonzero self-restores; status is not authoritative. */
    *owned = status == WL_COLUMNAR_MEMORY_ADMISSION_OK
        || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY;
    if (*owned) return 0;
    return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
}

static int
wl_columnar_session_hash_registry_forward_move(
    wl_columnar_eval_serial_framed_attempt_t *parent, unsigned index)
{
    wl_columnar_session_hash_registry_attempt_t *attempt = &parent->publication;
    wl_col_session_t *session = parent->session;
    wl_columnar_session_hash_registry_image_t *image = &attempt->image;
    wl_columnar_memory_reservation_t *source, *destination;
    uint64_t bytes;
    switch (index) {
    case 0: source = &session->rels_reservation;
        destination = &attempt->retired_rels;
        bytes = attempt->old_rel_bytes; break;
    case 1: source = &session->rel_hash_reservation;
        destination = &attempt->retired_hash;
        bytes = attempt->old_hash_bytes; break;
    case 2: source = &image->rels_reservation;
        destination = &session->rels_reservation;
        bytes = attempt->rel_bytes; break;
    case 3: source = &image->hash_reservation;
        destination = &session->rel_hash_reservation;
        bytes = attempt->hash_bytes; break;
    default: return EINVAL;
    }
    wl_columnar_memory_governor_t *governor = session->memory_governor
        ? wl_columnar_memory_governor_ref_get(session->memory_governor) : NULL;
    bool live, destination_live;
    int rc = wl_columnar_session_hash_registry_token(source, governor, bytes,
            &live);
    if (rc) return rc;
    rc = wl_columnar_session_hash_registry_token(destination, governor, 0,
            &destination_live);
    if (rc || destination_live) return rc ? rc : EINVAL;
    if (!live) {
        if (governor && index >= 2) return EINVAL;
        attempt->journal[index] = 1;
        return 0;
    }
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_session_hash_test_registry_move) {
        rc = wl_columnar_session_hash_test_registry_move(parent, index,
                false);
        if (rc) return rc;
    }
#endif
    if (!wl_columnar_memory_reservation_move(destination, source)) return EBUSY;
    attempt->journal[index] = 2;
    switch (index) {
    case 0: attempt->old_credit_rels = true; break;
    case 1: attempt->old_credit_hash = true; break;
    case 2: attempt->credit_rels = false; break;
    case 3: attempt->credit_hash = false; break;
    }
    return 0;
}

/* Separate validation from movement on noncancelable reverse turns. There is
 * no token mutation or competing lease between them in the serialized parent
 * contract. The move primitive still checks actual original-address authority. */
static int
wl_columnar_session_hash_registry_reverse_prepare(
    wl_columnar_eval_serial_framed_attempt_t *parent)
{
    wl_columnar_session_hash_registry_attempt_t *attempt = &parent->publication;
    if (!attempt->move) {
        attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_FREE_R;
        return 0;
    }
    unsigned index = attempt->move-1;
    if (attempt->journal[index] != 2) {
        attempt->journal[index] = 0; --attempt->move; return 0;
    }
    wl_columnar_memory_reservation_t *source, *destination;
    uint64_t bytes;
    switch (index) {
    case 0: source = &attempt->retired_rels;
        destination = &parent->session->rels_reservation;
        bytes = attempt->old_rel_bytes; break;
    case 1: source = &attempt->retired_hash;
        destination = &parent->session->rel_hash_reservation;
        bytes = attempt->old_hash_bytes; break;
    case 2: source = &parent->session->rels_reservation;
        destination = &attempt->image.rels_reservation;
        bytes = attempt->rel_bytes; break;
    case 3: source = &parent->session->rel_hash_reservation;
        destination = &attempt->image.hash_reservation;
        bytes = attempt->hash_bytes; break;
    default: return EINVAL;
    }
    wl_columnar_memory_governor_t *governor = parent->session->memory_governor
        ? wl_columnar_memory_governor_ref_get(
        parent->session->memory_governor) : NULL;
    bool live, destination_live;
    int rc = wl_columnar_session_hash_registry_token(source, governor, bytes,
            &live);
    if (rc || !live) return rc ? rc : EINVAL;
    rc = wl_columnar_session_hash_registry_token(destination, governor, 0,
            &destination_live);
    if (rc || destination_live) return rc ? rc : EINVAL;
    attempt->reverse_source = source;
    attempt->reverse_destination = destination;
    attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_REVERSE_MOVE;
    return 0;
}

static int
wl_columnar_session_hash_registry_reverse_move(
    wl_columnar_eval_serial_framed_attempt_t *parent)
{
    wl_columnar_session_hash_registry_attempt_t *attempt = &parent->publication;
    if (!attempt->move || attempt->move > 4 || !attempt->reverse_source
        || !attempt->reverse_destination) return EINVAL;
    unsigned index = attempt->move-1;
    if (attempt->journal[index] != 2) return EINVAL;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_session_hash_test_registry_move) {
        int rc = wl_columnar_session_hash_test_registry_move(parent, index,
                true);
        if (rc) return rc;
    }
#endif
    if (!wl_columnar_memory_reservation_move(attempt->reverse_destination,
        attempt->reverse_source)) return EBUSY;
    attempt->journal[index] = 0;
    switch (index) {
    case 0: attempt->old_credit_rels = false; break;
    case 1: attempt->old_credit_hash = false; break;
    case 2: attempt->credit_rels = true; break;
    case 3: attempt->credit_hash = true; break;
    }
    attempt->reverse_source = NULL; attempt->reverse_destination = NULL;
    --attempt->move;
    attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_REVERSE;
    return 0;
}

static int
wl_columnar_session_hash_registry_refund(
    wl_columnar_memory_reservation_t *token, bool *owned)
{
    if (*owned) {
        if (!wl_columnar_memory_rollback(token)) return EBUSY;
        *owned = false;
    }
    return 0;
}

static bool
wl_columnar_session_hash_registry_inert(
    const wl_columnar_memory_reservation_t *token)
{
    uint64_t state = atomic_load_explicit(&token->state, memory_order_acquire);
    return token->identity == token && !token->replacement_bytes
           && !token->transition_kind
           && !atomic_load_explicit(&token->owner_bits, memory_order_acquire)
           && (state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
           || state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED);
}

int
wl_columnar_session_hash_registry_attempt_step(
    wl_columnar_eval_serial_framed_attempt_t *parent)
{
    int rc = wl_columnar_session_hash_registry_owner(parent);
    if (rc) return rc;
    wl_columnar_session_hash_registry_attempt_t *attempt = &parent->publication;
    wl_columnar_session_hash_registry_image_t *image = &attempt->image;
    if (parent->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_IMAGE
        || attempt->identity != attempt || attempt->parent != parent
        || attempt->drained) return EINVAL;
    if (attempt->phase < WL_COLUMNAR_SESSION_HASH_REGISTRY_REVERSE) {
        rc = wl_columnar_session_hash_registry_charge(parent, 256);
        if (rc) {
            wl_columnar_session_hash_registry_fail(parent, rc); return 0;
        }
    }
    switch (attempt->phase) {
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_HOLE:
        if (attempt->slot == image->expected_nrels) {
            attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_SIZE;
        } else if (!image->expected_rels[attempt->slot]) {
            attempt->first_hole = attempt->slot;
            attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_SIZE;
        } else ++attempt->slot;
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_SIZE: {
        if (attempt->first_hole == UINT32_MAX
            && image->expected_nrels == UINT32_MAX) {
            rc = EOVERFLOW; break;
        }
        image->nrels = image->expected_nrels
            + (attempt->first_hole == UINT32_MAX);
        image->rel_cap = image->expected_rel_cap > image->nrels
            ? image->expected_rel_cap : image->nrels;
        if (!image->nrels || image->nrels > UINT32_MAX / 2) {
            rc = EOVERFLOW; break;
        }
        image->hash_nbuckets = session_rel_next_pow2(image->nrels * 2);
        image->hash_chain_cap = image->nrels;
        uint64_t head_bytes, next_bytes;
        if (!image->hash_nbuckets
            || !wl_columnar_memory_size_mul(image->rel_cap,
            sizeof(*image->rels), &attempt->rel_bytes)
            || !wl_columnar_memory_size_mul(image->expected_nrels,
            sizeof(*image->expected_rel_contents), &attempt->expected_bytes)
            || !wl_columnar_memory_size_mul(image->hash_nbuckets,
            sizeof(uint32_t), &head_bytes)
            || !wl_columnar_memory_size_mul(image->nrels,
            sizeof(uint32_t), &next_bytes)
            || !wl_columnar_memory_size_add(head_bytes, next_bytes,
            &attempt->hash_bytes)
            || attempt->rel_bytes > SIZE_MAX ||
            attempt->expected_bytes > SIZE_MAX
            || head_bytes > SIZE_MAX || next_bytes > SIZE_MAX) {
            rc = EOVERFLOW; break;
        }
        attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_ADMIT_R;
        break;
    }
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_ADMIT_R:
        rc = wl_columnar_session_hash_registry_admit(parent, attempt->rel_bytes,
                &image->rels_reservation, &attempt->credit_rels);
        if (!rc) attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_ALLOC_R;
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_ALLOC_R:
        image->rels = (col_rel_t **)wl_columnar_session_hash_registry_allocate(
            (size_t)attempt->rel_bytes);
        if (!image->rels) rc = ENOMEM;
        else attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_ADMIT_E;
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_ADMIT_E:
        rc = wl_columnar_session_hash_registry_admit(parent,
                attempt->expected_bytes, &image->expected_rels_reservation,
                &attempt->credit_expected);
        if (!rc) attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_ALLOC_E;
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_ALLOC_E:
        if (attempt->expected_bytes) {
            image->expected_rel_contents =
                (col_rel_t **)wl_columnar_session_hash_registry_allocate(
                (size_t)attempt->expected_bytes);
            if (!image->expected_rel_contents) {
                rc = ENOMEM; break;
            }
        }
        attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_ADMIT_H;
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_ADMIT_H:
        rc = wl_columnar_session_hash_registry_admit(parent,
                attempt->hash_bytes,
                &image->hash_reservation, &attempt->credit_hash);
        if (!rc) attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_ALLOC_HEAD;
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_ALLOC_HEAD:
        image->hash_head = wl_columnar_session_hash_registry_allocate(
            (size_t)image->hash_nbuckets * sizeof(uint32_t));
        if (!image->hash_head) rc = ENOMEM;
        else attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_ALLOC_NEXT;
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_ALLOC_NEXT:
        image->hash_next = wl_columnar_session_hash_registry_allocate(
            (size_t)image->nrels * sizeof(uint32_t));
        if (!image->hash_next) rc = ENOMEM;
        else {
            attempt->cursor = 0;
            attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_ZERO_R;
        }
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_ZERO_R:
        if (attempt->cursor == image->rel_cap) {
            attempt->cursor = 0;
            attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_ZERO_HEAD;
        } else image->rels[attempt->cursor++] = NULL;
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_ZERO_HEAD:
        if (attempt->cursor == image->hash_nbuckets) {
            attempt->cursor = 0;
            attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_ZERO_NEXT;
        } else image->hash_head[attempt->cursor++] = UINT32_MAX;
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_ZERO_NEXT:
        if (attempt->cursor == image->nrels) {
            attempt->cursor = 0;
            attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_COPY;
        } else image->hash_next[attempt->cursor++] = UINT32_MAX;
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_COPY:
        if (attempt->cursor == image->expected_nrels) {
            attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_INSERT;
        } else {
            col_rel_t *rel = image->expected_rels[attempt->cursor];
            image->expected_rel_contents[attempt->cursor] = rel;
            image->rels[attempt->cursor] = rel;
            attempt->byte = 0;
            if (rel && !rel->name) {
                rc = EINVAL; break;
            }
            if (rel) attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_COMPARE;
            else ++attempt->cursor;
        }
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_COMPARE: {
        unsigned char a =
            (unsigned char)image->rels[attempt->cursor]->name[attempt->byte];
        unsigned char b = (unsigned char)parent->candidate->name[attempt->byte];
        if (a != b) {
            ++attempt->cursor;
            attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_COPY;
        }else if (!a) rc = EEXIST;
        else if (attempt->byte == UINT32_MAX) rc = EOVERFLOW;
        else ++attempt->byte;
        break;
    }
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_INSERT:
        image->rels[attempt->first_hole == UINT32_MAX
            ? image->expected_nrels : attempt->first_hole] = parent->candidate;
        attempt->cursor = 0;
        attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_HASH_SLOT;
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_HASH_SLOT:
        if (attempt->cursor == image->nrels) {
            attempt->cursor = 0;
            attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_VALIDATE;
        } else if (!image->rels[attempt->cursor]) ++attempt->cursor;
        else {
            attempt->byte = 0;
            attempt->name_hash = UINT64_C(14695981039346656037);
            attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_HASH_BYTE;
        }
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_HASH_BYTE: {
        unsigned char c =
            (unsigned char)image->rels[attempt->cursor]->name[attempt->byte];
        if (!c) attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_HASH_LINK;
        else if (attempt->byte == UINT32_MAX) rc = EOVERFLOW;
        else {
            attempt->name_hash ^= c;
            attempt->name_hash *= UINT64_C(1099511628211); ++attempt->byte;
        }
        break;
    }
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_HASH_LINK: {
        uint32_t bucket = (uint32_t)(attempt->name_hash &
            (image->hash_nbuckets-1));
        image->hash_next[attempt->cursor] = image->hash_head[bucket];
        image->hash_head[bucket] = attempt->cursor++;
        attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_HASH_SLOT;
        break;
    }
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_VALIDATE:
        rc = wl_columnar_session_hash_registry_tuple(image);
        if (rc) break;
        if (attempt->cursor == image->expected_nrels) {
            attempt->move = 0;
            attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_MOVE;
        } else if (image->expected_rels[attempt->cursor]
            != image->expected_rel_contents[attempt->cursor]) rc = EBUSY;
        else ++attempt->cursor;
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_MOVE:
        rc = wl_columnar_session_hash_registry_tuple(image);
        if (rc) break;
        if (attempt->move == 4) {
            attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_CHECK_R;
        } else {
            rc = wl_columnar_session_hash_registry_forward_move(parent,
                    attempt->move);
            if (!rc) ++attempt->move;
        }
        break;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_CHECK_R:
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_CHECK_H: {
        bool hash = attempt->phase == WL_COLUMNAR_SESSION_HASH_REGISTRY_CHECK_H;
        wl_columnar_memory_governor_t *governor =
            parent->session->memory_governor
            ? wl_columnar_memory_governor_ref_get(
            parent->session->memory_governor) : NULL;
        bool live_new, live_old;
        rc = wl_columnar_session_hash_registry_token(hash
                ? &parent->session->rel_hash_reservation
                : &parent->session->rels_reservation, governor,
                hash ? attempt->hash_bytes : attempt->rel_bytes, &live_new);
        if (!rc) rc = wl_columnar_session_hash_registry_token(hash
                ? &attempt->retired_hash : &attempt->retired_rels, governor,
                    hash ? attempt->old_hash_bytes : attempt->old_rel_bytes,
                    &live_old);
        if (!rc && (live_new != (governor != NULL) || live_old != (hash
            ? attempt->old_credit_hash : attempt->old_credit_rels))) rc =
                EINVAL;
        if (!rc) attempt->phase =
                hash ? WL_COLUMNAR_SESSION_HASH_REGISTRY_CUTOFF
                : WL_COLUMNAR_SESSION_HASH_REGISTRY_CHECK_H;
        break;
    }
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_CUTOFF: {
        rc = wl_columnar_session_hash_registry_tuple(image);
        if (!rc) rc = wl_columnar_session_hash_registry_tail(parent);
        if (rc) break;
        /* No callbacks, token move, allocation, credit or poll after cutoff. */
        wl_col_session_t *session = parent->session;
        session->rels = image->rels; session->nrels = image->nrels;
        session->rel_cap = image->rel_cap;
        session->rel_hash_head = image->hash_head;
        session->rel_hash_next = image->hash_next;
        session->rel_hash_nbuckets = image->hash_nbuckets;
        session->rel_hash_chain_cap = image->hash_chain_cap;
        image->rels = NULL; image->hash_head = NULL; image->hash_next = NULL;
        parent->candidate = NULL; parent->candidate_owned = false;
        parent->materialized = false; parent->committed = true;
        if (parent->controlled_materialized_batch_fallback
            && session->controlled_materialized_batch_published_count
            != UINT64_MAX)
            ++session->controlled_materialized_batch_published_count;
        attempt->committed = true;
        attempt->journal[0] = 0; attempt->journal[1] = 0;
        attempt->journal[2] = 0; attempt->journal[3] = 0;
        attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_FREE_E;
        return 0;
    }
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_REVERSE:
        parent->cleanup_result =
            wl_columnar_session_hash_registry_reverse_prepare(parent);
        return 0;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_REVERSE_MOVE:
        parent->cleanup_result =
            wl_columnar_session_hash_registry_reverse_move(parent);
        return 0;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_FREE_R:
        free((void *)image->rels); image->rels = NULL;
        attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_FREE_HEAD; return 0;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_FREE_HEAD:
        free(image->hash_head); image->hash_head = NULL;
        attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_FREE_NEXT; return 0;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_FREE_NEXT:
        free(image->hash_next); image->hash_next = NULL;
        attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_CREDIT_R; return 0;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_CREDIT_R:
        rc = wl_columnar_session_hash_registry_refund(&image->rels_reservation,
                &attempt->credit_rels);
        if (!rc) attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_CREDIT_H;
        parent->cleanup_result = rc; return 0;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_CREDIT_H:
        rc = wl_columnar_session_hash_registry_refund(&image->hash_reservation,
                &attempt->credit_hash);
        if (!rc) attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_FREE_E;
        parent->cleanup_result = rc; return 0;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_FREE_E:
        free((void *)image->expected_rel_contents);
        image->expected_rel_contents = NULL;
        attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_CREDIT_E; return 0;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_CREDIT_E:
        rc =
            wl_columnar_session_hash_registry_refund(
            &image->expected_rels_reservation,
            &attempt->credit_expected);
        parent->cleanup_result = rc;
        if (!rc) {
            attempt->cursor = 0;
            if (attempt->committed) {
                attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_OLD_R;
                parent->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_GATE_RELEASE;
            } else attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_PROOF;
        }
        return 0;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_OLD_R:
        if (parent->child_bound || parent->child.identity) return EBUSY;
        free((void *)image->expected_rels); image->expected_rels = NULL;
        attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_OLD_HEAD; return 0;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_OLD_HEAD:
        free(image->expected_hash_head); image->expected_hash_head = NULL;
        attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_OLD_NEXT; return 0;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_OLD_NEXT:
        free(image->expected_hash_next); image->expected_hash_next = NULL;
        attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_OLD_CREDIT_R;
        return 0;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_OLD_CREDIT_R:
        rc = wl_columnar_session_hash_registry_refund(&attempt->retired_rels,
                &attempt->old_credit_rels);
        if (!rc) attempt->phase =
                WL_COLUMNAR_SESSION_HASH_REGISTRY_OLD_CREDIT_H;
        parent->cleanup_result = rc; return 0;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_OLD_CREDIT_H:
        rc = wl_columnar_session_hash_registry_refund(&attempt->retired_hash,
                &attempt->old_credit_hash);
        if (!rc) {
            attempt->cursor = 0;
            attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_PROOF;
        }
        parent->cleanup_result = rc; return 0;
    case WL_COLUMNAR_SESSION_HASH_REGISTRY_PROOF: {
        const wl_columnar_memory_reservation_t *token;
        switch (attempt->cursor) {
        case 0: token = &image->rels_reservation; break;
        case 1: token = &image->hash_reservation; break;
        case 2: token = &image->expected_rels_reservation; break;
        case 3: token = &attempt->retired_rels; break;
        case 4: token = &attempt->retired_hash; break;
        default: token = NULL; break;
        }
        if (token) {
            parent->cleanup_result =
                wl_columnar_session_hash_registry_inert(token)
                ? 0 : EBUSY;
            if (!parent->cleanup_result) ++attempt->cursor;
        } else if (image->rels || image->hash_head || image->hash_next
            || image->expected_rel_contents || attempt->credit_rels
            || attempt->credit_hash || attempt->credit_expected
            || attempt->old_credit_rels || attempt->old_credit_hash
            || attempt->journal[0] || attempt->journal[1]
            || attempt->journal[2] || attempt->journal[3]
            || attempt->reverse_source || attempt->reverse_destination
            || (attempt->committed && (image->expected_rels
            || image->expected_hash_head || image->expected_hash_next))) {
            parent->cleanup_result = EBUSY;
        } else {
            /* Abort keeps session-owned old arrays; forget only our borrows. */
            image->expected_rels = NULL; image->expected_hash_head = NULL;
            image->expected_hash_next = NULL;
            attempt->drained = true;
            attempt->phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_DRAINED;
            parent->cleanup_result = 0; parent->cursor = 0;
            parent->phase = attempt->committed
                ? WL_COLUMNAR_EVAL_SERIAL_FRAMED_SPARE_PROOF
                : WL_COLUMNAR_EVAL_SERIAL_FRAMED_CANDIDATE_ABORT;
        }
        return 0;
    }
    default: return EINVAL;
    }
    if (!rc) rc = wl_columnar_session_hash_registry_tail(parent);
    if (rc) wl_columnar_session_hash_registry_fail(parent, rc);
    return 0;
}
