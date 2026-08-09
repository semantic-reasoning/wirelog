/*
 * columnar/frontier.c - wirelog Frontier & Affected Strata Detection (Phase 4)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Affected strata and rule detection for incremental re-evaluation.
 * Extracted from backend/columnar_nanoarrow.c for modular compilation.
 */

#include "columnar/internal.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#ifdef __SSE2__
#include <emmintrin.h>
#endif

#ifdef __ARM_NEON__
#include <arm_neon.h>
#endif

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
