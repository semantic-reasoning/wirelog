/*
 * sip.h - Semijoin Information Passing Optimization
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * SIP pre-filters intermediate join results by inserting SEMIJOIN nodes
 * before joins in chains of 3+ atoms.  A SEMIJOIN keeps only rows from
 * the left (intermediate) collection whose join keys appear in the right
 * (next-to-join) relation, reducing intermediate result sizes.
 *
 * Transformation (3-atom chain, before -> after):
 *
 *   JOIN(key=w)                    JOIN(key=w)
 *     JOIN(key=y)                    SEMIJOIN(key=w, right=c)
 *       SCAN(a)          ->            JOIN(key=y)
 *       SCAN(b)                          SCAN(a)
 *     SCAN(c)                            SCAN(b)
 *                                      SCAN(c) [clone]
 *                                    SCAN(c)
 *
 * Pipeline position: runs after JPP (join ordering), before DD plan gen.
 *
 *   Parse -> IR -> Stratify -> Fusion -> JPP -> SIP -> DD Plan -> FFI
 */

#ifndef WIRELOG_SIP_H
#define WIRELOG_SIP_H

#include <stdint.h>

struct wirelog_program;

/**
 * wl_sip_stats_t:
 *
 * Statistics from a single SIP pass invocation.
 *
 * @semijoins_inserted:        Number of SEMIJOIN nodes inserted in chains
 *                             of 3+ atoms (standard SIP).
 * @chains_examined:           Number of join chains examined (>= 3 atoms).
 * @demand_semijoins_inserted: Number of demand SEMIJOIN nodes inserted for
 *                             2-atom joins with recursive left scans
 *                             (demand-driven filtering, Issue #192).
 */
typedef struct {
    uint32_t semijoins_inserted;
    uint32_t chains_examined;
    uint32_t demand_semijoins_inserted;
} wl_sip_stats_t;

/**
 * wl_sip_apply:
 * @prog:  (borrow): Program to optimize.  Must not be NULL.
 * @stats: (out) (nullable): If non-NULL, receives pass statistics.
 *
 * Insert SEMIJOIN nodes into join chains of 3+ atoms to pre-filter
 * intermediate results.  Operates in-place on the program's merged
 * relation IR trees.
 *
 * Returns:
 *    0: Success (possibly zero insertions if no eligible chains).
 *   -2: Invalid input (NULL program).
 */
int
wl_sip_apply(struct wirelog_program *prog, wl_sip_stats_t *stats);

#endif /* WIRELOG_SIP_H */
