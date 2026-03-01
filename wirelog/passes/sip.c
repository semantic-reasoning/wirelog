/*
 * sip.c - Semijoin Information Passing Optimization
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Inserts SEMIJOIN nodes into join chains of 3+ atoms to pre-filter
 * intermediate results before joins.
 *
 * For each non-innermost JOIN in a left-deep chain, a SEMIJOIN is
 * inserted between the JOIN and its left child.  The SEMIJOIN filters
 * the intermediate result (left child output) against the JOIN's
 * right relation, keeping only rows whose join keys appear in the
 * right relation.  This reduces intermediate result sizes.
 *
 * Transformation (outside-in walk):
 *
 *   JOIN_outer(key=w)              JOIN_outer(key=w)
 *     JOIN_inner(key=y)              SEMIJOIN(key=w, right=c)
 *       SCAN(a)                        JOIN_inner(key=y)
 *       SCAN(b)            ->            SCAN(a)
 *     SCAN(c)                            SCAN(b)
 *                                      SCAN(c) [clone]
 *                                    SCAN(c)
 *
 * Idempotency: if a SEMIJOIN already sits between a JOIN and its
 * left child, no additional SEMIJOIN is inserted for that level.
 */

#include "sip.h"
#include "../ir/ir.h"
#include "../ir/program.h"

#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Helper: create a minimal SCAN clone (relation name + column names)       */
/* ======================================================================== */

static wirelog_ir_node_t *
clone_scan_metadata(const wirelog_ir_node_t *scan)
{
    if (!scan || scan->type != WIRELOG_IR_SCAN)
        return NULL;

    wirelog_ir_node_t *clone = wl_ir_node_create(WIRELOG_IR_SCAN);
    if (!clone)
        return NULL;

    if (scan->relation_name)
        wl_ir_node_set_relation(clone, scan->relation_name);

    if (scan->column_names && scan->column_count > 0) {
        clone->column_names
            = (char **)calloc(scan->column_count, sizeof(char *));
        if (!clone->column_names) {
            wl_ir_node_free(clone);
            return NULL;
        }
        clone->column_count = scan->column_count;
        for (uint32_t i = 0; i < scan->column_count; i++) {
            if (scan->column_names[i])
                clone->column_names[i] = strdup_safe(scan->column_names[i]);
        }
    }

    return clone;
}

/* ======================================================================== */
/* Helper: find the left-deep join chain root, skipping wrappers            */
/* ======================================================================== */

static wirelog_ir_node_t *
descend_to_join(wirelog_ir_node_t *node)
{
    while (node) {
        switch (node->type) {
        case WIRELOG_IR_PROJECT:
        case WIRELOG_IR_FLATMAP:
        case WIRELOG_IR_FILTER:
        case WIRELOG_IR_ANTIJOIN:
            if (node->child_count > 0) {
                node = node->children[0];
                continue;
            }
            return NULL;
        case WIRELOG_IR_JOIN:
            return node;
        default:
            return NULL;
        }
    }
    return NULL;
}

/* ======================================================================== */
/* Helper: count join depth of a left-deep chain                            */
/* ======================================================================== */

static uint32_t
count_join_depth(const wirelog_ir_node_t *node)
{
    uint32_t depth = 0;
    while (node && node->type == WIRELOG_IR_JOIN) {
        depth++;
        node = (node->child_count > 0) ? node->children[0] : NULL;
        /* Skip over any SEMIJOIN already inserted (for idempotency) */
        if (node && node->type == WIRELOG_IR_SEMIJOIN)
            node = (node->child_count > 0) ? node->children[0] : NULL;
    }
    return depth;
}

/* ======================================================================== */
/* Core: insert semijoins in a join chain                                   */
/* ======================================================================== */

/**
 * Walk a left-deep join chain from outside to inside.  For each JOIN
 * whose left child is another JOIN (i.e., not the innermost), insert
 * a SEMIJOIN between the JOIN and its left child.
 *
 * Returns the number of semijoins inserted, or -1 on allocation error.
 */
static int
insert_semijoins_in_chain(wirelog_ir_node_t *join_root)
{
    int inserted = 0;
    wirelog_ir_node_t *node = join_root;

    while (node && node->type == WIRELOG_IR_JOIN && node->child_count >= 2) {
        wirelog_ir_node_t *left = node->children[0];
        wirelog_ir_node_t *right = node->children[1];

        /* Skip if left child is already a SEMIJOIN (idempotency) */
        if (left && left->type == WIRELOG_IR_SEMIJOIN) {
            /* Descend through the SEMIJOIN to the next JOIN */
            node = (left->child_count > 0) ? left->children[0] : NULL;
            continue;
        }

        /* Only insert if left child is a JOIN (chain of 3+ atoms) */
        if (!left || left->type != WIRELOG_IR_JOIN)
            break;

        /* Right child must be a SCAN to clone */
        if (!right || right->type != WIRELOG_IR_SCAN)
            break;

        /* Create a SEMIJOIN node */
        wirelog_ir_node_t *sj = wl_ir_node_create(WIRELOG_IR_SEMIJOIN);
        if (!sj)
            return -1;

        /* Copy join keys from the parent JOIN to the SEMIJOIN */
        if (node->join_key_count > 0) {
            sj->join_key_count = node->join_key_count;
            sj->join_left_keys
                = (char **)calloc(node->join_key_count, sizeof(char *));
            sj->join_right_keys
                = (char **)calloc(node->join_key_count, sizeof(char *));
            if (!sj->join_left_keys || !sj->join_right_keys) {
                wl_ir_node_free(sj);
                return -1;
            }
            for (uint32_t k = 0; k < node->join_key_count; k++) {
                if (node->join_left_keys[k])
                    sj->join_left_keys[k]
                        = strdup_safe(node->join_left_keys[k]);
                if (node->join_right_keys[k])
                    sj->join_right_keys[k]
                        = strdup_safe(node->join_right_keys[k]);
            }
        }

        /* SEMIJOIN child[0] = the left subtree (intermediate result) */
        wl_ir_node_add_child(sj, left);

        /* SEMIJOIN child[1] = clone of right SCAN (for relation name) */
        wirelog_ir_node_t *scan_clone = clone_scan_metadata(right);
        if (!scan_clone) {
            /* Undo: detach left from sj before freeing */
            sj->children[0] = NULL;
            sj->child_count = 0;
            wl_ir_node_free(sj);
            return -1;
        }
        wl_ir_node_add_child(sj, scan_clone);

        /* Re-point parent JOIN's left child to the SEMIJOIN */
        node->children[0] = sj;

        inserted++;

        /* Move to the next level (the left child, which is a JOIN) */
        node = left;
    }

    return inserted;
}

/* ======================================================================== */
/* Tree walker: process a single IR tree (or UNION children)                */
/* ======================================================================== */

static int
sip_process_tree(wirelog_ir_node_t *ir, wl_sip_stats_t *stats)
{
    if (!ir)
        return 0;

    /* Recurse into UNION children */
    if (ir->type == WIRELOG_IR_UNION) {
        for (uint32_t i = 0; i < ir->child_count; i++) {
            int rc = sip_process_tree(ir->children[i], stats);
            if (rc != 0)
                return rc;
        }
        return 0;
    }

    /* Find the join chain root (skip PROJECT, FLATMAP, FILTER, ANTIJOIN) */
    wirelog_ir_node_t *join_root = descend_to_join(ir);
    if (!join_root)
        return 0;

    /* Need at least 2 JOINs (3+ atoms) to apply SIP */
    uint32_t depth = count_join_depth(join_root);
    if (depth < 2)
        return 0;

    if (stats)
        stats->chains_examined++;

    int inserted = insert_semijoins_in_chain(join_root);
    if (inserted < 0)
        return -1;

    if (stats)
        stats->semijoins_inserted += (uint32_t)inserted;

    return 0;
}

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

int
wl_sip_apply(struct wirelog_program *prog, wl_sip_stats_t *stats)
{
    if (!prog)
        return -2;

    if (stats) {
        stats->semijoins_inserted = 0;
        stats->chains_examined = 0;
    }

    if (!prog->relation_irs)
        return 0;

    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (prog->relation_irs[i]) {
            int rc = sip_process_tree(prog->relation_irs[i], stats);
            if (rc != 0)
                return rc;
        }
    }

    return 0;
}
