/*
 * wirelog-ir.h - wirelog Intermediate Representation API
 *
 * Copyright (C) CleverPlant
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Licensed under LGPL-3.0-or-later
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.  If not, see
 * <https://www.gnu.org/licenses/lgpl-3.0.html>.
 */

/**
 * @file wirelog-ir.h
 * @brief Intermediate-representation (IR) inspection API for compiled wirelog programs.
 */

#ifndef WIRELOG_IR_H
#define WIRELOG_IR_H

#include "wirelog/wirelog-export.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

/* ======================================================================== */
/* IR Node Types                                                            */
/* ======================================================================== */

typedef struct wirelog_ir_node wirelog_ir_node_t;

/**
 * wirelog_ir_node_type_t:
 *
 * Intermediate representation operator node types
 */
typedef enum {
    WIRELOG_IR_SCAN,               /* Scan base relation */
    WIRELOG_IR_PROJECT,            /* Column projection */
    WIRELOG_IR_FILTER,             /* Predicate filter */
    WIRELOG_IR_JOIN,               /* Join operator */
    WIRELOG_IR_FLATMAP,            /* Fused join+map+filter */
    WIRELOG_IR_AGGREGATE,          /* Aggregation */
    WIRELOG_IR_ANTIJOIN,           /* Negation (antijoin) */
    WIRELOG_IR_UNION,              /* Union (append) */
    WIRELOG_IR_SEMIJOIN,           /* Semijoin (SIP pre-filter) */
    WIRELOG_IR_COMPOUND_INLINE,    /* Inline compound: f/N inline (Issue #531) */
    WIRELOG_IR_COMPOUND_SIDE,      /* Side-relation compound: f/N side (Issue #531) */
} wirelog_ir_node_type_t;

/* ======================================================================== */
/* IR Node Operations                                                       */
/* ======================================================================== */

/**
 * wirelog_ir_node_get_type:
 * @node: IR node
 *
 * Get the type of an IR node.
 *
 * Returns: Node type
 */
WIRELOG_PUBLIC wirelog_ir_node_type_t
wirelog_ir_node_get_type(const wirelog_ir_node_t *node);

/**
 * wirelog_ir_node_get_relation_name:
 * @node: IR node
 *
 * Get the output relation name of an IR node.
 *
 * Returns: (transfer none): Relation name string
 */
WIRELOG_PUBLIC const char *
wirelog_ir_node_get_relation_name(const wirelog_ir_node_t *node);

/**
 * wirelog_ir_node_get_child_count:
 * @node: IR node
 *
 * Get the number of child nodes.
 *
 * Returns: Child count (0 for leaf nodes like SCAN)
 */
WIRELOG_PUBLIC uint32_t
wirelog_ir_node_get_child_count(const wirelog_ir_node_t *node);

/**
 * wirelog_ir_node_get_child:
 * @node: IR node
 * @index: Child index (0-based)
 *
 * Get a child node.
 *
 * Returns: (transfer none): Child node, or NULL if invalid index
 */
WIRELOG_PUBLIC const wirelog_ir_node_t *
wirelog_ir_node_get_child(const wirelog_ir_node_t *node, uint32_t index);

/* ======================================================================== */
/* IR Printing / Debugging                                                  */
/* ======================================================================== */

/**
 * wirelog_ir_node_print:
 * @node: IR node to print
 * @indent: Indentation level
 *
 * Print an IR node (for debugging).
 */
WIRELOG_PUBLIC void
wirelog_ir_node_print(const wirelog_ir_node_t *node, uint32_t indent);

/**
 * wirelog_ir_node_to_string:
 * @node: IR node
 *
 * Convert an IR node to a string representation.
 *
 * Returns: (transfer full): String representation (must be freed)
 */
WIRELOG_PUBLIC char *
wirelog_ir_node_to_string(const wirelog_ir_node_t *node);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_IR_H */
