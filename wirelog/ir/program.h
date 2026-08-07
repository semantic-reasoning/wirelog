/*
 * program.h - wirelog Program Internal Structure
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 * Defines the internal structure of wirelog_program_t (opaque in public header).
 */

#ifndef WIRELOG_IR_PROGRAM_INTERNAL_H
#define WIRELOG_IR_PROGRAM_INTERNAL_H

#include "ir.h"
#include "../intern.h"
#include "../wirelog-types.h"
#include "../passes/magic_sets.h"

/* Forward declaration for AST (used only in program struct) */
struct wl_parser_ast_node;
typedef struct wl_parser_ast_node wl_parser_ast_node_t;

/* ======================================================================== */
/* Relation Metadata                                                        */
/* ======================================================================== */

typedef struct {
    char *name;
    wirelog_column_t *columns;
    uint32_t column_count;
    /* Issue #977: true once a `.decl` for this relation has been collected.
     * Distinguishes "declared with zero columns" (`.decl p()`, which parses
     * and leaves column_count == 0) from "never declared" -- a relation
     * created implicitly by a rule head or by a fact.  `column_count > 0`
     * cannot make that distinction, so fact-arity validation keys off this
     * flag instead.
     *
     * The predicate is specifically "the user wrote a `.decl`", not "some
     * arity is on record".  wl_ir_program_add_magic_relation() sets a real
     * column_count without setting this, deliberately: magic relations are
     * compiler-generated and must not be validated against user-facing
     * arity rules.  That is inert today -- they are created after metadata
     * collection and never carry facts -- but any future consumer of this
     * flag should read it as "user-declared", not "has a known width". */
    bool has_decl;
    bool has_input;
    bool has_output;
    bool has_printsize;
    /* .input parameters */
    char **input_param_names;
    char **input_param_values;
    uint32_t input_param_count;
    char *input_io_scheme;   /* strdup'd from io="..." param; NULL => default "csv" */
    /* .output parameters */
    char *
        output_file; /* filename from .output relation(filename="..."), or NULL */
    /* Inline facts (row-major int64_t array) */
    int64_t *fact_data;
    uint32_t fact_count;
    uint32_t fact_capacity;
    /* Issue #535: RDF named-graph support */
    bool has_graph_column;
    uint32_t graph_column_index;  /* valid only when has_graph_column == true */
} wl_ir_relation_info_t;

/* ======================================================================== */
/* Rule IR                                                                  */
/* ======================================================================== */

typedef struct {
    char *head_relation;
    wirelog_ir_node_t *ir_root;
} wl_ir_rule_ir_t;

/* ======================================================================== */
/* Program Structure                                                        */
/* ======================================================================== */

struct wirelog_program {
    /* Relation metadata */
    wl_ir_relation_info_t *relations;
    uint32_t relation_count;
    uint32_t relation_capacity;

    /* Schemas (synthesized from relation metadata for public API) */
    wirelog_schema_t *schemas;

    /* Strata (stub: single stratum until stratification is implemented) */
    wirelog_stratum_t *strata;
    uint32_t stratum_count;

    /* Rule IR trees */
    wl_ir_rule_ir_t *rules;
    uint32_t rule_count;
    uint32_t rule_capacity;

    /* Merged per-relation IR (rules with same head UNIONed) */
    wirelog_ir_node_t **relation_irs;

    /* Stratification flag */
    bool is_stratified;

    /* Source AST (retained for debugging, freed on program_free) */
    wl_parser_ast_node_t *ast;

    /* Symbol intern table (string -> int64 mapping) */
    wl_intern_t *intern;

    /* Magic Sets pass metadata */
    bool magic_sets_applied;       /* True after magic sets pass */
    uint32_t magic_relation_count; /* Number of magic relations added */

    /* .query demands collected from parser */
    wl_magic_demand_t *demands;
    uint32_t demand_count;
    uint32_t demand_capacity;

    /* Public optimizer facade stats (#841). */
    bool optimizer_stats_valid;
    uint32_t optimizer_original_node_count;
    uint32_t optimizer_optimized_node_count;
    uint32_t optimizer_passes_applied;
    uint32_t optimizer_fusion_count;
    uint32_t optimizer_join_reorders;
    double optimizer_time_ms;
};

/* ======================================================================== */
/* Program API (internal)                                                   */
/* ======================================================================== */

struct wirelog_program *
wl_ir_program_create(void);
void
wl_ir_program_free(struct wirelog_program *program);

int
wl_ir_program_collect_metadata(struct wirelog_program *program,
    const wl_parser_ast_node_t *ast);

int
wl_ir_program_convert_rules(struct wirelog_program *program,
    const wl_parser_ast_node_t *ast);

int
wl_ir_program_merge_unions(struct wirelog_program *program);

void
wl_ir_program_build_schemas(struct wirelog_program *program);
void
wl_ir_program_build_default_stratum(struct wirelog_program *program);

/**
 * wl_ir_program_add_magic_relation:
 * Add a new relation with the given name and column count.
 * No-ops if relation already exists.
 * Returns 0 on success, -1 on memory error.
 */
int
wl_ir_program_add_magic_relation(struct wirelog_program *prog, const char *name,
    uint32_t column_count);

/**
 * wl_ir_program_add_magic_rule:
 * Add a new rule with the given head relation and IR tree.
 * Takes ownership of ir_root.
 * Returns 0 on success, -1 on memory error.
 */
int
wl_ir_program_add_magic_rule(struct wirelog_program *prog,
    const char *head_relation,
    wirelog_ir_node_t *ir_root);

/**
 * wl_ir_program_rebuild_relation_irs:
 * Free existing relation_irs and rebuild from current rules[].
 * Must be called after adding magic rules/relations before plan generation.
 * Returns 0 on success, -1 on memory error.
 */
int
wl_ir_program_rebuild_relation_irs(struct wirelog_program *prog);

/**
 * wl_ir_program_free_strata:
 * Free existing strata array (for re-stratification after magic sets).
 */
void
wl_ir_program_free_strata(struct wirelog_program *prog);

#endif /* WIRELOG_IR_PROGRAM_INTERNAL_H */
