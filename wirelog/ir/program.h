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
    bool has_input;
    bool has_output;
    bool has_printsize;
    /* .input parameters */
    char **input_param_names;
    char **input_param_values;
    uint32_t input_param_count;
    /* .output parameters */
    char *
        output_file; /* filename from .output relation(filename="..."), or NULL */
    /* Inline facts (row-major int64_t array) */
    int64_t *fact_data;
    uint32_t fact_count;
    uint32_t fact_capacity;
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
