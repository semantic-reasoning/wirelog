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
/* wirelog_error_t, for wl_ir_parse_string_err() below (#979). */
#include "../wirelog.h"
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
     * cannot make that distinction, so both the fact-arity and the
     * rule-head-arity validation passes key off this flag instead.
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

/* The number of PHYSICAL columns a relation's `.decl` describes -- the row
 * stride of every buffer that stores or transports its tuples.
 *
 * An `inline` compound column is stored as `compound_arity` contiguous
 * physical slots; a `side` compound is a single handle slot, and so is every
 * scalar.  This is the same walk collect_decl() (ir/program.c) runs to assign
 * compound_inline_col_offset -- for every declaration the parser can
 * produce, the prefix sum there ends at exactly this value -- and the same
 * layout col_rel_t records in compound_arity_map.
 *
 * Physical width is authoritative for every storage question: fact-buffer
 * and `.input` row strides, the `num_cols` a reader is handed, the width a
 * relation is materialised at.  The *logical* column_count stays
 * authoritative for every declaration question: columns[], wirelog_schema_t,
 * wirelog_program_get_schema().  Issue #985 is what happened while the two
 * were confused for each other.
 *
 * Derived on demand rather than stored, because column_count has more than
 * one writer -- collect_decl(), which is re-entrant for a repeated `.decl`
 * (#724), and wl_ir_program_add_magic_relation(), which calloc()s columns[]
 * and never runs the offset loop at all.  A cached field would have to be
 * refreshed by both; a derivation cannot go stale.  The calloc'd case is
 * exact rather than merely safe: WIRELOG_COMPOUND_KIND_NONE is 0, so every
 * zeroed column contributes 1 and the sum is column_count.
 *
 * The `!rel->columns` early return is unreachable for a nonzero
 * column_count and is not covered by a test on purpose -- there is no
 * observable behaviour to pin.  Both writers named above allocate
 * columns[] before assigning column_count and bail out on allocation
 * failure without assigning it, so `column_count > 0 && columns == NULL`
 * cannot be constructed; the branch exists so that the zero case reads as
 * a deliberate 0 rather than as a loop that happens not to run.  A mutant
 * that changes it to `return 0` is an equivalent mutant, not a coverage
 * gap.
 *
 * The INLINE arm deliberately adds compound_arity unguarded, mirroring
 * collect_decl() exactly rather than defending against a zero arity.  A
 * guard such as `compound_arity > 0 ? compound_arity : 1` would look safer
 * but is the opposite: it is the only construct that could make the two
 * walks disagree (1 here against 0 there), turning an invariant violation
 * into a silent width mismatch instead of an obvious one.
 *
 * The invariant holds regardless: parse_compound_metadata() resets the whole
 * metadata struct -- kind included -- on every failure path, returning
 * kind = NONE for a non-positive arity and for an inline arity above
 * WL_IR_COMPOUND_INLINE_MAX_ARITY, and program.c's collect_decl() is the
 * only writer of columns[].compound_kind.  So INLINE implies
 * 1 <= compound_arity <= WL_IR_COMPOUND_INLINE_MAX_ARITY.
 *
 * static inline in the header rather than a function in program.c because
 * tests/test_io_ctx links only io/io_ctx.c plus intern, and
 * io/io_ctx_internal.h already includes this header. */
static inline uint32_t
wl_ir_relation_physical_width(const wl_ir_relation_info_t *rel)
{
    uint32_t phys = 0;

    if (!rel)
        return 0;
    if (!rel->columns)
        return rel->column_count;

    for (uint32_t i = 0; i < rel->column_count; i++) {
        const wirelog_column_t *col = &rel->columns[i];
        phys += (col->compound_kind == WIRELOG_COMPOUND_KIND_INLINE)
            ? col->compound_arity
            : 1u;
    }
    return phys;
}

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

    /* Why the lowering stages rejected this program, if they did (#979).
     * First writer wins -- see wl_ir_program_set_error().  Empty until a
     * rejection, and read by wl_ir_parse_string_err() *before* it frees the
     * program on the failure path, since this dies with the struct. */
    char parse_error[512];

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

/**
 * wl_ir_parse_string_err:
 * @program_text: Datalog source
 * @error: (out) (nullable): coarse error code, as wirelog_parse_string()
 * @errbuf: (out) (nullable): buffer for the human-readable rejection reason
 * @errcap: size of @errbuf in bytes
 *
 * wirelog_parse_string() with somewhere for the reason to go (issue #979).
 *
 * The public entry point's signature is ABI-frozen and has no message
 * parameter, so it forwards here with (NULL, 0) and behaves exactly as
 * before.  This variant is deliberately internal rather than a new
 * `wirelog_parse_string_ex()` export: the only consumer that needs it is the
 * CLI, which compiles wirelog_sources directly into its executable rather
 * than linking libwirelog, so it reaches internal symbols without a new
 * public one.  Adding a public symbol would mean updating three per-platform
 * lists under abi/ plus the abidiff baseline, and committing to the surface
 * forever -- a decision worth making on its own evidence rather than as a
 * side effect of a diagnostics fix.  See #1024, which wants to know whether
 * such rejections should be load-time errors at all.
 *
 * On failure @errbuf receives the reason when one is available: the parser's
 * "line %u, col %u: ..." text for a syntax error, or the rejecting stage's
 * message for a semantic one.  It is set to the empty string on entry, so an
 * empty buffer after a NULL return means the failure had no message, not
 * that the caller forgot to look.  @errbuf is caller-owned; nothing retains
 * a pointer to it.
 *
 * Returns: the program, or NULL on failure.
 */
struct wirelog_program *
wl_ir_parse_string_err(const char *program_text, wirelog_error_t *error,
    char *errbuf, size_t errcap);

int
wl_ir_program_collect_metadata(struct wirelog_program *program,
    const wl_parser_ast_node_t *ast);

int
wl_ir_program_convert_rules(struct wirelog_program *program,
    const wl_parser_ast_node_t *ast);

int
wl_ir_program_merge_unions(struct wirelog_program *program);

/**
 * wl_ir_program_set_error:
 * @program: (nullable): the program being lowered
 * @fmt: printf-style format for the rejection reason
 *
 * Record why lowering rejected @program, and log it (issue #979).
 *
 * Replaces the bare WL_LOG(WL_LOG_SEC_PARSER, WL_LOG_ERROR, ...) that each
 * rejection site used to call.  Those messages were already well composed --
 * they name the relation, the counts and the source line -- but WL_LOG could
 * not deliver them: wl_log_thresholds defaults to WL_LOG_NONE (0), the gate
 * is (LVL) <= threshold, and WL_LOG_ERROR is 1.  Formatting once here and
 * both storing and logging keeps a single format string per rejection, so
 * the stored text and the logged text cannot drift apart.
 *
 * First writer wins.  Each stage returns non-zero as soon as it rejects, so
 * in practice only one call fires per parse; keeping the first rather than
 * the last means that if that ever stops holding, the user sees the root
 * cause instead of whatever happened to run last.
 */
void
wl_ir_program_set_error(struct wirelog_program *program, const char *fmt, ...)
#if defined(__GNUC__) || defined(__clang__)
__attribute__((format(printf, 2, 3)))
#endif
;

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
