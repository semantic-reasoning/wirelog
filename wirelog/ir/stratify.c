/*
 * stratify.c - wirelog Stratification & SCC Detection
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Implements dependency graph construction, Tarjan's SCC detection,
 * and stratification for Datalog programs with negation.
 */

#include "stratify.h"
#include "program.h"

#include <stdlib.h>
#include <string.h>

#define EDGE_INITIAL_CAPACITY 16

/* ======================================================================== */
/* Dependency Graph — Helpers                                               */
/* ======================================================================== */

/**
 * Find the graph-local index for a relation name.
 * Returns the index if found, or UINT32_MAX if not in graph.
 */
static uint32_t
graph_find_relation(const wl_ir_stratify_dep_graph_t *g,
    const struct wirelog_program *prog, const char *name)
{
    for (uint32_t i = 0; i < g->relation_count; i++) {
        uint32_t ri = g->relation_map[i];
        if (ri < prog->relation_count
            && strcmp(prog->relations[ri].name, name) == 0) {
            return i;
        }
    }
    return UINT32_MAX;
}

/**
 * Add an edge to the dependency graph.
 * Returns 0 on success, -1 on memory error.
 */
static int
graph_add_edge(wl_ir_stratify_dep_graph_t *g, uint32_t from, uint32_t to,
    wl_ir_stratify_dep_type_t type)
{
    if (g->edge_count >= g->edge_capacity) {
        uint32_t new_cap = g->edge_capacity == 0 ? EDGE_INITIAL_CAPACITY
                                                 : g->edge_capacity * 2;
        wl_ir_stratify_dep_edge_t *tmp = (wl_ir_stratify_dep_edge_t *)realloc(
            g->edges, new_cap * sizeof(wl_ir_stratify_dep_edge_t));
        if (!tmp)
            return -1;
        g->edges = tmp;
        g->edge_capacity = new_cap;
    }

    g->edges[g->edge_count].from = from;
    g->edges[g->edge_count].to = to;
    g->edges[g->edge_count].type = type;
    g->edge_count++;
    return 0;
}

/**
 * Walk an IR tree recursively, collecting dependency edges.
 *
 * @param node    Current IR node
 * @param head_gi Graph-local index of the head (defining) relation
 * @param g       Dependency graph (edges appended)
 * @param prog    Program (for relation name lookup)
 * @param dep     Dependency type context (POSITIVE normally, NEGATION inside ANTIJOIN)
 */
static int
walk_ir_tree(const wirelog_ir_node_t *node, uint32_t head_gi,
    wl_ir_stratify_dep_graph_t *g, const struct wirelog_program *prog,
    wl_ir_stratify_dep_type_t dep)
{
    if (!node)
        return 0;

    switch (node->type) {
    case WIRELOG_IR_SCAN:
    case WIRELOG_IR_COMPOUND_INLINE:
    case WIRELOG_IR_COMPOUND_SIDE: {
        /* SCAN (optionally with compound column annotation) references a relation — add edge from head to this relation */
        const char *rel = node->relation_name;
        if (rel) {
            uint32_t to_gi = graph_find_relation(g, prog, rel);
            if (to_gi != UINT32_MAX) {
                if (graph_add_edge(g, head_gi, to_gi, dep) != 0)
                    return -1;
            }
            /* EDB relations (not in graph) are silently ignored */
        }
        break;
    }

    case WIRELOG_IR_ANTIJOIN:
        /* children[0] = positive side, children[1] = negated side */
        if (node->child_count >= 1
            && walk_ir_tree(node->children[0], head_gi, g, prog, dep) != 0)
            return -1;
        if (node->child_count >= 2
            && walk_ir_tree(node->children[1], head_gi, g, prog,
            WL_IR_STRATIFY_DEP_NEGATION)
            != 0)
            return -1;
        break;

    case WIRELOG_IR_AGGREGATE:
        /* Aggregate child SCANs get AGGREGATION edges */
        for (uint32_t i = 0; i < node->child_count; i++) {
            if (walk_ir_tree(node->children[i], head_gi, g, prog,
                WL_IR_STRATIFY_DEP_AGGREGATION)
                != 0)
                return -1;
        }
        break;

    default:
        /* PROJECT, FILTER, JOIN, FLATMAP, UNION — recurse into children */
        for (uint32_t i = 0; i < node->child_count; i++) {
            if (walk_ir_tree(node->children[i], head_gi, g, prog, dep) != 0)
                return -1;
        }
        break;
    }

    return 0;
}

/* ======================================================================== */
/* Dependency Graph — Build                                                 */
/* ======================================================================== */

int
wl_ir_stratify_dep_graph_build_ex(const struct wirelog_program *prog,
    wl_ir_stratify_dep_graph_t **out_graph)
{
    if (!out_graph)
        return -1;
    *out_graph = NULL;

    if (!prog || prog->rule_count == 0)
        return 0;

    wl_ir_stratify_dep_graph_t *g = (wl_ir_stratify_dep_graph_t *)calloc(
        1, sizeof(wl_ir_stratify_dep_graph_t));
    if (!g)
        return -1;

    /*
     * Step 1: Identify IDB relations (those that appear as rule heads).
     * Build relation_map: graph node index -> program relation index.
     */
    uint32_t *idb_flags
        = (uint32_t *)calloc(prog->relation_count, sizeof(uint32_t));
    if (!idb_flags) {
        free(g);
        return -1;
    }

    /* Mark relations that are rule heads as IDB */
    for (uint32_t r = 0; r < prog->rule_count; r++) {
        const char *head = prog->rules[r].head_relation;
        if (!head)
            continue;
        for (uint32_t i = 0; i < prog->relation_count; i++) {
            if (strcmp(prog->relations[i].name, head) == 0) {
                idb_flags[i] = 1;
                break;
            }
        }
    }

    /* Count IDB relations */
    uint32_t idb_count = 0;
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (idb_flags[i])
            idb_count++;
    }

    if (idb_count == 0) {
        free(idb_flags);
        free(g);
        return 0;
    }

    g->relation_map = (uint32_t *)calloc(idb_count, sizeof(uint32_t));
    if (!g->relation_map) {
        free(idb_flags);
        free(g);
        return -1;
    }

    g->relation_count = idb_count;
    uint32_t gi = 0;
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (idb_flags[i])
            g->relation_map[gi++] = i;
    }
    free(idb_flags);

    /*
     * Step 2: Walk each rule's IR tree to extract dependency edges.
     */
    for (uint32_t r = 0; r < prog->rule_count; r++) {
        const char *head = prog->rules[r].head_relation;
        if (!head)
            continue;

        uint32_t head_gi = graph_find_relation(g, prog, head);
        if (head_gi == UINT32_MAX)
            continue;

        if (walk_ir_tree(prog->rules[r].ir_root, head_gi, g, prog,
            WL_IR_STRATIFY_DEP_POSITIVE)
            != 0) {
            wl_ir_stratify_dep_graph_free(g);
            return -1;
        }
    }

    *out_graph = g;
    return 0;
}

wl_ir_stratify_dep_graph_t *
wl_ir_stratify_dep_graph_build(const struct wirelog_program *prog)
{
    wl_ir_stratify_dep_graph_t *graph = NULL;
    if (wl_ir_stratify_dep_graph_build_ex(prog, &graph) != 0)
        return NULL;
    return graph;
}

void
wl_ir_stratify_dep_graph_free(wl_ir_stratify_dep_graph_t *graph)
{
    if (!graph)
        return;
    free(graph->relation_map);
    free(graph->edges);
    free(graph);
}

/* ======================================================================== */
/* SCC Detection — Iterative Tarjan's Algorithm                             */
/* ======================================================================== */

/* Tarjan stack frame for iterative implementation */
typedef struct {
    uint32_t node;
    uint32_t edge_idx; /* Next edge index to process for this node */
} tarjan_frame_t;

wl_ir_stratify_scc_result_t *
wl_ir_stratify_scc_detect(const wl_ir_stratify_dep_graph_t *graph)
{
    if (!graph || graph->relation_count == 0)
        return NULL;

    uint32_t n = graph->relation_count;

    wl_ir_stratify_scc_result_t *result = (wl_ir_stratify_scc_result_t *)calloc(
        1, sizeof(wl_ir_stratify_scc_result_t));
    if (!result)
        return NULL;

    result->scc_id = (uint32_t *)calloc(n, sizeof(uint32_t));
    if (!result->scc_id) {
        free(result);
        return NULL;
    }

    /*
     * Tarjan's algorithm state:
     * - disc[i]: discovery index of node i (UINT32_MAX = unvisited)
     * - low[i]: lowest reachable discovery index
     * - on_stack[i]: whether node is on Tarjan's stack
     * - stack[]: the Tarjan stack (node indices)
     */
    uint32_t *disc = (uint32_t *)malloc(n * sizeof(uint32_t));
    uint32_t *low = (uint32_t *)malloc(n * sizeof(uint32_t));
    bool *on_stack = (bool *)calloc(n, sizeof(bool));
    uint32_t *stack = (uint32_t *)malloc(n * sizeof(uint32_t));
    tarjan_frame_t *call_stack
        = (tarjan_frame_t *)malloc(n * sizeof(tarjan_frame_t));

    /*
     * Build adjacency list from edges for O(V+E) traversal.
     * adj_start[i] = index into adj[] where node i's neighbors begin
     * adj[k]       = neighbor node index
     */
    uint32_t *adj_count_arr = (uint32_t *)calloc(n, sizeof(uint32_t));
    uint32_t *adj_start = (uint32_t *)calloc(n + 1, sizeof(uint32_t));
    uint32_t *adj = NULL;

    if (!disc || !low || !on_stack || !stack || !call_stack || !adj_count_arr
        || !adj_start) {
        goto cleanup_error;
    }

    /*
     * Count outgoing edges per node.
     *
     * Both endpoints are tested here so that this pass admits exactly the
     * edges the fill loop below stores.  Testing only `from` here counted an
     * edge with an out-of-range `to`, reserving an adjacency slot the fill
     * loop never wrote while adj_start[] still spanned it -- so the traversal
     * read an indeterminate neighbour index and subscripted with it (#1083).
     * The two predicates must stay identical.
     *
     * Nothing in CI will tell you if they stop being identical.  Before
     * #1083 this file was in the clang-tidy backlog with an
     * uninitialized.Assign report pointing straight at the consequence of
     * this predicate, which makes it easy to assume the analyser backstops
     * the invariant.  It does not: the allocation change below is what
     * silences all three diagnostics, and with it in place a re-broken
     * predicate measures ZERO clang-tidy diagnostics.  The file is on the
     * ratchet allowlist and would stay green.  tests/test_stratify_scc_bounds.c
     * is the only thing that catches a recurrence.
     */
    for (uint32_t e = 0; e < graph->edge_count; e++) {
        uint32_t from = graph->edges[e].from;
        uint32_t to = graph->edges[e].to;
        if (from < n && to < n)
            adj_count_arr[from]++;
    }

    /* Prefix sum for adj_start */
    adj_start[0] = 0;
    for (uint32_t i = 0; i < n; i++)
        adj_start[i + 1] = adj_start[i] + adj_count_arr[i];

    uint32_t total_adj = adj_start[n];

    /*
     * One slack slot, so the request is never for zero objects: calloc(0, n)
     * is permitted to return NULL, and with both endpoints tested above
     * total_adj == 0 is the COMMON case -- it holds for every program whose
     * IDB relations have no IDB-to-IDB edges -- so a NULL there would send a
     * routine graph down the error path.  The slack is unconditional rather
     * than `total_adj ? total_adj : 1` on purpose: a branch on total_adj lets
     * the static analyser explore a state where the buffer has the concrete
     * length 1, which it cannot correlate with the counting loop, and it then
     * reports two false clang-analyzer-security.ArrayBound hits on a file the
     * ratchet requires to be clean.
     *
     * calloc, not malloc, and this is not a mask: after the predicate fix
     * above no reserved slot is left unwritten, so the zeroes are never read.
     * They exist to keep tests/test_stratify_scc_bounds.c deterministic.  If
     * the two predicates ever drift apart again, an unwritten slot reads back
     * as 0 -- a phantom edge to node 0 that merges components on every single
     * run -- instead of heap garbage whose value decides what happens next.
     *
     * Reverting to malloc does not disarm that test: with the predicate
     * drifted it still fails 25 runs out of 25, as a SIGSEGV under the
     * MALLOC_PERTURB_ that `meson test` always sets and as a clean assertion
     * failure without it.  What malloc costs is the failure MODE, not the
     * outcome.  calloc is what makes the failure identical on every allocator
     * and in every heap state, and legible -- a named semantic assertion
     * rather than a crash whose cause the next reader has to reconstruct.
     */
    size_t slots = (size_t)total_adj + 1u;
    adj = (uint32_t *)calloc(slots, sizeof(uint32_t));
    if (!adj)
        goto cleanup_error;

    /* Fill adjacency list */
    memset(adj_count_arr, 0, n * sizeof(uint32_t));
    for (uint32_t e = 0; e < graph->edge_count; e++) {
        uint32_t from = graph->edges[e].from;
        uint32_t to = graph->edges[e].to;
        if (from < n && to < n) {
            adj[adj_start[from] + adj_count_arr[from]] = to;
            adj_count_arr[from]++;
        }
    }

    /* Initialize discovery array */
    for (uint32_t i = 0; i < n; i++)
        disc[i] = UINT32_MAX;

    uint32_t timer = 0;
    uint32_t stack_top = 0;
    uint32_t scc_count = 0;

    /* Iterative Tarjan's — process all nodes */
    for (uint32_t start = 0; start < n; start++) {
        if (disc[start] != UINT32_MAX)
            continue; /* Already visited */

        uint32_t cs_top = 0; /* call stack depth */

        /* Push initial frame */
        disc[start] = low[start] = timer++;
        on_stack[start] = true;
        stack[stack_top++] = start;

        call_stack[0].node = start;
        call_stack[0].edge_idx = adj_start[start];
        cs_top = 1;

        while (cs_top > 0) {
            tarjan_frame_t *frame = &call_stack[cs_top - 1];
            uint32_t v = frame->node;
            uint32_t edge_end = adj_start[v + 1];

            if (frame->edge_idx < edge_end) {
                uint32_t w = adj[frame->edge_idx];
                frame->edge_idx++;

                if (disc[w] == UINT32_MAX) {
                    /* Unvisited — push new frame (like recursive call) */
                    disc[w] = low[w] = timer++;
                    on_stack[w] = true;
                    stack[stack_top++] = w;

                    call_stack[cs_top].node = w;
                    call_stack[cs_top].edge_idx = adj_start[w];
                    cs_top++;
                } else if (on_stack[w]) {
                    if (disc[w] < low[v])
                        low[v] = disc[w];
                }
            } else {
                /* All neighbors processed — check if v is SCC root */
                if (low[v] == disc[v]) {
                    /* Pop SCC from stack */
                    uint32_t w;
                    do {
                        w = stack[--stack_top];
                        on_stack[w] = false;
                        result->scc_id[w] = scc_count;
                    } while (w != v);
                    scc_count++;
                }

                /* Pop call stack and update parent's low-link */
                cs_top--;
                if (cs_top > 0) {
                    uint32_t parent = call_stack[cs_top - 1].node;
                    if (low[v] < low[parent])
                        low[parent] = low[v];
                }
            }
        }
    }

    result->scc_count = scc_count;

    /* Cleanup success path */
    free(adj);
    free(adj_start);
    free(adj_count_arr);
    free(call_stack);
    free(stack);
    free(on_stack);
    free(low);
    free(disc);
    return result;

cleanup_error:
    free(adj);
    free(adj_start);
    free(adj_count_arr);
    free(call_stack);
    free(stack);
    free(on_stack);
    free(low);
    free(disc);
    wl_ir_stratify_scc_free(result);
    return NULL;
}

void
wl_ir_stratify_scc_free(wl_ir_stratify_scc_result_t *result)
{
    if (!result)
        return;
    free(result->scc_id);
    free(result);
}

/* ======================================================================== */
/* Stratification Pipeline                                                  */
/* ======================================================================== */

int
wl_ir_stratify_program(struct wirelog_program *program)
{
    if (!program)
        return -1;

    /*
     * 0-rule programs: produce 1 empty stratum (honors >= 1 contract).
     */
    if (program->rule_count == 0) {
        wl_ir_program_build_default_stratum(program);
        program->is_stratified = true;
        return 0;
    }

    /* Step 1: Build dependency graph */
    wl_ir_stratify_dep_graph_t *graph = NULL;
    int graph_status = wl_ir_stratify_dep_graph_build_ex(program, &graph);
    if (graph_status != 0)
        return -1;

    if (!graph) {
        /* No IDB relations (shouldn't happen with rule_count > 0,
           but handle gracefully) */
        wl_ir_program_build_default_stratum(program);
        program->is_stratified = true;
        return 0;
    }

    /* Step 2: Detect SCCs */
    wl_ir_stratify_scc_result_t *scc = wl_ir_stratify_scc_detect(graph);
    if (!scc) {
        wl_ir_stratify_dep_graph_free(graph);
        return -1;
    }

    /* Step 3: Map SCC IDs to stratum IDs.
       Tarjan's iterative algorithm produces SCCs such that sinks
       (no outgoing deps) get lower IDs — this already matches
       execution order (dependencies computed first = lower stratum). */
    uint32_t *stratum_id
        = (uint32_t *)malloc(graph->relation_count * sizeof(uint32_t));
    if (!stratum_id) {
        wl_ir_stratify_scc_free(scc);
        wl_ir_stratify_dep_graph_free(graph);
        return -1;
    }

    for (uint32_t i = 0; i < graph->relation_count; i++)
        stratum_id[i] = scc->scc_id[i];

    /* Step 4: Validate — negation within SCC = not stratifiable */
    for (uint32_t e = 0; e < graph->edge_count; e++) {
        wl_ir_stratify_dep_edge_t *edge = &graph->edges[e];
        if (edge->type == WL_IR_STRATIFY_DEP_NEGATION
            && edge->from < graph->relation_count
            && edge->to < graph->relation_count) {
            if (scc->scc_id[edge->from] == scc->scc_id[edge->to]) {
                /* Negation cycle detected — not stratifiable */
                free(stratum_id);
                wl_ir_stratify_scc_free(scc);
                wl_ir_stratify_dep_graph_free(graph);
                return -2;
            }
        }
    }

    uint32_t num_strata = scc->scc_count;

    /* Step 5: Assign strata — build wirelog_stratum_t array */

    /* Free existing strata */
    if (program->strata) {
        for (uint32_t i = 0; i < program->stratum_count; i++)
            free((void *)program->strata[i].rule_names);
        free(program->strata);
    }

    program->strata
        = (wirelog_stratum_t *)calloc(num_strata, sizeof(wirelog_stratum_t));
    if (!program->strata) {
        program->stratum_count = 0;
        free(stratum_id);
        wl_ir_stratify_scc_free(scc);
        wl_ir_stratify_dep_graph_free(graph);
        return -1;
    }
    program->stratum_count = num_strata;

    for (uint32_t s = 0; s < num_strata; s++)
        program->strata[s].stratum_id = s;

    /* Count rules per stratum */
    uint32_t *rules_per_stratum
        = (uint32_t *)calloc(num_strata, sizeof(uint32_t));
    if (!rules_per_stratum) {
        free(stratum_id);
        wl_ir_stratify_scc_free(scc);
        wl_ir_stratify_dep_graph_free(graph);
        return -1;
    }

    /* Map each rule to a stratum based on its head relation's SCC */
    uint32_t *rule_stratum
        = (uint32_t *)calloc(program->rule_count, sizeof(uint32_t));
    if (!rule_stratum) {
        free(rules_per_stratum);
        free(stratum_id);
        wl_ir_stratify_scc_free(scc);
        wl_ir_stratify_dep_graph_free(graph);
        return -1;
    }

    for (uint32_t r = 0; r < program->rule_count; r++) {
        const char *head = program->rules[r].head_relation;
        if (!head)
            continue;

        uint32_t gi = graph_find_relation(graph, program, head);
        if (gi != UINT32_MAX) {
            rule_stratum[r] = stratum_id[gi];
            rules_per_stratum[stratum_id[gi]]++;
        }
    }

    /* Allocate rule_names arrays per stratum */
    for (uint32_t s = 0; s < num_strata; s++) {
        if (rules_per_stratum[s] > 0) {
            program->strata[s].rule_names = (const char **)calloc(
                rules_per_stratum[s], sizeof(const char *));
            if (!program->strata[s].rule_names) {
                free(rule_stratum);
                free(rules_per_stratum);
                free(stratum_id);
                wl_ir_stratify_scc_free(scc);
                wl_ir_stratify_dep_graph_free(graph);
                return -1;
            }
        }
    }

    /* Fill rule_names */
    uint32_t *fill_idx = (uint32_t *)calloc(num_strata, sizeof(uint32_t));
    if (!fill_idx) {
        free(rule_stratum);
        free(rules_per_stratum);
        free(stratum_id);
        wl_ir_stratify_scc_free(scc);
        wl_ir_stratify_dep_graph_free(graph);
        return -1;
    }

    /* Issue #987: the fill must honour the same bound the counting loop
     * above established.  That loop only counts a rule when its head is
     * found in the dependency graph; a rule whose head is absent keeps
     * rule_stratum[r] == 0 from calloc and would otherwise be written into
     * stratum 0 anyway -- past the end of an array sized without it.  That
     * is an out-of-bounds *write*, and with more than one such rule it
     * corrupts the glibc heap in a plain release build.
     *
     * Magic-set rewriting manufactures those graph-absent heads: it names
     * demand relations from an atom's physical width while get_arity()
     * reports the declared logical width (passes/magic_sets.c), so the
     * rewritten head can carry a name no relation in the graph matches.
     * Fixing that confusion is tracked separately; this bound is correct
     * regardless of how a head comes to be missing.
     *
     * rules_per_stratum[] is also what :rule_count is set from below, so
     * the two loops have to agree here even setting memory safety aside. */
    for (uint32_t r = 0; r < program->rule_count; r++) {
        if (!program->rules[r].head_relation)
            continue;
        uint32_t s = rule_stratum[r];
        if (s < num_strata && program->strata[s].rule_names
            && fill_idx[s] < rules_per_stratum[s]) {
            program->strata[s].rule_names[fill_idx[s]++]
                = program->rules[r].head_relation;
        }
    }

    for (uint32_t s = 0; s < num_strata; s++)
        program->strata[s].rule_count = rules_per_stratum[s];

    /* Step 6: Detect recursive strata.
       A stratum is recursive if its SCC contains >1 node (mutual recursion)
       or if any edge within the SCC is a self-loop (self-recursion). */
    {
        /* Count nodes per SCC */
        uint32_t *scc_size = (uint32_t *)calloc(num_strata, sizeof(uint32_t));
        if (scc_size) {
            for (uint32_t i = 0; i < graph->relation_count; i++)
                scc_size[scc->scc_id[i]]++;

            for (uint32_t s = 0; s < num_strata; s++)
                program->strata[s].is_recursive = (scc_size[s] > 1);

            /* Also check self-loops (self-recursion in singleton SCC).
               Both positive and aggregation edges indicate recursion;
               e.g. dist(y, min(d+w)) :- dist(x, d), wedge(x, y, w). */
            for (uint32_t e = 0; e < graph->edge_count; e++) {
                if (graph->edges[e].from == graph->edges[e].to
                    && (graph->edges[e].type == WL_IR_STRATIFY_DEP_POSITIVE
                    || graph->edges[e].type
                    == WL_IR_STRATIFY_DEP_AGGREGATION)) {
                    uint32_t s = scc->scc_id[graph->edges[e].from];
                    program->strata[s].is_recursive = true;
                }
            }
            free(scc_size);
        }
    }

    program->is_stratified = true;

    free(fill_idx);
    free(rule_stratum);
    free(rules_per_stratum);
    free(stratum_id);
    wl_ir_stratify_scc_free(scc);
    wl_ir_stratify_dep_graph_free(graph);
    return 0;
}
