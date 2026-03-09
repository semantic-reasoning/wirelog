# Rule-Level Frontier Skip Optimization (Phase 4+)

## Executive Summary

Current stratum-level frontier tracking becomes ineffective when inserted facts affect ALL strata transitively. CSPA exhibits this pathological case: base fact insertion → all strata affected → frontier reset to UINT32_MAX → skip condition always false → no performance gain.

Rule-level frontier provides fine-grained control: only rules affected by inserted facts will reset their frontier. Unaffected rules maintain converged frontiers, skipping unnecessary iterations.

## Current Architecture: Stratum-Level Frontier

### Data Structure
```c
typedef struct {
    uint32_t iteration;
    uint32_t stratum;
} col_frontier_t;

/* Session stores one frontier per stratum */
col_frontier_t frontiers[MAX_STRATA];
```

### Skip Logic
```c
// In col_eval_stratum()
if (iter > frontiers[stratum_idx].iteration) {
    // Skip entire stratum at this iteration
}
```

### Problem with CSPA

**Rule dependency graph (K=3 mutual recursion):**
```
valueAlias(x,y) :- valueFlow(z,x), memoryAlias(z,w), valueFlow(w,y)
                   ↑               ↑                    ↑
                   |               |                    |
              All in SAME stratum, mutually dependent
```

**Insertion flow:**
1. Insert base facts (valueFlow, memoryAlias)
2. Affected strata detection: "valueFlow → valueAlias depends on valueFlow" → ALL strata affected
3. Reset affected strata frontiers to UINT32_MAX
4. Skip condition: `iter > UINT32_MAX` → always false
5. Result: Iterations 0-5 must execute fully; only iteration 6+ skipped

**Speedup: 0.34x** (regression due to overhead)

## Proposed Solution: Rule-Level Frontier

### Data Structure
```c
/* Per-rule frontier (instead of per-stratum) */
col_frontier_t frontiers[MAX_RULES];  // Indexed by rule_id

/* Rule dependency graph (new) */
typedef struct {
    uint32_t rule_id;
    uint32_t head_relation;  // EDB/IDB relation this rule derives
    uint32_t body_count;
    uint32_t *body_relations;  // IDB relations in rule body
} col_rule_info_t;

col_rule_info_t rules[MAX_RULES];
uint32_t rule_count;
```

### Skip Logic
```c
// In col_eval_relation_plan() for a specific rule
if (op->rule_id != INVALID && iter > frontiers[op->rule_id].iteration) {
    // Skip this rule at this iteration
    return 0;  // No output
}

// Rule frontier updated on iteration completion
frontiers[rule_id] = (iter, stratum);
```

### Affected-Rule Detection
```c
uint64_t col_compute_affected_rules(
    wl_session_t *session,
    const char *inserted_relation  // "valueFlow", "memoryAlias", etc.
)
{
    /* Build rule dependency graph:
     * For each rule, check if body contains inserted_relation
     * Transitively include rules that depend on affected rules */

    uint64_t affected = 0;

    // Direct: rules with inserted_relation in body
    for (uint32_t i = 0; i < rule_count; i++) {
        for (uint32_t j = 0; j < rules[i].body_count; j++) {
            if (rules[i].body_relations[j] == inserted_relation_id) {
                affected |= (1ULL << i);
            }
        }
    }

    // Transitive: rules that depend on affected rules
    for (int rounds = 0; rounds < MAX_ITERATIONS; rounds++) {
        uint64_t new_affected = affected;
        for (uint32_t i = 0; i < rule_count; i++) {
            for (uint32_t j = 0; j < rules[i].body_count; j++) {
                uint32_t body_rel_id = rules[i].body_relations[j];
                // If this body relation is derived by an affected rule
                if (affected & (1ULL << get_rule_for_head(body_rel_id))) {
                    new_affected |= (1ULL << i);
                }
            }
        }
        if (new_affected == affected) break;
        affected = new_affected;
    }

    return affected;
}
```

### Expected Benefit on CSPA

**Scenario: valueFlow insertion (200 facts)**

Base fact insertion affects only rules that derive valueFlow:
- Rule A (valueAlias derives from valueFlow): affected → reset frontier
- Rule B (valueFlow is base): not derived by any rule → frontier preserved if possible
- Rule C (memoryAlias derives...): NOT directly affected by valueFlow → frontier preserved!

**Iteration reduction:**
- Without rule-level: 6→5 iterations (only iteration 6 skipped)
- With rule-level: 6→4-5 iterations (unaffected rules skip multiple iterations)

**Performance:**
- Current stratum-level: reeval=32.9s, speedup=0.34x
- Proposed rule-level: reeval≤20s, speedup≥0.6x

## Integration Points

### 1. Session Structure (columnar_nanoarrow.c)
```c
typedef struct {
    wl_session_t base;

    /* Current: stratum frontiers */
    // col_frontier_t frontiers[MAX_STRATA];

    /* New: rule frontiers + rule dependency info */
    col_frontier_t rule_frontiers[MAX_RULES];
    col_rule_info_t rules[MAX_RULES];
    uint32_t rule_count;
} wl_col_session_t;
```

### 2. Plan Generation (exec_plan_gen.c)
Annotate each operator with rule_id during plan generation.

### 3. Evaluation (columnar_nanoarrow.c)
```c
// In col_eval_relation_plan()
uint32_t rule_id = plan_op->rule_id;  // From plan metadata
if (rule_id != INVALID && iter > sess->rule_frontiers[rule_id].iteration) {
    // Skip this rule
}
```

### 4. Insertion (columnar_nanoarrow.c)
```c
// In col_session_insert_incremental()
uint64_t affected_rules = col_compute_affected_rules(sess, inserted_relation);
for (uint32_t r = 0; r < rule_count; r++) {
    if (affected_rules & (1ULL << r)) {
        sess->rule_frontiers[r].iteration = UINT32_MAX;  // Reset only affected
    }
}
```

## Implementation Strategy

### Phase 1: Infrastructure
- Add rule metadata to session
- Implement affected-rule detection
- Add rule_id to plan operators

### Phase 2: Skip Logic
- Implement rule-level skip condition in col_eval_relation_plan
- Update frontier tracking on iteration completion

### Phase 3: Testing & Tuning
- Verify correctness with synthetic workloads
- Benchmark CSPA incremental performance
- Validate speedup >= 0.6x

## Alternative Approaches Considered

### 1. Stratum+Rule Hybrid
- Track both stratum and rule frontiers
- Use whichever allows more skipping
- **Rejected**: Complex, marginal benefit

### 2. Call-Graph Based Skip
- Build rule call graph (which rules call which)
- Skip entire sub-trees not affected
- **Rejected**: Requires major restructuring of evaluation

### 3. Delta-Only Rule Selection
- Already implemented: K-fusion expands rules into K copies
- **Limitation**: Still executes all iterations on affected rules

## Risk Mitigation

### Risk: Correctness regression
- Comprehensive test suite for rule skip logic
- Verify output unchanged vs stratum-level frontier
- Property-based testing on synthetic workloads

### Risk: Complex rule dependency graph
- Limit to <= 64 rules (uint64_t bitmask)
- Fallback to stratum-level frontier if > 64 rules
- Clear error message on limit exceeded

### Risk: Transitive dependency detection bug
- Unit test affected-rule detection on known graphs
- Manual verification on CSPA rule graph
- Architect review of graph traversal algorithm

## Success Criteria

1. **Correctness**: Output identical to stratum-level frontier across all tests
2. **Performance**: CSPA incremental speedup >= 0.6x (vs current 0.34x)
3. **Iteration reduction**: CSPA 6→4-5 iterations (selective skip, not all)
4. **Quality gates**: All tests pass, zero diagnostics, atomic commits
5. **Architect approval**: Design and implementation reviewed and approved
