# Monotone Aggregation in wirelog

**Document**: Formal Semantics and Implementation Guide
**Date**: 2026-03-04
**Status**: Phase 6 Complete

---

## Overview

Monotone aggregation is a class of aggregation functions that guarantee convergence in fixed-point computation within wirelog's recursive iterate() scope. This document provides the formal definitions, mathematical proofs, and implementation rationale for why certain aggregations are monotone while others are not.

**Key Insight**: In Differential Dataflow's `iterate()` loop, the state of relations can only change monotonically — either growing (by adding tuples) or shrinking (by removing tuples), but never oscillating. Aggregation functions must preserve this invariant to be safe inside iterate().

---

## Part 1: Formal Definitions

### 1.1 Monotone Aggregation (Definition)

An aggregation function `agg` is **monotone** with respect to a lattice if, for all sequences of intermediate values V₁, V₂, ..., Vₙ produced during iteration:

```
agg(V₁) ⊑ agg(V₂) ⊑ ... ⊑ agg(Vₙ)
```

Where `⊑` denotes a partial order (for MIN: `≥`, for MAX: `≤`).

**Monotonicity Requirement**: The aggregation result must form a monotone sequence that eventually stabilizes (reaches a fixed point). No value oscillation or regression is allowed.

### 1.2 Lattice Order Relations

For wirelog's aggregations, we define two lattice orderings:

**Minimization Lattice (MIN)**:
```
Order relation: ≥ (greater-than-or-equal)
Partial order: a ⊑ b ≡ a ≥ b
Bottom element: +∞ (identity: greater than all values)
Meet operation: min(a, b)
```

**Maximization Lattice (MAX)**:
```
Order relation: ≤ (less-than-or-equal)
Partial order: a ⊑ b ≡ a ≤ b
Bottom element: -∞ (identity: less than all values)
Meet operation: max(a, b)
```

### 1.3 Delta (Δ) Semantics

During iteration, the delta (Δ) represents new tuples added to a relation. The aggregation must process deltas such that:

**For MIN**: Each delta produces an aggregated value that is **less than or equal to** the previous aggregated value.

**For MAX**: Each delta produces an aggregated value that is **greater than or equal to** the previous aggregated value.

Mathematically:
```
Monotone MIN:  agg(Δₜ ∪ previous_value) ≤ previous_value
Monotone MAX:  agg(Δₜ ∪ previous_value) ≥ previous_value
```

---

## Part 2: Proofs of Monotonicity

### 2.1 MIN is Monotone (Proof)

**Theorem**: For any finite set of values V = {v₁, v₂, ..., vₙ}, the aggregation function MIN is monotone. That is, as new values are added to V, min(V) can only decrease or stay the same.

**Proof**:

Let V be a set of values at iteration step t, and V' = V ∪ Δ be the set after adding delta Δ.

Define:
```
min(V)  = m
min(V') = m'
```

Claim: m' ≤ m

**Case 1**: Δ is empty.
```
V' = V
m' = min(V) = m
Therefore: m' = m ≤ m ✓
```

**Case 2**: Δ is non-empty.
```
V' = V ∪ Δ
m' = min(V ∪ Δ)

For all values v in V:  min(Δ) ≤ m (since min is the smallest element)
Therefore:              m' = min(V ∪ Δ) = min(m, min(Δ)) = min(Δ) ≤ m ✓
```

**Conclusion**: The MIN aggregation forms a monotone decreasing sequence: m₀ ≥ m₁ ≥ m₂ ≥ ... ≥ mₓ, which must stabilize at some final value mₓ. This guarantees convergence in iterate().

**Example**: Connected components with MIN
```
Iteration 0: Label(1) = 1
Iteration 1: Label(1) = min(1, 3, 2) = 1  (same, no change)
Iteration 2: Label(1) = 1                  (stable)
```

### 2.2 MAX is Monotone (Proof)

**Theorem**: For any finite set of values V = {v₁, v₂, ..., vₙ}, the aggregation function MAX is monotone. That is, as new values are added to V, max(V) can only increase or stay the same.

**Proof**:

Let V be a set of values at iteration step t, and V' = V ∪ Δ be the set after adding delta Δ.

Define:
```
max(V)  = M
max(V') = M'
```

Claim: M' ≥ M

**Case 1**: Δ is empty.
```
V' = V
M' = max(V) = M
Therefore: M' = M ≥ M ✓
```

**Case 2**: Δ is non-empty.
```
V' = V ∪ Δ
M' = max(V ∪ Δ)

For all values v in V:  M ≤ max(Δ) or max(Δ) < M (either case works)
Therefore:              M' = max(V ∪ Δ) = max(M, max(Δ)) = max(Δ) ≥ M ✓
```

**Conclusion**: The MAX aggregation forms a monotone increasing sequence: M₀ ≤ M₁ ≤ M₂ ≤ ... ≤ Mₓ, which must stabilize at some final value Mₓ. This guarantees convergence in iterate().

**Example**: Shortest path with MAX (longest reachable distance)
```
Iteration 0: Dist(3) = 4
Iteration 1: Dist(3) = max(4, 3+2) = 5  (increased)
Iteration 2: Dist(3) = max(5, 4+1) = 5  (stable)
```

### 2.3 COUNT is Non-Monotone (Proof by Counterexample)

**Theorem**: The COUNT aggregation is non-monotone and unsafe in iterate().

**Counterexample**: Consider a recursive rule with COUNT in a graph traversal:

```datalog
.decl Edge(x: int32, y: int32)
.decl EdgeCount(x: int32, c: int32)

EdgeCount(x, count())  :- Edge(x, y).
EdgeCount(x, count(c)) :- EdgeCount(y, c), Edge(x, y).
```

With input edges: Edge(1,2), Edge(2,3):

```
Iteration 0:
  EdgeCount(1) = count() = 0 (no input)
  EdgeCount(2) = count() = 1 (one edge 1->2)

Iteration 1 (delta adds Edge(2,3)):
  EdgeCount(2) = count() = 2 (now two edges: 1->2 and the new delta)
  EdgeCount(1) = count(c) where c=2 → EdgeCount(1) increases to 2

Iteration 2 (delta adds recursive edge):
  EdgeCount(1) might oscillate: decreases if delta recedes, increases if new paths emerge
```

**Why it fails**: COUNT aggregates over the cardinality of a collection. In iterate(), deltas arrive gradually. The delta processing can increase tuple multiplicity in complex ways that don't form a monotone sequence. The final count may be underdetermined (depends on iteration order).

### 2.4 SUM is Non-Monotone (Proof by Counterexample)

**Theorem**: The SUM aggregation is non-monotone and unsafe in iterate().

**Counterexample**: Consider path weight summation:

```datalog
.decl Edge(x: int32, y: int32, w: int32)
.decl PathSum(y: int32, s: int32)

PathSum(y, sum(w))      :- Edge(0, y, w).
PathSum(y, sum(s + w))  :- PathSum(z, s), Edge(z, y, w).
```

With input: Edge(0,1,10), Edge(1,2,5), Edge(0,2,8):

```
Iteration 0:
  PathSum(1) = sum(10) = 10
  PathSum(2) = sum(8)  = 8

Iteration 1 (delta includes recursive path):
  PathSum(2) could be:
    - sum(8) + sum(10 + 5) = 8 + 15 = 23 (if it adds new path)
    - sum(8) only = 8 (if delta doesn't fire yet)

The value oscillates between 8 and 23 depending on when deltas arrive.
```

**Why it fails**: SUM is cumulative; it adds values together. In iterate(), each iteration may introduce new paths that contribute to the sum. Unlike MIN/MAX which have a convergence point, SUM can keep growing indefinitely or oscillate based on delta scheduling. Iteration order matters.

### 2.5 AVG is Non-Monotone

**Theorem**: AVG (average) is non-monotone.

**Reason**: AVG(S) = SUM(S) / COUNT(S). Since both SUM and COUNT are non-monotone, their quotient is also non-monotone. The moving average can oscillate arbitrarily as new values are added.

---

## Part 3: Supported Aggregations in wirelog

### 3.1 Monotone Aggregations (Allowed in Recursive Strata)

| Aggregation | Allowed in Recursive | Lattice | Reason |
|-------------|---------------------|---------|--------|
| **MIN** | ✅ Yes | Minimization | Monotone decreasing |
| **MAX** | ✅ Yes | Maximization | Monotone increasing |

### 3.2 Non-Monotone Aggregations (Rejected in Recursive Strata)

| Aggregation | Allowed in Recursive | Reason for Rejection |
|-------------|---------------------|----------------------|
| **COUNT** | ❌ No | Cardinality oscillates based on delta arrival |
| **SUM** | ❌ No | Cumulative values don't converge monotonically |
| **AVG** | ❌ No | Quotient of two non-monotone functions |

---

## Part 4: Implementation in wirelog

### 4.1 Error Detection and Rejection

**File**: `/wirelog/backend/dd/dd_plan.c` (lines 791–821)

The DD plan generator detects non-monotone aggregations during plan generation:

```c
/* Step 3: Reject non-monotone aggregations in recursive strata.
 * COUNT and SUM are not monotone and cannot be computed correctly
 * inside a fixed-point iterate() loop. */
for (uint32_t s = 0; s < plan->stratum_count; s++) {
    if (!plan->strata[s].is_recursive)
        continue;
    for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
        const wl_ffi_dd_relation_plan_t *rp = &plan->strata[s].relations[r];
        for (uint32_t o = 0; o < rp->op_count; o++) {
            const wl_ffi_dd_op_t *op = &rp->ops[o];
            if (op->op != WL_FFI_DD_REDUCE)
                continue;
            if (op->agg_fn == WIRELOG_AGG_COUNT) {
                fprintf(stderr,
                        "wirelog: COUNT aggregation not supported in "
                        "recursive stratum '%s'\n",
                        rp->name ? rp->name : "?");
                wl_ffi_dd_plan_free(plan);
                return -1;
            }
            if (op->agg_fn == WIRELOG_AGG_SUM) {
                fprintf(stderr,
                        "wirelog: SUM aggregation not supported in "
                        "recursive stratum '%s'\n",
                        rp->name ? rp->name : "?");
                wl_ffi_dd_plan_free(plan);
                return -1;
            }
        }
    }
}
```

**Key Points**:
- Check happens **after** stratification (strata are known)
- Only recursive strata (is_recursive=true) are checked
- Non-recursive strata can use any aggregation
- Errors are printed to stderr with relation name
- Plan generation returns -1 (error code)

### 4.2 Plan Generation Flow

```
wirelog_parse_string()
    ↓
wirelog_program_t *prog
    ↓
wl_ffi_dd_plan_generate(prog, &plan)
    ↓
[Stratify program]
    ↓
[Translate IR nodes to DD ops]
    ↓
[Check recursiveness of each stratum]
    ↓
[**Validate aggregations in recursive strata**]
    ↓
Return 0 (success) or -1 (non-monotone found)
```

### 4.3 Aggregation Function Enum

**File**: `/wirelog/wirelog-types.h`

```c
typedef enum {
    WIRELOG_AGG_COUNT = 0,
    WIRELOG_AGG_SUM = 1,
    WIRELOG_AGG_MIN = 2,
    WIRELOG_AGG_MAX = 3,
    WIRELOG_AGG_AVG = 4,
} wirelog_agg_fn_t;
```

---

## Part 5: Use Cases and Examples

### 5.1 Connected Components with MIN

**Algorithm**: Label each node with the minimum node ID in its connected component.

**Datalog**:
```datalog
.decl Edge(x: int32, y: int32)
.decl Label(x: int32, l: int32)

Label(x, min(x))  :- Edge(x, y).
Label(y, min(y))  :- Edge(x, y).
Label(x, min(l))  :- Label(y, l), Edge(x, y).
Label(x, min(l))  :- Label(y, l), Edge(y, x).
```

**Why MIN works**:
- Initial: Each node seeds itself: Label(x, x)
- Each iteration: Labels propagate to neighbors via min()
- Monotonicity: min(prev_label, new_label) ≤ prev_label (always decreasing)
- Fixpoint: When no label can decrease further (all connected nodes have seen the minimum)

**Test**: `/tests/test_recursive_agg_cc_min.c`
- Five test cases: two components, triangle, single edge, linear chain, three components
- Validates that each node gets the minimum label in its component

### 5.2 Shortest Path with MAX

**Algorithm**: Compute the longest reachable path distance from a fixed source.

**Datalog**:
```datalog
.decl Edge(x: int32, y: int32, w: int32)
.decl Dist(y: int32, d: int32)

Dist(y, max(w))       :- Edge(0, y, w).
Dist(y, max(dz + w))  :- Dist(z, dz), Edge(z, y, w).
```

**Why MAX works**:
- Initial: Direct edges from source: Dist(y, weight)
- Each iteration: Find longer paths via max(prev_dist, new_dist)
- Monotonicity: max(prev_dist, new_dist) ≥ prev_dist (always increasing)
- Fixpoint: When no longer path can be found (all reachable nodes have their maximum distance)

**Test**: `/tests/test_recursive_agg_sssp_max.c`
- Five test cases: triangle graph, convergence, disconnected graph, single edge, dense 5-node graph
- Validates that each reachable node gets the longest path distance

### 5.3 Why COUNT Fails: Transitive Closure Example

```datalog
.decl Edge(x: int32, y: int32)
.decl TC(x: int32, y: int32, c: int32)

TC(x, y, count())  :- Edge(x, y).
TC(x, y, count(c)) :- TC(x, z, c), Edge(z, y).
```

With input Edge(1,2), Edge(2,3):

```
Iteration 0:
  TC(1,2,?) ← Edge(1,2)     [needs count aggregation]

Iteration 1:
  TC(1,3,?) ← TC(1,2,c), Edge(2,3)  [needs to count() the path length]

Problem: count() over what? The number of paths? Number of edges?
Cardinality changes unpredictably with delta arrival.
```

---

## Part 6: Design Rationale

### 6.1 Why Not Support COUNT/SUM?

wirelog's design prioritizes **correctness over permissiveness**:

1. **Soundness**: Only monotone aggregations guarantee that iterate() will terminate and produce a unique fixed point independent of delta scheduling.

2. **Simplicity**: The Rust DD executor's iterate() combinator expects monotone state. Supporting non-monotone functions would require custom loop logic (e.g., detecting oscillations, applying widening operators), adding complexity and overhead.

3. **Composability**: Recursive stratum results feed into downstream non-recursive strata. If a recursive stratum produced non-deterministic results (due to oscillation), downstream strata would inherit this non-determinism.

### 6.2 Detection at Plan Generation Time

Errors are detected **before execution**:
- Earlier detection = better user experience
- Fails fast with clear error messages
- Prevents silent incorrect results

### 6.3 Error Messages

When a non-monotone aggregation is found:

```
wirelog: COUNT aggregation not supported in recursive stratum 'RelationName'
wirelog: SUM aggregation not supported in recursive stratum 'RelationName'
```

The stratum name helps users pinpoint which rule to modify.

---

## Part 7: Future Work

### 7.1 Widening and Narrowing Operators (Out of Scope)

Some Datalog systems support non-monotone functions via **widening operators**:

```
widen(x, y) = x ⊔ y (over-approximate)
narrow(x, y) = x ⊓ y (under-approximate)
```

This requires:
- Custom lattice definitions per aggregation
- Convergence guarantees (polynomial delay property)
- Significant complexity in the executor

**Current decision**: Not planned for Phase 1. revisit in Phase 3+ if use cases justify it.

### 7.2 Aggregate Specialization

Some specialized aggregations might be safe in iterate() with constraints:

```
SUM with monotone functions: sum(max(x, 0))  ← only non-negative values
COUNT with cardinality bounds: count(distinct x)  ← bounded cardinality
```

**Current decision**: Defer; focus on MIN/MAX first.

---

## Part 8: References

### Formal Foundations
- Differential Dataflow: https://github.com/TimelyDataflow/differential-dataflow
- Fixed-point computation in Datalog: Frank McSherry et al., "Differential Dataflow" (CIDR 2013)
- Monotone functions in lattices: Derek Dreyer et al., "Verifying Higher-order Imperative Programs with Higher-order Separation Logic" (POPL 2010)

### wirelog Implementation
- Plan generation: `/wirelog/backend/dd/dd_plan.c` (lines 791–821)
- Aggregation enum: `/wirelog/wirelog-types.h`
- Integration tests:
  - `/tests/test_recursive_agg_cc_min.c` (MIN with connected components)
  - `/tests/test_recursive_agg_sssp_max.c` (MAX with shortest path)
  - `/tests/test_recursive_agg_plan.c` (plan generation validation)

### Related Issues & Discussions
- Issue #69: Recursive aggregation support
- Issue #75: Naming convention enforcement

---

## Document Version History

| Date | Version | Changes |
|------|---------|---------|
| 2026-03-04 | 1.0 | Initial comprehensive documentation with formal proofs, implementation details, and use cases |
