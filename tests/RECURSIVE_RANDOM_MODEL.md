# Generated recursive models (#2242)

`meson test -C build recursive_random_model` uses xorshift32 seed 2242 and
150 programs. Each has `e0/3`, `e1/2`, `e2/3`, `e3/2`, `l0/2`, `a/2`, `b/2`,
4–7 rules plus the `l0` copy rule, and two batches of facts from 0..7.

Alternating skeletons exercise differently keyed joins of the same recursive
inputs and EDB FILTER followed by a recursive JOIN. Additional rules have 1–3
atoms, double weight for a/b, 55% reuse of existing variables and 15% input
constants (0..3), optional bound EDB/l0 negation (35%) and comparison (30%).
The skeleton values are randomly permuted within 0..3 and 4..7; comparisons
retain their ordinary integer meaning. Random facts supplement each skeleton.
This is a coverage-directed generator rather than an unrestricted grammar.

Every program runs with workers 1 and 2, and no passes, SIP, or fusion+JPP+SIP.
Even-index programs use STEP then SNAPSHOT; odd-index programs use SNAPSHOT.
Both insertion rounds are checked (1,800 exact model comparisons). W=1 is an
engine under test, never the reference. The independent interpreter unifies
structured atoms against tuple sets and computes a fresh least fixed point
from all EDB facts each round. Negation only reads fixed EDB/l0 sets. A small
literal oracle self-test checks recursion, constants, repeated variables,
negation and comparison, including removal of a derivation when negative input
grows. Engine snapshots reject duplicate or out-of-domain tuples.

Fixed-seed coverage is measured, not inferred from the skeleton labels:

| Shape | Programs | Rate | Required minimum |
| --- | ---: | ---: | ---: |
| Body constant | 105 | 70% | 75 (50%) |
| Repeated variable in an atom | 129 | 86% | 75 (50%) |
| Same input pair, different JOIN keys | 76 | 50.7% | 75 (50%) |
| EDB FILTER followed by recursive JOIN | 88 | 58.7% | 75 (50%) |

JOIN coverage inspects compiled operators, including the nested sequences
inside K_FUSION. All 450 requested two-worker STEP executions must actually
activate multiple workers. The campaign deliberately uses two widths to keep
runtime bounded; it does not claim the full existing fixture matrix.

Mutation validation uses separate compiled objects and archives, leaving the
production sources unchanged. With the base-case skip's
`wl_columnar_eval_plan_var_skip_scan` condition removed, this seed produces a
wrong model at index 1, second insertion round, W=1, no passes. With operation
signatures zeroed at both keyed cache lookup and insertion, index 0 produces a
wrong model at W=1, no passes. Both failures report missing/unexpected tuples,
not merely an engine error. The diagnostics include the generated source,
seed/index/configuration and both fact batches for replay.
