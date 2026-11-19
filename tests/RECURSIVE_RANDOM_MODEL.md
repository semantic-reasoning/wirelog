# Generated recursive models (#2242)

`meson test -C build 'recursive_random_model_*'` runs three deterministic
50-program shards using xorshift32 seed 2242. Their disjoint union is the
original 150-program corpus. Each has `e0/3`, `e1/2`, `e2/3`, `e3/2`, `l0/2`, `a/2`, `b/2`,
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

Each shard selects original indices modulo three. Every process generates
all 150 programs in the original RNG order before skipping nonselected engine
executions; global indices still select the STEP/SNAPSHOT parity and diagnostics.
Each selected program retains all six optimizer/worker configurations and both
rounds. Successful shards require 50 programs, 300 actual engine sessions,
600 completed model comparisons and 150 actual multiworker STEP activations.
All three remain in Meson's default selection with the existing 180-second
per-process watchdog. Partitioning changes process lifetimes/concurrency and
the aggregate watchdog allowance, not the required corpus or comparisons; it
is not a total-walltime guarantee under arbitrary load.

For the original single-process campaign, run
`build/tests/test_recursive_random_model` without arguments. Arguments `0`, `1`
and `2` select individual shards; malformed or extra arguments fail. Full mode
retains the original coverage minima and requires 900 sessions, 1,800 model
comparisons and 450 actual multiworker STEP activations.

Fixed-seed coverage is measured, not inferred from the skeleton labels:

| Shape | Programs | Rate | Required minimum |
| --- | ---: | ---: | ---: |
| Body constant | 105 | 70% | 75 (50%) |
| Repeated variable in an atom | 129 | 86% | 75 (50%) |
| Same input pair, different JOIN keys | 76 | 50.7% | 75 (50%) |
| EDB FILTER followed by recursive JOIN | 88 | 58.7% | 75 (50%) |

The measured shard vectors (constants, repeated variables, differently keyed
JOIN pairs, filtered recursive paths) are `(34,42,25,30)`, `(35,43,26,29)` and
`(36,44,25,29)`. Their sums match the full-corpus table. Each shard requires at
least 25 programs in each category, preserving a combined minimum of 75.

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
