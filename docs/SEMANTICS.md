# wirelog Semantic Model

This document records observable semantic decisions in the wirelog
engine. Each section is an ADR-style entry: a short statement of the
rule, the rationale, and the public-API surfaces that hold it today.

Each entry is tagged with its **Status**:

- **Stable** — a contract the engine honors across a major version.
  Changes go through a deprecation cycle in `docs/MIGRATION.md`.
- **Current** — describes what the engine does today on the path to
  1.0. May evolve before 1.0 GA based on review or implementation
  realities. See `stable-release-plan.md` for the trajectory.

Until 1.0 ships, expect most entries to be **Current**.

## Weighted evaluation boundary (Status: Current)

Wirelog's normative weight model is the signed-integer Z-set/differential
model: inserts contribute `+1`, removals `-1`, duplicate derivations add, and
joins multiply. This is not a general probabilistic semiring. The proposed
future extension and its retraction, optimizer, and ABI constraints are in
[`docs/design/weighted-semiring-addon.md`](design/weighted-semiring-addon.md).
No probabilistic semantics or public weight API is currently provided. Scalar
row-local addons proposed by issue #912 cannot change this evaluation model.

## Scalar addon boundary (Status: Current)

Built-in expression functions retain their current parser, plan, evaluator,
and optimization semantics. The scalar addon ABI is shipped and public:
`wirelog/wirelog-extension.h` declares the descriptor, the registry, and the
reference-counted snapshot handles, and the columnar evaluator resolves a
`@call` against a snapshot supplied at plan or session creation. The design
rationale, together with the registry, callback, error, and lifetime
constraints, is in
[`docs/design/scalar-function-addon.md`](design/scalar-function-addon.md),
which was written as an issue #912 proposal and still reads as one, ahead of
the surface that shipped.

An addon callback cannot change the engine's relational, differential, or
aggregate semantics. In particular, callback errors are not silently converted
to false predicates.

The `DETERMINISTIC` and `PURE` capability bits are addon attestations; Wirelog
does not independently prove them, and no engine path reads either bit today.
Registration accepts any combination of the known bits and rejects only an
unknown one, and the evaluator consults `THREAD_SAFE` and `REENTRANT` alone.
Join-order planning is the pass that holds the line: `wl_jpp_apply` declines
to reorder or re-project any rule whose IR contains an extension call, so it
cannot duplicate, drop, or relocate a call site on the strength of an
unverified declaration. That is a statement about one pass and not about the
optimizer as a whole -- fusion, for instance, folds a PROJECT over a FILTER
into a single FLATMAP and carries the filter expression onto the fused node,
and it is not extension-aware. None of this bounds how often the evaluator
invokes a call site that survives optimization; for that, see "Scalar addon
invocation count" below. A future optimizer contract may enable specific
transformations for callbacks with both bits.

### Scalar addon invocation count

This section is about a `@call` in a value position -- the one that produces a
column value, as in `started(id, @call("act.start", id)) :- ready(id).` Such a
call is a replayable expression, not an action hook. How many times a step
invokes it is not part of the contract, and an invocation is not evidence that
anything changed.

With a delta callback installed through `wirelog_easy_set_delta_cb` and the
session driven by `wirelog_easy_step`, a step that re-derives the rule
re-derives it in full: the callback runs once for every row the rule then
derives, including rows whose derivation did not change, and once more for
each repeated derivation of the same row. What the count follows is that
derivation -- not the size of the change, and not the row count of any
relation the rule reads.

A selective body narrows it. For
`started(id, @call("act.start", id)) :- ready(id), enabled(id).` with two of
five `ready` rows enabled, a step invokes the callback twice, whatever
`ready` holds. Where the rule has a single body atom, as in the example this
section opened with, the derivation is that relation's rows, and there
retracting one row of five invokes the callback four further times while
inserting one row into a four-row relation invokes it five times. A rule
that derives nothing does not invoke it at all.

A step whose mutation does not reach the rule leaves the callback alone. The
engine decides that at stratum granularity rather than per rule --
`col_compute_affected_strata` masks strata -- so whether a particular mutation
reaches a particular rule is a property of how the program stratifies, and
mutating a relation the rule does not read is not on its own a guarantee.

Without a delta callback, `wirelog_easy_step` is not incremental (#1994). A
step with an insert or removal pending since the last step or snapshot that
evaluated discards the derived rows and re-derives every rule, so the addon
callback runs once for every row its rule then derives, whether or not the
mutation reaches that rule. A step with nothing pending derives nothing and
invokes nothing. Other configurations are not specified here. Choosing a
configuration for its invocation count would rest on something a release may
change, so a host should not do it -- and a delta callback is installed because
the host wants deltas, not as a tuning knob.

In a session with more than one worker, such a step returns, for a
non-recursive stratum that joins, negates, tests membership or aggregates,
the model a snapshot returns (#2134). Before, the step evaluated it on
worker partitions of its inputs and could lose or gain rows.

The delta stream carries the set difference, so a re-derivation that
reproduces a row publishes nothing. Inserting a sixth row into a five-row
source can invoke the callback six times and publish one `+1` event; in the
selective-body case above, retracting a `ready` row that was not enabled
invokes the callback twice and publishes no event at all. A host watching
deltas can therefore neither count the invocations nor see the work.

Two obligations follow for a host. The first is value stability: a callback
must be safe to run again for an argument tuple it has already answered, and
should answer the same value when it does. One that returns a fresh value per
invocation -- a ticket, a timestamp, a counter -- makes the derived row change
on re-derivation, so the row is retracted at its old value and republished at
the new one, and the host sees a `-1`/`+1` pair for a row whose input never
moved.

The second is that exactly-once external effects are the host's. Wirelog
attaches no execution identity to an invocation, does not de-duplicate
invocations, and does not roll back an effect a callback already performed; a
step that fails afterwards does not undo it. The discipline that survives the
invocation count is to keep the effect out of the callback: derive the request
as data, make the callback a lookup keyed by an identity the host chose, and
perform the effect from the host's delta handling, where each request arrives
as one `+1`. That last step depends on the first obligation, because an
unstable callback republishes the same logical request under a new value and a
host keyed on the delta row would act on it twice. In-callback memoization on
the host's key, returning the original value on a repeat, is the same
discipline inside the callback; it is process-local and does not by itself
survive a crash. Wirelog does not offer crash-safe exactly-once effects.

`THREAD_SAFE` and `REENTRANT` are the policy bits the evaluator acts on.
Declaring `PURE` or `DETERMINISTIC` does not reduce the count, and omitting
them does not cause registration or evaluation to reject the addon, which is
why their absence is not what admitted the addon in issue #1986.

---

## Inline `.dl` facts (Status: Current)

### Rule

Inline ground facts written in a `.dl` source (e.g.
`role_permission("wr.system_admin", "wr.policy.write").`) are part of
the EDB. After parsing the program, the engine seeds these facts into
the columnar session as base rows exactly once, on the first
plan/session build:

- via `wirelog_easy_open` / any `wirelog_easy_*` lazy entry point: at first
  build, before any host delta callback can be installed.
- via the CLI driver: at the same point in the
  `wl_session_create` → `wl_session_load_facts` →
  `wl_session_load_input_files` sequence.

Snapshots returned by `wirelog_easy_snapshot()` and IDB rows derived by the
optimizer pipeline therefore observe inline facts on the first call,
without any host action.

### Z-set semantics for host insert / remove

`wirelog_easy_insert()` and `wirelog_easy_remove()` are differential operations
on the session's z-set state:

- `wirelog_easy_insert(R, row)` raises the multiplicity of `row` in `R`
  by `+1`.
- `wirelog_easy_remove(R, row)` lowers it by `-1`.

If a host inserts a row that is already present from the inline-fact
seed, the row's multiplicity becomes `+2`. A subsequent
`wirelog_easy_remove()` of the same row leaves multiplicity `+1`; the row
remains observable in snapshots until both copies are retracted.

This matches differential dataflow conventions and is consistent with
`wl_session_*` (internal) and the future `wirelog_session_*` advanced
surface (see `stable-release-plan.md` §3).

### How a host can mirror static facts safely

Two patterns are supported:

1. **Do nothing** — the engine seeds inline facts on its own. The host
   is free to insert *only* the facts it wants to add at runtime.
2. **Pre-check via `wirelog_program_get_facts`** — a host that needs
   to know which inline facts are present can iterate them and skip
   matching rows in its mirror logic. This avoids the +2 multiplicity
   case if the host design demands set semantics.

### What is *not* promised

- The relative *order* of inline-fact rows visible in a snapshot is
  not stable across releases. Hosts must not depend on positional
  ordering.
- The *identity* of intern ids assigned to symbols in inline facts is
  not stable across runs of the same program.

  Because ids are unstable, nothing observable may be derived from their
  order. The ordering operators (`<`, `>`, `<=`, `>=`) and the ordering
  aggregates `min()`/`max()` therefore compare `string`/`symbol` columns
  **lexicographically**, not by id — see `docs/SYNTAX.md`, "Comparison
  Operators". Id order still leaks through in two places. A column with no
  declared type: the engine has nothing to compare but the ids, so the
  result is as unstable as they are -- declare the relation. And a
  comparison with one string and one numeric operand: it stays an integer
  comparison, described in `docs/SYNTAX.md`.

  A column declared `symbol` whose values were never interned is the
  aggregate's version of the same caveat as the digests below: `min()` and
  `max()` reduce it numerically -- the numeric answer for numeric data --
  rather than failing the query, and report the mistype under `WL_LOG=EVAL`.
  Where a group mixes interned and un-interned values, the interned ones
  win, in both directions, so the comparison does not depend on how many
  strings happen to have been interned when the row is visited.

  `hash()` and the digest family — `crc32_*`, `md5`, `sha*`,
  `hmac_sha256`, `uuid5` — take the **string's bytes** for a column
  declared `symbol`/`string`, `strlen()` many with no NUL terminator, and
  the 8-byte `int64` representation for a numeric one. So a fingerprint
  is a property of the data and reproduces outside wirelog. Two caveats,
  both consequences of `.decl` types not being enforced. A column with no
  declared type digests the id, and is therefore as unstable as the id
  assignment. A column declared `symbol` whose values were never interned
  digests their `int64` representation — the numeric answer for numeric
  data — rather than failing the query; in head position a failure would
  abort the whole projection, not drop a row. Both are reported under
  `WL_LOG=EVAL`. The guarantee is only as good as the declaration.
- Multiplicities other than the basic z-set arithmetic above (e.g.
  weighted aggregates) are subject to the engine's
  multiplicity-tracking rules, not promised by this document.

### Cross-facade parity (Status: Current)

`wirelog/wirelog-advanced.h` ships the public `wirelog_session_*` surface
as the advanced peer of `wirelog_easy`. Both facades share this exact
semantic model: open-time inline-fact seeding, z-set host insert/remove,
optional pre-check via `wirelog_program_get_facts`. This is the contract
that prevents behavioral drift between the easy and advanced surfaces.

Internal `wl_session_*` and `wl_compute_backend_t` remain private. The
advanced API exposes backend selection through the
`wirelog_backend_kind_t` enum (`DEFAULT`, `COLUMNAR`); new backends
will appear as additive enum values in future minor releases.

#### Advanced explicit spill configuration (#1372)

`wirelog_session_create_with_options` accepts a versioned v1 descriptor with an
optional retained scalar snapshot and explicit spill directory/quota/scratch
caps. NULL preserves legacy defaults; existing creators are unchanged. Inline
facts still load before return. Options and directory storage are borrowed only
for construction; the program stays borrowed for the session lifetime.

`wirelog_session_seal_input` exposes the internal pre-evaluation eligible-input
transition; `wirelog_session_seal_join_input` is a compatibility spelling with
identical behavior. A sealed JOIN input may additionally feed an exact
`VARIABLE → MAP → CONSOLIDATE` direct column-projection rule. Column indices
may repeat or reorder. One FILTER between VARIABLE and MAP may compare one
INT64 column with one integer literal (`=`, `!=`, `<`, `>`, `<=`, `>=`), with the
column on the left. Other predicates and arbitrary cold expressions remain
unsupported. An initial `VARIABLE → JOIN` may instead consume a cold left
source, including two sealed sources or the same binding on both sides.
Each JOIN uses one matching INT64 or STRING equality key. Chained JOINs continue from an admitted
resident intermediate; FILTER/MAP before the first cold JOIN, multi-key/cross/
theta JOINs, other key types and right filters remain unsupported. An eligible direct/filtered projection alone is sufficient; a
JOIN consumer is not required. Reads
use independent bounded pins; projected output and consolidation remain resident.
Output capacity is admitted for the full source row count even if no row passes.
It retains the conservative serial immutable-plan
eligibility checks and makes all fact/compound mutation unsupported after
success. Additional already-loaded eligible inputs may be sealed independently
before evaluation; failure preserves prior bindings and the failed resident
source. Complete narrow projections over other resident scalar EDBs coexist
using their own schema checks. Refusal cannot reopen the seal window after
an evaluation attempt. Cold lookup never substitutes empty input for an
unsupported consumer. Every session operation remains externally serialized.
Memory admission, allocation and disk capacity/quota failures retain distinct
public error classifications. This advanced-only opt-in has no easy-facade
counterpart; existing cross-facade defaults remain unchanged. Full lifecycle,
limits, platform cleanup and a complete host example are in
[MEMORY.md](MEMORY.md#public-advanced-spill-configuration-and-sealing-1372).

#### Parity audit (v0.41 / #785)

The cross-facade contract is mechanically pinned by a per-test
parity rule: every `test_*` function in
`tests/test_wirelog_easy.c` is either (a) **paired** with a
same-named function in `tests/test_wirelog_advanced.c` or
(b) **annotated** with a `/* PARITY: ... */` block-comment on the
line(s) immediately preceding the declaration, naming the
structural reason no advanced analogue exists (typically the
easy facade carries a convenience surface -- `*_sym` variadics,
`wirelog_easy_open_opts_t`, `wirelog_easy_print_delta` -- that
has no `wirelog_session_*` counterpart).  Enforced by
`scripts/ci/check-test-parity.py` (`meson test --suite abi:test_parity`).

The rule is per-test, not a numeric ratio: future additions on
either side cannot silently regress parity.  Either pair the new
test or annotate the easy declaration with the structural reason.

**Reverse parity is intentionally asymmetric.**  Advanced-only
tests (`test_create_columnar`, `test_create_invalid_backend`,
`test_null_safety` for backend selection) do not back-port to the
easy facade -- the easy surface hides backend selection by
design.  The asymmetry is recorded here so future maintainers do
not assume symmetric coverage.

Standalone direct/filtered cold projections also accept STRING lanes from the
originating program's intern table, preserving STRING output types and IDs.
Every STRING lane is validated at capture, including unprojected lanes. Empty
strings and non-ASCII strings retain their ordinary intern identity. JOINs
also preserve STRING payload lanes and compare STRING keys only after both
inputs prove the same originating intern table. Coincident IDs from another
table are refused; SIDE/nested layouts and whole-compound consumers remain
unsupported. Declared flat INLINE fields may use the same scalar projection
and JOIN shapes after source-layout and original-use provenance checks. Pins retain backing storage; the
public program must still outlive its sessions, and intern storage is a resident
memory floor. A copied raw ID alone does not extend that lifetime.

### References

- `wirelog/wirelog-easy.c` — `ensure_plan_built` performs the seed.
- `wirelog/session_facts.c` — backend-agnostic loader.
- `wirelog/cli/driver.c` — the canonical sequence the easy facade mirrors.
- `wirelog/wirelog.h` — `wirelog_program_get_facts` for host pre-check.
- `CHANGELOG.md` — entry for #718.
- `stable-release-plan.md` §3, §7 — public-surface and conformance plans.

## Recursive aggregation residue (Status: Implemented for MIN/MAX outside a shared SCC)

Tracked under #692.

### Definition

Residue is the count of `'not yet implemented'` markers and disabled
conformance tests in `tests/test_recursive_agg*.c` that prevent
recursive aggregation programs (CC-min, SSSP-max, count-stratified)
from producing semantically correct output at workers in {1, 4, 8, 16}.

### Current state

- `WL_PLAN_OP_REDUCE` carries the aggregate expression into the exec
  plan, so recursive aggregate rules such as `min(l)` and `max(d + w)`
  aggregate the requested expression rather than the fallback column.
- Recursive MIN/MAX IDB materialization is canonicalized after fixed-point
  convergence in both sequential and TDD final-merge paths, removing
  dominated rows from snapshots.
- `tests/test_recursive_agg_conformance.c` covers CC-min, SSSP-max, and
  stratified COUNT for workers in {1, 4, 8, 16}.
- The old `tests/test_recursive_agg*.c` DD/Rust FFI harness remains
  disabled because that backend was removed; the columnar conformance
  harness is the active replacement.
- `col_op_reduce_weighted` remains a weighted Z-set helper and is not part
  of the #692 MIN/MAX recursive aggregate closure.
- A rule head carries **at most one** aggregate (#973). `AGGREGATE` holds a
  single `agg_fn`/`agg_expr` pair, so a head such as `t(g, min(v), max(v))`
  kept only the last aggregate and emitted fewer columns than its `.decl`
  declared. In a recursive stratum that mismatch is an out-of-bounds read:
  `col_op_reduce` sizes the output region from the emitted arity while
  `col_rel_append_all` reads the declared arity. Such heads are now rejected
  during lowering. Supporting them only in non-recursive strata was
  considered and rejected — stratification runs *after* rule lowering, so
  `convert_rule` cannot know whether the head relation is recursive, and
  `col_canonicalize_recursive_aggregate_relation` is on the hot path for
  recursive aggregate rules regardless of the MIN/MAX guard.
- The #973 rejection does not, on its own, cover every arity mismatch: a
  single-aggregate head with too few arguments for its `.decl`, such as
  `cc(y, min(c))` against a 3-column `cc`, reaches the same out-of-bounds
  read without ever having two aggregates. That shape is now rejected
  separately, before lowering, by the rule-head arity check in
  `wl_ir_program_collect_metadata` (#977). The two checks are complementary
  and neither subsumes the other — an arity check passes
  `t(g, min(v), max(v))`, which has three head arguments against a 3-column
  `.decl`, and the aggregate-count check passes `cc(y, min(c))`, which has
  one aggregate.
- Both checks cover the parser path only. IR built directly through the API
  still reaches the crash.
- **A recursive MIN/MAX aggregate may not share an SCC with any other
  relation** (#1021). The heading above says "outside a shared SCC" because
  of this: MIN/MAX is implemented, but only for an aggregate whose stratum
  holds it alone. Plan generation refuses the rest.

  The reason is that canonicalization runs at the fixpoint, so a relation in
  the aggregate's own SCC reads the aggregate's per-iteration content --
  each round's per-rule REDUCE output, before that round's cross-rule
  domination. What such a consumer observes is decided by the evaluation
  strategy rather than by the program, and the two build configurations
  disagree: the issue's repro answers `Big(3) Big(4)` over labels that all
  settle at 1 in the default build, and additionally corrupts the aggregate
  to `Label(3,2) Label(4,3)` under `ENABLE_K_FUSION=0`. This is *not* limited
  to consumers that read the aggregate column non-monotonically; a consumer
  with no predicate on that column at all is configuration-dependent for the
  same reason.

  Stated as an SCC rule rather than as "no other relation may reference the
  aggregate" because `wl_ir_stratify` assigns one stratum per SCC, so the two
  are the same rule and the second understates it.

  The check is `validate_recursive_aggregates()` in `wirelog/exec_plan_gen.c`,
  and it must stay above `rewrite_lftj_chains()` and
  `rewrite_multiway_delta()`: after those, a fused aggregate relation holds no
  REDUCE operator to find.

  The rule is coarse and knowingly refuses programs that answer correctly in
  both build configurations today; the CHANGELOG entry for #1021 names five.
  One of the five is not merely accidentally correct: when the aggregated
  value is functionally determined by the group key, no value is ever
  dominated away, so a round's content is the fixpoint's content and
  configuration-dependence is impossible by construction. The rule refuses
  that class anyway, because it reasons about SCC shape rather than about
  functional dependencies. Narrowing it is Future work, below.

### References

- Issue #692 — Blocker B5: Recursive aggregation residue = 0.
- Issue #973 — at most one aggregate per rule head.
- Issue #977 — `.decl`-versus-program arity validation: inline facts and
  rule heads are validated; rule *bodies* and facts on undeclared relations
  are not.
- `wirelog/columnar/ops.c` — `col_op_reduce`.
- `wirelog/columnar/eval_serial.c` — recursive aggregate canonicalization.
- `wirelog/ir/program.c` — `convert_rule` multi-aggregate head rejection.
- `tests/test_recursive_agg_conformance.c` — active columnar harness.
- Issue #1021 — a recursive MIN/MAX aggregate may not share an SCC with any
  other relation.
- `wirelog/exec_plan_gen.c` — `validate_recursive_aggregates`, both passes.
- `tests/test_recursive_agg_contract.c` — the #1021 rejection and its two
  acceptance controls.

---

## Per-iteration recursive aggregate domination (Status: Future)

Target: milestone 0.70.0. Tracked under #1135. Narrows the #1021 rejection
above.

### What is deferred

Recursive MIN/MAX canonicalization runs once, at the fixpoint. Moving it into
per-iteration consolidation (`col_op_consolidate_diff`,
`wirelog/columnar/diff.c`) would make each round's cross-rule domination
visible to that round's readers.

That does not by itself make a same-SCC consumer well defined, and this
section does not claim it would. The observation window has two halves:

- **Intra-round** — a consumer that reads one rule's REDUCE output before a
  sibling rule of the same head has contributed a dominating value in the
  same round. Eager domination in consolidation removes this half.
- **Inter-round** — a consumer that reads a value that a *later* round
  dominates away. This half survives eager domination, and it is the half
  the #1021 repro exercises, which is why #1021 is a plan-time rejection
  rather than a scheduling change.

So the deferred work buys real correctness, but not all of it, and the
rejection above cannot simply be lifted once it lands.

### Preconditions

- The `recursive_agg_contract_nofusion` CI test target runs the
  recursive-aggregate fixtures under `ENABLE_K_FUSION=0`. Every fixture a
  narrowing would readmit has to be shown to answer the same in both build
  configurations.

  The tree is not starting from nothing here. `k_fusion_memory_nofusion`
  (`tests/meson.build`) is built with `c_args: ['-DENABLE_K_FUSION=0']` and
  runs in the default `meson test`, evaluating a transitive closure at K=2
  and K=4; `bench/meson.build` has a second such target. The recursive
  aggregate contract now follows that pattern. (Separately,
  `scripts/ci/check-clang-tidy-ratchet.py` scans `wirelog/exec_plan_gen.c`
  under the macro via `ALTERNATE_CONFIGS`, but that is static analysis and
  runs nothing.)
- Re-admission fixtures, the programs #1021 refuses that answer correctly in
  both configurations now, and the counter-fixtures a narrowing must keep
  refusing. Both sets are written out in #1135.

### Non-goal

`wl_plan_stratum_t.is_monotone` is computed (negation only) and has no
consumer. It is not a clearance signal for this work: it does not decide
whether a same-stratum rule reads an aggregate column non-monotonically, and
#1021 established that non-monotone reads are not the whole hazard anyway.

---

## Numerical fail-closed arithmetic (Status: Current)

Tracked under #822. Milestone v0.43.

### Rule

Columnar expression evaluation is fail-closed for integer conditions
that cannot be represented as an `int64_t` result:

- `+`, `-`, and `*` use checked `int64_t` arithmetic.
- `/` and `%` reject divide-by-zero and the `INT64_MIN / -1`
  representability overflow.
- `bshl` and `bshr` reject negative shift counts and shift counts
  greater than or equal to 64.
- `to_number()` inside columnar expressions rejects numeric prefixes
  outside the `int64_t` range.  Non-numeric and empty strings remain a
  successful conversion to `0`; numeric prefixes such as `"42abc"`
  still parse the leading number.

The failure mode depends on the operator context:

- `FILTER` predicates reject the row and continue evaluation.
- `MAP`/head expressions return `ERANGE` to the caller.
- `REDUCE` aggregate expressions and `sum()` accumulation return
  `ERANGE` to the caller.

Bitwise `band`, `bor`, `bxor`, and `bnot` are total over the stored
`int64_t` bit pattern.  Hash, crypto digest, HMAC, and UUID built-ins
define their own folding or availability behavior and are not
overflow-prone arithmetic operators under this rule.

### Audit notes

- `wirelog/columnar/ops.c` is the runtime enforcement point.  Both the
  slow bytecode evaluator and compiled evaluator share checked
  arithmetic helpers for `+`, `-`, `*`, `/`, `%`, `bshl`, and `bshr`.
  The filter path fails closed by row rejection; MAP and REDUCE paths
  propagate expression failure as `ERANGE`.
- `wirelog/string_ops.c` keeps the legacy `string_ops_to_number()`
  saturation behavior for direct internal callers, but exposes
  `wl_string_ops_to_number_checked()` for columnar expression
  evaluation.  The checked helper rejects `int64_t` range errors with
  `ERANGE`.
- `wirelog/passes/` rewrites and annotates IR but does not perform
  runtime integer arithmetic for Datalog expression results.  The
  serialized expression tags from `wirelog/exec_plan_gen.c` are
  evaluated by the columnar runtime described above.
- `col_op_reduce_weighted()` remains a weighted Z-set helper for signed
  multiplicity accounting, not a Datalog arithmetic expression
  evaluator.  It is outside the #822 user-expression fail-closed
  contract.

### Coverage

`tests/test_arithmetic_overflow.c` covers overflow and invalid
arithmetic in filters, MAP/head expressions, REDUCE aggregate
expressions, `sum()` accumulation, shift bounds, and checked
`to_number()` range errors.  `tests/test_string_ops.c` covers the
checked and legacy `to_number()` contracts.

### References

- Issue #822 — numerical safety unified: overflow -> fail-closed.
- `wirelog/columnar/ops.c` — runtime expression evaluators and REDUCE.
- `wirelog/string_ops.c` — checked `to_number()` helper.
- `tests/test_arithmetic_overflow.c` and `tests/test_string_ops.c`.

---

## v0.40 API audit closure (Status: Current)

The v0.40 API audit (epic #680, Risk-C and Blocker-B series catalogued
in `stable-release-plan.md` §1.3) drove a sweep of the public-API
surface against `AGENTS.md:17-20` (public uses `wirelog_*` /
`WIRELOG_*`, internal uses `wl_*` / `WL_*`).  Every concrete open
question raised during the audit pass has been resolved through a
landed PR; this section records the closures so v1.0 readers can
reconstruct the reasoning.

### Decisions taken

| Question | Decision | Anchor |
|---|---|---|
| `wirelog/io/io_adapter.h` classification | Public, with full `wirelog_io_*` rename + ABI version 1u→2u | #762 (replaces audit-era #706) |
| `wl_easy` facade prefix | Renamed to `wirelog_easy_*`; file moved to `wirelog/wirelog-easy.h` | #756 |
| `WL_STR_FN_*` enum constants | Renamed to `WIRELOG_STR_FN_*` | #757 |
| `wl_on_delta_fn` / `wl_on_tuple_fn` callback typedefs | Renamed to `wirelog_on_*_fn` | #758 |
| `WL_PUBLIC` export-attribute macro | Renamed to `WIRELOG_PUBLIC` | #759 |
| `wl_intern_t` typedef on `wirelog/wirelog.h` | Renamed to `wirelog_intern_t` (internal `struct wl_intern` tag retained) | #760 |
| Future-drift prevention | `scripts/ci/check-public-prefix.py` (suite `abi`) refuses any `wl_*` / `WL_*` symbol on a public installed header | #761 |
| Public-header SSoT 3-way verification | `scripts/ci/check-public-header-surface.py` already shipped at #705; PR #770 added the standalone-include compile matrix that closes Blocker B2 | #689 (B2) |
| Symbol-visibility default | `gnu_symbol_visibility: 'hidden'`; `WIRELOG_API` annotation gates the dynamic-symbol table (alias of `WIRELOG_PUBLIC`; see `wirelog/wirelog-export.h`); SOVERSION=1 decoupled from `project_version` | #733 (K0/K1/K2), #782 |

### What remains open

No audit-era "binding decision required" notes remain unresolved at
the time of writing.  The audit residue in `.omc/plans/issue-706-brief.md`
references its own forward-link to #762; subsequent ABI work (libabigail
manifest, branch-protection, signing) is tracked under v0.41+ blockers
(#690 B3) and the v1.0.0-rc and GA epics, not under the v0.40 audit.

### References

- Epic #680 — v0.40 API Audit.
- Epic #755 — public-API prefix rename rollup (closed all sub-issues
  above).
- `stable-release-plan.md` §1.3, §12.1.
- `docs/THREADING.md` — threading model + atomics audit (closes #734
  under v0.41 epic #681).

---

## Optimizer-equivalence conformance (Status: Implemented)

Tracked under #700. Milestone v0.43.

### Matrix definition

The optimizer-equivalence matrix exercises 4 optimizer passes × {on, off}
= 16 toggle combinations, running each combination through a fixed program
corpus and asserting that every result Z-set equals the result Z-set
produced by the baseline (all passes enabled). The four passes and their
entry-point symbols are:

| Pass | Entry-point symbol |
|---|---|
| Magic Sets | `wl_magic_sets_apply_with_demands` |
| SIP (Supplementary Magic Sets / sideways information passing) | `wl_sip_apply` |
| Logic Fusion | `wl_fusion_apply` |
| JPP (Join-Project-Plan) | `wl_jpp_apply` |

### Current state (v0.43)

- **Harness present.** `tests/test_optimizer_equivalence.c` runs the 16-way
  matrix over join, recursive, and aggregate programs and is registered as
  `meson test --suite optimizer`.
- **Toggle API consumed.** `wirelog_optimize_with_config` is the shared
  optimizer facade for public callers. The CLI and easy facade call
  `wirelog_optimize()`, which funnels through the config-aware path.
- **Subsumption is canonicalization.** `wl_subsumption_apply` still runs
  before the toggle axis and is intentionally not treated as an optimizer
  pass in the matrix.
- **Taxonomy decision.** The public enum `wirelog_opt_pass_t` at
  `wirelog/wirelog-optimizer.h:57-64` lists
  `WIRELOG_OPT_LOGIC_FUSION / WIRELOG_OPT_JOIN_PROJECT_PLAN /
  WIRELOG_OPT_SEMIJOIN / WIRELOG_OPT_SUBPLAN_SHARING /
  WIRELOG_OPT_BOOLEAN_SPEC`. Magic Sets remains outside that public enum for
  v0.43; the matrix names the internal `wl_*_apply` symbols directly.

### Definition of "all-free adornment"

An adornment is "all-free" when `bound_mask == 0`: every argument position
of the adorned predicate is free at the Magic Sets demand site. The Magic
Sets pass handles this at `wirelog/passes/magic_sets.c:761-765` (a demand
root) and `wirelog/passes/magic_sets.c:874-877` (an IDB body atom in the
Phase 2 BFS) by taking a skip path — no demand propagation is emitted
because there are no bound columns to propagate. The other three passes
(SIP, Logic Fusion, JPP) have no equivalent adornment concept and therefore
no analogous counter or skip path.

### Definition of "aggregate-skip"

When a pass encounters an AGGREGATE node it cannot safely rewrite, it must
skip that rule entirely without miscompiling. Magic Sets, SIP, Logic Fusion,
and JPP expose `skipped_aggregate` counters in their internal stats structs.
The matrix test verifies those counters and asserts that Magic Sets leaves an
aggregate demand untransformed.

### v0.43 closure

The v0.43 implementation keeps Magic Sets off the public
`wirelog_opt_pass_t` enum and expresses the conformance axis in terms of the
internal `wl_*_apply` symbols. This avoids a public enum expansion while still
testing the actual optimizer pipeline.

### References

- `wirelog/wirelog-optimizer.h:57-64` — `wirelog_opt_pass_t` enum.
- `wirelog/wirelog-optimizer.h:71-79` — `wirelog_opt_config_t` struct.
- `wirelog/wirelog-optimizer.h:134-137` — `wirelog_optimize_with_config` declaration.
- `wirelog/passes/magic_sets.c:212-226` — `relation_has_aggregate_rule()`, the
  aggregate-skip predicate. It is what excludes aggregate rules, not head
  extraction: since Issue #990 `get_head_vars()` no longer names AGGREGATE and
  returns 0 for such a root only because it carries no head projection. Applied
  at `:767-771` (demand root) and `:848-852` (Phase 2 BFS body atom).
- `wirelog/passes/magic_sets.c:761-765`, `:874-877` — all-free adornment skip
  paths.
- `tests/test_optimizer_equivalence.c` — 16-combination equivalence matrix.
- `stable-release-plan.md` — v0.43 milestone scope.

## Evaluation-control foundation and controlled initial STEP (#1819, #1821)

The [#1821 closure plan](implementation/1821/CLOSURE_PLAN.md) tracks serial
coverage, remaining work packages, and the evidence required for closure.
Its [serial coverage matrix](implementation/1821/SERIAL_COVERAGE.md) distinguishes
session reachability, private operator evidence, and remaining enforcement gaps.

`wirelog/evaluation_control.h` is internal and uninstalled. It provides a
retained control, exclusive logical-session ownership, attempt accounting,
cooperative cancellation requests and completed reports. Internal session
options v3 append `evaluation_control`; v2 callers retain their existing
memory-governor/Windows-handle behavior. Creation normalizes a verified prefix
into a full v3 object; v2 unknown tails are ignored. Options storage is borrowed
only during creation. A control on a backend without the options hook is refused.

An attached control enables a limited controlled initial `wl_session_step()`
for an acyclic plan using the columnar backend. Before evaluation, the session
checks every stratum, rule head, and operator kind. Unsupported recursive plans, parallel
execution, differential/retraction state, unsupported operator kinds, and
pre-existing rule-head relations return `ENOTSUP` without evaluation. The
observed callback route is currently limited further to one unary INT64 EDB
rule with `VARIABLE -> MAP(identity col0) -> CONSOLIDATE`, optionally with
one FILTER between VARIABLE and MAP. Predicate validation and execution use
the existing charged numeric FILTER path, including rejected rows.
Its missing IDB head is represented by a virtual empty observer baseline; that
baseline does not register or publish the relation. The framed evaluator
registers the output only with its normal commit. A separately registered
empty IDB head is rejected because replacing an existing target is outside
this route. All other callback plans return `ENOTSUP` before the control
attempt begins.

The framed evaluator validates each operator's detailed metadata before
executing that operator; a later execution error can therefore follow an
already published rule prefix. A work stop at a rule boundary preserves that
committed prefix, and a fresh STEP attempt resumes at the first unfinished
rule. In the observed route, a stop inside FILTER retains the unevaluated baseline
for a reset/retry of the same logical STEP; unrelated observer state is still
rejected. A stop after rule commit resumes observer
preparation or delivery without rerunning the rule. Before callbacks begin,
the complete event batch is charged and a final cancellation cutoff is polled.
Publication then latches before entering user code and delivers the full batch
synchronously without further polls. Cancellation requested by a callback
does not truncate delivery or change that attempt's successful result. A
cleanup retry after publication resumes retirement only and never replays
events.
Repeated STEP after full completion is a semantic no-op. Because incremental
re-evaluation is not implemented in this slice, session mutations remain
`EBUSY` after completion until the session is destroyed. Snapshot is `ENOTSUP`
before an eligible controlled STEP completes and `EBUSY` while that STEP is
pending. After completion, read-only snapshot delivery is allowed under a
separate control attempt; mutation and another concurrent session operation
remain blocked. A null callback is rejected before a control attempt begins.

Controlled completed snapshot prepares a private callback-ready row batch
inside its own attempt. Plan entries, registry slots and name bytes are
charged before traversal; scalar-source readers protect the copied tuples.
Checked workspace sizes are admitted to the memory governor before allocation.
Cancellation, work exhaustion, reader contention or allocation failure during
preparation publishes no tuples and releases workspace and readers. Retrying
starts a fresh snapshot attempt against the unchanged completed result.

Readers are released and finite dispatch work is prepaid before the final
cancellation check. Once publication starts, the full callback batch completes
without further polls, so cancellation requested from a callback remains
sticky for the next snapshot. This finite, already-admitted dispatch tail is
proportional to the prepared entries and rows; it is not a 256-action polling
bound across the callback batch. Host callback bodies remain cooperative and
non-preemptible. No lookup, tuple gathering or allocation occurs after the
cutoff. Names, row order, duplicate plan entries and empty/missing results
retain the raw emitter's behavior. This path does not evaluate or modify the
completed result; fresh/dirty evaluation remains unsupported here.
Uncontrolled sessions retain existing behavior.

When framed host setup or cleanup must be retried, STEP returns `EAGAIN` with
the original operation attempt active. Only another `wl_session_step()` may
resume it; other session operations and control reset/configure return `EBUSY`
while that attempt is active. Once a work stop finishes the attempt, control
reset/configure may raise or remove its limit, but other session operations
remain `EBUSY` until the logical STEP completes. Session destruction drains an
active framed continuation before releasing session storage. This controlled
initial-step slice does not complete whole-session recovery, general observer
delivery beyond the exact unary identity-map route above, recursive,
differential, weighted, TDD/parallel, or every fallback path, and does not
close #1817 or expose an installed public option.

### Implemented primitive contract

- `create(limit)` gives the caller one reference. `attach(control, owner)`
  claims a non-NULL stable logical-owner key and retains a separate reference.
  A second attachment, including the same key, returns `EBUSY`. `detach` is
  quiescent and drops that reference; `transfer` changes the key without an
  extra attachment/reference. The session wrapper claims with its admission
  object before backend construction, unwinds failed construction, and detaches
  after backend destruction drains borrowers. Worker copies borrow the pointer
  and clear their ownership marker.
- The future easy wrapper must claim at open, including lazy open, and transfer
  its logical claim when materializing the backend. Host references remain
  independently owned; a cancelling thread retains one across its request.
  No operation accepts a dangling pointer or makes session destruction generally
  concurrent with arbitrary host calls.
- `begin(control, owner)` starts exactly one attempt, freezes the configured
  limit, increments its identity and resets consumption/stop/error. Identity
  exhaustion returns `EOVERFLOW` before starting; IDs never wrap. A successful
  begin always needs finish, including pre-cancelled attempts.
- Zero limit means unlimited. `charge(work)` accounts **admitted logical work**
  before processing; `charge(0)` polls. Exact allowance is legal; a charge larger
  than remaining allowance fails without consuming it. Arithmetic uses checked
  subtraction. Unlimited accounting past `UINT64_MAX` latches `EOVERFLOW`, an
  accounting failure rather than budget exhaustion, without wrapping.
- Cancellation is sticky until quiescent `reset`. At a check with no prior
  disposition, cancellation observed there wins over a new budget denial.
  `WL_EVALUATION_CONTROL_CANCELLED` and `WL_EVALUATION_CONTROL_WORK_LIMIT` are
  distinct negative dispositions, not errno aliases. A previously latched stop
  or accounting error persists; subsequent charges do no work.
- Concurrent worker charges share one allowance under the control mutex.
  Configuration, reset, owner transfer and detach refuse active attempts.
  A host serializes lifecycle calls with session operations; finish requires
  all worker charges drained. `request_cancel` alone needs no control/session
  mutex and can run from another thread with its own live reference.
- `finish(owner, execution_status, cleanup_status)` preserves both errors and
  the independently latched stop. Return precedence is genuine execution error
  (including accounting overflow), cleanup error, then control stop. A control
  stop passed as execution status becomes the stop field rather than an engine
  failure. Finish does **not** poll: its caller must perform the final check at
  the publication cutoff. Late requests remain sticky for the next attempt.
- `report` returns `ENOENT` until the first finish, then a coherent last-completed
  record: attempt ID, frozen limit, admitted consumption, stop, execution error,
  cleanup error and final result. Configuration/reset and the next active attempt
  do not erase or mutate that completed report. Counts are not CPU instructions
  or proof that all admitted work completed; interruption can leave admitted
  work unprocessed. No refund/reservation machinery is implemented here.

### Required downstream work metric and checkpoint placement

The table is an **integration contract**, not a claim of current instrumentation.
One logical unit charges one listed primitive action before performing it.
Implementations must check at entry and after at most
`WL_EVALUATION_CONTROL_QUANTUM` (**256**) admitted units per active worker or
coordinator; final tails check too. This limits work between checks, not elapsed
return time. Bulk reservation must disclose unused admitted slack (at most one
quantum per active worker), never report it as measured completed work.

Fresh contiguous relation lanes have a separate allocation tariff: before an
allocator call, admit one logical work unit for each requested 64-bit cell,
across slices capped by both the caller's remaining quantum and 256 units. A
slice never charges more than its caller can admit. If it exhausts that caller
quantum, the builder yields even when the tariff is complete; the allocator
action is admitted by a later call. Partial precharge is bound to the exact
control identity and attempt ID that began it, so a different control or a
successor attempt cannot resume it. The allocator call itself is a finite,
noninterruptible region and copies no existing relation cells; failed
allocations do not refund the admitted work. This tariff bounds the capacity a
finite attempt can request before allocation, while row copies remain charged
and resumable independently.

| Engine path | Charged primitive and checkpoint obligation |
|---|---|
| `columnar/eval_plan.c`, `eval_serial.c`, `eval.c` | One operator dispatch or recursive progress transition, including empty progress; internal replay keeps the same attempt |
| `columnar/ops.c`, `filter.c`, `expr.c` | One input-row visit/expression evaluation; rejected rows count |
| `columnar/join.c`, `join_batch.c`, `diff_join_batch.c`, `join_pipeline.c` | One hash/build/probe-chain visit or candidate-pair inspection; unsuccessful probes and rejected candidates count |
| `columnar/lftj.c` | One seek/comparison/traversal action, including unsuccessful seeks |
| `columnar/relation.c`, `arrangement.c`, `eval_dedup.c`, `merge.c`, `partition.c` | One compare/hash action or copied/visited scalar cell; fresh contiguous relation lanes additionally admit one unit per requested 64-bit cell before allocation; sort, consolidation, materialization and coordinator partitioning require bounded chunks |
| `columnar/eval_tdd_plan.c`, `eval_tdd_queue.c`, `kfusion.c`, `workqueue.c` | One dispatch/exchange/progress action; payload processing additionally charges rows/cells, and blocking waits must observe stop |

The internal `wl_columnar_merge_incremental_delta_attempt_advance` path charges
each copied or compared scalar cell, each copied evaluator-dedup slot, and each
nullary tuple action before touching it, and caps each call at 256 units.
`EAGAIN` retains its private candidate and source writer for the next slice.
Cancellation and global work exhaustion discard the candidate; commit polls
once more before publishing the consolidated source image and exact new-row
delta. Preparation allocations/admission and mandatory cleanup remain
synchronous regions. The legacy recursive evaluator still calls the
synchronous consolidation entry point, so this opt-in attempt is a bounded
operator building block and does not yet claim recursive STEP coverage.

`wl_columnar_merge_incremental_delta_attempt_begin_candidate` extends that
primitive for a sorted private working relation and a separate staged
candidate relation. It currently accepts positive-arity, all-`INT64` relations
without timestamps, compound/graph metadata, evaluator-dedup tables, or Arrow
schema metadata. Zero-column relations remain unsupported. The working
relation must already be canonical and sorted; candidate rows may be unsorted
and repeated. A successful commit publishes
`set(working U candidate)` and returns exactly
`set(candidate) - set(working)`. The candidate is never mutated. A working
writer and candidate reader are acquired without waiting, in relation-address
order, and held through publication or cleanup. Every copied cell and tuple
comparison is charged before access, including candidate sorting and duplicate
removal. A same-attempt `EAGAIN` retains the continuation for the next slice;
cancellation or global work exhaustion discards private output and releases
both input protections. Retrying after either terminal stop starts a fresh
merge from the unchanged input relations under a new control attempt. The
primitive does not yet connect to recursive evaluator dispatch or enable
controlled recursive STEP.
The ordinary recursive SCC transaction is a separate integration: it evaluates
complete preserved positive bodies from empty private INT64 heads, freezes all
head inputs for each round, and compares canonical CONSOLIDATE results one cell
per charged action. It does not invoke the candidate-merge primitive above.
Every child cleanup receipt is ACKed before rebinding. Later strata see only
completed private predecessors, and all public heads remain unchanged until
one prepaid registry installation. Input removals recompute from empty state;
signed callbacks describe the difference, not execution of differential
operators. Work exhaustion retains the private round; cancellation drains it
before retry. Obsolete round storage remains owned by a stable retirement
receipt even after its descriptor is freed. The exact legacy binary route
retains its independent admission and cost contract. See the
[G3a qualification ledger](implementation/1821/CLOSURE_PLAN.md#g3a-implementation-and-qualification-2026-10-27)
for admitted types/operators, iteration ceiling and remaining paths.

Generated LFTJ operators in the supported serial controlled composition now
use an internal resumable attempt. It charges source-name bytes and candidate
lookups during discovery, copies at most 256 scalar cells per action, and uses
a bottom-up merge sort with at most 256 compare/copy units per action. Join
seek, candidate traversal, output-row construction and output-cell writes are
charged before work; the controlled LFTJ route refuses schemas wider than 64
columns. Each cleanup retry validates and charges its own live funding grant;
source-reader admission and release perform at most one gate CAS per action.
Admission retains the descriptor pin while it waits for the storage-owner gate;
it returns EAGAIN without advancing the input cursor, and snapshots relation
shape only after both gates are held. Descriptor admission precedes storage
owner admission, matching relation mutation lock order. Reservation finishing
first claims a preparation state and publishes an immutable receipt; later
same-thread calls return governor credit with at most one CAS per action.
Writer-finalization refusals retain the successful semantic result and the
draining writer for retry. Relation destruction, allocator/Arrow release and
stack publication remain synchronous cooperative cleanup boundaries. This
route does not make other LFTJ entry points or fallback paths controlled.

Before allocating its private output row or copied input/sort arrays, this
route admits a checked allocation tariff in resumable charges of at most 256
units. The tariff is one unit per output-row scalar slot plus eight units for
that allocation, and for each nonempty input two units per scalar slot across
its row and scratch arrays plus eight units per array allocation. Empty inputs
have no row/scratch allocation tariff. The size pass charges eight units for
initialization and eight units per input descriptor, and uses checked byte/work
arithmetic. The complete
tariff must fit before the governor reservation or any of those allocations
begin; a one-unit-short limit stops before allocation. A later governor denial
still leaves the already admitted tariff in the attempt's work count. This is
a logical admission cost, not an allocator-time estimate: `malloc`/`free`
latency remains synchronous and has no wall-clock bound.

Controlled initial STEP supports the generated two-column INT64 path plans with
either recursive rule `path(x,z) :- path(x,y), edge(y,z)` or
`path(x,w) :- path(x,y), edge(y,z), edge(z,w)`, together with the base rule
`path(x,y) :- edge(x,y)`. The two-edge form extends each recursive path by two
edges per round, matching the rule's semantics; it is not a general transitive
closure rewrite. Both forms require the exact one-stratum, one-target plan and a
single registered `edge` input. Other recursive join counts and operators are
rejected before an evaluation attempt. The terminal zero-worker `EXCHANGE` is a
charged fixed-cost no-op in this single-session evaluator; it preserves the
result stack and never invokes redistribution. Other `EXCHANGE` positions or
metadata remain unsupported. The evaluator snapshots and leases `edge` from
before the first recursive body read through convergence, then checks that the
snapshot still matches after acquiring the lease.

All recursive body evaluations, private captures, cumulative union and exact
new-row delta work use the root STEP control and attempt. Frontier and
publication relations are built through the managed binary builder, which
admits each fresh lane's requested cells within the caller quantum before
allocation. On initial evaluation, `path` remains absent from the registry
until the frontier is empty, when one final schemaful relation is published.
On a completed route, positive two-column appends to the registered `edge`
relation through `wl_session_insert` are the only mutations admitted. Their
relation snapshot is recorded only after the complete append succeeds. A later
controlled `STEP` recomputes the closure privately and replaces `path` only
after convergence; snapshots and mutations remain gated while that attempt is
pending. The edge reader lease remains held through replacement registration
preparation, a final edge snapshot check, and the registry commit. A stop before
that commit drains child cleanup receipts before releasing the lease. The
prepared registration, merge state and managed private relations are then
cleaned while the session remains pending and supported mutations stay gated;
the previous `path` remains registered for retry. Commit is the publication
point: afterward the new `path` is authoritative, and lease release plus final
private retirement resume as pending cleanup without publishing again. If
relation storage changes outside the admitted append path, the route is
invalidated and neither stale `path` snapshots nor further appends are
admitted. Other recursive plans and mutation shapes remain unsupported, so
#1821 still requires additional operator coverage.

When a composed attempt owns an LFTJ child at the moment its original
operation control latches cancellation or work-limit, the internal
`wl_columnar_eval_plan_numeric_composed_cleanup_lftj()` path can drain that
child without resuming semantic evaluation. Each call requires a fresh
384-unit host grant, verifies the original sticky stop with a zero-work poll,
and performs at most one LFTJ release action. Cleanup preserves an earlier
semantic error; otherwise it records the exact original stop in both parent
and child, so a work-limit is not changed into cancellation. A missing stop
returns `EBUSY` after consuming the host grant. Other control errors are
returned without treating them as cleanup authorization.

After LFTJ cleanup reaches `DONE`, the composed attempt is ACKed and its
cleanup frame is explicitly finished. This discards any private result that
LFTJ had already placed on that frame's stack; it does not publish the result
to the session registry. This cleanup route covers only composed LFTJ owners.

Admitted composed FILTER/MAP unary children and CONCAT children use their
owned step/ACK state machines to drain bounded cleanup after cancellation or
work exhaustion. A child step returns a pending receipt to the parent; the
host must consume that exact receipt in a separate funded action before the
parent continues. ACK validates child cleanup and ownership before resetting
the child context. These routes cover only the admitted composed owners; they
do not enable other composed children, public/session APIs, or a shared
host/scheduler action envelope.

Composed scalar-source owners have a separate
`wl_columnar_eval_plan_numeric_composed_cleanup_source()` route. It skips
source lookup, output-slot creation, and installation once the original
operation has latched cancellation or work-limit. If source end must release
readers, cleanup returns its exact child protocol as a pending receipt; the
caller must consume that receipt under a separate host action before cleanup
can abort a borrowed result slot or release the attempt. Cleanup refuses an
already-pending child receipt without changing it or spending funding. Other
source cleanup actions consume one fresh 384-unit host grant before polling
the original sticky stop. A source-slot abort first checks for the stop, then
charges one 384-unit grant; if result disposal is refused, its borrowed slot
remains owned and can be retried with fresh funding. Neither route installs or
publishes a partial source result.

Fixed serial plan/data/configuration must give reproducible admitted work.
Worker schedules, optimizer choices and releases can change totals; cross-width
iteration/count equality is not promised. Parsing, planning and input ingestion
are outside the evaluation-work count. Output cardinality and iteration count
alone cannot bound an expensive acyclic JOIN with no matching output.

The generated integer FILTER aware-host fixture verifies one concrete checkpoint
boundary after predicate evaluation: it searches fresh executions for the first
configured allowance that reaches a known-true row, then observes `WORK_LIMIT`
on the following charged action. Its retry checks output against a raw reference
and literal rows. That result is limited to this fixture; it does not establish a
universal FILTER cost, FLOAT coverage, cancellation behavior, or coverage of
other evaluator routes.

The controlled session STEP fixture additionally drives 64-by-513 product and
same-key JOIN candidate sets through `wl_session_step`. A deterministic hook
stops the first attempt after 128 phase-17 candidate checkpoints; a measured
one-unit-short allowance then produces a work-limit stop after 127 checkpoints.
Neither stop publishes the rule head. Resetting the control and resuming yields
all product rows, while 32,832 matching keyed candidates rejected by the
following predicate produce an empty head. This proves those two acyclic JOIN
shapes through the session driver, including substantial candidate work and a
no-output case; it does not extend support to recursive, differential,
batch/pipeline, or fallback routes.

Synchronous allocator/runtime calls, whole-input library sorts/copies, host
scalar addons, and output callback bodies cannot be preempted by this token.
Downstream integration must split engine-owned large sorts/copies into bounded
work or reject unsupported controlled paths before execution; merely polling
around an arbitrary whole-input operation does not satisfy the table. Host
addon/callback duration and mandatory cleanup/rollback/task drain are excluded
from the unit-to-return bound. There is no millisecond deadline guarantee.
Cleanup remains permitted after exhaustion so recovery can retain/release owners.

### Delta observer retirement retry

Observer cleanup releases readers and physical staging/baseline storage before
returning their memory credits. A refused credit remains owned with its original
identity so a later cleanup can retry without repeating completed frees. If a
completed STEP has already delivered its callback and bookkeeping but observer
retirement then fails, the next STEP call retries retirement only; it does not
evaluate again or deliver the callback twice. Retirement left by observer
construction failure is cleared before the next evaluation. This defines cleanup
retry only; it does not enable controlled observed STEP evaluation.

### Required recovery and publication contract (not implemented here)

One external STEP or evaluating SNAPSHOT is one attempt, including an internal
snapshot-to-step resume. Internal fallback/replay shares its consumption.
An explicit retry begins fresh accounting but preserves the logical STEP's
original notification baseline, pending inputs and retractions. Configure/reset
must remain possible between stopped attempts even if input mutation is `EBUSY`.
Callback-free retry needs its own recovery proof; observer baseline retention
alone does not establish that property.

Perform a final stop check before observable tuple/delta publication. Before
that cutoff a stopped evaluation delivers nothing; after publication starts,
finish the successful batch and leave late cancellation for the next attempt.
Host callback side effects cannot be rolled back. Notification-only cancellation
remains distinct from evaluation cancellation. The future public surface must
provide retained opaque handles, request/configure/reset, completed reports and
distinct stop errors through both facades, without exposing these internal types.

### Publication-cutoff state map (#1953; both paths implemented)

A callback-free W1 STEP arms `EVALUATE_REMAINING` after each fully successful
stratum, including checked cleanup. A later ordinary error preserves that
completed prefix; STEP or SNAPSHOT resumes at the next stratum. A failed first
stratum creates no completed-prefix token. Nonempty input mutation remains
excluded while the token is pending, and successful STEP completion clears it.
Worker `FINALIZE_NONREC` and final `COMPACT` continuations retain their existing
precedence.

Rows name fields and functions, never line numbers, for the reason
`scripts/ci/ownership_doc_anchors.py` records: a line citation rots within
days. `scripts/ci/check-state-map-anchors.py` checks both columns of every row
in both directions -- each function named must really touch the field, and
every function the gate can attribute the field to must be named. That bound is
real and worth reading: the gate sees functions under `wirelog/`, nothing in
`tests/`, and only those with a single indexed definition. It credits a field
only where a body names it before an assignment or reads it by name, so a bulk
write is invisible -- `col_session_create_internal` zeroes the twelve session
fields through one `calloc`, `base_nrows` belonging to the relation rather than
the session, and it is named in the one row where it also assigns by name. Text
inside a preprocessor directive is invisible too, because the index blanks it
and this gate blanks it with the index. For a qualified subject the gate
requires the field to be reached through `->` from a name the body declares as
a pointer to that struct, so `entry->rel->base_nrows` counts when `rel` is such
a pointer and `entry->owner->base_nrows` does not; a body that declares the
struct and still reaches the field some other way stops the gate rather than
being guessed at. Within that bound the columns are complete, so this section
states no convention about what they leave out. An earlier revision stated one
instead, and it was false on both of its halves: prose the gate cannot read,
excusing omissions the gate cannot see.

What the gate does not check is the verdict, because `PRESERVE` and `COMMIT`
are not facts about the present code. `PRESERVE` means a stopping return must
skip the write; `COMMIT` means it must perform it anyway. Those are judgements
about what a stopped attempt should leave behind, and they are reviewed by
people. The columns exist so that the evidence under them is not also a matter
of opinion.

Scope: the fields the two evaluating paths commit in their bookkeeping block,
the one each runs once its strata are done. Where that block sits relative to
publication is the difference between the paths, and the two bullets below set
it out. The stable-snapshot fast path commits none of them. The columns name
functions, so they cannot separate it from the evaluating path inside
`col_session_snapshot_impl`; what they do show is why that function appears in
the read column of `last_removed_relation` and `retraction_seeded` -- its only
reads of either are the fast path's own guard. It carries a cutoff of its own
for its own emit rather than relying on one placed for the evaluating path.

The columns credit `col_worker_session_create` only where it overwrites a field
by name. Its `*out_worker = *coordinator;` mentions no field and is invisible
to the gate, so the six rows that name it are six explicit resets, and they
come in two groups with different reasons. Four of them are the mapped members
of the `plain_step_completion_*` group, cleared along with that group's other
members because recovery continuations belong to the coordinator and a
disposable worker clone must never inherit one. `last_inserted_relation` and
`last_removed_relation` are nulled much later, as borrowed coordinator pointers
a worker must not use. Both groups refuse to inherit something; neither is
inheritance, which is what the struct copy above them does and what no column
shows.

The two evaluating paths are not mirror images, and the difference decides
where a cutoff belongs.

- `col_session_step_impl` publishes, when it has an observer, before it
  commits: the observer publish invokes the host callback, and the bookkeeping
  follows. The callback-free shape publishes nothing and falls straight into
  the same bookkeeping.
- `col_session_snapshot_impl` commits before it delivers: the bookkeeping, then
  a gauge sample, then the emit.

So the snapshot cutoff must sit above the bookkeeping, not merely above the
emit. By the emit that follows the bookkeeping, `has_evaluated`,
`snapshot_stable_valid` and every `base_nrows` are committed, and no later line
in `col_session_snapshot_impl` rolls them back. How far above the bookkeeping
it sits is settled now that the cutoff exists: above `col_rel_compact_many` as
well. The step cutoff sits after compaction for a reason its own comment gives
-- both shapes carry a resume token across it, and cutting earlier would leave
a stopped plain step with no token -- and that reason does not transfer,
because the snapshot path sets no such token. What settles it instead is the
converse: compaction can return `EBUSY`, and `wl_evaluation_control_finish`
prefers a non-zero execution status over `control->stop`, so compacting first
would report that failure for an attempt the caller asked to cancel.

Until #2114 the snapshot path also pre-seeded `$d$<name>` deltas between its
entry and the bookkeeping, for an evaluation that re-ran the strata an
incremental insert reached on top of the rows the last one had left, reading
the seeded delta in every iteration. That route lost rows and is gone. After
an incremental insert, `session_snapshot_insert` now brings the reached strata
up to date with occurrence-bound instances
(`wl_columnar_eval_tdd_insert_stratum`) and registers no `$d$` relation,
whether or not a delta callback is installed (#2121); any other evaluation
clears the derived rows and evaluates every stratum in full. A snapshot fires
no delta events on either route, including when it first drains a refused
plain step, which resumes without the delta observer; and a later step does
not report the rows the snapshot derived.
A stop between those heads' update and the bookkeeping leaves
`last_inserted_relation`, `pending_input_change` and every `base_nrows` as
they were, so a retry runs the insert route again from the same appended rows
and finds nothing the heads do not already hold. A caller that installs a
delta callback and steps instead of retrying is not told about the rows those
heads already gained; the removed pre-seed route behaved the same way.

A field's row does not mean both paths write it. Four do not: neither path
touches the coordinator's `delta_seeded`, which a TDD worker sub-pass and the
insert route write only on worker clones, and the snapshot path never writes
`plain_step_completion_pending`, `plain_step_completion_phase` or
`plain_step_completion_active`. Which of those three the snapshot path still
reads is in the read column, one field at a time; an earlier revision
summarised it here and got it wrong within one round. Both paths clear
`last_removed_relation` and `retraction_seeded` in their bookkeeping block
through `session_retraction_cleanup`; the snapshot path started doing so in
#2106, when a snapshot that kept a removal staged re-evaluated on every later
call.

| field | written by | read by | verdict |
|---|---|---|---|
| `last_inserted_relation` | `col_controlled_step_recursive_drive`, `col_session_snapshot_impl`, `col_session_step_impl`, `col_worker_session_create`, `session_note_inserted_input`, `col_controlled_step_staging_finish`, `wl_columnar_session_observed_action`, `wl_columnar_session_seal_join_input` | `col_eval_stratum_tdd_recursive`, `col_session_snapshot_impl`, `col_session_step_impl`, `session_note_inserted_input`, `session_snapshot_insert`, `wl_columnar_session_seal_join_input`, `wl_columnar_session_observed_action` | PRESERVE |
| `pending_input_change` | `col_controlled_step_recursive_drive`, `col_session_create_internal`, `col_session_remove`, `col_session_remove_incremental`, `col_session_snapshot_impl`, `col_session_step_impl`, `session_note_inserted_input`, `col_controlled_step_staging_finish`, `wl_columnar_session_observed_action` | `col_session_snapshot_impl`, `col_session_step_impl`, `wl_columnar_session_observed_action` | PRESERVE |
| `pending_full_input_eval` | `col_controlled_step_recursive_drive`, `col_session_insert`, `col_session_remove`, `col_session_snapshot_impl`, `col_session_step_impl`, `session_note_inserted_input`, `col_controlled_step_staging_finish`, `wl_columnar_session_observed_action` | `col_session_snapshot_impl`, `col_session_step_impl`, `wl_columnar_session_observed_action` | PRESERVE |
| `has_evaluated` | `col_controlled_step_recursive_drive`, `col_session_snapshot_impl`, `col_session_step_impl`, `col_controlled_step_staging_finish`, `wl_columnar_session_observed_action` | `col_eval_stratum_tdd_recursive`, `col_session_snapshot_impl`, `col_session_step_impl`, `wl_columnar_session_seal_join_input`, `wl_columnar_session_observed_action` | PRESERVE |
| `snapshot_stable_valid` | `col_controlled_step_recursive_drive`, `col_session_remove`, `col_session_remove_incremental`, `col_session_snapshot_impl`, `col_session_step_impl`, `session_note_inserted_input`, `wl_columnar_session_observed_action` | `col_session_snapshot_impl` | PRESERVE |
| `delta_seeded` | `insert_clone_prepare`, `tdd_worker_subpass_fn` | `col_op_variable`, `has_empty_forced_delta`, `tdd_worker_subpass_fn`, `wl_columnar_eval_nonrec_relation_parallel`, `wl_columnar_join_select_right`, `wl_columnar_join_op_attempt_start_owned`, `wl_columnar_eval_plan_numeric_source_select`, `wl_columnar_eval_plan_composed_source_select`, `wl_columnar_eval_tdd_plan_prepare_inputs`, `wl_columnar_eval_tdd_plan_prepare_insert`, `wl_columnar_join_spill_eligible`, `wl_columnar_session_cold_mode` | PRESERVE |
| `last_removed_relation` | `col_session_remove_incremental`, `col_worker_session_create`, `session_retraction_cleanup`, `col_controlled_step_staging_finish`, `wl_columnar_session_observed_action` | `col_session_remove_incremental`, `col_session_snapshot_impl`, `col_session_step_impl`, `col_controlled_step_staging_prepare`, `wl_columnar_session_seal_join_input`, `wl_columnar_session_observed_action` | PRESERVE |
| `retraction_seeded` | `col_session_step_impl`, `col_stratum_step_retraction_nonrecursive`, `col_stratum_step_with_delta`, `insert_clone_prepare`, `session_retraction_cleanup`, `col_controlled_step_staging_finish`, `wl_columnar_session_observed_action` | `col_controlled_step_static_preflight`, `wl_columnar_filter_consolidate_attempt_start_owned`, `col_op_variable`, `col_session_snapshot_impl`, `col_stratum_step_with_delta`, `has_empty_forced_delta`, `wl_columnar_eval_nonrec_relation_parallel`, `wl_columnar_join_select_right`, `wl_columnar_join_op_attempt_start_owned`, `wl_columnar_join_probe_op_attempt_start_owned`, `wl_columnar_eval_plan_numeric_source_attempt_begin`, `wl_columnar_eval_tdd_plan_prepare_inputs`, `wl_columnar_eval_tdd_plan_prepare_insert`, `wl_columnar_ops_reduce_attempt_start_owned`, `wl_columnar_merge_concat_attempt_start_owned`, `wl_columnar_eval_plan_numeric_composed_attempt_begin_internal`, `wl_columnar_eval_serial_framed_relation`, `wl_columnar_join_spill_eligible`, `wl_columnar_session_cold_mode` | PRESERVE |
| `plain_step_completion_pending` | `col_eval_stratum_tdd_nonrecursive`, `col_session_step_impl`, `col_worker_session_create` | `col_session_insert`, `col_session_insert_incremental`, `col_session_make_compound`, `col_session_remove`, `col_session_remove_incremental`, `col_session_snapshot_impl`, `col_session_step_impl`, `wl_columnar_eval_resume_nonrecursive_completion`, `wl_columnar_session_seal_join_input` | PRESERVE -- with `plain_step_completion_phase` this is the resume token |
| `plain_step_completion_phase` | `col_eval_stratum_tdd_nonrecursive`, `col_session_step_impl`, `col_worker_session_create`, `wl_columnar_eval_resume_nonrecursive_completion` | `col_session_step_impl`, `wl_columnar_eval_resume_nonrecursive_completion` | PRESERVE |
| `plain_step_completion_step_context` | `col_session_snapshot_impl`, `col_session_step_impl`, `col_worker_session_create` | `col_eval_stratum_tdd_nonrecursive`, `col_session_step_impl` | PRESERVE |
| `plain_step_completion_active` | `col_eval_stratum_tdd_nonrecursive`, `col_session_step_impl`, `col_worker_session_create`, `wl_columnar_eval_resume_nonrecursive_completion` | `col_session_snapshot_impl`, `col_session_step_impl` | **COMMIT** on the step path -- a re-entrancy latch, not progress. While it stays set beside `plain_step_completion_pending`, the entry guard of both `col_session_step_impl` and `col_session_snapshot_impl` returns `EBUSY` above every line that would clear it. The snapshot path never writes it, so the snapshot cutoff has nothing to perform here. |
| `col_rel_t::base_nrows` | `col_controlled_step_recursive_drive`, `col_rel_compact_impl`, `col_rel_deep_copy`, `col_rel_deep_copy_governed_impl`, `col_rel_install_shared_view_unprotected`, `col_rel_reset_rows_locked`, `col_session_remove`, `col_session_remove_incremental`, `col_session_snapshot_impl`, `col_session_step_impl`, `col_stratum_step_retraction_nonrecursive`, `session_private_rel_truncate`, `session_seed_shadow_apply_remove`, `tdd_empty_relation_candidate`, `tdd_seed_bdx_coordinator_idb`, `wl_columnar_eval_serial_canonicalize_aggregate_locked`, `wl_columnar_merge_incremental_delta_attempt_advance`, `wl_columnar_relation_new_empty_governed_like_locked`, `wl_retraction_restore`, `col_controlled_step_staging_finish`, `wl_columnar_session_observed_action` | `col_rel_compact_impl`, `col_rel_deep_copy`, `col_rel_deep_copy_governed_impl`, `col_rel_install_shared_view_unprotected`, `col_rel_mutable_image_validate`, `col_session_remove`, `col_session_remove_incremental`, `col_stratum_step_retraction_nonrecursive`, `session_private_rel_truncate`, `session_snapshot_insert`, `session_seed_shadow_apply_remove`, `session_seed_shadow_plan_remove`, `wl_columnar_eval_delta_observer_begin`, `wl_retraction_stage_prepare`, `wl_columnar_eval_delta_build_action`, `wl_columnar_managed_candidate_input_shape`, `wl_columnar_merge_incremental_delta_attempt_advance`, `wl_columnar_session_observed_action`, `col_session_controlled_mutation_committed` | PRESERVE -- the insert route (`session_snapshot_insert`) takes rows `[base_nrows, nrows)` of the inserted relation as the rows an incremental insert appended, so committing the bookkeeping's `base_nrows = nrows` on a stopped attempt would make the retry find no inserted rows and leave the heads without them. `col_controlled_step_recursive_drive` advances this boundary only in `FINALIZE_COMMITTED`, after successful publication; a stopped attempt retains the old boundary. That is the whole verified consequence; see the note below before adding another |

Two non-field actions sit in the same region and need their own verdicts.
`col_session_reclaim_quiescent` is **COMMIT**: the step cutoff's unwind calls
it, as the stratum- and compaction-failure unwinds beside it do, and both
snapshot cutoffs do the same. It releases mat-cache pins before any ledger test
-- though it returns earlier still when the session has retained cleanup
outstanding, so it is not unconditional, and several other unwinds in both
paths do skip it. `col_session_mem_sample` is **PRESERVE**: no cutoff performs
it, and none should. The worry that deferred that verdict -- that reclaiming on
a gauge the sample would have refreshed tests a stale one -- does not survive
the mechanics. The gauges are absolute rather than accumulated, so the next
sample recomputes them; evaluation calls the sample already, from
`eval_serial.c`, `eval.c` and `eval_delta.c`, on any route where a stratum ran;
and the reclaim risks at most a missed eviction of recomputable cache entries,
or one it did not need.

What the snapshot cutoffs add to the contract above: that clause defers a stop
arriving *after publication starts*, and on the evaluating path the point of no
return is earlier than publication, because the bookkeeping commits before the
emit and cannot be rolled back. So read the clause as naming the bookkeeping
commit rather than the emit on that path; the stable fast path commits nothing,
so for it the clause means what it says. Neither cutoff is a claim about
streaming, and the gap there is narrower than "a `delta_cb` fires during the
strata" suggests: `wl_columnar_delta_events_publish` has one caller, inside
`col_stratum_step_with_delta`, whose only production caller is
`col_session_step_impl` -- its other caller is a retraction helper still marked
`UNUSED`, reached only by a test hook. So the one route by which a snapshot
streams is the plain-step drain, and that returns through the step path's own
cutoff. This describes the historical whole-session fixpoint route, whose
controlled continuation remains unsupported. The private numeric/composed paths
and the aware framed entry described below now charge work explicitly; they do
not yet provide a controlled whole-session fixpoint continuation.

A `retraction_seeded` left set by a stop above its commit registers nothing --
the step path's retraction pre-seed only looks relations up -- but it keeps the
snapshot fast path closed and changes forced-delta selection.

Successive drafts of the step-path comment each asserted a further purpose for
`base_nrows` and each was falsified: that the retry route reads it, that
advancing it would describe unpublished rows, that it routes the retry, that
advancing it would silently drop rows. Some were wrong about direction or role;
the last was wrong about consequence, because `col_session_emit_snapshot`
iterates `row < r->nrows` and never mentions the field. The row above therefore
names the one consequence that was read out of the observer's own condition,
and a new claim about this field belongs in a test before it belongs here.


### Numeric MAP result schemas

A MAP whose every result lane is a numeric compiled expression or a physical
projection installs its schema before producing rows, including empty inputs.
Plain variables and projections preserve the selected physical type; missing
metadata or out-of-range zero-fill is INT64. Explicit FLOAT loads/constants
and FLOAT arithmetic produce FLOAT, comparisons and BOOL literals produce
BOOL, and integer arithmetic produces INT64. The cached producer follows the
existing permissive expression stack rules: ignored full-stack pushes and
aggregate no-ops do not replace the actual result producer.

Well-formed compositions of existing fixed-result scalar builtins also have
static result types, including string literals, conversions, string functions,
comparisons and numeric digest/UUID functions. UUID5_RFC produces STRING;
numeric UUID functions produce INT64. Classification checks the complete buffer,
operand depth and final producer without evaluation, interning, crypto or
callbacks. It preserves direct STRING lanes beside interpreted builtin results
through ordinary JOIN/SEMIJOIN, including empty MAP inputs. Classification does
not validate runtime values or guarantee successful evaluation.
Opaque extension expressions retain the legacy whole-output
untyped schema when any lane is unresolved. This includes mixed numeric/addon
results; they do not acquire a fabricated partial schema. Numeric schema
resolution invokes no callbacks. Existing extension invocation/admission rules
remain in force, and controlled MAP still refuses unsupported expression
families instead of falling back to raw evaluation.

Following CONSOLIDATE uses actual FLOAT metadata for numeric ordering and
zero equality. Typed MAP append rejects a nonfinite copied FLOAT value with
`EINVAL`; a nonfinite FLOAT load or arithmetic expression failure gives MAP
`ERANGE`. Omitting the invalid source lane does not introduce a whole-source
finite scan. FILTER continues to reject a row on expression failure, rather
than returning MAP's expression error. These rules cover the documented
private scalar plans, not public/default activation or the remaining #1821
operator families.


### Generated integer MAP host boundary

The generated integer MAP `output(x+1) :- input(x)` is exercised through the
aware framed host with 4096 input rows. A checkpoint after expression
materialization identifies the 1024th row boundary. The allowance immediately
below that boundary and the boundary itself stop without publishing output; a
fresh generation on the same operation completes and agrees with raw execution
and the literal result. This fixture describes only the generated integer MAP
host path and does not define allowances for other expressions or routes.

### Controlled integer CRC expressions

The aware framed generated MAP and FILTER routes accept `crc32_ethernet(x)`
and `crc32_castagnoli(x)` when `x` is an integer value. Each operation digests
the native eight-byte `int64` representation, matching raw evaluation; the
controlled path uses a fixed eight-byte CRC loop and checks the original
attempt between bytes. A stopped attempt publishes no partial result and a
fresh generation reproduces the raw result. CRC calls over symbols, other
digest functions, and other evaluator routes remain outside this support.

### Private scalar SEMIJOIN and ANTIJOIN

The bounded existence-join component consults the full named right relation.
Duplicate right matches do not multiply left rows. SEMIJOIN emits a left row
once on its first match; ANTIJOIN emits it once only when no candidate matches.
Zero keys mean any right row is a match. An existing empty right therefore
keeps no SEMI rows and all ANTI rows. A missing registry relation is ENOTSUP in
this initial controlled component; raw's missing-right unchanged repush is not
reinterpreted as empty data.

SEMI projects only left columns, defaults project_count0 to all left columns,
and produces canonical colN names with selected physical types/defaultINT64.
An out-of-range projection is zero/INT64. Nullary output has no native type
lanes. ANTI ignores projection fields and preserves complete left schema,
including names, optional type metadata and graph/declaration metadata. Both
right-present results are non-delta, without segment metadata, and ignore
ordinary JOIN delta selection and output-limit/peak policy.

Controlled materialization and raw keep-all shortcuts agree on valid admitted
logical results; physical identity, relation name, retained sort cache and
exact allocation thresholds may differ. Invalid manually corrupted FLOAT
payloads are outside that equivalence: typed copying still rejects them.
The component remains private and opt-in. Both source and prebound numeric
plans admit these opcodes and invoke their original owned entry directly.

### Controlled ordinary JOIN right predicates

The prepared ordinary JOIN producer exposes a private controlled step/peek/ack
surface for bounded producer work. Acknowledging a ready batch advances only
producer state; it does not publish rows to a caller or sink. Ordinary JOIN
dispatch and end-to-end result publication are follow-on work. This producer
slice admits one native INT64 equality key and at most 64 output lanes. Hashing
reads only that fixed eight-byte key; each chain candidate comparison and
chain advance is a separate charged action. `cursor_get` reports only the
last acknowledged producer position; peeked rows and speculative traversal
do not advance it. The opt-in runner requires an already-built current right
arrangement; a missing or stale arrangement is rejected without construction
or rebuild. Start synchronously acquires source-reader leases and admits the
fixed-capacity private result, so only `advance` is covered by its work
allowance. An advance with zero allowance is a true no-op. If the allowance is
too small for its next indivisible action, it returns resumable progress with
`required_work` and changes no traversal or output state. Each accepted action
is charged before execution and cannot exceed the call's allowance. The runner
copies complete lanes and timestamp records under the same attempt, then
validates the pending generation before publishing the private row count and
acknowledging exactly once. A final saved-attempt and input-generation poll
immediately before transfer is the completion linearization point. Only DONE
transfers the relation; insufficient fixed output capacity is sticky `ENOSPC`.
Ordinary plan dispatch and STEP remain unchanged, and arrangement construction
and other JOIN families remain follow-on work.

An ordinary controlled JOIN evaluates a supported numeric `right_filter_expr`
over each present right row before key construction and output work. A failed
numeric expression rejects that row, matching FILTER semantics. Missing and
forced-empty right inputs skip the predicate. SEMIJOIN and ANTIJOIN retain
their separate contract and reject right predicates. Unsupported string or
extension expressions return `ENOTSUP` without invoking raw callbacks.

Selection retains original physical right-row indices, so keyed hash chains,
cross joins, duplicates, projection, and output order use the same source rows
as raw JOIN. For each passing row, all typed FLOAT lanes must be finite,
including lanes unused by the predicate, keys, or projection; otherwise JOIN
returns `EINVAL` before evaluating a later row. Rejected rows do not undergo
that payload check. Predicate truth is computed from the original stored bits;
the existing output path canonicalizes signed zero as before.
Complete generated SIP/negation bodies and composed MAP/probe chains retain
terminal CONSOLIDATE semantics, including sorted distinct FLOAT output.
Public/default/session scheduling remains separate from this interface.

### Controlled STEP materializing batch fallback (#1821)

With non-strict `WIRELOG_JOIN_BATCH_BYTES`, controlled initial STEP admits one
exact `VARIABLE → JOIN → FILTER → MAP → CONSOLIDATE` relation when charged
preflight proves that both AUTO inputs select their full relations. The plan's
delta modes remain unchanged. This route materializes the complete private
JOIN result before FILTER and MAP run, then publishes the final candidate at
the framed registry cutoff. The batch setting authorizes this compatibility
route; it does not bound its intermediate memory. Strict mode and other
batch-configured STEP shapes remain unsupported. Its published counter counts
only candidates that cross the registry cutoff.

For non-batch controlled STEP, the initial plan and registry proof is also
resumable. A call charges at most 64 plan-proof and scheduler actions combined.
Each action advances a bounded cursor, processing at most one plan descriptor,
registry slot, relation-name byte, or empty-stratum skip. When the bound is
reached, STEP returns `EAGAIN` with the same attempt and cursors, and continues
on the next call. Name matching advances byte by byte, so a long shared prefix
cannot hide an unbounded string comparison. The proof checks
supported nonrecursive plan operators and confirms that registry heads match
the already committed relation prefix before evaluation starts. A terminal
stop discards the proof; a fresh attempt starts it again while preserving
already committed output. The established batch proof remains separate.

This enables the bounded preflight for supported serial nonrecursive STEP
plans. Recursive, delta-observer, differential, multiworker/TDD, strict-batch,
and unsupported operator routes remain fail-closed. Controlled SNAPSHOT is
still unsupported.


### COUNT operand and result domains

COUNT returns an INT64 cardinality for each nonempty input group, including
when the ignored operand is FLOAT or STRING. Its plan keeps the actual operand
domain separately; the COUNT result domain is always SCALAR. FLOAT payloads
do not initialize a count, and nonfinite ignored operand values or an ignored
numeric division-by-zero expression do not affect cardinality. Selected FLOAT
group keys still undergo normal typed validation. Empty input yields no groups,
including an ungrouped COUNT; no synthetic zero row is introduced.

Raw REDUCE retains its existing expression preparation and memory admission
order even for COUNT: unused expression compilation may still fail for resource
reasons. This contract does not promise unchanged allocation peaks or a bypass
of source, output and scratch admission. Other aggregate functions retain their
existing numeric operand and result domains.

### Private initial numeric REDUCE

The opt-in REDUCE child accepts materialized scalar input in an initial serial,
nonrecursive attempt. COUNT ignores its operand and returns INT64; numeric
SUM/MIN/MAX support integer and FLOAT operands, and average requires FLOAT.
STRING ordering and interpreter-only aggregate expressions are excluded. A NULL
control delegates to the raw reducer. The private numeric plan driver admits this opcode in both source-bound
and prebound segments, using the original numeric record for child authority.

A protected source reader remains held while bounded hash slots and dense group
records accumulate values in first-seen group order. Each input row evaluates
its operand before grouping; a newly encountered group receives head-ordered
native FLOAT validation before insertion and before the next row. Thus an early
bad FLOAT key wins over a later arithmetic error, while a same-row operand
error wins over key validation. Existing-group arithmetic uses the raw ordered
checked integer or finite FLOAT updates. Canonical signed-zero keys coalesce;
no extra BOOL or INT32 value restrictions are introduced.

Only after the scan succeeds are complete typed tuples appended to the private
output. The existing original unary decision publishes the complete result.
Before that decision, cancellation, work-limit, allocation or quota failure
keeps the exact original stack entry. Empty input produces an empty relation,
including global aggregates. Output metadata follows raw head placement and
canonical colN names, without graph/timestamp/delta metadata. Out-of-range key
indices retain raw column0 fallback with INT64 output metadata. The unusual
manual FLOAT descriptor with no operand column retains literal bits1 fallback.

This private accumulator has different allocation peaks and work fees from
raw incremental output updates. COUNT skips irrelevant expression compilation;
raw COUNT can still fail during unused expression compilation. Only admitted
semantic results are compared; resource failure precedence is not promised.

Complete private numeric aggregate plans execute the original preserved body,
from initial VARIABLE through numeric operators, REDUCE and terminal
CONSOLIDATE. Both opcode preflights admit REDUCE; its own domain checks still
exclude STRING ordering, interpreter-only expressions and recursive/weighted
configurations. A failure preserves the immediately preceding original stack
entry and stops later operators. Earlier successful operators are not undone;
a canonical full-plan retry uses the stable original registry and a new record
after real cleanup and numeric ACK. This does not activate public/default
session scheduling or complete issue #1821.

### Generated COUNT REDUCE host boundary

The aware-host generated one-group integer `COUNT` fixture scans 4096 rows.
Phase 36 identifies entry into a source row before adding that row to the
aggregate: progress 1023 marks the 1024th row. A high-limit attempt completes;
the minimum allowance that reaches that checkpoint and a repeated run both
stop with `WORK_LIMIT` before publication. A fresh generation on the same
operation then publishes one result, `4096`, and matches raw execution. This
is fixture-specific evidence, not a general REDUCE work tariff or proof for
grouped aggregates, other functions, FLOAT, or other evaluator routes.

The generated grouped `COUNT` fixture also scans 4096 two-column rows across
four recurring keys. Its phase 36 checkpoint is before the row's grouping and
count update. The high-limit run checks each key's count, while the minimum
allowance reaching progress 1023 and its repeat stop with `WORK_LIMIT` before
publication; a fresh generation then retries and matches both the raw evaluator
and the literal per-key counts. This covers grouped key creation and updates
only in this generated framed route. It says nothing about metering the separate
legacy result-to-head `append_all()` copy.

Generated grouped integer `SUM`, `MIN`, and `MAX` fixtures each scan 4096
two-column rows across four recurring keys with varying signed values. The
fixtures observe phase 47 after an existing group is processed and its
aggregate slot is assigned. The minimum allowance that reaches progress 1023
and a repeated run both stop with `WORK_LIMIT` before publication. Fresh
generations retry and match the literal per-key values and the raw evaluator.
This is bounded evidence for these generated framed routes; it does not
establish overflow, FLOAT, `AVG`, legacy
`append_all()` copying, or other evaluator coverage.

Generated grouped finite FLOAT `SUM`, `MIN`, `MAX`, and `AVG` fixtures also
scan 4096 two-column rows across four recurring keys. The per-key input order
starts between the extrema, then visits a new maximum and minimum, and uses
exact binary fractions so literal sums, extrema, and averages are stable. The
fixtures assert FLOAT operand/result descriptors, phase 47 after existing-group
processing and aggregate-slot assignment, and the same minimum-allowance,
repeat, no-publication, and fresh-generation retry properties above. This
covers these finite generated framed routes; it does not establish behavior for
nonfinite values, overflow, non-dyadic rounding, other evaluator paths, or
legacy `append_all()` copying.

### Original two-entry replacement

The private binary transaction snapshots the complete top two relation entries,
including each input's segment-array ownership, while keeping lower entries
unchanged. Its candidate is a flat owned, non-delta relation. Commit compares
both original entries and checks candidate collisions before the last saved-ID
poll; the subsequent fixed publication moves both originals into independent
retired slots and replaces the pair with one result. Failure before that cutoff
preserves both originals. Each retired input is disposed in its own host action;
a reader may retain either input after successful publication without changing
the committed output. Abort similarly disposes only the private candidate.

A borrowed relation still owns its segment array. Duplicate non-NULL segment
pointers between either input and another stack entry are rejected independently
of relation storage aliases. Two borrowed references to the same source with
independent segment arrays remain valid. Overlapping source storage involving an
owned stack entry is excluded. No segment-array contents need traversal for this
pointer-ownership check.

The sibling numeric stack-begin claims an exact caller-supplied nonempty depth,
validates every entry with paid steps and binds the real original plan record.
It adds ownership authority, not CONCAT opcode or UNION grammar admission.
The existing top-one and initially-empty begin APIs retain their contracts.

### Private controlled scalar CONCAT

The private `wl_columnar_merge_concat_attempt_*` interface materializes the
left rows followed by the right rows, retaining duplicates and supporting empty
and nullary inputs. It requires two ordinary scalar entries of equal width,
without timestamps, in the initial serial domain. Non-NULL operation control
never falls back to raw execution; the raw single-entry K-copy marker remains
outside this controlled domain. Output is owned, flat and non-delta, with no
K-way optimization segment array. Input segment arrays remain independent
transaction owners. Terminal CONSOLIDATE supplies UNION set semantics when a
new composed driver is selected; the legacy numeric segment entrypoints still reject UNION plans.

Names, graph metadata and declared width come from the left. Nonempty-right
FLOAT compatibility uses the initial left schema (untyped means INT64); integer
physical families remain permissive. Empty right skips compatibility and type
adoption. An untyped left adopts nonempty right native metadata. FLOAT values
are validated before publication and signed zero is canonicalized. The supported
semantic error sequence follows left validation then right compatibility and
validation. Lease preparation, allocation/admission and peak-memory error
precedence can differ from the raw pool path. Both inputs remain protected
until construction and copying finish. Stops preserve both original entries;
committed output survives independent reader-retained retirement of either input.

### Private complete scalar UNION bodies

`wl_columnar_eval_plan_numeric_composed_attempt_begin` admits a complete,
immutable initial nonrecursive scalar body. Initial depth zero uses a genuine
empty-frame claim; depths1..32 use the genuine expected-depth stack claim.
Before any lookup or publication, paid abstract-depth preflight visits every
opcode: VARIABLE adds one, CONCAT requires two and removes one, supported
numeric children require one and preserve depth, and final depth must be one.
Prebound inputs are checked separately for the scalar/noTS domain. Registry,
plan and borrowed registry storage remain immutable and live through ACK.
Prebound owned entries retire only through their controlled child transactions.

Each VARIABLE independently resolves/holds its source and pushes above the
current lower entries. Private empty outputs belong to the original frame
result until the saved-ID-checked, callback-free push cutoff. Lower entry
fields never change during a push. A failed later source drains its lease and
then retires its own candidate on a separately funded action before DONE/ACK;
its failure leaves preceding owned entries reachable for ordinary frame cleanup.
CONCAT has dedicated child/ACK states and uses the original binary transaction.
Successful completion counts every opcode, including VARIABLE and CONCAT,
after its ownership transition; legacy source counts remain unchanged.

Distinct composed PENDING/DONE/source states exclude legacy step/consume/ACK
from this protocol. A child returns to the real host before a later fresh action
consumes its result. Each funding layer admits its own work before its own
mutation. A deeper host refusal may leave genuine parent PENDING without
promoting stale child status, completion or runtime depth. Scalar-source calls
use the original operation and retain their separate semantic/stop/drain rules.
Cancellation or saved-ID turnover stops subsequent forward work; cleanup and ACK
use independent funding and cannot debit the original control or its successor.
Default/public session dispatch, recursive K-copy CONCAT, weighted/differential,
TDD, batch/pipeline, parallel, LFTJ and fused evaluation remain outside this route.

The private fresh-head bridge starts on an empty cleanup frame and a named,
terminal CONSOLIDATE body. Its original frame-embedded parent exists before the
original embedded composed child begins. It rejects an already registered head;
this admission does not implement existing-head append or dirty reevaluation.
The child's genuine DONE result remains owned by the original stack entry while
an independent protected scalar source, named constructor, B2 writer and governed
workspace copy its ordered scalar rows. Sorted, unique, flat, untimestamped
results preserve schema, type, graph and declared-width metadata. Empty results
have zero cached runs. Only the completed candidate reaches READY; this private
bridge alone does not publish it to the session registry.

On failure, bounded B2/source/workspace/candidate retirement retains the original
parent and cleanup frame. One original stack/result slot is disposed per action;
a reader refusal preserves that slot and its ownership. Inert placeholders keep
the actual child runtime depth until genuine child ACK. A bounded spare-slot
proof then permits top-zero normalization. After all recoverable owners drain,
parent ACK and known-empty frame close form one final mandatory action. The
existing physical-free/credit/reference invariant remains in force. Generic
cleanup cannot close a live parent. This publication protocol remains limited
to its validated one-head shape.

The private framed result can be published as one absent head. A separately
prepared registry fills the first hole or appends and builds exact FNV1a hash
chains without whole-image copies or string scans in one action. The final
saved-original-ID checkpoint precedes a fixed callback-free pointer cutoff.
Commit is sticky: later cleanup resumes without rerunning the body or testing
head absence again. Before cutoff every completed credit move is reversed on
failure and the old registry remains authoritative. Publication currently
supports canonical EMPTY/positive RESERVED old tokens, and genuinely noGov
all-zero tokens without writes; a RELEASED old-token history is unsupported.
This internal publication protocol does not enable unsupported controlled
routes or existing-head append/reevaluation; bounded default STEP preflight is
described in the #1821 section above.

### Private aware-host relation entry

`wl_columnar_eval_serial_framed_relation` keeps its synchronous raw behavior when
both the attached control and host request are NULL. An attached control requires
an explicit internal host request. Each such call runs one funded action and
returns to the real host; it never drains progress in an engine loop. The host
uses the actual immutable generated relation plan, begins the original operation
once with `base.operation_admission`, and retains session/input/native ownership
until terminal close. The original operation remains active across progress.

The integer return is protocol status. Only `TERMINAL` makes `execution_result`
and `committed` valid, after actual frame close. `MORE` may mean retained cleanup;
`STATUS` is separately funded and preserves the outstanding non-status receipt.
The host completes each fresh grant with `finish(grant, owner, 0, 0)` after the
entry returns. It keeps a completed borrowed grant and its exact generation until
successful consume, including when a newer consume or status action is refused.
A failed consume does not replace that earlier receipt. A host needs at least two
stable grants and selects one that is not borrowed, rather than blindly rotating
when status calls intervene. Semantic success never follows from protocol zero
alone. The final close writes external outputs before freeing the frame.

Fresh bootstrap prepays an independent first known-empty-frame close before
frame acquisition. The controlled allocator admits and zeroes the full actual
frame. An original saved-generation zero checkpoint separates its final return
from parent begin. Failure before parent activation closes only the frame that
this same call just created. It returns `BOOTSTRAP_RECEIPT`, whose completed grant
is consumed on a later host call: host refusal permits retry; a successful host
report makes a nonzero saved protocol terminal. Without a durable parent this
report proves its own grant generation and disposition only. Exact association
with the same plan/session/original generation/native host, and absence of
synthesis or replay, are trusted serialized caller requirements.

An authentic existing parent resumes before any fresh lookup. Original attempt
turnover authorizes cleanup of that old parent without charging or finishing the
successor. READY uses the private reversible registry publisher. Once its fixed
cutoff transfers the candidate, later progress retires old arrays and credits
after genuine child ACK, without rerunning the body or undoing the committed
head. An original cancellation merely requested after cutoff stays sticky for
the next attempt; `finish(0, 0)` does not poll it.

Governed generated-body coverage also exercises ordinary JOINs whose lowered descriptor
contains a nonempty numeric `right_filter_expr`. The actual generated relation
body runs through terminal CONSOLIDATE and publication. Tests cover a 64-by-257
filtered product and a keyed empty result that visits 16,448 colliding unequal
candidates. A keyed stop tests the source-derived 57,472-unit local boundary
from the final right-hash insertion through candidate 128: one unit less admits
only 127 candidate comparisons, while the exact amount reaches candidate 128
before deterministic cancellation.
Separate cancellation cases stop during product pairs and keyed comparisons;
each stopped generation is reported and retried with a fresh generation. HOST
grants remain distinct from original
operation work. These tests establish this initial generated route; they do not
extend support to SEMI/ANTI right predicates, whole-session STEP/SNAPSHOT,
recursive or differential evaluation, or public/default activation.

For the generated 64-by-257 product, the test also records an empirical
original-work allowance by cancelling at the 128th pair checkpoint. One unit
less reaches only 127 pair callbacks and returns work exhaustion; the measured
allowance reaches callback 128 and returns the deliberately requested
cancellation. Output cells and B2 append work contribute between pair
checkpoints, so this fixture measurement is not a general source tariff.

Supported serial nonrecursive STEP now uses the bounded default proof described
above. The single-relation INT64 path plans described above are the only
recursive routes admitted; other recursive plans and unsupported whole-session
routes are rejected before evaluation. Controlled SNAPSHOT can deliver an
already completed STEP; evaluating fresh or dirty state remains unsupported.
TDD/multiworker, general differential, and unrestricted observer composition
remain outside this slice. The restricted unary observed routes are described below.
Raw references and coverage for those routes remain, with this split:

| Former whole controlled expectation | Replacement or remaining scope |
|---|---|
| STEP admission owner and same-attempt pre-cancellation | Bounded default proof and real aware-entry retry/cancellation coverage; unsupported recursive and observer routes remain fail-closed |
| STEP observer cutoff and retry | Restricted unary identity/optional-FILTER initial and successor delivery; general observer composition remains deferred |
| Snapshot evaluating/stable cutoffs preserve `base_nrows`, stable flags, pin/reclaim unwind and cancellation precedence | Raw evaluating/stable references and direct lower quiescent/reclaimer checks; whole controlled snapshot bookkeeping/precedence remains deferred |

This adds bounded preflight for supported serial nonrecursive controlled STEP
plans. It does not complete recursive/differential/weighted execution,
TDD/parallel, remaining LFTJ fallback paths, K-fusion, controlled SNAPSHOT, or
full issue #1821.

### Resumable observer baseline construction (internal opt-in)

`wl_columnar_eval_delta_observer_build_start`, `_build_advance` and
`_build_discard` construct the existing delta observer's baseline independently
of whole controlled STEP. Supported rule heads must already exist and have
one flat INT64 column (implicit INT64 is accepted), no timestamps, and initialized
canonical self-owned storage. All heads, seed shadows and retired shadows are
preflighted before any reader is acquired. Unsupported schema returns ENOTSUP;
there is no raw fallback. The host must keep the plan, registry and relation
contents stable throughout construction and serialize all calls. Plan/name
pointers are checked for replacement across resumable phases; the host must
also keep the plan structure and name-string contents immutable in place.

Advance accepts a live control and its current nonzero attempt ID. One charged
unit advances one persistent cursor, allocation, name byte, captured cell,
heapsort comparison/swap, merge/dedup action or reader transition. A budget stop
retains the cursor and any original reader authority; a new attempt can resume.
EAGAIN denotes progress or a contended CAS. Positive construction failures
require discard. Only the READY transition assigns `session.delta_observer`.
The baseline preserves the raw committed-model rule: head plus retired rows,
minus pending seed inserts with multiplicity, then sorted unique values.
Native-byte-order heapsort is resumable with O(n log n) comparisons and swaps;
each advance performs at most one bounded comparison, swap, or cursor/state
transition. This API does not promise raw throughput or a wall-clock bound.
Fixed constructor bookkeeping is separately allocated; baseline metadata and
payload retain existing governor admission.

Discard advances mandatory cleanup one unit per call. The host repeats EAGAIN
under external serialization. Invalid or corrupted authority remains reachable
and returns an error; it is not cleared to manufacture successful cleanup.
Session destruction drains this continuation before relation storage and aborts
if it cannot finish. Worker cloning refuses a live constructor at either end.
This baseline constructor is an internal preparation primitive; by itself it
does not claim complete #1821 coverage or enable unrestricted public dispatch.

### Attempt-controlled integer HASH in generated MAP/FILTER (#1821 increment)

The internal attempt-controlled scalar path admits the generated unary
`hash(int64)` opcode through framed relation MAP and FILTER. It hashes exactly
the operand's native eight-byte `int64_t` representation with xxHash 0.8.4
XXH3-64 and retains the established unsigned-hash-to-int64 lane behavior.
The fixed eight-byte XXH3 implementation path has a 192-unit logical source
tariff, charged before popping or reading the operand. A guarded poll follows
the noninterruptible library call before the value is pushed. The tariff covers
the fixed length-eight path and load/mix/avalanche operations; it is not a
wall-clock or CPU-time guarantee. `hash(symbol)` / `HASH_S`, other digest
opcodes, and variable-length operands remain unsupported by this attempt path.
This narrow operator increment does not enable public STEP or complete #1821.

### Resumable signed-delta observer preparation (internal opt-in)

`wl_columnar_eval_delta_observer_prepare_start`, `_prepare_advance` and
`_prepare_discard` extend the internal continuation through current-head
capture, normalization and signed event construction. The session must already
own a READY baseline observer from the builder above. This initial stepped path
supports unary INT64 heads; the existing synchronous prepare remains available
for other schemas and retains its prior semantics. The host keeps the plan,
registry and source contents stable and serializes all calls across attempts.
Preflight and capture use separate reader leases, each released before the next
phase.

Each advance performs one bounded name-lookup byte, reader transition, cell
copy, heapsort comparison/swap, deduplication action, merge action or phase
transition. A work-limit stop retains progress and reader authority for a fresh
attempt ID. Cancellation leaves the record owned and requires discard. Events
remain private until READY marks the completed buffer publishable; publication
is a no-op before that point. STEP, raw prepare, a second observer begin and
worker cloning refuse while preparation is live. Discard drains partial reader
acquisition and staging; teardown retries EAGAIN before freeing the observer.
Positive terminal errors are sticky: later advances return the same error until
discard drains the continuation. Only EAGAIN and WORK_LIMIT retain a resumable
cursor; cancellation is sticky and requires discard.
If a work-limit stop lands after staging credit commits, that completion state
is retained and a fresh attempt completes READY without committing twice.
This is still a preparation primitive, not whole-plan observer delivery or
callback scheduling, and does not complete #1821 acceptance coverage.

The controlled observed STEP host also admits a narrow successor after its
initial STEP has completed: exactly one nonzero row insert or remove on the
unary INT64 input of the supported one-rule `VARIABLE -> MAP(identity) ->
CONSOLIDATE` or `VARIABLE -> FILTER -> MAP(identity) -> CONSOLIDATE` plan.
The optional FILTER uses the existing charged numeric predicate path: rejected
rows consume work and observe cancellation even when no output is produced. Unsupported mutation shapes fail before backend mutation;
zero-row calls do not create successor work. A supported mutation makes
snapshots busy until the next controlled STEP. That STEP recomputes into a
private relation, compares it with the last committed output, and emits the
net signed `+1`/`-1` events. The prior public output remains registered through
work limits, cancellation and preparation errors. The candidate replaces it
only after event preparation and the final control poll succeed. A stop before
that boundary can be resumed without repeating the framed rule; post-commit
cleanup cannot publish the events twice. This remains a controlled internal
route: it does not enable public/default activation, multirow updates, other
plan shapes, weighted rows, JOIN, recursion or the remaining #1821 acceptance
criteria.
This host also bounds that shape to three registered relation slots (input,
committed output and one private candidate); sessions exceeding that bound
reject successor mutation before it enters the backend. Removal can retain
`$r$input` staging and exhaust that bound after a successful successor; the
next mutation then returns `ENOTSUP` without changing input or output. Repeated
mutation cycles need bounded staging retirement/reuse coordinated with #1820;
this support does not establish indefinitely reusable successor sequences.

Other observer plans remain disabled. The restricted recursive reachability
route and controlled initial single-rule STEP entrypoint are documented above. For that route, the committed unary INT64
output is verified as sorted and unique under a reader lease, then streamed in
signed INT64 ascending order after all callback work is precharged and the
final cancellation poll succeeds. Observer delivery does not allocate
row-sized current or event arrays and has no separate source-row limit. The
reader lease and cursors survive work-limit stops; cancellation and destruction
before publication release the lease without callbacks, while cleanup after
publication cannot replay callbacks. Framed consolidation still materializes
and charges its own sorting scratch. This increment does not claim complete
#1821 coverage or enable unrestricted public dispatch.

### Managed binary candidate union and novel delta (#1821 increment)

The internal managed candidate merge accepts unnamed, private binary INT64
relations without schema, ledger, or delta-baseline metadata. It requires a canonical sorted source; the candidate may
arrive unsorted and contain duplicates. The result is the numerically sorted,
distinct union of both inputs, and the delta contains exactly the union rows not
present in the original source. Neither input changes. The replacement and
delta are transferred together exactly once; until then both remain private.

The operation is resumable under one shared attempt allowance and a maximum of
64 charged comparison, copy, builder, or state actions per advance. A terminal
stop before transfer retains every partial allocation and gate until charged
maintenance cleanup completes. This is a restricted internal preparation
primitive; it does not activate unsupported evaluator routes or satisfy all of
issue #1821.

### Controlled consolidation prefix discovery (#1821)

When `diff_enabled` is set, the ordinary controlled CONSOLIDATE child discovers
its strictly increasing prefix under the existing scalar source lease. It reads
actual values rather than trusting `sorted_nrows`; this also handles order-changing
MAP projections. Nonfinite FLOAT values are rejected before discovery. Finite
FLOAT comparison, including equal signed zeros, uses the same stable ordering
as the full-sort path. The first equal or descending pair ends discovery.

Only suffix indices enter merge sort. A separate stable merge uses implicit
prefix row indices, so alternating suffix scratch arrays never expose uninitialized
prefix slots. Existing deduplication and private output publication follow.
No source metadata is modified and no new memory owner is introduced.

Each discovery pair pays 128 for traversal, 128 for comparator entry, 128 per
inspected column, and 128 for its decision. Merge pays 128 for each traversal,
comparison fees when both runs remain, and 128 for index emission. Existing
checkpoint tails preserve the 256-work quantum and framed continuation; sorting,
output construction and cleanup retain their existing fees and owners. The
one-column discovery boundary test derives 512 per pair after an empirical
setup anchor. Whole-operation exact/one-short tests use empirical allowances.
The optimization does not promise lower total work for every input.

Disabling `diff_enabled` retains full-sort behavior and tariffs. This algorithm
is not active differential dispatch, persistent prefix reuse, a persistent JOIN
arrangement, trace publication or incremental retraction scheduling. Those
#1821 coverage obligations remain open.

### Controlled persistent ordinary JOIN indexes (#1821, G3b)

The general serial STEP host can share persistent native differential hash
arrangements across ordinary keyed JOIN children. Eligibility requires the
selected full right input to be a public EDB with contiguous INT64 storage,
without a right filter, materialization, spill/batch policy or recursive head.
Other supported inputs retain the existing controlled JOIN path. This uses
native one-based chains but does not enable the raw differential dispatcher,
signed frontier scheduling, or incremental retraction propagation.

An unchanged source snapshot reuses its index; changed snapshots build a private
replacement. Distinct key vectors use distinct entries and repeated requests
share one index. All requested indexes and result heads publish at the same
host cutoff. Existing raw pins or pending/deferred registry transactions refuse
this image replacement; unrelated unpinned entries survive unchanged.

A pre-cutoff work limit after index preparation begins aborts and drains the
private host transaction. Reset/retry recomputes it under a fresh attempt;
consumed work is not refunded. Cancellation and errors likewise leave public
heads and indexes unchanged. After cutoff, retirement and callback delivery
complete without replay or rollback.

### Controlled acyclic append successors

The general observed STEP host can evaluate a proven append to one EDB using
only its appended interval. Eligibility is decided for the whole host before
execution: all heads are positive, nonrecursive INT64 compositions reading EDBs
directly, with at most one changed-input read along each evaluated branch.
A charged stack-degree proof adds JOIN inputs and takes the maximum over UNION
alternatives, so separate linear branches may each read the appended suffix.
Pure projections, filters and unions are supported. Unchanged branches
may recompute duplicates. Nonlinear repeated changed reads, transitive IDB dependencies,
nonmonotone operations, mixed input changes and removals retain controlled full
recomputation.

An authenticated exact operator occurrence selects the private delta. JOIN
executes delta-left/full-right or full-left/delta-right; full-right native
arrangements may be reused, while delta-right indexes remain ephemeral. Each
changed head becomes the metered consolidated union of its old head and new
contributions. Heads, arrangements and committed input boundaries advance at
one cutoff. The implementation does not activate raw synchronous differential
operators or claim recursive frontier, weighted trace or retraction support.

### Controlled recursive row frontiers

The general controlled STEP host evaluates qualified positive, nonaggregate,
positive-arity INT64 SCCs with attempt-local row frontiers. A charged audit of
all preserved head bodies requires at most one same-SCC VARIABLE or JOIN-right
read per evaluated branch: VARIABLE pushes degree zero/one, JOIN adds its right
source degree, and CONCAT takes the maximum of its alternatives. Multiple linear
UNION branches are admitted; a UNION followed by another recursive JOIN remains
nonlinear. Other sources remain full immutable inputs. A
referenced head's frontier is selected by the authenticated exact operator
occurrence, including empty frontiers; the executing head does not substitute
its own frontier. Round zero borrows the referenced head's empty bottom.

Every qualified child computes a terminal controlled consolidation of its body
contribution and prior accumulated head. After all SCC children complete and
ACK cleanup, a charged lexicographic difference of new and old accumulated sets
produces each next frontier. Ordering follows actual controlled CONSOLIDATE
output provenance, rather than sorted-prefix metadata. All differences finish
before rebinding; every old frontier retires before another child starts.
Convergence requires every next frontier to be empty.

Initial evaluation and all insert/remove successors restart from empty bottom,
then use frontier rounds within the attempt. This handles changed external
inputs joining old closure tuples without retaining a cross-attempt delta.
Genuinely nonlinear bodies use the existing full frozen-round route where
admitted. The compiler preserves ordinary operators for proven monotone,
nonaggregate linear UNION alternatives, avoiding unnecessary K expansion;
nonlinear K expansion remains unchanged. Default K_FUSION/LFTJ and representation
refusals remain in force. Seed branches can execute again in subsequent rounds;
this does not promise a universal raw performance improvement. Public heads
and observer events retain the existing common cutoff. Raw epoch/rule-frontier
arrays, weighted traces and incremental retraction are separate contracts.

### INLINE declaration provenance for source initialization

Execution plans retain owned logical declaration metadata independently of
physical child lane types. Declared flat INLINE inputs, including empty and
all-numeric inputs, receive their real logical arity map at session creation.
Host inserts still supply the declared physical scalar lanes. Extracted child
results remain scalar, rather than inheriting the original arity map merely
because physical widths coincide.

A conservative per-relation summary preserves unsupported nested, whole-value,
wrong-functor and wrong-arity body uses before optimizer/lowering information
loss. It does not change existing resident semantics or prevent source schema
initialization. Missing declaration provenance cannot authorize future compound
cold reads. Supported scalar field projections and JOINs can seal real flat
INLINE sources only after validating their initialized layout against these
declarations and the original-use summary. Numeric-only INLINE still pins the
functor intern table. Output lanes retain scalar types without the source arity
map; wrong functor/arity, nested and whole-compound uses refuse before retiring
the source.

### Controlled scalar selectors and completed recursive removal

The non-strict, iteration-zero scalar materialized batch fallback admits
VARIABLE AUTO/FORCE_FULL/FORCE_DELTA/FORCE_EMPTY/FORCE_EMPTY_AFTER_SEED and
JOIN-right AUTO/FORCE_FULL/FORCE_DELTA. AUTO uses a nonempty smaller delta;
JOIN-right AUTO keeps its full source when the left is already delta.
FORCE_DELTA uses an available nonempty delta, or the existing unseeded base-case
full source when none exists. VARIABLE FORCE_EMPTY produces an empty source;
FORCE_EMPTY_AFTER_SEED has AUTO behavior in this admitted iteration-zero domain.
These are internal execution-plan contracts, not new public selector APIs.
Strict batching, output policy, worker and foreign active-state checks remain
unchanged. The owned evaluator authenticates sources and bills selection,
materialization and JOIN work under the same attempt.

A completed, quiescent callback-free binary INT64 recursive `edge`/`path` host
can accept removal of declared edge rows when its old input/head provenance and
preserved body are valid. A successful nonzero removal switches the next
operation to bounded private full reconstruction. Failed and zero-row requests
leave the completed fast path intact; an actual foreign source reader can still
return EBUSY. The reconstructed heads and signed observer differences publish
at the existing common cutoff. Fixed private bindings use a bounded local JOIN
index; only authenticated dynamic general-transaction bindings can request
persistent index ownership.

| Removal lifecycle boundary | State disposition |
|---|---|
| Mutation admission/refusal | Validate the old completed provenance before editing inputs. Failure does not promote or replace the frozen evaluation report. |
| Successful nonzero mutation | Keep public old heads and notification baseline; the changed EDB remains pending for private reconstruction. Obsolete append-only provenance no longer authenticates a completed result. |
| Stop before cutoff | Drain/dispose private children and reversible claims; preserve old public heads and pending changed inputs. Caller reset/retry uses a new report, with no events from the failed candidate. |
| Cutoff and delivery | Publish one registry image and admitted signed event batch. Late cancellation cannot split or replay that batch; late reported work exhaustion does not roll back committed output. |
| Completion and later calls | Retire retained ownership exactly once. A no-op retry does not repeat events; later mutations re-evaluate from current inputs. Session/control ownership teardown returns governor credit to zero. |

Raw incremental removal still stages one input relation at a time; attempting
another relation's removal before evaluating retains its EBUSY contract. This
is separate from a foreign source lease. Scalar signed notifications are not
physical differential trace weights/timestamps. Their transport intersections
and remaining recursive/resource/configuration domains stay under #2293's
children. The `evaluation_differential` tests qualify the predicates above,
including deterministic source/index/probe/claim/cutoff stops and retry.
