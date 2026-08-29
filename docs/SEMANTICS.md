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
and optimization semantics. Issue #912 proposes a future row-local scalar
function addon, but provides no runtime or public API; its registry, callback,
error, and lifetime constraints are documented in
[`docs/design/scalar-function-addon.md`](design/scalar-function-addon.md).

An addon callback cannot change the engine's relational, differential, or
aggregate semantics. In particular, callback errors are not silently converted
to false predicates.

The `DETERMINISTIC` and `PURE` capability bits are addon attestations; Wirelog
does not independently prove them. Until an optimizer pass has access to the
extension snapshot that carries those declarations, optimizer passes preserve
the source order and expression structure of rules containing scalar addon
calls. This conservative boundary prevents callback duplication, elimination,
or movement based on an unverified capability declaration. A future optimizer
contract may enable specific transformations for callbacks with both bits.

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
- `tests/test_recursive_agg_kfusion.c` — the #1021 rejection and its two
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

- A CI job that runs the recursive-aggregate fixtures under
  `ENABLE_K_FUSION=0`. Every fixture a narrowing would readmit has to be
  shown to answer the same in both build configurations, and no job does that
  today.

  The tree is not starting from nothing here. `k_fusion_memory_nofusion`
  (`tests/meson.build`) is built with `c_args: ['-DENABLE_K_FUSION=0']` and
  runs in the default `meson test`, evaluating a transitive closure at K=2
  and K=4; `bench/meson.build` has a second such target. So the meson pattern
  for an unfused test binary already exists and is short to copy. What is
  missing is a target that puts *these* programs through it. (Separately,
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
