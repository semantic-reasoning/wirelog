# Changelog

All notable changes to wirelog are documented in this file.

## [Unreleased]

### Added

- **Parsed `.input` metadata query** (#1070): applications can use
  `wirelog_program_relation_has_input()` to inspect whether a relation has a
  parsed `.input` directive without opening its source or invoking an I/O
  adapter.

### Changed

### Deprecated

### Removed

### Fixed

### Performance

### Security

### Documentation

## [0.54.0] - 2026-08-11

### Added

- **DOOP fact-catalogue drift gate** (#952): the set of DOOP `.facts` files is
  written down in four places -- `download.sh`, `doop_edbs[]`,
  `doop_fact_files[]`, and the file count in `run_doop_validation.sh`. They
  drifted apart silently once (#950) and the test that should have caught it
  skipped itself instead. `scripts/ci/check-doop-catalogue.sh` now compares
  all four and fails the `abi` suite on disagreement. It parses sources only,
  so it runs on hosts that will never fetch the 740 MB archive, and it refuses
  to compare catalogues it could not parse -- a parser returning nothing would
  otherwise make every set trivially equal, which is the exact shape of
  failure it exists to catch.

### Changed

- **`wirelog_program_get_facts` reports the physical row stride** (#985): its
  `num_cols` output is the number of `int64_t` slots each returned tuple
  occupies -- which is what an embedder must index the buffer by -- and it is
  no longer necessarily equal to the `column_count` of
  `wirelog_program_get_schema`. The two agree for every relation that
  declares no `inline` compound column; where one is declared they can
  differ, as in `.decl pred(id: int64, payload: f/2 inline)`, which reports
  `column_count` 2 and `num_cols` 3. (They still agree at `inline` arity 1 --
  one declared column, one slot -- so an inline compound column is a
  necessary but not a sufficient condition.) Logical width stays
  authoritative for every declaration question (`wirelog_schema_t`,
  `wirelog_program_get_schema`); physical width is authoritative for every
  storage question.

  The signature is unchanged and there is no ABI break, but this is a
  semantic change for embedders. It affects no relation without an inline
  compound column, which is every relation this API could previously return
  facts for at all -- the fact syntax for an inline-compound relation did not
  work before this release (see the `Fixed` entry below).

- **`wirelog_io_ctx_num_cols` and `wirelog_io_ctx_col_type` are physical**
  (#985): the same width change, on the installed I/O adapter contract. Third
  parties implement `read()` against these two accessors, so the change is
  called out separately from `wirelog_program_get_facts` above.

- **A host insert must match the declared physical width** (#1038): the
  `ncols` passed to `wirelog_easy_insert` is now checked against the
  relation's `.decl` on the *first* insert as well as every later one, and a
  mismatch returns an error instead of succeeding. Both the direct and the
  incremental (delta-callback) insert paths enforce it. Removal is unchanged:
  a remove against a relation with no established width is still the no-op it
  always was, because there is nothing to remove.

  This is the third embedder-visible consequence of #985 and the one that
  was missing from the two entries above: for a relation declaring an
  `inline` compound column, the width a host must insert at moved from the
  logical count to the physical one. A host that inserted
  `.decl p(id: int64, lbl: pair/2 inline)` at 2 and matched the old fact
  path must now insert at 3.

  Previously a relation's width was whatever its first producer happened to
  supply -- the schema was set lazily from the caller's `ncols` and nothing
  compared it against the declaration, so the check that already rejected
  every *subsequent* mismatch was working from an accidental baseline. Both
  directions were silent: too narrow fabricated a slot no source ever wrote
  (`insert(p, {5, 55}, ncols=2)` derived `outr(5, 55, 0)`), and too wide
  dropped the surplus without a word, which needed no compound at all --
  a two-column `.decl` accepted a first insert of three.

  Relations with no `.decl` are unaffected and stay caller-defined, as does
  the degenerate zero-arity `.decl p()`, which carries no data for a width
  check to protect.

  `wirelog_io_ctx_num_cols(ctx)` is now the physical row stride -- the
  `int64_t` slots one tuple occupies, which is what `docs/io-adapters.md` has
  always required `read()` to size its buffer by and the width wirelog
  inserts that buffer at. It previously reported the declared column count,
  which for a relation with an `inline` compound column names a stride the
  storage does not use. `wirelog_io_ctx_col_type(ctx, i)` is indexed by the
  same physical position, and each slot of an inline compound now reports
  `WIRELOG_TYPE_INT64` rather than the compound column's declared type: the
  slots carry raw `int64` payload. For
  `.decl inp(id: int64, p: pair/2 inline, s: symbol)` an adapter is now told
  4 columns of `INT64, INT64, INT64, STRING`, against 3 of
  `INT64, <p's declared type>, STRING` before.

  `WIRELOG_IO_ABI_VERSION` is unchanged and no signature moved; only the
  values differ, and only for a relation declaring an `inline` compound
  column. An adapter that already sizes its output from `num_cols` needs no
  change -- the built-in CSV adapter did and does. One that reconstructs the
  stride from a schema of its own will now disagree with wirelog for those
  relations. See the accessor block in `wirelog/io/io_adapter.h` and
  [docs/io-adapters.md](docs/io-adapters.md).

- **Warning-clean library build** (#940): the last three `-Wunused-function`
  warnings in library code are gone. `expand_multiway_delta` is now compiled only under
  `#if !ENABLE_K_FUSION`, the configuration that actually calls it (used by
  `bench_flowlog_seq`); it was previously compiled and unused in every default
  build. `col_stratum_step_retraction_nonrecursive` is marked `UNUSED` rather
  than deleted, since it remains the reference implementation for the deferred
  Issue #158 retraction path.

### Deprecated

### Removed

- **Dead row-major SIMD kernels** (#939): the compile-time dispatchers
  `col_filter_fast`, `hash_int64_keys_fast` and `keys_match_fast` had no call
  sites on any target, so the AVX2 and NEON kernels they selected were
  unreachable. They took row-major row pointers while the filter and join hot
  paths had moved to column-native access, so they could not be reconnected as
  written.
- **Unreachable session helper** (#940): `col_session_cleanup_old_data` had no
  callers anywhere and is removed.

### Fixed

- **Magic Sets keeps unsafe consumers complete** (#1046, #1047, #1048): the
  guard-viability closure now propagates through output consumers, negated
  reads, and aggregate inputs. A guarded producer is therefore never read as
  a partial relation by an unguarded consumer, which fixes lost output rows,
  answers invented through negation, and aggregate values computed from a
  subset. These fixes are included in 0.54.0; the design history and the
  original failure shapes remain documented in the #1027 entry below.
- **Single-rule IDB snapshots are set-valued** (#957): snapshot evaluation
  consolidates single-rule IDBs before downstream joins, so duplicate
  derivations no longer multiply snapshot rows or join results.
- **0.54.0 includes the SEMIJOIN layout fix** (#974): this is the first tagged
  release intended to carry #955, closing the release-availability gap that
  previously required downstream users to pin an untagged commit.

- **A relation with an `inline` compound column has a working fact and
  `.input` syntax** (#985): such a relation has a *logical* width (its
  declared columns) and a *physical* width (inline slots expanded), and no
  component agreed which was authoritative. The fact path, the `.input`
  path, and `wirelog_program_get_facts` strided by the logical width while
  the storage was laid out physically, so:

  ```
  .decl src(a: int64, b: int64, c: int64)
  .decl pred(id: int64, payload: f/2 inline)
  src(1,10,20).  pred(7,99).  pred(x,y,z) :- src(x,y,z).
  ```

  emitted `pred(7, 99)` and `pred(1, 10)`, dropping the 20: the two-argument
  fact fixed the relation's runtime width at 2 and the rule's third column
  was truncated away by `col_rel_append_all` (`columnar/relation.c`), which
  copies `dst->ncols` columns with no clamp against `src->ncols`. In the
  other direction a body pattern that destructured the column read the
  inline slot the fact never wrote --
  `outr(id, p, q) :- pred(id, f(p, q)).` produced `outr(1, 99, 0)`, exit 0,
  AddressSanitizer silent -- the `0` coming from
  `tmp[c] = (src < e.rel->ncols) ? row[src] : 0;` in `columnar/ops.c`. An
  `.input` file was mis-read the same way: a four-field file for
  `.decl inp(id: int64, p: pair/2 inline, s: symbol)` loaded three fields
  under the wrong types and evaluated to `outr(1, 7, 1, "pair")`, four
  values of which none were the file's.

  Physical width is now authoritative for every storage question -- the
  inline-fact insert stride, the `.input` insert stride,
  `wirelog_io_ctx_num_cols` and its `col_types` (an inline compound's slots
  are raw `int64` payload), the CSV stride in
  `wirelog_load_facts_from_csv`, and `wirelog_program_get_facts`. It is
  derived from `columns[]` on demand by `wl_ir_relation_physical_width()`
  rather than stored, because `column_count` has more than one writer and a
  cached copy could go stale. Logical width remains authoritative for every
  declaration question.

  **Compatibility:** a fact whose argument count is not the relation's
  physical width is now rejected at load with `WIRELOG_ERR_PARSE`, reported
  through `WL_LOG=PARSER:1`. For an inline-compound relation the diagnostic
  names both widths, since "2 arguments but 2 columns" would otherwise read
  as a contradiction. Nothing expressible is taken away: `pred(1,99).` was
  the only spelling that parsed, and it was the broken one, while the
  correct flat spelling `pred(1,10,20).` was *rejected*. There is no
  compound fact notation to redirect to either -- `pred(1, f(10,20)).` and
  `pred(1, [10,20]).` are parse errors, as is `pred(x, f(y,z))` in a rule
  head. Filling the unwritten slot instead of rejecting is not available:
  `0` is a valid `int64` and a valid intern id alike. The rule-head analogue
  was already rejected under #977; this makes facts agree with it. No
  benchmark, example or `.dl` workload in the tree declares an inline
  compound column at all. Around a dozen test files do -- `test_program`,
  `test_parser`, `test_wirelog_easy`, `test_wirelog_advanced`,
  `test_symbol_ordering`, `test_symbol_aggregates`, `test_symbol_digests`
  and more; the set moves with the suite, so read that as a sample and not
  as a closed list -- and each of them seeds such a relation through the API
  at the physical width rather than through an inline fact, so none changes
  behaviour. The passing suite is the evidence for that, not the
  enumeration.

  **A bound `.query` on such a relation is still not fixed.** `.query`
  arity flows through `passes/magic_sets.c`, which is deliberately left
  logical here: with #989 open the demand relation is never seeded, so the
  arity-mismatch warning is currently what routes the inline-compound case
  onto the skip path and lets it produce rows at all. Making that arity
  physical today converts a warning plus correct output into silence plus
  zero rows. It is blocked on #989, not overlooked.
- **Mixed insert/remove steps evaluate the union of their strata** (#1031):
  `col_session_step` assigned the affected-strata mask from the inserted
  relation and then *intersected* the removed relation's mask into it. A step
  that inserted into one relation and removed from another therefore evaluated
  the intersection of two sets that need not overlap, and when they did not,
  no stratum ran and the step emitted nothing. Neither effect is lost: the
  removed row has already been compacted out of the EDB and the inserted row
  is already in it, so each one surfaces on the next step whose own mask
  happens to cover the stratum that derives it -- separately, on steps whose
  inputs have nothing to do with either, arbitrarily far apart and arbitrarily
  far in the future. Until then the session reports a state its inputs no
  longer describe, and an intervening step with no input change reports
  nothing at all, because the delta path returns early once a delta callback
  is installed, no input change is pending, and both relation pointers are
  clear. The same `&=` also defeated `pending_full_input_eval` -- the "this
  step cannot be incremental" safety valve -- narrowing a full evaluation down
  to the removal's strata whenever a removal was pending. The mask is now a
  union seeded at 0, guarded so a step with only an insertion, only a removal,
  or neither keeps exactly the mask it had before; seeding the union at
  `UINT64_MAX` instead would have widened every case to full evaluation, the
  mixed one this fix exists to narrow included.

- **Magic Sets no longer guards a relation it never generates a demand for**
  (#1027): when an IDB body occurrence binds none of its columns, Phase 2 adds
  no adorned predicate for that relation and Phase 4a generates no demand rule
  -- but Phase 4b guarded its rules anyway. The relation was then restricted to
  a demand relation nothing ever populates, while its own body needed it
  unrestricted, so the recursion was cut. Left-recursive `p` under
  `.query p(f, b)` over `e = {(1,2), (2,3), (3,4)}`, demand seeded at `y = 4`,
  answered with one tuple against a six-tuple closure, losing two of the three
  the query asked for. Measured identically with Logic Fusion on and off, and
  with and without a filter on the rules, so this is not a #990 artifact.

  A guard-viability closure now runs between Phase 2 and Phase 3. It is seeded
  with every relation such an occurrence reads and closed under "R unguarded
  implies every IDB R reads is unguarded" -- an unguarded relation is evaluated
  in full, so a guarded child would cut its fixpoint one level down instead.
  Every relation in the closure is dropped from the transformation entirely: no
  magic relation, no demand rule (including demand rules that other, guarded
  relations would otherwise key on it), no guard. Rules outside the closure are
  guarded and pruned exactly as before. The correct fix is predicate splitting,
  which this pass cannot do -- `insert_magic_guard` rewrites the rule root in
  place, so one set of rules serves every adornment of a relation.

  The Phase 2 event has its own statistic, `unrestrictable_relations`, instead
  of sharing `skipped_all_free` with the unrelated Phase 1 event (a demand root
  adorned all-free, which is a genuine no-op). Read it as "how much of the
  program the pass declined to optimise in order to stay correct", not as "how
  many answers were lost": it is seeded from a binding pattern, which does not
  decide whether the occurrence contributes anything for the demand actually
  seeded.

  **The pruning this costs is not marginal.** Its dominant trigger is a rule
  with a constant in the bound head position, `X(1, y) :- ...` under
  `.query X(b, f)`: a constant yields no head variable, so Phase 2's bound set
  starts empty, every IDB in that body binds nothing and seeds the closure, and
  the transitive step unguards the subprogram below. Changing one character in
  `X(a, b) :- p(a, b), q(a, b).` to `X(1, b) :- ...` takes the same program
  from 4 adorned predicates and 4 guards to 2 and 1. Over a 342-program census
  on which the pass is sound and query-complete both with and without the
  closure, the unguarded oracle derives 1785 rows, the pass without the closure
  820, and with it 1085; it derives more than before on 94 of the 342, roughly
  one program in five. A separate sample found the constant-in-bound-head shape
  in almost every program the closure made lossier -- that sample and the 342
  census are different runs, so read the shape as the dominant trigger and the
  94 as the rate, not as two views of one population.

  The closure now covers the unsafe-consumer cases that were tracked
  separately: an output consumer (#1046), a relation read under negation
  (#1047), and a relation defined by an aggregate rule (#1048). The old
  failure mode was that an unguarded relation could read a guarded, partial
  producer, losing output rows or deriving answers that were not in the
  oracle; aggregate values could likewise be computed from only a subset.
  The 0.54.0 closure propagates viability through each of these consumers,
  and the focused tests for all three cases are included in this release.

  One new decline path: the closure walks rules of relations Phase 2 never
  touched, so it is the first thing in the pass that can meet a rule with more
  than `MS_MAX_ATOMS` body atoms in a subprogram the demand never reached.
  Rather than fail -- which `api_facade.c` would report as
  `WIRELOG_ERR_MEMORY`, failing `wirelog_optimize()` for a program that
  optimized before -- the pass warns and leaves the program as written.

- **Magic guards are built left-deep** (#989): `insert_magic_guard` produced
  `JOIN(magic_scan, body)`. When the body was itself composite -- a JOIN chain,
  an ANTIJOIN from a negated atom, a SIP-inserted SEMIJOIN -- that is a
  right-deep JOIN, and a right-deep JOIN is structurally unrepresentable in the
  execution plan: `wl_plan_op_t.right_relation` is a relation *name*, and
  `translate_ir_node` collapses `children[1]` to `rn->relation_name`. The
  operator was emitted with `right_relation = NULL`, matched nothing, and the
  rule silently derived zero tuples. Measured on the `docs/SYNTAX.md` `.query
  Path(b, f)` example with the demand relation seeded by hand: 3 of the 6
  closure tuples, the base rule's only. A non-recursive three-way join under a
  guard produced zero rows, as did a guarded rule with a negated atom.

  The guard SCAN is now the *right* child, matching the parser's rule chain and
  jpp's chain rebuild, which already build left-deep. One producer does not:
  `convert_rule` nests a side-compound atom's `JOIN(scan, side_scan)` subtree
  on the right whenever that atom is not first in the body, which has always
  been unrepresentable and has always silently matched nothing. That shape is
  now a plan-generation error rather than an empty result; it is tracked as
  #994 and is not fixed here. The swap also inverts which side a bound-variable
  comparison resolves against: the guard's columns now come last in the layout,
  and each is named after a body column that resolves first, so the guard's
  `column_types` (#962) are no longer reachable by name. They are still filled
  in, since `col_ctx_lookup_type` has a positional `colN` fallback that can
  land in the guard's range.

  **This does not make a bound `.query` work.** Nothing seeds the demand
  relation, so the guards still reject everything; see the status note in
  `docs/SYNTAX.md`. What is fixed is the rewrite itself, which is a
  prerequisite.

- **Plan generation rejects an unrepresentable JOIN right child** (#989):
  translating a JOIN whose right child carries no relation name now returns an
  error instead of emitting `right_relation = NULL`. That NULL was an entire
  silent-wrong-answer class -- the operator matches nothing and the rule
  quietly under-derives; it is now a plan-generation failure. No in-tree
  program, benchmark, or example produces such a JOIN.

- **`original_rules_modified` counts guards actually inserted** (#989):
  `insert_magic_guard` returns 0 both when it inserts a guard and when it
  declines to, and the caller incremented the counter unconditionally. A rule
  whose bound head position holds a constant (`q(1, y) :- ...` under `.query
  q(b, f)`) gets no guard, because there is no variable to key on -- yet was
  reported as modified. A program with two real guards reported three. The
  function now returns a distinguishable outcome, and skipped rules are counted
  separately.

  Two skip counters are added, kept apart because they mean different things.
  `skipped_constant_head` is a *policy* skip: the rule is well-formed and the
  pass declines. `skipped_unsupported_head` is a *capability* gap: the pass
  found no head variable names to key the guard on. The shape that used to
  dominate it -- a rule fused to a `FLATMAP` root, which any rule with a filter
  becomes, since Logic Fusion runs before Magic Sets -- no longer reaches it:
  #990 made `get_head_vars` read the root's `project_exprs` rather than test
  its node type, and fusion leaves `project_exprs` in place. What remains
  reachable is a head wider than the 64-bit adornment mask. Together these
  counters are the only mechanism that can detect a guard silently not being
  inserted.
- **`average()` is rejected instead of answering with an arbitrary row**
  (#978): `average()` was never implemented. `col_op_reduce` seeds each group
  with the group's first operand, and its update `switch` has arms for
  `COUNT`/`SUM`/`MIN`/`MAX` and `default: break;` — so `WIRELOG_AGG_AVG` fell
  through and the seed was returned untouched. The answer followed scan
  order, not the data: `val(1,9). val(1,5). val(1,2).` gave `t(1, 9)` where
  the mean is 5, and the same three facts reordered as `val(1,1). val(1,2).
  val(1,9).` gave `t(1, 1)` where the mean is 4. Both with exit status 0 and
  no diagnostic. `sum`, `count`, `min` and `max` were and remain correct.

  Rules using `average`/`AVG` are now rejected at plan generation:
  `wl_plan_from_program()` returns `-1` and `WL_LOG=EVAL:1` names the
  aggregate and the workaround.

  **Why reject rather than implement.** There is no type to return a mean in.
  Every value is an `int64_t`; `WIRELOG_TYPE_FLOAT` exists in the public enum
  but is vestigial, since the lexer has type keywords for
  `int32`/`int64`/`string`/`symbol` only and no decimal literal — `.decl
  v(x: float)` and `1.5` are both syntax errors. So the choice was truncating
  integer division or rejection, and the asymmetry decides it: widening a
  rejection to a real mean later accepts strictly more programs and rewrites
  none, whereas replacing truncation with a real mean later would silently
  change the numbers every existing program prints. Precedent agrees —
  Soufflé refuses integer operands to `mean()` outright, and PostgreSQL,
  SQLite, MySQL and cozo all return a wider type rather than truncating. This
  follows the sequence adopted for #973: reject first, support later.

  The rejection is at lowering, not in the lexer. `average` and `AVG` stay
  reserved keywords, the AST keeps its `AGGREGATE` node and
  `WIRELOG_AGG_AVG` stays in the public enum, so no surface syntax has to be
  un-done if a float type arrives. It deliberately does not add `avg` or
  `AVERAGE` as keywords, which would take those identifiers away from users
  and contradict a documented decision.

  **Compatibility:** programs using `average()` now fail to load. They were
  not producing a mean before — they were producing whichever operand the
  scan reached first — so nothing correct is lost. No `.dl` file, benchmark
  workload or example in the tree uses it.

  Two adjacent defects are fixed with it. `wirelog_agg_fn_str()` printed
  `"avg"` for `WIRELOG_AGG_AVG`, a spelling the lexer refuses, so every AST
  and IR dump named an aggregate that could not be read back; it now prints
  `"average"`, and `tests/test_parser.c` asserts the printer/lexer round trip
  for all five aggregates. And `agg_to_tag()` mapped `WIRELOG_AGG_AVG` to
  `WL_PLAN_EXPR_AGG_SUM` ("approximate: no AVG tag"), so a serialized plan
  would have said SUM where the program said `average`; it now fails instead
  of substituting.

- **Inline facts are validated against their relation's declared arity**
  (#977): `collect_fact` packed `fact_data` using each fact's *own*
  argument count as the row stride, while both readers of that buffer --
  `wl_session_load_facts` and the public `wirelog_program_get_facts`, the
  latter reachable by an embedder with no session at all -- strided by a
  single fixed width taken from the `.decl` (the *declared* `column_count`
  at the time; #985 above moves both readers to the physical width, which is
  the same number for every relation that declares no `inline` compound
  column). Nothing reconciled the two. Depending on which
  side was wider this produced a heap over-read, an uninitialised read
  *inside* the allocation that AddressSanitizer cannot see (exit status 0,
  emitting heap bytes as query answers), or a fabricated tuple: `val(1,5,7).
  val(2,6,8).` against a two-column `.decl` emitted `t(7,2)`, a pair present
  in no source fact, and dropped `(6,8)`.

  Such facts are now rejected during metadata collection, reported through
  `WL_LOG=PARSER:1` with the relation name and both arities, and surfaced as
  `WIRELOG_ERR_PARSE`. The check runs as a pass after all declarations are
  collected rather than inside `collect_fact`, because a `.decl` may legally
  follow its own facts in source order.

  Relations are tracked by a new `has_decl` flag rather than by
  `column_count > 0`, since `.decl p()` parses and leaves `column_count` at
  zero; without the flag exactly those relations would be skipped.

  **Compatibility:** programs that previously loaded now fail to load. Every
  rejected shape was already producing a crash, uninitialised heap, or a
  fabricated tuple, so nothing correct is lost. Verified across every Datalog
  program in the tree, including those the benchmarks generate from CSV at
  runtime: no in-tree program changes behaviour.

  **One half of this check is superseded later in this same release.** As
  landed, the comparison was against the *logical* `column_count`, which for
  a relation with an `inline` compound column is not the width its storage
  uses. That rejected the flattened spelling -- `p(1,2,3).` against
  `.decl p(id: int64, lbl: pair/2 inline)`, previously accepted while
  silently dropping the third value -- and went on accepting the
  two-argument `p(1,2).`, which leaves the second inline slot never written.
  #985 (see its `Fixed` entry above) moves the comparison to the physical
  width, so in the released build `p(1,2,3).` is **accepted** and stored as
  three slots, and `p(1,2).` is the rejected spelling. Read the two entries
  together: the *existence* of a fact arity check is #977, the width it
  compares against is #985. Nothing here changes for a relation without an
  `inline` compound column, which is every relation in the tree.

  This covers the inline-fact half of #977. Rule bodies and facts on
  undeclared relations remain unvalidated; rule heads are covered by the
  entry below.

- **Rule heads are validated against their relation's declared arity**
  (#977): a head emitting a different number of columns than its `.decl`
  declares was accepted silently. In a recursive stratum it is a heap
  over-read -- `col_op_reduce` sizes its output region as
  `group_by_count + 1` while `col_rel_append_all` copies `dst->ncols` columns
  out of it with no clamp, so `cc(y, min(c)) :- cc(x, c, d), edge(x, y).`
  against a three-column `cc` seeded by a fact segfaulted (exit 139;
  AddressSanitizer reports `heap-buffer-overflow READ of size 8` in
  `col_rel_append_all` under `col_eval_stratum`).

  Recursion is not the precondition — a producer at the declared width is.
  The non-recursive `t(g) :- val(g, v).` against `.decl t/3` is the same
  over-read once a `t(9,9,9).` fact fixes the relation's width, reached
  through `col_op_map` rather than `col_op_reduce`. Absent such a producer
  the relation materialises at the narrower width and the mismatch is a
  silent wrong answer instead: one column emitted with exit status 0, or four
  for `t(g,v,g,v)` against `.decl t/2`.

  Such heads are now rejected during metadata collection, reported through
  `WL_LOG=PARSER:1` with the relation name and both arities, and surfaced as
  `WIRELOG_ERR_PARSE`. As with the fact check, the pass runs after all
  declarations are collected, so a `.decl` may follow its rules, and it keys
  off `has_decl`, so undeclared derived heads -- ordinary Datalog, ~90 of
  them in `bench/workloads/doop.dl` alone -- are not checked. This completes
  the rule-head half of the single-aggregate recursive crash that #973 could
  not reach.

  Heads are compared against the declared **physical** width, where an
  `inline` compound column counts as its full arity and a `side` compound as
  one handle column. The head grammar has no compound-term production
  (`pred(x, f(y, z))` is a parse error), so the flattened spelling
  `pred(x, y, z) :- src(x, y, z).` is the only way to write an
  inline-compound relation from a rule, and a logical comparison would reject
  it. When this landed the fact check above compared logically instead, an
  asymmetry documented here as deliberate; #985 removed it by moving the fact
  check to the physical width too, so in the released build both compare
  physically.

  **Compatibility:** programs that previously loaded now fail to load. Every
  rejected shape was already a crash or a wrong answer. One shape worth
  naming: the two-argument handle form `pred(x, y)` into
  `.decl pred(id: int64, payload: f/2 inline)` is now rejected, because it
  leaves the second inline slot unwritten and a body pattern that
  destructures the column reads it anyway --
  `outr(id, p, q) :- pred(id, f(p, q)).` produced `outr(1, 99, 0)`, a value
  present in no source data. Verified across every Datalog program in the
  tree -- standalone `.dl` files, programs embedded in C string literals and
  markdown fences: roughly 1500 declared rule heads across some 1300
  programs, no arity mismatch under either the logical
  or the physical rule, and no rule head anywhere in the tree writes into a
  relation with an inline-compound column. No in-tree program changes
  behaviour.

- **`min()`/`max()` over a symbol column compare strings, not intern ids**
  (#965): both reduced over the raw `int64`, which for a `symbol` column is
  its interned id. Ids are handed out in first-appearance order, so
  prepending one unrelated fact that claimed the lower ids flipped
  `min(v)` from `"zz"` to `"aa"` with no change to the data being reduced.
  #962 fixed this class for `<`/`>`/`<=`/`>=`; this is the aggregate half,
  and it applies to both reducers -- `col_op_reduce()` and the
  recursive-aggregate canonicalisation, which flipped independently at
  every worker count.

  The plan's REDUCE operator now carries the operand's value domain,
  derived at lowering time from `expr_result_type()` -- a bare column
  lookup would not do, since `min(to_upper(v))` reduces a runtime-interned
  id belonging to no column. An internal opcode in the manner of #962's
  `WL_PLAN_EXPR_CMP_STR_*` was the other candidate and would have been
  equally non-public; it was rejected because `col_op_reduce()` already
  dispatches on `op->agg_fn`, so an opcode would be a second source of
  truth for the same decision rather than a refinement of it.

  Following #963, a column declared `symbol` whose values were never
  interned is reported rather than failed closed: it still reduces
  numerically, which is the right answer for numeric data, and erroring
  out would turn programs that work today into no output at all. Where a
  group mixes interned and un-interned values the interned ones win, for
  `min` and `max` alike.  That rule is a correctness choice, not a
  termination one: the intern table grows *during* evaluation, so
  "reversible" is a property of the value and the table, not of the value
  alone.  Termination holds either way -- the table only grows, so between
  two of its sizes the comparator is a fixed total order and each group's
  stored value strictly improves.

- **Rule heads with more than one aggregate are rejected instead of
  miscompiled** (#973): `t(g, min(v), max(v)) :- val(g, v).` kept only the
  last aggregate and emitted a two-column tuple against a three-column
  `.decl`. Outside a recursive stratum that was a wrong answer with exit
  status 0 and no diagnostic; **inside** one it was a segfault -- the
  columnar REDUCE path sizes its output region from the emitted arity while
  the append path reads the declared arity, so evaluation walked off the end
  of the region. Such rules now fail to lower, surfacing as
  `WIRELOG_ERR_PARSE`, with a `PARSER`-section `WL_LOG` error naming the fix
  (derive each aggregate in its own rule and join them).

  `wirelog_parse_string()` now calls `wl_log_init()` before parsing, so
  `WL_LOG=PARSER:1` surfaces that message and the other lowering rejections
  (`__graph_metadata` arity, unsafe variables). Previously the logger was
  initialized only in plan generation and session creation, both of which run
  after parsing, so no `WL_LOG` value could reach a lowering diagnostic.
  Default output is unchanged; a user who sets nothing still sees only
  `Parse error`, which is #979.

  **Compatibility:** programs that previously loaded now fail to load. This
  is not limited to wrong-arity results — `t(g, min(v), max(v))` against a
  *two*-column `.decl t(g, a)` emitted a correctly-sized two-column relation,
  exit status 0, and is now rejected as well. The rejection is intentional in
  both cases: the previous result was not correct for any reading of the
  rule, whether or not its arity happened to match.

  **This does not close the whole crash class on its own.** The underlying
  trigger is emitted arity differing from declared arity inside a recursive
  stratum, and multiple aggregates are only one way to produce it. A
  *single*-aggregate head with too few arguments --
  `cc(y, min(c)) :- cc(x, c, d), edge(x, y).` against a three-column `cc` --
  crashes identically with only one aggregate; that shape is rejected by the
  rule-head arity check, also under #977 (see the entry above). The two fixes
  are complementary and neither subsumes the other: an arity check accepts
  `t(g, min(v), max(v))` against a three-column `.decl`, and the
  aggregate-count check accepts `cc(y, min(c))`. Both gate the parser path
  only: IR built directly through the API is still unvalidated.

- **`uuid5()` framing carries the operand's type, not only its length**
  (#968): #963 length-prefixed each operand of the three symbol-bearing
  `uuid5` opcodes, which fixed the split point but not the *domain*. Two
  collisions survived it, both reproduced end to end. `uuid5("", "")`
  framed to `LE64(0) || "" || LE64(0) || ""` -- sixteen zero bytes, exactly
  what the unframed int64-only opcode emits for `uuid5(0, 0)` -- so both
  returned `6655197997870229985`. And a length prefix says nothing about
  what an operand *is*, so `uuid5("abcdefgh", x)` equalled
  `uuid5(7523094288207667809, x)`, that integer being precisely the `int64`
  whose little-endian bytes spell `abcdefgh`.

  Each framed operand is now `tag || LE64(len) || bytes`, where the tag is
  one byte: `'S'` for a symbol's bytes, `'I'` for an `int64`'s. The encoding
  is thereby injective over *typed* operand pairs rather than only over byte
  strings. The tag is taken from the bytes actually emitted, not from the
  opcode's declared operand type, so the documented fallback -- a `symbol`
  column holding a value that was never interned digests its `int64` form --
  is tagged `'I'` and cannot collide with the symbol spelling those same
  eight bytes.

  **`0x2A`, the int64-only opcode, is untouched**: still unframed, still
  untagged, still returning every value it ever has. It is the only `uuid5`
  encoding that has shipped, and its two operands are 8 bytes each and both
  `int64`, so neither ambiguity can arise there. Adding a framed `UUID5_II`
  opcode was considered and rejected: the unframed 16-byte encoding is
  currently the *only* domain separation `uuid5` has, so framing the integer
  path would have pulled it into the second collision's family -- trading
  one degenerate collision for an unbounded one. No opcode was added;
  `0x6A`..`0x6C` and their framing are unreleased, so only their values
  moved, and the five pinned constants in `tests/test_cryptographic_hashes.c`
  move with them.

  **Injective encoding is not collision-free output**, and the docs no
  longer let that read as if it were. `uuid5()` returns `digest[0..7]` with
  four bits overwritten by the version nibble, so it has at most 2^60
  distinct results however unambiguous its input. Separately,
  `docs/SECURITY_MODEL.md` now states domain non-separation as its own
  caveat for the unary digests and `hmac_sha256()`, where it is *not* being
  fixed: those are bound by #963's byte-transparency contract -- `printf
  '%s' abcdefgh | xxhsum -H3` must reproduce `hash("abcdefgh")` -- which
  leaves no room for a type tag. That caveat is distinct from the 64-bit
  fold already documented: the fold is a birthday bound, while this is
  constructible at zero cost.

- **`hash()` and the digest family digest string bytes, not intern ids**
  (#963): `hash`, `crc32_ethernet`, `crc32_castagnoli`, `md5`, `sha1`,
  `sha256`, `sha512`, `hmac_sha256` and `uuid5` digested the `int64_t` on the
  evaluation stack. For a `symbol` column that `int64_t` is an interned id, so
  the result described the intern table rather than the string:
  `hash("abc")` matched no external tool, and the same string digested
  differently depending on what had been interned first.
  `examples/04-hash-functions` showed both failures at once. Prepending one
  row to `records.csv` gave alice bob's fingerprint, and
  `hash("carol@example.com")` collided with the Part 3 checksum `hash(5)`
  over a genuine integer -- both were really digests of small ids. `examples/05-crc32-checksum` was outright broken: its
  committed checksums are `crc32(payload)`, so every one of its six frames was
  reported corrupt and the `diff` its README documents failed.

  `exec_plan_gen.c` now types the operands of a digest the same way #962 types
  the operands of a comparison, and emits one of thirteen new payload-free
  opcodes, `0x60`..`0x6C`, when an operand is string-typed. Those digest the
  string's own bytes -- `strlen()` many, **no NUL terminator** -- which is what
  `xxhsum -H3`, `zlib.crc32` and `sha256sum` see for the same input.
  `examples/05` now reproduces its committed output exactly, byte for byte,
  with no edit to any golden; `examples/04`'s fingerprints move to the values
  `xxhsum -H3` prints, and its checksum and deduplication outputs do not move
  at all.

  `hmac_sha256(msg, key)` and `uuid5(ns, name)` type their two operands
  independently and so get three opcodes each (`_SS`, `_SI`, `_IS`); the
  all-integer case keeps the existing opcode. This deliberately diverges from
  #962's rule that a one-sided type match keeps the integer opcode: an
  ordering comparison needs *both* ids reversed to mean anything, while each
  digest operand contributes its own bytes, so following that rule would leave
  every mixed call digesting an id -- the defect itself.

  **Column types are not enforced, and the fix inherits that.** A column
  declared `symbol` that holds values which were never interned digests their
  `int64` representation, i.e. exactly what the numeric opcode would have
  produced, and the query still runs. Failing the row instead was considered
  and rejected: in head position an expression failure is not a dropped row
  but an `ERANGE` that aborts the entire `PROJECT` operator, so a query that
  runs today would become `error: execution failed` with no output. A column
  with no declared type at all still digests the id and is reported at
  `WL_LOG=EVAL:2`; a failed reverse lookup is reported at `WL_LOG=EVAL:4`.
  `docs/SEMANTICS.md` and `docs/SYNTAX.md` state the limit.

  One thing the docs now say plainly that is not new behaviour:
  `md5`/`sha*`/`hmac_sha256` return `XXH3_64bits(digest)`, not the digest --
  reproducible as `printf 'abc' | sha256sum | cut -d' ' -f1 | xxd -r -p |
  xxhsum -H3`. `sha256("abc")` is not SHA-256 of "abc" and never was.

  One thing that *is* new, because this change would otherwise have created
  it: `uuid5()` concatenated its two operands, which is unambiguous only
  while both are fixed-width. Giving it variable-length operands would have
  made `uuid5("ab", "c")` and `uuid5("a", "bc")` hash identical bytes and
  return one value, and two distinct `(namespace, name)` pairs colliding is
  the one thing a namespaced identifier must not do. RFC 4122 avoids this by
  requiring the namespace to be exactly 16 bytes; wirelog adopted the
  two-operand signature without that property, so the three symbol-bearing
  opcodes frame each operand -- a one-byte domain tag and its length as a
  little-endian `uint64` (the tag was added by #968, below, before either
  shipped) -- before hashing. `uuid5(int64, int64)` keeps the unframed
  construction and its exact previous values: its operands are 8 bytes each,
  so nothing was ambiguous there, and an out-of-tree decoder that already
  implements `0x2A` keeps computing the same answer. Rejected alternatives:
  reducing the namespace to a fixed 16 bytes by digesting it, which is
  lossy, costs a second SHA-1 per row, treats the two operands asymmetrically
  and makes the result *look* RFC-conformant when it is not; and applying
  the framing to `0x2A` as well, which would have changed values that were
  never wrong. `uuid5()` remains non-conformant either way -- the namespace
  need not be a UUID and only 8 of the 16 bytes are returned -- and
  `docs/SYNTAX.md` keeps "unambiguous" and "RFC 4122" as separate claims.
  `hmac_sha256()` has no such exposure: it passes message and key to the
  HMAC primitive as distinct inputs rather than concatenating them, verified
  by evaluating both splits.

  Cost: none of these functions was ever on a fast path -- `col_expr_compile()`
  returns NULL for them and `filter_is_simple_cmp()` rejects them on shape --
  so unlike #962 there is no demotion. End-to-end over 1M rows of ~20-byte
  symbols, a program computing `hash(sym)` in a head position measures within
  a few percent of the same program over an `int64` column.

  **Out-of-tree backends:** plans now contain expression opcodes `0x60`..`0x6C`.
  They are payload-free, exactly like the opcodes they shadow, so a decoder
  that skips one byte per unknown opcode stays in sync with the rest of the
  stream -- this is why a flag byte on the existing opcodes was rejected. A
  decoder that treats unknown opcodes *permissively*, however, will admit rows
  through a filter it did not evaluate. `tests/fpga_backend.c` did exactly
  that; its default arm now rejects instead, matching `columnar/ops.c`, and it
  decodes the new opcodes. Its `WL_PLAN_EXPR_ARITH_HASH` arm was also a
  no-op that silently degraded `hash(x) = k` to `x = k`, and is now a real
  digest. `exec_plan.h` is not in `wirelog_public_headers`, so this is not an
  installed-ABI break.
- **Ordering comparisons on symbols compare strings, not intern ids** (#962):
  `<`, `>`, `<=` and `>=` mapped to the integer opcodes regardless of operand
  type, so on `string`/`symbol` columns they compared the interned integer
  ids. Ids are assigned in first-appearance order, so `S("zz", "aa").` with
  `R(a, b) :- S(a, b), a < b.` derived a row -- `"zz"` was seen first and
  therefore held the smaller id -- and inserting an unrelated *earlier* fact
  silently changed the answer. `exec_plan_gen.c` now types both operands and
  emits `WL_PLAN_EXPR_CMP_STR_LT`/`GT`/`LTE`/`GTE`, which reverse the ids and
  `strcmp` the strings. Those opcodes were already implemented in
  `columnar/ops.c` and simply had no emitter.

  Only the four ordering operators are converted, and only when *both*
  operands are string-typed. `=` and `!=` are unchanged: interning is
  canonical, so id equality already is string equality. A one-sided type
  match keeps the integer opcode, because the string opcodes return false
  whenever `wl_intern_reverse()` yields NULL -- applied to an integer operand
  they would drop every row rather than misorder them. String-function
  results (`cat`, `substr`, `to_upper`, `to_lower`, `str_replace`, `trim`,
  `to_string`) are runtime-interned ids and count as string-typed, so
  `to_upper(a) < to_upper(b)` is lexicographic too.

  Columns with no declared type keep the old id-order behaviour: undeclared
  relations are legitimate (head and intermediate relations need no `.decl`)
  and rejecting them would break programs that work today. Each such
  comparison, and each string-vs-numeric mix, is now reported at
  `WL_LOG=EVAL:2`.

  `min()` and `max()` over symbol columns still reduce by id and remain
  non-lexicographic; that is #965. `examples/06-timestamp-lww` and
  `examples/07-multi-source-analysis` recommended `min(Val)`/`min(Name)` as a
  *deterministic* tiebreak, which was never true; both READMEs now say so.

  Cost: a converted comparison falls off the compiled and column-native SIMD
  filter paths onto the bytecode interpreter, because `col_expr_compile()`
  and `filter_is_simple_cmp()` both reject unrecognised opcodes. Measured at
  1M comparisons, 34 ms -> 111 ms (+77 ns per comparison); about 86% of that
  is the demotion and 14% the two `wl_intern_reverse()` calls plus `strcmp`.
  Integer filters are untouched and keep both fast paths -- a separate
  integer-comparison program over the same 1M rows measures 40 ms before
  and after, and does not become comparable to the 34 ms string baseline
  above because the two programs differ in more than the operator.
  Recovering a fast path for string comparisons is #966.

  **Out-of-tree backends:** plans now contain expression opcodes `0x52`..`0x55`
  where only `0x24`..`0x27` appeared before. A `wirelog/backend.h` consumer
  that decodes expression buffers and treats unknown opcodes permissively --
  as the in-tree `tests/fpga_backend.c` did -- will silently *admit every row*
  through such a filter rather than fail. Decode the four new opcodes, or
  reject unknown ones. This is not an installed-ABI break: `exec_plan.h` is
  not in `wirelog_public_headers`.
- **Shared intern table is now safe under parallel evaluation** (#958): every
  worker borrows the coordinator's `wl_intern_t` -- it is propagated by the
  bitwise session copy and is absent from both borrowed-field null-out lists --
  so all three worker families (non-recursive, TDD, K-fusion) called an
  unsynchronized hash table concurrently. A rule that builds strings in a head
  position could therefore hand two ids to one string, and the `realloc()` that
  doubled the id -> string array could relocate it out from under a worker
  mid-dereference: measured at 100% SIGSEGV for a 31k-row recursive rule and a
  100k-row non-recursive rule at W >= 8. The id -> string storage is now
  segmented and never moves, `count` is published with a release store after
  the entry is complete and read with an acquire load, and writers serialize on
  a mutex. `wl_intern_reverse()` and `wl_intern_count()` stay lock-free: reverse
  runs once or twice per row inside `contains`/`strlen`/`substr`/`to_number`,
  a lock there measured 10.9x slower at W=8 than the unlocked baseline.

  Two consequences to know about, both of which only affect W > 1.

  Ids are assigned in worker-interleaving order, so an id is unique and
  stable within a run but not reproducible across runs. That is visible in
  results, not just internally. At the time of this change `<`, `>`, `<=`,
  `>=`, `min` and `max` on symbols all compared the intern id rather than
  the string; the four ordering operators are fixed by #962, recorded above
  in this same section, and `min`/`max` are #965. `hash()`, `crc32`,
  `md5`, `sha*`, `hmac` and `uuid5` still digest the id rather than the
  string bytes. Queries using any of those over symbols produced during
  evaluation can therefore give different answers on different runs. Those
  are pre-existing defects, filed separately; before this change the same
  programs crashed instead.

  Serializing writers costs throughput on workloads that intern heavily
  during evaluation -- a rule with `cat()` in head position, say. Measured
  at a fixed 1.6M unique inserts split across workers: 561 ms at W=1
  against 2,224 ms at W=8, with 8.8 s of system time and 938k voluntary
  context switches. That is futex convoy rather than plain serialization,
  and it is a follow-up. The comparison is against a baseline that
  SIGSEGVs on the same input, so there is no correct prior timing to
  regress from.
- **`scripts/run_doop_validation.sh` no longer expects the vanished CSV
  layout** (#952): it counted 34 `*.csv` files and spot-checked
  `ActualParam.csv`, so after the archive switched to 35 string-valued
  `.facts` (#950) it failed for every user with `expected 34 CSV files,
  found 0`. It now counts `.facts`, points at the download script when the
  dataset is absent, and carries the reconciled tuple oracle
  (14,096,448 after #955) --
  the previous 6276338 matched neither the measurement nor the README's
  6,276,657, so the two in-tree "expected" values for one workload had
  disagreed by 319 for some time. The default is W=1-specific and is not
  applied at W>1, which is not reproducible (#958). It also gained an
  iteration oracle as a cheap second signal -- it does catch defects the
  tuple total hides (the heap-typing bug fixed in #951 ran 70 iterations
  while its total stayed within 3% of correct), but not the converse: 28
  survives configurations where the whole type hierarchy derives zero
  rows, so a matching iteration count proves nothing on its own.
  `--help` printed a fixed `head -40` window and had silently begun
  truncating its own options list.

- **SEMIJOIN/ANTIJOIN widened the plan's output layout** (#955):
  `collect_output_columns` grouped them with JOIN and concatenated the right
  child's columns, but both operators only filter and emit the left side.
  Every SIP-inserted semijoin therefore inflated the reported layout by the
  right relation's arity, shifting every join key and fused projection
  resolved above it; `col_rel_col_idx` returned -1 for the out-of-range
  `colN` and the resolution sites silently fell back to column 0.  A plan
  bug became a wrong answer instead of an error.

  The engine's output stopped being a model of its own program.  In DOOP a
  `VarPointsTo` rule generated `col7=col0` over a six-column left input, so
  it compared `inv` against `sn`; 354 immediate consequences of that one
  rule were missing from the final fixpoint with every body atom present,
  and `ArrayIndexPointsTo` derived nothing at all.  Against the full-DOOP
  reference outputs in the recovered archive, `VarPointsTo` measured 5,266
  where the reference has 4,455,314.  After the fix: 4,121,488.

  SIP is on by default, so this reached user programs with a rule of this
  shape, not only benchmarks.  Among the 15 shipped workloads only DOOP's
  plan changes; out-of-range resolutions go 47 to 0 there and are 0
  everywhere else both before and after.

  DOOP's cost changes accordingly -- 94 s to about 23 minutes at W=1, 28 to
  153 iterations, 39 GB peak -- and it no longer completes at W>1 (#959).
  The old speed was under-derivation.

- **DOOP `Method_Descriptor` derived from the wrong column** (#956): #951
  reconstructed the relation (the archive stopped shipping it) by projecting
  `Method.facts` column 3, the parameter list.  DOOP's convention is
  `?returnType "(" ?paramTypes ")"`.  Confirmed against the recovered
  2026-05-24 archive's own `Method_Descriptor.csv`: the descriptor form
  matches all 70,373 rows, the params form matches none.

  Two consequences.  562 `MethodLookup` rows were lost to the
  `!MethodImplemented` antijoin, and -- less visibly -- the coarser key
  merged covariant overrides and bridge methods, leaving 1,829 ambiguous
  virtual-dispatch keys where the correct reading leaves 625.  Wrong
  dispatch targets, not just missing rows.

  The constant sites move with it: `MainMethodDeclaration` from
  `"java.lang.String[]"` to `"void(java.lang.String[])"`, and
  `ClassInitializer` from `""` to `"void()"`.  Missing the second would
  have silently emptied `ClassInitializer` -- all 1,900 `<clinit>` rows
  carry the empty parameter list, and no `rt(params)` descriptor can be
  empty.

  Every existing test supplied `Method_Descriptor` as EDB facts, so none
  reached the derivation and no amount of constant-pinning could have
  caught this.  Two tests now derive it, and the assertions include a
  method with non-empty parameters -- with only no-argument methods,
  `cat(rt, p)` without parentheses and the arguments transposed both
  produce the same partition as the correct rule and survive a
  count-only test.

- **Unbounded source construction in a filter test** (#939, #940): a test-source
  builder accumulated `snprintf` return values, which report the length that
  would have been written rather than the length written, so the write cursor
  could advance past the buffer once anything truncated. It now shares the
  bounds-checked builder, which treats truncation as an explicit failure.
  Latent only: the buffer was oversized, so nothing truncated in practice.

### Performance

- **Column-native SIMD filter scan** (#939): `col_op_filter`'s
  simple-comparison fast path now scans a contiguous key column with an AVX2
  kernel that processes 8 rows per iteration and left-packs surviving row
  indices through a 2 KB `.rodata` table. Scan throughput is roughly 10x the
  scalar loop; end to end the path measures 1.5x-3.3x the previous fused loop
  on narrow relations. Scanning is tiled and falls back to the fused loop when
  the first tile keeps more than seven eighths of its rows, since a
  near-total-pass predicate materializes faster without the selection-vector
  indirection. Wide relations remain materialize-bound and are unchanged to
  within a few percent. Builds without AVX2 keep the previous fused loop, which
  measured faster there than the selection vector.

### Security

### Documentation

- **Benchmark table re-measured** (#952): the README portfolio was last
  measured 2026-05-24 and four workloads' *output* had changed since. CRDT
  (1,301,914 -> 2,156,530 tuples), DDISASM (531 -> 900) and Polonius
  (1,807 -> 1,999) moved under `61e2530` (#914); this was confirmed rather
  than assumed by building both sides of that commit, which reproduce the
  old and new values exactly. DOOP moved under #950/#951.

  Each workload is now measured in its own process. Peak RSS is a
  process-wide high-water mark, so the convenient single `--workload all`
  invocation reports the largest workload's footprint for everything that
  follows it -- CSPA's 325 MB was being attributed to five later rows.

  The DOOP row carries the archive sha256 it describes and its ~33 GB
  memory requirement, which is a barrier for anyone following the
  reproduction block. Its difference from the old row is accounted for
  rather than presented as a refresh: upstream replaced the archive 51
  minutes after the old row was measured, the original is still served at
  a pinned revision of the same git-backed mirror, and building the
  benchmark at `70f4d84` against it reproduces the old row exactly
  (6,276,657 tuples, 28 iterations, 11.8 GB). On a basis excluding the two
  now-derived relations the archive change was the smaller term; after
  #955 landed, the engine change dominates and no tuple-level
  decomposition against the old row is meaningful.  `AssignLocal` alone
  dropped 306,227 -> 144,124 rows between the two archives.

## [0.53.0] - 2026-07-31

### Added

### Changed

### Deprecated

### Removed

### Fixed

- **Query-mode snapshot correctness** (#929, #930): snapshots after input
  changes or retractions no longer reuse stale materialized derived state that
  could emit phantom or duplicate tuples and suppress valid
  stratified-negation results. Completed evaluation state is now tracked
  separately, bulk inserts force full recomputation, and full recomputation
  resets each stratum and rule frontier.
- **Static string literal interning** (#932, #933): plan generation now
  pre-interns static filter, projection, and aggregate literals. Those plan
  literals now stay consistent in paired Easy read and delta sessions
  regardless of which evaluates first, preventing downstream host-row
  mismatches and `UNKNOWN` failures.
- **Relation-name buffer lifetime** (#931, #934): pending insert and remove
  evaluation now retains the session-owned canonical relation name, so callers
  and FFI bindings can reuse or release their relation-name buffer after the
  API call returns without silently losing `step()` or snapshot deltas.
- **CRDT release performance gate** (#935): the gate now validates the final
  `result` relation instead of optimizer-sensitive aggregate snapshot rows,
  while retaining aggregate counts as diagnostics. The corrected post-#914
  full-snapshot evaluation had a 36,303 ms nine-run median, establishing a
  38,120 ms gate target.

### Performance

### Security

### Documentation

## [0.52.0] - 2026-06-28

### Added

### Changed

### Deprecated

### Removed

### Fixed

- **Unsafe variables in negated body atoms** (#920): the IR lowering now
  rejects rules where a named variable appears only inside a negated body
  atom (e.g. `c(X) :- a(X), !b(X, Y).`).  Such variables have an unbounded
  range and were previously treated like wildcards, silently accepting the
  rule and producing range-dependent results.  Every named variable in a
  negated atom must now also be bound by a positive body atom, otherwise the
  rule is rejected with `WIRELOG_ERR_INVALID_IR`; the error names the
  supported workaround (project the negated relation to a key relation
  first).  Wildcards (`!b(X, _)`) and constant columns remain accepted.
- **String literal escape decoding** (#925): the lexer now recognizes `\"`
  and `\\` escapes inside string literals.  An escaped quote no longer
  terminates the string early, and the decoded token value unescapes `\"`
  and `\\` instead of preserving the backslash verbatim.

### Performance

### Security

### Documentation

- **Negation and side compounds** (#921): documented that negating a
  side-tier compound body pattern (e.g. `!event(ID, metadata(...))`) is
  rejected because negated side-join lowering is not yet implemented, and
  the supported positive-extraction workaround that preserves the same
  expressive power.  The IR-conversion error now names the workaround and
  points at `docs/COMPOUND_TERMS.md`.

## [0.51.0] - 2026-06-13

### Added

### Changed

### Deprecated

### Removed

### Fixed

- **Non-recursive strata after recursive evaluation** (#914): reset the
  columnar evaluator's iteration context before non-recursive strata run,
  preventing stale recursive iteration state from incorrectly suppressing
  static EDB reads and dropping derived rows such as
  `requires_review(...)`.
- **Windows CI compiler selection** (#917): force the MSVC compiler in the
  Windows PR and main workflows so the intended toolchain is selected
  consistently.

### Performance

### Security

### Documentation

## [0.50.0] - 2026-05-28

### Added

### Changed

- **RC freeze PR gate for `1.0` branch targets** (#747): added
  `scripts/ci/check-changelog-rc.sh` and wired it as an always-emitting
  `RC changelog freeze gate` context in `.github/workflows/ci-pr.yml`.
  The gate SKIPs/passes on non-`1.0` PRs, and enforces on `1.0` PRs:
  `meson.build` project version must match `1.0.0` or `1.0.0-*`
  (e.g. `1.0.0-rc1`), `CHANGELOG.md` `[Unreleased]` must remain frozen
  versus base, and changelog edits must be confined to `## [1.0.0]`.

### Deprecated

### Removed

### Fixed

### Performance

### Security

### Documentation

- **Docs-freeze readiness follow-up** (#900): updated
  `docs/MIGRATION.md` external migration guidance to use public APIs
  (`wirelog_session_make_compound()` / `wirelog_easy_make_compound()`
  and `wirelog_session_step()` / `wirelog_easy_step()`), strengthened
  GA deferral cross-links for #752 and #753 in release-facing docs, and
  aligned the remaining current-source branch-name reference to `1.0`.

- Clarified #746 release-branch readiness/cutover sequencing:
  preparatory repo-side CI targeting now lands before branch creation,
  while final `1.0` branch-protection acceptance is deferred to
  the last-moment RC1 cutover when `project_version` is `1.0.0-rc1`.
  Updated Phase B concrete required-check naming to
  `mbedtls-enabled / ubuntu-latest / gcc`, removed the path-filtered
  perf-suite context from required branch-protection checks, and
  documented that final acceptance requires a dedicated always-emitting
  release perf context (`WIRELOG_PERF_REQUIRE=1`) plus CODEOWNERS and
  CLA workflow/context prerequisites verified by a synthetic PR.

## [0.44.0] - 2026-05-25

### Added

- **CRC-32 checksum expressions** (#884): added the `crc32_ethernet(x)`
  and `crc32_castagnoli(x)` columnar expression built-ins. Each accepts a
  single arithmetic argument and returns the CRC-32 of its 8-byte `int64`
  representation as a non-negative `int64` (CRC-32/ISO-HDLC and CRC-32C
  respectively). Unlike the mbedTLS-backed digest/HMAC built-ins, the
  CRC-32 functions are always available regardless of the `mbedTLS` build
  option. See `docs/SYNTAX.md` and `examples/05-crc32-checksum/`.

### Changed

### Deprecated

### Removed

### Fixed

### Performance

### Security

### Documentation

## [0.43.0] - 2026-05-24

### Added

- Added a dedicated `mbedtls-enabled / ubuntu-latest / gcc` CI leg
  that configures with `-DmbedTLS=enabled`, verifies
  `WL_MBEDTLS_ENABLED=1`, and runs `cryptographic_hashes` so optional
  crypto paths are covered without changing the default disabled
  artifact posture (#843).
- Added `wirelog_program_get_relation_ir()`, a relation-scoped public
  accessor that returns the borrowed merged IR root for a derived
  relation and exposes multi-rule relations as `WIRELOG_IR_UNION`
  without inventing a program-level super-root (#860).

### Changed

- **Numerical safety fail-closed policy** (#822): columnar arithmetic
  now rejects unrepresentable `int64` results instead of wrapping,
  saturating, or leaking undefined behavior. Filters fail closed by
  rejecting rows; MAP/head and REDUCE expression contexts propagate
  `ERANGE`. The policy covers checked `+`, `-`, `*`, `/`, `%`,
  `bshl`, `bshr`, checked `to_number()` range parsing, and `sum()`
  accumulation, with the audit recorded in `docs/SEMANTICS.md`.

### Deprecated

### Removed

### Fixed

- **Recursive aggregation conformance for columnar MIN/MAX** (#692):
  REDUCE plan ops now carry aggregate expressions, so recursive
  aggregates evaluate `min(l)` / `max(d + w)` instead of falling back to
  a positional input column. This release includes the #852 recursive
  aggregation fix recorded under #859 while preserving #692 context.
  Recursive MIN/MAX IDBs are canonicalized after sequential
  fixed-point convergence and TDD final merge so dominated aggregate
  rows are removed from snapshots. Adds `recursive_agg_conformance`
  coverage for CC-min, SSSP-max, and stratified COUNT at workers 1,
  4, 8, and 16.

### Performance

- **Stable snapshot fast path** (#811): clean sessions now read cached
  materialized IDB rows directly on repeated snapshots instead of
  re-running TDD evaluation when no input or retraction state is pending.
- **CSPA W=1 perf gate** (#818): added `test_cspa_perf_gate` as a
  dedicated static `cspa-fast` regression guard for `W=1` using 9-trial
  median timing, 20,381-tuple/6-iteration correctness sentinels, and
  the shared `WIRELOG_PERF_GATE` / `WIRELOG_PERF_REQUIRE` control
  flow.
- **v0.43 benchmark speedup notes draft** (#794, #512, #791): added
  the portfolio speedup draft with #512 timing deltas, changed-commit
  provenance, and memory-trade-off context:
  [docs/release-notes/v0.43.md](docs/release-notes/v0.43.md).

### Security

### Documentation

- Documented the required-check policy for mbedTLS-enabled validation,
  including the stable `mbedtls-enabled / ubuntu-latest / gcc` check
  name and its PR, main-monitoring, `1.0`, and release-tag
  roles without changing the default `mbedTLS=disabled` artifact
  posture (#849).
- Added `docs/PLATFORM_SUPPORT.md` to classify Android/iOS release
  artifacts as Tier-2 for 1.x, documenting that Android AAR/Prefab and
  iOS XCFramework publication are deferred until explicit future
  promotion work is completed (#697).
- Added `docs/ios.md` with iOS integration guidance for
  `wirelog.xcframework` consumption, Swift callback registration with
  trailing `user_data`, simulator architecture handling, and
  App Store/tooling constraints for #470.
- Added `docs/android.md` with Android integration guidance for
  AAR/Prefab consumption patterns, JNI thread attachment patterns,
  `Context.getFilesDir()` path handling, and Android CI/alignment
  requirements for #466.

## [0.41.0] - 2026-05-20

### Added

- **Android CI smoke + 16 KB alignment hard gate** (#465):
  new `.github/workflows/android.yml` with three jobs.  `alignment-arm64`
  cross-compiles `libwirelog.so` via `cross/android-arm64.ini` and runs
  `scripts/ci/check-android-alignment.sh` against the result; the script
  uses `readelf -lW` to enumerate PT_LOAD segments and asserts each
  carries `Align >= 0x4000` (16 KB).  Hard-fails the PR on any
  insufficiently-aligned segment -- Play Store rejects new 4 KB-only
  arm64 binaries on API 35+ uploads (Nov 2025 enforcement).
  `negative-host` is the load-bearing self-test: builds the host Linux
  `.so` (which has 4 KB PT_LOAD alignment) and asserts the SAME script
  exits non-zero, proving the parser works in both directions and
  protecting against the fake-closure mode where a regex bug always
  passes.  `smoke-x86_64` cross-compiles the second ABI from #464
  without an alignment assertion (the 16 KB link flag is gated on
  `cpu_family == 'aarch64'`).  NDK r27c is installed via
  `nttld/setup-ndk@v1` and symlinked to `/opt/android-ndk-r27c` so the
  committed cross-files resolve their `ndk` constant without local
  edits.  All three jobs use the default `continue-on-error: false`
  (the deliberate opposite of #708's tsan-native advisory leg).
  Issue body's earlier `readelf -l | grep LOAD` recipe was superseded
  by the wide-mode parser; issue body's `r26d` NDK pin was reconciled
  up to `r27c` to match the cross-files shipped in #464.
- **Android meson option + NDK cross-files** (#464):
  new `meson_options.txt` boolean `android` (default false).  When
  enabled, `meson.build` excludes `wirelog_cli` from the build (no
  useful CLI entry on JNI / NDK apps), appends
  `-Wl,-z,max-page-size=16384` to `libwirelog.so`'s `link_args` on
  `aarch64` for Android 14+ 16 KB page compatibility (the flag is
  meaningless on x86_64 emulator builds and is omitted there), and
  cascades the CLI guard to the four CLI-dependent baseline tests in
  `tests/meson.build` (`baseline_int_edges`, `baseline_sym_family`,
  `baseline_tab_nodes`, `cli_version`).  Symbol-visibility policy is
  unchanged -- `gnu_symbol_visibility: 'hidden'` plus `WIRELOG_API`
  annotations on the 9 installed headers already cover all 53 Android
  exports, so the v1.0 ABI manifest gate at
  `scripts/ci/check-abi-symbols.sh` remains intact across platforms.
  Two NDK cross-files ship: `cross/android-arm64.ini` and
  `cross/android-x86_64.ini`, both pinned to NDK r27c (LTS), with
  `[constants]`-driven path composition and a documented override path
  for non-standard NDK install prefixes.  `armeabi-v7a` is intentionally
  not supported (Play Store has required 64-bit since 2019; armv7
  would carry a third toolchain for zero modern delivery value).  The
  recipe lives at `docs/cross-compile-android.md`; CI matrix coverage
  is a separate follow-up (out of scope for #464).
- **libabigail `.abi.json` ABI manifest + abidiff CI gate** (#786):
  the v1.0 ABI golden file gains its libabigail half.  Where the
  53-entry `abi/libwirelog-1.0.symbols` allowlist (#733 K2) only
  checks the dynamic-symbol *set*, the new
  `abi/libwirelog-1.0.abi.json` baseline (16 KB / 127 lines,
  generated reproducibly via
  `scripts/release/regenerate-abi-manifest.sh`) pins struct member
  offsets and padding, function-signature shapes, visibility
  attributes, and typedef targets.  `scripts/ci/check-abi-manifest.sh`
  (registered as `meson test --suite abi:abi_manifest`) runs
  `abidiff --suppr abi/libwirelog-1.0.suppr` (suppression file
  optional) against the just-built `build/libwirelog.so` and fails
  the gate when libabigail reports incompatible-change bits (rc & 4).
  Both Linux GCC and Clang legs of `.github/workflows/ci-pr.yml`
  and `.github/workflows/ci-main.yml` now `apt-get install
  abigail-tools` (the Ubuntu 24.04 package name; `libabigail-tools`
  is not a valid candidate on that runner image) so `abidiff` is
  on PATH in CI.  On non-x86_64 hosts (e.g. the new
  `ubuntu-24.04-arm` PR leg) the gate SKIPs cleanly with a clear
  message -- abidiff treats ELF architecture as an ABI-breaking
  change (`architecture changed from 'elf-amd-x86_64' to
  'elf-arm-aarch64'`, rc=12), so the committed x86_64 baseline
  cannot directly diff against an arm64 build.  Issue #824 explicitly
  scopes v1.0 Linux arm64 ABI coverage to the arch-agnostic
  `abi_symbols` allowlist on every PR; per-arch libabigail baselines
  are out of scope for #681 / v1.0 unless a later policy issue adds
  an arm64 baseline file and regeneration workflow.  The gate's other
  SKIP paths remain for platforms where libabigail is unavailable
  (Windows, macOS, cross-builds).  Demonstrated firing on a
  synthetic break: adding a new `WIRELOG_API` function trips the
  gate with `abidiff rc=4` and a clear remediation paragraph
  pointing at the regeneration script.  Closes the libabigail half
  of original ABI-Infrastructure scope from epic #690 (now
  decomposed under #786).
- **Cross-facade test-parity audit + CI gate** (#785):
  `tests/test_wirelog_advanced.c` grows from 7 to 21 test
  functions, mirroring `tests/test_wirelog_easy.c` invariants
  through the public `wirelog_session_*` surface.  14 new
  advanced tests cover parse errors, num_workers explicit
  values, intern stability (twice -- before and after first
  step), snapshot relation filtering, repeated open/use/close
  ordering, recursive multi-round delta callbacks, and the full
  inline-compound + side-compound parity sweep (5 inline-compound
  binding patterns + 1 side-compound saturation).  Every
  easy-side test is either paired with a same-named advanced
  test or carries a `/* PARITY: ... */` block-comment on its
  declaration line naming the structural reason no advanced
  analogue exists (12 facade-only annotations covering opts
  struct, `*_sym` variadics, and `wirelog_easy_print_delta`;
  the #665 partial-conjunction regression tests are now paired on
  the advanced side via #825).
  New gate `scripts/ci/check-test-parity.py` (registered as
  `meson test --suite abi:test_parity`) enforces the rule
  per-test, not as a numeric ratio, so future additions on
  either side cannot silently regress parity.  Result at this
  commit: 15 paired + 14 annotated = 29 easy tests covered.
- **`scripts/ci/check-threading-doc.sh`** (#734): static gate
  registered as `meson test --suite abi:threading_doc` that counts
  `atomic_*` call sites in `wirelog/` production sources and asserts
  the count matches the number of audit-table rows in
  `docs/THREADING.md` §5.  Drift in either direction (atomic_* added
  in code without a doc row, or doc row removed without code change)
  fails the gate with a clear diagnostic pointing at the section to
  update.
- **Compile-only smoke test for `WIRELOG_DEPRECATED_SINCE`** (#782):
  `tests/standalone/test_standalone_wirelog_deprecated_macro.c`
  annotates a static probe function with
  `WIRELOG_DEPRECATED_SINCE(99, 99)` and references it from `main`,
  so the macro's cross-compiler expansion path
  (GCC/Clang `__attribute__((deprecated))`, MSVC
  `__declspec(deprecated)`, no-op for unknown compilers) is
  exercised by the build before any real public-API deprecation
  ships.  Registered as `meson test --suite abi:standalone_include_wirelog_deprecated_macro`;
  the call-site deprecation diagnostic is suppressed per-target via
  `cc.get_supported_arguments(['-Wno-deprecated-declarations',
  '/wd4996'])` so the test does not break `-Werror` CI.
- **Public-API attribute lint backstop** (#782):
  `scripts/ci/check-public-api-macro.py` (registered as
  `meson test --suite abi:public_api_macro`) bans
  `WIRELOG_PUBLIC` on the 8 prototype headers after the v0.40-cycle
  adoption sweep to `WIRELOG_API`.  `WIRELOG_PUBLIC` remains valid
  only inside `wirelog/wirelog-export.h` where the platform `#if`
  ladder defines it.  Sources its header list by importing
  `PUBLIC_HEADERS` from the sibling `scripts/ci/check-public-prefix.py`
  so the surface stays in sync without a second hand-maintained list.

### Changed

- **CI: enroll Linux arm64 (`ubuntu-24.04-arm`) GCC build in the PR
  gate matrix** (#787): mirrors the arm64 leg already present in
  `.github/workflows/ci-main.yml` so the abi suite (`abi_symbols`,
  `public_header_surface`, `public_prefix`,
  `public_doxygen_headers`, `public_api_macro`, `test_parity`,
  `threading_doc`, `version_sync`, `changelog_format`,
  `release_template`, and the standalone-include compile tests)
  exercises the second supported architecture on every PR before
  merge.  Same-source-of-truth allowlist
  (`abi/libwirelog-1.0.symbols`) is intentionally arch-agnostic --
  `gnu_symbol_visibility: 'hidden'` plus `WIRELOG_API`-only export
  keeps SIMD/NEON wrappers internal so the 53-symbol set is
  identical across x86_64 and arm64.  Also fixes a stale matrix
  comparison in `ci-main.yml:84-95` that compared against the
  unused literal `'arm64'` instead of the actual runner label
  `'ubuntu-24.04-arm'`, so the SIMD-capability verification block
  now runs on the arm64 leg as intended.  Required-check
  promotion is a follow-up repo-admin action (branch protection),
  not part of this PR; per #824, Linux arm64 v1.0 ABI coverage is the
  arch-agnostic `abi_symbols` gate while libabigail `.abi.json`
  enforcement remains Linux x86_64-only.
- **Adopt `WIRELOG_API` on every installed public prototype** (#782):
  76 occurrences of `WIRELOG_PUBLIC` across the 8 prototype headers
  (`wirelog.h`, `wirelog-types.h`, `wirelog-ir.h`, `wirelog-parser.h`,
  `wirelog-optimizer.h`, `wirelog-easy.h`, `wirelog-advanced.h`,
  `wirelog/io/io_adapter.h`) are renamed to `WIRELOG_API`.  The rename
  is binary-identical: `#define WIRELOG_API WIRELOG_PUBLIC` at
  `wirelog/wirelog-export.h:34` expands to the same platform-specific
  attribute (`__attribute__((visibility("default")))` on GCC/Clang,
  `__declspec(dllexport)` / `__declspec(dllimport)` on Windows,
  no-op on `WIRELOG_STATIC` or unknown compilers).  The 53-entry
  allowlist at `abi/libwirelog-1.0.symbols` continues to pass the
  `abi_symbols` gate unchanged.  `WIRELOG_PUBLIC` is retained as a
  backward-compat alias for downstream code that referenced it
  directly during the v0.40 cycle.  Closes the adoption tail of
  v0.40 epic #680 exit condition 6 (macros were "introduced" at the
  definition level in v0.40; this commit puts them in use).
- **Forward-looking documentation of the visibility attribute renamed
  to `WIRELOG_API`** (#782): `docs/SEMANTICS.md` visibility table,
  `meson.build` library() comment, `scripts/ci/check-abi-symbols.sh`
  docstring, and the previously-stale "adoption sweep tracked
  separately" note in `wirelog/wirelog-export.h:31-33` all updated to
  reflect the new canonical attribute name.  `AGENTS.md` gains a
  "Visibility Attribute" subsection mandating `WIRELOG_API` for new
  public-API declarations.  History in `CHANGELOG.md` and
  `docs/MIGRATION.md` is left untouched -- those sections describe
  v0.40 state and should not retroactively change.

### Deprecated

### Removed

### Fixed

- **Advanced-side #665 partial-conjunction parity** (#825):
  `tests/test_wirelog_advanced.c` now mirrors the two easy-facade
  #665 partial-conjunction regression tests under both default and
  multi-worker execution.  The easy-side parity annotations now point
  at the live paired advanced tests instead of a closed #785 follow-up,
  and `scripts/ci/check-test-parity.py` continues to pass.
- **Optimizer-equivalence conformance matrix** (#700):
  `tests/test_optimizer_equivalence.c` now exercises the 16 combinations
  of Magic Sets, SIP, Logic Fusion, and JPP over join, recursive, and
  aggregate programs, comparing result Z-sets against the all-enabled
  baseline.  Magic Sets remains outside the public `wirelog_opt_pass_t`
  enum for v0.43; the matrix names the internal `wl_*_apply` symbols
  directly.
- **Config-aware optimizer pipeline wiring** (#700):
  the CLI and easy facade now call the shared `wirelog_optimize()` path
  instead of invoking optimizer passes directly, so public optimizer config
  behavior is exercised by user-facing pipelines.
- **Observable aggregate-skip counters** (#700):
  Magic Sets, SIP, Logic Fusion, and JPP stats now expose
  `skipped_aggregate`, and aggregate IR trees are skipped instead of being
  rewritten by passes that cannot safely transform them yet.
- **Platform ABI advisory test registration** (#788, #681):
  the macOS advisory export check is registered only on Darwin hosts
  and the Windows advisory export check only on Windows hosts.  This
  keeps Linux arm64 runners that happen to have `pwsh` installed from
  spending the Meson test timeout launching a Windows-only PowerShell
  check that should never run on that platform.

### Performance

### Security

### Documentation

- **`docs/SEMANTICS.md` optimizer-equivalence conformance section** (#700):
  now records the implemented v0.43 conformance state: matrix harness
  present, CLI/easy facade wired through the config-aware optimizer facade,
  Subsumption treated as canonicalization outside the toggle axis, Magic Sets
  kept off the public pass enum, and aggregate-skip behavior observable
  across all four matrix passes.

- **README Performance section: clarify `--repeat 5` methodology** (#736):
  fixes the stale `repeat=1` label on line 63 (now reads `--repeat 5`
  medians) to match the correct description already present on lines
  88-90.  Adds a short note explaining that historic single-trial numbers
  from pre-`1e6af00` README revisions are not directly comparable to
  current 5-trial medians; the +26% CSPA W=1 delta (1.55s -> 1.95s) is
  a measurement-methodology change, not a runtime regression, and 1.95s
  is the honest baseline.  Closes #736.
- **`docs/SEMANTICS.md` recursive aggregation residue definition + v0.43
  slip** (#692): adds a new "Recursive aggregation residue (Status:
  Future)" section defining "residue" operationally as the count of
  `'not yet implemented'` markers and disabled conformance tests in
  `tests/test_recursive_agg*.c` that block CC-min, SSSP-max, and
  count-stratified programs from producing correct output at workers in
  {1, 4, 8, 16}.  Records the current state (harness disabled at
  `tests/meson.build:184-198, 2190-2194`; `col_op_reduce` IS wired
  (`WL_PLAN_OP_REDUCE` case at `eval.c:267-269`) but conformance cannot
  run because the harness is disabled; `col_op_reduce_weighted` built
  but NOT dispatched (no `WL_PLAN_OP_REDUCE_WEIGHTED:` case) in the
  recursive dispatch switch at `wirelog/columnar/eval.c:241-288`;
  count-stratified
  scope asymmetry) and the path to residue = 0: Phase 2B prerequisite
  (#735, #809/#810/#811) then v0.43 harness re-port.  Narrows the v0.42
  exit criterion to "non-agg recursion residue = 0" via Phase 2B;
  recursive aggregation residue = 0 slips to v0.43 per architect+critic
  synthesis on 2026-05-18.
- **Advisory TSan compile smoke for `-Dthreads=native`** (#708, #826):
  `.github/workflows/ci-pr.yml` gains a `tsan-native` job mirroring the
  existing `tsan` (posix) configuration through configure/compile only,
  but it no longer runs the full native C11 runtime suite under TSan.
  Issue #826 showed that instrumented workers created through
  `thrd_create` can crash GCC/libtsan itself with
  `ThreadSanitizer:DEADLYSIGNAL` / SEGV `0x18`, before wirelog
  synchronization can be diagnosed; suppressions and per-test skips would
  therefore be misleading.  `docs/THREADING.md` section 11 now makes
  `-Dthreads=posix` the only gating race-detection surface, records
  native/glibc as compile-only advisory coverage, and leaves runtime C11
  backend coverage to the ordinary non-TSan matrix.  Closes #826; refs
  #708.
- **`docs/SEMANTICS.md` cross-facade parity audit subsection**
  (#785, under epic #681): the existing "Cross-facade parity
  (Status: Current)" block gains a new sub-block recording the
  per-test paired-or-annotated rule, the lint backstop, and the
  intentional reverse-parity asymmetry (backend selection is
  advanced-only by design).  Future maintainers reading the
  semantic model now see why some `test_create_*` tests are
  one-sided.
- **`docs/THREADING.md`** (#734, under epic #681): new canonical
  document covering wirelog's threading model -- backend selection
  (C11 `<threads.h>` > Win32 > POSIX, with `-Dthreads=posix` forcing
  pthreads as required for TSan), the three-layer atomics surface
  (direct `<stdatomic.h>` on GCC/Clang, MSVC shim in
  `mem_ledger.h:24-86`, MSVC shim in `lockfree_queue.c:22-37`), a
  40-row atomics audit table (every `atomic_*` call site in
  `wirelog/` with file:line + memory order + per-row justification),
  the lock-free SPSC delta queue ordering contract, K-fusion's two
  thresholds (K≥2 plan emission via `WL_PLAN_OP_K_FUSION`, K≥4
  parallel runtime via `WL_KFUSION_MIN_PARALLEL_K`), the
  compound-arena epoch boundary contract anchored by the
  `sess->coordinator == NULL` gate (#579), and the signal-safety
  stance (WL_LOG NOT async-signal-safe; do not call wirelog from
  signal handlers).  `README.md` and `docs/SEMANTICS.md` gain
  cross-links.

## [0.40.0] - 2026-05-12

### Added

- **File-level Doxygen markers on every public header + CI gate**
  (#780, closes #680 exit condition): every entry in
  `wirelog_public_headers` (plus the standalone
  `install_headers('wirelog/io/io_adapter.h', ...)` call) now
  carries `@file` + `@brief` inside a `/** ... */` JavaDoc block
  placed between the SPDX C-comment and the include guard.  Eight
  headers gained both markers; `wirelog/wirelog-advanced.h` gained
  `@brief` (it already had `@file`).  A new gate
  `scripts/ci/check-public-doxygen-headers.py` (registered as
  `meson test --suite abi:public_doxygen_headers`) sources its
  header list from `parse_meson_sot` in
  `scripts/ci/check-public-header-surface.py` (the single SoT) and
  fails when any installed public header is missing either marker.
  Detection is regex-strict (`^\s*\*\s*@file\b`, `^\s*\*\s*@brief\b`)
  so per-parameter `@filename:` annotations in GTK-Doc-style function
  blocks do not satisfy the file-level check.
- **Release-process documentation + release-template CI gate** (#772):
  `docs/RELEASE_PROCESS.md` defines the canonical procedure for cutting
  a release tag, including the 9-section release-note template and the
  publication procedure (release PR, tag, GitHub Releases body, signed
  artefacts).  Two helper scripts land alongside:
  `scripts/release/extract-changelog-section.sh` (emits one CHANGELOG
  versioned section to stdout, used by `gh release create
  --notes-file`), and `scripts/ci/check-release-template.sh`
  (registered as `meson test --suite abi:release_template`) which
  diffs the published GitHub Releases body for the current tag against
  the corresponding CHANGELOG section.  The gate SKIPs outside
  tag-triggered CI context (PR builds, main branch) and enforces
  inside the release-tag.yml workflow (#749 B19).
- **CHANGELOG format CI gate** (#471):
  `scripts/ci/check-changelog-format.py` (registered as
  `meson test --suite abi:changelog_format`) asserts the
  `[Unreleased]` section follows the Keep-a-Changelog conventions
  documented in `CONTRIBUTING.md`: every bullet sits under one of
  the allowed categories (`Added`, `Changed`, `Deprecated`,
  `Removed`, `Fixed`, `Performance`, `Security`, `Documentation`)
  and carries at least one `#N` PR or issue reference.  Versioned
  section headers must use `## [X.Y.Z] - YYYY-MM-DD`.  CONTRIBUTING.md
  gains a "Changelog Conventions" section documenting the format,
  cutover procedure, and the freeze rule for `1.0`
  (cross-link with #747 B18).
- **ABI symbol allowlist gate (#733 K2)**: `meson test --suite abi:abi_symbols`
  diffs `nm -D --defined-only build/libwirelog.so | awk '$2=="T"'`
  against `abi/libwirelog-1.0.symbols`.  53 entries seeded from the
  current export set after `gnu_symbol_visibility: 'hidden'` lands.
  Any new public symbol must update the allowlist in the same PR;
  any accidental loss of an exported symbol fails the gate.  SKIPs
  cleanly on platforms without `libwirelog.so` (Windows / static-
  only).  Cross-link with #690 B3 (libabigail-based richer ABI
  manifest will sit alongside).

### Changed

- **libwirelog symbol-visibility default = hidden, SOVERSION=1**
  (#733 K1): the shared library now exports only `WIRELOG_PUBLIC`-
  annotated symbols (53 in v0.40 baseline).  Internal `wl_*` /
  `col_*` / `arr_*` / `eval_stack_*` symbols are no longer
  reachable through `libwirelog.so`'s dynamic-symbol table; the
  total exported `T`-class count drops from 348 to 53.  SONAME
  bumps from `libwirelog.so.0` to `libwirelog.so.1` via explicit
  `soversion: '1'`, decoupled from the pre-1.0 `project_version`
  so the 1.0 ABI commitment is fixed ahead of `1.0.0` release.
  macOS gains explicit `darwin_versions: ['1', '1.0.0']`.
  Source-incompatible for downstream code that relied on
  resolving internal symbols through `libwirelog.so` -- those
  consumers must rebuild against the public surface (or against
  the non-installed `libwirelog_static.a` which retains all
  symbols).
- **Public-API prototype annotation sweep** (#733 K0): the 7
  installed public headers (`wirelog.h`, `wirelog-types.h`,
  `wirelog-parser.h`, `wirelog-ir.h`, `wirelog-optimizer.h`,
  `wirelog-easy.h`, `wirelog-advanced.h`) plus
  `wirelog/io/io_adapter.h` ctx accessors gain `WIRELOG_PUBLIC`
  on every function prototype (72 prototypes total).  No
  source-level behaviour change before K1; load-bearing for the
  visibility flip in K1 (without it, every public function
  becomes hidden).

### Added

- **Standalone-include compile matrix for public headers** (#689):
  9 small `tests/standalone/test_standalone_<HEADER>.c` stubs, each
  including exactly one public installed header and a trivial
  `main()`, registered as `meson test --suite abi:standalone_include_*`.
  Failure = a public header has a hidden dependency on another
  header being included first (broken self-containment).  The
  GCC/Clang/MSVC compiler matrix is realised at the GitHub Actions
  level: each platform compiles every stub via its native CC.  Closes
  Blocker B2 of the v0.40 API audit and supplements the existing
  3-way SSoT verification at `scripts/ci/check-public-header-surface.py`.
- **CI lint backstop for public-API prefix conformance** (#761):
  `scripts/ci/check-public-prefix.py` (registered as
  `meson test --suite abi:public_prefix`) scans the 9 public
  installed headers for any `wl_*` / `WL_*` token that might
  leak through after the v0.40 API audit closes its sibling
  renames (#762 / #756 / #757 / #758 / #759 / #760).  An
  inline `ALLOW_LIST` documents the single intentional
  exception today (`struct wl_intern` tag forward-declared in
  `wirelog/wirelog.h` for the public `wirelog_intern_t`
  typedef per #760's design).  Closes the public-API prefix
  audit epic #755 by making the rule mechanically enforced
  rather than human-only.

### Changed

- **I/O adapter framework renamed and ABI bumped to 2u for v1.0
  prefix conformance** (#762): the entire `wirelog/io/io_adapter.h`
  surface (20 identifiers including `wl_io_*` symbols, `WL_IO_*`
  macros, the `"wl_io_plugin_entry"` dlsym key, and the
  `WL_IO_ABI_VERSION` macro) is renamed to `wirelog_io_*` /
  `WIRELOG_IO_*`, and `WIRELOG_IO_ABI_VERSION` is bumped from
  `1u` to `2u`.  The registration-time
  `adapter->abi_version != WIRELOG_IO_ABI_VERSION` check rejects
  v0.30.0 plugins loud (abi_version == 1u) with a clear
  `wirelog_io_last_error()` diagnostic.  Path-B plugins must
  rebuild and rename the exported entry symbol from
  `wl_io_plugin_entry` to `wirelog_io_plugin_entry`; otherwise
  the loader's `dlsym` lookup returns NULL.  Source- and binary-
  incompatible across the entire I/O adapter surface; pre-1.0
  ABI break per the v0.40 API audit decision.  Part of public-
  API prefix audit epic #755.
- **wl_easy facade renamed and moved for v1.0 prefix conformance**
  (#756): the `wl_easy` convenience facade is renamed across its
  entire surface from `wl_easy_*` / `WL_EASY_*` to
  `wirelog_easy_*` / `WIRELOG_EASY_*`.  The header and source
  files move from `wirelog/wl_easy.h` / `wirelog/wl_easy.c` to
  `wirelog/wirelog-easy.h` / `wirelog/wirelog-easy.c`; test files
  move accordingly.  `AGENTS.md` public-headers list and
  `meson.build` SSoT entries update with the move.  Source-
  incompatible across the entire facade; downstream consumers
  update `#include` paths plus a textual symbol rename.  Part of
  public-API prefix audit epic #755.
- **Export-attribute macro renamed for v1.0 prefix conformance**
  (#759): `wirelog/wirelog-export.h` no longer defines
  `WL_PUBLIC`; the new public name is `WIRELOG_PUBLIC`.
  `WIRELOG_API` continues as the alias and remains the recommended
  attribute name for new public-API declarations.  Source-
  incompatible for downstream code that referenced `WL_PUBLIC`
  directly; migrate via a textual rename to `WIRELOG_PUBLIC` (or
  switch to `WIRELOG_API`).  Part of public-API prefix audit
  epic #755.
- **String-fn enum constants renamed for v1.0 prefix conformance**
  (#757): the 12 `WL_STR_FN_*` enum constants in
  `wirelog/wirelog-types.h` (declaring the string-op kinds shipped
  in #444) are renamed to `WIRELOG_STR_FN_*`.  Source-incompatible
  for downstream consumers passing the constants to public string-
  op APIs; migrate via a textual rename.  Enum values and behaviour
  are unchanged.  Part of public-API prefix audit epic #755.
- **Public-API typedef renamed for v1.0 prefix conformance** (#760):
  the `wirelog/wirelog.h` umbrella header no longer exposes the
  internal-style `wl_intern_t` typedef name.  The new public name is
  `wirelog_intern_t`; `wirelog_program_get_intern()` returns
  `const wirelog_intern_t *`.  The internal header
  `wirelog/intern.h` keeps `typedef struct wl_intern wl_intern_t;`
  for in-tree callers (both names alias the same struct).  Two
  docstrings that previously named the internal `wl_dd_load_edb()`
  helper are rephrased to refer to it as the project's internal
  EDB-load helper.  Source-incompatible for downstream consumers
  using `wl_intern_t` after including only `wirelog/wirelog.h`;
  migrate via a textual rename to `wirelog_intern_t`.  Part of
  public-API prefix audit epic #755.
- **Callback typedefs renamed for v1.0 prefix conformance** (#758): the
  public-API callback typedefs `wl_on_delta_fn` and `wl_on_tuple_fn`
  in `wirelog/wirelog-types.h` are renamed to `wirelog_on_delta_fn` and
  `wirelog_on_tuple_fn` respectively, matching the `AGENTS.md:17-20`
  rule that public typedefs use the `wirelog_` prefix.  Source-
  incompatible for downstream consumers of the
  `wirelog_session_set_delta_cb` / `wirelog_session_snapshot` /
  `wirelog_easy_set_delta_cb` / `wirelog_easy_snapshot` parameter types.  Migrate
  via a textual rename; no signature change.  Tracked under
  `docs/MIGRATION.md` 0.30 -> 1.0 section.  Part of public-API prefix
  audit epic #755.

### Performance

- **CRDT W=8 -6.3% via leading-key cache in compact_runs heap** (PR #731): the K-way merge in `col_rel_compact_runs` now shadows the leading column with a stack-resident `int64_t lead_key[]` array parallel to the heap entries; the inner sift-down comparator short-circuits on column 0 instead of indirecting through `col_rel_row_cmp` for every comparison. CRDT W=8 5-rep median moves from 19.43s to 18.20s (-6.3%); first time `W=8 < W=1` on the dev box. No row-layout change, no sort-algorithm change, no public-header surface impact. Cross-workload (DOOP / CSPA / Polonius / Galen / DDISASM) tuple counts and gold relations preserved.

### Added

- **CRDT median-time perf gate** (PR #731): `tests/test_crdt_perf_gate.c` registered under `meson test --suite perf`. Drives the same Datalog source as `bench_flowlog --workload crdt` through the public `wirelog_*` API, asserts tuple count == 1,301,914 before any timing assertion, asserts coefficient of variation <= 3% (else SKIP), asserts median wall <= `WL_CRDT_PERF_GATE_TARGET_MS` (19,840 ms = baseline 18,890 ms x 1.05). Three-mode SKIP/SKIP/FAIL behaviour: SKIP by default; FAIL-loud under `WIRELOG_PERF_REQUIRE=1` when the cpufreq governor is not `performance` OR when the build is not `-Dwirelog_log_max_level=error` (per #731 follow-up commit). The escalator pattern lets dev hosts run `meson test --suite perf` cleanly while merge runners that opt into REQUIRE mode catch misconfiguration loud.
- **`bench/bench_crdt_workload.h`** (PR #731): the CRDT verification template is moved out of `bench_flowlog.c` into a shared header so the bench driver and the perf gate cannot drift on rule structure.
- **`wirelog/wirelog-advanced.h`** (#717, #703): New public header exposing the fine-grained `wirelog_session_*` API as the stable peer of `wirelog_easy`. Eight thin wrappers (`create` / `destroy` / `insert` / `remove` / `step` / `set_delta_cb` / `snapshot` / `make_compound`) over the internal session primitives. Backend selection through the `wirelog_backend_kind_t` enum (`DEFAULT` / `COLUMNAR`) — no vtable exposure. Inline `.dl` facts are seeded eagerly at `wirelog_session_create()` time, matching the wirelog_easy contract from #718. Internal `wl_session_*` and `wl_compute_backend_t` remain private.
- **CI guard** (#717): `scripts/ci/check-advanced-header.sh` (suite `abi`) fails when `wirelog/wirelog-advanced.h` includes any internal header.

### Fixed

- **wirelog_easy inline-fact materialization** (#718): `wirelog_easy_open` now seeds inline `.dl` facts into the columnar session at first lazy build, matching the CLI driver's order-of-operations. Previously the wirelog_easy facade dropped every static fact silently, so snapshots and IDB derivations re-evaluated against an empty EDB and returned no rows.

### Added

- **CLA-bot automation** (#702): `.github/workflows/cla.yml` runs the `contributor-assistant/github-action` on every PR. The bot blocks merging until every contributor has signed the wirelog CLA (recorded in `signatures/version1/cla.json`), protecting the LGPL-3.0-or-later + commercial dual-license model. Operational prerequisite documented inside the workflow file: a `CLA_SIGN_TOKEN` PAT secret with `contents:write` + `pull_requests:write` must be registered after merge.

### Documentation

- **README.md benchmark table refresh** (PR #731): full 16-workload portfolio re-measured at `--repeat 5` (5-trial medians) on the same dev host (cpufreq governor `schedutil`), replacing the previous `--repeat 1` snapshot. CRDT row reflects the new W=8 win (18.20s median, down from 19.43s). DOOP W=8 reflects unrelated post-baseline main-branch work (-51% vs prior table). Recipe block updated to `--repeat 5`. Numbers are descriptive; the regression gate in `meson test --suite perf` is the gated path.
- **`docs/SECURITY_MODEL.md`** (#701): new document recording the threat model, the mbedTLS-enabled build's license stack (Apache-2.0 + Apache-2.0 sub-dependencies on top of LGPL-3.0-or-later wirelog), and a good-faith export-control self-classification (ECCN 5D002.c.1 + License Exception ENC for `mbedTLS=enabled`; EAR99 for the default `disabled` build). Linked from README.md and from the `mbedTLS` option description in `meson_options.txt`.
- **README.md** (#717): replace the now-misleading `wl_session_*` / `wirelog/session.h` advisory with `wirelog_session_*` / `wirelog/wirelog-advanced.h`. The internal session header is explicitly called out as private.
- **`docs/SEMANTICS.md`** (#718): new document recording the engine's observable semantic-model decisions and the path toward 1.0 stabilization. First entry: inline `.dl` fact loading rules and the z-set host insert/remove model (status: Current).
- **`docs/SEMANTICS.md`** (#717): promote the cross-facade parity section from Future to Current now that the advanced surface ships.
- **`wirelog/wirelog.h`** (#717): expand the `wirelog_executor_t` docstring to clarify that it is the batch facade and to point at `wirelog_session_t` / `wirelog_easy_session_t` for incremental delta-callback workflows.

## [0.30.0] - 2026-05-07

### Added

- **I/O Adapter Framework** (#446): User-defined I/O adapters via runtime registry (`wirelog_io_register_adapter`). Public header `wirelog/io/io_adapter.h` with opaque context, ABI versioning (`WIRELOG_IO_ABI_VERSION=1`), and thread-safe registration API
- **Built-in CSV Adapter** (#455): CSV loading refactored into the adapter framework; backward-compatible `.input(filename=...)` dispatch
- **wirelog_easy Facade** (#445): Simplified high-level API (`wirelog-easy.h`) for common session workflows
- **String Operations** (#444): String-typed column functions (`strlen`, `cat`, `substr`, `contains`, `to_upper`, `to_lower`, `trim`, `str_replace`, `to_string`, `to_number`)
- **Path A Example** (#462): Standalone pcap adapter skeleton with CI compile-check against installed headers
- **C11 Threading Backend** (#494): Add C11 `<threads.h>` backend with auto-detection; POSIX/MSVC fallback preserved. `call_once` pattern for adapter registry initialization
- **Binary Size Gate** (#460): CI regression gate for `.text` section growth (5KB budget)
- **I/O Adapters User Guide** (#463): `docs/io-adapters.md` with Path A/B workflows, ABI policy, ownership rules, and thread-safety notes
- **Retraction Support** (#443): Fact retraction with recursive re-evaluation
- **Delta Query Examples**: Examples 08-12 demonstrating retraction, recursive update, time evolution, and snapshot-vs-delta patterns
- **Compound Terms**: Compound declaration parsing, inline compound declaration patterns, public side compound allocation, compound side support in flowlog benches, and daemon-style rotation examples
- **TDD Diagnostics**: Planner decision diagnostics, fallback decision stats, recursive TDD profiling counters, branch eligibility reports, and opt-in stratum profiling
- **Global-read TDD Infrastructure**: Guarded global-read recursive TDD path, candidate classification, rollback support, incremental shared-view refresh, and opt-in mixed child-plan execution

### Changed

- **Adaptive Worker Semantics**: Treat requested workers as an adaptive upper bound for K-fusion and TDD rather than a fixed allocation target
- **DOOP Benchmark Validation**: Validate DOOP benchmark output and refresh benchmark snapshots with W=N behavior
- **Recursive Evaluation**: Enable global-read TDD candidates by default while preserving guarded fallback paths
- **K-Fusion Scheduling**: Keep branch workers single-threaded, skip inactive branches, and use serial K-fusion below the dispatch threshold
- **CI Quality Gates**: Run CodeQL on main pushes and PRs, update artifact upload actions, and strengthen lint/code-quality checks

### Fixed

- **DOOP Worker Scaling** (#659): Current `main` no longer reproduces the workers-created-but-single-core DOOP path; W=8/W=16 runs preserve tuple/iteration parity and show active worker CPU
- **Worker Session Isolation**: Isolate filtered caches in worker sessions and add shared-view cleanup regression coverage
- **TDD Exchange Correctness**: Fix owner exchange delta registration and improve owner-mode fallback behavior for recursive workloads
- **MSVC Portability**: Add atomics shims, environment helper shims for tests, and portable atomic counter initialization
- **Join Robustness**: Fail closed on join output overflow and harden materialized join ownership

### Performance

- Parallelize eligible non-recursive relation plans, differential keyed joins, semijoin probing, and selected columnar join paths
- Optimize CRDT keyed probes and cap TDD width adaptively
- Reuse diff join left hash buckets and cache diff join match pairs
- Inline and specialize join, semijoin, arrangement, and owner-exchange key hashing helpers

## [0.21.0] - 2026-03-19

### Added

- **ARM NEON SIMD Optimization** (#231): Full SIMD vectorization for hash and key-match operations on ARM64 architectures with correctness tests (#234)
- **Memory Backpressure System** (#224): Thread-safe memory ledger tracking with JOIN budget enforcement and graceful backpressure mechanisms
- **Intra-join Backpressure** (#5): Soft EOVERFLOW truncation with memory-aware output limiting to prevent cardinality explosion

### Changed

- **Performance**: AVX2 SIMD hash/key-match now paired with ARM NEON equivalents for complete x86-64/ARM64 coverage (#231)
- **Stride-based Evaluation** (#237): Implemented in wirelog engine for improved iteration efficiency
- **Consolidation Fast-path** (#239): Optimized append for pre-sorted delta relations
- **Join Dispatch**: Inline scalar hash for kc<2 to eliminate function call overhead

### Fixed

- Handle missing right relation in JOIN by returning empty result
- Guard direct stdatomic.h includes for MSVC compatibility
- Propagate ENOMEM from col_rel_append_row at consolidation and delta-seeding sites
- Fixed col_rel_compact() right-sizing after deduplication

### Performance

- K-fusion parallel threshold to avoid small-K overhead
- Optimized row comparison via SIMD dispatcher (kway_merge)
- Per-worker arena isolation and delta_pool right-sizing for K-copy reduction

## [0.20.0] - 2026-02-28

### Added

- **CRC-32 Checksumming** (#145): Hardware-accelerated CRC-32 with Ethernet and Castagnoli variants via TDD
- **Hash Function** (#144): Built-in `hash()` function using xxHash3 with high-throughput performance
- **Bitwise Operators** (#72): Complete bitwise AND, OR, XOR, NOT support in parser and evaluator
- **Symbol Type** (#137): String column type support via symbol interning
- **CSV Output Directives** (#137): `.output(filename="...")` directive support for query result export
- **wirelog-cli** (#136): Restored CLI driver executable with enhanced CSV loading and directives integration

### Fixed

- Variable name resolution in expression serializer
- MSVC compilation compatibility (getcwd, atomics, C11 support)
- Cross-platform CRLF line ending normalization in CLI tests
- CSV loading for symbol/string columns
- LTO linker compatibility for CLI executable

## [0.11.0] - 2026-02-28 — Phase 1 Entry

### Added

- **ARM NEON SIMD Optimization** (#231): Full SIMD vectorization for hash and key-match operations on ARM64 architectures with correctness tests (#234)
- **Memory Backpressure System** (#224): Thread-safe memory ledger tracking with JOIN budget enforcement and graceful backpressure mechanisms
- **Intra-join Backpressure** (#5): Soft EOVERFLOW truncation with memory-aware output limiting to prevent cardinality explosion

### Changed

- **Performance**: AVX2 SIMD hash/key-match now paired with ARM NEON equivalents for complete x86-64/ARM64 coverage (#231)
- **Stride-based Evaluation** (#237): Implemented in wirelog engine for improved iteration efficiency
- **Consolidation Fast-path** (#239): Optimized append for pre-sorted delta relations
- **Join Dispatch**: Inline scalar hash for kc<2 to eliminate function call overhead

### Fixed

- Handle missing right relation in JOIN by returning empty result
- Guard direct stdatomic.h includes for MSVC compatibility
- Propagate ENOMEM from col_rel_append_row at consolidation and delta-seeding sites
- Fixed col_rel_compact() right-sizing after deduplication

### Performance

- K-fusion parallel threshold to avoid small-K overhead
- Optimized row comparison via SIMD dispatcher (kway_merge)
- Per-worker arena isolation and delta_pool right-sizing for K-copy reduction

## [0.11.0] - 2026-02-28 — Phase 1 Entry

Phase 1 begins. wirelog now supports string-typed columns via symbol interning,
external CSV data loading, and head arithmetic — the foundational capabilities
needed for real-world Datalog applications (DOOP, Polonius, network policy).

## [0.10.1] - 2026-02-28

### Added

- **Symbol interning** (`wl_intern_t`): bidirectional string-to-integer mapping
  for string-typed columns. The DD executor continues to operate on `Vec<i64>`;
  strings are interned at parse/load time and reverse-mapped on output. (#42)
- **`.input` CSV loading** (`wirelog_load_input_files()`): relations with
  `.input(filename="...", delimiter="...")` directives now load CSV data
  automatically during pipeline execution. (#18)
- **Head arithmetic**: `project_exprs` / `map_exprs` in the DD plan enable
  arithmetic expressions (e.g., `cost(x, c+1)`) in rule heads. (#20)
- **CC and SSSP benchmark workloads**: Connected Components and Single-Source
  Shortest Path programs added to the benchmark suite. (#27)
- **Benchmark framework**: timing utilities, graph data generator, FlowLog
  benchmark driver, and `meson -Dbench=true` build option.
- **CodeQL CI**: GitHub Advanced Security workflow with security-and-quality
  query suite.

### Fixed

- Recursive aggregation (`min`/`max`) not propagating across DD iterations. (#21)
- FFI null-guard checks flagged by CodeQL.
- Version macros in `wirelog.h` now match the project version
  (`WIRELOG_VERSION_MINOR` corrected from 1 to 10).

### Changed

- Rust executor minimized to DD-essential surface only; non-critical Rust code
  removed.
- Architecture docs updated for DD integration and version numbering.

## [0.10.0] - 2026-01-15

Initial Phase 0 release: parser, IR, optimizer, Differential Dataflow executor
via Rust FFI, CLI driver with inline-fact evaluation pipeline.
