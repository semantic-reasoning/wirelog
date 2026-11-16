# JOIN domain qualification for #2371

This is an evidence inventory for ordinary and materialized JOIN. It records
what the implementation bundle actually exercises; it does not close #2371,
#2300 or any lifecycle/accounting sibling. The current INLINE increment starts from main
`3bdf3c458bc85b91345f10d900c3fc0d94de7eea`, including merged #2377 and the
latest #2301 source changes. It changes ordinary JOIN source admission through
an existing bounded adapter; no price, quantum, public ABI or layout changes.
The evaluation quantum remains256.

The previous public-history/materialized-memory bundle used baseline
`8266d893`. Its original test commits `d9f7775a`/`03a863a3` were merged as
`bc32ba90`/`84ae1dcf`, followed by ledger `3bdf3c45`. That earlier bundle did
not change production C. The new ordinary INLINE admission is `8c5a28dc`, with
consuming/retirement tests `f7e234dc`. Use the reviewed PR HEAD/configuration
for subsequent certificates; earlier green runs and experimental Native/Source
snapshots are not current route certificates.

## Finite scenario inventory

`evaluation_join_domains_plain` and `evaluation_join_domains_batch` each run
20 independent histories: four predicate shapes, keyed/product JOIN, and
healthy/CANCEL/WORK variants for each filtered shape (the unfiltered shape has
only its healthy variant). These are public parser/compiler-selected plans and
real `wl_session_step` calls, with no rewritten plan or synthetic owner.

| Shape | Actual generated condition | Positive destination evidence | Reference and limit |
|---|---|---|---|
| kind0 | Numeric constant argument `b(k,y,1)` or product `b(j,y,1)` | RHS evaluation phase44; private hash insertion phase28 for keyed nonempty selected rows | Literal accepted RHS values2/4, later6; rejected flag0; numeric RHS failure expressions are covered separately |
| kind1 | STRING constant argument `b(k,y,"keep")` or product equivalent | Actual scalar STRING RHS equality with session intern table, phase44 and keyed private hash | Literal accepted strings `keep`/`also`, later `next`; rejected flag `reject` |
| kind2 | Numeric constant RHS flag plus post-JOIN `strlen(y)>2` | Positive composed scalar FILTER on the real generated projected output | Only length4 values reach the post-FILTER; the short `x` row already fails its RHS flag. This is not independent post-FILTER rejection coverage |
| kind3 | Unfiltered RHS over the accepted data only | Plain keyed phase47 proves persistent row insertion; configured keyed batch requires real pipeline PRODUCE | Equivalent result control with different input data; no new persistent-reuse or identical-input cache-equivalence claim |

Every history checks a relation-qualified independent fixture reference,
projected output width and nonempty physical INT64/STRING column types, exact
signed callback differences, no duplicate events and an unchanged repeated
public call. The sequence is initial empty output, filtered all-rejected RHS
(where applicable), mixed accepted/rejected RHS, RHS append, RHS deletion and
left append. Duplicate RHS input is deleted with its actual multiplicity;
publication is checked as a set at each stage. Both controlled and uncontrolled
successful results must independently match the fixture reference; the raw
result is not used to compute the expected answer.

The initial registry assertion deliberately permits missing OR empty. After the
first evaluation the actual RHS registry relation must be present-empty. This
does not certify a separately selected naturally absent registry route.
Phase7 records only the engine's `used_delta` boolean. It is not a source
identity, FORCE_DELTA/forced-empty or full selector-matrix certificate. The
phase47 diagnostic is also used by empty-filter recipe validation; filtered
cases' printed counts must not be interpreted as persistent indexing. Only the
unfiltered keyed/plain assertion excludes that alternate producer.

CANCEL/WORK histories stop at RHS row1 after two genuine evaluations, following
an earlier nonempty public output. They assert the exact stop result, old
public-head identity and content, retained pending input, no early signed
events, nonzero performed work and zero cleanup error. A genuine control reset
retries the same session and pending input. The WORK injection asserts a real
`charge_attempt(..., UINT64_MAX)` refusal; it is not an independently derived
primitive exact/one-short boundary. Final governor reservations must be zero
after both sessions, control, plan and program-owned intern bindings are freed.

## Actual materialized selection and memory pressure

`evaluation_materialized_body` retains the natural compiler K-fusion lineage,
whole-SCC selection and authorized preserved-body identity. Its owned child
JOINs have no primitive materialized flags. This accepted equivalent route is
not a controlled materialization-cache hit.

The existing plain/batch, strict/output-policy and CANCEL/WORK/ended-generation
modes remain registered. Two new modes add actual enforcing-governor admission:

| Mode | Reached boundary | Required result and recovery |
|---|---|---|
| `batch-memory-first` | Selected body's JOIN preparation has committed arrays, before initial publication | Genuine peer reservation consumes available governed capacity; producer must select ENOSPC and increment its denial counter; no published image/events |
| `batch-memory-successor` | Same authentic preparation after the first9-row public image and new pending input | Same denial while previous `path`9 and `plain`5 contents remain visible, with no new events and pending input intact |

Peer bytes are derived from the actual governor's usable and reserved totals.
The token stays owned until the first producer error hook observes the actual
ENOSPC/denial. That hook releases peer capacity before mandatory cleanup, and
the token must reach RELEASED. The completed report preserves ENOSPC, positive
work and zero cleanup error. Reset/retry uses the same session/input and then
checks the existing independent literal `path`9→12→6 and `plain`5→6→5 histories,
exact signed events, unchanged repeated calls and final zero governor bytes.

The raw session shares the successful reference fixture but does not receive
the same injected pressure. These modes prove controlled admission/recovery;
they do not claim equal-pressure raw denial or an allocator ENOMEM result.
Existing `batch-error-ended` separately qualifies the injected allocation error
and first-error-versus-ended-Original behavior.

## Ordinary JOIN consuming INLINE sources

The previous direct `a(i,pair(x,y)),b(x,z)` controlled route returned ENOTSUP
although uncontrolled execution returned `(1,7)` and `(2,8)`. Ordinary JOIN
now calls the existing owned INLINE-admission reader for both input sides.
The shared helper admits INLINE only for opcode JOIN; SEMIJOIN/ANTIJOIN retain
zero admission flags. The reader validates the arity map in charged chunks
of at most eight entries under its original descriptor/owner readers. Existing
retained BEGIN/END, physical metadata and gather/retirement paths are reused.
No raw fallback, new representation or widened support gate is introduced.

`evaluation_compound_sources` replaces that exact ordinary refusal oracle with
literal success on observed STEP, unobserved STEP and evaluating SNAPSHOT. It
checks actual fused JOIN keys `col1`/`col0`, projection0/4, direct INLINE source
publication, source identity/map retention and final governor zero.

`evaluation_join_inline_observed`, `_plain` (unobserved STEP) and `_snapshot`
each run four generated programs with healthy/CANCEL/WORK variants:36 histories.
Batch/output environment overrides are cleared for these ordinary routes.

| Program | Actual INLINE input and physical mapping | Predicate/result evidence |
|---|---|---|
| kind0 | Left compound occupies lanes0/1, key is col2, value col3; scalar RHS key col0 | Numeric RHS constant flag plus a genuine joined `y=0` rejected by `x<y` |
| kind1 | Right compound occupies lanes0/1, key col2, value col3, flag col4; scalar left key col0 | RHS flag after compound lanes, accepted/rejected rows and post-JOIN comparison |
| kind2 | Both sources INLINE; left STRING/FLOAT prefix before key col2; right STRING value inside its compound | Typed reader snapshots; accepted flag and matching short string reaches JOIN gather, then fails `strlen(y)>2` |
| kind3 | Same typed sources, actual zero-key product | Independent product reference and non-vacuous STRING post-FILTER rejection |

The STRING/FLOAT prefix's physical lane types are observed. This is not FLOAT
result-arithmetic or timestamp qualification. Expected values come from explicit
logical fixture facts and key selection, independent of raw output and the
production expression evaluator. Source kinds/readers and current physical
columns are read from the authentic owned JOIN context. Phase17 decodes its
actual row; phase18 witnesses matching row gather. The predicate-false row has
an accepted RHS flag and is witnessed at gather before its absence is checked
in the final literal result. This resolves rejection on these new INLINE
compositions; the old kind2 positive-only scalar history above remains limited.

Each history goes from empty RHS to populated output, then appends input,
removes an actual single RHS fact and repeats unchanged calls. Left duplicate
input is intentional; this is not duplicate-RHS deletion coverage. Observed
STEP checks exact signed set differences; SNAPSHOT checks complete tuple images.
CANCEL/WORK is injected at row gather with both source records currently HELD
and the selected side's current descriptor INLINE. Accumulated history bits
cannot satisfy that stop condition. The prior nonempty head/image, pending
input and source snapshots/arity maps must remain unchanged, with no early
events/tuples, exact report cause and zero cleanup error. Genuine control reset
retries the same session/input; successful destruction leaves governor zero.

Eight separately labelled constructed internal `evaluation_join` cases exercise
both sides: valid ordinary INLINE admission, malformed-map EINVAL, and unchanged
SEMI/ANTI ENOTSUP. They are admission-boundary evidence, not generated selectors.
The existing SIDE refusals remain. This increment does not qualify configured
strict/batch or output/memory-policy INLINE products, materialized INLINE,
complete selectors, timestamp propagation, cache pins/reuse or full Source/carry.

## Reused evidence and remaining acceptance

| Obligation | Existing evidence reused | Still outstanding for complete #2371 acceptance |
|---|---|---|
| Ordinary key/product/projection/collision and accepted RHS expressions | `evaluation_join`, generated large/filter/collision cases in `evaluation_serial`, scalar recovery and the new public histories | Complete accepted expression/type/error/representation crossproducts beyond the specified scalar and INLINE predicates |
| Full/delta/empty selectors and signed future changes | Existing direct selector fixtures, serial append dispatches and the new literal public histories | Bind each newly enabled route to actual source identity and all required selector partitions; phase7 alone is insufficient |
| Persistent full unfiltered index and filtered/private fallback | `test_controlled_persistent_join`, persistent stop/refusal matrices in `test_evaluation_serial.c`; new literal public build/private witnesses | Complete consuming predicate/representation/policy and same-input cache equivalence; new tests do not claim reuse from output equality |
| Pins and deferred rebuild/eviction | `join_right_filter_cleanup` managed cache/pinned fallback/admission tests; existing strict internal-cache fixtures | Missing authentic consuming intersections, with internal pin setup labelled as such; no new public primary-cache producer is invented |
| Materialized policy/memory and future calls | All12 `evaluation_materialized_body_*` modes, host strict/policy/lifecycle suites | Remaining empty/wide/metadata/selector/cache-equivalence and containing bounds, not a primitive controlled-cache-hit claim |
| Batch/pipeline, recursive/frontier and representations | Existing JOIN host suites and natural recursive materialized histories | Each accepted operator-by-representation and RUNNING/FINISH intersection, including required signed timestamps/weights; new callback sign checks are not full timestamp qualification |
| Ordinary INLINE representation | Direct public fused-projection regression;36 generated histories with real held-reader retry;8 constructed admission boundaries | Remaining policy/selector/cache/materialized/metadata and other operator representation intersections |
| Reached prepared ownership and disposal | Merged #2364, existing recovery/lifecycle fixtures | Remaining #2367/#2368 local funding/caller/ACK and MSVC-C11 layout qualification on the actual routes |
| RUNNING/FINISH and containing accounting | Existing components and local interval notes | Reached #2369 adapters and #2370 full entry/return, Original/SOURCE identity and correlated Native/Source carry certificates |

#2299 expression machinery is reused. #2301 owns representation access, while
this issue still owns the consuming JOIN intersections it enables. These are
precise handoffs, not a requirement to wait for unrelated sibling domains.
Required unavailable platform/Source evidence remains outstanding. There is no
new raw fallback, supported-domain refusal, language/API semantics, quantum
increase, cap increase or replacement index project in this bundle.

## Reproduction and validation boundary

Use a uv virtual environment as required by AGENTS.md. For an environment with
Meson/Ninja already available to that environment:

```sh
uv venv .venv
. .venv/bin/activate
CC=clang python -m mesonbuild.mesonmain setup build -DmbedTLS=disabled -Db_lto=false
python -m mesonbuild.mesonmain compile -C build
python -m mesonbuild.mesonmain test -C build --no-rebuild --print-errorlogs \
  --suite evaluation_join_hosts
CC=clang python -m mesonbuild.mesonmain setup build-asan -DmbedTLS=disabled \
  -Dexamples=false -Db_lto=false -Db_sanitize=address,undefined
python -m mesonbuild.mesonmain compile -C build-asan \
  test_evaluation_join_inline test_evaluation_join test_evaluation_compound_sources \
  test_evaluation_join_domains test_evaluation_materialized_body \
  test_relation_scalar_source_end test_evaluation_facts_sources \
  test_evaluation_empty_edb_sources
python -m mesonbuild.mesonmain test -C build-asan --no-rebuild --print-errorlogs \
  'evaluation_join_inline_*' evaluation_join evaluation_compound_sources \
  'evaluation_join_domains_*' 'evaluation_materialized_body_*' \
  relation_scalar_source_end evaluation_facts_sources evaluation_empty_edb_sources
```

The preceding #2377 snapshot had Clang23 JOIN-host101/101, ASan/UBSan16/16 and
GCC16 focused15/15. The INLINE increment (native LTO disabled) has Clang23 JOIN-host104/104 plus5
facts/compound/empty/scalar-END/serial tests, ASan/UBSan22/22 and GCC16 focused19/19,
plus formatting/diff checks.
These are the completed focused results before the documentation-only ledger
commit. The final PR body records the exact published HEAD and final broader
run; do not infer a final-SHA full-suite result from an earlier in-flight run.
MSVC and its conditional C11 actor profile have not been run in this Linux
workspace. Passing Linux tests do not close those acceptance items or #2371.
