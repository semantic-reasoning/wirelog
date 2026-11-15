# JOIN domain qualification for #2371

This is an evidence inventory for ordinary and materialized JOIN. It records
what the implementation bundle actually exercises; it does not close #2371,
#2300 or any lifecycle/accounting sibling. The implementation baseline is main
`8266d8939f7aa013c10725b416a1a880f84dcb4a`, which includes merged PR #2364 and
its Windows continuation. The earlier handoff's failing Linux totals and
experimental Native/Source snapshots are historical, not current evidence.

The new test commits are `d9f7775a` (public JOIN histories) and `03a863a3`
(materialized memory admission). Their production C code is identical to the
baseline. Use the reviewed PR HEAD and configuration when binding subsequent
certificates; neither a historical green run nor these test commits qualify a
missing caller interval. The evaluation quantum remains256.

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

## Reused evidence and remaining acceptance

| Obligation | Existing evidence reused | Still outstanding for complete #2371 acceptance |
|---|---|---|
| Ordinary key/product/projection/collision and accepted RHS expressions | `evaluation_join`, generated large/filter/collision cases in `evaluation_serial`, scalar recovery and the new public histories | Complete accepted expression/type/error/representation crossproducts; post-FILTER rejection beyond the positive case above |
| Full/delta/empty selectors and signed future changes | Existing direct selector fixtures, serial append dispatches and the new literal public histories | Bind each newly enabled route to actual source identity and all required selector partitions; phase7 alone is insufficient |
| Persistent full unfiltered index and filtered/private fallback | `test_controlled_persistent_join`, persistent stop/refusal matrices in `test_evaluation_serial.c`; new literal public build/private witnesses | Complete consuming predicate/representation/policy and same-input cache equivalence; new tests do not claim reuse from output equality |
| Pins and deferred rebuild/eviction | `join_right_filter_cleanup` managed cache/pinned fallback/admission tests; existing strict internal-cache fixtures | Missing authentic consuming intersections, with internal pin setup labelled as such; no new public primary-cache producer is invented |
| Materialized policy/memory and future calls | All12 `evaluation_materialized_body_*` modes, host strict/policy/lifecycle suites | Remaining empty/wide/metadata/selector/cache-equivalence and containing bounds, not a primitive controlled-cache-hit claim |
| Batch/pipeline, recursive/frontier and representations | Existing JOIN host suites and natural recursive materialized histories | Each accepted operator-by-representation and RUNNING/FINISH intersection, including required signed timestamps/weights; new callback sign checks are not full timestamp qualification |
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
CC=clang meson setup build -DmbedTLS=disabled
meson compile -C build
meson test -C build --no-rebuild --print-errorlogs --suite evaluation_join_hosts
CC=clang meson setup build-asan -DmbedTLS=disabled -Dexamples=false \
  -Db_lto=false -Db_sanitize=address,undefined
meson compile -C build-asan test_evaluation_join_domains \
  test_evaluation_materialized_body test_evaluation_join_host_policies \
  test_evaluation_join_host_strict
meson test -C build-asan --no-rebuild --print-errorlogs \
  'evaluation_join_domains_*' 'evaluation_materialized_body_*' \
  evaluation_join_host_policies evaluation_join_host_strict
```

The integrated test snapshot was checked with Clang23 native JOIN-host101/101,
ASan/UBSan affected16/16 and GCC16 focused15/15, plus formatting/diff checks.
These are the completed focused results before the documentation-only ledger
commit. The final PR body records the exact published HEAD and final broader
run; do not infer a final-SHA full-suite result from an earlier in-flight run.
MSVC and its conditional C11 actor profile have not been run in this Linux
workspace. Passing Linux tests do not close those acceptance items or #2371.
