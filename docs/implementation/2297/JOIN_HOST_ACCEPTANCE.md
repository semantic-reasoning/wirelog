# Controlled JOIN host acceptance (#2297)

This record covers ordinary serial JOIN batching and its FILTER/MAP/probe
consumers through the owning session. It extends the private SOURCE and saved
attempt completion foundation delivered by #2341. It does not close #2298
(LFTJ), #2293 (serial umbrella), or worker/global allowance work in #1822.

## Ownership and accounting

Every controlled producer and consumer retains the actual semantic control and
the generation begun by its session owner. Internal suspension, capacity
exchange, sink acknowledgement and retry do not begin a replacement semantic
attempt. A stale saved generation cannot charge or finish its successor.
Private financial controls pay engine SOURCE work; they confer no permission to
mutate a native frame, receipt, relation or transaction.

The admission-owned BANK constructs financial controls before their SOURCE
body. Constructor callbacks perform only financial creation/attachment/begin.
Lexical scope stores session/admission/lane provenance, never a borrowed BANK
context or native permit. Actual SOURCE phase settlement checks the genuine
seven-field report against the saved ID and charged sum. The common BANK
finisher performs a real zero checkpoint before FINISH without changing total
consumption. The shared quantum remains 256; fees, processed work and unused
admission require separate bounds.

## Required destinations

| Rows | Supported predicate and actual destination | Evidence |
| --- | --- | --- |
| E08/F01/F03 | Configured acyclic scalar JOIN with eligible FILTER/MAP/FULL-probe successors: bounded prepared producer and controlled relation sink | `evaluation_join_hosts` domains/recovery/failures/funding |
| E09/F02a/F02b | Product, right predicate, delta-right or ineligible pipeline: bounded ordinary JOIN retaining selector and policy processing | Host domain/recovery and strict refusal fixtures |
| F02c | Missing arrangement: owned private index; row does not fit: non-strict bounded ordinary alternative, strict policy refusal | Strict host fixtures, including empty and protected cache cases |
| F02d | A materialized INPUT supplied by its real transaction: bound private relation source, not a public same-name relation | Lifecycle successors, snapshots, appends and strict private-right fixtures |
| C04 | Local output cap and strict mode compose with sink commit/ACK and native retirement | Policy tests and private-cap transaction test |

`op->materialized`/CSE algorithms, differential algorithm expansion, additional
LFTJ hosting and asynchronous worker transport retain their separate issue
ownership. Existing qualified LFTJ row-local MAP behavior remains intact.

## Policy and publication

Zero local output limit disables the cap; a committed extent at or above the
nonzero limit yields EOVERFLOW. A consumed FILTER/MAP/probe pipeline counts its
sink rows. A separately executed FILTER does not subtract from the preceding
JOIN's count. Successful sink ACK advances the receipt generation exactly once
before policy failure is returned. Strict mode executes intrinsically eligible
keyed FULL-input producers, validates physical keys/cache protection and row
fit, and preserves legitimate strict refusals for ineligible configurations.

General observed transactions bind each child to its actual private image and
stable generation. Precommit failure publishes neither heads nor delta events.
After cutoff, a fully prepaid callback batch completes exactly once. Snapshot
uses its actual saved semantic attempt for outer completion even if a callback
ends Original or begins a successor. A successor remains uncharged and active
until its own owner ends it.

## Native error and destruction

A native error retains its real frame/context, creator thread, source binding,
receipt key, producer phase and cleanup owner. Financial refusal never creates
native authority. Recovery repeats the actual ownership/provenance checks before
mutation and drains the real receipt before clearing the cursor or completing
Original. A foreign-thread STEP returns EBUSY without changing that ownership.
An active Original successor also prevents destruction; the owning slot remains
unchanged. Bootstrap failure without a native frame retires only its real
financial grant and preserves the original execution error.

## Public SOURCE admission

Configured single-worker acyclic sessions with at most32 strata pay actual
public admission, state inspection and bounded static predicates through their
admission-owned BANK. The flag selects financing only. Actual Native ownership
and eligibility are checked at execution. Static read outputs are initialized
and consumed only after success. Mutation uses explicit DIRECT mode, while a
failed SOURCE read returns its error without a DIRECT fallback. A test requests
a genuinely invalid kernel action immediately before the unpublished fresh
shape read: no attempt/head/callback is published, then the same session retries
successfully after the fault is removed.

The MAX32 negative-stratum fixtures check exact STEP and Snapshot content,
once-only callback delivery, unchanged Original reports on invalid admission,
real controller-identity exhaustion and complete destruction. Destruction drains
finite existing cleanup cursors instead of imposing a sixteen-call cutoff.

## Regression evidence and reproduction

The following are assertions in executable fixtures, not empirical work-cost
formulas. `evaluation_join_hosts` checks high fanout, no-output candidates,
capacity/resume, real producer/consumer receipts, failure and funding boundaries.
`evaluation_join_probes` checks actual FULL source identity and literal results.
`evaluation_join_host_strict` checks eligible execution, genuine strict refusals,
key/type ordering, missing arrangements, oversized rows and protected caches.
`evaluation_join_host_policies` checks cap boundaries and cancellation around ACK.

`evaluation_join_host_lifecycle_private-cap` observes28 intermediate and35 output
rows under cap32, requires EOVERFLOW with no published heads/events, then raises
the cap and checks exact28/35 content and unique events against raw execution.
Lifecycle native fixtures check owner-thread rejection, receipt ACK, frozen
Original reports and successor-preserving destruction. Snapshot fixtures deliver
289 unique rows under callback END/rebind and actual final-identity exhaustion.
The admission fixtures exercise32 genuine compiler strata and the real failed
read/retry path. Real successor histories also witness multiple completed64-byte
metadata reset chunks; Native resource disposal precedes all such clearing.

Run these suites in the repository's uv virtual environment after Meson setup:

```sh
python -m mesonbuild.mesonmain test -C build --print-errorlogs \
  --suite evaluation_join_hosts
python -m mesonbuild.mesonmain test -C build --suite abi --print-errorlogs
python -m mesonbuild.mesonmain test -C build --print-errorlogs
```

Use separate address+undefined and POSIX thread-sanitizer builds for the same
host suites. Exact final commit identities, executed commands and outcomes
belong to the PR validation record; hosted MSVC results are separate from
Linux execution. Fees, processed work and unused admission retain their separate
source bounds in [MEMORY.md](../../MEMORY.md); passing fixtures alone cannot
certify a whole SOURCE interval.
