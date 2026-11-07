# Serial session LFTJ acceptance (#2298)

The supported nonrecursive, single-worker serial session route uses the real
compiler-selected LFTJ child, including default batch-off STEP and Snapshot,
plain and observed delivery. O11/F06/F06a are qualified for this route. The
existing at-most-32-strata controlled admission, numeric/materialized source
restrictions and protected differential/TDD/worker guards remain explicit.
This record does not qualify worker scheduling, new public APIs or the broader
product/recursive/materialized work owned by other issues.

## Reachability and policy

The compiler and genuine public session construct the owner and current child;
tests do not seed an opcode or a positive authorization flag. Structural
classification reads operator enums before an attempt. The authenticated
financial route uses the existing BANK and saved Original checks; it does not
select unlimited raw execution when the JOIN batch option is off.

`evaluation_lftj_hosts_domains`, `admission`, `microsteps` and `policy` cover
natural plans, empty/nonempty sources, nonleading and duplicate keys, accepted
INT64/FLOAT lanes, wide 75-column output, maximum64 input relations and invalid
metadata. The cap is on input count, not total output width. Actual repeated
LFTJ and CONCAT plans preserve the earlier owned stack result across later
runtime admission. Strict batch policy refuses at actual operator execution;
nonstrict fallback records its reason/count exactly once at each real reached
operator, including first-warning logging and retry. LFTJ output is not
incorrectly charged against the ordinary JOIN cap; a following JOIN still
honors its own cap.

## Bounded preparation and optional cache

The controlled destination always prepares an owned per-attempt source copy,
bounded copy/merge-sort/seek traversal and private output. Persistent sorted
arrangement reuse is optional and is replaced here by that qualified bounded
rebuild. Full materialized source selection, relation identities, types,
set/duplicate behavior and final signed successor effects match raw execution;
this does not claim a forced differential selector where the real route uses
full recomputation. The raw reference may warm or rebuild its arrangement
cache. `test_successors` and `qualification_cache` prove actual current source
generation/snapshot matches when acquiring a raw cached arrangement, valid
row/column shape and a genuine pin followed by zero pins. Controlled sessions
retain zero sorted-cache entries. Historical unused raw entries may remain;
requiring every historical slot to have the newest generation would invent a
cache contract.

The expanded scalar source inventory, actual native fronts and narrow
library/allocation/callback boundaries are in [LFTJ_SOURCE_ACCOUNTING.md](LFTJ_SOURCE_ACCOUNTING.md)
and [LFTJ_NATIVE_CLEANUP.md](LFTJ_NATIVE_CLEANUP.md). Their complete source
uppers are separate from logical tariffs, future retirement shares and
processed lower bounds. Quantum256 is unchanged. Retained child/receipt
ownership, readers, resource-free descriptor advancement, ACK chunking and
writer retirement are covered through genuine original-ended cleanup and a
separate cleanup financier, including active successor protection. No
successor pays for the old attempt.

## Finite rejected traversal and aggregate effects

`evaluation_lftj_hosts_qualification:test_lftj_nonoutput_seek` has40 histories:
8 batch/plain-observed/STEP-Snapshot configurations, each with a full empty
probe, fresh finite C and C+1, cancellation, and an intersecting singleton
positive. Disjoint nonempty sources `[1]`, `[2]`, `[3]` reach actual first-pass
SEEK with maximum3, index0, lo0/hi1, key1 and zero emitted rows. C is the
explicitly empirical host prefix measured separately per configuration, never
a source upper or a searched budget. The selected comparison tariff1 is
independently specified: C refuses it; C+1 performs exactly that comparison
before a later positive output allocation admission refuses. Both failures
have no output writer, publication or event. Fully funded empty success still
passes output creation and a second scan, with zero product emissions. The
same plan over `[3]` sources produces literal singleton3.

`test_lftj_aggregate_successors` runs16 actual LFTJ-to-REDUCE histories across
batch mode, observation, STEP/Snapshot and precommit CANCEL/WORK. Independent
COUNT literals are `{(1,8),(2,1)}` then removal gives `{(1,4),(2,1)}` and append
`{(1,4),(2,2)}`. Content, Arrow/scalar INT64 schemas, raw references and exact
signed -old/+new changes are checked. Dirty failure retains the previous head
identity/content and produces no events; reset and raised-limit retry delivers
the changed group exactly once, and a future clean call does not replay it.
`evaluation_reduce_batch_owner` separately qualifies authentic composed batch
REDUCE permission, all malformed owner/current-op/thread/status and protected
mode refusals, genuine ended/successor IDs and the actual first192 payment
boundaries. Existing standalone/noncomposed batch refusal and primitive exact
191/192,128/192,383/384 oracles remain independent.

## Publication, reports and resources

Before cutoff, `recovery`, `failures`, `native`, `metadata-native`, `microsteps`
and `policy` coordinate real COPY/sort/seek/output/cleanup failures. They check
literal prior heads and source/schema/snapshot images, all seven Original
report fields, no premature events, retry and future calls. Peer governor
credit and checked destruction distinguish retained prepared host storage from
leaked child resources; teardown returns reserved credit to zero. Observer or
Snapshot delivery storage may legitimately retire during a later dirty call,
so an earlier public absolute credit count is not a rollback baseline.

`test_lftj_postcut_delivery` adds8 real callback-time histories. At the first
callback the whole committed10-row head is already visible. Requesting CANCEL
sets a late unlatched flag: public success and report stop NONE remain valid.
An explicit UINT64_MAX charge demonstrates sticky WORK propagation only;
public/report result is WORK with execution/cleanup statuses0, while committed
head and all10 callbacks/Snapshot tuples still complete. Neither callback-time
case rolls back or truncates committed delivery. A future/reset call preserves
content and does not replay events; it may create a fresh successful Original.
UINT64_MAX and elapsed watchdogs are not finite tariff/source evidence.

## Scope and validation

The serial LFTJ predicates owned by #2298 are complete; ordinary JOIN predicates
remain recorded independently under #2297. G4 as a broader group is not a claim
that unrelated operator intersections, worker control (#1822), installed API
qualification (#1823), product qualification (#1824) or the entire umbrella
have completed. Validation results and platform limits accompany the closing
PR. Windows/MSVC cannot be executed locally in this environment; its CI result
must be read as platform evidence, not assumed from native/sanitizer success.
