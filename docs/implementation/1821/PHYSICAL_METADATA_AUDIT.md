# Physical scalar metadata disposition and verification

The physical record is `col_delta_timestamp_t`: iteration, stratum, worker,
reserved0 and signed int64 multiplicity. A tuple callback's `diff` is a
different observable. Ordinary generated scalar plans evaluate value tuples;
the standalone weighted JOIN/COUNT kernels are not generated opcodes.

| Actual route | Physical disposition | Verification |
| --- | --- | --- |
| VARIABLE full/AUTO/explicit delta | Borrow the selected descriptor, including its physical stamp image. A shared view's own timestamp array is authoritative. | Actual numeric source-plan full/delta selector fixtures; physical source/alias oracles; complete session bodies. |
| Forced empty VARIABLE | New empty value relation deliberately has no stamp array, matching raw empty-variable policy. | Numeric-plan fresh and prebound composed fixtures retain the explicit empty/no-stamp assertion. |
| FILTER | Preserve all five fields of every retained row. Zero/negative weights do not turn into predicate decisions. | `evaluation_numeric_operators` independent all/mixed/none field oracles; `evaluation_session_physical` observes real private FILTER output before MAP. |
| MAP/value projection | Deliberately produce no timestamp image. | Existing physical numeric chains and generated session bodies; original source images are checked independently. |
| Ordinary JOIN, left and right | Derive value tuples with no timestamp image, including timestamped selected sources; this is not weight multiplication. | Generated physical JOIN and source-side fixtures plus the existing full/delta selector matrix. |
| SEMIJOIN/ANTIJOIN with a registered right input | Value projection, no output stamps. Timestamped left/right sources use existing controlled leases. | Generated physical positive/negative bodies and numeric operator qualification. Raw missing-right ANTI pass-through is a separate raw contract; the controlled registry domain requires the right relation. |
| Ordinary REDUCE | Count/aggregate rows using ordinary raw arithmetic; do not sum physical weights. Output has no stamps. | Physical direct COUNT/SUM/MIN/MAX/AVG matrix with independent literal answers; generated COUNT over five rows whose signed weights sum to a different value. |
| Two-input CONCAT | Preserve left/right value order and duplicates, deliberately omit stamps as raw two-input CONCAT does. | Physical CONCAT fixture and generated UNION session bodies. Raw single-entry K-copy/pass-through is outside this two-entry primitive's domain. |
| CONSOLIDATE | Stable value sort/dedup keeps the entire first incoming representative's stamp. It neither chooses minimum timestamp nor sums equal-row weights. | Nullary, scalar and wide duplicate fixtures, including an already sorted prefix. Earlier records have larger timestamps than later duplicates; independent five-field oracles check both raw and bounded outputs. |
| Selected-row append | Copy all five fields when the original source requested preservation. | `evaluation_physical`: widths0/3/17, owned and real shared-view descriptors, weights0,+1,-1,+3,-4, independent source/output arrays. |
| Explicit row-copy value projection | Deliberately omit stamps by its existing API contract. | Separate no-stamp assertions in `evaluation_physical`; never described as a preservation API. |
| Raw recursive new-row delta | Derive iteration/stratum/worker0/reserved0/multiplicity+1 for newly derived set rows. | Real `edge/path` session hook checks every physical delta record, including iteration from independent chain distance and stratum from the generated plan. |
| Timestamped recursive STEP/SNAPSHOT | Use existing preserved-body reconstruction; the specialized binary value-only fast path retains its original domain. | Physical `edge/path` source and complete closure, observed/plain STEP/SNAPSHOT, later removal and signed differences. |
| Removal staging `$r$` | Copy the selected source's complete physical record verbatim. Do not rewrite its multiplicity to imitate callback deletion signs. | Exact staged removed-row stamp and value assertions, surviving source records after evaluation, later STEP/SNAPSHOT and stopped reconstruction retry. |
| Snapshot delivery | Callbacks expose values; gathering does not reinterpret source weights. A successful raw call may compact oversized storage and advance storage generation while retaining the exact physical row image. | Exact physical source fields/values and tuple images. Precommit stop assertions additionally require unchanged source generations, public-head identity/content and no events. |

The composed VARIABLE/prebound checks no longer blanket-reject timestamps.
Each real consumer owns its preservation or value-projection contract. The
CONSOLIDATE builder requests preservation, uses its existing stable index sort
and selected-row append, then seals value-order caches only after all records
are complete. Its sorted-cache helper does not inspect or modify stamp values.
Existing compound, active raw-state, worker, strict and ownership refusals
retain their separate contracts.

Fresh ordinary UNION cleanup contains five fixed384 protocol payments; the
old four-payment grant could repeatedly stop before binary cleanup progressed.
Its finite host allowance is now five. The independent evaluation operation
still receives the same work debits. Actual private CONCAT publication is
tested with cancellation and work exhaustion, not an unlimited funding bypass.

`evaluation_physical` stops after real descriptor/owner claims and timestamp
capture, and after a value append before timestamp-tail completion. It also
injects constructor allocation and governor admission failures; failed private
outputs are discarded and reset/retry retains exact source images and credits.
Actual session tests stop after private FILTER/CONCAT/JOIN publication before
the public transaction cutoff, preserve old heads and pending physical input,
and retry the same wrapper. Raw evaluating SNAPSHOT emits its tuple image
without running raw STEP's notification transaction; the bounded initial
snapshot's transaction and later STEP signed differences are tested according
to their respective existing contracts.

Timestamped scalar retirement keeps the same four governor tokens. Timestamp
heap storage is part of the retained token. Reversible preparation validates
shape, direct aliases and physical payload coverage in charged fixed/per-column
actions. A timestamp-only committed stage frees the buffer and advances its
cursor before polling. Credit remains held through descriptor disposal and
terminal writer consumption. The ledger tail publishes fixed-size RELATION
and actually charged TIMESTAMP frees atomically; an unbilled deferred image
does not create a synthetic refund. Conversely raw incremental set consolidation
can already have freed the timestamp buffer while its ledger charge remains
pending; retirement refunds that actual recorded charge even when physical
timestamp capacity is zero. Live allocation size is not a substitute for the
ledger's recorded ownership.

`evaluation_physical_retirement` covers widths0/1/3/257, governed/ungoverned
storage and absent/charged/deferred ledgers, including timestamp-only ledger
charges on nullary rows. It checks source-reader contention, cancellation
after actual metadata validation and READY, malformed shape/direct aliases/
retained coverage, one-action stepping, work exhaustion, cancellation after
physical timestamp free, no early credit, exact subsystem balances and DONE
idempotence. Session removal additionally reaches this retirement path through
the real wrapper and requires zero governor credit after full ownership teardown.
An API-created raw-consolidation fixture covers deferred timestamp refunds
with governed and ungoverned storage. An independent timestamp owner retains
its37-byte canary charge throughout retirement, so a synthetic refund cannot
hide behind a clamped global ledger balance.

Standalone `phase3c_join_integration` and `phase3c_reduce_integration` directly
call the weighted kernels for multiplication and signed COUNT references.
Those results are labelled standalone evidence, not proof of a generated
weighted opcode, persistent weighted trace engine, compound/spill coverage or
the remaining general-recursion/platform acceptance obligations under #2293.
The complete legacy cap disposition is recorded separately in
[ITERATION_CAP_AUDIT.md](ITERATION_CAP_AUDIT.md).
