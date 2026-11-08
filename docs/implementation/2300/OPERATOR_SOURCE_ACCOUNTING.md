# Operator domain accounting (#2300)

The #2300 target covers operator dispatch and algorithm domains
O04/O05/O06/O09, F05/F05a. The first atomic foundation below is an internal component. Its
production integration and the remaining operator domains are still open;
this document does not assert that #2300 can close yet.

## Nonstream private batch retirement component

`wl_columnar_filter_batch_end_step_owned_funded` performs one retirement
action at an authentic numeric-owned FILTER/MAP `BATCH` or ordinary
JOIN/probe/REDUCE `JOIN_BATCH` context. `filter_context_nonstream_owned` proves the
actual frame and numeric owner and excludes stream hosts. Context validation
proves current session, thread, lifecycle and absence of reentry. The selected
stage additionally requires the live pin and exact session, Original control
and saved attempt tuple. Other stages return `EBUSY`; a mismatched tuple or
drain is a protocol error. No financial check authorizes resource access.

The unchanged general `private_batch_drain_step_funded` proves the real
batch, nonce and writer before any release. It returns a nonzero protocol
separately from its `rc == 0` physical `release_error`. The wrapper preserves
that distinction, clears busy before its final real check, and advances the
context only after observing actual completion. A footer refusal may follow
real release progress; the next call observes that progress rather than
repeating release or acquired-count changes. Execution and semantic results
remain the initiating results. Only a successful footer publishes status.

The component accepts ended Original generations and does not charge or alter
their active successors. It has no numeric PENDING/consume behavior and makes
no claim about framed/pipeline integration. Existing production callers,
legacy step and ACK bodies, native latch and relation helper are unchanged.

### Financial contract

The **caller** supplies a distinct real controller with a fresh active saved
generation, assigns no incoming SOURCE promise to it, invokes this component
once, and genuinely finishes/reports/releases that generation. A retry uses
a new generation. Freshness and exclusive use are internal caller
preconditions; this helper neither verifies them with budget peeks nor creates
a permission flag. A production driver must separately qualify its handoff
and all simultaneously retained obligations before using this component.

There is one positive admission: the existing named
`WL_COLUMNAR_FILTER_ATTEMPT_BATCH_RETIRE_WORK`, **192**, at entry. Every
subsequent check uses the saved financier generation and price zero. Limit191
refuses without consuming work; exact192 admits the action. A post-admission
invalid proof, cancellation or native retention may leave all192 unused.
With incoming assigned promise0 and performed lower0, local outstanding is
therefore at most192, below quantum256. No prior Original reservation is
reissued, borrowed, refunded or counted as new payment here.

This argument is about logical admission. Scalar SOURCE distance is a separate
metric: an unchanged native authority interval can have upper224 even though
the logical action costs192. Likewise, HOST384 in legacy composition is BODY
callback financing, not automatically a newly issued SOURCE tariff. The
lexical SOURCE record is finished by `eval_serial_source_body` before BODY.
This component certificate does not inherit or certify a legacy HOST ledger.

### Expanded scalar source intervals

Count evaluated predicates, scalar stores, arithmetic/address fronts, actual
arguments/calls/results/returns and native first fronts. Controller/native
interiors retain only their existing narrow qualification. No engine grid
scan, allocator wrapper loop or generic destructor is excluded as a library
interior. The direct component has no pipeline, framed gateway or LATE
ancestor; later integration must count its actual ancestors.

| Complete selected interval | Upper | Basis |
|---|---:|---|
| Direct entry through192 admission/refusal and return | 32 | Record local, parameter/separate-controller leaves, real charge arguments and result/return |
| Successful entry192 through nonstream getter/context to real0, including invalid returns | 128 | Getter48 + nonstream wrapper10 + caller6 + context32 + caller5 + local1 + zero front5 =107, reserve21 |
| Postproof0 through pin/tuple/stage/drain guards to relation entry0 | 48 | Fixed selected predicates, busy/done stores and six real callee arguments/front; invalid exits included |
| Relation outer0 through valid authority to its next real0 | 224 | Unchanged structural36 + writer validators82 + inert/init/return12 + outer authority22 + root/call/check40 =192, reserve32 |
| Invalid relation proof through busy clear and wrapper footer0 | 208 | Prior complete254 minus old LFTJ ancestor68 + new return/busy/footer12 =198, reserve10 |
| Native release plus acquired-count/phase through wrapper footer0 | 128 | Prior176 minus old ancestor68 + new footer12 =120, reserve8 |
| CLEAR/FINAL through wrapper footer0 | 144 | Prior192 minus ancestor68 + footer12 =136, reserve8 |
| NEW postauthority0 through binding/result/phase publication and footer0 | 64 | At most12 relevant selectors/stores plus actual call/result/return and footer fronts |
| Wrapper final0 through status/error path and direct return | 32 | Longest YIELD suffix19, including five status fields and call/return; reserve13 |

The relation body matches base blob
`69481f4b3dea3cdfa3f79b763255f2f1f6398786`. Reused local expansions are the
funded drain rows in [the LFTJ accounting map](../2298/LFTJ_SOURCE_ACCOUNTING.md).
Old LFTJ ancestors are explicitly removed; their ownership or caller proof
is not reused. Physical release and acquired-count bookkeeping remain one
uninterrupted native action. No price is substituted for a source upper.

### Evidence and integration limits

`evaluation_numeric_plan` registers `test_owned_batch_retirement_primitive`.
It uses compiler-lowered MAP/FILTER/JOIN/ANTI/COUNT operations and authentic
initializers and ownership, visits NEW/OWNER/DESCRIPTOR/CLEAR/FINAL, and uses
one actual new financier per invocation. It asserts literal typed output and
cloned raw parity,191/192, stopped/stale financiers, wrong owner/frame/thread/
session/control/generation/nonce, physical retention, successful and failed
release followed by footer cancellation, ended Original and actual successor
with unchanged seven-field frozen report and zero successor consumption.
Peer37 governor credit remains through cleanup and returns to zero after its
own rollback. A genuine early-cancelled MAP proves the no-batch transition
preserves its initiating error. The existing generated composed JOIN pipeline
also attempts this helper during actual consumer dispatch and verifies its
nonstream refusal before continuing to the original raw-parity result.
These are component assertions, not public production wiring.

The next foundation must own generic numeric ROW capture/growth and displaced
or partial storage, owned output/input FINISH retirement, scalar source END,
registered native/latch/ACK callers and stream state/close tails. Broad
prototype A1/A2 has no approval for activation. U1 SOURCE family admission
remains blocked until those real paths and their full caller/refusal/carry
certificates are complete. The final closing PR must contain a qualified
production consumer of this primitive and complete the remaining #2300 work.
