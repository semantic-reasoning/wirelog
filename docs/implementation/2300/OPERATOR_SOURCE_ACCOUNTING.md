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

## Owned private-row component

`wl_columnar_filter_row_component_append_owned` appends to a genuine quiet
numeric-owned `BATCH` or `JOIN_BATCH` context. The external, caller-owned
`wl_columnar_relation_row_debt_t` stays at its final address from header-only
initialization through terminal unbind. It owns the actual capture, ROW,
prepared governor/ledger, refund and downsize records; none is copied after
acquisition. The cleanup frame holds one nonowning anchor and the context
keeps its real busy borrow until the receipt reaches `DONE`. Ordinary STEP,
batch END, ACK and frame destruction cannot consume that borrow.

This component does not change production ROW routing. Production operators
append while their initializers are `RUNNING`; this API accepts their later
quiet batch stage. Its supplied test row is explicitly a component input,
not a claim about a natural program result. Production wiring, owned FINISH,
stream/native callers and their complete financial ledger remain separate
activation requirements.

The return integer reports protocol/authority/funding refusal. The five-field
status separately reports the initiating execution error, physical cleanup
error, whether the row was published, pending cleanup and completion. Before
binding, refusal preserves the receipt and caller status. After binding,
semantic failure records its first cause and retains exact ownership; it
never returns a successful pending row that an outer loop could replay.
Success requires synchronous retirement and unbind. A stopped financier
leaves status unchanged even if physical progress preceded the footer;
`status_owned` observes that canonical progress. Terminal query accepts NULL
frame/owner after they have been freed, and reads only the receipt header.

Before publication, staged arrays belong to the ROW record. A bounded handoff
records their initialized extent before any returning check. After the
private cutoff, the new row/image/generations remain installed and displaced
old arrays belong to the receipt. The existing prepared governor finish,
relation/lease stores and ledger apply/release stay contiguous. No shared
ledger gate remains held across a check. Retirement frees one recorded lane,
then its pointer table, then timestamps; typed refund/downsize and capture
buffer/credit/reference release follow in separate actions. Failed native
CAS preserves its actual receipt and reports `EAGAIN`; a retry gets new
funding, without repeating an already completed free or refund.

The selected ROW-record tail and complete ROW/capture handoffs end at real
saved Original checks. This includes early capture/renewal errors: cleanup
with no ROW record uses the capture-only proof, accepts authentic
`HELD`/`DRAINING`/`ENDED` batches, and does not demand released writers be
reacquired. ROW-resource cleanup instead validates both held writers and
their exact saved nonce. Capture credit distinguishes actual RESERVED
owner-zero, COMMITTED self-owner and the recorded origin of RELEASING; it
binds the actual held governor reference before releasing credit.

### Row source intervals

The same scalar metric and narrow native/allocator boundaries above apply.
Each row includes its actual argument, callback-dispatch and returned error
front. Geometry uses genuine checks between fixed groups of at most two
range comparisons. Full native batch authority has its own initial/footer
checks, separate from context and geometry proof.

| Complete selected interval | Upper | Basis |
|---|---:|---|
| Fresh header-only setup / DONE reset | 64 / 128 | 34 meaningful header stores, portable thread front; reset additionally proves inert unbound state |
| Forward bootstrap through first actual Original check, including invalid return | 160 | Existing context/getter107 + explicit tuple/pin/batch/semantic32 + caller16 + reserve5 |
| Dedicated cleanup bootstrap through distinct-financier check/admission | 192 | Fixed header48 + actual anchored context128 + exclusion/check/return16 |
| Full mutation tuple through actual check/refusal | 96 | Self/thread24 + 11 field leaves + nested fields-call8 + caller24, reserve29 |
| Geometry setup / one span / two-pair group | 96 / 64 / 96 | Fixed setup; one overflow span; at most two subtraction-based range helpers |
| Last geometry check through header and bind's internal check | 160 | Return12 + fixed header48 + frame/context/target/nonce64 + portable thread/check24 + reserve12 |
| Bind check through finite anchor/busy publication and ROW128 refusal | 96 | Owner publication24 + returned call/check32 + refusal/status32 + reserve8 |
| Full batch/native authority through common footer | 224 | Existing structural/native192 + strict held/count8 + financial/refusal16 + reserve8 |
| ROW target/extent after authority / capture-only lineage | 64 / 96 | No width scan; capture-only forbids image/ROW/refund/downsize debt |
| One lane/table/timestamp/capture-buffer action through footer | 128 | Dispatch24 + single free/clear/cursor20 + returned bookkeeping/observer/footer48 + reserve36 |
| Prepared ledger / governor cancellation through footer | 128 / 128 | Exact final-address owner/Original tuple before native precheck; no retained HELD gate |
| Refund/downsize inert setup through native precheck | 128 | Eight meaningful inert fields plus exact record/target proof, no structure-sized clear |
| One refund/downsize begin or advance through footer | 224 | Native128 + typed result/readiness32 + complete returned wrapper48 + reserve16 |
| Capture credit role proof / one finish step through footer | 128 / 224 | RESERVED/COMMITTED/RELEASING owner proof precedes separate native check |
| Capture reference release through footer | 128 | First ref/native/free front46 + clear/progress12 + caller48 + reserve22 |
| Final check through complete unbind and returned status | 128 | Fixed owner checks24 + cause/anchor/pointer/busy handoff24 + status/return32 + reserve48 |
| DONE query / active query without a poll | 72 / 248 | Active: header53 + actual live112 + fields-only tuple23 + output geometry43 + status13 + reserve4 |
| Selected RECORD first admission / initialized record tail | 96 / 128 | Existing first80 plus pointer selection/caller; meaningful records39 and safe selected error handoff |
| SHAPE / one scalar cell / operator byte chunk | 160 / 96 / 192 | Actual old packets plus selected error handoff; operator copy stays eight bytes per183, legacy generic stays sixteen |
| Ordinary SIZE/credit/allocation/recipe/validate packet | 192 | Largest151 + selected handoff24 + caller16, reserve1; nested preparation remains separate |
| Governed ledger cutoff through completed publication and ROW footer | 252 | Old complete225 + debt stores7 + routing3 + result/phase3 + real cut8 + caller reserve6 |
| Prepublication initialized-record error handoff through footer | 128 | First native/restore64 + mandatory gate alternative40 + fixed owner/phase/check24 |
| Selected append footer through next live-context check | 160 | Cause/return16 + observer7 + actual live112 + returned refusal24 + reserve1 |
| Capture initialized header / wide ref-token setup | 96 / 128 | Readiness after meaningful header; native ref first front and complete token setup before check |
| Last capture cell/error return through selected append footer | 128 | Cell36 + retained dispatch8 + safe cause/phase/footer24 + own restore48 + reserve12 |
| Actual batch check through selected capture admission / renewal packet | 240 / 240 | Old postbatch223 + current call/progress16; unchanged renewal packets plus returned phase/footer16 |

The zero-final-payload branch uses the existing seven-field meaningful inert
reservation initializer only after actual native release completion. It has
no `sizeof(token)` clear and stays inside the native action224 envelope.
Capture's source reserve stages have independent uppers112/64/144/64 and
commit80/64/128, distinct from their logical prices100/124/164/78 and98/81/63.
The selected caller adds at most48; the own-credit restore/error/footer
contour is bounded224. No unrelated reservation or general engine retry loop
is hidden in the inherited native own-credit restoration boundary.

### Row admission and inherited obligations

The selected192 and ROW128 admissions replace the original numeric prefix.
Before128, distinct already performed leaves total64: structural20, two real
writer validations10, outer authority11, context8, frame/record9 and six
bootstrap pin/session/control/id/output/private-output leaves. Thus local
incoming assigned promise0 gives192+128-64 <=256. Geometry, thread fronts,
binding stores and future native success are not borrowed as this lower.

Normal retirement adds one named logical action price1. Each iteration first
records a new selected phase/cursor and intent; the action verifies that
snapshot. At least one distinct meaningful store has already performed before
each new1, including native retries, so its added charge minus new lower is
at most zero. Earlier intent does not fund a later retry. This does not pay
native source224 or erase old obligations: governor finish44, ledger
initial26/apply117/release23, ROW terminal48 and each actually compatible
prior unfinished region retain their own identities. Their real terminal or
cancellation work executes; zero checks alone do not settle them.

One cleanup invocation uses a fresh distinct dedicated financier generation,
incoming assigned promise0, one price192, conservative lower0 and unused at
most192. It never polls Original, including an ended generation or its
genuine successor. Exclusion uses actual frame-owned context before calling
any API on the financier; an untrusted capsule control cannot select it.
Full production parent/stream/HOST correlated carry remains unqualified by
this component contract.

### Shared callers and component evidence

Legacy capture now has a small adapter to the shared algorithm. Its
first-admission contour is255: old248 plus at most seven argument/call/return
cells. The ready predicate occurs after that check and the retained-error
branch at its own exit. The historically unqualified generic terminal
capture255 contour remains explicitly unqualified; this first-admission
proof does not certify its old synchronous disposer. Selected retained mode
avoids that disposer and has its own genuine footer.

The shared ROW selector adds at most four entry cells before RECORD and one
false debt leaf after record initialization for legacy/LFTJ callers. The
legacy growth cutoff upper becomes228 from225; its old mandatory retirement
boundary remains. Owned LFTJ nongrowth SHAPE128/CELL64 stay covered; its
inert terminal128 gains one false leaf (129). The scalar initializer BIND254
and numeric PREFIX249 bodies and record sizes remain unchanged. The cleanup
frame gains only one pointer; its dynamic accounted constructor may execute
another already bounded sixteen-byte zero iteration. Direct continuation,
dispose, free and free-target paths add one anchor guard before consuming an
entry. Generic legacy teardown needs its own later caller qualification.

The existing registered `evaluation_numeric_plan` target contains the actual
component fixtures. Generated MAP/FILTER/JOIN/ANTI/COUNT contexts prove both
accepted stages. Tests cover widths1/16/17/257, actual64→65 and128→129 growth,
spare capacity, aliased input capture, FLOAT negative zero/invalid NaN,
timestamped FILTER with signed old metadata and default appended metadata,
capture/column/timestamp allocation failure and enforcing-cap provenance.
Independent literal content checks accompany genuine READY governor/ledger
cancellation and postpublication cleanup. Original cancellation, initialized
record-tail refusal, renewal ENDED gap and partial-release DRAINING retain
exact ownership. Actual precommit RESERVED capture proceeds through
RELEASING under fresh cleanup grants. A peer reservation forces real refund
and downsize credit CAS loss; peer37 and ledger peers11/13 detect duplicate
credit/free accounting.

The retire1 oracle separately labels the observed setup allowance: allowance0
cannot free the first displaced lane, allowance1 frees exactly that lane,
and the next admission refuses. The action tariff is independently1; no
total-budget search supplies its proof. Dedicated cleanup191/192, stale and
cancelled generations, real progress followed by footer cancellation,
copied receipts, thread/session/control/status-alias refusal, reentry and
legacy STEP/END/ACK refusal are asserted. Ended Original reports retain all
seven fields and genuine successors spend zero. DONE queries work after
frame/control destruction before a fresh header-only reset.


## U0b.2: externally owned FINISH retirement component

The generic operator FINISH component reuses the existing finite scalar
retirement machine. It admits actual FILTER, MAP, JOIN, SEMIJOIN, ANTIJOIN,
REDUCE and CONSOLIDATE contexts only after CONTROL has released the output
writer/pin and reached quiet nonstream FINISH. It does not activate a new
production route. An external direct or composed numeric owner, including an
actual outstanding CHILD result, remains frozen under the prepared marker.
The embedded framed child is excluded by its actual address after canonical
owner proof and before any financier API. This excludes ordinary framed and
registered native/error parents; it does not rely on a native-cleanup flag.
Their existing production behavior is unchanged. Coherent production ROW,
stream, framed/native and SOURCE integration remains a later required unit.

The caller supplies fresh zero or genuinely ACKED final-address prepared
storage, distinct from frame, owner and selected descriptors. The constructor
never reads a dormant owner-thread payload. Fixed bounded proof packets check
actual operation, context, range, scalar state and scratch ownership; captured
plan/cursor/phase and actual context/control lineage are rechecked before
retaining Original and publishing the marker last. Original is retained for
identity/lifetime, never polled during cleanup. An ended Original and an
active successor therefore authorize neither work nor a debit here.

HEAD distinguishes committed replacement, uncommitted provisional output,
present-empty ANTI metadata-only keep-all, and an inert selected owner.
Missing right is MISSING_EXACT: it preserves the exact original entry without
a replacement commit. ANTI keep-all retains the live relation and exact owned/borrowed bit,
clears live delta/segment metadata and retires only the actual historical
segment array. HEAD never selects that live relation for destruction. The
segment action is irreversible; decline cannot resurrect a freed array.
Before scalar commit, decline performs real partial initializer reset and
claim release, restores only the legal retained entry, then clears and ACKs.
After irreversible scalar commit, work must drain through the finite machine.
There is no generic destructor or unlimited fallback.

### Source contours and financier schedules

The emitted-source support counts actual helper/caller paths separately from
logical admission totals. All new financiers have incoming assigned promise0.
The constructor has one positive256 admission with conservative lower0;
HEAD has one positive192 admission with lower0. Each proof packet executes a
real zero check. A zero check does not discharge an existing promise and a
phase with several positive admissions does not claim a whole-call256 source
loan. HOST384 is not imported as a SOURCE tariff. Correlated production carry
and containing serial/native caller bounds must be qualified at integration.

| Actual contour | Conservative source upper |
| --- | ---: |
| context / operator state / completed-resource state | 48 / 80 / 64 |
| each scratch group / terminal-inert receipt / prepared+funding proof | 32 / 128 / 96 |
| begin initial proof / geometry / operator-state packet | 160 / 112 / 112 |
| begin completed-resource / scratch / descriptor-range packet | 112 / 80 / 80 |
| begin inert receipt / final lineage / positive binding packet | 176 / 144 / 160 |
| generic step or decline initial proof | 128 |
| HEAD observer and renewed proof / context and state | 144 / 192 |
| HEAD completed-resource or scratch / disposition / fixed handoff | 112 / 112 / 96 |
| generic non-HEAD delegation prefix / decline prefix | 144 / 144 |
| segments / selection / commit / ordinary dispose | 82 / 95 / 190 / 161 |
| ledger wrapper / scalar clear / claim release / decline restore | 121 / 211 / 189 / 71 |
| work clear / ACK / error or footer | 99 / 125 / 48 |
| scalar INIT BOOT first prefix / post128 / ZERO / BIND / RESET | 176 / 144 / 208 / 224 / 192 |
| finite timestamp payload including the generic return | 192 |

The new generic non-HEAD dispatcher returns the existing helper directly:
there is exactly one extra returned cell after its actual final check. In
particular timestamp payload191 becomes192, rather than hiding a new result
or status-copy suffix. The initializer BIND224 is counted along the real
staged path, not formed by adding a wrapper to the separate legacy BIND254.
The old INERT authority, BIND254 and numeric PREFIX249 stay literal. Shared
read-only JOIN-state and scratch extraction adds six and seven cells to those
old helpers respectively: their uppers become89 and72.

| Actual phase | Logical admissions in order |
| --- | --- |
| begin / HEAD | 256 / 192 |
| segments / select | 128 / 128 |
| INIT BOOT / ZERO / BIND / RESET | 192+128 / 192+256 / 192+192 / 128+128 |
| PREPARE phase0 / phase1 or2 / ordinary token or pair | 256+1 / 192+1 / 128+1 |
| timestamp payload21 | 128+1+192 |
| COMMIT | 192 |
| ordinary DISPOSE / native credit | 256+1 / 256+64 |
| ledger DISPOSE12 with actual debt | 128+109+166+28 |
| scalar clear / work clear / ACK | 256 / 128 / 192 |
| decline / partial initializer reset if needed | 128 / 128 |
| decline claims / clear / restore / work clear | 256 / 256 / 128 / 128 |

The inherited native charge/lower identities remain distinct; in particular
BOOT's real overlap lower64 precedes its128. No earlier setup intent pays for
a future retry. Each exact/one-short phase oracle states cumulative logical
admissions independently of production fee selection. These tables qualify
the component and its real native subpaths; they do not certify the sum of
unused loans for a future containing serial SOURCE turn.

### Actual component evidence

`evaluation_numeric_plan` drives real generated MAP/FILTER/JOIN/ANTI/COUNT
operations after an actual preceding owned output. SEMI and CONSOLIDATE
fixtures are explicitly IR components, not claims of compiler selection.
Direct CHILD/PENDING and composed UNARY_CHILD/COMPOSE_PENDING witnesses retain
the old result tuple and completed host financier allocation/report through
component ACK, then consume the old tuple first. STEP, consume and ACK are
refused without mutation while the marker owns the frame. All seven fields
of ended Original reports remain equal; genuine successors consume zero.

The absent-right and present-empty ANTI fixtures separately cover actual
borrowed VARIABLE input and owned MAP output, with and without historical
segment/delta metadata, plus decline after irreversible segment disposal.
They preserve the input snapshot, exact ownership and literal values. The
borrowed keep-all regression fails on the old HEAD ownership predicate and
passes only when equality of owned bits permits the real false/false case.

Independent literal content, actual ownership and peer37/governor and11/13
ledger baselines cover successful and cancelled families. A real peer reserve1
between the governor total read and credit CAS forces native EAGAIN; a fresh
step settles it exactly once. Funding-generation turnover is injected at the
outer HEAD observer, each of four initializer APIs, ledger READY before CAS,
and governor credit before CAS. The old generation is rejected, successor
consumption remains zero, irreversible progress is retained and fresh grants
finish cleanup. Ledger READY refusal leaves its actual writer gate clear.

Explicit existing constant-true FILTER bytecode isolates actual retirement
for nullary,1,65,257 columns, empty/nonempty and mixed INT64/FLOAT/STRING/
UINT64/BOOL storage. Values use real interned symbols and typed constructors;
timestamps carry literal stratum/worker/iteration and signed multiplicity.
Owned input comes from executing the preceding FILTER. Every scalar-init
phase is declined through actual reset/claim release, then reentered and
fully drained. Critical timestamp payload321 logical admission is exercised.
These are component intersections; natural session/recursive coverage remains
required before issue closure.

`evaluation_serial` independently produces an ordinary framed quiet FINISH
and proves generic begin returns EBUSY without financier consumption or frame
mutation, then resumes the legitimate driver. The existing host lifecycle
`native-end` fixture proves actual session registration with its read-only
API for both ended Original and active successor. It rejects generic begin
on the real embedded child before funding, preserves full context/report,
then completes the existing native cleanup and verifies literal retry and
resource baselines. Neither fixture manufactures native permission flags.
