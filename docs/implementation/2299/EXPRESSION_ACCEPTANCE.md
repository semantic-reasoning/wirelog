# Bounded expression acceptance (#2299)

Baseline: main `54f76fb7`. This ledger tracks O02/O03 and C01/C02. It is an
inventory and implementation contract, not a statement that the issue is closed.
PR #2344 is still open at this baseline; its JOIN host work must be preserved
when integrating scalar domains. #1819 and #1953 have merged. Existing numeric
qualification remains in `docs/implementation/1821/SERIAL_COVERAGE.md` and
`test_evaluation_numeric{,_operators,_plan}.c`; it is not reimplemented here.

## Finite tag inventory

Every enumerated expression tag has exactly one row below. Admission columns
are independent: adding a scalar route never hides numeric/empty-only support.
`admitted` means the separate scalar decoder recognizes a tag; it does not
certify all contexts. `baseline-numeric` retains existing numeric evidence;
`unit4-sessions` names the first string-domain session qualification below,
not all recursive/pipeline/library contexts. `unit5-sessions` records the
read-only function evidence in unit5a below. `existing-numeric`
means the current attempt decoder admits it; it does not certify every host
context. `empty-only` means the decoder admits it only for raw-compatible empty
setup, not ordinary evaluation. `open` identifies raw-supported work still
needing bounded implementation/integration. `legacy-rejected` is not a coverage
gap to cure by adding language semantics. The inventory guard compares these
rows with source, so adding or broadening a decoder tag requires revisiting the
ledger explicitly.

| Tag | Numeric decoder | Scalar decoder | Host evidence |
| --- | --- | --- | --- |
| `WL_PLAN_EXPR_VAR` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_CONST_INT` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_CONST_STR` | open | admitted | unit4-sessions |
| `WL_PLAN_EXPR_BOOL` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_VAR_FLOAT` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_CONST_FLOAT` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_VAR_STRING` | open | admitted | unit4-sessions |
| `WL_PLAN_EXPR_EXTENSION_CALL` | open | open | open |
| `WL_PLAN_EXPR_EXTENSION_CALL_ABI` | open | open | open |
| `WL_PLAN_EXPR_ARITH_ADD` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_SUB` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_MUL` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_DIV` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_MOD` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_BAND` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_BOR` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_BXOR` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_SHL` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_SHR` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_FLOAT_ADD` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_FLOAT_SUB` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_FLOAT_MUL` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_FLOAT_DIV` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_BNOT` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_HASH` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_CRC32_ETH` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_CRC32_CAST` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_ARITH_MD5` | open | open | open |
| `WL_PLAN_EXPR_ARITH_SHA1` | open | open | open |
| `WL_PLAN_EXPR_ARITH_SHA256` | open | open | open |
| `WL_PLAN_EXPR_ARITH_SHA512` | open | open | open |
| `WL_PLAN_EXPR_ARITH_HMAC_SHA256` | open | open | open |
| `WL_PLAN_EXPR_ARITH_UUID4` | open | open | open |
| `WL_PLAN_EXPR_ARITH_UUID5` | open | open | open |
| `WL_PLAN_EXPR_CMP_EQ` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_CMP_NEQ` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_CMP_LT` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_CMP_GT` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_CMP_LTE` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_CMP_GTE` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_CMP_FLOAT_EQ` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_CMP_FLOAT_NEQ` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_CMP_FLOAT_LT` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_CMP_FLOAT_GT` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_CMP_FLOAT_LTE` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_CMP_FLOAT_GTE` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_AGG_COUNT` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_AGG_SUM` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_AGG_MIN` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_AGG_MAX` | existing-numeric | admitted | baseline-numeric |
| `WL_PLAN_EXPR_AGG_AVG` | legacy-rejected | legacy-rejected | legacy-rejected |
| `WL_PLAN_EXPR_STR_FN_STRLEN` | open | admitted | unit5-sessions |
| `WL_PLAN_EXPR_STR_FN_CAT` | open | open | open |
| `WL_PLAN_EXPR_STR_FN_SUBSTR` | open | open | open |
| `WL_PLAN_EXPR_STR_FN_CONTAINS` | open | admitted | unit5-sessions |
| `WL_PLAN_EXPR_STR_FN_STR_PREFIX` | open | admitted | unit5-sessions |
| `WL_PLAN_EXPR_STR_FN_STR_SUFFIX` | open | admitted | unit5-sessions |
| `WL_PLAN_EXPR_STR_FN_STR_ORD` | open | admitted | unit5-sessions |
| `WL_PLAN_EXPR_STR_FN_TO_UPPER` | open | open | open |
| `WL_PLAN_EXPR_STR_FN_TO_LOWER` | open | open | open |
| `WL_PLAN_EXPR_STR_FN_STR_REPLACE` | open | open | open |
| `WL_PLAN_EXPR_STR_FN_TRIM` | open | open | open |
| `WL_PLAN_EXPR_STR_FN_TO_STRING` | open | open | open |
| `WL_PLAN_EXPR_STR_FN_TO_NUMBER` | open | admitted | unit5-decimal |
| `WL_PLAN_EXPR_STR_FN_UUID5_RFC` | open | open | open |
| `WL_PLAN_EXPR_CMP_STR_EQ` | empty-only | admitted | unit4-sessions |
| `WL_PLAN_EXPR_CMP_STR_NEQ` | empty-only | admitted | unit4-sessions |
| `WL_PLAN_EXPR_CMP_STR_LT` | empty-only | admitted | unit4-sessions |
| `WL_PLAN_EXPR_CMP_STR_GT` | empty-only | admitted | unit4-sessions |
| `WL_PLAN_EXPR_CMP_STR_LTE` | empty-only | admitted | unit4-sessions |
| `WL_PLAN_EXPR_CMP_STR_GTE` | empty-only | admitted | unit4-sessions |
| `WL_PLAN_EXPR_ARITH_HASH_S` | open | admitted | unit5-digests |
| `WL_PLAN_EXPR_ARITH_CRC32_ETH_S` | open | admitted | unit5-digests |
| `WL_PLAN_EXPR_ARITH_CRC32_CAST_S` | open | admitted | unit5-digests |
| `WL_PLAN_EXPR_ARITH_MD5_S` | open | open | open |
| `WL_PLAN_EXPR_ARITH_SHA1_S` | open | open | open |
| `WL_PLAN_EXPR_ARITH_SHA256_S` | open | open | open |
| `WL_PLAN_EXPR_ARITH_SHA512_S` | open | open | open |
| `WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS` | open | open | open |
| `WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI` | open | open | open |
| `WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS` | open | open | open |
| `WL_PLAN_EXPR_ARITH_UUID5_SS` | open | open | open |
| `WL_PLAN_EXPR_ARITH_UUID5_SI` | open | open | open |
| `WL_PLAN_EXPR_ARITH_UUID5_IS` | open | open | open |

## Value, error and context contract

The authority is `wl_columnar_expr_eval_run_ctx`, the checked `string_ops`
functions, and the actual host's policy. The compiler emits checked `colN`
references; raw VAR compatibility accepts some permissive/truncated names.
Malformed raw bytecode is not newly accepted to broaden generated coverage.
The raw stack pops zero on underflow and ignores excess pushes. Aggregate
COUNT/SUM/MIN/MAX are expression no-ops; AVG has no raw switch case and stays
rejected. Preserve payload truncation, trailing bytes and per-tag failure
behavior rather than imposing a new universal typed-stack contract.

| Domain | Required compatibility and ownership evidence |
| --- | --- |
| String variables/literals | VAR_STRING needs an existing ID and intern context; CONST_STR has a distinct null-intern fallback. Literal payload length and NUL-terminated intern value can differ for embedded NUL. Preserve allocation versus denied-growth status. |
| Comparisons and read-only functions | Compare actual strings, preserve each invalid-ID/default result, byte versus UTF-8 codepoint semantics, empty operands and substring boundaries. Length, compare and search work must count actual inspections, including unsuccessful nested searches. |
| Producing functions | CAT/SUBSTR/case conversion/REPLACE/TRIM/TO_STRING use checked errors. Own temporary buffers and intern pins through cleanup. TO_NUMBER preserves checked range/parse behavior. Retained intern entries after an otherwise failed host are explicitly distinguished from leaked temporary reservations. |
| HASH/CRC/digests | Preserve symbol-versus-integer operand interpretation and legacy invalid-symbol fallback. Test integer and string variants separately. SHA/MD5/HMAC and UUID paths need enabled and disabled mbedTLS controls and matching error outcomes. |
| UUID | Preserve legacy UUID5 framing and separate RFC UUID5 string namespace validation; UUID4 is nondeterministic, so test required structure/context and failure effects rather than literal values. |
| Extensions | Preserve descriptor lookup, ABI identity, arity/types, scalar-result permission, configured/active worker policy, session reentrancy stack and callback lease lifetime. Meter lookup, engine argument preparation and result validation/copy separately from the cooperative callback body. |

FILTER semantic failure rejects a row; MAP follows its current semantic/status
policy. Control stops and genuine engine/allocation errors must not be folded
into semantic rejection. Full, delta and empty selectors, signed events,
weights/timestamps, aggregate consumers and deletion behavior remain host
obligations. Type inference and support predicates must agree with the context
actually provided by FILTER/MAP, recursive and pipeline hosts. A decoder
accepting a tag before its host can carry the full context is not support.

## Bounded byte and intern primitive contract

Implement immutable byte primitives before allocating transforms. A byte
inspection, byte copy/store, hash update, collision comparison, probe step,
slot initialization and resize movement are distinct charged actions. Each
charge includes its actual engine loop/caller scaffolding, with finite tariff
and nested source bounds documented before activation. Every admitted segment
must fit quantum 256; do not use a whole-input debit followed by raw strlen,
memcpy, strcmp, hash, conversion or interning. Exact/one-short expectations
come from this documented tariff contract, not an observed successful debit.

Controlled intern insertion is a transaction:

1. Validate the saved attempt, poll before work, scan/hash/copy with bounded
   checkpoints, then take the existing writer mutex.
2. Probe with metered comparisons. A concurrent duplicate may have appeared
   while preparing the copy: release the speculative copy and return the
   existing ID without charging retained growth.
3. Admit growth; stage any segment and new slot table. Initialize, hash,
   probe and migrate with checkpoints into private storage.
4. Before publication, poll and establish a finite prepaid publication/cleanup
   allowance. No cancellation edge may occur after taking ownership away from
   a local reservation unless the table has a complete valid receipt.
5. Publish once using the existing release-store count protocol, preserving
   prior reverse pointers and IDs. Unlock and release old temporary storage.

Every prepublication error/stop leaves count, slots, segment publication,
existing reverse pointers and governor totals unchanged; free all staged
storage, roll back local reservations and unlock. Retry after reset or a larger
limit must succeed exactly once. Mutex acquisition and allocator bodies are
narrow host boundaries; engine initialization/copy loops around them are not.
Intern pins cover all borrowed string pointers and callback arguments. Tests
must include collisions, resize, concurrent-duplicate preparation and pin-held
owner retirement. Existing raw interning keeps its own compatibility path.

## Library, extension and qualification boundaries

Use incremental library update APIs for variable-size digest/HMAC/UUID inputs.
Charge engine framing and buffer preparation and document bounded init/update/
finalize calls separately. Do not claim cancellation inside a library call.
For callbacks, finish mandatory callback/session-stack release even after a
stop or control rebind, poll immediately after return, then validate/copy the
result under the correct attempt. Preserve genuine callback error versus stop
precedence according to the existing path, with explicit tests.

Each newly admitted domain needs compiler-selected session FILTER and MAP
positive, rejected and no-output cases, deterministic mid-work stops, independent
primitive exact/one-short limits, invalid/type/library/context errors,
allocation/governor denial, precommit nonpublication, reset/raised-budget retry,
subsequent calls, exact relation-qualified values and required signed events,
and resource/pin/lease balance. Run native plus ASan/UBSan, optional-library
on/off, and relevant numeric/serial/recursive/pipeline regressions. Wall time is
only a watchdog. Update the ledger with concrete assertions and commands before
marking a domain qualified. Missing evidence stays outstanding.

## Immutable byte prerequisite (unit 2)

The `wl_columnar_expr_*_attempt_controlled` byte helpers are prerequisites;
none of the `open` expression rows is activated by these helpers alone.
They require a live saved attempt and borrowed immutable source storage.
They neither allocate nor acquire intern ownership; callers retain an intern
pin when reading an interned string. Copy destinations are private disjoint
scratch. Scalars remain unchanged on failure; after accepted copy entry the
`copied` count instead reports stores already completed, even when a later poll
fails. A rejected entry leaves that count unchanged.

The fixed tariff is T=224. Entry and final publication each pay T. C-string
inspection pays T per byte including its terminating NUL. Comparison pays T
per unsigned byte pair up to and including the first mismatch. Copy and FNV1a
hash each pay separate T for a LOAD and a STORE/update. Search pays T for each
candidate plus T for every compared pair (including partial failures). Search
uses subtraction-based bounds, returns `0,false` when absent and `0,true` for
an empty needle; an oversized needle has no candidate or pair cost.

Successful-path independent budget formulas:

| Operation | Exact work |
| --- | --- |
| C-string of n non-NUL bytes | 224 × (n + 3) |
| Compare inspecting k pairs | 224 × (k + 2) |
| Copy/hash of n bytes | 224 × (2n + 2) |
| Search with c candidates and p inspected pairs | 224 × (c + p + 2) |

The source regions reuse the existing saved-ID guard (G9), guarded debit
prefix (C13) and tail (T26/T29). The helper source audit allows at most 128
additional actions for validation, scalar loads/arithmetic, loop/child/caller
transitions, refused-debit return and output publication; 9+13+29+128=179 is
below T224 and quantum256. This is a conservative source-action allowance,
not an instruction count or a wall-time claim. Hook bodies are test-only;
revalidation after them remains production control logic. A LOAD never funds
its later STORE/update. Final guarded polling occurs before scalar publication.
Invalid bootstrap is fixed-size and reads neither source nor output.

`test_evaluation_expression_bytes.c` independently specifies literal exact and
one-short budgets, known FNV vectors, early mismatch and nested partial search,
unsigned/UTF-8/embedded-NUL byte values, empty inputs, invalid admission,
LOAD-before-STORE denial, cancellation at every checkpoint and final cutoff,
controller replacement without successor debit/hook invocation, and a real
pin-held retired intern owner. The large length case stops after byte1024,
then retries the same immutable input successfully. Empty spans still pay
entry and final charges. No UTF-8 decoding is claimed by these byte helpers.

## Transactional intern prerequisite (unit 3)

`wl_intern_control_put_attempt` adds a separate attempt-bound transaction;
raw `wl_intern_put_checked` is unchanged. Both governed and ungoverned tables
are supported. An ungoverned table uses the explicit final guarded cutoff;
a governed table also uses the existing attempt-bound reservation commit.
Caller ownership (or a matching pin), immutable preparation input, stable
governor attachment and exclusive output are required for the whole call.
A nonzero return leaves the output untouched.

Preparation scans, copies the NUL, then hashes the owned snapshot before taking
the writer lock. All locked comparisons and publication use that snapshot.
Each slot inspection, segment pointer initialization, slot initialization,
migration record/store and destination probe has its own 224-unit region.
Three separately admitted metadata regions cover count/segment/resize policy,
checked byte dimensions and governor/token coherence. Fixed regions also admit
entry, prepared-copy and locked checkpoints, resource caller scaffolding and
first mandatory cleanup before acquisition. No callback can invoke another
locking table operation from a locked test checkpoint. The prepared-copy
checkpoint is explicitly unlocked.

The byte helpers fund their own loops. Intern caller regions separately cover
validation, helper arguments/returns, loop transitions, guarded admission,
allocation returns and error dispatch. Each fixed/probe/initialization region
has a 224-unit source allowance; allocator/mutex bodies are narrow boundaries.
Migration never performs an input-sized raw strlen/hash/memset. Slot placement
for the new string is completed in private storage before committing.

Governed admission reserves the complete replacement footprint while the old
one remains live, exactly as raw `reserve_growth` does:

```
replacement = old_retained - retired_slot_array
              + string_bytes_including_NUL + new_segment + new_slot_array
high_water  = old_retained + replacement
```

The old reservation's identity, COMMITTED state, governor, owner and byte
count are checked under the writer lock before admission. Local destinations
are initialized EMPTY. Commit uses the existing saved-attempt API; after it
returns COMMITTED, ownership moves are guaranteed by those checked invariants.
An impossible move failure aborts rather than returning partial ownership.

Two prepaid fixed regions precede the final cutoff, funding publication and
the first old-resource retirement caller. The indivisible publication suffix
has two straight-line reservation moves (the existing 40-action ceiling each)
and at most112 further caller/local publication actions: a conservative
192-action ceiling, below quantum256. There are no hooks, variable loops or
admissions in this suffix. The count uses the existing release-store protocol.
Old slots and the old reservation are disposed before unlocking, so a following
writer does not see a completed replacement plus its unretired old footprint.
Free/mutex bodies and raw refund/release CAS contention are explicitly narrow
mandatory-cleanup boundaries, not claimed finite by prepaid caller work.
No cleanup hook/poll can charge a successor attempt.

Before commit, a stop/error discards private buffers, rolls back pending credit
and unlocks. Previously published count, slot capacity, IDs and reverse pointer
identities are unchanged after return, and governor totals return to baseline.
A duplicate requires no growth admission, including at an exhausted growth
budget; its speculative copy is disposed after a separate guarded cutoff.
ENOMEM, EOVERFLOW, EINVAL, `WL_INTERN_ERR_MEMORY_BUDGET` and control dispositions
remain distinct. Advisory status retains the governor's actual finite-limit
semantics; it does not imply that a configured usable limit can be ignored.

Independent ungoverned work fixtures (T=224):

| Fixture | Exact work |
| --- | --- |
| First `x`, allocating64 reverse pointers | 95T =21280 |
| Duplicate `x`, one probe | 30T =6720 |
| `c19` then unique `c42`, both in bucket32 | 50T =11200 |
| `key00`..`key47` then `next`, resize64→128 | 1373T =307552 |

The resize oracle counts10 old probes and nine length5 collision scans,
128 slot initializations, 48 length5 migration records, 61 destination probes
and one pending-entry probe; it is not inferred from successful consumed work.
A separate76T boundary stops during the first reverse-segment initialization.
Independent memory tests set high-water exact/one-byte-short at the first
segment, resize and second segment; denied calls retain the original footprint
and retry after raising the limit. Collision values, high-byte raw/controlled
lookup, empty/UTF-8/NUL compatibility, allocation failure, copy/comparison and
commit-cutoff stops, controller rebind, interrupted duplicates and real
pin-held owner retirement are exercised. A second raw-writer thread is joined
at the unlocked prepared-copy boundary, proving duplicate serialization
without racing hook state. Late cancellation is tested at the first subsequent
observable boundary, not by adding a hook inside irreversible publication.
This unit does not admit any additional expression tag.


## First scalar domain: strings and comparisons (unit 4)

A separate owned scalar bytecode snapshot admits VAR_STRING, CONST_STR and the
six string comparisons, delegating ordinary numeric instructions to the existing
numeric decoder/step. The numeric owner and its tariffs remain unchanged.
FILTER and MAP try numeric compilation first and only try scalar compilation
when it reports unsupported without an error. No raw evaluator fallback runs.
The scalar owner caches the actual capped-stack producer for type inference;
aggregate no-ops, zero underflow and ignored excess pushes retain raw behavior.
Compilation does not intern literals. Runtime literals preserve their complete
payload span independently of the NUL-terminated intern value.

The synchronous host borrows the program's intern and full extension/worker/
session context from its live session/plan. It never changes that intern's
attached governor to match a later session. Scalar bytecode and temporary copy
reservations belong to the host governor; retained symbols belong to the
intern's governor. An interned literal may survive a later cancelled host;
that published symbol is distinct from temporary credit, which must retire.
Raw CONST_STR put failures map to ENOMEM; scratch admission retains ENOSPC,
overflow and control dispositions. Semantic malformed evaluation rejects a row;
control/allocation errors leave scalar output arguments unpublished.

Each scalar region pays T224 and revalidates its saved attempt after the test
checkpoint. Phases70–72 cover tag/length/name decoding; each decimal digit and
encoded byte is separately metered. Opaque literal payload skipping is constant
work; its complete owned copy uses the byte primitive's per-load/store tariff.
Phases73–79 cover compile traversal, allocation/first cleanup, final ownership
publication and cached type lookup. Phases80–88 cover literal preparation,
per-instruction dispatch, string lookup/push, comparison publication and final
output. Variable C-string lengths, literal copies and pair comparison execute
only through the bounded primitives. Their regions pay their own costs, not
an input-sized debit followed by a raw traversal.

The fixed scalar regions reuse G9+C13+T29 and allow128 additional source actions
for branch/stack/metadata/child-call transitions (179 total <224). The finite
post-commit owner publication includes one40-action reservation move and fewer
than64 scalar assignments/branches, below256. Allocator/free bodies and mandatory
reservation cleanup/refcount contention are narrow host boundaries; no
cancellable variable engine loop is hidden there. MAP's lazily allocated scalar
pointer array preserves the numeric-only table layout/debits. Four separately
charged256 grants prepay its physical and credit retirement turns; each slot
initialization separately pays192. Cleanup visits only the published initialized
prefix and retires numeric/scalar owners and the two arrays independently.

`evaluation_expression_strings` compares all six operators against raw bytecode,
checks embedded NUL, unsigned bytes, capped stacks, invalid IDs, null context,
owned snapshots, exact/one-short work and memory bounds, allocation failures,
decode/copy/evaluation stops, saved-ID replacement and credit balance. Generated
session FILTER checks every comparator with exact relation values and positive/
empty results. Generated MAP mixes projected strings, arithmetic and a literal;
the parser does not admit a comparison as a head argument. Session tests stop
after earlier rows/columns and during lazy initialization, assert no premature
relation/events, retry, verify exact insertion/deletion events and a subsequent
no-op call. Empty MAP and shared-plan sessions with different governors exercise
context ownership. These constitute `unit4-sessions`; recursive/pipeline host
qualification and all other open function/library/extension rows remain pending.


## Read-only string functions (unit 5a)

STRLEN, STR_ORD, CONTAINS, STR_PREFIX and STR_SUFFIX enter the scalar decoder
and retain raw ID-based operands; numeric-carried valid intern IDs are valid.
With no intern the expression returns0 for all five. With an intern, absent IDs
produce-1 for length/ordinal and false for predicates. Empty ordinal returns-1.
The predicates return true for empty needles only when both IDs resolve.
Prefix and suffix prove needle length <= source length before offset/comparison.
No function allocates or interns a result; literal operands retain unit4 policy.

`wl_columnar_expr_utf8_next_attempt_controlled` preserves the raw permissive
UTF-8 step. It accepts overlong/surrogate/out-of-range values, skips an invalid
lead by1, and advances by i on an invalid continuation at i, leaving that
inspected byte for the next step. It stops inspecting at the first NUL; empty
input returns advance0/codepoint-1. Outputs are unchanged on stop. Entry and
final publication each pay224; each actual byte inspection pays224. Thus an
I-byte inspection has exact work224*(I+2), including an inspected terminating
NUL. No more than four bytes are inspected in one invocation. Counting repeats
steps until advance0, charging repeated inspections when a bad continuation or
NUL is revisited. Phases90 entry,91 byte,92 final expose saved-attempt checks.
Reader phases93/95 separately fund caller entry/publication; phase94 pays one
224 region per UTF-8 step, including the terminating step. Reader-only work
(excluding expression decode/dispatch) is224*(2+sum(3+I_i)) for STRLEN and
224*(5+I) for ORD. Predicates pay two caller regions plus existing exact length
and comparison/search tariffs; an oversized needle never compares a pair.

Each fixed region uses the prior G9+C13+T29 bound and at most128 additional
source actions for bounded decode branches, one byte/stack operation, checked
lookup and loop/child transitions:179<224<=256. UTF-8 continuation reads each
have a distinct admission; variable string lengths/search use existing byte
primitives. Final output publication follows the saved-attempt guard. No raw
strlen/strstr/strncmp/memcmp or whole-string raw UTF-8 counting is called.

The string test target adds exact/one-short UTF-8 steps for empty, ASCII,
2/3/4-byte, invalid lead/continuation, permissive values and every truncation.
Inputs end exactly at their NUL under sanitizers. Direct raw parity covers all
five functions with ordinary/invalid/zero-underflow integer-carried IDs and null
context, plus cached result types. It cancels each reader inside its traversal,
retries the same owner, and replaces attempts during continuation/final cutoff
without scalar publication or successor debit. Generated FILTER and MAP cover
all five functions with exact positive/false/empty results, mixed string and
numeric columns, stops after earlier work, retry, signed deletion and a later
no-op call. MAP additionally forces deterministic mid-traversal budget denial.
Observers are installed before the initial empty step. Owner/intern resource
retirement is checked. `unit5-sessions` is this evidence; recursive/pipeline
string-domain qualification and all other open tags remain outstanding.


## Checked decimal prefix (unit 5b)

TO_NUMBER preserves the checked raw conversion: absent intern is a semantic
failure, absent/empty ID produces0, a missing numeric prefix produces0, trailing
suffix is ignored, and an out-of-int64 prefix is a semantic failure. FILTER
rejects such a row; MAP returns ERANGE without publishing the partial result.
The scalar cache records unary INT64 output. Integer-carried IDs remain valid.

The decimal primitive owns input traversal rather than calling strtoimax on an
unbounded string. It uses locale isspace on each unsigned byte, optional sign,
ASCII decimal digits and unsigned magnitude limits INT64_MAX/INT64_MAX+1. The
ctype classification is a fixed-size library boundary. Overflow is latched
while the complete remaining digit prefix is still traversed and charged;
no suffix beyond its first byte is inspected. The final saved-attempt poll
precedes either output publication or ERANGE. Nonzero returns leave output
unchanged; negative control stops and identity loss remain distinct.

Entry96 and final99 each pay224, every inspected byte97 pays224 and every
digit accumulation98 pays224, including digits visited after overflow.
Independent exact work is224*(2+I+D) for I byte inspections and D digits.
TO_NUMBER host lookup/dispatch pays separate100/101 regions. Each region uses
the prior179-action conservative source ceiling (<224<=256); one ctype lookup
or unsigned update plus branch/child/loop transitions stays within128 added
source actions. No variable raw conversion or new allocation occurs.

Tests specify literal exact/one-short fees for empty/no-prefix/sign, decimal
suffix, six C whitespace classes, INT64 limits and40-digit overflow. They stop
after overflow and at final cutoff, replace attempts during digit/final regions,
and assert no output or successor debit. Raw-expression parity covers every
single-byte prefix and checked limit/error cases under available C, C.UTF-8
and en_US UTF-8 locales, restoring LC_CTYPE afterward. Generated MAP checks
mixed projection/arithmetic values, empty output, deterministic cancellation
and budget denial, nonpublication/retry, exact signed deletion, subsequent calls
and governor balance. A real overflowing MAP fails ERANGE then recovers after
the bad input is removed; generated FILTER rejects overflow while retaining
valid/zero-prefix rows. `unit5-decimal` records this session evidence; additional
host qualification and other open domains remain outstanding.


## String XXH3 and CRC (unit 5c)

HASH_S and CRC32_ETH_S/CRC32_CAST_S are unary INT64 scalar instructions.
A valid intern ID selects its C-string bytes excluding NUL. Absent IDs or
absent intern/context select the operand's actual native sizeof(int64_t)
object bytes; they never select an empty string. Embedded-NUL literals digest
their intern value, independently of the full typed payload. CRC results are
zero-extended uint32; XXH3 uses the raw uint64-to-int64 conversion. The same
fixed debug diagnostic is retained for absent symbol IDs. Its logger body is
a narrow synchronous boundary around a fixed message and int64 formatting.
Numeric HASH/CRC decoding, evaluator and established tariffs are unchanged.

The separate expr_hash.c translation unit includes XXH_INLINE_ALL before any
xxhash include. The pinned xxHash0.8.4 state layout and implementation therefore
share one header identity, avoiding the vendor prohibition on exposed stack
states crossing a dynamic-library ABI. State uses vendor-declared alignment
and XXH3_INITSTATE, never exposed fields or a hidden heap state. Wrapper symbols
are internal; no XXH implementation symbol is newly exported. Every update
accepts at most64 bytes. Reset/update/digest are explicit vendor cooperation
boundaries even if the compiler/LTO inlines them: their bodies are not claimed
as engine source actions funded by224. An update may also process the fixed
256-byte internal buffer; digest may consume retained stripes. There is no
input-sized whole-span update or one-shot fallback and no cancellation claim
inside a synchronous vendor routine.

XXH3 engine work is224*(4+ceil(n/64)): entry120, INITSTATE/reset caller121,
each update caller123, digest caller125 and final cutoff127. Zero-cost guarded
post-call checkpoints122/124/126 immediately observe stops and saved-ID changes.
Every pre-call hook revalidates before invoking the vendor; final publication
follows the saved-ID cutoff. The caller source envelope is G9+C13+two T29 tails
plus at most99 local/branch/loop/child-call actions,179<224<=256, excluding only
the named vendor bodies. INITSTATE's fixed stores remain engine work.

CRC reuses immutable wl_columnar_expr_crc_eth_bytes/cast_bytes, with no lazy
table generation. Entry114 and final117 each pay224. Every source LOAD115 and
separate table/update116 pays224, so independent exact work is224*(2n+2).
The common179-action envelope covers one load or update and caller transitions.
Scalar102/103/104 regions separately cover ID/native-byte preparation, child
selection and final push. All helpers borrow immutable spans, allocate nothing,
retain no context and leave outputs unchanged on nonzero returns.

Tests compare streaming boundaries0/1/8/16/17/63/64/65/127/128/129/240/241/255/
256/257/1024/4096 against the linked one-shot oracle, with independent literal
exact/one-short budgets. Empty XXH3 has fixed vendor-vector golden
0x2D06800538D394C2; CRC123456789 goldens are0xCBF43926/0xE3069283. Stops and
attempt replacement cover each pre/post vendor call, final cutoff, CRC LOAD/
update seams and long traversal. Direct raw parity covers empty/large/malformed
symbols, native fallback including INT64 limits, underflow/capped stack,
embedded NUL, cached type and reuse of a stopped compiled owner.

Generated FILTER and MAP assert actual _S tags and compare exact relation
values. MAP mixes string projection, all three digests and numeric arithmetic;
empty output, mid-work cancellation/exhaustion after earlier columns, retry,
exact signed deletion, subsequent calls and no new result interning are checked.
Shared-plan sessions with different governors check ownership retirement.
Legacy retained serial symbol-HASH/CRC refusal fixtures now require successful
publication and exact raw-reference values, including native fallback IDs.
Production/benchmark/standalone source lists include the helper. Native and
ASan/UBSan plus mbedTLS-on optimized/non-LTO and default mbedTLS-off/LTO builds
qualify this bundle. Matching production size profiles measure .text from
850089 to853493 bytes (+3404), informational under the user's size policy.
`unit5-digests` records this evidence; crypto, allocating strings, extensions
and additional recursive/pipeline domain qualification remain outstanding.
