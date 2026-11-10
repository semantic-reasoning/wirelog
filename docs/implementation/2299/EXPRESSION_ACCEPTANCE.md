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
| `WL_PLAN_EXPR_STR_FN_CAT` | open | admitted | unit6-builders |
| `WL_PLAN_EXPR_STR_FN_SUBSTR` | open | admitted | unit6-selectors |
| `WL_PLAN_EXPR_STR_FN_CONTAINS` | open | admitted | unit5-sessions |
| `WL_PLAN_EXPR_STR_FN_STR_PREFIX` | open | admitted | unit5-sessions |
| `WL_PLAN_EXPR_STR_FN_STR_SUFFIX` | open | admitted | unit5-sessions |
| `WL_PLAN_EXPR_STR_FN_STR_ORD` | open | admitted | unit5-sessions |
| `WL_PLAN_EXPR_STR_FN_TO_UPPER` | open | admitted | unit6-builders |
| `WL_PLAN_EXPR_STR_FN_TO_LOWER` | open | admitted | unit6-builders |
| `WL_PLAN_EXPR_STR_FN_STR_REPLACE` | open | admitted | unit6-replace |
| `WL_PLAN_EXPR_STR_FN_TRIM` | open | admitted | unit6-selectors |
| `WL_PLAN_EXPR_STR_FN_TO_STRING` | open | admitted | unit6-builders |
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

The inline helper deliberately selects the scalar backend on GCC/AArch64 in
all build profiles, overriding an external XXH_VECTOR selection there. The
pinned NEON implementation directly dereferences a 16-byte vector at secret
offsets that can be only 8-byte aligned. Dependency-target compiler options do
not protect this inline consumer. Scalar access keeps alignment checks enabled
and preserves exact XXH3 digests, stack ownership, 64-byte updates and caller
tariffs. Other compiler/architecture pairs retain the default vendor backend.
Offset spans 0 through 15 cover long inputs and streaming boundaries against
the linked one-shot oracle, with exact and one-short budgets.

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


## Owned string results: CAT/case/integer format (unit 6a)

CAT, TO_UPPER, TO_LOWER and TO_STRING enter the scalar owner with STRING result
metadata. They pop the same integer IDs/values as raw expressions. Absent
intern returns0 for CAT/TO_STRING and the original operand for UPPER/LOWER.
With an intern, absent source IDs return-1 as a successful value, not semantic
rejection. CAT and case conversion always allocate/intern, including empty or
unchanged results; no new allocation-free shortcut changes their failure paths.
UPPER/LOWER are ASCII-only and preserve high/malformed bytes. Checked size/NUL
overflow returns EOVERFLOW instead of unsafe raw allocation arithmetic.

The private builder admits length+1 scratch bytes from the scalar owner's
governor, holds the pending receipt through filling and controlled interning,
then physically frees the buffer before rolling back its credit. The owner
retains that governor throughout; retained symbols remain with the actual
intern governor, which can differ. Region130 prepays first free/refund caller
work before admission/allocation. Allocator/free bodies and mandatory refund
CAS contention remain the previously documented narrow boundaries. Cleanup
checkpoint143 receives NULL and cannot debit a successor attempt. A final
saved-ID cutoff precedes the returned ID and later expression publication.
An interned output can legitimately survive a stop after its own cutoff;
private scratch and owner credit still retire.

For existing intern footprint B, owner P and result storage S=length+1, a
non-resizing new entry requests full replacement B+S while old B remains live.
Same-governor admission high-water is2B+P+2S; with separate governors, owner
high-water is P+S and intern high-water2B+S. These are retained/reserved logical
bytes. Physical scratch S and the intern primitive's speculative copied S are
simultaneously live during preparation, alongside source/owner storage and any
staged intern metadata. The speculative copy's existing unit3 accounting and
mandatory cleanup contract are unchanged. A duplicate has no growth admission
but still prepares its private intern copy. Tests derive the exact/one-byte-
short example from B, sizeof(owner)+bytecode and S=2, not a successful debit.

CAT uses the bounded byte copy twice and a separate NUL store. The case-copy
primitive separately pays224 for LOAD145, ASCII transform146 and STORE147 per
byte, plus entry144/final148: exact224*(3n+2). Its destination is private and
disjoint; copied reports completed stores after accepted entry, even on stop.
The builder's length scan includes the source NUL, then case-copy processes
only non-NUL bytes and region135 stores a known terminator without rereading it.

TO_STRING keeps raw-compatible fixed24-byte stack storage and snprintf with a
fixed format/int64 argument. The formatter's narrow synchronous body emits at
most20 sign/decimal bytes plus NUL; no input-sized engine traversal is hidden.
Entry139 and final142 pay224 each, with guarded post-call141. The buffer is
private and may be written before a later stop; length remains unchanged on
nonzero. No new heap scratch admission is imposed for this fixed stack path.

Successful builder caller work (excluding expression decode/dispatch, governor
admission and intern child work) is7T plus existing length/copy tariffs for CAT,
7T plus length/case-copy tariffs for UPPER/LOWER, and6T for TO_STRING (T=224,
including its two formatter regions). Regions129/130/131/135/136/137/138 cover
entry, prepared scratch/first cleanup, allocation, NUL, intern caller, post-call
and final result. Each fixed region stays in the179-action envelope; the
positive-error wrapper allowance is192<224<=256. Child traversal funding is
separate from caller and prepaid cleanup, with no unbounded raw copy/transform.

A shared intern-result mapper now preserves positive identity/accounting errors
via an atomic saved-attempt zero-debit poll. Ordinary intern failures retain raw
ENOMEM mapping. A genuine positive engine error precedes a later negative
cancellation, matching the host return policy. This also corrects literal
interning's former masking of sticky accounting EOVERFLOW. Phase150 is guarded
before its test hook and atomically revalidates afterward; it never debits a
successor. Tests assert no consumption wrap, exact final report/disposition,
unchanged scalar outputs, complete cleanup and reset/retry, including an
allocation failure followed by cancellation.

Tests cover raw null/invalid-ID/default/underflow semantics, UTF8/high bytes,
INT64 extrema, STRING schemas, exact/one-short case/format fees, scratch and
intern high-water on one or two governors, allocation failures even for empty/
unchanged results, and TO_STRING success with no remaining owner scratch budget.
Stops/rebinds cover transform stages, scratch acquisition, nested intern prep/
probe and post-intern/pre-expression cutoffs, distinguishing retained symbols
from temporary leaks. Generated FILTER and MAP select each function, check
positive/false/empty results, mixed numeric/string columns, mid-second-row
cancellation/exhaustion, exact signed deletion and a later no-op call. Shared-
plan sessions exercise different scratch/intern governors and final balances.
`unit6-builders` records this evidence; SUBSTR/TRIM/REPLACE, crypto/extensions
and further recursive/pipeline qualification remain outstanding.


## UTF-8 substring and locale trim (unit 6b)

SUBSTR has ternary stack bookkeeping; TRIM is unary, both with STRING result
metadata. They retain raw C-string/ID semantics rather than using a literal's
full typed payload. Missing intern returns0 for SUBSTR and the operand for TRIM.
Invalid IDs, negative indices and beyond-end start are successful-1 values,
not new semantic failures. Start exactly at end is valid, even for length0;
beyond end is invalid even for length0. A valid selected span always allocates
and interns, including empty/full/unchanged spans, preserving raw failures.

The UTF-8 selector uses the previously qualified permissive step, retaining
invalid-continuation advance and repeated inspection. Separate start/length
walks never add signed start+length; huge requested counts stop at NUL. Entry155
and final158 each pay224, and every walk transition156/157 pays224 in addition
to the step's224*(I+2) for its actual I byte inspections. Independent work is
224*(2+sum(I+3)) over visited steps, including terminating steps when visited.
Zero length can avoid a second walk; negative indices require no source read.
Nonzero leaves all metadata unchanged; a completed invalid selector changes
only valid=false, leaving its span outputs unchanged.

TRIM matches raw unsigned-byte locale isspace, not ASCII case transformation
or Unicode whitespace decoding. Each front152/back153 inspection pays224;
entry151/final154 pay224 each, and the original full C string is measured with
the bounded length helper. For n non-NUL bytes, F front and B back inspections,
exact work is224*(n+F+B+5). F includes the first nonspace/NUL; B includes a
nonspace stopping byte when one is inspected, and is0 after an all-white front.
Ctype classification is the existing fixed one-byte library boundary.

Selector caller159 funds ID/context dispatch. Valid spans enter160 and reuse
the unit6a scratch, copy/NUL, interning and cleanup/cutoff protocol. This adds
7T span-caller work plus byte-copy/governor/intern children (T=224), separately
from selector work and caller159. Source/pin lifetime and different owner/intern
governors remain unchanged. Each fixed region fits the179-action envelope;
each walk charges its own caller transition and UTF-8 child, with no raw
strlen/UTF-8 walk, full-span debit, or unmetered copying. The selected source
remains immutable through both span computation and copying.

Independent exact/one-short vectors cover end/beyond-end, zero/negative/MAX
counts, every relevant malformed/truncated/permissive UTF-8 shape, six C spaces,
all-white/empty strings and nonspace high bytes. Stop/rebind tests cover every
selector phase with unpublished metadata and no successor debit. Raw parity
runs under available C/C.UTF-8/en_US locales and restores LC_CTYPE. A capped
stack with SUBSTR followed by integer/float pushes verifies ternary cached
producer depth; embedded-NUL literals retain C-string semantics. Valid empty
operations still fail their real scratch allocation; invalid substring paths
remain allocation-free as raw does.

Generated FILTER/MAP exercise both functions, exact STRING/INT64 schemas and
relation-qualified contents (including the raw-1 substring cell), positive/
false/empty results, interruption inside a second-row UTF-8 continuation,
deterministic work denial, retry, exact signed deletion and subsequent calls.
Owned-scratch stops/rebinds before and after intern publication distinguish
retained results from temporary credit. Shared-plan sessions with distinct
governors assert final balances. `unit6-selectors` records this evidence;
REPLACE, crypto/extensions and further recursive/pipeline qualification remain
outstanding.


### Unit6c: nonoverlapping replacement

REPLACE validates all three symbol IDs before either allocation-free shortcut.
A missing intern returns the original source operand; invalid IDs yield the
ordinary value -1. Empty patterns and zero matches return the original ID,
even when both owner/intern budgets have no spare byte. A nonempty match still
allocates and interns when the replacement equals the pattern. Matching uses
C-string spans, byte equality and nonoverlapping advancement by pattern length.
Source and pattern lengths are bounded; replacement traversal is delayed until
at least one match exists. Both passes use the same immutable source/pattern.
No match vector, raw strlen/strstr/memcpy or raw expression fallback is used.

Phases170/171 admit entry and each of three reverse lookups;172/173 admit each
first-pass search and accepted match;174/178 admit checked sizing;175/176
admit each second-pass search and match;177 admits tail consistency and182
admits final publication. Every region costs T=224. The sizing primitive costs
2T on success, leaves output unchanged on stop, and checks removed bytes,
replacement multiplication, addition and room for NUL. Impossible metadata
returns EINVAL; unrepresentable size returns EOVERFLOW before allocation.
For M>=1 matches the scalar caller costs T(4M+15), separately from three
length traversals, both bounded find passes, prefix/replacement/tail copies,
and nested interning. Every search includes its failed partial comparisons.
Monotonic positions stay within source length; checked remaining-output
subtractions and the final match-count/written-size checks protect both passes.

Scratch follows unit6a ownership and cleanup: reservation overlaps the live
owner/source, and survives through speculative intern preparation. Physical
scratch is freed before credit rollback, including cancellation during either
search/copy pass. Published intern entries survive a later cutoff legitimately.
Raw-reference tests cover aliasing, adjacent/overlapping candidate matches,
empty source/pattern/replacement, growth/shrink, repeated-prefix misses, high
bytes, null intern and invalid IDs. Exact448/one-short447 sizing tests and
synthetic overflow cases need no giant allocation. Stops/rebinds inside both
search passes assert unchanged caller outputs/status, scratch retirement, no
successor debit and exact retry. Generated FILTER/MAP verify STRING metadata,
mixed numeric columns, empty/false/positive results, cancellation before
publication, retry, signed deletion and subsequent no-op calls.
`unit6-replace` records this evidence. Crypto/extensions and further actual
recursive/pipeline domain qualification remain pending.

### Latest-main JOIN integration

The foundation merge incorporates main `c9a1dcd9c8aa4b67f3dd58e4b7fdb1134aa14612`
(PR2344), preserving saved-ID atomic charging in shared expression/filter
helpers, all three attempt-bound append-batch acquisitions, SOURCE financing
and native cleanup authority. Forward integration retains the scalar lazy
array/initialized prefix, both program kinds in inert checks, and independent
physical/credit retirement. The two inventory scripts retain scalar admission
and mutation assertions while every text operation explicitly specifies UTF-8.

`joined_strings` in `test_evaluation_expression_strings.c` parses a real keyed
JOIN with a numeric FILTER prefix followed by string CONTAINS FILTER and
REPLACE MAP. Pipeline EXCHANGED/AFTER_ACK hooks prove the numeric prefix actually
ran; string scalar hooks prove the materialized suffix actually ran afterward.
The existing numeric-only pipeline classifier remains a valid optimization
boundary, with a controlled context-bearing suffix and no raw fallback.
Both ordinary and forced-full JOIN selectors exercise empty, accepted/rejected,
signed deletion and subsequent no-op results with exact STRING/INT64 cells and
relation-qualified event counts. Earlier standalone suites remain separate
regression evidence rather than proof of this intersection.

Deterministic stops cover post-result interning cancellation/work exhaustion,
scalar-owner allocation, first lazy scalar-array slot initialization, and
post-intern Original finish plus successor creation. Stopped paths publish no
relation/events, finish native cleanup with no active/pending frame or cleanup
reservation, preserve the Original report and consume zero successor work.
After closing that unused successor, reset/retry produces exactly the expected
rows/events; session/intern destruction returns governor reservations to zero.
An output-limit failure stays EOVERFLOW with no publication and succeeds after
raising the limit. Strict policy deliberately refuses an unconsumed suffix;
both raw and controlled sessions return ENOTSUP on the same generated plan,
and disabling strict policy permits the bounded route. Dedicated upstream
strict-eligible tests remain the positive evidence for those separate routes.
This integration does not qualify all recursive/aggregate/timestamp or future
crypto/extension intersections; those obligations remain explicit.


### Failed hot scalar successors: terminal correction boundary

Existing TO_NUMBER on clean5fae reproduces a shared hot-MAP defect independently
of crypto: after a successful empty step, a later ERANGE rolls back all private
storage but leaves the backend PENDING, so declared-input correction returns
EBUSY. A separate TO_NUMBER Original-END fixture also exposes retained native
child cleanup; that second obligation is a separate recovery unit.

A failed successor now retains a terminal retry receipt only after genuine
child ACK (or authenticated no-frame rejection), complete uncommitted private
transaction disposal, and the matching Original FINISH receipt. The receipt
binds the real session/step identity, immutable plan, public registry/count,
Original control identity and attempt ID, and the child's closed generation.
A 192-unit maintenance prepayment funds registration before freeing transaction
finance. The existing independent SOURCE FINISH wrapper is also selected for
this ordinary non-JOIN boundary: its real224/240 admissions separate Original
FINISH from terminal publication, without polling a sticky or ended Original.
New registration work fits176<=192<=quantum256; final publication uses the
existing SOURCE240 terminal metadata stage. Terminal-proof queries are fixed metadata and add no input-sized scan. Existing
public admission selection and declared-EDB mutation validation are unchanged.

A quiescent authentic receipt exposes RETRYABLE, allowing only declared EDB
correction at the existing facade gate. Wrong names/widths, failed or zero-row
requests do not promote it. Only a real successful admitted mutation exposes
DIRTY; its trusted commit notification refreshes registry metadata because an
incremental removal can create a retraction relation. Read-only admission never
performs that refresh. A real new attempt consumes the receipt, reuses the
existing bounded private-successor reset/rebuild path, and revalidates inputs.
No successful result is fabricated: retry without correcting bad input fails
again, preserving the prior committed image and notification baseline.

The regression covers empty/nonempty previous commits, ERANGE/ENOMEM and
sticky accounting EOVERFLOW with cancellation, exact relation generations and
signed events, wrong/zero mutations, repeated correction/remove-reinsert,
frozen reports, unused successor protection, evaluating snapshot after
correction, subsequent no-op and governor retirement. Native Original-END
retained-child authority remains separately tracked until its own repair.
