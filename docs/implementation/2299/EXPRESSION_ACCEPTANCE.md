# Bounded expression acceptance (#2299)

Baseline: main `54f76fb7`. This ledger tracks O02/O03 and C01/C02. It is an
inventory and implementation contract, not a statement that the issue is closed.
PR #2344 is still open at this baseline; its JOIN host work must be preserved
when integrating scalar domains. #1819 and #1953 have merged. Existing numeric
qualification remains in `docs/implementation/1821/SERIAL_COVERAGE.md` and
`test_evaluation_numeric{,_operators,_plan}.c`; it is not reimplemented here.

PR #2375's integration with main `3bdf3c45` qualifies loaded ordinary seed
STEPs against the head-input fixtures in `evaluation_facts_sources`. It commits
the preserved seed shadow's `base_nrows` only at successful publication, so a
later observed STEP reports new rows rather than reporting an unchanged seed
again. Precommit cancellation, reset/retry, unchanged STEPs and host head
insert/remove overlap are covered. The later bare-head increment below admits a genuinely empty unloaded head;
evaluating SNAPSHOT remains outside this qualified intersection. The following ordinary routing increment
also admits the fully declared observed identity-MAP fixture: the same immutable
seed shape hint now selects the existing general host before legacy observer
preflight. DIRECT-only routing, paid canonical name matching and the C1/batch/
SNAPSHOT exclusions remain distinct; the hint grants no actor or Native owner.

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
| `WL_PLAN_EXPR_EXTENSION_CALL` | open | admitted | unit8-extensions |
| `WL_PLAN_EXPR_EXTENSION_CALL_ABI` | open | admitted | unit8-extensions |
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
| `WL_PLAN_EXPR_ARITH_MD5` | open | admitted | unit7-digests |
| `WL_PLAN_EXPR_ARITH_SHA1` | open | admitted | unit7-digests |
| `WL_PLAN_EXPR_ARITH_SHA256` | open | admitted | unit7-digests |
| `WL_PLAN_EXPR_ARITH_SHA512` | open | admitted | unit7-digests |
| `WL_PLAN_EXPR_ARITH_HMAC_SHA256` | open | admitted | unit7-hmac |
| `WL_PLAN_EXPR_ARITH_UUID4` | open | admitted | unit7-uuid-numeric |
| `WL_PLAN_EXPR_ARITH_UUID5` | open | admitted | unit7-uuid-numeric |
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
| `WL_PLAN_EXPR_STR_FN_UUID5_RFC` | open | admitted | unit7-uuid-rfc |
| `WL_PLAN_EXPR_CMP_STR_EQ` | empty-only | admitted | unit4-sessions |
| `WL_PLAN_EXPR_CMP_STR_NEQ` | empty-only | admitted | unit4-sessions |
| `WL_PLAN_EXPR_CMP_STR_LT` | empty-only | admitted | unit4-sessions |
| `WL_PLAN_EXPR_CMP_STR_GT` | empty-only | admitted | unit4-sessions |
| `WL_PLAN_EXPR_CMP_STR_LTE` | empty-only | admitted | unit4-sessions |
| `WL_PLAN_EXPR_CMP_STR_GTE` | empty-only | admitted | unit4-sessions |
| `WL_PLAN_EXPR_ARITH_HASH_S` | open | admitted | unit5-digests |
| `WL_PLAN_EXPR_ARITH_CRC32_ETH_S` | open | admitted | unit5-digests |
| `WL_PLAN_EXPR_ARITH_CRC32_CAST_S` | open | admitted | unit5-digests |
| `WL_PLAN_EXPR_ARITH_MD5_S` | open | admitted | unit7-digests |
| `WL_PLAN_EXPR_ARITH_SHA1_S` | open | admitted | unit7-digests |
| `WL_PLAN_EXPR_ARITH_SHA256_S` | open | admitted | unit7-digests |
| `WL_PLAN_EXPR_ARITH_SHA512_S` | open | admitted | unit7-digests |
| `WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS` | open | admitted | unit7-hmac |
| `WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI` | open | admitted | unit7-hmac |
| `WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS` | open | admitted | unit7-hmac |
| `WL_PLAN_EXPR_ARITH_UUID5_SS` | open | admitted | unit7-uuid-numeric |
| `WL_PLAN_EXPR_ARITH_UUID5_SI` | open | admitted | unit7-uuid-numeric |
| `WL_PLAN_EXPR_ARITH_UUID5_IS` | open | admitted | unit7-uuid-numeric |

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

Preserve the original library call route and refusal semantics. Prefer bounded
updates only after provider equivalence is qualified: compute and multipart
providers may support or reject different operations. MD5/SHA use original
PSA hash_compute; HMAC preserves original import/mac_compute; UUID5 preserves
its original multipart sequence. Charge engine framing and buffer preparation
separately. Named synchronous provider bodies may be input-sized and
uninterruptible, including provider-owned allocations; they are outside the
engine quantum, not funded engine loops. Never claim cancellation inside them.
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
child cleanup; that independent recovery unit is specified below.

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
retained-child authority is qualified by the separate repair below.


### Authentic ordinary unary cleanup prerequisite

The unchanged existing-TO_NUMBER reproducer on clean5fae ends Original during a
later row and starts a successor: EINVAL previously retained TABLE_PROGRAM and
12040 cleanup bytes. The independent unary birth/cleanup record now covers this
actual observed VARIABLE/FILTER/MAP/CONSOLIDATE composition. It does not copy
JOIN/AUTO permission or broaden scheduler/operator eligibility. Registration
occurs before child callbacks; failed registration rolls back the genuine fresh
bootstrap rather than leaving an unregistered callback-capable frame.

The regression extends `test_evaluation_map_recovery.c` with60 actual host cuts:
decimal midrow; scalar-owner allocation/commit/publication; post-result intern;
lazy scalar-array allocation and first initialized slot; two nested literal
intern boundaries; and a witnessed actual FILTER runtime. Each runs END alone,
END plus successor, sticky accounting failure before END (with/without
successor), and evaluating snapshot END (with/without successor). Independent
native metadata exhaustion resumes. Matching Original positive errors survive
later identity failure, frozen reports remain exact, unused successors consume
zero, no callbacks/public rows/events occur after native selection, and private
cleanup/reservations reach real terminal ACK. Retry yields exact values/types,
insert/delete/reinsert signed events and no duplicate no-op events; destruction
returns governor reservations to zero.

Negative tests use a copied context, Original/successor as attempted finance,
a different native thread, and displaced actual indexed receipt while the frame
is active and after physical retirement but before receipt ACK. All preserve
public mutation/snapshot exclusion and actual remaining ownership. Restoring
the test-only corruption permits genuine ACK. Repeated owning histories verify
actual indexed provenance. Thirteen
literal one-short funding histories cover lookup, framed funding, every
registration admission, indexed binding and actual plan-op scan; exact consumed fees are asserted, no child runs before
successful birth, and the fresh rollback releases its real pin/frame/receipt.

The financial/state map is in [MEMORY](../../MEMORY.md). Native grants and SOURCE
allowances remain separate; expression/provider work is not financed by them.
This prerequisite adds no crypto or extension opcode and does not discharge
remaining tag or legal-host qualification rows.

The independent receipt-index qualification uses actual component reservations
and fresh frame allocation at indices0,1,31: literal fees448/640/6400, one-short
refusal, a full missing scan6272, and cancellation/rebinding at entry, slot and
final publication. Outputs and Original consumption remain unchanged on stops;
new financial successors consume zero. Real cancel-begin/frame close and exact
reserved-key release retire all owned resources. A separate authentic public
history performs cold empty hash FILTER without a callback, then installs its
callback and runs an observed hot successor in index1. END+successor cleanup and
retry preserve the older committed FREED0 receipt byte-for-byte, publish the
correct one-row typed result and signed deletion/no-op events, and retire only
the actual new owner. Existing string digest FILTER tests also cover that legal
history unchanged. A wider projection list sizes birth capacity from its actual
head op count; per-op and per-slot work remains individually admitted.


ACK-completed boundary qualification ends Original in the actual successful
receipt-ACK financial-finish hook. The native caller prepays192 before ACK and
retains its genuine key/index/result/generation/serial journal across that hook.
Four independent SOURCE192 proofs plus128 publication authenticate the ACKED
transfer into private disposal without another ACK or freed-context access.
Five literal SOURCE limits411/603/795/987/1115 refuse each boundary with consumed
work220/412/604/796/988. Every history observes exactly one genuine ACK, no result
or signed events before failure, zero active/pending cleanup reservations after
retirement, an unchanged Original report and an unused successor with zero work.
Closing that successor and resetting retries the actual input successfully with
exact typed results and no duplicate no-op events. Native-grant ACK denial and
all six grant-finish END cuts remain separate from these SOURCE proof denials.


Current-main integration retains LFTJ native and metadata owners, their32/8/zero
packet boundaries and the funded composed ACK. Unary proof rejects LFTJ/JOIN/
CONCAT and never falls back to their authority. Existing SOURCE80 close/rebegin
precedes host tails72/non-unary or96/unary; the latter has real processed lower16
before CLOSE176, bounding outstanding at256. Receipt lookup has a separate actual
224+32 generation settled before native384 or any lookup-result return. Exact
SOURCE256/255/223 and native384/383 cases exercise real allocated/retired receipt
owners, with public indexed-hook and SOURCE-tail END/rebind integration controls.
The integrated financial map is in LFTJ_SOURCE_ACCOUNTING.md; earlier financial
capacity and logical fees are never treated as engine progress or native proof.


## Unit7a: original PSA digest compatibility

The eight native-int64 and symbol MD5/SHA1/SHA256/SHA512 opcodes use the
original per-evaluation `psa_crypto_init` and exactly one `psa_hash_compute`.
Algorithm, input pointer/length and output capacity16/20/32/64 match raw; the
actual length starts0 and must exactly match. Init, compute and wrong-length
failures remain semantic failures with value0. No multipart operation, fallback,
provider acceptance normalization or engine allocation is introduced. Integer
opcodes digest native int64 bytes even when the integer names an existing
symbol. Symbol opcodes resolve the pinned context string and meter its length
up to first NUL; invalid IDs/null context keep raw native-byte fallback.
Disabled builds fail the instruction before operand resolution/scanning, while
earlier bytecode instructions retain their original behavior.

The source proof partitions fixed engine framing from synchronous provider
bodies. Init/compute and provider-owned allocation are explicit uninterruptible
cooperation boundaries, potentially input-sized. Engine scanning and hash-fold
work remain separately bounded; no raw engine expression fallback exists.
The helper allocates no retained state and publishes outputs only after a final
saved-attempt guard. Tests wrap the original API calls to check arguments,
compute-only acceptance, refusal without multipart fallback, provider-memory
refusal and both shorter/longer reported lengths.

Let T=224. ENTRY200, INIT201, COMPUTE203 and FINAL205 are distinct positive
admissions; POST_INIT202 and POST_COMPUTE204 poll the saved Original immediately
after the synchronous call. Existing guard/checkpoint wrappers G9+C13+T29 plus
at most157 fixed caller/branch actions bound each helper region by208<=224<=256;
bootstrap tag/argument selection is at most64. Child hash folding admits its
own byte work. All four digest sizes fit one64-byte hash block, so successful
helper work is4T+5T=9T=2016. Init failure is3T=672; compute failure or wrong
length is4T=896; disabled semantics are2T=448. Tests assert literal exact and
one-short limits and separate call history; a one-short result can occur after
the provider has already completed. These fees never claim provider latency
is bounded by the engine quantum.

Scalar regions206/207/208 separately admit operand preparation, provider entry
and returned-result interpretation. Each fixed region uses the same224 packet
and conservative source208 envelope: scalar wrapper51 plus caller/stack/tag/
lookup/fixed-native-copy fronts at most157. Symbol-length traversal has its
own existing per-byte packets; digest folding has its existing hash packets.
The scalar decoder adds only explicit eight unary tags and INT64 producer
metadata. It preserves existing compiler type/arity checks and the current
GCC/AArch64 scalar XXH3 guard.

`test_evaluation_expression_crypto.c` keeps the original hot MAP failure and
correction fixtures, and checks enabled/disabled helper/raw parity, known digest
vectors, all8 compiler-selected FILTER/MAP opcodes, empty/false/accepted rows,
signed deletion events and typed projected cells. Deterministic pre/post-call
cancel/work/END+successor controls require no publication, frozen Original,
successor0, actual cleanup closure and reset/retry. A populated mixed unary
MAP/independent LFTJ plan additionally checks real exclusive owner registration
at provider return, resource closure, unchanged prior rows, retry and deletion.
The independent SOURCE and native ownership mechanism is the preceding R1/R2
bundle; a provider callback never supplies its own cleanup permission.

The helper is registered in production, bench and both test backend lists, and
in the clean LLVM22 partition. A presence-macro undefined alternate uses `-U`,
not `-D...=0`; real enabled and disabled production builds are each analyzed.
The disabled build never gains a mandatory PSA header dependency for linting.
Unit7a does not qualify HMAC, UUID, extensions or the remaining recursive,
pipeline and worker host intersections.

## Unit7b: original HMAC import/MAC/destroy compatibility

Four HMAC-SHA256 opcodes preserve the original message/key order and independent
native/symbol selection. The helper calls init, prepares the original HMAC /
SIGN_MESSAGE / HMAC(SHA256) attributes, imports the unchanged key, resets
attributes after every import attempt, calls mac_compute with capacity32 and
actual_len0, and attempts exactly one destroy before folding the complete32-byte
MAC. Empty keys retain the original single zero-byte substitution. Import/MAC
refusal, provider memory errors, short/long lengths and destroy failure remain
semantic failures. There is no long-key normalization, multipart route, retry
of provider refusal, inferred key policy or provider acceptance expansion.

Attribute lifetime, imported-key authority and destroy-attempt state are
separate. Ownership is acquired only on successful import, even if a failed
provider writes an ID. Destroy consumes the local authority before the call;
a failed status or postcall control stop cannot cause a second attempt. A real
provider destroy failure may leave provider-owned resources unrecoverable, as
with raw; tests destroy the real key before overriding its status. They do not
claim unconditional provider recovery from an arbitrary destroy failure.

T=224. ENTRY230, INIT231, FIRST_CLEANUP_PREPAY233, ATTRIBUTES234, IMPORT235,
MAC237, NORMAL_DESTROY239 and FINAL241 are eight positive fixed packets. The
unchanged32-byte hash fold contributes5T, so success is13T=2912. Init failure is
3T=672; import failure6T=1344; MAC/wrong-length/destroy failure8T=1792; disabled
semantics2T=448. Post-init232, post-import236, post-MAC238 and post-destroy240
observe saved Original identity immediately after the call/ownership transition.
Every fixed packet uses the existing G9+C13+T29 wrapper plus at most157 fixed
caller actions (208<=224<=256): attribute initialization/setters, fixed native
buffers, ownership booleans, reset/destroy call fronts and error routing contain
no input-sized engine loops. Import/MAC/init/reset/destroy provider interiors
are explicit synchronous cooperation or mandatory-cleanup boundaries.

FIRST_CLEANUP_PREPAY precedes attribute preparation and import. Its bounded
cleanup suffix resets live attributes and consumes one actual imported-key
owner without checkpoints or control debit. Therefore cancellation, accounting
error, Original END or denial of the normal destroy packet still completes
mandatory local cleanup before returning the primary error. Post-destroy and
final stops find consumed authority and cannot repeat destroy. Pre/post/final
control errors keep outputs untouched and outrank pending semantic library
failure; sticky positive accounting error keeps its normal precedence over
later cancellation. Provider fault wrappers are test instrumentation, not
engine control checkpoints in this mandatory suffix.

Scalar242 admits pop/selection; each243 span admission independently resolves
its actual intern string or copies into a distinct caller-owned native buffer.
Existing metered C-string scanning owns each length traversal. Scalar244 splits
message and key preparation;245 guards result interpretation. Fixed fronts
retain the224/208 envelope. The binary producer and result remain INT64; arity
and incompatible output declarations retain existing parser rejection. Disabled
instructions execute preceding bytecode but skip span resolution and all PSA
calls. Existing digest8-tag fees and qualification are unchanged.

Tests retain digest assertions and add RFC/raw vectors, keys0/1/63/64/65/4096
with actual provider refusal parity, all4 scalar/native/symbol/context variants,
13 exact-prefix/one-short boundaries, each pre/post-call cancel/work/END/
overflow seam, provider-memory faults and combined library/control failures.
Wrappers assert exact attributes, bytes, capacity, actual length, reset and
exactly-once key destruction. Actual FILTER/MAP covers all4 tags with typed
values, false/empty controls and signed deletion events. Hot MAP and populated
mixed LFTJ histories require exclusive real ownership, unchanged prior rows,
Original report/successor0, no premature events, resource closure and retry.
Long-message preparation stops after prior rows at the real512-byte scan seam
and succeeds unchanged after reset. Disabled MAP ERANGE permits actual public
input correction; disabled FILTER rejects rows. Inventory qualification is
restricted to these four tags; UUID, extensions and broader host rows stay open.

## Unit7c1: five numeric UUID routes

UUID4 and the four numeric UUID5 operand variants preserve their original PSA
routes. UUID4 calls init and generate_random16, sets version4/variant10 and
copies the first native int64 bytes. Only byte6 version is observable in this
8-byte result; tests do not invent an observable byte8 variant. UUID5 keeps
init, SHA1 setup, optional first header, first complete data update, optional
second header, second complete data update and finish20 with actual_len0.
It never substitutes hash_compute, subdivides provider updates or XXH-folds
the digest. Numeric conversion sets version5/variant10 and copies the native
first8 bytes. Provider bodies remain explicit synchronous boundaries; input
update latency is not claimed to be inside the engine quantum.

Base UUID5 always uses native8+8 unframed bytes even for valid intern IDs.
SS/SI/IS resolve each selected symbol separately, use first-NUL C-string bytes,
and retain native-byte fallback and the original diagnostic for invalid IDs or
missing context. Framed operands use the actual resolved type byte S/I, LE64
length and exact operand bytes. Two distinct caller-owned native buffers stay
stable through the synchronous operation. Each9-byte header is built through
nine separately admitted stores, followed by one complete9-byte update.

Operation state initialization is also engine work. The installed PSA hash
operation is232 bytes; a single runtime aggregate zeroing would not establish
the claimed fixed source bound. The implementation instead copies the exact
static `PSA_HASH_OPERATION_INIT` representation into an uninitialized actual
local operation in separately admitted chunks of at most8 bytes. No active
operation is copied, no all-zero representation is assumed, and no API can
read partially initialized state. STATE_INIT301 separately funds scalar flags
and digest20 initialization. Each318 copy interval conservatively counts at
most12 actions per byte (96), guard51 and a complete64-action fixed allowance
for indexing/loop/return/next-admission:211<=224<256. Other fixed caller regions
retain their208 bound. There is no per-call whole-state assignment or bulk
zeroing hidden outside these cuts.

Let T=224 and I=sizeof(operation)/8 plus1 if a remainder exists. Current ABI
has I29. Pair helper success is(9+I)T unframed and(29+I)T framed, currently38T
and58T. Enabled semantic-fault prefixes addI to the original fixed ordinals:
init5, setup6; unframed firstdata7, seconddata8, finish9; framed firstheader16,
firstdata17, secondheader27, seconddata28, finish29. Each includes exactly one
semantic FINAL315. Disabled pair remains2T and does no state initialization or
operand lookup. UUID4 helper success is4T, init failure3T, random failure4T,
disabled2T. Scalar decode, operand scans/preparation and numeric conversion /
return347/348 are separate funding, not included in pair-helper tariffs.

Abort caller credit302 precedes setup. Setup-attempted state is acquired before
the setup call, so failed setup also follows the original abort path. Any
control stop or provider failure after that point aborts once without engine
checkpoints or successor debit. Abort authority is consumed before the call;
failure cannot trigger a repeated attempt or replace a primary control error.
A successful expected-length finish consumes the operation before postfinish314;
no abort follows a subsequent control stop. Wrong-length success retains raw's
idempotent abort. Fault wrappers perform actual cleanup before overriding its
status; arbitrary provider cleanup failure is not promised recoverable.
Every semantic result/error still passes final saved-Original guard315. Partial
state-copy stops own no provider call, setup authority or abort obligation.

Tests independently check SHA1/native/frame vectors, exact original API
arguments and update counts (including empty/default-fallback scalar inputs),
UUID4 deterministic entropy and actual observable structure, all prefix and
one-short limits, every8-byte state-copy offset, pre/post provider seams,
mid-header construction, wrong lengths and combined abort/control failures.
All nonzero control returns preserve output sentinels and failed flag; frozen
Original and successor0 are checked before actual reset/retry. Scalar tests
include underflow and capped-stack nullary producer behavior. Malformed arity
and incompatible output declarations retain parser rejection.

Compiler-selected FILTER/MAP tests require all5 opcode counters with accepted,
false and empty rows; exact typed values, signed deletion and no duplicate
future-call events; hot prior-row nonpublication, unchanged relation generation,
resource closure and successful retry. Populated independent LFTJ heads retain
exclusive real owners. Stops after UUID4 random and incomplete UUID5 data update
check actual abort0/1; long operand preparation stops at a real512-byte seam
before operation setup. Provider failures map to ERANGE with successful retry.
Real random UUID4 raw/controlled hosts compare structure and full-tuple signed
ledgers, never accidental literal equality. Disabled FILTER rejects rows and
MAP ERANGE permits public input correction. Qualification is limited to these
five numeric tags. RFC STRING UUID5, extensions and remaining hosts stay open.


## Unit7c2: existing RFC UUID5 string expression

Only UUID5_RFC is newly qualified here; extension admission and remaining host
coverage remain open. Raw lookup order is namespace then name, with missing name
rejected before parsing and malformed namespace rejected before the name scan.
The parser accepts exactly36 ASCII bytes, case-insensitive hex and hyphens at
8/13/18/23, with arbitrary namespace version/variant. It reads at most37 bytes
and stops at first NUL. Private16-byte output is copied only after FINAL353.
With T224: ENTRY350, actual LENGTH_READ351, actual PARSE_CHAR352, FINAL353;
valid75T, NULL2T, first NUL at j<36 gives(j+3)T, overlong39T,
first invalid syntax index b gives(40+b)T. Each character region conservatively
counts120 actions plus guard51 and caller40=211<=224. Final fixed memcpy16
counts32 byte actions plus the fixed guard/caller envelope. No aggregate input
scan or partial caller publication occurs.

The reused unframed SHA1 pair hashes namespace16 then the complete name,
including an explicit empty-name update; state-copy and abort contracts remain
unit7c1's(9+I)T (currently38T), not the old9T. Version5 and variant10 modification
has its own scalar admission375. Formatting is ENTRY360,37 STORE361 regions
(including hyphens and NUL), FINAL362:39T. Each store derives one output byte;
the final fixed memcpy37 counts74 byte actions+guard51+caller40=165<=224.
Both helpers preserve output sentinels on nonzero control returns. Semantic
parse invalidity publishes only validfalse after the final guard.

Controlled interning always runs, including duplicate strings, using the actual
intern governor and existing transactional error precedence. Allocation failure
returns ENOMEM without publishing the scalar value. A stop after successful
interning may retain a legitimate persistent symbol, while the row and relation
generation remain uncommitted. STRING result schema is independent of stack
provenance: the existing raw opcode pushes an INT64 intern ID. Raw registered
extension callbacks verify RFC INT64 versus literal/VAR_STRING STRING arguments;
a read-only test observer checks the controlled RFC stack slot. Controlled
extension admission is not implied by this reference test.

Tests cover exact independent helper tariffs/every positive prefix, every
parser/store stop offset, cancellation/work/Original-END-successor/overflow,
raw scalar IDs/NULL/invalid namespaces/names/order, empty and non-UTF8 names,
allocation failure even for duplicates, governor denial and duplicate reuse,
retained-symbol stops and retry. Actual compiler STRING MAP and FILTER hosts
check full/delta/empty/all-false results, typed cells, signed delete/reinsert,
hot prior-row generation stability, malformed namespace correction via public
input APIs, mixed populated LFTJ exclusive owners, real resource closure,
frozen Original/successor0 and retry. Disabled MAP retains ERANGE and public
correction; FILTER rejects without provider work. Existing digest/HMAC/numeric
UUID qualifications and fees are unchanged.

## Unit8a: bounded extension lookup and indexed callback lease foundation

Both extension expression tags remain open. This unit supplies internal
preparation helpers, without invoking extensions or changing scalar admission.
Exact registry representations move to a private noninstalled header; the raw
standalone registry target retains its original registry+thread linkage.
Production, benchmark and both backend test source lists include the separate
controlled translation unit, registered clean in the LLVM22 ratchet.

The caller retains an authentic immutable snapshot during lookup/acquisition.
Encoded name span is at most255 bytes with a readable terminator at the bound;
a larger span is EINVAL before reading or charging. Earlier NUL ignores the
encoded suffix. NULL snapshot/name is a guarded raw semantic miss. The helper
preserves case and trims only space/TAB/LF/CR. It preserves the raw temporary
malloc(length+1), failure-as-invalid-name/missing semantics, and free; there is
no new governor refusal or allocation-free substitution.

With T224, lookup admits ENTRY400, each actual first-NUL READ401, FRONT402,
BACK403, ALLOC404, each COPY_LOAD405 and COPY_STORE406, NUL407, RECORD408,
each compared byte PAIR409 and FINAL410. R=N+1 includes first NUL. Empty has
F=B=0; alltrim has F=N,B=0; otherwise F=leadingtrim+1,B=trailingtrim+1.
Stopping nontrim bytes are deliberately reinspected by each loop. Success or
missing-after-search is(4+R+F+B+2L+E+P)T; invalid empty/trim is(2+R+F+B)T;
allocation failure(3+R+F+B)T; NULL2T. P includes mismatch/terminating pairs.
For entries a/ab/abc, literal schedules a13T,ab20T,abc28T,abd27T are independent
of observed accounting. One255-byte matching name costs1029T. Every nonzero
return preserves outputs/TLS diagnostic and frees local normalization scratch.

Diagnostics publish only after final saved-ID guard. A fixed known-literal
bridge writes the same raw TLS storage without strncpy's255-byte padding:
max28-byte memcpy includingNUL costs56 read/write actions, dispatch24,guard51,
caller/output40=171<=224<256. Raw public diagnostic setter is unchanged.
Other byte/record intervals conservatively fit120+51+40=211<=224.

Indexed lease is5T: ENTRY420,RELEASE_PREPAY421,LOCK_CALLER422,
VALIDATE_COMMIT423,FINAL425. First4 admissions occur before registry mutex;
postunlock424 is zero-poll only. No hook/control call executes under that mutex.
Complete fixed validation/commit/unlock interval including refusal and next
admission fronts is196 actions (51+8+20+24+12+12+12+5+20+32). Both counters
are checked before either increment; a local receipt is recorded immediately
then unlocked. Wrong index/exact descriptor is semantic miss; refcount or active
lease saturation stagesEOVERFLOW, with no mutation. Final saved-ID guard takes
precedence over staged errors. Nonzero outputs remain unchanged.

Postacquire/final stop consumes the local receipt and calls raw lease release
once, prepaid, without successor hooks/debit; no snapshot dereference follows
rollback because destructor code may release it. On success caller owns one
raw lease release. Legal unregister and final snapshot release cannot destroy
the leased entry until that release. Mutex/allocator/user destructor bodies are
narrow synchronous boundaries; engine normalization/comparison is not opaque.
Tests exercise literal exact/prefix-one-short tariffs, real raw parity and
diagnostics, trim/embeddedNUL/case/maximum names and allocator failure; each
read/trim/copy/record/pair/final and lease phase under cancel/work/Original-END
successor/overflow, frozen reports/successor0/reset retry, both saturation
counters, foreign authentic descriptor, exactly-once resource retirement and
actual unregister/final-snapshot-with-lease lifetime. Raw registry lifecycle
regressions and standalone linkage remain separate controls.

## Ordinary observed JOIN plus scalar END recovery prerequisite

Committed8a STRLEN-after-JOIN reproduced the missing native birth independently
of extension admission: Original END plus successor returned EINVAL with12160
bytes and an active frame retained, successor work0. Batching and globalC1 flags
were bothfalse, so late JOIN registration skipped the real joined frame, while
pure-unary registration correctly excluded JOIN. Pointer presence and AUTO debt
were not substitute native permission.

Fresh ordinary observed nonC1/nonbatch scalar-required capture now registers
an abort-capable composed JOIN
frame before any child can invoke library/user work. The independently metered
shape is VARIABLE/FILTER/MAP/JOIN/CONCAT with terminal CONSOLIDATE and at least
one JOIN; LFTJ and other existing routes retain their own contracts. Pure-unary
birth remains separate and exclusive. ROOT, transaction/head/bindings, exact
frame/thread/Original/operation and actual indexed/full-key checks precede
publication. Failed admission rolls back only the genuine fresh bootstrap and
pin through the existing failed-begin owner, never a fabricated ACK.

Registrar literal work is256 for ENTRY, each actual OP, ROOT, BINDING,
KEY_THREAD, INDEX_BIND and PUBLICATION:256*(N+6) for eligible N records. Each
packet funds first, then atomically polls the saved Original with zero debit;
financial refusal is observed before Original. Cancellation, sticky accounting,
END or rebind stops before that packet's body. The final publication poll is
the last callback boundary before stores. This is fresh forward preparation;
registered repeat and native retirement never execute these polls.

The controller source is expanded, rather than treated as an opaque boundary:
charge_attempt18 includes null/ID checks5, mutex front2, active/ID selection4,
nested charge/result4, unlock front2 and return1. charge_locked has at most24
fixed actions covering active/accounting/stop/cancel, limit and overflow tests,
updates and return (including2 conservative alternative-assignment slack).
Only primitive mutex/atomic retry bodies remain cooperative. Both controller
calls84 plus helper/caller glue22 plus maximum stage144 total250<=256. Stage
ceilings ENTRY96, OP/ROOT64, BINDING112, KEY144, INDEX112 and PUBLICATION96 include
fixed nested frame/owner/thread, indexed five-field key reads/copies/equality,
error tails and transitions. Test barriers are production-disabled. The helper's
stage/progress plumbing fits the22 glue allowance; no whole plan loop is hidden.

The first ADVANCE financier has finite capacity max(existing,10240+512*N):
6400 maximum initial receipt lookup,768 host fronts,1536 unary fixed and1536 JOIN
fixed, plus two actual256 per-op scans. Capacity is not an executed work debit.
The four-op compiler fixture VARIABLE/JOIN/MAP/CONSOLIDATE with authentic
historical receipt index0 uses current index1: lookup640+host768+unary768
precedes new JOIN2560, total4736. Independent one-short oracles use capacities
2431,2687,...,4735 and consumed2176,2432,...,4480. No scalar forward work occurs
before real refused bootstrap retirement and retry. Every fresh stage also
covers cancellation from an actual other thread, work exhaustion, END alone,
END/successor and sticky overflow, with exact independent prefix and frozen
Original consumption. Funding-first refusal is checked separately from the
Original cancellation later returned by the session facade.

Before first host finance, and before `child_active`, a readonly forward pass
classifies actual immutable head expressions on Original funding. It calls the
existing numeric decoder at instruction starts; only ENOTSUP rewinds a private
position for the existing scalar decoder. Success there describes scalar
fallback. Both ENOTSUP results describe false without publishing a query error;
real control/accounting errors propagate unchanged. No provider, intern,
lookup, allocation or evaluation occurs. Constant payload bytes are never tag
candidates. The cache is actual transaction/head/generation metadata, initialized
with the real head storage and replaced before each new child generation;
framed birth still requires the complete native shape/root/binding/key proof.
The cache never supplies native permission. Pre-frame refusal uses the genuine
transaction disposal path and creates no frame or receipt ACK.

Classifier fixed work is ENTRY224, PRE224/POST224 around every numeric and
scalar decoder attempt, and FINAL224: (2+2R+2F)*224 plus the existing decoder
child fees. A child control error returns without POST/FINAL; all outputs stay
unchanged. Plan traversal pays ENTRY/FINAL224, OP224 per visited operator,
MAP_HEADER224 and SLOT224 per visited expression-array record. Scalar discovery
shortens the actual traversal. Head-region source includes both controller
bodies84, fixed body at most120 and fronts20, total224. The preceding fresh
unary registration now uses the same256 funding/Original-poll packets as JOIN,
so a long MAP/FILTER prefix cannot hide cancellation before JOIN classification.
Registered/native retirement never uses these forward polls.

Primitive tests model every positive224 boundary for empty, numeric, scalar,
numeric-prefix/scalar-last, malformed and unknown encodings, including scalar
opcode aliases inside integer payload. Compiler head tests stop all new
pre-frame phases and long OP63 from another thread, retaining exact Original
reports, output identity/generation, successor0 and genuine transaction cleanup.
A provenance-only test fault invalidates the actual cached generation at first
grant creation, for both unary and JOIN heads. Protected rejection occurs before
any scalar work or publication, with real bootstrap/resource closure and a
normal fresh-description retry. Pure-unary registration covers all seven fresh
stages and actual64-MAP prefix;
all new JOIN stages, six real grant/ACK finish cuts and signed retry controls
remain. Existing numeric AUTO metadata-purpose2304 and C1 SOURCE672/LATE407
assertions pass unchanged: numeric/projection and existing C1/batch heads keep
their original real retirement routes.

The SOURCE host wrapper selects2048 from the authenticated current scalar-head
fact before a birth record exists; this selects finance, never authority,
and does not set globalC1. Existing BANK248/88/104 policy, checked generation
settlements and independent gateway/native/tuple/close funding remain. LATE157
observes the already published JOIN record without repeating publication170.
Actual receipt location is bound once at birth. Generic lookup selects exactly
one registered JOIN/unary record, validates its actual slot/full key, and never
rescans on a protected mismatch; native action still requires the complete
route proof. Existing legacy C1 registration retains its prior fallback.

Tests use real physical empty-frame APIs for old FREED records, then genuine
compiler/session work at actual indices0/1/31. STRLEN and CAT cover hot/cold
generation stability, cancellation, exhaustion, Original END alone/successor,
sticky overflow, frozen reports/successor0, complete native closure, raised-limit
retry, exact typed cells and signed delete/reinsert/no-op events. All six genuine
grant-finish boundaries pass, including exactly one successful receipt ACK and
actual slotFREE before cut5 END. No new JOIN ACK journal was needed or added.
Negative probes reject wrong full keys/index binding, protected stored-key
mismatch, use of Original as financier and duplicate published registration,
without Original debit or ownership changes. Extension invocation and the
remaining legal expression-host matrix are still separate qualifications.

## One-column observed FILTER recovery adapter

The special one-column INT64 observed FILTER route left its real delta observer
held after an already-admitted scalar-owner ENOMEM, while Original was finished
and the backend reported IDLE. A following declared-input removal correctly
returned EBUSY. The independent `strlen(to_string(x)) > 0` reproduction has the
same result with and without a preceding successful SNAPSHOT; this is not a
snapshot-field repair or a new extension-only defect.

Structurally valid four-op VARIABLE/FILTER/MAP/CONSOLIDATE heads now select the
qualified general private transaction. Numeric FILTER also takes that recovery
protocol; its numeric expression primitives and append optimization remain.
Three-op identity projection keeps its old narrow route, including the protected
multi-row mutation refusal and successful individual appends. No observer pointer
is cleared to create permission. Genuine transaction disposal, receipt closure
and the existing R1 terminal record authorize correction after a positive error.

The changed preflight relation predicate adds at most six fixed source actions;
the complete payload has a conservative26-action bound. SOURCE mode still uses
the existing admission-reader208/96/80 contract; DIRECT mode still calls the
fixed payload directly. FILTER `done` skips the later general-map read. These
are callback/delta bounds, not newly invented full BANK FIRST/bootstrap bounds
or a claim that every preflight read is BANK-financed. There is no payload scan,
allocation, provider, lookup or native permission in this structural choice.

General APPEND_AUDIT previously treated every FILTER as append-compatible and
later rewrote its plan with an old-head VARIABLE/CONCAT. A scalar FILTER now
uses the existing allocation-free, Original-funded numeric/scalar descriptor
classifier before APPEND_COPY/PLAN. A scalar requirement takes the existing
canonical full-general fallback; no CONCAT/native authority is fabricated or
added to the unary grammar. Numeric FILTER retains the actual append plan and
its independent positive/linearity proof. Unknown/malformed descriptions remain
runtime decisions. Earlier audit frontier writes are inactive after fallback
and are disposed or rebuilt by the actual transaction owner.

The descriptive append helper pays PRE128, the existing classifier child fees,
and POST128, publishing eligibility only after POST. Each new parent interval
has a conservative complete bound42 controller +56 fixed helper/caller +24
next-front =122 <=128 <=256. The scalar fallback reset/return and numeric FILTER
continuation are separate paths. Numeric continuation includes only FILTER's
fixed predicates, degree/cursor checks and resets, not VARIABLE/JOIN name-byte
or MAP projection loops. Invalid arguments/pre-PRE fronts belong to the existing
incoming fixed frontier; no later payment retroactively finances them. Failed
PRE/child/POST leaves the eligibility output unchanged. No parent capacity pays
for a whole expression traversal; child decoder regions retain their own fees.

`evaluation_observed_filter_recovery` uses actual compiled one-column plans.
It covers raw/control numeric, scalar, semantic-rejection and unchanged identity
controls; populated/empty baselines; successful snapshots before error; repeated
real owner ENOMEM; wrong/zero mutation admission and legal remove/reinsert after
R1; old committed pointer/generation, report, exact signed events and no-op retry.
Canonical scalar execution has actual authenticated unary owner2. All cancellation,
work, END-alone/successor and sticky-overflow histories close real observers,
frames, receipts and memory, with frozen Original reports and successor0.

Independent literal oracles cover every PRE128/classifier224/POST128 payment
and one-short prefix. Stops inside the audit classifier occur after actual
frontier writes and before APPEND_COPY/PLAN, with rollback/retry checked. A
separate numeric positive proves the runtime six-op append/CONCAT plan still
occurs. Fixtures use an enforcing64MiB governor; zero spare admission budget
refuses private work without publication, then raised-budget retry produces the
exact model. Session/program/control teardown returns governed reservations0.
This adapter does not close extension admission or the remaining legal-host rows.

## Unit8b: the two existing scalar extension call tags

This unit admits only `EXTENSION_CALL` and `EXTENSION_CALL_ABI` through the
bounded scalar compiler/evaluator. It adds no function, descriptor permission,
result type or public API. Qualification belongs to the tested FILTER/MAP
hosts below; it does not certify the remaining right-filter, REDUCE input,
recursive/SCC, inline or worker intersections merely because decoding succeeds.

Payload decoding and evaluation are separate paid traversals. With `T=224`
and a name of `N` bytes, phases450..455 cost `(11+N)T` for the legacy tag or
`(23+N)T` for ABI. The phase452 position traversal does not claim to copy name
or ABI bytes: runtime separately copies `N` name bytes at456, adds NUL at457,
and rereads12 ABI bytes at458. Those runtime costs are `(N+1)T` or `(N+13)T`.
Immutable owner allocation/copy and the existing scalar70 front are separate.
Malformed D0/D1/D2 prefixes cost3T/6T/(11+N)T respectively. A malformed marker
is published only at decoder FINAL; it becomes a row-time semantic error,
retaining earlier literal/callback effects and preventing trailing execution.

The invocation helper uses16 fixed regions500..515, plus one520 payment for
each actual session-key depth comparison and one521 payment per actual
argument slot. Thus successful helper work is `(16+D+A)T`, with runtime
payload, qualified8a lookup and its five-region indexed lease added separately.
Failure prefixes are2T malformed,3T missing context,3T+lookup missing name,
4T+lookup ABI mismatch,5T+lookup worker policy, `(6+D)T+lookup` reentry,
`(7+D)T+lookup` arity, `(8+D)T+lookup` result declaration,
`(9+D+a)T+lookup` argument mismatch, `(10+D+A)T+lookup` TLS depth cap,
`(13+D+A)T+lookup+lease` lease disappearance, and
`(15+D+A)T+lookup+lease` callback failure. Actual invalid result and success
use all16 regions. Parent evaluator FINAL88 remains an additional payment.

Source accounting includes both saved-ID controller calls and their engine
branches:84 controller actions,27 wrapper guards and12 transition/caller
fronts give123. The legacy guard51 convention alone is insufficient for this
complete expanded claim. Decoder decisions fit203, per-position work147,
name/ABI runtime155, and argument-slot preparation187, all within224.
Result initialization writes at most32 constant-zero bytes under the static
size assertion. Including invoke fronts and55 mandatory release/pop actions,
the critical interval is `96+123=219 <=224 <256`. Final FILTER/MAP return,
status routing and publication fit198. These are source action ceilings, not
machine instruction counts. The219 interval leaves five actions of slack;
changes to controller, initialization, release or caller shape require recount.

No argument-array or256-byte name-buffer aggregate initialization occurs.
Arguments borrow full string spans under actual intern/context lifetime;
BOOL literals remain BOOL, while string-producing builtins and RFC UUID5
produce the raw evaluator's INT64 intern IDs. Whole MAP schema remains unknown
and untyped when an extension is structurally reached, including ignored/capped
results. Empty MAP setup neither looks up nor invokes an extension. The
qualified auto constructor preserves that raw schema mode without raw fallback.

All required context fields and invoke/userdata are captured before invoking
user code. A real indexed entry lease survives legal unregister/final snapshot
release. Mandatory cleanup consumes the local lease exactly once, releases it
before popping the TLS session key, and performs no checkpoint between invoke
return and that cleanup. User invoke/destroy, mutex/atomic primitives and
allocator/free bodies retain their named synchronous cooperation boundaries;
engine policy, arguments, counters, loop work and call fronts are counted.
No snapshot/descriptor/context read follows callback release. The next saved
Original check detects callback/destructor END or rebinding without polling or
debiting its successor. Value/failed/status publication is guarded separately.

Lookup diagnostics commit at their own child cutoff and are observable inside
the callback. Parent control errors preserve value/failed/status, not arbitrary
prior TLS, intern, callback or destructor effects. Raw callback/error/type/
policy ordering and facade mappings remain authoritative. An engine owner
allocation ENOMEM is distinct from callback/semantic allocation failure.

Actual compiler/session fixtures cover both tags, FILTER/MAP, ordinary unary,
ordinary JOIN and a unary head with an LFTJ side head. Live host observations
prove exact unary owner2 or JOIN owner1, frame/receipt authentication and real
cleanup/ACK; no owner3 or dangling context stands in for that proof. Nine
named seams cover name/ABI/policy/arguments/preinvoke/postcleanup/final plus
host body/final copy, with cancellation, work denial, END+successor and overflow.
Exact fee oracles are literal schedules; host allowances remain empirical.
Positive/false/empty, embedded NUL/full spans, malformed precedence, whole-MAP
schema, stack caps, depth64, worker/reentry policy and callback/destructor
lifetime are separately tested. Public facade cases retain actual STEP and
SNAPSHOT, error texts/codes, immutable committed images, frozen reports,
correction before reset, signed deletion/reinsertion/noop and final resources.

This qualification relies on the independently committed observed JOIN and
one-column FILTER recovery prerequisites. Scalar FILTER avoids the numeric
append/CONCAT optimization before COPY/PLAN; numeric FILTER retains its real
six-op append control. No extension permission is granted to intern-only
right-filter contexts. Remaining legal-host rows stay open for later units.

The public controlled allocation cases inject failure immediately before actual
phase76 owner/immutable-byte allocations, each repeated on a new attempt. They
expect engine ENOMEM/MEMORY, statusOK and unchanged seed diagnostics, with no
callback after arming. A prior successful transaction can legitimately retain
metadata credit until the next STEP. A test-only readonly getter authenticates
its actual RESERVED token, identity, governor and absence of other old credits;
the failed terminal must equal the sampled reservation minus that exact credit.
The second terminal must remain equal, and complete destruction must return the
retained governor to zero. No hardcoded byte allowance, extra drain or mutable
receipt is substituted for genuine retirement and R1 correction admission.

## Unit9a: ordinary JOIN scalar right predicates

On main d0c5df81 a compiler-generated `right(k,"yes")` predicate was14
bytes (VARcol1/CONST_STR/CMP_STR_EQ): raw STEP succeeded while controlled STEP
returned ENOTSUP. Right selection now retains its numeric-first actor and,
only when needed, uses the qualified scalar compiler/evaluator. Context grants
only intern access; all seven fields are explicitly assigned with no snapshot,
session key, worker or scalar-result extension permission. Semantic failure
rejects the row as raw filtering does; engine/control errors remain errors.
Compiler-invalid extension right-filter IR is tested separately from parsing:
a valid ordinary direct callback is moved into the right input and rejected by
the existing right-filter capability validator, without invoking a callback.

Scalar fallback compile and each scalar row add a distinct256 caller packet.
The complete compile envelope is190; explicit context construction/eval is
123 expanded controller/fronts +7 stores +16 local/call fronts +51 next child
fronts =197 <=256. Child traversal and owner allocation/copy remain separately
paid. The last scalar FINAL88 through JOIN's saved-ID zero return and next
CELL payment fits198; a zero charge is an identity edge, not finance. Existing
numeric actor and empty-right prices are unchanged. Common PROGRAM retirement
consumes and clears scalar_program through its existing prepaid disposal path.

Right predicates are now also described by the Original-funded head classifier
before genuine child birth. The JOIN-specific phase561 path fits208<=224;
there is no new565 phase. The description is published only at existing564
with actual transaction/plan/generation checks. It selects the existing
JOIN_DYNAMIC finance protocol; it grants no native ownership by itself.
Without that description, Original END at scalar decode70 left12168 bytes and
an active frame. Actual compiler/session tests now require owner1, authenticated
frame/private bindings/receipt, frozen Original, successor0 and complete native
cleanup/ACK. Numeric right predicates acquire the real readonly classifier
fees (ENTRY/FINAL plus PRE/POST for each decoder child); this is distinct from
preserving numeric actor fees. Existing independent classifier/decoder literal
exact/one-short oracles remain authoritative.

The dedicated evaluation_expression_hosts target checks keyed/product JOIN,
numeric controls, positive/rejected/invalid-symbol rows, actual typed empty
right setup, signed deletion/reinsertion/noop, governing reservations and
compiler capability boundaries. Deterministic stops cover right decode/eval/
FINAL and pre-frame classifier phases550..555/564, each with cancel, work denial,
END alone, END+successor and overflow. Old output pointers/generations/events
remain unchanged before commit; retry and final governor0 are mandatory.
Filtered SEMI/ANTI are not certified by this ordinary JOIN actor: an actual
negative atom with a string constant still exposes a concrete probe adapter
gap that subsequent completion units must repair.

The compiler-produced numeric right predicate has an independent literal
classification oracle: VARcol1/CONST_INT1/CMP_EQ costs28*224 +192 =6464.
ENTRY, each PRE/POST, decoder/tag/payload cuts and FINAL are explicit; the
single column digit is192. Every positive-cut one-short budget preserves the
output sentinel and consumes exactly the preceding literal prefix. This
additional session-admission work is not inferred from a successful run.

## Completion: filtered existence probes

The compiler-generated negative atom `!right(k,"yes")` previously returned
ENOTSUP under control although raw execution succeeded. ANTI and the internal
SEMI actor now use the same bounded right selection as ordinary JOIN. Selection
runs before key/schema validation; logical selected count governs existence,
while hash-chain storage and source indices retain physical row bounds. Only
selected original indices enter chains. A filtered-only256 packet covers the
index selection/hash caller and keyless existence decision; unfiltered192
actor prices remain unchanged. Missing-right exact repush precedes expression
setup, including SEMI projection. Scalar failures reject rows; extension
permissions remain absent.

Actual SIP construction requires a plain SCAN on its pruned right input. A
literal introduces FILTER and prevents that SEMI rewrite. Tests therefore name
the generated ANTI, internally constructed filtered SEMI, and actual unfiltered
SIP SEMI as separate witnesses. They do not claim a compiler-generated filtered
SEMI. Keyed/keyless numeric/string positives, original-index collision chains,
initially/all-filtered and typed-empty inputs, signed delete/reinsert/noop,
missing projection and nullary/type validation have explicit raw controls.

Both abort scanners now recognize the owned existence-probe cleanup grammar.
The controlled scanner retains ENTRY33, OP64 and FINAL17 and adds a direct128
payment only for each visited ANTI/SEMI. Before that extra debit the complete
old64 interval is suffix8 + rc2 + opcode5 + dispatch1 + call fronts4 + next
controller42 + refusal rc/return2 =64. Labels execute no extra statement; no
helper, hook or predicate precedes the direct payment. The new body including
next OP fronts fits94<=128. A mixed VARIABLE/JOIN/probe/CONSOLIDATE independent
literal oracle totals434 with every-one-short prefix; materialized and depth0
probe exits consume353 and225. Existing numeric schedules stay unchanged.

Head classification includes probe predicates (fixed212<=224); genuine native
birth uses the shared JOIN_DYNAMIC grammar (fixed254<=256), retaining frame,
transaction, generation and receipt authentication. The expanded host tests
require owner1 during scalar work and exercise decode/eval/FINAL and descriptor
stops for all three actor kinds: cancel, work denial, END alone, END+successor
and overflow. Frozen Original reports, successor0, immutable committed images,
retry, native ACK and governed teardown0 remain mandatory. Classification
selects finance; it never grants cleanup authority.

## Completion: scalar REDUCE operands and unary retirement

Generated SUM/MIN/MAX over `strlen(s)` now retain numeric-first compilation and
use the bounded scalar actor for nonnumeric bytecode. Explicit intern-only
context matches raw eval_i64: configured/active workers1, no extension snapshot,
scalar-result permission, parallel execution or session key. Semantic failure
remains ERANGE; existing subsequent nonfinite/type checks remain in place.
COUNT still does not classify or evaluate its ignored operand, including
malformed bytecode. No aggregate algorithm or extension-aggregate language
permission changes. A valid parsed direct extension FILTER is transplanted into
aggregate IR and rejected by the exact aggregate capability diagnostic.

REDUCE's saved-ID guard followed by ordinary charge differs from JOIN's
charge_attempt wrapper. The actual guard24 + controller42 envelope is66;
scalar compile2*66+16+51=199<=256 and context2*66+7+16+51=206<=256. FINAL88,
result publication12, guard/zero66, caller12, next positive front66 and reserve20
fit176<=198. No callback occurs between saved-ID validation and ordinary charge.
The scalar program joins the existing PROGRAM owner and84 retirement payments;
it does not acquire a second workspace owner.

The abort grammar now recognizes owned REDUCE under the same extra128 cut as
probes. Every visited REDUCE, including numeric/COUNT, adds128 to OP64; ordinary
JOIN and unrelated numeric schedules are unchanged. Non-COUNT aggregate
operands also acquire their actual readonly descriptor classification fees.
The post-probe REDUCE descriptor branch fits220<=224; COUNT takes no classifier
child. Generated SUM/MIN/MAX/COUNT and mixed JOIN bodies have independent
ENTRY33/OP64/REDUCE128/FINAL17 exact and every-one-short schedules. Actual numeric
SUM(k) VARcol0 classification has13 literal positive cuts totaling12*224+192=2880.

Reducer-only scalar heads use genuine unary owner2; mixed JOIN+REDUCE uses
owner1 only with a real JOIN. The mixed birth grammar's extra no-op REDUCE label
fits256 exactly and grants no joined fact. The unary per-op birth scan fits
158<=256 with its grouped no-op REDUCE label. All existing frame, private binding,
transaction/generation and full receipt authentication remains mandatory.
Actual later-row END exposed a unary latch that allowed only FILTER/MAP and
rejected REDUCE's legitimate shared scratch flags. The new REDUCE-specific
shape authenticates the exact current operation throughout scratch and
pre-decision epochs. JOIN_CLEAR genuinely retires scratch; JOIN_DECIDE alone
clears operation/pending state. CONTROL/FINISH/RESET permit that retired state
without reading a dormant old control. Unary JOIN/probe substitution, foreign
operation, early NULL/pending and inappropriate stages remain rejected.

Registered cleanup latches now take an unconditional independent256 packet
immediately after existing112, before context/shape inspection, for unary and
mixed actors alike. Old112 suffix8+rc2+call4+controller42+refusal2=58<=112;
expanded context/recognition/epoch/tail/next fronts fit248<=256. This explicitly
reprices native retirement, including existing FILTER/MAP, without changing
Original expression fees. The actual unary retirement cell is3560<=4096 and
mixed dynamic cell2104<=3072; tests assert real financier reports and identity.
Since #2300 retains scalar source readers, a retirement cell that starts with a
live non-bare reader also performs one funded END action: dispatch1 +
wrapper128 + leaf128 on top of the unchanged cell, so3817<=4096 unary and
2361<=3072 mixed. Main ran that
END unfunded inside the same cell; the test classifies each cell from its
starting filter state, requires both kinds and a drained reader, and checks
that an undrained END continues in the next cell.
SOURCE2048 is a distinct outer financier and unchanged. The directly
authenticated latch's literal88+(5*192+128)+112+256=1544 schedule is tested at
1543 (consumed1288, no shape mutation) and1544 through scratch, PROGRAM/SOURCE,
JOIN_DECIDE and post-decision epochs, with protected mismatch controls.

The registered evaluation_expression_reduce target compares exact typed cells
and signed events for grouped/ungrouped unary/mixed SUM/MIN/MAX, deletion,
reinsertion and no-op. It covers COUNT ignored scalar/malformed/over-stack-cap operands and empty input,
later-row decimal overflow ERANGE and correction, repeated workspace/program
ENOMEM and retry, actual compiler capability rejection, native receipt ACK,
and governed final0. Deterministic decode/eval/FINAL and pre-frame description
stops exercise cancel, work denial, END alone, END+successor and overflow with
frozen Original, successor0 and immutable committed pointers/generations/events.

## Completion: observed one-column computed MAP routing

A generated `out(strlen(to_string(x))):-input(x)` with an observer previously
returned ENOTSUP, while the same accepted rows ran without an observer. The
narrow observer now retains only its exact identity descriptor: no expression,
a sole zero-sized expression (NULL or nonNULL bytes), or the seven-byte VARcol0
encoding. Other expressions select the existing private general transaction.
Pointer/count/size guards precede every byte read; malformed descriptors remain
runtime validation inputs. The ordinary projection checks stay unchanged.
The local delta_cb guard makes the already-existing outer observer boundary
explicit; plain/C1 execution is not assigned observed unary ownership.

The descriptor predicate has no input-sized traversal or decoder: original
projection24 + descriptor guards24 + fixed memcpy4 + fixed memcmp12 + tail6
+ observer guard4 =74<=existing96 payload allowance. Existing SOURCE admission
208/96/80 and DIRECT contracts remain distinct; this is no new BANK/bootstrap
proof. Numeric computed MAP gets the actual general host admission cost, while
numeric expression actor tariffs remain unchanged. SNAPSHOT independently
selects its existing general path.

The expression_hosts target now checks actual one-column scalar, numeric,
string and extension MAP. Scalar body compilation/evaluation authenticates the
real current MAP child (including its pending READY start), frame/private
bindings/receipt and unary owner2. Pre-frame metadata compilers are not counted
as that actor. Identity controls retain actual narrow observer delivery with no
general head description; absent/empty descriptor variants also retain it.
The independent plain/C1 positive controls require bounded expression work and
actual typed values without borrowing observed owner2 permission.

Tests cover real typed-empty inputs, populated and loaded-inline inputs,
signed deletion/reinsertion/noop, a prior successful SNAPSHOT, later semantic
failure/correction, and repeated injected program ENOMEM inside the genuine
MAP owner. Extension tests check STRING argument copies, actual single-worker
context/session key and busy callback destruction, unregister while sessions retain leases,
callback failure/nonpublication/retry and exactly-once final destruction.
Empty and populated scalar work plus pre-frame classification exercise cancel,
work denial, END alone, END+successor and overflow. Committed pointers,
generations/schema/events, frozen Original, successor0, real ACK and governed0
remain mandatory. Operator-stage UNKNOWN still means the entire MAP is
untyped. Final canonical copies may spell implicit INT64 explicitly, as the
existing relation/merge contracts do; STRING/FLOAT/BOOL are never equated to it.

This routing unit does not close the separately reproduced first-missing
logical-schema adapter: before any input establishes a schema, the raw source
has original width0, while the general bridge reifies declared physical width.
For `to_string(x)` that exposes raw UNKNOWN versus controlled STRING even with
no rows. Typed-empty and later-empty cases are distinct and positively checked.
The required raw-equivalent scalar schema walk/provenance adapter remains a
concrete next unit. Likewise, no-observer first-missing admission95 (also seen
with numeric identity), recursive effectful seed scheduling and typed SCC
transport are not certified by this routing change.

## Completion: complete scalar MAP schema inference

MAP metadata now uses a separate controlled whole-expression query. The older
producer-only query and its78/79 two224 regions remain unchanged. Raw MAP first
queries a compiled producer, then falls back to its interpreted grammar. Scalar
ownership alone does not imply that raw compilation failed: string comparisons
are accepted by the raw compiler. Therefore one shared bounded decode advances
independent raw-compiler and interpreted states. Raw producer state preserves
its permissive underflow, ignored pushes at capacity and aggregate no-ops.
Each selected producer replaces both type and known-state; invalid early types
do not poison a later valid producer. The interpreted state validates every
variable's actual width/type, opcode arity, depth and final singleton. Neither
track is resurrected after grammar/type failure. Complete raw compilation wins;
its unknown type permits interpreted fallback. Neither path evaluates values,
allocates another compiled program, resolves an extension or invokes a callback.

New610 ENTRY,611 PRE,614 RAW_TYPE,612 INTERP_TYPE and613 FINAL each pay224.
ENTRY/PRE/INTERP/FINAL expand to controller84+fixed108+front24=216;
RAW to84+producer20+result_type36+stores10+status8+front24=182.
Existing decoder byte/digit prices remain separate. Equal-signature MAP
substitution retains its incoming166<=192 and FINAL handoff146<=224.
Successful unknown makes the entire MAP schema unknown, including already
inferred columns; later columns cannot restore a partial type vector. Numeric
metadata inference is unchanged.

The expression_schema target independently checks every positive exact and
one-short payment for VARwidth0=3328, VAR/TO_STRINGwidth0=3552 (unknown),
VAR/TO_STRINGwidth1=4000 (STRING), twoVAR/CMP_STRwidth0=6656 andwidth1=7104
(BOOL), underflowCMP_STR=1344 (raw compiled BOOL), one-byteCONST_STR=1792 and
extension earlyUNKNOWN=672 (no payload lookup). Its oracle compares actual raw
compile-first inference across accepted scalar tags, widths and valid/invalid
source types, aggregate no-ops, capped pushes and producer replacement. All
new stages and shared decode boundaries exercise cancel/work/END/successor/
overflow with untouched output sentinels. Existing producer-only tests stay
unchanged. Genuine host stops additionally retain frozen Original, successor0,
committed image/schema/events and real native retirement.

Actual MAP operator tests cover zero physical width, typed-empty sources,
multiple output columns in both orders and sticky whole UNKNOWN. Populated
subsequent MAPs also match the raw operator. Empty result deep copies expose
an existing scalar-source rejection of zero-capacity descriptors with no
column buffers; that exact empty-chain adapter remains an integration test
requirement for the next source unit. This primitive does not yet repair the general
bridge's first-missing source reification; the physical source adapter remains
a separate integration unit, including future graph-lane semantics.

## Completion: actual empty physical source and graph header

Observed SCHEMA now substitutes a source-derived private constructor for its
old declared-width constructor. Declaration validation stays in its existing
caller; the same seven arguments replace an already-local width with the
already-local original pointer. New header reads occur after the child's
positive ENTRY. The real source's physical width, names/types and graph/declared
header supply the recipe. Bare INT64 input remains physical width0, with a
schema-valid private descriptor; no declared lane is synthesized. The public
original remains the EDB image, preserving later schema establishment and
insertion validation. Eager STRING/FLOAT and established empty inputs keep
their actual schemas. No shadow schema/provenance accessor was introduced.

The child acquires independent genuine descriptor and storage-owner readers.
INIT621 explicitly initializes the nonmoving record and pre-admits mandatory
reader-release fronts22 and post-build outer unwind18 before either ownership
is acquired:196+22+18+markers2+new phase-hook front2=240<=256. Normal631 separately
pays normal release; failed631 or earlier errors use INIT's unused reserve.
Builder failure retains its own partial-allocation disposal; successful install
transfers the fully built private relation to the outer local. Failed outer
publication disposes that actual local exactly once using the pre-admitted
outer fronts and the existing constructor's narrow mandatory physical/token
retirement boundary. No stopped Original or successor is polled for cleanup.
After drain the code reads only saved control/id/locals before final632.

New620 ENTRY,621 INIT,622 owner resolution,623 identity,624..626 bare-header
chunks,627 graph/header decision,629 recipe,631 normal drain and632 final each
pay256. Each actual run-array record628 separately pays224. Owner resolution/identity fit248,
header chunks219, graph/header228, run178, recipe200 and final215 in their
respective packets, including the new phase-hook fronts. No whole-header clear
or hidden run-array traversal occurs. Source leases and guarded publication
retain the same parent handoff; no SCHEMA logical1 retrospectively funds new
header work. Existing per-byte/record constructor and reader prices remain
separate. The four independent literal constructor vectors test every positive
exact/one-short cut: empty name35724/36616 and `input`36204/37096 for ungoverned/
governed outputs on the current944-byte descriptor ABI. Tests express descriptor
copy geometry with sizeof and eight-byte224 packets; other prices are numeric
literals, not production constants or measured host totals.

Scalar source validation permits a future graph lane only with physical0,
rows0 and index<declared width under genuine held readers. Its expanded fixed
predicate remains218<=VALIDATE224. Actual raw direct MAP succeeds on this
compiler-produced placeholder; raw FILTER new-like returns EINVAL. All three
scalar new-like siblings (legacy, operator and typed CONCAT) therefore pay a
new256 packet after their existing LIKE128 tail for the precise physical graph
output guard. Original name/out validation and hook remain. The bare-empty
sibling's prior graph refusal remains unchanged. Each new output guard has an
independent128/256 exact/one-short oracle after an actual1632 source open,
compared with the raw new-like result. Positive typed CONCAT remains covered.

A genuine empty MAP result deep copy has nonzero schema width, zero capacity
and no column table. Raw subsequent MAP accepts it. Scalar source admission now
requires the table only with positive capacity and omits lane reads at capacity0;
nrows<=capacity and gather's row<nrows guard prohibit nonexistent-row access.
SNAPSHOT remains163<=192, and positive-capacity lane validation204<=224. Tests
exercise both empty and populated two-MAP chains, whole UNKNOWN in either
column order, and the existing positive-capacity invalid-table/lane controls.
An initial failing empty-chain test had reused a freed fixture input; the copy
is now unconditional, and independent raw-only chaining confirmed the contract.

The registered empty_expression_sources target proves actual first-missing,
eager STRING/FLOAT, typed/later-empty and future-insert/remove/noop behavior,
constant/numeric controls, exact MAP UNKNOWN and private/public graph headers,
real authenticated unary frame/private binding/receipt/owner2, and graph FILTER
error followed by valid retry and a rejected row. It checks actual source
constructor allocation failures and governor denial with repeated recovery.
All new source phases, both reader acquisitions and before-release hook exercise
cancel, work denial, END alone, END+successor and overflow. Actual session stops
preserve original snapshots and relation-qualified old committed images/events;
Original reports remain frozen and successor consumption0. Final plan/session/
intern-owner teardown returns the governor to0.

WORK_LIMIT intentionally retains the existing pre-child observed transaction
for raised-limit retry. Tests authenticate its actual metadata/image reservation
identities, governor and RESERVED states, exact layout-derived bytes, original
reader identities/gates and captured snapshots. Reservation growth must equal
these real tokens, with no fixed tolerance; repeated same-frontier stops cannot
grow it. Other errors discard the transaction. This does not alter the existing
retention policy or treat its original readers as leaked constructor readers.
Completed controlled STEP still rejects unsupported insertion shapes with its
existing ENOTSUP admission status; raw insertion reports declaration EINVAL.
Both preserve the original. No previously invalid declaration domain is enabled.

Actual CONCAT tests pass a constructor-produced future-graph placeholder
through raw evaluation and genuine controlled START/STEP/ACK. They retain
identical refusal, untouched inputs and no output, and retry corrected physical
inputs on the same unbound frame and fresh authenticated bound context. ANTI
and SEMI missing-right controls keep the original input before malformed keys,
projection or right-predicate decoding; this includes owned/borrowed timestamped
future-graph inputs.

CONSOLIDATE has a distinct raw empty-input contract. Non-timestamped inputs,
and genuinely owned timestamped inputs, preserve a future graph header without
new-like emission. Borrowed timestamped inputs retain raw refusal. A new unary
info getter reads ownership from the actual protected frame snapshot, with the
existing numeric-writable and unary-forward guards. Its expanded getter costs93;
the whole caller interval costs229 including the hook, paid by a separate256
before the getter. The original INPUT192 interval remains67. An independent
3904 exact/3903 one-short oracle includes this packet, without inferring prices
from measured host totals.

The timestamp-preserving empty-source sibling keeps the same seven-argument
interface and genuine source readers. Phase636 pays256 before timestamp
inspection; the existing builder allocates and initializes the actual64-cell
timestamp lane. Independent literal vectors are77212/78502 for `$consol` and
77020/78310 for `input` on the current ABI, ungoverned/governed. Every positive
cut has exact/one-short checks. Allocation failures, governor denial and retries
cover all four allocations. Actual CONSOLIDATE tests cross ownership, timestamp
presence and authenticated numeric-owner binding, check the real getter's phase,
owner and thread guards, and inject cancel/work/END/successor/overflow at new
source phases, input-info and halfway through timestamp initialization. They
retain source snapshots, frozen Original reports, successor0 and closed real
readers, native frames and reservations.

Shared constructor tariff changes are reflected in independent existing host
oracles: ANTI output adds256 while SEMI's typed constructor is unchanged. Each
recursive serial round has three new-like guards (CONCAT, CONSOLIDATE and the
detached FRAMED_NEW output), plus CONSOLIDATE input-info, totaling four new256
packets. Existing successful, exact/one-short, denial and raised-retry assertions
remain in place. Historical unit6 review findings require actual CONCAT coverage
and both shared-oracle corrections; they are fixed rather than overruled.

ANTI output additionally preserves the raw legacy error distinction: with no
output governor its pointer-return new-like factory maps graph refusal to
ENOMEM; an actual session/left/right output governor selects checked EINVAL.
Explicit internal scalar/empty ANTI twins change only this paid semantic guard,
with identical caller fronts and positive fee vectors. Actual pool/fallback,
owned/borrowed and timestamp/no-timestamp combinations cover all four governor
placements. The scalar graph interval remains184<=256 and empty invalid path
124<=128. All four scalar graph guards return any saved tail stop before graph
refusal; cancel/work/END/successor/overflow injection proves that semantic errno
does not hide control status. Default scalar and empty constructors retain
EINVAL, and missing-right probes retain their earlier passthrough.

This unit closes observed first-missing schema transport. Initial-missing
identity and computed expressions now succeed with an observer in actual
raw/control checks; identity remains a regression control for its narrow route.
The reproduced no-observer missing-source admission gap remains an explicit next
adapter; populated C1 and worker-control ownership are not reclassified here. Recursive actual-tape scheduling and typed SCC integration
also remain open work rather than certified expression-host rows.

## Completion: callback-free first-missing input and late rollback

Fresh callback-free plans with one nonrecursive EDB/head and a
VARIABLE,[FILTER],MAP,CONSOLIDATE tape now select the existing private
transaction for a real bare input instead of returning ENOTSUP. Two fixed readonly routing
actions inspect metadata86 and header38 within96 each. They read no name or
expression bytes and borrow no reader. The whole new block is restricted by
the already-local DIRECT mode before state initialization: SOURCE/C1 skips
both actions entirely, adding no208 read. Observed metadata returns false before
source-header access, preserving the previously successful observed identity
and computed routes. Populated, typed-empty and batch controls keep their old
selection. The STEP keeps observed=false; no callback
or observer object is manufactured to choose private execution.

The hint permits one public original, or two with an actual graph flag on the
bare first source. Actual compiler graph input has one EDB and two public
originals because it creates `__graph_metadata`. The pointer at registry slot0
is only a routing hint. Charged full-name matching, original readers/snapshots,
private binding generations and genuine frame/numeric owner proofs still select
and authorize the source. Unknown VARIABLE remains raw/control ENOENT2, never
an ordinal fallback. A malformed bare capacity reaches real child readers and
is refused before any native frame or expression; correcting it permits retry.
Actual copied typed-empty zero-capacity inputs remain on their existing route.

The automatic auxiliary original is captured under its real reader, preserved
exactly once in the coordinated registry image and released by its real owner.
Graph FILTER retains raw EINVAL22 before a valid insertion; direct MAP succeeds.
The registered tests check real original/auxiliary pointers and snapshots,
zero observer state, raw operator UNKNOWN/known schema and effective types,
signed delete/reinsert, no-op and invalid insertion, real native frame/bindings/
receipt proofs, all source cutpoints and repeated allocation/governor failures.
WORK_LIMIT accounting still authenticates the exact actual metadata/image
credits and both original readers, without fixed byte tolerances.

An independent graph-minus-plain pricing oracle derives ZERO's64-byte chunks
from actual original-record ABI geometry. On the current1224-byte record it
adds19*64=1216. The auxiliary LOOKUP adds9, its two real reader steps add4,
the second declaration lane adds1, registry/release/claim actions add3, and
cutoff19->20 adds1. The body additionally visits the actual auxiliary once in
framed target lookup and once in composed VARIABLE DELTA lookup,128 each.
Total Original delta1490 spans38 extra positive cuts; maintenance's two actual
reader-release payments are separate. First occurrences of PLAN/LEASES/SCHEMA/
EVALUATE/RELEASE/CLAIMS/CUTOFF/RETIRE/FINISH compare independently derived offsets,
with genuine slot/source-count/frame witnesses. No observed full-host total is
used to invent a normative budget. An earlier1234 draft omitted the two128 body
visits; that source derivation and approval were withdrawn and corrected.

Late END testing exposed a baseline general-transaction defect in both numeric
and scalar, observed and unobserved controls: after a precommit claim/CUTOFF
failure, the real transaction marked rollback but stale-ID entry recognized
only its later discard phase. Public STEP returned EIO, repeated calls rejected
EINVAL without draining, and destruction failed. The new narrow readonly
selector recognizes genuine quiescent rollback with saved positive error,
CLAIMS..CUTOFF phase, closed generation, current registry/count and operation
identity, no competing frame/receipt/native owner, and real saved maintenance
when present (absent only before the first rollback iteration). Existing claim
cancellation, retirement and reader validators remain the cleanup authority.
The selector does not create a receipt, edit an owner or grant forward execution.

The original SOURCE state getter is byte-for-byte unchanged. An existing
optional post-STEP state-only observer runs after generic SOURCE-scope restoration
and protects this actual rollback continuation. Matched direct observation fits
213<=256; stale entry to the first funded maintenance action fits206<=256.
All old SOURCE160 native-state families therefore gain zero predicates or fees.
The existing interface requires an observer/commit pair even for state-only use:
the paired commit endpoint fail-stops if called. The observer never returns
COMPLETION_READY or reads/writes selected_out, so ordinary flow cannot invoke
that endpoint. Global pair validation and generic EAGAIN invariants remain.
Real rollback transitions into the existing discard path and only finishes the
saved old ID after actual disposal; an ended Original or successor is never
charged or rewritten.

Forty-eight registered late-END histories cross scalar/numeric expressions,
observer on/off, graph/plain originals, preclaim/after-ready-claim/CUTOFF and
END alone/with successor. Twelve additional observed two-head histories prove
cancellation after the first genuine READY claim and with two public-head
claims before cutoff. Every public cleanup
turn checks frozen Original, successor0, old head/input/auxiliary images and
signed events; valid insert/remove/snapshot/callback replacement stay busy.
Actual retirement/rollback/discard/maintenance progress is observed through
existing readonly test seams. Terminal EINVAL follows real closure, then raised
retry matches raw and successful destruction returns the governor to0. Backend
pair negatives and untouched completion sentinels cover state-only handoff.
Long-name32/240 fixtures reach charged byte matching with the same first-NAMES
prefix and demonstrate no new preattempt name traversal.

This closes the qualified ordinary first-missing callback-free adapter and its
exposed late rollback debt. Inferred inline schemas, preserved inline IDB seeds,
actual recursive tapes/effect scheduling and typed SCC transport remain concrete
following units; this row does not certify their completion or worker ownership.

### Completion unit 8: inferred input schemas and later mutations

`WL_PLAN_WIDTH_UNDECLARED` records the absence of an explicit declaration.
The compiler's inferred type count does not restrict physical input arity.
SCHEMA now preserves real source geometry and declared_ncols, including an
established nonempty nullary input. Undeclared inputs skip declaration-lane
checks; their actual source and scalar actors still validate physical types.
The empty-source constructor runs only for an actual missing schema (or a
missing explicitly declared width), never to invent an inferred declaration.

Each SCHEMA action now pays256 before inspecting the declaration/source;
its fixed parent front fits231 and declaration lane174, including the extra
phase-selector comparisons. Explicit one-input width w visits w+2 actions;
undeclared one-input sources visit2 regardless of physical width. Thus the
unit7 historical graph-minus-plain delta1490 becomes1745: its extra declaration
lane now pays256 rather than1. Constructor vectors and the separate two
maintenance reader releases are unchanged. Whole-host allowances are never
inferred from a measured completion total.

Both completed-host mutation policies admit an actual matched undeclared EDB
target before calling the existing backend validator. They do not invent a
width, accept arbitrary IDB targets, or bypass established physical schemas.
Explicit admission remains unchanged. Mutation admission adds only fixed
sentinel comparisons outside STEP/SOURCE and grants no scalar/native owner.

Registered histories cover inferred/explicit INT64, STRING and FLOAT programs,
loaded/unloaded and observer on/off, callback replacement, future fact loading,
valid insert/remove/reinsert/no-op, failed wrong-width inserts and unknown/IDB
targets. Six-domain histories compare raw values, effective types and interned
string contents. Isolated first insertion arities0..3 before and after empty
success preserve declared_ncols0 and the raw nonempty-nullary default value0.
The existing integer to_string operation formats the underlying int64 payload,
including a FLOAT payload; these tests preserve the raw operation's behavior.

Every SCHEMA action receives independent exact256 and one-short255 local
budget tests. A real Original legitimately pays adversarial padding up to the
chosen remaining budget; padding is not a normative host price or a counter
write. Report ID and overflow-safe post-padding debits are checked. A refused
constructor's first positive action can precede its checkpoint, so no invented
checkpoint witness is required: the real WORK_LIMIT and source transition are
the refusal evidence. Public source/model pointers and snapshots stay fixed;
normal reset and raised retry close actual owners and return the governor to0.
Cancellation, work exhaustion, overflow, END and END-with-successor also run
at each SCHEMA action, checking frozen Original/successor0 and raw retry parity.
Existing compile/evaluate/final and late claim/rollback qualifications remain.

This unit closes inferred physical input handling. Preserved inline IDB seeds,
actual recursive scheduling/typed SCC transport and worker qualification remain
separate obligations and are not certified by this row.

### Completion unit 9: immutable serial K branch views

The immutable scalar view now recognizes actual recursive single-K_FUSION
origin tapes, in addition to the existing K_FUSION/zero-worker EXCHANGE form.
The new serial shape does not require materialized JOIN; the legacy two-op
shape retains that requirement and its existing validation/error behavior.
An absent terminal EXCHANGE is represented by NULL, without forming ops[1].
Actual origin descriptors, branch order, delta modes, expression/key pointers
and repeated terminal CONSOLIDATE operations are copied verbatim into one
checked arena. Origin plans/payloads remain borrowed; destruction releases only
the arena once. Malformed count/extent checks precede branch descriptor reads.
Unsupported nested/opaque/aggregate branch operations remain unselected.

The real compiler nonlinear reach fixture proves one K_FUSION with two
nonmaterialized alternatives before inspecting the view. Tests compare all
copied descriptor fields and deep origin hashes across repeated create/destroy,
allocator failure, successful NULL and misalignment. Backed malformed/overflow
metadata and legacy worker/materialization controls remain allocation-free.
Session tests intercept the actual newly eligible view arena malloc, preserve
a committed peer reservation, retry, and verify zero attempt/report during
immutable preparation. Actual governor refusal preserves its precise
WL_ERR_MEMORY_BUDGET outcome and retries after raising the budget. Program-owned
intern attachment is explicitly detached through its real API before testing
the isolated governor denial; final plan/program disposal releases all credits.

This is synchronous session preparation, with honest arena reservation and
ownership, not free per-STEP work or an execution permission. The actual serial
fixture still returns ENOTSUP through its existing controlled runtime gate,
while raw evaluation produces exactly reach{1,2,3}. No recursive scheduler,
native frame classifier, SOURCE authority or projection restriction changes.
Runtime raw-tape/epoch/seed-skip, typed SCC and seed effects remain separate
integration obligations.

### Completion unit 10: pipeline library and extension lifetime qualification

The existing joined string tests already qualify actual numeric-prefix
EXCHANGED/ACK, scalar FILTER/REPLACE suffixes, full/delta selectors, strict and
output policies, signed deletion/no-op and terminal recovery. Additional cases
now use real generated SHA256 and direct STRING-argument extension MAPs through
that same pipeline. Actual dispatch tags, prefix EXCHANGED/ACK and authenticated
current framed/native child ownership prove the selected context. Every output
cell and effective type is compared with raw execution, including rejected
numeric-filter rows. Crypto preserves its known three-column schema; extension
whole-MAP UNKNOWN remains NULL even with an INT64-result descriptor.

The extension registry is unregistered and its external snapshot released
before execution. Real plan/session retention keeps the descriptor alive;
actual callback leases and TLS depth are observed at the appropriate boundaries.
Callbacks validate complete argument bytes, configured/active workers1,
nonparallel execution and actual session key. try_destroy remains EBUSY in the
callback. The destructor runs exactly once only after the genuine retained
owners close. A second-row callback failure preserves its precise existing
CALLBACK_FAILURE status, prior callback effects, old public output and no signed
events; retry and raw parity then succeed.

Forty-eight enabled and thirty-eight disabled histories cover observer on/off
positives, deletion/reinsert/no-op, prelease521/511, acquired-lease512 before
invoke, post mandatory-release/TLS-pop513 and prepublication515. At each selected
boundary cancellation, work exhaustion, overflow, END and END-with-successor
preserve all three public relation pointers/snapshots and frozen Original on
EVERY public drain turn; successor consumption remains0. Preinvoke stops have
zero callback effects, postinvoke stops one; cleanup never reinvokes callbacks.
One callback-free END-with-successor control verifies that context separately.

A raw-valid601-byte literal extension argument supplies runtime-copy cuts at
byte200, after actual literal81 and prefix ACK. Phase81 is after credit
admission but before allocation; the subsequent byte-copy43 proves successful
allocation and copying. This excludes earlier bytecode copying even though the
literal makes bytecode exceed200. Direct VAR_STRING supplies a held span and is
not falsely counted as a copy. Nested function syntax inside an extension
argument was rejected by the existing parser and is not added as a feature.

Enabled crypto tests stop at provider setup203 and AFTER synchronous digest
computation204, not a fictitious digest-block midpoint. Existing primitive
oracles retain engine byte traversal and library-boundary evidence. Disabled
crypto reaches actual dispatch206, matches raw ERANGE without compute204 or
public output/events, and closes its real owners. No new pipeline implementation,
worker authority, host allowance or library cooperation claim is introduced.
Final resource checks require actual frame cleanup, callback leases/TLS0,
session destruction and governor0. The independent primitive tariff vectors
remain authoritative; host reports are not used to invent expected prices.

Seed composition and actual recursive epoch/typed transport remain separate
integration obligations; this qualification grants no multiworker permission.

### Completion unit 11: explicit FINISH result fields

The saved-ID FINISH primitive now initializes its ten semantic result fields
explicitly and copies its seven report fields explicitly in both OWN_FINISHED
and ALREADY_ENDED branches. Mutex scope, owner/generation checks, stop and error
priority, actual active-ID publication and report_valid ordering are unchanged.
There is no wrapper, new controller payment, API or native permission. Padding
bytes are not a semantic report or serialization contract.

This makes the fixed source work countable as ten stores and one reached
seven-field load/store copy, rather than a conservative56-byte aggregate zero
and40-byte read/write aggregate copy. The existing conservative primitive216
ceiling becomes104; only one of the two copy branches executes in a call.
This is a source-level bound, not a machine performance claim or a new fee.
A previously counted ordinary constructor-error handoff can consequently be
bounded by244 under that exact boundary. It does not certify a complete new
refused-controller/selector/caller path: first PRE/reserve refusal, nonselected
SOURCE callers and retained-attempt retry still require their own composition.
No unadmitted reserve can fund its own refusal.

Existing evaluation_control and evaluation_attempt_identity tests provide real
OWN/ENDED/MISMATCH/rejected-owner matrices, complete report fields, stop/error
priority, threaded generation changes and frozen Original/successor0 checks.
They are reused rather than adding assignment-mirror tests. Serial, expression,
empty-source and native/snapshot recovery regressions remain required because
the primitive is shared. Typed bottom transport and ordinary prechild finish
integration are subsequent units; this scalarization alone grants neither.


### Bulk-append default timestamp prerequisite

A real inline-seed growth oracle exposed uninitialized timestamp metadata in
raw bulk append. The destination's timestamp buffer could contain uninitialized
new capacity or stale spare entries; append copied provenance only when the
source also carried timestamps. Newly appended rows from an untracked source
now receive zero provenance, matching row and atomic-row append. Existing live
prefixes, source timestamps and unused spare entries are preserved. Allocation,
admission, COW and schema preparation still finish before this nonfallible write.
No resize helper, capacity policy or allocator ordinal changes.

The registered memory-admission relation test poisons actual spare entries and
checks default provenance, exact tracked-source copies, old prefix, untouched
remaining spare fields, values, empty-source behavior and untracked destination
behavior. It crosses heap, retained-governor, unmanaged arena and actual shared
view ownership, growth/no-growth and nullary/scalar widths. The stale-slot test
fails deterministically before the fix. Existing allocation/admission/ownership
failure tests are retained. Timestamp capacity proves addressability, not that
all spare entries carry initialized provenance; seed composition must not read
unknown spare slots or restore stale provenance over newly appended defaults.
This prerequisite alone does not activate the inline-seed or recursive host.

### Standalone physical seed transport prerequisite (unit 12)

`session_seed.h` defines an internal, opaque physical actor. Its caller supplies
stable disjoint context storage, one captured Original attempt, actual target,
optional seed shadow, an immutable reset recipe, and subsequently the actual
rule result. Calls are externally serialized and non-reentrant; sources and
recipe storage remain alive through End. The actor checks its creator thread.
It creates no native frame, registry entry, session completion record, installed
API, or SOURCE permission. This is a reusable physical prerequisite, not runtime
activation or completion of issue #2299.

Prepare physically creates the actual private reset base and restores seed rows
before reporting RULE_READY. It retains independent timestamp capacity even when
observed reset gives the columns capacity zero. Actual rule append subsequently
prepares missing type metadata before real same-base payload growth, installs
metadata afterward, and uses real B2 row mutation. Conditional normalization
sorts scalar values stably and retains the earliest duplicate's timestamp; it
does not sum multiplicities. The returned result precedes STEP compaction.
Tests explicitly compact the taken result when comparing it with completed raw
STEP, so those boundaries are not conflated.

The physical Arrow representation follows this source tree's actual helpers:
raw type preparation and reconstruction use `g` for FLOAT and `l` for other
scalar types, while the native type vector retains its actual scalar enum.
An initial base copies its actual canonical one-character source format.
Malformed NULL, empty, or longer formats are refused before a two-byte copy.
No arbitrary Arrow parser or unbounded Arrow release is hidden in a packet.

The ownership ledger contains the real target/shadow/rule readers; private
working and conditional normalized relations; constructor descriptor, metadata,
payload and length-table tokens; restore and gather/index scratch; replacement
columns and timestamps; real growth/refund/downsize records; and staged new and
detached old metadata. Private results have no external `mem_ledger` attachment.
Governor tokens cover their actual allocations, including the old/new metadata
policy overlap until old physical allocations retire. Each maintenance turn
frees one allocation or advances a real credit receipt. Direct scalar retirement
receipts destroy only the actor's two owned output slots. Actual source readers
end before those private relations retire. A live refund remains discoverable
until its receipt is DONE even after its token leaves REPLACING.

Every new physical action buys 256 before its body. Copy and clear actions cover
at most eight bytes. Tight B2 child return retains exactly the existing 253 plus
three result-transport slots at the immediate actor boundary: continuation is
installed before invoking the child and examined only after the next purchase.
This does not certify an external session caller that assigns the returned
status and dispatches progress/errors afterward. The test driver is semantic
oracle scaffolding, not such a certified controller. Future canonical integration
needs its own reviewed result carrier and funded caller envelope; no caller work
can be retrospectively charged to a successful child or a refused reserve. Final metadata binding uses a
saved-ID poll, two real token moves and fixed publication, with impossible move
failure treated as an invariant violation. Real cleanup uses a distinct active
funding attempt and never spends the stopped Original or its successor.

The registered `evaluation_seed_physical` target exercises the 40 parsed raw
loaded/unloaded, observed/plain, scalar/inferred/filtered and timestamp cases;
repeated resets and insert/remove histories; actual bulk and normalized-output
growth; malformed format rejection; real allocator prefixes and governor
refusals; component cancellation/local-255/local-256/overflow/END-successor cuts;
and all initialization cut points. These are component identities and local
boundary witnesses, not an observed whole-host tariff. Checks include actual
schema/type/name and logical timestamp equality, independent cap-zero/TS
geometry, exclusive Take, repeatable End, source gate release, frozen Original
reports, successor zero consumption and final governor zero. Complete source,
LLVM/profile and independent final review gates remain separate requirements.

Qualification exposed a raw append defect: a timestamped destination receiving
an unstamped source could publish stale or uninitialized logical stamps. The
separately reviewed prerequisite `e81be70a` corrects the existing final append
boundary to zero exactly newly logical unstamped rows. Its raw tests and ledger
remain separate. This actor follows that reviewed base: numeric B2 supplies zero
to newly logical unstamped rule rows, actual initial logical target rows and
stamped rule sources retain provenance, and stable normalization preserves the
earliest representative. The old spare-history compatibility path and caller
initialized-capacity extent were removed. Timestamp capacity is never treated
as an initialized semantic extent; raw comparisons cover actual logical rows.

The v3 governor-refusal claim is withdrawn: its mode 6 accidentally took the
controller-padding path, and its conditional budget-restoration assertion was
unreachable. Those historical passing runs do not establish governor denial.
The corrected mode 6 changes only the actual shared governor's usable bound at
selected positive admissions, requires that injection to occur, checks actual
DENIED status and real token/claim state, and requires ENOSPC with an unstopped
Original report. Zero-byte payload cases are excluded from admission evidence.
The original and peer source images and shared reservation total remain intact;
restoring the usable bound enables a successful physical retry on the same real
sources, followed by actual owner cleanup and final governor zero. No reserved
counter is restored or invented. Unlimited controller overflow is a separate
mode with an exact EOVERFLOW assertion.

Actual extension calls distinguish pre-rule restore denial (zero callbacks)
from post-rule bulk-append denial (1101 callbacks). A separate genuinely
untyped governed target and peer are created through existing allocation,
schema and row-append APIs; their real STRING rule result exercises metadata
length/new-tree denial and metadata cancellation, END/successor, local budget
and overflow cuts. Its raw comparison uses the actual append/consolidate/compact
helpers on that actual peer rather than modifying a session registry or clearing
a type pointer. Read-only test observers report actual denied token state and
never confer source or cleanup authority.


### Capacity-one typed operator constructor prerequisite

The internal capacity-one typed operator constructor copies the existing typed
operator's allocation, ownership, failure and saved-attempt behavior, changing
only the requested capacity from64 to1. The existing entry point and all runtime
callers remain unchanged. This supplies a real empty typed-bottom primitive;
it does not enable typed SCC scheduling or grant a native recovery context.

The registered relation-creation test uses actual compiled IDB schema vectors
for integer, STRING, FLOAT, normalized int32 and17 mixed lanes. The output owns
its own type vector and survives destruction of the actual plan and program.
Physical capacity is1, timestamps are absent, declared input arity remains0,
and the actual Arrow representation follows the original helper: FLOAT g and
other types l. Zero-width NULL and non-NULL type-vector cases retain their
separate existing semantics. Append/clear is compared against the raw relation
and raw type installer, including actual stored values and ownership tokens.

An independent literal packet sequence uses8-byte copy chunks and8-pointer
chunks. The positive typed surcharge is304+320 per lane; a non-NULL zero-width
vector costs192 and a NULL zero-width vector adds0. Complete ordered packets,
exact funding and one-short refusal are tested at widths0,1,4,17, with real
allocator failure at every allocation ordinal and actual governor rejection
at each reserve. Boundary type enums, invalid arguments and inactive/stale
attempt identities preserve the original constructor contract.

Selected genuine entry, enumeration, allocation, commit, installation and
external boundaries exercise cancellation, work exhaustion, overflow, END and
END with a successor. Checks use independently calculated prefixes and frozen
semantic report fields; they do not equate a missing prepaid COMMIT tail hook
with missing parent consumption. Every failed construction leaves the output
NULL, closes real ownership, preserves successor consumption0 and admits a
fresh retry. Typed schema binding, comparison, caller finance and recursive runtime
admission remain separate obligations.

### CI regression prerequisite: controlled append timestamp defaults

The admitted no-growth controlled append route now matches raw append when the
destination tracks timestamps and the source does not: exactly the newly
logical timestamp range defaults to zero. It reuses the existing five-field
controlled zero helper after cell copies and before final publication. The live
prefix and remaining spare cells retain their values; timestamp padding remains
unspecified. Tracked-source copying and the unsupported growth/sourceNoTS guard
retain their existing behavior. No allocation, credit or schema policy changes.

The helper buys5 per appended row, in chunks of at most51 rows/255 work. The
three-row raw/control typed/untyped parity oracle therefore adds15 whenever the
destination tracks timestamps. Independent51/52-row cases test the first255
and final5 packets, exact and one-short refusal, cancellation at each real tail,
unchanged logical rows/generation/credits/source/live prefix and untouched spare
canaries, followed by a genuine reset and exact-limit retry. Empty source adds
no timestamp packet or spare rewrite. A real growth/sourceNoTS case still
returns ENOTSUP after the existing two gate purchases.

This unit fixes the native-reproduced controlled timestamp mismatch. The separate
CONSOLIDATE ordered-literal regression remains known and unfixed here: two
existing256 graph/input guards are absent from that test's independent oracle
(expected50869 versus actual51381 for width0/rows0). Its test-only correction is
the separately approved next unit. Passing this scoped unit is not a full CI or
remote TSan success receipt, and does not activate a canonical seed route.

### CI regression prerequisite: ordered CONSOLIDATE literal guards

The independent CONSOLIDATE schedule now includes the two graph/input guards
already present in production: INPUT192, guard256, OUTPUT192 after the unary child and before the finite
value scan, and LIKE128, graph256, RECIPE128 before output construction. Each
guard executes once on these ordinary fixtures, including width0/rows0; the
source-derived total increase is256+256. No production tariff or guard changes.

The existing complete width0/rows0 exact/one-short prefix matrix and selected
width0/1/17 nonempty prefix matrices retain their exact packet order, raw result,
resource and acknowledgement assertions. The two added packets are exercised
as distinct admissions rather than an appended measured512 adjustment. This
closes the separately recorded literal baseline failure; final-head remote CI
and the remaining canonical/typed/recursive integration still require evidence.

The initial proposed oracle placed the input packet at the later SHAPE192/128
pair. Although its total was correct, the complete prefix test rejected that
ordering. The corrected sequence places it after unary child160/128/256 and
INPUT192: independently derived prefix3648, guard256, exact3904 or one-short
3903 with consumed3648. The later SHAPE192/128 pair stays unchanged. The failed
candidate, initial review and diagnostic are retained in the workflow artifacts.

### Optional private final compaction prerequisite

After actual seed Finalize has drained its readers, B2 batch, scratch and unused
output, the caller may invoke `wl_columnar_session_seed_compact_final_step` before
Take. Existing callers that omit it still receive the original pre-compaction
result. The new COMPACTING progress value is appended; it blocks Take until the
optional operation completes, and repeated completed calls do not repeat a
same-capacity replacement. This is an internal standalone prerequisite, with no
canonical registry/session activation, Source getter, Native permission, or
whole-issue completion claim.

The method verifies the actual selected private owner, self-storage identities,
quiescent descriptor/payload gates, absence of aliases/ledger/shared/arena/pool,
and the actual payload token and geometry. A real zero payload with EMPTY token
and no owner/claim is valid even when the actor's governor still owns descriptor
and metadata credits. Guards are separate bounded actions; a progress enum or a
financial observation alone never grants Native access.

Raw policy remains exact: zero columns do nothing; positive-width empty results
free columns and timestamps, retain scratch and metadata, preserve run metadata,
and touch storage generation only if storage existed. Positive compaction uses
`capacity > 4*rows` and `max(2*rows,64)`, including a real replacement when both
old and new capacity are64. It copies only initialized logical rows and at most
eight bytes per action, zeroing its own new timestamp tail. Initial metadata,
raw FLOAT bits and view generation are preserved. The local copier uses explicit
byte leaves so its source bound does not depend on a loop-index estimate.

The actual governor admits old plus incoming primary/TS storage, with scratch
counted once. A real typed DENIED/OVERFLOW or allocation failure is a soft skip;
control stops remain errors. Commit failure is EFAULT after real rollback. A
failed raw rollback is EAGAIN with the same matching REPLACING claim retained;
retry does not refund/readmit it. A foreign kind or size remains EBUSY until its
actual holder withdraws that claim. Tests use real financial peer reservations,
not invented counter values or physical-allocation/Native claims.

Distinct maintenance funding frees one staged/displaced component or advances
one real refund/downsize/release component per turn. It settles credits only
after the corresponding physical storage is gone. Partial empty destruction
remains compactor-owned until its header and credit close, so generic relation
retirement never receives a half-cleared grid. End can drain this state after
Original cancellation or END/successor without spending Original or successor.
The new record is initialized by the existing metered opaque-context byte loop.

The prepare fee remains228. Its accepted expanded source bound is128, including
failed checkpoint cancellation, plus direct result3 and external64 =195; the
withdrawn80 estimate is not used. Other new intervals reserve64 external source
slots, including actual refusals, and do not certify an arbitrary future session
caller. The active compact cleanup calls actual refund/downsize, two-phase
empty payload release and the explicit96-byte token reset; it does not call
retirement receipt begin/acknowledge. After compact ownership closes, a later
generic End resumes the existing standalone retirement path, which is outside
this optional method's reserved64 composition. Final canonical integration
still needs its own concrete caller proof.

### Physical seed credit receipt funding prerequisite

Actual nonempty credit retirement now buys a separate256 on genuine distinct
cleanup funding immediately before receipt begin (cut1504), and another256
immediately before DONE acknowledgment (cut1505). Existing `retiring` and actual
receipt phase retain the two substages; no new durable state or token API is
introduced. Empty/ungoverned/already released credits and retirement step64 do
not acquire these fees. A refused purchase changes neither token nor receipt
nor credit ownership; a new genuine cleanup attempt resumes the same objects.

These same-depth boundaries include the real receipt clearing bytes. If R is
`sizeof(wl_columnar_memory_retirement_receipt_t)`, the source envelopes are
begin83+R and acknowledge75+R to the immediate actor API return. On the current
qualified ABI R=88, these are171 and163; proposed next20 plus signed carrier62
would compose to253 and245. These are not ABI-independent253 claims: the proposed
external82 composition requires R<=91 for begin and R<=99 for acknowledge.
Standalone256 envelopes instead require R<=173 and R<=181 respectively. The
implementation uses actual `sizeof` clearing, without a platform88 static
assertion or a shortened clear; future geometry/caller qualification must use
its actual R. This prerequisite does not activate that future canonical caller.
The incoming refusal envelopes120/110 remain paid by the real preceding actor
cut, including the new controller front and full unwind.

The registered physical fixture classifies1504/1505 as cleanup funding and
observes const actual token/receipt images around both purchase and primitive.
It checks literal255 refusal and256 admission, cancellation, overflow, END and
END/successor, real fresh cleanup retry, Original report/successor preservation,
physical source images and actual governor totals. An actual ungoverned source
with no scratch exercises fresh governed output resize without editing owner
fields. Censused owner identities select cases rather than determining prices;
base length/D/M/P, restore, scratch, fresh resize and metadata length/new/old
credits are required. Existing42 real governor denial/raised-retry cases remain.

### Standalone single-range radix workspace prerequisite

The optional internal radix workspace attempt prepares owned scratch for one
actual range under a genuine PAYLOAD_MUTATION lease, with skip_sorted=false and
no additional scratch. It does not sort or activate a runtime, recursive host,
pool owner/reset protocol or Native permission. The stable context owns the
canonical reservation and every successful allocation until real End completes.
Its single const borrow requires empty disjoint external output storage and a
synchronously completed consumer before End. Existing raw preparation and sort
APIs remain unchanged; a consumer still uses the actual leased sorter validators.

Preparation authenticates current lease/set identity, nonce, array pointers and
counts, actual member/role/descriptor/owner slots, generations and writer gates.
Membership, physical lanes, types and FNV fingerprint are each visited one per
real256 action. Finished/reused sets fail before further source reads. Checked
sizes and raw allocation order are retained: timestamps first, then all three
A/B/temp attempts before their combined failure verdict. Integer/STRING k8/k16
and FLOAT/small-range insertion thresholds remain raw policy. No uninitialized
scratch content is read or eagerly cleared. Actual DENIED maps to ENOMEM and the
source denial flag; overflow stays EOVERFLOW. A new attempt may retry the same
held source after its actual resource obstruction is removed.

The complete live-set guard is conservatively expanded as96 source slots,
including dispatch/locals, rather than the preliminary46 estimate. Descriptor/self/canonical slots, range bounds/
timestamp shape/capture, and generation validation/capture therefore use
adjacent real256 stages. Shape/generation capture precedes its validation, and
final checks reject subsequent changes. The already paid canonical-writer packet freshly
validates its actual token even when it equals the self writer. Actual fixed
snapshot checks immediately precede READY/borrow's savedOriginal zero; a zero is a guard, not a fresh purchase. The
owned token initialization (actual88 bytes within reserved96) and retained-
governor reserve SELECT/CALL have no
source read or full lease guard; later source-read stages still authenticate
the live set. SELECT captures the actual retained budget pointer; the separately
paid CALL preinstalls its continuation and directly returns only sticky result3.
The actual source reserve leaf retains100/124/164/78 fees, its qualified inter-poll/terminal
bound134 and direct result3 plus reserved external64 =201; conservative first
refusal is162. Neither number certifies a whole positive-to-positive aggregate
or arbitrary future canonical caller.

End frees one actual scratch component per paid action before closing credit.
A separate owned CREDIT_SELECT inspects the initialized canonical token itself,
including RESERVED after child success but before admission_active publication.
Real separately paid begin/ack stages count the actual88-byte receipt clear;
retirement step64 uses genuine distinct cleanup funding. End never requires the
old source/lease snapshot after lawful consumer sorting or source destruction.
It clears workspace bytes in funded chunks only after credits close, then drops
its real governor and Original references. Context initialization likewise
meters8-byte chunks and uses bounded retain_try, including genuine failed-CAS
and post-retain rollback cases. Original/frozen report and successor consumption
remain independent of actual maintenance progress.

The registered fixture compares raw geometry/provenance/allocation ordinals at
0/1/2/32/33/49999/50000 rows for INT64/STRING/FLOAT/mixed types and timestamps,
actual seven-member leases, shared-owner COW consumer, staged refusal/cancel/
END-successor/overflow cuts, literal source100/124/164/78 exact/one-short vectors,
real governor denial/financial overflow peers/raised retries, and the actual
reserve-before-verdict End gap. This is a prerequisite, with no whole issue,
canonical scheduler, pool lifetime or remote CI completion claim.

Independent review correction R1 preserves the actual nullary insertion route:
ALLOC_INSERT and BIND_INSERT select positive insertion count, including width0
with insertion_bytes0. The raw allocator still receives malloc(0); a nonNULL
result is a real owned pointer and publishes insertion capacity=count, while
NULL remains raw-compatible ENOMEM. No minimum-size substitution, nullary
refusal or fictitious zero-byte governor admission is introduced. Governed and
unmanaged count2/32 cases with timestamps on/off compare all actual allocator
ordinals, including the failing zero-size call, capacity/pointer/error and End
closure. Count0/1 and count33 remain independent no-insertion/radix controls.

The resource fixture also resolves review note N1: its existing actual radix
allocation observer is installed with fail_at0 and calls reset during resource
preparation, then restored. The no-allocation assertion on real DENIED now
observes allocator calls rather than an inactive counter. This counter supplies
no work-price oracle. Rejected v1 candidate/review/source receipts remain
preserved under `/tmp/2299-radix-workspace-*-v1` and the archive manifest; this
correction requires a fresh exact raw review and final gates.

### Actual raw recursive cursor before controlled activation

The serial recursive branch now immediately drains a private stack-owned cursor.
The existing raw caller uses it in the same invocation; it does not introduce a
public yield, a heap cursor, a new control permission or a controlled fallback.
Setup, source-reader/pool/sort/merge blocks and K alternatives remain synchronous
raw work in this stage. The original attached-control refusal and nonrecursive
entry stay byte-identical. No native raw helper is certified bounded by moving
it behind a cursor phase.

The cursor separates outer/sub-pass selection, actual selected-head call and
completion, round delta work, error exits and final teardown. The real raw head
still appends FULL before the next head is selected. Old DELTA registration and
best-effort mid-pass removal retain their original order; strict terminal
removal still preserves the first refusal while completing teardown. The
reservation is initialized at its final embedded address and is never copied.
Reader, clone and workspace locals stay inside a single synchronous transition.
Retained cleanup_pending still releases only bookkeeping, preserving the actual
registered delta/cache owners. Aggregate failure retains its distinct cleanup.

The registered recursive_cursor_parity fixture compares 24 actual compiler and
session executions with immutable output from implementation0057d753: ordered
cell bits, schemas, timestamp fields, generations, event order, cap/finality and
pending state. It covers the default limit and multi-head caps1/2 with retry, INT64/STRING/FLOAT
projection SCCs, asserted compiler-selected K, recursive string MAP/FILTER,
actual arithmetic error, typed empty/missing input and extension context/effects.
The extension seed runs once on success, is not replayed by a no-op, and runs
again only on the actual failed-attempt retry; its descriptor remains alive
until real session/plan cleanup. Original18 witnesses were frozen before this
refactor, then independently reproduced byte-identically on untouched0057d753;
six extra controls were captured on that same immutable implementation. The
fixture never calls the new cursor as its reference evaluator or compares
addresses, padding or timing. current_iteration is the last entered sub-pass,
which may differ from the last fact-producing total_iterations.

This stage supplies the real raw schedule seam. Bounded actual delta/radix/run
and pool continuations, authenticated recursive source/frame ownership, typed
controlled transport and ordinary/K expression activation remain separate
integration work. The cursor alone does not close those acceptance rows.

### Actual inline IDB seed Session integration after #2366

Integration baseline is main `8266d893`. The canonical nonrecursive
VARIABLE/[FILTER]/MAP/CONSOLIDATE host composes loaded inline IDB facts with
the separately evaluated rule result. The physical seed actor owns its
preparation, sorting/compaction, Take and End. Session owns the stable arena,
private bindings, actual frame receipts/ACKs and publication. Numeric, string
and float values, rejected FILTER rows, empty input, reversed declarations and
loaded/unloaded facts have raw Session controls.

The independent NORMAL financier belongs only to the selected seed producer.
Its actual accounting overflow retains the old control and identity through
PROBE, SETTLE, constructor-issued BIRTH, DETACH and SWAP. Pending construction
precedes old detachment. A healthy zero probe retains the native terminal
error rather than repeatedly replacing a healthy controller. No seed control
is passed to shared scalar retirement, differential cleanup, observer cleanup,
failed-retry or transaction disposal; those use current main's MAIN owner.

Actor End and its reader/credit closure precede arena release. Financier close
validates its actual FINISH report, detaches and releases its reference. It
never frees the transaction or finalizes Original. Main alone disposes arrays,
moves metadata credit, frees the transaction and releases the moved credit.
Its completed-owner, prepared-close and terminal report/ACK protocols remain
in force. Terminal entry and physical transaction free reject live seed debt.

Precommit errors retain the public descriptor and events. Actual Original END
and an unrelated successor do not finance cleanup: frozen reports and successor
consumption are checked separately. The post-SOURCE observation callback keeps
actual canonical custody pending through a settled MAIN READY result. This
state-only observation creates no Native registration, grant, receipt or ACK;
the Source state getter keeps main's behavior. Committed delivery remains a
separate result from financial cleanup.

The registered seed host suite partitions every discovered public frontier and
partial-header boundary in32 interleaved shards per observer mode, including
Original-END/successor histories. Header tests wait for actual initialized
extents after current main's metadata binding; they do not infer ownership from
public STEP counts. Accepted completed destruction checks the actual absence of
seed financier/actor/arena debt, including a retained main-owned header when
Original ended during publication. Unreachable old seed-only destructor birth
injections are removed; live seed-debt refusal and current main's prepared-close
and report refusal tests remain separate witnesses.

Second-attempt arena ENOMEM regression cases exercise real seed accounting
overflow before seed closure and separately stop the actual MAIN financier
after moved metadata credit has been returned. The latter observes RELEASED,
cleared moved-credit custody and the stopped report, then requires the original
ENOMEM, unchanged output/events, frozen Original/successor0, actual cleanup,
reset/raw retry parity, subsequent insertion and zero final resources.

Generic and specialized JOIN Native dispatchers retain main's implementation;
the qualified child ingress is the actual unary cleanup owner. The two-EDB
empty-input expression controls also exercise the noncanonical general route.
This bundle does not activate arbitrary JOIN/K, recursive cursor transport,
pool or prepared-sort continuations, and does not claim whole-issue completion
for #2299 despite its automatic closure with #2366.


### Declared observed canonical seed STEP qualification

On main `e93020a3`, the fully declared INT64 identity MAP in
`evaluation_facts_sources`, with a loaded head seed, reached legacy observed
preflight phase6 and returned ENOTSUP; the raw
session produced the three expected rows. The matching undeclared controls
reached canonical phase64. The ordinary DIRECT seed hint now admits observers
before legacy identity-MAP classification. The actual paid name matcher still
selects the head slot, and a two-EDB non-head match uses ordinary count3 with no
seed actor. SOURCE/C1 skips the hint; C1, batches and evaluating SNAPSHOT retain
their separate gates. No actor, financier, MAIN, Native or report/ACK authority
is added by the hint.

`evaluation_facts_sources` checks every declaration/load/observer/STEP/SNAPSHOT
cell, literal initial and later rows, signed events, unchanged evaluation,
head insert/remove overlap, schema parity and real cutoff cancellation with
unchanged shadow/publication before retry. The declared and reversed-declaration
host positives prove actual seed ownership, and the existing stop matrix now
also runs the declared identity program. Unmatched observed identity candidates
exercise cancellation, work exhaustion, Original END and an unrelated successor,
with no actor/publication and raw retry parity. Other width mismatches and evaluating SNAPSHOT remain explicitly refused.

The `declared-lifecycle` family executes the actual declared program through
arena governor denial with raised-limit retry and a subsequent insert; committed
END/delivery; constructor/FINISH failures; real financier accounting overflow,
settlement and replacement; failed arena admission/return; observer reader
closure; and second-attempt arena ENOMEM after MAIN moved-credit return. It
retains precise first-error, frozen Original/successor0, no duplicate credit or
events, raw image/schema and final resource assertions. These are actual parsed
program arguments to the original helpers, not production flags used to pretend
selection.

Thirty-two `declared-frontier` shards discover the current declared observed
public frontier census independently, replay every ordinal modulo32 with and
without a successor, and require the matching phase/witness before ending
Original. Each drain checks frozen reports, successor0 and the unpublished old
image/events. Reader binding has its own positive owner witness. The full local
runner retains the unpartitioned completeness oracle; no observed successful
work allowance is used as a primitive price.

This closes the qualified loaded ordinary declared observed seed intersection.
Recursive/K cursor transport, pool and prepared-sort integration, evaluating SNAPSHOT and broader contexts
remain separate work.


### Bare unloaded canonical seed heads

On main `d7950d60`, ordinary canonical seed STEPs with facts not loaded
returned EINVAL while the uncontrolled Session succeeded. The physical actor
built a width-zero working descriptor before seeing the actual rule result.
It then refused that result's width. The target and shadow readers now prove
an empty bare target and no seed rows before deferring private construction.
The public head and its snapshot stay unchanged. RULE_READY may then own no
working descriptor; End drains the actual initialized sources and readers.

After attaching the actual rule result, its real reader and scalar source
precede construction. The existing staged base builder takes the actual rule
width and names, the normal schema-initialization capacity and no timestamps.
The head retains its own declaration and graph identity metadata. Initial
column types remain absent, as in raw schema initialization; the existing
append metadata owner copies actual rule types only for nonempty output.
Empty results still publish their schema. Existing schemaful paths keep their
original construction, width checks, restoration and normalization.

The next observed Session refusal was EVENTS phase10: baseline construction
already accepted the real bare head, but prepare compared its width0 with the
new width1. No baseline schema validator or missing-head permission changes.
During COPY, under the actual source reader, an internal provenance bit records
only a real empty bare head with no physical payload or retired/seed companions
and zero baseline/pending rows. Under the actual candidate reader, prepare may
then give that zero-cell private baseline its first actual candidate width
before checked staging allocation, clearing the bit. Schemaful nullary heads,
nonempty baseline and pending/retired history retain their width refusals.
An allocation failure after promotion retains a coherent empty baseline for
same-width retry; no public schema or event is committed by this operation.

The physical matrix compares the existing INT64/STRING/FLOAT controls with the
uncontrolled evaluator. The additional unloaded component family runs a real
body insert through discovered component identities, cancellation, exhaustion,
Original END, cleanup funding and every actual constructor allocation prefix;
real governor denials require raised-limit retries and final resource closure.
`evaluation_facts_sources` now compares all ordinary declaration/load/observer
cells through empty evaluation, body insertion and unchanged evaluation.
The unloaded host also compares empty schema and later facts loading, and its
separate stop family retains the old bare descriptor/image and no events before
reset/retry. The observer fixture separately rejects schemaful empty/present
nullary, nonempty different-width and pending/retired history controls, and
retries a genuine post-promotion allocation failure.

This increment admits empty bare ordinary heads only. Evaluating SNAPSHOT,
C1/batch, arbitrary schemaful width repair, recursive/K, pool and prepared-sort
activation retain their separate ownership and qualification requirements.

The follow-on unloaded qualification uses the actual fully declared identity
program both with no loaded facts and with a real body row5 inserted into each
Session before the first STEP. The history fixture requires literal row5,
then rows1/5/7 after loading facts, no events on a no-op, signed insertion and
removal of head row9, and no extra event when a seed row5 overlaps the rule.
Empty declared/undeclared numeric, STRING and FLOAT controls also load their
facts after their first empty evaluation and retain raw schema/geometry parity.

The unloaded lifecycle family covers real arena denial/raised retry on the bare
head, then a nonempty rule through committed END/delivery, constructor and
FINISH refusals, actual financier overflow/replacement, arena return, the
observer reader-binding purchase and second-attempt ENOMEM after MAIN moved
credit returned. Existing loaded fixtures retain their literal two-event
oracles; the new one-row fixture supplies its separate literal one-event oracle.

Sixty-four registered shards independently discover all public precommit
frontiers for each observer mode and for empty/nonempty body input. Each actual
ordinal is replayed modulo32 with Original ended, with and without an unrelated
successor. The replay requires the same phase and ownership witness, frozen
Original and successor0, unchanged public image/events, reset/raw retry parity
and final resource closure. The complete local runner retains the unpartitioned
oracle. No successful total work allowance is treated as a primitive price.

Static source inspection bounds the added fixed C fronts conservatively:
TARGET eligibility64, NULL-working dispatch12, source-init/begin caller20,
rule-donor/base-begin caller32, base completion24, identity selection10,
observer provenance72 and width-promotion24. Counts include evaluated member,
address, predicate, store and call/return fronts; they are local added-work
bounds, not fees or complete ancestor certificates. RULE12 executes the actual
step1402 and base1000 admissions (256 each); their financial sum512 is not a
Source distance. Existing staged builder/source/reader children keep their own
actual admissions. No new data-sized loop, synchronous typed constructor or
Source/Native permission is introduced. Complete containing observer/Source
and carry proofs outside this ordinary intersection are not certified here.

### Matching typed staged delta prerequisite and retained sort tails (2026-11-18)

The internal retained `begin_candidate` delta component now admits exactly
matching positive-arity INT64, STRING and FLOAT lanes, including mixtures.
Existing exact declaration, column-name, Arrow-schema and lane metadata equality
remains authoritative. STRING compares stored signed IDs; the caller retains the
intern lifetime. This adds no decoded-string lookup or lexical-order contract.
Nonempty candidate COPY validates finite FLOAT cells in both inputs and
normalizes signed zero only in the private image/delta. NaN and infinities are
rejected. The canonical, valid, sorted source remains a caller precondition.
Empty candidates preserve the existing no-op behavior without revalidating or
repairing source values. Nullary candidate, timestamp, compound, pool/arena,
alias/shared-storage and unsupported lane restrictions remain in place.

Qualification exposed an existing retained merge-sort defect, also reproducible
with an empty source and four sorted distinct INT64 candidate rows. After the
left run was exhausted, `sort_i == sort_j == sort_mid` could hold. Comparing the
selected row with the current left cursor then advanced the wrong cursor and
repeated a right row. Both copy-completion arms now identify a left selection by
`sort_copy_row < sort_mid`: the selected run ranges are disjoint and the midpoint
is fixed while a row copy is active. No attempt field, allocation, charge,
publication order or retry protocol was added. The large finite FLOAT fixture
that previously produced only 64 novel rows from 300 distinct candidates now
requires all 300 literal novel rows before exercising actual late delta growth.
This is a local code/test finding; it does not identify a cause of prior hosted CI
failures.

`test_consolidate_incremental_delta` contains the following direct component
qualification, in addition to its existing tests:

- 60 scalar/mixed-width (1/3/17), shape and quantum combinations compare full
  literal union/difference rows, typed metadata and an independent ordinary
  consolidation oracle. Empty inputs, overlapping/duplicate tuples, sorted
  candidates and signed stored STRING IDs are included.
- 192 generated sorted/reversed/interleaved input cases cover INT64, STRING,
  finite FLOAT and width-17 shared prefixes, with total sizes 1/3/5/17/257/301
  and quantum 1/default. Literal rows and independent raw consolidation must
  agree; candidate physical order remains unchanged.
- Actual one-unit advance censuses are 58/58/170/954 calls and work units for
  STRING/FLOAT scalar and width-3/17 mixed fixtures. The tests witness every
  retained COPY/SORT/DEDUP/COUNT_NOVEL/EMIT_DELTA phase, every copy/compare lane,
  and DONE. They replay 1,244 cancellation cuts (including commit), 1,240 real
  accounting-overflow cuts, all 1,236 positive insufficient work limits and four
  exact completed limits. Every terminal replay closes actual retained owners
  and retries from original inputs with fresh control. Overflow uses the
  existing test-only accounting-history seed, followed by a real refused charge.
- 17 terminal fixtures cover advance/commit cancellation, work exhaustion,
  invalid FLOAT cells in either input, real metadata-allocation failure,
  enforcing-governor refusal, mismatched types and every other declared lane
  type plus an unknown tag. Two additional fixtures check private signed-zero
  normalization and the unchanged no-op boundary.
- Four actual ownership histories exercise both pointer lock orders, source and
  candidate writer contention, wrong-thread advance/commit/abort and competing
  begin, or an actual completed/restarted control generation. The owning thread
  aborts and reacquires the original inputs before successful publication.
- The 300-row late-growth fixture requires an actual governor denial counter
  increment after image/sort storage was admitted, exact reservation rollback,
  unchanged input snapshots/cells and successful fresh retry with all delta
  rows. Metadata allocation and late reserve are actual tested failure fronts;
  this is not an exhaustive census of every physical allocator call.

These tests check real source writer/candidate reader ownership, delta transfer,
refund/reference closure, first-result/report behavior and literal run geometry.
They do not create Native/Source authority. ALLOC_DELTA remains synchronous
inside advance and is not counted as an externally retained phase. Begin still
performs its existing synchronous schema/name/type traversal and allocation;
abort retains its existing cleanup interior. No whole-call 256 bound is claimed.

This closes a matching typed staged-delta transport prerequisite, not #2299.
Raw recursive ROUND pool clones, prepared sort/delta integration, timestamp and
compound transport, typed SCC/K execution, Snapshot/C1 intersections and the
remaining containing Source/observer/carry proofs remain separate work. No
Session routing or recursive/Source activation changes in this increment.

### Declared unary STRING projection SCC integration (2026-11-19, core)

The existing retained general Session transaction now admits an ordinary
single-worker STEP with one declared STRING EDB and one monotone, nonaggregate
recursive stratum containing one or two declared unary STRING pure-IDB heads.
The preserved lowered bodies must be exact VARIABLE/identity-MAP/CONSOLIDATE
projections, optionally two such branches joined by CONCAT. Both original and
preserved body operators and variable names are audited. Actual idb-schema names
and VARIABLE binding names are matched byte by byte before child execution;
real reader-held EDB/public-head descriptors must have matching self-owned,
schemaful STRING storage. The route rejects actual `$in$<head>` seed shadows, including empty
historical ones, instead of dropping host head seeds. Declared inline head facts
also remain outside this scope.

Initial private bottoms use the existing capacity-one typed constructor on the
actual Original attempt. Its fixed local STRING enum lives through the
synchronous call; the returned relation owns independent metadata. The route
uses full previous-round bindings: existing PLAN copies already force FULL,
EDB append rewriting is disabled before audit, and recursive frontier rewriting
is disabled before RANGE. Previous and candidate types are validated before
stored-ID equality decides convergence. Child receipts/ACK, previous retirement,
atomic publication and report/first-error protocols retain their existing
owners. No raw recursive cursor, pool delta or typed frontier implementation is
substituted for these owners.

The dedicated `evaluation_recursive_string_*` core tests compare literal IDs
and independent uncontrolled execution for one-/two-head cycles, observer off/on,
a cold empty EDB, duplicate facts, insert/remove/no-op and typed empty results.
Ninety-six first/later-round histories stop at comparison, an actual VARIABLE
source END or a real child receipt ACK with cancellation, work exhaustion,
Original END and END-with-successor. Every public drain turn preserves the old
head pointers/snapshots/rows and no events; successor work stays zero. Real typed
constructor allocation failure, enforcing-governor denial and iteration-cap
failure preserve their first result and retry from unchanged public heads.
Actual declaration/type/width, nonidentity expression, JOIN, seed, alias, name,
evaluating Snapshot, batch and worker exclusions have separate negative checks.
Recursive session creation already disables C1; this route does not create C1
permission or manufacture an unsupported C1 session for testing.

Added actions keep bounded metadata work separate: routing examines at most two
fixed schema/body records; schema/source names consume one byte per action;
body validation examines one operator’s fixed metadata (seven bytecode bytes
for MAP) per action, then compares VARIABLE names separately. Conservative local added C-front
bounds (member/address/predicate/store and call/return fronts included) are
routing192, schema128, body192, source224, compare192 and typed-bottom caller32;
append/frontier dispatch add at most3 each. The caller bound excludes the typed
constructor's independently admitted interior. These are local work bounds, not
new fees, successful-work-derived prices or complete Source ancestor/whole-call
256 certificates. New phases pass the actual common Original admission and
preserve existing phase numbers.

Core integration is qualified here; complete new-route lifecycle/cut census
remains follow-up work. FLOAT, wider/mixed typed SCCs, additional topology,
expression/K bodies, evaluating Snapshot/C1/batch, seeded heads, pool/prepared
sort transport and the remaining containing Source/carry proofs stay open.

### Unary STRING metadata and typed-bottom checkpoint qualification (2026-11-19)

This increment qualifies the new metadata actions and round-zero typed-bottom
constructor fronts for the existing one/two-head, observer off/on successor
fixtures. Each fixture first establishes healthy public heads, then inserts the
same new interned input into controlled and independent uncontrolled sessions.
It does not expand the admitted STRING domain or close the complete public
ownership-signature/lifecycle or containing Source/carry census.

A healthy run records every actual common checkpoint occurrence in phases
128/129/130, including repeated equal phase/index values within a public STEP.
Both actual outgoing boundaries are separate keys: schema/body to NAMES (4),
and later source audit to EVALUATE (8). Replays require the entire reached prefix
of these selected observations to match before injecting at its exact ordinal.
A divergence is diagnosed and fails; it is not redirected to another action.

A `WL_SESSION_TEST_HOOKS` bracket surrounds only the actual round-zero
`new_typed_one` invocation. It exposes the real session, Original/saved attempt,
round, head and entry/exit result, plus a const private output. The existing
relation work callback is enabled only within that authenticated scope. ENTRY
precedes constructor admission. Each work callback follows its paid body and
precedes the actual zero poll, including zero-work reservation tails. Healthy
EXIT owns an empty, self-owned, schemaful unary STRING result of capacity one
before entry binding; a stop there must retire that private result later, not
pretend that it never existed. The bracket adds no poll, result change, runtime
state or authority and compiles out of normal builds.

For the current Linux fixtures, both observer settings discover these counts;
each invocation rediscovers them instead of treating them as API constants:

| Heads | Metadata 128/129/130 | Actual outgoing boundaries | Constructor work tails | ENTRY/EXIT | Allocation calls |
| --- | --- | --- | --- | --- | --- |
| 1 | 5 / 27 / 18 | 2 | 197 | 2 | 13 |
| 2 | 6 / 36 / 24 | 2 | 195 per head | 2 per head | 13 per head |

All 240 metadata/boundary keys and 1,186 constructor keys across the four
fixtures receive cancellation, a genuinely refused work charge, actual Original
END, and END followed by a genuinely begun successor: 960 + 4,744 = 5,704 stop
histories. Work values and observed totals are not prices. Work tails retain
actual token-commit prefixes 0/1/3/7 through the existing commit callback. The
constructor trace is the finite reached schedule; it does not certify all
identity-CAS retry schedules or opaque governor/allocator-library interiors.
Repeated work values remain distinct occurrences, and no unreachable healthy
suffix is required after an intentional stop.

Allocation qualification uses the existing one-based constructor allocation
seam, separately from work-tail ordinals. A scoped healthy UINT32_MAX countdown
counts actual constructor heap/Arrow allocation calls. Ordinals 1..N must consume
the seam and return ENOMEM; N+1 must succeed with one remaining count. Every
EXIT clears the countdown before unrelated work. The four fixture cells cover
78 genuine allocation failures and six N+1 controls. Failed-constructor error
and actual private-output ownership are checked before ordinary cleanup/retry.

Every failed public drain preserves old head pointers, generation snapshots,
STRING type/literal rows and zero emitted events. END cases preserve every
frozen Original report field, and successors consume zero work. Actual cleanup
is followed by reset and same-input retry, with literal IDs/schema/sorted rows
and signed events compared with uncontrolled execution. Session teardown must
return all governor reservations and leave its sole reference. This final zero
is not presented as a complete intermediate financial/owner certificate.

`evaluation_recursive_string_cuts` assigns each metadata and constructor key
exactly once across 16 modulo shards per fixture, plus separate allocation
families, retaining normal 120-second watchdogs. Each shard checks its exact
selected outcome count. Pilot ASan timings for the two-head observed fixture
were 3.072 seconds (metadata shard), 16.707 seconds (constructor shard), and
4.175 seconds (allocation family); timings are watchdog evidence only. The
separate `evaluation_recursive_string_focused` families select actual ENTRY/EXIT
and both handoffs plus first/last metadata, commit-prefix and zero-tail
occurrences for focused TSan/optional-library execution; these representatives
are not relabeled exhaustive. The original seven core families and their 96
histories remain registered independently.

Native and ASan run all discovered new metadata/work/allocation cuts. Focused
GCC/Clang TSan and enabled optional-library runs cover every head/observer cell,
all allocation ordinals and both END outcomes at their selected fronts. Full
public child/MAIN/observer/retirement/publication products, arbitrary names and
graph sizes, broader types/topology/modes/transport and remaining Source/carry
proofs remain follow-up work. No whole-call 256 or whole-#2299 closure follows.

### Unary STRING public child/receipt CANCEL and WORK representatives

This increment qualifies a finite set of actual public-return child/receipt
custody representatives for **CANCEL and genuinely refused WORK only**. Public
Original END/END-with-successor, round-retirement/MAIN injection, public SEED,
observer preparation, registry/claims/publication and postcommit delivery remain
open. Existing internal metadata/bottom/core END tests are separate evidence;
they do not qualify these public END products.

The parsed one/two-head fixtures first establish their ordinary warm result,
then append the same freshly interned STRING input to controlled and raw
sessions. The real session must have no cold relation. Current main also changes
cold-source execution/layout, so current cold-source and session-options tests
are included in regression validation; their admission is not borrowed by this
STRING route.

A `WL_SESSION_TEST_HOOKS`-only copied view checks actual transaction initialization
extent, array/schema/original construction markers and head/entry bounds before
reading roles. It returns initialized scalar words, no mutable pointers,
controller or grant. Normalized signatures retain actual phase, round, head and
round index; previous/candidate/frontier presence, binding/Original equality and
round-owner roles; admission and receipt/ACK flags; genuine retirement journal
state; authenticated framed child phase, pin/gate/source/batch/candidate/capture
custody and bound numeric operator/cursor; normalized MAIN and observer journals.
Nested child/source reads require their own initialization markers. No retired
relation payload or nested cold attempt is inspected. Historical IDs/addresses
are reduced to role/presence predicates. Repeated name/row/copy/clear offsets are
not distinct normalized classes, and observed work is not a price.

After the actual round-zero typed-bottom callback, injection requires a real
public `EAGAIN`, `round_active`, uncommitted/unpublished transaction, actual phase
EVALUATE(8), COMPARE(19) or REBIND(20), and no MAIN identity, round owner or
round-retirement progress identity. SEED(18), RETIRE(21) and all other phases are
outside this injection partition. Filtering limits injection only: every
prepublication return, including excluded gaps and the entire stopped drain,
still checks old public pointers, complete generation snapshots, unary STRING
rows/types and zero events. Cleanup and same-input retry follow the ordinary
Session protocol even when they later enter excluded phases.

Each process rediscovers its healthy trace with actual global public-call
ordinals. Excluded gaps break segments; no synthetic REBIND-to-EVALUATE edge is
invented across omitted retirement/seed work. Select first/last occurrence of
each exact class, both endpoints of first/last occurrence of each distinct
ordered within-segment class-pair transition, and every segment endpoint. The
focused rule retains class/pair first occurrences and segment endpoints. Union
selected ordinals before deterministic modulo16 sharding; every selected point
receives both outcomes. Replays authenticate every reached eligible signature
and its global ordinal before injection; divergence fails rather than retargets.
These are representatives of observed classes/transition types, not every
repeated transition, public offset or internal action.

Current Linux discovery gives the same counts for observer off/on:

| Heads | Eligible returns | Classes | Transition instances / types | Segments | Full / focused representatives | Excluded RETIRE returns |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1,696 | 342 | 1,554 / 364 | 2 | 472 / 396 | 939 |
| 2 | 4,065 | 830 | 3,651 / 883 | 3 | 1,136 / 960 | 2,807 |

Private previous/candidate, child and receipt custody are actually witnessed.
COMPARE/REBIND have no public `EAGAIN` observation in these fixtures; their
internal actions are not newly qualified by a fabricated public witness.
Native/ASan full families cover 6,432 selected CANCEL/WORK histories across four
cells; focused first-occurrence families cover 5,424. Normal 120-second watchdogs
remain. Counts are rediscovered, not API constants. Focused TSan and optional
library runs exercise the separately labeled focused families, retaining every
observed class/pair type and segment boundary.

A terminal WORK result can retain a genuine metadata-origin/credit owner rather
than dispose the transaction. That actual predicate selects checks for protected
insert/remove/snapshot/destroy `EBUSY`, unchanged first report and public image,
and actual reset/same-input retry. A disposed case instead checks actual
transaction absence through the completed-owner view; generic backend PENDING
alone is not proof of a live transaction. Retry must match literal IDs, STRING
schema/sorted rows and signed events in raw execution. Actual teardown returns
all governor reservations and its sole reference; this is not a complete
intermediate financial certificate.

Two broader experimental probes did **not** establish closure and are excluded
from passing totals. On base `006d9793`, an experimental two-head observed public
child cut with actual Original END first returned `EINVAL` with native cleanup
pending; attempted ordinary drains reached the 1,000,000-iteration bound still
at `EAGAIN`. A one-head retirement WORK probe also reached that bound instead
of WORK, at actual RETIRE(21), round/index 0, head 1, child absent, MAIN identity
present (phase 2, recipient 2), retirement identity present but not begun. These
are observed probe facts, not a diagnosed cause, alternate closure protocol,
universal failure or ordinary-user reachability claim. Their experimental
ordinals are not stable identifiers. Public END/successor and retirement/MAIN
qualification require follow-up; no normal-runtime fix is claimed here.

FLOAT, wider/mixed SCCs, additional topology, expression/K bodies, evaluating
Snapshot/C1/batch modes, seeded heads, pool/prepared-sort transport and remaining
containing Source/carry proofs remain outside the qualified scope. This
increment does not close the public lifecycle or #2299.

### Selected retirement MAIN packet after Original WORK

The previously unclosed retirement-WORK probe has a bounded follow-up fix. On
base `a8617bea`, a current-fixture reproduction reached round-zero RETIRE with
an uncalled SCALAR MAIN packet already AVAILABLE and a registered retirement
progress not yet begun. Genuine refused Original work then produced 19,040
ordinary `EAGAIN` returns with the same packet. The regression selects this
actual owner state; historical public ordinals are not its authority.

The source cycle is specific: retirement admission returns the sticky Original
WORK before calling the selected recipient; MAIN dispatch leaves AVAILABLE to
that recipient; operation finish refuses while that MAIN packet is selected.
The fix retains the Original zero-work poll and permits only its WORK result
for an exact existing AVAILABLE/READY SCALAR packet to reach ordinary
`main_action`. The match includes session, Step, transaction, Original identity
and attempt, actor, action/target/NULL slot and the live progress resource tuple.
A cleared READY progress is an existing receipt only. Native proof, private
financier, execution, report and receipt consumption remain the existing owners.
Fresh retirement still returns the Original stop before selecting work. Other
errors, cancellation and the earlier ended-Original branch keep their paths.

Five deterministic modes run for one/two heads and observer off/on: no-stop
control, refused WORK on the selected AVAILABLE packet, fresh-action refusal,
matching READY consumption and cleared-progress READY consumption. The
unchanged production code fails the AVAILABLE regression at its finite bound,
with zero performed actions/reports instead of terminal WORK. After the fix,
that selected packet performs exactly one Native action and settles exactly one
report before terminal WORK. Fresh/READY modes perform no new Native action
after refusal, and READY no longer owns a pending packet at the terminal return.
Every fixture must witness its actual state; none silently succeeds without it.

Each stopped public return preserves old head pointers, generation snapshots,
STRING schema/literal rows and no events. The terminal report belongs to the
actual Original attempt and retains WORK; genuine retained metadata/credit
ownership still protects input mutations. Reset and same-input retry must match
raw sorted rows and signed events. Actual teardown returns all reservations and
the governor's sole reference. This is a regression for an existing packet,
not a complete intermediate financial or retirement/MAIN lifecycle certificate.

The independent public-END child probe remains open. This fix does not qualify
new types/topology/modes/seeds/transport, every retirement offset/packet kind,
observer/publication products or remaining containing Source/carry proofs.
Existing internal and public child/receipt suites remain separate evidence.

### Observed CONCAT ownership after public Original END

The public-END follow-up has a concrete current-base correction. On `79a5b502`,
finite diagnostic sampling closed 1,700 of 1,760 owner histories, while 60 CONCAT
histories remained pending for both END and END-with-successor. An early
VARIABLE cut closed after 1,176 cleanup calls; a shorter bound would have
misclassified it. These diagnostic counts are not exhaustive qualification.

The failing path reaches `COMPOSE_CONCAT_CHILD` through registered observed
cleanup. GDB showed an authenticated `observed_native_cleanup` parent and a real
private funding attempt (limit 3,072, consumed 1,320), followed by `EINVAL` from
the CONCAT latch's unconditional JOIN-owner proof. The frame remains owned and
the same cleanup cannot advance. This establishes a wrong proof selection,
not a refused grant or a general END failure.

The CONCAT latch now selects the existing full observed cleanup proof only for
that recorded observed-cleanup parent; otherwise it keeps the JOIN proof.
Either proof failure returns directly, with no alternate-proof fallback. Numeric
owner, parent/child, frame and CONCAT context/resource authentication, funding
charges, running/committed checks and first nonzero execution result remain.
Provisional observed cause follows the existing filter latch's semantic-result
precedence. No new UNARY admission or manufactured Native authority is added.

Four fixtures (one/two heads, observer off/on) discover actual first/last
CONCAT_START-pending and CONCAT_CHILD witnesses for every reached recursive
round, authenticate the complete copied signature at the real public-call
ordinal, then separately perform actual Original END and END-with-successor.
The one-head fixtures reach two rounds and 16 CONCAT histories each; two-head
fixtures reach three rounds and 24 each, for 80 CONCAT histories total. Eight
early VARIABLE END controls and four healthy CONCAT controls are separate.
Absent witnesses fail; diagnostic or historical ordinals are not retargeted.
The unchanged-production promoted regression remains pending at its 20,000-call
bound, with first `EINVAL`, while the corrected cases genuinely finish cleanup.
That bound exceeds the measured successful diagnostic cleanup maximum of 2,067
calls and includes the healthy prefix; normal 120-second test watchdogs remain.

Every cleanup return preserves old public head pointers, generation snapshots,
unary STRING literal rows/types and zero events. Every visible error must remain
`EINVAL`, and the already finished Original report stays fieldwise unchanged.
A genuine successor remains the same active attempt and consumes zero work
throughout old cleanup; after closure its own owner checks an uncancelled zero
poll and finishes it with zero consumption/result. Pending protected APIs remain
`EBUSY`. Final checks require no active/pending frame, transaction, MAIN/parent or
retirement debt, then actual reset/same-input retry matches raw rows/events.
Teardown returns all governor reservations and its sole reference.

This qualifies the finite observed-CONCAT END/successor regression within the
existing declared unary STRING projection route. It does not certify every
public offset, all child/retirement/MAIN/observer/publication states, wider types
or topology, additional evaluation modes/seeds/transport, or the remaining
containing Source/carry proofs. It does not close #2299.

### Public bound-child END stage representatives

This test-only follow-up expands the finite public END/successor representatives
beyond the two CONCAT numeric stages. It uses the merged observed-CONCAT proof
selection without changing production code, admission, recovery or ownership.
Current-base discovery and replay use the existing copied, initialization-guarded
custody view; earlier diagnostic counts are not substituted for this campaign.

Injection requires an actual public `EAGAIN`, the existing eligible predicate,
EVALUATE phase8 and an active uncommitted/unpublished round, no MAIN or retirement
identity, and an authenticated frame/context with capture-only, child-bound and
numeric-identity flags. Round-owner absence is part of existing eligibility.
Unbound/bootstrap, frame-absent receipt-only, retirement/MAIN, observer and
publication states are outside this injection slice. The healthy discovery
reuses the existing post-typed-bottom public trace. Excluded returns remain
outside the representative set; replay cleanup may traverse them normally.

The explicit initialized class tuple is round/head; active/changed/append/frontier
round flags; child-active/thread-valid flags; receipt pending/ACK/acknowledged,
index-valid and ACK-committed flags; both normalized relation-custody masks; and
child words2..12 (outer phase/pending/custody, numeric identity/phase/pending,
operation cursor/kind, initialized Source phase and primary/cleanup result).
Numeric cursor identifies an operation stage, not per-row progress. There are
no raw addresses, attempt IDs, public ordinals or repeated copy/row offsets in
the class key. The entire copied public signature is stored separately and must
match at the discovered actual public-call ordinal before real END injection;
replay never retargets a nearby stage.

Select first and last observation of every class, union identical endpoints,
then shard selected cut indices modulo16. Each selected cut independently runs
END and END-with-successor. Per-cut output records class, index, real public
ordinal, endpoint roles and outcome; shard-union validation requires every
selected index exactly once for each outcome and both endpoints of every class.
These are finite class representatives, not every public offset or transition
pair. Actual per-round/head presence and declared CONCAT-start/child witnesses
are required; absent witnesses fail.

Current discovery is identical with observer off/on:

| Heads | Bound-child observations | Classes | Selected cuts | END/successor histories per cell | Reached rounds |
| --- | --- | --- | --- | --- | --- |
| 1 | 1,604 | 324 | 402 | 804 | 2 |
| 2 | 3,798 | 776 | 958 | 1,916 | 3 |

All four cells total5,440 histories. VARIABLE, identity MAP, CONCAT and
CONSOLIDATE operation stages actually occur, as does a completed cursor with no
current operation. All five receipt flags remain zero in this bound-child
slice; no receipt-flag transition or frame-absent receipt qualification follows.
The full copied signature still authenticates each injection.

The eligible healthy trace also records excluded unbound/frame-absent/other
returns:88/2/2 for one head and255/6/6 for two. Outside eligible trace, retirement
and selected-owner counters are939 each for one head and2,807 each for two.
Those counters overlap and must not be summed or counted as injected histories.
They do not establish END behavior at retirement or MAIN.

Every injected case reuses the existing first-`EINVAL` and completed-Original
report preservation, unchanged public head pointers/generation snapshots/STRING
rows/schema/no events, pending protected-API `EBUSY`, successor active identity
and zero consumption, uncancelled successor finish after genuine closure,
IDLE/no frame/transaction/MAIN/parent/retirement debt, same-input reset/retry to
raw rows/events and final governor reservations/reference checks. The existing
20,000-call drain bound and ordinary120-second watchdog remain. Healthy and
early VARIABLE controls plus the92-case CONCAT regression remain separate.
Thread-sanitizer CI applies its existing timeout multiplier4, making the
effective watchdog480 seconds; no new timeout multiplier is introduced.

Native and ASan/UBSan validate all16 shards and all5,440 new histories. GCC/Clang
TSan and enabled-crypto native/ASan use the explicitly named focused shard0/16
across all four cells:344 new histories, not all new classes/endpoints. The
existing92 CONCAT/early/healthy cases remain in every profile. Full stage tests
are not registered when `b_sanitize` contains `thread`, so all-tests hosted TSan
CI also runs only the focused new family. Native/ASan register the full family
and the separately named focused aliases. This split keeps
full stage qualification distinct from focused sanitizer/library regression.

No new runtime defect or production fix is claimed. Full public offsets,
ordered transitions, concurrency/financial products, unbound/bootstrap and
frame-absent receipt-only END, retirement/MAIN/observer/claims/publication and
remaining containing Source/carry proofs remain open. Existing unary STRING
scope is unchanged; wider/mixed/FLOAT, extra topology/expression/K bodies,
evaluating Snapshot/C1/batch, seeds and pool/prepared-sort transport remain
outside qualified scope. This increment does not close #2299.

### Frame-present child-unbound END representatives

This test-only increment qualifies finite first/last END/successor representatives
for the adjacent frame-present child-unbound slice. Production code, recovery,
observation hooks and unary STRING admission do not change. Healthy discovery
and stopped replays run on the current implementation; previous bound-child
campaigns are separate evidence.

Injection requires an actual eligible public `EAGAIN` after the real typed-bottom
checkpoint, EVALUATE phase8 in an active uncommitted/unpublished round, no
MAIN/round-owner/retirement identity, and an initialized authenticated frame
with capture-only set and child-bound clear. Numeric identity absence is not a
condition: the copied residual identity word actually takes both values0 and1.
Nested numeric phase/cursor/op are not directly read when unbound. The existing
copied view supplies initialized zeros under its nested guards; Source phase is
still guarded by its own initialization marker.

The exact class tuple reuses the preceding stage tuple: round/head and round
custody flags, child-active/thread-valid, all five receipt flags, normalized
relation owner masks and initialized child words2..12, including residual numeric
identity. Complete copied signatures authenticate the actual discovered public
ordinal independently. First/last endpoints are unioned before each cut runs
real END and END-with-successor; no retargeting, synthetic grant or manufactured
cleanup authority is used.

Healthy discovery actually contains LOOKUP/CHILD_BEGIN and pending child-begin
states, plus SPARE_PROOF/NORMALIZE/CLOSE_PROOF/DONE. LOOKUP must have capture-done
clear and SPARE_PROOF must have it set at every reached round/head. Actual
`CHILD_ACK` consumption clears child-bound and advances to SPARE_PROOF; that
source path and observed capture state distinguish later unbound states from
initial lookup. Unbound is therefore not a synonym for bootstrap. The observed
pending actions are none, CHILD_BEGIN and CHILD_ACK. All five receipt flags stay
zero; no receipt transition or frame-absent receipt qualification follows.

Current discovery is identical with observer off/on:

| Heads | Unbound observations | Classes | First/last cuts | END/successor histories per cell | Rounds |
| --- | --- | --- | --- | --- | --- |
| 1 | 88 | 14 | 18 | 36 | 2 |
| 2 | 255 | 42 | 54 | 108 | 3 |

All four cells total288 new histories. Per-cut records identify class, cut index,
actual public ordinal, endpoint roles and outcome. Coverage validation requires
every cut exactly once per outcome and both endpoints of every discovered class.
The four complete selectors fit the existing120-second watchdog; TSan retains
its existing hosted multiplier4/effective480seconds. No focused subset or new
sharding is needed for the new unbound family: all288 run in every profile.

Excluded eligible observations remain separate: one head has2 frame-absent,
1,604 bound-numeric and2 bound-but-numeric-cleared returns; two heads have
6/3,798/6 respectively. These are not injected by the unbound selector. Outside
eligibility, retirement and selected-owner counters are939 each for one head
and2,807 each for two; these overlap and must not be added. The later unbound
stage does not qualify the preceding bound-but-numeric-cleared cut or following
frame-absent receipt cut.

The existing replay still requires first visible `EINVAL` and an unchanged
completed Original report on every cleanup return, the old public head pointers,
generation snapshots, STRING rows/schema and no events, pending protected API
`EBUSY`, same active successor with zero consumption, its own uncancelled finish
after old cleanup, genuine IDLE/no frame/transaction/MAIN/parent/retirement debt,
ordinary reset/same-input raw retry and final governor reservations/reference.
The20,000-call bound is unchanged. No new runtime defect or fix is claimed.

Validation uses all new histories in native, ASan/UBSan, GCC/Clang TSan and
enabled-crypto native/ASan. Sized existing regressions on native/ASan include
all92 CONCAT/early/healthy cases,20 retirement modes, four bound-child focused
shard0 cases, STRING core/stops/refusals/failures, focused metadata/constructor
and public focused shard0 cases. Extra profiles use a smaller explicitly listed
old healthy/CONCAT/bound-child/stops/retirement/controller selection. Current
cold right-JOIN/source/resident integration and ABI/source/format/CI-shard guards
are checked on the current baseline. The full previous bound-child, internal
metadata and public CANCEL/WORK campaigns are not reported as rerun here.

This is finite class-endpoint qualification, not all public offsets, transition
pairs, concurrency schedules or financial products. Bound-but-numeric-cleared,
frame-absent receipt-only, retirement/MAIN/observer/claims/publication END and
remaining containing Source/carry proofs remain open. Existing unary STRING
ordinary STEP scope is unchanged; wider/mixed/FLOAT, extra topology/expression/K,
evaluating Snapshot/C1/batch, seeds and pool/prepared-sort remain outside scope.
This increment does not close #2299.


### Adjacent public END custody bundle and phase-8 partition (2026-11-25)

This three-commit bundle qualifies the two adjacent public END families left
open by the preceding frame-present unbound increment: bound-but-numeric-cleared
and frame-absent retained receipt custody. It adds no runtime admission, language
feature or production hook. The earlier statements that these two families remain
open are superseded only for the finite fixtures and endpoints described here.

`cleared` requires an initialized capture-only frame, child-bound set and numeric
identity absent. The exported numeric words5..9 must be zero. Actual observations
are framed PENDING with pending action CHILD_ACK, capture-done set and no Session
receipt flags. This is distinct from an unbound child: the framed ACK has not yet
cleared child-bound. The `partition` audit checks phase and pending action
separately; they must not be conflated.

`receipt` requires an eligible frame-absent public return with positive copied
receipt custody, all16 child words zero, and the immediately preceding copied
observation at the same round/head authenticated as a real unbound frame. The
observed Session tuple is pending=1, ACK=1, acknowledged=0, index-valid=0,
ACK-committed=0 (tuple3, reported receipt bitmap8). Zero copied child phase/pending
words are absent-frame initialization, not observed framed execution stages.
No vanished payload is inspected and no receipt, grant or owner is manufactured.

Both selectors discover healthy actual public EAGAIN observations after typed
bottom, group initialized finite stage/custody tuples and union each class's
first/last endpoints. Full copied keys and actual public ordinals independently
authenticate replay. Every selected endpoint executes both genuine Original END
and Original END with a live successor. The shared oracle retains first visible
EINVAL and the frozen Original report, unchanged old public heads/STRING values/
schemas/generations and no events, protected API EBUSY, successor identity and
zero consumption, genuine cleanup closure without frame/transaction/MAIN/parent/
retirement debt, successor finish, reset/same-input raw retry, exact rows/signed
events and final governor/reference balance. The20000-call drain and120-second
registration watchdogs are unchanged.

| Family | Heads | Observer modes | Observations/classes/cuts per cell | END/successor histories per cell |
| --- | --- | --- | --- | --- |
| Bound numeric-cleared | 1 | off/on | 2/2/2 | 4 |
| Bound numeric-cleared | 2 | off/on | 6/6/6 | 12 |
| Frame-absent receipt | 1 | off/on | 2/2/2 | 4 |
| Frame-absent receipt | 2 | off/on | 6/6/6 | 12 |

The two families contribute64 new histories per profile. Here first and last
coincide for every newly discovered class; the runner unions endpoints rather
than double-counting them. Validation parses each cut/class/ordinal/outcome
record, requiring exactly one outcome2 and one outcome3 for each cut and both
endpoints of every class.

The independent `partition` census takes **all** recorded eligible phase8
EVALUATE returns as its domain, not the union of accepted predicates. It fails
on unknown or overlapping shapes and requires each family at every reached
round/head. Observer off/on yield the same current discovery:

| Heads | Domain | Bound live numeric | Frame-present unbound | Bound cleared | Frame-absent receipt | Rounds |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1696 | 1604 | 88 | 2 | 2 | 2 |
| 2 | 4065 | 3798 | 255 | 6 | 6 | 3 |

Eligible other-phase observations are counted separately (zero in these current
fixtures); COMPARE/REBIND19/20 are not silently admitted. Outside eligibility,
retirement and selected-owner exclusion counters are939 each for one head and
2807 each for two; they overlap and must not be summed.

Validation on the combined test source: all12 new-family/partition registrations
pass in Clang native, Clang ASan/UBSan, enabled-mbedTLS native and ASan/UBSan, and
GCC/Clang POSIX TSan (72 registration executions total). Every profile executes
all four head/observer cells and all new endpoints/outcomes; native/ASan builds
use debugoptimized and non-LTO. Existing native/ASan regression selections also
pass36 adjacent registrations (CONCAT END, unbound, retirement, focused lifecycle
and bound-stage cases) and14 broader registrations (STRING rows/stops/refusals/
failures, resident facts, cold-right JOIN and selected source/build policy guards)
each. Another12 registrations per native/ASan profile cover metadata shard0,
public-focused shard0 and bottom-allocation cases across all four fixtures.
Seven additional source-access/public-header/expression-inventory/shard contract
guards pass. Full earlier internal metadata, bound-stage and public
CANCEL/WORK campaigns are historical unless explicitly named as rerun; no full
matrix or hosted MSVC certification is inferred from this local selection.

This closes the **discovered phase8 four-family custody partition and the two
new finite END endpoint families**, not every offset, transition pair, concurrent
schedule or financial product. Retirement/MAIN/observer/claims/publication END,
containing Source/carry proofs and other legal hosts remain open. The qualified
ordinary unary STRING STEP domain is unchanged: wider/mixed/FLOAT, additional
topology, expression/K bodies, evaluating Snapshot/C1/batch, seeded heads and
pool/prepared-sort remain outside scope. This bundle does not close #2299.
