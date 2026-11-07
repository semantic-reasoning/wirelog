# Bounded expression acceptance (#2299)

Baseline: main `54f76fb7`. This ledger tracks O02/O03 and C01/C02. It is an
inventory and implementation contract, not a statement that the issue is closed.
PR #2344 is still open at this baseline; its JOIN host work must be preserved
when integrating scalar domains. #1819 and #1953 have merged. Existing numeric
qualification remains in `docs/implementation/1821/SERIAL_COVERAGE.md` and
`test_evaluation_numeric{,_operators,_plan}.c`; it is not reimplemented here.

## Finite tag inventory

Every enumerated expression tag has exactly one row below. `existing-numeric`
means the current attempt decoder admits it; it does not certify every host
context. `empty-only` means the decoder admits it only for raw-compatible empty
setup, not ordinary evaluation. `open` identifies raw-supported work still
needing bounded implementation/integration. `legacy-rejected` is not a coverage
gap to cure by adding language semantics. The inventory guard compares these
rows with source, so adding or broadening a decoder tag requires revisiting the
ledger explicitly.

| Tag | Baseline disposition |
| --- | --- |
| `WL_PLAN_EXPR_VAR` | existing-numeric |
| `WL_PLAN_EXPR_CONST_INT` | existing-numeric |
| `WL_PLAN_EXPR_CONST_STR` | open |
| `WL_PLAN_EXPR_BOOL` | existing-numeric |
| `WL_PLAN_EXPR_VAR_FLOAT` | existing-numeric |
| `WL_PLAN_EXPR_CONST_FLOAT` | existing-numeric |
| `WL_PLAN_EXPR_VAR_STRING` | open |
| `WL_PLAN_EXPR_EXTENSION_CALL` | open |
| `WL_PLAN_EXPR_EXTENSION_CALL_ABI` | open |
| `WL_PLAN_EXPR_ARITH_ADD` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_SUB` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_MUL` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_DIV` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_MOD` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_BAND` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_BOR` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_BXOR` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_SHL` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_SHR` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_FLOAT_ADD` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_FLOAT_SUB` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_FLOAT_MUL` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_FLOAT_DIV` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_BNOT` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_HASH` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_CRC32_ETH` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_CRC32_CAST` | existing-numeric |
| `WL_PLAN_EXPR_ARITH_MD5` | open |
| `WL_PLAN_EXPR_ARITH_SHA1` | open |
| `WL_PLAN_EXPR_ARITH_SHA256` | open |
| `WL_PLAN_EXPR_ARITH_SHA512` | open |
| `WL_PLAN_EXPR_ARITH_HMAC_SHA256` | open |
| `WL_PLAN_EXPR_ARITH_UUID4` | open |
| `WL_PLAN_EXPR_ARITH_UUID5` | open |
| `WL_PLAN_EXPR_CMP_EQ` | existing-numeric |
| `WL_PLAN_EXPR_CMP_NEQ` | existing-numeric |
| `WL_PLAN_EXPR_CMP_LT` | existing-numeric |
| `WL_PLAN_EXPR_CMP_GT` | existing-numeric |
| `WL_PLAN_EXPR_CMP_LTE` | existing-numeric |
| `WL_PLAN_EXPR_CMP_GTE` | existing-numeric |
| `WL_PLAN_EXPR_CMP_FLOAT_EQ` | existing-numeric |
| `WL_PLAN_EXPR_CMP_FLOAT_NEQ` | existing-numeric |
| `WL_PLAN_EXPR_CMP_FLOAT_LT` | existing-numeric |
| `WL_PLAN_EXPR_CMP_FLOAT_GT` | existing-numeric |
| `WL_PLAN_EXPR_CMP_FLOAT_LTE` | existing-numeric |
| `WL_PLAN_EXPR_CMP_FLOAT_GTE` | existing-numeric |
| `WL_PLAN_EXPR_AGG_COUNT` | existing-numeric |
| `WL_PLAN_EXPR_AGG_SUM` | existing-numeric |
| `WL_PLAN_EXPR_AGG_MIN` | existing-numeric |
| `WL_PLAN_EXPR_AGG_MAX` | existing-numeric |
| `WL_PLAN_EXPR_AGG_AVG` | legacy-rejected |
| `WL_PLAN_EXPR_STR_FN_STRLEN` | open |
| `WL_PLAN_EXPR_STR_FN_CAT` | open |
| `WL_PLAN_EXPR_STR_FN_SUBSTR` | open |
| `WL_PLAN_EXPR_STR_FN_CONTAINS` | open |
| `WL_PLAN_EXPR_STR_FN_STR_PREFIX` | open |
| `WL_PLAN_EXPR_STR_FN_STR_SUFFIX` | open |
| `WL_PLAN_EXPR_STR_FN_STR_ORD` | open |
| `WL_PLAN_EXPR_STR_FN_TO_UPPER` | open |
| `WL_PLAN_EXPR_STR_FN_TO_LOWER` | open |
| `WL_PLAN_EXPR_STR_FN_STR_REPLACE` | open |
| `WL_PLAN_EXPR_STR_FN_TRIM` | open |
| `WL_PLAN_EXPR_STR_FN_TO_STRING` | open |
| `WL_PLAN_EXPR_STR_FN_TO_NUMBER` | open |
| `WL_PLAN_EXPR_STR_FN_UUID5_RFC` | open |
| `WL_PLAN_EXPR_CMP_STR_EQ` | empty-only |
| `WL_PLAN_EXPR_CMP_STR_NEQ` | empty-only |
| `WL_PLAN_EXPR_CMP_STR_LT` | empty-only |
| `WL_PLAN_EXPR_CMP_STR_GT` | empty-only |
| `WL_PLAN_EXPR_CMP_STR_LTE` | empty-only |
| `WL_PLAN_EXPR_CMP_STR_GTE` | empty-only |
| `WL_PLAN_EXPR_ARITH_HASH_S` | open |
| `WL_PLAN_EXPR_ARITH_CRC32_ETH_S` | open |
| `WL_PLAN_EXPR_ARITH_CRC32_CAST_S` | open |
| `WL_PLAN_EXPR_ARITH_MD5_S` | open |
| `WL_PLAN_EXPR_ARITH_SHA1_S` | open |
| `WL_PLAN_EXPR_ARITH_SHA256_S` | open |
| `WL_PLAN_EXPR_ARITH_SHA512_S` | open |
| `WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS` | open |
| `WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI` | open |
| `WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS` | open |
| `WL_PLAN_EXPR_ARITH_UUID5_SS` | open |
| `WL_PLAN_EXPR_ARITH_UUID5_SI` | open |
| `WL_PLAN_EXPR_ARITH_UUID5_IS` | open |

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
