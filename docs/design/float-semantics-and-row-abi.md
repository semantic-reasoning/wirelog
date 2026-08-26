# Floating-Point Semantics and Row ABI

**Status:** Normative design prerequisite for float support (issue #1229).
**Related:** [#1226](https://github.com/semantic-reasoning/wirelog/issues/1226),
[#1225](https://github.com/semantic-reasoning/wirelog/issues/1225).
**Scope:** IEEE-754 `double` values in parser, execution, relations, public APIs,
Arrow, CSV, and aggregates.

This document fixes the contracts that must be implemented before #1226 is
landed. It intentionally describes the target behavior rather than claiming
that float execution is already available.

## 1. Representation and type identity

`wirelog_float_t` is an IEEE-754 binary64 (`double`) and occupies one existing
64-bit physical relation lane. A supported build must satisfy `sizeof(double) ==
8`, `__STDC_IEC_559__`, `FLT_RADIX == 2`, `DBL_MANT_DIG == 53`, and
`DBL_MAX_EXP == 1024`, enforced by C11 static assertions. The lane
is copied with `memcpy` (or an equivalent C11 bit-copy helper), never by a
pointer cast or a signed-integer conversion. The relation's logical type
metadata remains authoritative; a float lane must not be compared, sorted, or
hashed as an `int64_t`.

Serialized float values use the IEEE binary64 bits encoded as an unsigned
64-bit little-endian word. This makes snapshots and CSV/Arrow round trips
independent of host byte order; native relation lanes may retain the existing
`int64_t`-sized allocation, but conversion to and from the canonical wire word
is mandatory at an interchange boundary. The in-memory typed-row ABI uses
host-order `uint64_t` bit lanes; only serialization byte-swaps to the canonical
little-endian word. Supported builds use round-to-nearest,
preserve subnormals, disable contraction/FMA for deterministic reductions, and
either set `FLT_EVAL_METHOD == 0` or explicitly store every operation at
binary64 precision.

`WIRELOG_TYPE_FLOAT` is the sole public type identity for float columns. Adding
float support must not renumber existing enum values or change the size or
meaning of existing public structs.

## 2. Value policy

The initial implementation accepts finite binary64 values only.

| Value | Parse/API/CSV input | Expression result | Equality/hash/order |
|---|---|---|---|
| finite value | accept | accept | numeric value |
| `+0.0`, `-0.0` | accept, canonicalize | canonicalize | equal and same hash |
| NaN | reject with a diagnostic | reject/fail closed | never enters a relation |
| `+inf`, `-inf` | reject with a diagnostic | reject/fail closed | never enters a relation |

Validation is transactional: parser/lowering rejects the program; a typed
insert/remove validates every row before changing the relation; and a failed
evaluation step rolls back all changes from that step and emits no callbacks.

Canonicalization converts either signed zero to `+0.0` at every write
boundary, including parser materialization, API insertion, CSV decoding, and
operator results. This preserves reflexive relation equality and makes joins,
deduplication, and arrangements agree.

Ordering is the ordinary total order of finite numeric values after zero
canonicalization. In particular, all negative finite values precede zero and
all positive finite values follow zero. A future extension may define a total
order for NaN and infinities, but no implementation may silently invent one.

The same typed helpers must be used by radix sort, merge comparison, binary
search, TDD deduplication, arrangement hashing, joins, semijoin/antijoin, and
LFTJ. Equality, ordering, and hashing are one contract: if two values compare
equal, they must hash equally.

### 2.1 Snapshot encoding

The float portion of a versioned snapshot is a sequence of little-endian
`uint64_t` words in physical-lane order, preceded by the existing relation
schema/type metadata and row count. The format version and endianness marker
are mandatory; a reader rejects an absent marker, a non-binary64 type code, or
an invalid word before mutating any relation. This is the serialization
contract for the existing snapshot/export path; it does not change the legacy
callback ABI.

## 3. Language and mixed types

Decimal and exponent literals are float literals. The initial grammar accepts
`DIGITS "." DIGITS [EXP]` and `DIGITS EXP`, where `EXP` is
`[eE][+-]?DIGITS`. It rejects `.5` and `1.` until a lexer rule can distinguish
those spellings unambiguously from punctuation. A float literal is therefore
unambiguously followed by the fact terminator in `1.5.`. Unary minus is a
separate unary token accepted before a float literal with or without whitespace,
while subtraction remains a binary operator; the AST records minus as unary
negation and constant folding diagnoses non-finite results. Float literals are
represented as a distinct AST/IR value, not as an integer token. Integer
magnitude and overflow follow the signed-int64 rules in `docs/SYNTAX.md`.

Initial mixed-type rules are deliberately narrow:

* A float column may receive an integer literal only when conversion to binary64
  is exact; otherwise compilation fails with a source diagnostic.
* Integer and float columns may not be compared, joined, or used as a common
  key implicitly. A future explicit cast can widen this contract.
* Mixed integer/float arithmetic and comparisons are rejected at plan lowering;
  there is no implicit promotion. An integer literal in an explicitly resolved
  float context uses the exact-conversion rule above.
* Arithmetic operands must have one resolved numeric type. Float arithmetic
  produces a canonicalized finite float; overflow to infinity or NaN is an
  error, not a stored value.
* A decimal literal is never silently truncated to an integer.

The parser/IR and execution plan therefore need a distinct float constant
opcode/value kind. A bit pattern occupying the existing physical lane is not
itself a type conversion.

Compound syntax uses the same typed literal rules as scalar arguments: for
example, `make_compound(f, 1.5)` stores one `WIRELOG_TYPE_FLOAT` argument and
`make_compound(f, 1)` stores an integer argument. The parser preserves the
argument type in AST/IR; it does not route a float through the legacy
`wirelog_compound_arg_t.value` field. The typed constructor in §4 is the only
public construction path for a float argument. Side-tier handles remain
opaque integer handles in the parent relation, while the side relation stores
the typed argument metadata.

## 4. Public row ABI

Existing public callbacks and row APIs that expose `int64_t` buffers remain
binary compatible and retain their integer contract. Their signatures must not
be changed to `double *`, and a float bit pattern must not be accepted through
an integer API without an explicit documented operation.

Float exposure is additive through one selected ABI: a versioned
`wirelog_typed_row_v1_t` descriptor and matching typed insert/snapshot/delta
entry points. Its exact v1 layout is:

```c
typedef struct wirelog_typed_row_v1 {
    uint32_t struct_size;
    uint16_t abi_version;       /* 1 */
    uint16_t reserved;
    uint32_t logical_ncols;
    uint32_t physical_nlanes;
    uint32_t physical_stride;
    const uint32_t *types;              /* wirelog_column_type_t codes */
    const uint32_t *lane_offsets;       /* logical_ncols */
    const uint32_t *physical_types;     /* physical type codes */
    const uint64_t *lanes;              /* physical_nlanes */
} wirelog_typed_row_v1_t;
```

The optional diagnostic output is also fixed-width and caller-owned:

```c
typedef struct wirelog_typed_error_v1 {
    uint32_t struct_size;
    uint32_t code;       /* stable subcode, zero on success */
    uint32_t row_index;
    uint32_t logical_col;
    char *message;
    uint32_t message_capacity;
} wirelog_typed_error_v1_t;
```

The library writes at most `message_capacity - 1` bytes, always NUL
terminates when capacity is nonzero, and retains no pointer. `code`, row, and
column identify the first validation failure; the function result remains the
existing `WIRELOG_ERR_EXEC` for ABI compatibility.

The v1 entry points are `wirelog_session_insert_typed`,
`wirelog_session_remove_typed`, `wirelog_session_set_typed_delta_cb`, and
`wirelog_session_snapshot_typed`; their callback is
`wirelog_on_typed_tuple_fn`. The exact batch signatures are:

```c
wirelog_error_t wirelog_session_insert_typed(
    wirelog_session_t *, const char *,
    const wirelog_typed_row_v1_t *rows, uint32_t num_rows,
    wirelog_typed_error_v1_t *error);
wirelog_error_t wirelog_session_remove_typed(
    wirelog_session_t *, const char *,
    const wirelog_typed_row_v1_t *rows, uint32_t num_rows,
    wirelog_typed_error_v1_t *error);
typedef void (*wirelog_on_typed_tuple_fn)(
    const char *relation, const wirelog_typed_row_v1_t *row,
    int32_t diff, void *user_data);
wirelog_error_t wirelog_session_set_typed_delta_cb(
    wirelog_session_t *, wirelog_on_typed_tuple_fn, void *user_data);
wirelog_error_t wirelog_session_snapshot_typed(
    wirelog_session_t *, wirelog_on_typed_tuple_fn, void *user_data);
```

Each array element is one row; `num_rows` is the batch size and each row's
`physical_nlanes` is its physical width. `types` and `physical_types` are
fixed-width `uint32_t` codes equal to `wirelog_column_type_t` values; the ABI
does not depend on the implementation's C enum width. A descriptor is valid
only when all pointers required by a nonzero count are non-null, offsets are
monotonic and in range, `physical_stride == physical_nlanes`, and its logical
types/offsets/physical types exactly match the relation schema. Invalid
descriptors, an unsupported type/version, or a float passed to an old int64-
only entry point return the existing `WIRELOG_ERR_EXEC` code and leave state
unchanged. No new enum value is introduced by this ABI version.

The descriptor is row-major by logical column: `lanes` has
`physical_stride == physical_nlanes`, `lane_offsets[i]` identifies the first
physical lane for logical column `i`, and `physical_types[j]` identifies the
type of every physical slot; inline compounds use consecutive offsets.
Callback views are borrowed for the duration of the callback only;
insertion views are read synchronously and are not retained. The caller owns
all input buffers, output lanes are 8-byte aligned, and the library owns output
buffers only until the callback returns. `struct_size` must cover all v1 fields,
permits additive tail fields, and `abi_version` is rejected when unsupported.
New public prototypes use `WIRELOG_API`; existing ABI surfaces are preserved
and return an explicit unsupported-type error if an old int64-only entry point
is used with a float relation.

`lanes` contains host-order binary64 bit patterns for float slots and existing
integer bit patterns for integer slots; it is not a byte string. The typed
accessor performs `memcpy(&value, &lane, sizeof value)`. A typed compound
constructor is likewise additive:

```c
typedef struct wirelog_typed_compound_arg_v1 {
    uint32_t type; /* wirelog_column_type_t code */
    uint64_t bits;
} wirelog_typed_compound_arg_v1_t;
wirelog_error_t wirelog_session_make_compound_typed(
    wirelog_session_t *, const char *, uint32_t,
    const wirelog_typed_compound_arg_v1_t *, uint32_t, uint64_t *);
```

The legacy `wirelog_session_make_compound` rejects float arguments with
`WIRELOG_ERR_EXEC`; it never reinterprets an `int64_t` argument as a float.

For completeness, v1 lane encoding is: signed/unsigned 32-bit and 64-bit
columns use their existing zero/sign-extended integer representation; booleans
use `0` or `1`; interned strings use their existing integer symbol id; float
columns use the binary64 bits described in §1; and compound parent columns use
opaque handles. `physical_types` is authoritative for validation, so a caller
cannot smuggle a float through an integer slot. The descriptor's `struct_size`
must equal or exceed `offsetof(wirelog_typed_row_v1_t, lanes) + sizeof(lanes)`;
all schema arrays remain borrowed for the duration of the call.

The compatibility matrix is:

| Surface | Existing behavior | Float behavior |
|---|---|---|
| `int64_t` row callback | unchanged integer rows | not used for implicit float conversion |
| `wirelog_typed_row_v1_t` API | additive, versioned | reports `WIRELOG_TYPE_FLOAT`, reads `wirelog_float_t` |
| relation metadata | existing values preserved | carries float type per logical column |
| direct IR/plan consumers | existing opcodes preserved | new float kind/opcode, rejected by old consumers |

## 5. Arrow and columnar interchange

An Arrow float column is `NANOARROW_TYPE_DOUBLE`, not `NANOARROW_TYPE_INT64`
with a reinterpretation. Relation construction, deep copies, snapshots, and
exports must preserve the logical schema for every column. The Arrow buffer is
materialized as native `double` values from the canonical lane bits; the
validity and row-count behavior remains identical to integer columns.

The initial Arrow mapping is one field per logical scalar column. Inline
compound expansion retains the existing physical field ordering and emits one
field per physical argument slot; a float argument is `DOUBLE`. Relation
construction and deep copies carry a logical `wirelog_column_type_t` plus a
physical-lane type/offset entry for every slot. Nested Arrow structs are not
used: side-tier compounds are exported as their existing handle field and
separate side relation, while an inline compound is flattened in physical
field order. Export rejects a request that cannot provide this exact mapping.

## 6. CSV and CLI contract

Float input is parsed with a locale-independent decimal parser. In source
programs it accepts the same exact forms as the language (`DIGITS.DIGITS`,
optionally with an exponent, or an integer mantissa with an exponent); `.5`,
`1.`, NaN, and infinity are rejected initially. In a schema-declared float CSV
field, an integer digit sequence is additionally accepted as the exact decimal
spelling of a binary64 value, so `%g` output such as `1` round-trips. A leading
sign is accepted as part of the input field and is canonicalized for negative
zero. The typed adapter path uses the v1 row
descriptor; the legacy `int64_t **` adapter path returns
`WIRELOG_ERR_EXEC` for a float schema. Whitespace and error-location
behavior must match existing scalar fields.

Float output uses a locale-independent, shortest round-trippable
representation with no more than `DECIMAL_DIG` significant digits. The
implementation may use the equivalent of `%.*g` with precision `DECIMAL_DIG`
(never a hard-coded 17); suppressed trailing zeroes are intentional, so `1`
is a valid rendering of `1.0`.
`-0.0` is emitted as `0`. The output/input round trip must recover the same
finite binary64 value, modulo the specified zero canonicalization.
CSV schema or relation metadata determines whether a field is integer or
float; the reader must not infer a float merely because an integer field has a
large magnitude.

## 7. Aggregates and determinism

`min` and `max` use the typed finite-value order. For a consolidated Z-set,
`sum` is `Σ(multiplicity * value)`, `average` is that sum divided by
`Σ(multiplicity)`, and min/max consider values with positive effective
multiplicity. Float aggregates return `WIRELOG_TYPE_FLOAT`; existing integer
aggregates retain their existing result types. An empty `sum` returns `0.0`;
empty `min`, `max`, and `average` produce no aggregate row and a successful
empty result. A zero effective average count also produces no row. Retractions
are applied during consolidation and are never silently treated as positive
input. Overflow, invalid operation, and non-finite intermediate values fail
the evaluation before publishing its partial result; the input batch/state is
unchanged and no callbacks are emitted for that failed step.

Parallel reduction uses one canonical complete binary tree over the ordered
input leaves. The leaves are the distinct post-consolidation rows sorted by
typed lexicographic row order; each leaf stores its value multiplied by its
effective multiplicity, so multiplicities are folded rather than repeated.
For a non-power-of-two count, the tree is the minimal complete tree whose
missing leaves are exact `0.0` values. Every internal node performs one
binary64 addition; workers may evaluate ready nodes in any schedule, but node
ownership and child order are fixed by the tree and the final result is stored
after every operation. `average` reduces the corresponding value/count trees.
Thus a fixed input produces the same bits regardless of worker count or
scheduling on a supported platform. For
`n` finite inputs with no underflow, the permitted pairwise-sum error is
`gamma_k * Σ(abs(x))`, where `u = 2^-53`, `k = ceil(log2(n))`, and
`gamma_k = (k*u)/(1-k*u)`; the bound assumes round-to-nearest and no FMA,
excess precision, or flush-to-zero.

## 8. Staged implementation and conformance fixtures

Implementation proceeds in these gates:

1. Add type metadata, literal AST/IR, plan constant kind, and parser tests.
2. Add shared typed equality/order/hash helpers and route every relation/index
   consumer through them.
3. Add float expression evaluation and `sum`/`min`/`max`, then `average`.
4. Add typed public access, Arrow schema propagation, CSV decoding, and
   round-trippable output.
5. Run focused parser, radix, join, dedup, aggregate, and I/O tests plus the
   sanitizer and threaded suites.

The conformance suite must include:

* negative and positive finite sort order, including values adjacent to zero;
* `+0.0`/`-0.0` insertion, deduplication, joins, and identical hashes;
* rejection of NaN/infinities at every input boundary and on invalid results;
* exact versus inexact integer-to-float cases;
* float key joins, arrangements, semijoins, LFTJ, and multi-column ordering;
* Arrow `DOUBLE` schema preservation and finite CSV output/input bit equality;
* aggregate empty, overflow, weighted retraction, zero-count, and
  fixed-schedule determinism cases;
* compile-time v1 descriptor layout/size checks, legacy-API rejection, typed
  callback lifetime checks, canonical little-endian encoding, and cross-endian
  fixture decoding;
* decimal boundary, exponent, unary-minus, fact-terminator, and compound float
  argument parsing cases.

No implementation stage is complete until its fixtures exercise the shared
typed helpers rather than only testing parser acceptance.
