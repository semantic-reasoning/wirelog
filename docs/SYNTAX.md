# Wirelog Syntax Reference

**Last Updated:** 2026-04-06

---

## Table of Contents

1. [Declarations](#declarations)
2. [Rules](#rules)
3. [Inline Facts](#inline-facts)
4. [Directives](#directives)
5. [Query Directive](#query-directive)
6. [Types](#types)
7. [Expressions](#expressions)

---

## Declarations

Declare relations with typed attributes:

```
.decl RelationName(attr1: type1, attr2: type2, ...)
```

Example:

```
.decl Edge(src: int32, dst: int32)
.decl Name(id: int32, label: string)
```

---

## Rules

Rules define how to derive new tuples from existing data:

```
Head(x, y) :- Body1(x, z), Body2(z, y).
```

Features:
- **Negation**: `!Rel(x)` in rule body (stratified negation)
- **Comparisons**: `x < y`, `x = y`, `x != y`, `x >= y`, etc.
- **Arithmetic**: `x + 1`, `x * y`, `x % 2` in head or comparisons
- **Aggregation**: `min(x)`, `max(x)`, `sum(x)`, `count(x)`, `average(x)` in head
- **Wildcards**: `_` for anonymous variables
- **Plan marker**: `.plan` before a rule for optimization hints

---

## Inline Facts

Define base data directly in the program:

```
Edge(1, 2).
Edge(2, 3).
Name(1, "alice").
```

Inline facts contain only constants (integers or strings), not variables.

---

## Directives

### .input

Load relation data from external files:

```
.input Edge(IO="file", filename="edge.csv", delimiter=",")
```

### .output

Write computed relation to stdout or a file:

```
.output Path
.output Path(filename="path.csv")
```

### .printsize

Print the number of tuples in a relation:

```
.printsize Path
```

---

## Query Directive

The `.query` directive specifies **demand-driven optimization** by declaring
which argument positions of a relation are bound (known at query time) versus
free (to be computed). This enables the Magic Sets optimization pass to restrict
evaluation to only the tuples reachable from the query, reducing intermediate
result sizes for recursive programs.

### Syntax

```
.query RelationName(a1, a2, ..., aN) .
```

Each `a_i` is one of:
- `b` -- **bound**: the argument value is known at query time
- `f` -- **free**: the argument value is to be computed

### Examples

Query all paths reachable from a known source node:

```
.decl Edge(x: int32, y: int32)
.decl Path(x: int32, y: int32)
.output Path
.query Path(b, f) .

Path(x, y) :- Edge(x, y).
Path(x, y) :- Edge(x, z), Path(z, y).
```

Here `.query Path(b, f)` declares that the first argument of `Path` is bound
(e.g., we only want paths starting from a specific node). The engine generates
magic demand relations (`$m$Path_bf`) that restrict evaluation to only reachable
tuples, avoiding full materialization of the transitive closure.

Query with all arguments bound (point query):

```
.query Path(b, b) .
```

Query with a mixed pattern on a ternary relation:

```
.query Triangle(b, f, b) .
```

### Behavior

- `.query` is **optional**. Programs without `.query` directives evaluate all
  `.output` relations fully (all-free adornment), which is the default behavior.
- When `.query` is present, the Magic Sets pass generates demand propagation
  rules that prune the search space based on the bound positions.
- An all-free `.query` (e.g., `.query Path(f, f)`) is equivalent to no `.query`
  and results in no optimization (the pass is a no-op).
- Multiple `.query` directives may appear in a single program for different
  relations.

### Background

The Magic Sets transformation is based on the foundational work by Beeri and
Ramakrishnan:

> C. Beeri and R. Ramakrishnan. "On the power of magic." *Journal of Logic
> Programming*, 10(3-4):255-299, 1991.

The technique rewrites a Datalog program to simulate top-down evaluation within
the bottom-up (semi-naive) framework, achieving goal-directed computation
without sacrificing the termination guarantees of bottom-up evaluation.

---

## Types

Supported column types:
- `int32` -- 32-bit signed integer
- `int64` -- 64-bit signed integer
- `string` -- variable-length string
- `symbol` -- interned symbol (string stored as integer ID)
- `functor/arity` -- compound term handle stored in a 64-bit column
- `functor/arity side` -- explicit side-relation compound storage
- `functor/arity inline` -- inline compound storage, limited to arity 4

---

## Expressions

### Arithmetic Operators

`+`, `-`, `*`, `/`, `%` (modulo)

### Bitwise Operators

`band(x, y)`, `bor(x, y)`, `bxor(x, y)`, `bnot(x)`, `bshl(x, y)`, `bshr(x, y)`

### Hash Functions

`hash(x)`, `md5(x)`, `sha1(x)`, `sha256(x)`, `sha512(x)`, `hmac_sha256(msg, key)`

`hash(x)` returns an `int64` xxHash3 value. The mbedTLS-backed
digest/HMAC built-ins also return `int64` values: digest or HMAC
bytes are folded with `XXH3_64bits()` and are not exposed as hex
strings or byte arrays. With `mbedTLS=disabled`, the crypto built-ins
parse and plan; when they are used to compute relation values or
predicates without crypto support, columnar expression evaluation fails
closed. Filters reject the row, while MAP/head and REDUCE expression
contexts return an evaluation error.

**`sha256(x)` is not SHA-256 of `x`.** These built-ins return
`XXH3_64bits(digest)`, so the result is 64 bits wide and is not the
digest itself. It is still reproducible outside wirelog, in two steps:

```
printf 'abc' | sha256sum | cut -d' ' -f1 | xxd -r -p | xxhsum -H3
```

The same recipe works for `md5`, `sha1`, `sha512` and `hmac_sha256`
(via `openssl dgst -mac hmac`). Use the digest for fingerprinting and
change detection, not where a full-width cryptographic digest is
required — 64 bits is not collision-resistant at scale.

### What the digest functions cover

`hash`, `crc32_ethernet`, `crc32_castagnoli`, `md5`, `sha1`, `sha256`,
`sha512`, `hmac_sha256` and `uuid5` all take their bytes from the
**declared type** of the operand:

- A `symbol`/`string` operand digests the string's own bytes,
  `strlen()` many, **with no NUL terminator**. `hash("abc")` is
  therefore the value `printf 'abc' | xxhsum -H3` prints, and
  `crc32_ethernet(payload)` is the value `zlib.crc32(payload)` prints.
- A numeric operand digests the 8-byte little-endian `int64`
  representation of its value.

`hmac_sha256(msg, key)` and `uuid5(namespace, name)` decide the two
operands independently, so `hmac_sha256(sym, 42)` keys the HMAC with the
8 bytes of `42` and messages it with the symbol's bytes.

**The guarantee is only as good as the `.decl`.** Column types are not
enforced: a column declared `symbol` that actually holds integers which
were never interned digests their `int64` representation instead — the
same answer the numeric form would have given, and the query still runs
rather than failing. A column with no declared type at all digests the
interned id, which is *not* stable across runs; `WL_LOG=EVAL:2` reports
each digest that falls back this way, and `WL_LOG=EVAL:4` reports each
value whose reverse lookup failed. Declare the relation.

### Checksum Functions

`crc32_ethernet(x)`, `crc32_castagnoli(x)`

Both return a non-negative `int64` in the range `[0, 2^32)`, computed
over the operand's bytes as described above: the string's bytes for a
`symbol` column, the 8-byte `int64` representation for a numeric one.
`crc32_ethernet` uses the Ethernet/ISO-HDLC polynomial (0x04C11DB7,
ISO 3309 / IEEE 802.3); `crc32_castagnoli` uses the Castagnoli
polynomial (CRC-32C, iSCSI/SCTP). Unlike the crypto hash built-ins
above, the CRC-32 functions are always available and do not depend on
the `mbedTLS` build option. See `examples/05-crc32-checksum/` for a
frame-integrity validation example.

### UUID Functions

`uuid4()`, `uuid5(namespace, name)`

The UUID built-ins require mbedTLS for non-zero runtime output and
return the first 8 UUID bytes as an `int64`, not formatted text.
Disabled UUID calls follow the same fail-closed behavior described for
crypto hash functions.

**Operand boundaries are unambiguous.** With a `symbol` operand,
`uuid5()` length-prefixes each operand before hashing — the length as a
little-endian `uint64` — so distinct `(namespace, name)` pairs always
hash distinct bytes:

```
uuid5("ab", "c")  !=  uuid5("a", "bc")
```

A bare concatenation could not promise that: `"ab" + "c"` and
`"a" + "bc"` are the same three bytes. RFC 4122 sidesteps the question
by fixing the namespace at 16 bytes, making the split point unambiguous
by construction; wirelog adopted the two-operand signature without that
property, so it makes the framing explicit instead. In Python:

```python
buf = pack('<Q', len(ns)) + ns + pack('<Q', len(name)) + name
d = bytearray(sha1(buf).digest())
d[6] = (d[6] & 0x0F) | 0x50
d[8] = (d[8] & 0x3F) | 0x80
value = unpack('<q', bytes(d[:8]))[0]
```

`uuid5(int64, int64)` keeps the older unframed `SHA-1(ns || name)` — its
operands are 8 bytes each, so its split point is already fixed and its
values are unchanged.

**`uuid5()` is still not RFC 4122**, and being unambiguous does not make
it so. RFC 4122 requires the namespace to *be* a 16-byte UUID and defines
a 128-bit result; wirelog accepts whatever bytes the operand carries and
returns only the first 8 of the 16 digest bytes. Treat the result as a
namespaced fingerprint, not as a UUID.

### Comparison Operators

`=`, `!=`, `<`, `>`, `<=`, `>=`

The ordering operators (`<`, `>`, `<=`, `>=`) compare **numerically** on
numeric columns and **lexicographically** (byte-wise, as `strcmp`) on
`string`/`symbol` columns. Which one applies is decided from the declared
column types, so a rule like

```
Before(a, b) :- Event(a), Event(b), a < b.
```

sorts event names alphabetically when `Event` is declared
`.decl Event(name: string)`.

`=` and `!=` are type-independent: symbols are interned canonically — one
id per distinct string — so id equality already is string equality.

**Undeclared columns compare by intern id.** Symbols are stored as
interned integer ids, and ids are assigned in the order strings are first
encountered. When a column has no declared type — the relation has no
`.decl`, or the ordering operates on a value the declaration does not
describe, such as an argument of an inline compound — the ordering
operators fall back to comparing those ids, which orders symbols by when
they were first seen rather than alphabetically. That result is *not*
stable: adding an unrelated earlier fact changes it. Declare the relation
to get lexicographic ordering. `WL_LOG=EVAL:2` reports each ordering
comparison that falls back this way.

Mixing a string operand with a numeric one in an ordering comparison also
falls back to id order, and is likewise reported at `EVAL:2`.

**The digest built-ins deliberately do not follow that last rule.** An
ordering comparison needs *both* operands reversed to the strings they
name before it means anything, so a one-sided type match keeps the
integer comparison. A digest has no such coupling: each operand
contributes its own bytes, so `hmac_sha256(sym, 42)` and
`uuid5(42, sym)` take the symbol's bytes for the symbol operand and the
`int64` bytes for the numeric one. Applying the comparison rule here
would leave every mixed call digesting an interned id, which is the
defect the digest functions were fixed for.
