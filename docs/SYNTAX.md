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
  (at most one per head — see below)
- **Wildcards**: `_` for anonymous variables
- **Plan marker**: `.plan` before a rule for optimization hints

### Limitation: one aggregate per rule head

A rule head may contain **at most one** aggregate. The internal `AGGREGATE`
node carries a single aggregate function and a single aggregate expression,
so there is nowhere to record a second one. Programs such as

```
t(g, min(v), max(v)) :- val(g, v).      /* rejected */
t(min(v), max(v))    :- val(g, v).      /* rejected */
t(g, min(v), min(w)) :- val(g, v, w).   /* rejected */
```

are rejected when the rule is lowered. Loading fails with
`WIRELOG_ERR_PARSE`, which the CLI prints as a bare `Parse error`. To see
which relation was rejected and why, run with `WL_LOG=PARSER:1`:

```
$ WL_LOG=PARSER:1 wirelog_cli prog.dl
[ERROR][PARSER] .../wirelog/ir/program.c:1811: relation 't' has 2 aggregates
in one rule head; at most one is supported. Derive each aggregate in its own
rule and join the results ...
```

Without `WL_LOG` set there is still no explanation — surfacing one by
default is tracked as #979.

Derive each aggregate in its own rule and join the results:

```
.decl val(g: int64, v: int64)
.decl tmin(g: int64, a: int64)
.decl tmax(g: int64, b: int64)
.decl t(g: int64, a: int64, b: int64)

tmin(g, min(v)) :- val(g, v).
tmax(g, max(v)) :- val(g, v).
t(g, a, b) :- tmin(g, a), tmax(g, b).
```

This rewrite is exact, not an approximation, and the reason is worth stating:
the split rules keep **identical bodies**, so they group over the same tuple
set and produce the same group keys. `REDUCE` emits one row per group, so the
join is total on both sides and no group can be dropped. That property holds
only while the bodies stay identical — if you later add a filter to one of
them, the join stops being total and groups can disappear.

This restriction is a memory-safety guard, not only a clarity one. A head
that emits a different number of columns than its `.decl` declares causes an
out-of-bounds read in the columnar REDUCE path when the relation is
recursive; multiple aggregates were one way to reach that state.

Two scope notes:

- The check covers the **parser path only**. A caller that builds an
  `AGGREGATE` IR node directly, without going through `wirelog_parse_string`,
  is not validated — neither the optimizer, the plan generator, nor the
  columnar REDUCE checks that the group-by width plus one matches the head
  arity.
- The check does **not** cover every arity mismatch. A single-aggregate head
  with too few arguments for its `.decl`, such as
  `cc(y, min(c)) :- cc(x, c, d), edge(x, y).` against a 3-column `cc`, is
  still accepted and still unsafe in a recursive stratum. General
  `.decl`-versus-rule arity validation is tracked separately as #977, whose
  fact half has since landed (see below) while the rule-head half has not.

### Inline facts must match their declared arity

A fact whose argument count differs from its relation's `.decl` is rejected
when the program is loaded (#977):

```
.decl val(g: int64, v: int64)
val(1, 5, 7).       /* rejected: 3 arguments, 2 declared */
val(1).             /* rejected: 1 argument, 2 declared */
val(1, 5).          /* accepted */
```

The `.decl` may appear before or after its facts; the check runs once all
declarations have been collected.

This is a memory-safety guard. Facts are packed at the width they are
written and read back at the declared width, so a mismatch previously caused
an out-of-bounds read, or an uninitialised read that produced heap bytes as
query answers with exit status 0, or a tuple assembled from two different
facts. Loading fails with `WIRELOG_ERR_PARSE`; run with `WL_LOG=PARSER:1` to
see which relation and which arities.

A relation with an inline-compound column must be given the compound form,
not a flattened one — `p(1,2,3).` against `.decl p(id: int64, lbl: pair/2
inline)` is rejected, because the fact is compared against the declared
*logical* width.

Facts on a relation with **no** `.decl` at all are unaffected by this check
and continue to fail later, during loading, with a less specific message.

The count is not the only constraint on aggregates in a head — the aggregate
must also be written **last**. `t(min(v), g) :- val(g, v).` lowers with a
single aggregate and the correct arity, but emits its columns group-by-first
regardless of the order written, so the values land in the wrong columns with
no diagnostic. That is tracked as #980 and is not affected by this check.

Aggregate names are matched as exact keywords. `average` and `AVG` are
recognized; `avg` and `AVERAGE` are not. An unrecognized name followed by `(`
has no production in head-argument position, so `t(g, avg(v), max(v))` is a
plain syntax error rather than a rule that bypasses this check.

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

**Nothing records which of the two forms was used**, so a symbol whose
bytes coincide with an `int64`'s digests identically to that integer:
`hash("abcdefgh") == hash(7523094288207667809)`. That is the price of
the byte transparency above — a type tag in the digest input is exactly
what would stop `printf '%s' abcdefgh | xxhsum -H3` from reproducing
`hash("abcdefgh")` — and it is not being changed. `uuid5()` is the one
exception; see below. `docs/SECURITY_MODEL.md` states the caveat in
full.

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

**Operands are framed, so the digest input is unambiguous.** With a
`symbol` operand, `uuid5()` prefixes each operand with a one-byte domain
tag — `'S'` for a symbol's bytes, `'I'` for an `int64`'s eight
little-endian bytes — and then its length as a little-endian `uint64`.
Distinct `(namespace, name)` pairs therefore always hash distinct bytes,
whether they differ in where they split or in what they are:

```
uuid5("ab", "c")       !=  uuid5("a", "bc")
uuid5("abcdefgh", "x") !=  uuid5(7523094288207667809, "x")
```

Neither holds for a bare concatenation: `"ab" + "c"` and `"a" + "bc"`
are the same three bytes, and `7523094288207667809` is exactly the
`int64` whose little-endian bytes are `abcdefgh`. The length settles the
first, the tag the second. RFC 4122 sidesteps both by fixing the
namespace at 16 bytes of one type; wirelog adopted the two-operand
signature without that property, so it makes the framing explicit
instead. In Python:

```python
frame = lambda tag, b: tag + pack('<Q', len(b)) + b

buf = frame(b'S', ns) + frame(b'S', name)      # uuid5(symbol, symbol)
# frame(b'I', pack('<q', v)) for an int64 operand

d = bytearray(sha1(buf).digest())
d[6] = (d[6] & 0x0F) | 0x50
d[8] = (d[8] & 0x3F) | 0x80
value = unpack('<q', bytes(d[:8]))[0]
```

`uuid5(int64, int64)` keeps the older unframed `SHA-1(ns || name)` — its
operands are 8 bytes each and both `int64`, so neither ambiguity can
arise and its values are unchanged.

**Unambiguous input is not a collision-free output.** The framing makes
the bytes fed to SHA-1 one-to-one with the typed operand pair. It says
nothing about the return value, which is the first 8 of the 16 digest
bytes with 4 bits overwritten by the version nibble: at most 2^60
distinct results exist, so `uuid5()` collides at scale like any 60-bit
fingerprint. See `docs/SECURITY_MODEL.md`.

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

The ordering **aggregates** `min()` and `max()` follow the same rule from
the same declared types, so `min(Name)` over a `symbol` column is the
alphabetically smallest name and not whichever was interned first.

Two of the paragraphs below read differently for aggregates. The
mixed-operand rule does not apply at all -- an aggregate compares values
drawn from one column, not two operands of different types. Where such a
column holds both interned and un-interned values, the interned ones win,
for `min` and `max` alike; that is the opposite of the ordering
comparisons, which fall back to comparing ids. And the undeclared-column
report is emitted once per aggregate at plan generation rather than per
comparison, so `WL_LOG=EVAL:2` names the aggregate, not each row.

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
