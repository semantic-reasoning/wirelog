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

### Checksum Functions

`crc32_ethernet(x)`, `crc32_castagnoli(x)`

Both compute a CRC-32 over the 8-byte `int64` representation of their
argument and return the result as a non-negative `int64` in the range
`[0, 2^32)`. `crc32_ethernet` uses the Ethernet/ISO-HDLC polynomial
(0x04C11DB7, ISO 3309 / IEEE 802.3); `crc32_castagnoli` uses the
Castagnoli polynomial (CRC-32C, iSCSI/SCTP). Unlike the crypto hash
built-ins above, the CRC-32 functions are always available and do not
depend on the `mbedTLS` build option. See `examples/05-crc32-checksum/`
for a frame-integrity validation example.

### UUID Functions

`uuid4()`, `uuid5(namespace, name)`

The UUID built-ins require mbedTLS for non-zero runtime output and
return the first 8 UUID bytes as an `int64`, not formatted text.
Disabled UUID calls follow the same fail-closed behavior described for
crypto hash functions.

### Comparison Operators

`=`, `!=`, `<`, `>`, `<=`, `>=`
