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
- **Aggregation**: `min(x)`, `max(x)`, `sum(x)`, `count(x)`, and
  `average(x)` in head (at most one per head — see below). `average` requires
  a declared `float` operand and returns a binary64 value.
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
- The check does **not**, by itself, cover every arity mismatch. A
  single-aggregate head with too few arguments for its `.decl`, such as
  `cc(y, min(c)) :- cc(x, c, d), edge(x, y).` against a 3-column `cc`, is
  unsafe in a recursive stratum for the same reason but has only one
  aggregate. That shape is rejected by the separate rule-head arity check
  described in [Rule heads must match their declared
  arity](#rule-heads-must-match-their-declared-arity) below (#977). The two
  are complementary: an arity check accepts `t(g, min(v), max(v))`, and the
  aggregate-count check accepts `cc(y, min(c))`.

### `average()`

`average(x)` and `AVG(x)` require `x` to be a declared `float` column and
return the arithmetic mean as a 64-bit IEEE-754 value. Integer and symbol
operands are rejected during lowering; use `sum` and `count` when an integer
result is intended. Float inputs must be finite, and `-0.0` is canonicalized
to `0.0`.

For example:

```
val(1, 9.0). val(1, 5.0). val(1, 2.0).
.decl t(g: int64, a: float)
t(g, average(v)) :- val(g, v).
```

`average` stays a reserved keyword and `WIRELOG_AGG_AVG` stays in the public
enum.

**One caveat on the workaround.** It is exact for a non-recursive rule. Do not
reach for it inside a *recursive* stratum: recursive `sum` and `count` are
themselves broken today — canonicalisation is skipped for anything that is not
`min`/`max`, so they emit multiple rows per group and violate the functional
dependency the head declares, silently and at exit 0. That is issue #991. A
recursive `average` is rejected here; substituting recursive `sum`/`count` for
it trades a loud failure for a quiet wrong answer.

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

Facts are compared against the declared **physical** width (#985), exactly as
rule heads are: an `inline` compound column counts as its full arity, a `side`
compound as the one handle column it is stored in. A fact is a row of the
relation's storage, and that is the stride the storage uses.

```
.decl p(id: int64, lbl: pair/2 inline)
p(1, 2, 3).         /* accepted: 3 physical columns, written flat */
p(1, 2).            /* rejected: 2, and leaves the second slot unwritten */

.decl q(a: int64, m: m/4 side)
q(1, 2).            /* accepted: a side compound is one handle column */
```

The flat spelling is the only spelling. `p(1, pair(2,3)).` and `p(1, [2,3]).`
are parse errors — facts admit numeric and string constants only — and the
head grammar has no compound-term production either, so `p(x, pair(y,z))` in a
rule head is a parse error as well. Flat is what a rule writes
(`p(x, y, z) :- src(x, y, z).`) and flat is what an `.input` file carries: one
field per physical column, four fields for
`.decl inp(id: int64, p: pair/2 inline, s: symbol)`.

Until #985 this comparison was logical, which rejected `p(1, 2, 3).` and
accepted `p(1, 2).` — that is, it rejected the working spelling and accepted
the broken one, leaving such a relation with no usable fact syntax at all. The
accepted form left the second inline slot never written, and a body pattern
that destructures the column read it regardless:
`outr(id, x, y) :- p(id, pair(x, y)).` produced a `0` present in no source
data. Rejecting is forced rather than chosen — `0` is a valid `int64` and a
valid intern id, so no filler value could mean "not written".

An inline compound fact can contain symbols. `p(1, "aa", "bb").` against
`.decl p(id: int64, lbl: pair/2 inline)` works, and
`o(a, b, c) :- p(a, pair(b, c)).` gives `o(1, "aa", "bb")`. For `.input`, the
legacy slash form keeps every slot at `int64`, so the same CSV values fail the
load. Use the typed form, for example
`.decl p(id: int64, lbl: pair(symbol, symbol) inline)`, to declare the slot
types and let the loader intern the text fields. The slots remain ordinary
`int64` cells; the declaration controls decoding and comparison semantics.

The physical width is also the row stride reported by
`wirelog_program_get_facts`. It agrees with the `column_count` of
`wirelog_program_get_schema` for every relation that declares no `inline`
compound column, and can exceed it where one is declared. See the note on
that function in `wirelog/wirelog.h`.

Facts on a relation with **no** `.decl` at all are unaffected by this check
and continue to fail later, during loading, with a less specific message.

### Rule heads must match their declared arity

A rule head that emits a different number of columns than its relation's
`.decl` declares is rejected when the program is loaded (#977):

```
.decl val(g: int64, v: int64)
.decl t(a: int64, b: int64)
t(g)          :- val(g, v).    /* rejected: emits 1, declared 2 */
t(g,v,g,v)    :- val(g, v).    /* rejected: emits 4, declared 2 */
t(g, v)       :- val(g, v).    /* accepted */
t(g, min(v))  :- val(g, v).    /* accepted: an aggregate is one column */
```

As with facts, the `.decl` may appear before or after its rules, and heads on
a relation with **no** `.decl` are not checked at all — undeclared derived
heads are ordinary Datalog and are used throughout the benchmark workloads.

This is a memory-safety guard. A head narrower than its `.decl` is an
out-of-bounds read whenever some *other* producer has already established the
relation at the declared width: the narrow head's operator sizes its output
region from the emitted arity, while `col_rel_append_all` copies the declared
arity out of it with no clamp. `cc(y, min(c)) :- cc(x, c, d), edge(x, y).`
against a 3-column `cc` seeded by a fact segfaulted.

Recursion is not the precondition — a declared-width producer is. The
non-recursive `t(g) :- val(g, v).` against a 3-column `t` is equally an
over-read once a `t(9,9,9).` fact fixes the width, this time through
`col_op_map` rather than `col_op_reduce`. With no such producer the relation
simply materialises at the narrower width and the mismatch is a silent wrong
answer instead.

Loading fails with `WIRELOG_ERR_PARSE`; run with `WL_LOG=PARSER:1` to see
which relation and which arities.

Heads are compared against the declared **physical** width — an `inline`
compound column counts as its full arity, a `side` compound as the single
handle column it is stored in. The reason is that the head grammar has no
compound-term production (`pred(x, f(y, z))` is a parse error), so the
flattened spelling is the only way to write an inline-compound relation from
a rule:

```
.decl src(a: int64, b: int64, c: int64)
.decl pred(id: int64, payload: f/2 inline)
pred(x, y, z) :- src(x, y, z).   /* accepted: 3 physical columns */
pred(x, y)    :- src(x, y).      /* rejected: 2, and leaves a slot unwritten */
```

The two-argument form is rejected simply because the comparison is physical
and 2 is not 3. Since the flattened spelling is the only one a rule can use,
a head that writes fewer columns than the relation physically has is an
under-write, not an alternative notation.

That it is not a *false* rejection is worth demonstrating separately: the
two-argument head leaves the second inline slot unwritten, and a body pattern
that destructures the column reads it anyway — `outr(id, p, q) :- pred(id,
f(p, q)).` produced `outr(1, 99, 0)`, a value present in no source data,
where the three-argument spelling correctly produces `outr(1, 10, 20)`.

The corresponding *fact* `pred(1, 99).` is rejected for the same reason, and
by the same physical comparison (see [Inline facts must match their declared
arity](#inline-facts-must-match-their-declared-arity) above). It was accepted
until #985, which is where the `outr(1, 99, 0)` above was observed.

Rule **bodies** are still not validated against their `.decl`.

The count is not the only constraint on aggregates in a head — the aggregate
must also be written **last**. `t(min(v), g) :- val(g, v).` lowers with a
single aggregate and the correct arity, but emits its columns group-by-first
regardless of the order written, so the values land in the wrong columns with
no diagnostic. That is tracked as #980 and is not affected by this check.

Aggregate names are matched as exact keywords. `average` and `AVG` are
recognized; `avg` and `AVERAGE` are not. An unrecognized name followed by `(`
has no production in head-argument position, so `t(g, avg(v), max(v))` is a
plain syntax error rather than a rule that bypasses this check. (`average`
and `AVG` are recognized by the lexer and require a float operand at lowering.
The keywords are
kept reserved so that `avg`/`AVERAGE` do not have to be taken away from user
identifiers later.)

---

## Inline Facts

Define base data directly in the program:

```
Edge(1, 2).
Edge(2, 3).
Name(1, "alice").
```

Inline facts contain only numeric or string constants, not variables.

---

## Directives

### .input

Load relation data from external files:

```
.input Edge(IO="file", filename="edge.csv", delimiter=",")
```

Hosts that need to inspect source-admission policy can call
`wirelog_program_relation_has_input()` after parsing.  The query reads parser
metadata only: it does not open the declared path, resolve the adapter, or
perform custom I/O.  Relation names are matched exactly and
case-sensitively; duplicate `.input` directives and `.input` directives for
undeclared relations still report `true`, while unknown relations and NULL
arguments report `false`.

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

> **Status (Issue #989): bound `.query` is accepted conservatively.**
> The directive currently carries only an adornment (`b`/`f`), not the bound
> values needed to seed a magic demand relation. The public optimizer therefore
> leaves a bound query unoptimized instead of installing an empty guard. This
> preserves the complete result until a query API with seed values is added;
> all-free adornments remain a no-op as before.
>
> **Not every relation is guarded.** Since Issue #1027 the pass excludes a
> relation from the transformation altogether when some occurrence of it needs
> to be read unrestricted -- when a rule body reads it without binding any of
> its columns -- along with everything such a relation transitively reads. An
> excluded relation is evaluated in full and yields its complete result, empty
> demand relation or not. The same goes for a rule the pass declines to guard
> for other reasons, such as a constant in the bound head position.
>
> The guard-viability closure also protects consumers that require complete
> inputs. It keeps IDBs read by unrestricted `.output`/`.printsize` consumers,
> under negation (#1047), or by aggregate rules (#1048) unrestricted, together
> with their positive IDB dependencies. Those fixes prevent a partial guarded
> relation from making negation derive false positives, an aggregate compute
> the wrong value, or an output consumer report incomplete results. The
> closure is conservative: affected relations may be evaluated in full, so
> this can give back some pruning, but it does not change the answer.
>
> The remaining bound-query limitation is seeding. The syntax supplies only
> the `b`/`f` pattern, not the bound values, so the public optimizer leaves a
> bound query unoptimized and preserves the complete result. An explicit
> caller that supplies demand seeds can use the lower-level API to obtain the
> intended restriction. Issue #995 remains: once multiple adornments of one
> relation are seeded, they conjoin into one rule body instead of forming
> separate adorned predicates, which can lose answers.
>
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
the magic demand relation `$m$Path_bf` and guards both `Path` rules with it,
which is *intended* to restrict evaluation to only reachable tuples and avoid
full materialization of the transitive closure.

As written, this program prints the full closure. There is no way to say
*which* source node is bound, so the public optimizer conservatively leaves the
query unoptimized. A future query API can seed `$m$Path_bf` and enable the
intended restriction without changing this source syntax.

`Path` is guarded here because the recursive atom `Path(z, y)` binds `z`
through `Edge(x, z)`. Written left-recursively instead --
`Path(x, y) :- Path(x, z), Edge(z, y).` under `.query Path(f, b)` -- the
recursive atom binds neither column, so the pass excludes `Path` from the
transformation and that program prints the full closure rather than nothing.
See the status note above and Issue #1027.

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
- The lower-level Magic Sets API can prune the search space when callers
  explicitly seed demand relations for bound positions; the public parsed
  `.query` path currently supplies no seed values, remains unoptimized, and
  preserves the complete result.
- A parsed `.query` with at least one bound position currently remains
  unoptimized because the syntax supplies no seed values (Issue #989). It
  preserves the complete result; explicit callers that provide demand seeds
  can use the lower-level API for the intended restriction.
- An all-free `.query` (e.g., `.query Path(f, f)`) is equivalent to no `.query`
  and results in no optimization (the pass is a no-op). All-free is the only
  adornment that is safe today.
- A rule whose bound head position holds a *constant* rather than a variable
  (e.g. `q(1, y) :- ...` under `.query q(b, f)`) is left unguarded: there is no
  variable to key the guard on. Such rules are reported by the pass's
  `skipped_constant_head` counter.
- A rule whose head is wider than 64 columns is left unguarded: the adornment
  is carried in a 64-bit mask, so there is no bit for the remaining positions.
  Such rules are reported by the `skipped_unsupported_head` counter.
- A rule that Logic Fusion has collapsed into a fused root — which includes any
  rule with a filter — **is** guarded, as of Issue #990. It reads its head
  variable names off the head projection the root still carries, not off the
  root's node type, so the rewrite fusion performs does not hide the head. Such
  rules were previously reported by `skipped_unsupported_head`; they are now
  counted in `original_rules_modified` like any other guarded rule.
- Both skip counters describe a rule that evaluates unrestricted: sound, but
  not the pruning `.query` asks for. Absence from both means the rule was
  guarded, which is a weaker statement than "correctly restricted" — see the
  status note above and Issue #1027.
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
- `float` -- finite IEEE-754 binary64 value
- `functor/arity` -- compound term handle stored in a 64-bit column
- `functor/arity side` -- explicit side-relation compound storage
- `functor/arity inline` -- inline compound storage, limited to arity 4

Integer literals are signed 64-bit values in the range
`-9223372036854775808` through `9223372036854775807`. A negative literal is
written with `-` immediately before its digits (for example `-5`); whitespace
between the sign and digits is not accepted. Values outside this range are
parse errors. The minus sign remains a binary subtraction operator when used
between expressions.

Float literals use either `digits.digits` with an optional exponent or
`digits` with an exponent (for example `1.5`, `1e3`, and `1.5e-2`). The
initial grammar rejects `.5`, `1.`, malformed exponents, and non-finite
results. A unary minus may be separated from a float literal by whitespace;
`-0.0` is represented canonically as `0.0`. Float values are supported by the
columnar executor, joins, ordering, consolidation, CSV input/output, and
aggregates.

---

## Expressions

### Arithmetic Operators

`+`, `-`, `*`, `/`, `%` (modulo)

`*`, `/`, and `%` have higher precedence than `+` and `-`. Operators within
each level associate left-to-right. For example, `8 + 3 * 2` yields `14`,
`20 - 5 - 3` yields `12`, and `24 / 3 * 2` yields `16`.
These rules apply to rule-head expressions, both sides of comparisons,
and arithmetic expressions passed to aggregate and built-in functions.

The arithmetic grammar is:

```text
arithmetic_expr = product (("+" | "-") product)*
product         = factor (("*" | "/" | "%") factor)*
```

Factors include variables, supported literals, and supported built-in calls.
General arithmetic grouping parentheses and unary negation of variables or
expressions are not supported. A minus sign on a numeric literal is supported
subject to the literal syntax rules above. Call parentheses, such as those in
`min(A + B * C)` and `band(A + B * C, 15)`, remain supported; they are not
general grouping parentheses. Use an intermediate relation when a different
arithmetic grouping is needed.

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

`uuid4()`, `uuid5(namespace, name)`, `uuid5_rfc(namespace_uuid, name)`

The legacy `uuid4()` and `uuid5()` built-ins require mbedTLS for non-zero
runtime output and return the first 8 UUID bytes as an `int64`, not formatted
text. `uuid5_rfc()` returns a formatted symbol as described below.
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

`uuid5_rfc(namespace_uuid, name)` is the standards-compliant string-returning
variant. `namespace_uuid` must be a canonical 36-character UUID string; it is
decoded to its 16 raw bytes before SHA-1 hashing. The name is arbitrary symbol
text. The result is the full canonical lowercase UUID string with version 5 and
the RFC variant bits set. A malformed namespace, or a call without mbedTLS,
rejects the row.

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

### Scalar Addon Calls

Scalar addons use the explicit `@call` marker so their syntax cannot be
confused with an existing relation named `call`:

```
result(y) :- input(x), @call("vendor.normalize", x, y).
```

The addon name must be a quoted `namespace.name` made from ASCII letters,
digits, underscores, and dots, with at least one dot. The name is trimmed
before validation. The ordinary relation form remains unchanged, including
string arguments:

```
call("literal", x).
```

Addon calls are resolved against an explicit registry snapshot at compile or
session creation. A serialized plan stores the stable name and ABI identity,
never a function pointer or descriptor address. Runtime FILTER/MAP support and
the callback lifetime contract are tracked separately in issue #1250.
