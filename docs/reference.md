---
title: Language Reference
nav_order: 3
---

# Language Reference

Complete reference for the wirelog Datalog language.

## Program Structure

A program consists of declarations, directives, facts, and rules in any order.

```
program = (declaration | directive | rule | fact)*
```

## Comments

```
# Hash comment (to end of line)
// Slash comment (to end of line)
```

## Identifiers

Identifiers start with a letter or underscore, followed by letters, digits, or underscores. Case-sensitive.

```
[a-zA-Z_][a-zA-Z0-9_]*
```

Examples: `edge`, `tc`, `pointsTo`, `_temp`, `Arc`

## Literals

| Type | Syntax | Examples |
|------|--------|---------|
| Integer | `[0-9]+` | `0`, `42`, `1000000` |
| String | `"..."` | `"file.csv"`, `"Alice"` |
| Boolean | `True`, `False` | |
| Wildcard | `_` | matches any value |

All values are stored internally as 64-bit integers. Strings are interned (deduplicated via a symbol table).

## Types

Used in `.decl` declarations:

| Type | Description |
|------|-------------|
| `int32` | 32-bit signed integer |
| `int64` | 64-bit signed integer |
| `string` | UTF-8 text (interned) |

## Facts

Ground atoms with constant arguments, terminated by a period.

```
relation(const1, const2, ...).
```

Examples:

```
edge(1, 2).
name(1, "Alice").
source(1).
```

## Rules

A head atom derived from body predicates, separated by `:-`.

```
head(args) :- pred1(args), pred2(args), ... .
```

Body predicates are comma-separated (logical AND). Shared variables across predicates form implicit joins.

```
tc(x, z) :- tc(x, y), edge(y, z).
```

## Operators

### Arithmetic

| Operator | Meaning | Precedence |
|----------|---------|------------|
| `*` | Multiplication | 2 (highest) |
| `/` | Division | 2 |
| `%` | Modulo | 2 |
| `+` | Addition | 1 |
| `-` | Subtraction | 1 |

All operators are left-associative. Parentheses are not supported in arithmetic expressions -- use intermediate rules instead.

### Comparison

| Operator | Meaning |
|----------|---------|
| `=` | Equal |
| `!=` | Not equal |
| `<` | Less than |
| `>` | Greater than |
| `<=` | Less than or equal |
| `>=` | Greater than or equal |

Used as body predicates:

```
big(x, y) :- edge(x, y), x > 10.
diff(x, y) :- data(x, y), x != y.
```

## Negation

Prefix `!` on a body predicate.

```
sink(x) :- node(x), !edge(x, _).
```

**Constraints**:
- Variables in negated atoms must be bound by positive atoms in the same rule
- Negation must be stratifiable (no recursive dependency through negation)

## Aggregation

Aggregate functions in head arguments. Grouping is implicit over non-aggregate head variables.

| Function | Meaning |
|----------|---------|
| `count(expr)` | Count occurrences |
| `sum(expr)` | Sum of values |
| `min(expr)` | Minimum value |
| `max(expr)` | Maximum value |

Functions are case-insensitive (`count` = `COUNT`).

```
cnt(x, count(y)) :- data(x, y).
shortest(x, min(d + w)) :- dist(x, d), edge(x, y, w).
```

## Directives

### `.decl` -- Declare Relation

```
.decl name(attr1: type1, attr2: type2, ...)
```

Example:

```
.decl edge(x: int32, y: int32)
.decl label(id: int32, name: string)
```

### `.input` -- Load CSV Data

Must follow a `.decl` for the same relation.

```
.input name(filename="path.csv", delimiter=",")
```

Parameters:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `filename` | CSV file path (relative to cwd) | required |
| `delimiter` | Field separator | `,` |
| `IO` | I/O mode | `"file"` |

CSV format: one tuple per line, fields separated by delimiter, empty lines skipped.

### `.output` -- Filter Output

```
.output name
```

If any `.output` directive exists, only marked relations are printed. Without `.output`, all derived relations are printed.

### `.printsize` -- Print Cardinality

```
.printsize name
```

### `.plan` -- Optimizer Hint

Appended after a rule to signal the optimizer:

```
path(x, z) :- edge(x, y), path(y, z). .plan
```

## Execution Model

1. **Parse** -- Datalog source to AST
2. **Optimize** -- Fusion, JPP (join reordering), SIP (semijoin pre-filtering)
3. **Stratify** -- Tarjan's SCC algorithm, topological ordering
4. **Translate** -- IR to Differential Dataflow operator graph
5. **Execute** -- Fixed-point iteration per stratum
6. **Output** -- Print result tuples

### Stratification

Rules are partitioned into **strata** based on dependencies:

- Stratum 0 executes first
- Negation and aggregation require the depended-on relation to be in a lower stratum
- Mutually recursive relations are in the same stratum and computed via fixed-point iteration

### Fixed-Point Semantics

Recursive strata are evaluated incrementally: new facts derived in each iteration are added until no new facts appear. The result is the **least fixed point** of the program.

## Lexical Tokens

| Token | Pattern |
|-------|---------|
| `IDENT` | `[a-zA-Z_][a-zA-Z0-9_]*` |
| `INTEGER` | `[0-9]+` |
| `STRING` | `"..."` |
| `HORN` | `:-` |
| `BANG` | `!` |
| `DOT` | `.` |
| `COMMA` | `,` |
| `LPAREN` / `RPAREN` | `(` / `)` |
| `COLON` | `:` |
| `EQ` / `NEQ` | `=` / `!=` |
| `LT` / `GT` | `<` / `>` |
| `LTE` / `GTE` | `<=` / `>=` |
| `PLUS` / `MINUS` | `+` / `-` |
| `STAR` / `SLASH` / `PERCENT` | `*` / `/` / `%` |
