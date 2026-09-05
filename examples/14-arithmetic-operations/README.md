# Example 14: Arithmetic Operations

## Overview

This example evaluates arithmetic expressions in a rule head. Each input row
contains three integers, and the derived `result` relation shows addition,
subtraction, multiplication, integer division, remainder, and the evaluation
order of chained operators. It also uses separate rules to demonstrate the
`min`, `max`, `average`, and `count` aggregate functions.

The Datalog program is:

```datalog
.decl sample(label: symbol, a: int64, b: int64, c: int64)
.decl result(label: symbol, added: int64, difference: int64,
            product: int64, quotient: int64, remainder: int64,
            precedence: int64)

result(Label, A + B, A - B, A * B, A / B, A % B, A + B * C)
    :- sample(Label, A, B, C), B != 0.
```

Multiplication, division, and remainder bind more tightly than addition and
subtraction. `A + B * C` therefore adds `A` to the product of `B` and `C`.
The `precedence` input row uses `8 + 3 * 2` and produces `14`, not `22`.
Operators at the same precedence level associate left-to-right: `A - B - C`
subtracts `B` and then `C`. General arithmetic grouping parentheses are not
currently supported; use an intermediate relation to request a different
grouping, such as adding `A` and `B` before multiplying by `C`.

Each aggregate is in its own rule because a rule head accepts at most one
aggregate. `min` and `max` operate on the integer column, `average` requires a
declared `float` operand, and `count` returns the number of input rows. The
aggregate program used by the demo is:

```datalog
.decl sample(a: int64, value: float)
.decl zero_input(value: float)
.decl zero_observed(value: float)
.decl minimum(value: int64)
.decl maximum(value: int64)
.decl average_value(value: float)
.decl sample_count(value: int64)

minimum(min(A)) :- sample(A, _).
maximum(max(A)) :- sample(A, _).
average_value(average(Value)) :- sample(_, Value).
sample_count(count(A)) :- sample(A, _).
zero_observed(Value) :- zero_input(Value).
```

## Build & Run

```sh
meson compile -C build arithmetic_demo
./build/examples/14-arithmetic-operations/arithmetic_demo
meson test -C build example_14_arithmetic_golden
```

## Expected Output

```text
Example 14: Arithmetic Operations
=================================

average_value(2.500000)
maximum(17)
minimum(-17)
result("negative", -12, -22, -85, -3, -2, -7)
result("positive", 22, 12, 85, 3, 2, 27)
result("precedence", 11, 5, 24, 2, 2, 14)
sample_count(3)
zero_observed(+0.0)

Done.
```

## Integer and Error Semantics

- The arithmetic expression columns are all `int64`; the separate `average`
  aggregate intentionally produces a `float` result.
- Division truncates toward zero. For example, `-17 / 5` is `-3`.
- The remainder keeps the dividend's sign: `-17 % 5` is `-2`.
- The typed float ingress canonicalizes both `-0.0` and `+0.0` to the same
  `+0.0` value. The demo inserts both spellings and prints the one observed
  relation row as `zero_observed(+0.0)`.
- The rule filters out rows with `B == 0`, so it never attempts division or
  remainder by zero. Do not treat that guard as a default value for invalid
  arithmetic; invalid operations should be rejected or handled explicitly by
  the surrounding rule logic.

See [Arithmetic Operators](../../docs/SYNTAX.md#expressions) in the syntax
reference for the complete expression grammar and supported operators.
The aggregate restrictions and `average` type requirement are described in
the same syntax reference.
