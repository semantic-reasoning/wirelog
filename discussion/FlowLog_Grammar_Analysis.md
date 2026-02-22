# FlowLog Grammar Analysis

Based on examination of https://github.com/flowlog-rs/flowlog repository.

## Source

FlowLog uses **Pest** (PEG parser) defined in `crates/parser/src/grammar.pest`.

## Complete Grammar Rules

### Top-Level

```
main_grammar = SOI ~ (declaration | input_directive | output_directive | printsize_directive | rule)* ~ EOI
```

### Declarations

```
declaration       = ".decl" ~ relation_name ~ "(" ~ attributes_decl? ~ ")"
input_directive   = ".input" ~ relation_name ~ "(" ~ input_params? ~ ")"
output_directive  = ".output" ~ relation_name
printsize_directive = ".printsize" ~ relation_name
```

### Input Parameters (key=value pairs)

```
input_params    = input_param ~ ("," ~ input_param)*
input_param     = parameter_name ~ "=" ~ parameter_value
parameter_name  = identifier
parameter_value = string
```

### Identifiers

```
identifier      = "_"? ~ ASCII_ALPHA+ ~ (ASCII_ALPHANUMERIC | "_")*
variable        = identifier
attribute_name  = identifier
relation_name   = identifier
```

### Data Types

```
data_type      = integer64_type | integer32_type | string_type
integer32_type = "int32"
integer64_type = "int64"
string_type    = "string"
```

### Constants

```
constant  = integer | string
integer   = ("+" | "-")? ~ ASCII_DIGIT+
string    = "\"" ~ (!"\"" ~ ANY)* ~ "\""
placeholder = "_"
boolean   = "True" | "False"
```

### Attribute Declarations

```
attributes_decl = attribute_decl ~ ("," ~ attribute_decl)*
attribute_decl  = attribute_name ~ ":" ~ data_type
```

### Rules

```
rule      = head ~ ":-" ~ predicates ~ "." ~ optimize?
head      = relation_name ~ "(" ~ (head_arg ~ ("," ~ head_arg)*)? ~ ")"
head_arg  = aggregate_expr | arithmetic_expr
predicates = predicate ~ ("," ~ predicate)*
predicate  = atom | negative_atom | compare_expr | boolean
optimize   = ".plan"
```

### Atoms

```
atom          = relation_name ~ "(" ~ (atom_arg ~ ("," ~ atom_arg)*)? ~ ")"
negative_atom = "!" ~ atom
atom_arg      = variable | constant | placeholder
```

### Expressions

```
arithmetic_expr = factor ~ (arithmetic_op ~ factor)*
factor          = variable | constant
arithmetic_op   = plus | minus | times | divide | modulo
plus   = "+"
minus  = "-"
times  = "*"
divide = "/"
modulo = "%"

compare_expr = arithmetic_expr ~ compare_op ~ arithmetic_expr
compare_op   = equal | not_equal | greater_equal_than | greater_than | less_equal_than | less_than
equal              = "="
not_equal          = "!="
greater_equal_than = ">="
greater_than       = ">"
less_equal_than    = "<="
less_than          = "<"

aggregate_expr = aggregate_op ~ "(" ~ arithmetic_expr ~ ")"
aggregate_op   = count | average | sum | min | max
count   = "count" | "COUNT"
average = "average" | "AVG"
sum     = "sum" | "SUM"
min     = "min" | "MIN"
max     = "max" | "MAX"
```

### Whitespace and Comments

```
WHITESPACE = " " | "\t" | NEWLINE
COMMENT    = ("#" | "//") ~ (!NEWLINE ~ ANY)*
```

## AST Types (from Rust source)

### Program
```rust
struct Program {
    relations: Vec<Relation>,
    rules: Vec<FlowLogRule>,
    bool_facts: HashMap<String, Vec<(Vec<ConstType>, bool)>>,
}
```

### Relation (Declaration)
```rust
struct Relation {
    name: String,
    attributes: Vec<Attribute>,
    input_params: Option<HashMap<String, String>>,
    output: bool,
    printsize: bool,
}
```

### Rule
```rust
struct FlowLogRule {
    head: Head,
    rhs: Vec<Predicate>,
    is_planning: bool,  // .plan marker
}
```

### Head
```rust
enum HeadArg { Var(String), Arith(Arithmetic), Aggregation(Aggregation) }
struct Head { name: String, head_arguments: Vec<HeadArg> }
```

### Predicate
```rust
enum Predicate {
    PositiveAtomPredicate(Atom),
    NegativeAtomPredicate(Atom),
    ComparePredicate(ComparisonExpr),
    BoolPredicate(bool),
}
```

### Atom
```rust
enum AtomArg { Var(String), Const(ConstType), Placeholder }
struct Atom { name: String, arguments: Vec<AtomArg> }
```

### Arithmetic
```rust
enum ArithmeticOperator { Plus, Minus, Multiply, Divide, Modulo }
enum Factor { Var(String), Const(ConstType) }
struct Arithmetic { init: Factor, rest: Vec<(ArithmeticOperator, Factor)> }
```

### Comparison
```rust
enum ComparisonOperator { Equal, NotEqual, GreaterThan, GreaterEqualThan, LessThan, LessEqualThan }
struct ComparisonExpr { left: Arithmetic, operator: ComparisonOperator, right: Arithmetic }
```

### Aggregation
```rust
enum AggregationOperator { Min, Max, Count, Sum }
struct Aggregation { operator: AggregationOperator, arithmetic: Arithmetic }
```

### Primitives
```rust
enum DataType { Int32, Int64, String }
enum ConstType { Int32(i32), Int64(i64), Text(String) }
```

## Key Differences from Soufflé

| Feature | FlowLog | Soufflé |
|---------|---------|---------|
| Types | int32, int64, string | number, symbol, unsigned, float |
| Comments | `#`, `//` | `%`, `//` |
| Float literals | No | Yes |
| Bitwise ops | No | Yes (band, bor, bxor) |
| `.type` directive | No | Yes |
| `.printsize` | Yes | Yes |
| `.plan` optimize | Yes | Yes |
| Boolean predicates | Yes (True/False) | No |
| Input params | key=value in parens | key=value in parens |
| Negation | `!atom` | `!atom` |
| Aggregation | count/COUNT, average/AVG, sum/SUM, min/MIN, max/MAX | Similar |

## Example Programs

### Transitive Closure
```
.decl Arc(x: int32, y: int32)
.input Arc(IO="file", filename="Arc.csv", delimiter=",")
.decl Tc(x: int32, y: int32)
Tc(x, y) :- Arc(x, y).
Tc(x, y) :- Tc(x, z), Arc(z, y).
.printsize Tc
```

### Shortest Path (with aggregation)
```
.decl arc(src: int32, dest: int32, weight: int32)
.decl sssp2(x: int32, y: int32)
.decl sssp(x: int32, y: int32)
.input arc(IO="file", filename="Arc.csv", delimiter=",")
sssp2(x, min(0)) :- id(x).
sssp2(y, min(d1 + d2)) :- sssp2(x, d1), arc(x, y, d2).
sssp(x, min(d)) :- sssp2(x, d).
```
