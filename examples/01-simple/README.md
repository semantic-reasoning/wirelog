# Example 01-Simple: Ancestry (Facts + Rules Tutorial)

## Objective

This is the first example demonstrating core Datalog concepts:
- **Facts**: Input data from CSV files
- **Rules**: Datalog logic with base and recursive cases
- **Output**: Derived facts computed from rules

## Example: Computing Ancestors

### Input Data

`ancestors_facts.csv` contains parent relationships (facts):
```
alice,bob        -> alice is parent of bob
bob,charlie      -> bob is parent of charlie
charlie,diana    -> charlie is parent of diana
bob,eve          -> bob is parent of eve
eve,frank        -> eve is parent of frank
```

### Datalog Program

`ancestors.dl` defines two rules:

**Rule 1 (Base Case):**
```prolog
ancestor(X, Y) :- parent(X, Y).
```
"Your parent is your ancestor"

**Rule 2 (Recursive Case):**
```prolog
ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).
```
"Ancestor of your ancestor is your ancestor"
(Transitive closure)

### Expected Output

Running the program computes all transitive ancestor pairs:
```
alice,bob         (direct)
alice,charlie     (alice->bob->charlie)
alice,diana       (alice->bob->charlie->diana)
alice,eve         (direct via bob)
alice,frank       (alice->bob->eve->frank)
bob,charlie       (direct)
bob,diana         (bob->charlie->diana)
bob,eve           (direct)
bob,frank         (bob->eve->frank)
charlie,diana     (direct)
eve,frank         (direct)
```

## How to Run

From the project root:

```bash
cd examples/01-simple
../../build/wirelog_cli ancestors.dl
```

Or from project root:
```bash
./build/wirelog_cli examples/01-simple/ancestors.dl
```

The program will:
1. Parse `ancestors.dl`
2. Load facts from `ancestors_facts.csv`
3. Evaluate the Datalog rules
4. Write output to `ancestors_output.csv`

## Verification

Compare generated output with expected output:

```bash
cd examples/01-simple
diff ancestors_output.csv ancestors_expected.csv
```

If no differences are printed, the output is correct.

## Key Concepts

### Facts vs Rules

- **Facts**: Base data (parent relationships in CSV)
- **Rules**: Logic to derive new facts (ancestor computation)

### Base Case vs Recursive Case

- **Base Case**: Direct facts (immediate parents)
- **Recursive Case**: Derived facts (transitive relations)

This pattern is fundamental to Datalog and appears in:
- Reachability analysis (graph traversal)
- Points-to analysis (data flow)
- Transitive closure (relations)

## Next Steps

After mastering this example, explore:
- `02-path-analysis`: Multi-way joins with multiple input files
- `03-points-to`: More complex rules resembling DOOP analysis
