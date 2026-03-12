# Directive Integration Example

This example demonstrates all three directives working together:
- `.decl` - Declares relation schema with symbol types  
- `.output` - Writes results to CSV file with filename parameter
- Inline facts - Base facts for the computation

## Usage

```bash
wirelog_cli complete_directives.dl
cat ancestors.csv
```

## Expected Output

The `ancestors.csv` file will contain transitive ancestor pairs in CSV format.

## About the directives:

**`.decl parent(x: symbol, y: symbol)`**
- Declares the `parent` relation with two symbol-typed columns
- Enables proper string deinterning in output

**`.output ancestor(filename="ancestors.csv")`**
- Marks the `ancestor` relation for output
- Specifies filename where results should be written as CSV

**Inline facts**
- `parent("alice", "bob").` provide base input data
- Combined with rule evaluation to compute ancestors

## Rule explanation

```datalog
ancestor(X, Y) :- parent(X, Y).          // Base case
ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).  // Recursive
```
