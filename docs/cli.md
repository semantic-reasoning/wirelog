---
title: CLI Usage
nav_order: 5
---

# CLI Usage

## Synopsis

```
wirelog-cli [options] <file.dl>
```

## Options

| Option | Argument | Default | Description |
|--------|----------|---------|-------------|
| `--workers` | N | 1 | Number of worker threads (1-256) |
| `--help`, `-h` | | | Print usage and exit |

## Basic Usage

```bash
# Run a Datalog program
wirelog-cli program.dl

# Run with 4 parallel workers
wirelog-cli --workers 4 program.dl
```

## Input File

The input must be a single `.dl` file containing a valid Datalog program. Multiple input files are not supported.

A program can contain:
- Inline facts: `edge(1, 2).`
- Rules: `tc(x, y) :- edge(x, y).`
- Declarations: `.decl edge(x: int32, y: int32)`
- Directives: `.input`, `.output`, `.printsize`

## CSV Data Loading

The `.input` directive loads data from CSV files:

```
.decl edge(x: int32, y: int32)
.input edge(filename="edges.csv", delimiter=",")
```

| Parameter | Description | Default |
|-----------|-------------|---------|
| `filename` | CSV file path (relative to current directory) | required |
| `delimiter` | Field separator character | `,` |

CSV format:
- One tuple per line
- Fields separated by delimiter
- Empty lines are skipped
- Quoted strings are supported (quotes stripped)
- Tab delimiter: `delimiter="\t"`

## Output Control

### Default behavior

Without `.output` directives, all derived relations are printed.

### Selective output

With `.output`, only marked relations are printed:

```
.output tc
```

### Output format

Results are printed to stdout as tuples:

```
relation(value1, value2, ...)
```

Integer values print as decimal numbers. String values print with double quotes:

```
tc(1, 2)
tc("Alice", "Bob")
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (parse, execution, or argument error) |

## Error Messages

Errors are printed to stderr with the `error:` prefix.

| Message | Cause |
|---------|-------|
| `no input file specified` | No `.dl` file argument |
| `cannot read file 'path'` | File not found or unreadable |
| `--workers requires an argument` | Missing worker count |
| `workers must be between 1 and 256` | Worker count out of range |
| `unknown option 'name'` | Unrecognized flag |
| `multiple input files not supported` | More than one `.dl` file |
| `execution failed` | Parse or execution error |

## Pipeline

The CLI executes these stages in order:

1. **Parse** -- source file to AST
2. **Optimize** -- apply Fusion, JPP, SIP passes
3. **Stratify** -- topological ordering via SCC detection
4. **Generate Plan** -- IR to DD operator graph
5. **Marshal** -- convert to FFI-safe format
6. **Create Workers** -- instantiate DD worker threads
7. **Load Facts** -- inline facts and CSV files
8. **Execute** -- fixed-point computation
9. **Output** -- print result tuples

If any stage fails, execution stops with exit code 1.

## Examples

```bash
# Simple program with inline facts
wirelog-cli hello.dl

# Large dataset with parallel workers
wirelog-cli --workers 8 analysis.dl

# Show help
wirelog-cli --help
```
