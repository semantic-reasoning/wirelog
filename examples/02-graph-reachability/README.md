# Graph Reachability: Network Connectivity Analysis

## Overview

This example demonstrates **graph reachability analysis** - finding all pairs of nodes that are connected through any path in a network.

**Real-world use cases:**
- Flight network: Which cities can you reach from any starting city?
- Social networks: Who can you contact through a chain of mutual acquaintances?
- Road networks: Which towns are reachable from a given location?
- Supply chains: Which suppliers can deliver to which warehouses?

## Files

- **flights.csv**: Direct flight routes reference (not used by default)
  - Format: `from_city,to_city`
  - Example: `Seoul,Busan` means there's a direct flight from Seoul to Busan

- **graph_reachability.dl**: Datalog query
  - Defines flight routes as inline facts (symbol type)
  - Computes all reachable city pairs via transitive closure
  - Outputs results to CSV via `.output` directive

- **reachable_routes.csv**: Generated output
  - All pairs of cities connected by any path
  - Example: `Seoul,Daegu` (Seoul → Daejeon → Daegu)
  - 15 reachable pairs from 6 direct flights

## How It Works

### Graph Representation
The input is a **directed graph** where:
- **Nodes** = Cities
- **Edges** = Direct flights

Example network:
```
Seoul ──→ Busan
  │       └──→ Daegu
  ↓           ↑
Daejeon ──────┘
  ↓
Gwangju ──→ Incheon
```

### Reachability Logic

**Step 1: Direct connections** (Base case)
```datalog
reachable(X, Y) :- flight(X, Y).
```
Any city with a direct flight is reachable.

**Step 2: Multi-hop paths** (Recursive case)
```datalog
reachable(X, Z) :- reachable(X, Y), flight(Y, Z).
```
If X can reach Y, and Y has a flight to Z, then X can reach Z.

The Datalog evaluator iterates this rule until no new facts are derived (convergence).

### Expected Output

Given the flights.csv data, the reachable routes should include:
```
Seoul,Busan
Seoul,Daejeon
Seoul,Daegu        (Seoul → Daejeon → Daegu)
Seoul,Gwangju      (Seoul → Daejeon → Gwangju)
Seoul,Incheon      (Seoul → Daejeon → Gwangju → Incheon)
Busan,Daegu
Daejeon,Gwangju
Daejeon,Incheon    (Daejeon → Gwangju → Incheon)
Daegu,Daejeon
Daegu,Gwangju      (Daegu → Daejeon → Gwangju)
Daegu,Incheon      (Daegu → Daejeon → Gwangju → Incheon)
Gwangju,Incheon
... and more
```

## Key Differences from Other Examples

| Aspect | Example 01 (Ancestors) | Example 02 (Directives) | Example 03 (Reachability) |
|--------|----------------------|------------------------|--------------------------|
| Domain | Family relationships | Family relationships | Network/Graph analysis |
| Input | External CSV file | Inline facts | External CSV file |
| Data type | String (names) | Symbol type | Symbol type |
| Purpose | Show .input/.output | Show symbol type | Show graph algorithms |
| Complexity | Simple genealogy | Multi-fact inline | Large transitive closure |

## Running the Example

```bash
cd examples/03-graph-reachability
wirelog-cli graph_reachability.dl
# Output written to reachable_routes.csv
```

Or with the Python test harness:
```bash
python3 ../../scripts/test_example.py graph_reachability.dl
```

## Learning Points

1. **Transitive Closure**: Core graph algorithm using recursion
2. **External Data**: Loading graph edges from CSV
3. **Output Filtering**: Saving computed results to files
4. **Scalability**: How Datalog handles larger graphs efficiently
