# Example 07-multi-source-analysis: Multi-Source Data Integration

## Overview

This example demonstrates **multi-source data integration** — a common challenge
when consolidating customer or product records from independent systems. Two CRM
databases (Source A and Source B) each hold a partial view of the customer base.
The goals are:

1. **Unify** all customers into a single integrated table
2. **Resolve conflicts** using a deterministic policy (Source A wins)
3. **Identify coverage gaps** — customers present in only one source
4. **Flag discrepancies** — records that differ between sources and need review
5. **Aggregate** across the integrated view for reporting

**Real-world use cases:**
- CRM consolidation after mergers and acquisitions
- Master data management (MDM) across regional databases
- ETL pipelines merging data warehouses with operational systems
- Deduplication before a data lake migration
- Cross-system customer 360 views

## Files

- **source_a.csv**: Customer records from CRM system A (primary source of record)
  - Format: `customer_id,name,country`
  - 7 customers: C001–C007

- **source_b.csv**: Customer records from CRM system B (secondary source)
  - Format: `customer_id,name,country`
  - 6 customers: C002–C004, C008–C010
  - Overlaps with A on C002–C004; C003 has a discrepancy (`Caroline Smith/GB` vs `Carol Smith/UK`)

- **multi_source.dl**: Datalog rules implementing the integration pipeline

- **expected_integrated.csv**: Expected unified customer table (10 customers, A-wins policy)

- **integrated_out.csv** / **a_only_out.csv** / **b_only_out.csv** / **conflict_out.csv** /
  **customers_per_country_out.csv**: Generated outputs (after running)

## How It Works

### Step 1: Unified Integration (Source A Wins)

```datalog
integrated(Id, Name, Country) :- src_a(Id, Name, Country).

integrated(Id, Name, Country) :-
    src_b(Id, Name, Country),
    !src_a(Id, _, _).
```

Source A records are always included unconditionally. Source B records are
included only when `!src_a(Id, _, _)` — the customer_id is absent from A.
This is the **Source A wins** (or "primary source") conflict resolution policy.
It requires no timestamp comparison; priority is encoded structurally.

### Step 2: Coverage Gap Detection

```datalog
a_only(Id) :- in_a(Id), !in_b(Id).
b_only(Id) :- in_b(Id), !in_a(Id).
```

These rules identify customers that exist in exactly one source. `a_only`
records are not in B and may need to be created there. `b_only` records
were unknown to A and are pulled in via the integration rule above.

### Step 3: Conflict Detection

```datalog
conflict(Id) :-
    src_a(Id, NameA, _),
    src_b(Id, NameB, _),
    NameA != NameB.

conflict(Id) :-
    src_a(Id, _, CountryA),
    src_b(Id, _, CountryB),
    CountryA != CountryB.
```

A conflict is any customer_id where A and B disagree on at least one field.
Two rules handle the two disjoint mismatch conditions (name vs country).
Conflict records are surfaced for human review before Source B is retired.

### Step 4: Regional Reporting

```datalog
customers_per_country(Country, count(Id)) :- integrated(Id, _, Country).
```

Aggregates the integrated view by country. Because integration is already
resolved, this count reflects the final canonical customer base.

### Integration Summary

| customer_id | Source A | Source B | Result | Notes |
|-------------|----------|----------|--------|-------|
| C001 | Alice Martin/US | — | Alice Martin/US | A only |
| C002 | Bob Chen/CN | Bob Chen/CN | Bob Chen/CN | identical in both |
| C003 | Carol Smith/UK | Caroline Smith/GB | Carol Smith/UK | conflict — A wins |
| C004 | David Kim/KR | David Kim/KR | David Kim/KR | identical in both |
| C005 | Eve Tanaka/JP | — | Eve Tanaka/JP | A only |
| C006 | Frank Muller/DE | — | Frank Muller/DE | A only |
| C007 | Grace Lee/KR | — | Grace Lee/KR | A only |
| C008 | — | Henry Dubois/FR | Henry Dubois/FR | B only (gap filled) |
| C009 | — | Iris Rossi/IT | Iris Rossi/IT | B only (gap filled) |
| C010 | — | James Park/KR | James Park/KR | B only (gap filled) |

## Running the Example

From the project root:

```bash
./build/wirelog_cli examples/07-multi-source-analysis/multi_source.dl
```

Outputs are written to:
- `examples/07-multi-source-analysis/integrated_out.csv`
- `examples/07-multi-source-analysis/a_only_out.csv`
- `examples/07-multi-source-analysis/b_only_out.csv`
- `examples/07-multi-source-analysis/conflict_out.csv`
- `examples/07-multi-source-analysis/customers_per_country_out.csv`

Verify the integrated table matches the expected output:

```bash
diff examples/07-multi-source-analysis/integrated_out.csv \
     examples/07-multi-source-analysis/expected_integrated.csv
```

Expected output (no diff):
```
(empty — files are identical)
```

## Key Concepts

### Why Negation-as-Failure for Source Priority

The rule `!src_a(Id, _, _)` uses negation-as-failure (NAF): a source B record
is included only if there is no evidence of that customer_id in source A.
This is the standard Datalog encoding of "fill gaps, don't overwrite":

| Scenario | NAF result |
|----------|-----------|
| Id in A only | A record included, B rule body fails |
| Id in B only | B record included (no A match to block it) |
| Id in A and B | A record included; B rule body fails |

NAF is safe here because `src_a` is a base (EDB) relation — it does not
depend on `integrated`, so there is no circular negation.

### Source Priority vs. LWW

This example uses structural priority (A always wins) rather than
timestamp-based LWW (see `06-timestamp-lww`). The tradeoffs:

| Strategy | Encoding | Best for |
|----------|----------|----------|
| Source priority (this example) | `!src_a(Id, _, _)` | Stable system of record |
| Last-Write-Wins | `max(timestamp)` | Concurrent distributed writes |
| Manual merge | surface conflicts only | High-value records needing review |

### Conflict Surfacing vs. Silent Resolution

Silently picking a winner hides data quality problems. This example separates
concerns: `integrated` applies the policy automatically, while `conflict`
surfaces every discrepancy for a data steward to review. After review, the
source data can be corrected, and the integration re-run — the Datalog rules
recompute correctly without state from the previous run.

### Incremental Evaluation

If new customers are inserted into `src_a` or `src_b`, wirelog's incremental
evaluation recomputes only the affected portions of `integrated`, `a_only`,
`b_only`, and `conflict` — not the entire table. This makes the pattern
efficient for streaming MDM pipelines where new records arrive continuously.

## Next Steps

- Add a `tiebreak` rule using `min(Name)` to resolve conflicts deterministically
  without human intervention
- Extend with a third source `src_c` and a priority chain (A > B > C)
- Combine with `06-timestamp-lww` to use update timestamps as tiebreakers
- Add a `merged(id, name_a, name_b, country_a, country_b)` relation to expose
  the full conflict record for audit reporting
