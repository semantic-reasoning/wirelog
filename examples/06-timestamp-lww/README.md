# Example 06-timestamp-lww: Last-Write-Wins Conflict Resolution

## Overview

This example demonstrates **Last-Write-Wins (LWW)** conflict resolution — a common
strategy for distributed systems where multiple nodes may write different values for
the same key. The rule is simple: the update with the highest timestamp wins.

**Real-world use cases:**
- IoT sensor networks: sensors send buffered readings that arrive out of order
- Distributed caches: replicas propagate updates asynchronously
- Event-sourced systems: replayed events with wall-clock timestamps
- CRDTs (Conflict-free Replicated Data Types): LWW-Register implementation
- Database replication: last-writer-wins merge on conflict

## Files

- **updates.csv**: All received update versions
  - Format: `record_id,unix_timestamp,value`
  - Column order: id first, timestamp second, value third
  - Contains multiple versions per record and exact-timestamp ties
  - Example: `sensor_1` appears at ts=100, 250, 300 — newest wins

- **lww_resolution.dl**: Datalog rules implementing LWW semantics
  - Declares `update(id, ts, value)` with ts before value so the `max()`
    aggregate reads the correct column (engine reads `row[group_by_count]`)
  - Finds the maximum timestamp per record using `max()` aggregation
  - Joins back to recover the full winning record
  - Handles timestamp ties by retaining all tied versions

- **expected_latest.csv**: Expected winning timestamp per record ID
  - Format: `record_id,winning_timestamp`

- **latest_ts_out.csv** / **latest_out.csv**: Generated outputs (after running)

## How It Works

### The LWW Algorithm

**Step 1: Find the winning timestamp per record**

```datalog
latest_ts(Id, max(Ts)) :- update(Id, Ts, _).
```

The `max()` aggregate groups all update tuples by `Id` and keeps only the
highest `Ts` value. This is the LWW "write wins" rule.

**Step 2: Recover the full record at the winning timestamp**

```datalog
latest(Id, Ts, Val) :- latest_ts(Id, Ts), update(Id, Ts, Val).
```

Joining `latest_ts` back against `update` filters out all older versions,
leaving only the record(s) that match the maximum timestamp.

### Timestamp Comparison and Conflict Resolution

| Scenario | Behavior |
|----------|----------|
| One clear winner (different timestamps) | The highest-ts version wins |
| Exact tie (same timestamp, different values) | Both versions are retained |
| Same timestamp, same value | Deduplicated to one tuple (Datalog set semantics) |

Ties on identical timestamps reflect a genuine concurrent write — neither
update can be declared "later". The Datalog program surfaces all tied
versions; the calling application is responsible for tiebreaking (e.g., by
lexicographic value order, node ID, or CRC).

### Example Trace

Input (`updates.csv`, format: `id,ts,value`):

```
sensor_1, 1700000100, 23.5   <- older
sensor_1, 1700000250, 23.9   <- older
sensor_1, 1700000300, 24.1   <- WINNER (highest ts)

sensor_2, 1700000200, 45.2
sensor_2, 1700000200, 45.8   <- TIE: same ts, different values
sensor_2, 1700000400, 46.0   <- WINNER (highest ts beats both ties)

sensor_3, 1700000150, 12.8
sensor_3, 1700000350, 13.5   <- WINNER

sensor_4, 1700000500, 99.0   <- TIE: same ts, different values -> both kept
sensor_4, 1700000500, 98.5   <- TIE: both survive into latest output
```

Expected `latest_ts` output:

```
sensor_1, 1700000300
sensor_2, 1700000400
sensor_3, 1700000350
sensor_4, 1700000500
```

Expected `latest` output (sensor_4 has a tie):

```
sensor_1, 1700000300, 24.1
sensor_2, 1700000400, 46.0
sensor_3, 1700000350, 13.5
sensor_4, 1700000500, 98.5
sensor_4, 1700000500, 99.0
```

## Running the Example

From the project root:

```bash
./build/wirelog_cli examples/06-timestamp-lww/lww_resolution.dl
```

Or from the example directory:

```bash
../../build/wirelog_cli lww_resolution.dl
```

Outputs are written to:
- `examples/06-timestamp-lww/latest_ts_out.csv`
- `examples/06-timestamp-lww/latest_out.csv`

Verify the winning timestamps match expectations:

```bash
diff examples/06-timestamp-lww/latest_ts_out.csv \
     examples/06-timestamp-lww/expected_latest.csv
```

## Key Concepts

### Why max() Aggregation for LWW

Monotone `max()` aggregation is the natural Datalog encoding of LWW:
- It is **monotone** — the winner can only increase as new updates arrive
- It is **declarative** — no explicit loop or comparison predicate needed
- It composes with incremental evaluation — wirelog reuses prior `max` results
  when new updates are inserted, recomputing only affected records

### LWW vs. Other Conflict Strategies

| Strategy | Datalog encoding | Use case |
|----------|-----------------|----------|
| Last-Write-Wins | `max(timestamp)` | Eventual consistency, low coordination |
| First-Write-Wins | `min(timestamp)` | Immutable records, audit logs |
| Merge all versions | No aggregation | CRDTs, multi-value registers |
| Majority vote | `count()` + threshold | Quorum-based systems |

### Caution: Clock Skew

LWW assumes timestamps are globally comparable. In distributed systems, clocks
drift. A version with a lower wall-clock timestamp may represent a logically
later write if clocks are not synchronized. Hybrid Logical Clocks (HLC) or
vector clocks address this, but LWW with wall-clock timestamps remains the
most common choice for simplicity.

## Next Steps

- Extend with a `tiebreak(Id, Val)` rule using `min(Val)` to resolve ties
  deterministically
- Add a `stale(Id, Val, Ts)` output to capture discarded versions for auditing
- Combine with `02-graph-reachability` to propagate LWW state across a network
