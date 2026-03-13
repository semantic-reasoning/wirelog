# Example 08: Delta Queries with Callback API

## Overview

This example demonstrates **delta queries** — receiving incremental change
notifications as the Datalog engine derives new facts. Instead of polling
for the full query result, you register a callback that fires exactly once
for each newly derived tuple with a `+1` (insertion) or `-1` (retraction)
diff marker.

**Real-world use cases:**
- Access control audit logs: emit an event each time a permission is granted
- Change-data-capture (CDC) pipelines: stream derived facts to a message queue
- Reactive UIs: update views only when the underlying data changes
- Policy enforcement: trigger side-effects on new rule violations

> **Note:** This is a C API example. The `--delta` CLI flag for `wirelog_cli`
> is not yet implemented. The delta-callback API is fully functional via the
> C session interface demonstrated here.

## Files

- **access_control.dl**: Datalog program for the access control example (reference)
- **delta_demo.c**: C program demonstrating `wl_session_set_delta_cb()`
- **example.csv**: Sample access control facts used as input
- **meson.build**: Build configuration for `delta_demo`

## How to Run

From the project root:

```bash
meson compile -C build delta_demo
./build/examples/08-delta-queries/delta_demo
```

Expected output:

```
Example 08: Delta Queries with Callback API
============================================

Inserting access control facts...

Delta output from wl_session_step():
+ granted("alice", "read")
+ granted("alice", "write")
+ granted("bob", "read")
+ granted("bob", "admin")
+ granted("carol", "read")

Done.
```

## Pattern Explanation

### 1. Register the Callback

```c
wl_session_set_delta_cb(session, on_delta, user_data);
```

The callback fires for every output tuple that is newly derived (diff = +1)
or retracted (diff = -1) when `wl_session_step()` is called.

### 2. Callback Signature

```c
typedef void (*wl_on_delta_fn)(
    const char *relation,  /* output relation name     */
    const int64_t *row,    /* column values (IDs)      */
    uint32_t ncols,        /* number of columns        */
    int32_t diff,          /* +1 insertion, -1 removal */
    void *user_data        /* caller-supplied context  */
);
```

### 3. Symbol Interning

Wirelog stores `symbol` column values as integer IDs. To produce
human-readable output in the callback, use `wl_intern_reverse()`:

```c
static void
on_delta(const char *relation, const int64_t *row, uint32_t ncols,
         int32_t diff, void *user_data)
{
    const wl_intern_t *intern = (const wl_intern_t *)user_data;
    const char *user = wl_intern_reverse(intern, row[0]);
    const char *perm = wl_intern_reverse(intern, row[1]);
    printf("%c %s(\"%s\", \"%s\")\n",
           diff > 0 ? '+' : '-', relation, user, perm);
}
```

To insert facts with symbol columns from C code, intern your strings
with `wl_intern_put()` before creating the session plan:

```c
wl_intern_t *intern = (wl_intern_t *)wirelog_program_get_intern(prog);
int64_t id_alice = wl_intern_put(intern, "alice");
int64_t id_read  = wl_intern_put(intern, "read");

int64_t row[] = { id_alice, id_read };
wl_session_insert(session, "can", row, 1, 2);
```

### 4. Datalog Program

```datalog
.decl can(user: symbol, perm: symbol)
.decl granted(user: symbol, perm: symbol)

granted(U, P) :- can(U, P).
```

`granted` is a derived (IDB) relation. Every time a new `can` tuple is
inserted and the session is stepped, the engine re-evaluates the rules
and fires the delta callback for any newly derived `granted` tuples.

## Expected Output

| Tuple | diff | Meaning |
|-------|------|---------|
| `granted("alice", "read")` | +1 | alice can read |
| `granted("alice", "write")` | +1 | alice can write |
| `granted("bob", "read")` | +1 | bob can read |
| `granted("bob", "admin")` | +1 | bob has admin access |
| `granted("carol", "read")` | +1 | carol can read |

The delta callback fires exactly once per new derived tuple —
not on every step if the fact was already known.

## Key Concepts

### Delta vs. Snapshot

| API | When to use |
|-----|-------------|
| `wl_session_set_delta_cb` + `wl_session_step` | Streaming, event-driven, incremental pipelines |
| `wl_session_snapshot` | One-shot batch queries |

Delta mode is efficient for cases where facts trickle in over time and
you want to react to each change without re-scanning the full result set.

### Incremental Evaluation

Wirelog's columnar backend uses semi-naive evaluation with set-difference
to compute exactly the new tuples between successive steps. The callback
receives only the delta — tuples that were not present in the previous
snapshot. This is efficient even for large relation sizes.

## Next Steps

- Add role-based rules: `granted(U, P) :- role(U, R), role_perm(R, P)`
- Extend to retraction: use `wl_session_remove()` to revoke permissions
  and observe diff=-1 callbacks
- Combine with `07-multi-source-analysis` to integrate permissions from
  multiple identity providers
