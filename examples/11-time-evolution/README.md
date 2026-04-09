# Example 11: Time Evolution

## Overview

Each `wl_easy_step()` call is one tick of logical time.  Only deltas
produced *during that step* are reported to the delta callback -- tuples
derived in earlier steps are never re-emitted.

## Datalog Program

```datalog
.decl event(id: symbol, kind: symbol)
.decl alert(id: symbol)

alert(ID) :- event(ID, "error").
```

A trivially non-recursive rule: any event whose kind is `"error"` produces
an alert.  The lesson here is purely about *time* (epochs), not about
complex derivation.

## Build & Run

```sh
meson compile -C build time_evolution_demo
./build/examples/11-time-evolution/time_evolution_demo
```

## Expected Output

```
Example 11: Time Evolution (Per-Epoch Delta Isolation)
=====================================================

=== epoch 1: insert e1(error), e2(info), e3(error) ===
+ alert("e1")
+ alert("e3")

=== epoch 2: insert e4(info) ===

=== epoch 3: insert e5(error) ===
+ alert("e5")

Done.
```

## What This Demonstrates

- **Per-epoch delta isolation** -- epoch 3 reports only `+ alert("e5")`,
  not the epoch-1 alerts `alert("e1")` and `alert("e3")`.
- **Insert-only monotone growth** -- no retraction is used; the fact set
  grows monotonically across steps.
- **Discrete time model** -- each `wl_easy_step()` advances logical time
  by one tick.  Facts inserted between steps are batched into the next
  step.

## What You Will NOT See Here

- **Retraction** (`-1` deltas) -- see Example 09 (#442).
- **Recursive relations under update** -- see Example 10 (#443).
- **Snapshot vs delta mode comparison** -- see Example 12 (#445).

## See Also

- `examples/08-delta-queries/` -- single-step delta introduction
- `examples/09-retraction-basics/` -- retraction deltas
- `examples/10-recursive-under-update/` -- recursive transitive closure
- `wirelog/wl_easy.h` -- convenience facade API
