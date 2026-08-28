# Internals Map

This document is for wirelog contributors and maintainers. It orients readers
to the source tree and the public/internal boundary. It is not an embedder API,
ABI promise, or stability contract.

Internal headers, internal symbols, file layout, and subsystem boundaries may
change without compatibility promises. Embedded applications should use the
installed public headers described in `docs/EMBEDDED.md`.

## Public and Internal Boundary

The installed public header list is the `wirelog_public_headers` array in
`meson.build`:

- `wirelog/wirelog.h`
- `wirelog/wirelog-types.h`
- `wirelog/wirelog-parser.h`
- `wirelog/wirelog-ir.h`
- `wirelog/wirelog-optimizer.h`
- `wirelog/wirelog-export.h`
- `wirelog/wirelog-easy.h`
- `wirelog/wirelog-advanced.h`

`wirelog/io/io_adapter.h` is installed separately under `wirelog/io`. The
generated `wirelog/wirelog-version.h` header is tracked separately from the
source header list.

Public API symbols and public macros use the `wirelog_*` / `WIRELOG_*`
prefixes. New installed public prototypes use `WIRELOG_API` from
`wirelog/wirelog-export.h`.

Internal headers and symbols use `wl_*` / `WL_*`. Where practical, internal
names encode their subdirectory or module. For example, parser internals live
under `wirelog/parser/*`, while IR internals live under `wirelog/ir/*`.
This naming helps maintainers navigate the tree; it does not make those names
stable for downstream code.

## Subsystem Map

| Area | Main files | Role |
| --- | --- | --- |
| Public API facades | `wirelog/wirelog.h`, `wirelog/wirelog-easy.h`, `wirelog/wirelog-advanced.h`, `wirelog/api_facade.c`, `wirelog/wirelog-easy.c`, `wirelog/wirelog_advanced.c` | Installed entry points and facade implementations for parser/program/executor, easy sessions, and advanced sessions. |
| Parser and AST | `wirelog/parser/*` | Lexer, parser, and AST construction for Datalog source. |
| IR, programs, stratification, interning | `wirelog/ir/*`, `wirelog/intern.*` | Public IR wrappers, internal program representation, stratification, and string interning. |
| Optimizer passes | `wirelog/passes/*` | Fusion, join plan placement, magic sets, sideways information passing, and subsumption passes. |
| Execution plan generation | `wirelog/exec_plan*` | Backend-neutral execution-plan types and lowering from IR/program state. |
| Session and backend abstraction | `wirelog/session.*`, `wirelog/session_facts.*`, `wirelog/backend.h` | Internal session lifecycle, inline fact seeding, and backend vtable boundary. |
| Columnar backend | `wirelog/columnar/*` | Relations, arrangements, frontier/progress tracking, expression evaluation, diff traces, delta pools, partitioning, and columnar session execution. |
| Arenas and compound storage | `wirelog/arena/*`, `wirelog/columnar/compound_side.*`, `wirelog/columnar/handle_remap*` | Arena allocation, compound arena handles, side relations, and handle remapping support. |
| I/O adapters | `wirelog/io/*` | Public adapter ABI implementation, CSV reader/adapter code, and adapter context internals. |
| Threading, workqueue, logging utilities | `wirelog/thread*`, `wirelog/workqueue.*`, `wirelog/util/*` | Thread backend wrappers, worker queues, lock-free helpers, and internal logging implementation. |
| CLI | `wirelog/cli/*` | Command-line driver, plugin loader, and CLI-specific wiring around the library. |

For broader design context, see `docs/ARCHITECTURE.md`. For semantic contracts
that internals must preserve, see `docs/SEMANTICS.md`, `docs/COMPOUND_TERMS.md`,
and `docs/RDF_QUADS.md`.

## Runtime Ownership Notes

Most internal state is process-local and session-scoped: generated plans,
sessions, executors, result objects, intern tables, compound arenas, callbacks,
and pending deltas are rebuilt or discarded through normal API lifetimes. See
`docs/CRASH_RESTART.md` for restart posture and `docs/EMBEDDED.md` for host
integration responsibilities.

Threading details are intentionally not duplicated here. Maintainers changing
worker dispatch, atomics, locks, session ownership, or fork-sensitive behavior
should update and validate `docs/THREADING.md` alongside code and tests.

Security and export posture are also documented separately. Maintainers changing
crypto build behavior, mbedTLS dependencies, or externally visible threat-model
assumptions should update `docs/SECURITY_MODEL.md`.

I/O adapter changes that affect the installed adapter ABI or host/plugin
integration should be reflected in `docs/io-adapters.md`.

The weighted/probabilistic evaluation proposal is intentionally outside the
current public and internal runtime boundary. See
[`docs/design/weighted-semiring-addon.md`](design/weighted-semiring-addon.md).
Any future implementation should begin as a separate backend; a public weight
ABI would require a distinct version namespace, `WIRELOG_API` declarations,
registration in `wirelog_public_headers`, and an ABI-manifest review.

## Changing Internals Checklist

Before landing an internal change, check whether it crosses a public boundary:

- Keep installed public headers free of internal `wl_*` / `WL_*` names unless a
  deliberate compatibility decision is documented.
- Use `wirelog_*` / `WIRELOG_*` and `WIRELOG_API` for new public installed
  declarations.
- If the public header surface or exported symbol set changes, update the ABI
  and public-header gates as needed.
- If a public source migration is required, update `docs/MIGRATION.md`.
- If runtime semantics change, update `docs/SEMANTICS.md` and focused tests.
- If compound terms, RDF quads, threading, crash/restart, embedded posture,
  security, or I/O adapter behavior changes, update the matching docs linked
  above.

Internal-only refactors do not need migration notes, but they should preserve
the public contracts and tests for the affected subsystem.
