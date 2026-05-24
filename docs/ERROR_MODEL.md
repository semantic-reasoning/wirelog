# Error Model

This document is the current user-facing policy for wirelog error reporting,
diagnostics/logging safety, signal and fork constraints, and restart handoff.
It describes the current contract; it does not claim a complete stable
error-code taxonomy.

## Error Boundaries

wirelog reports problems through three different channels:

- **Normal API errors**: public APIs report ordinary failures through
  `wirelog_error_t`, NULL returns, boolean returns, or facade-specific return
  values. Examples include parse failures, invalid program state, session
  creation errors, memory allocation failure, and easy/advanced session
  operation errors.
- **Diagnostics/logging**: `WL_LOG` and `WL_LOG_FILE` are diagnostic controls.
  They are for operator or developer visibility, not application control flow.
- **Process-fatal conditions**: crashes, aborts, process kills, and signal
  termination are outside normal API error reporting. After process restart,
  the host must rebuild wirelog state from host-owned inputs or snapshots.

Use API return values for control flow. Use logs to explain what happened after
the fact, not to decide whether an operation succeeded.

## Normal API Errors

The public API uses `wirelog_error_t` where the caller needs a structured
status. Pointer-returning APIs may return NULL and optionally write a
`wirelog_error_t` to an out-parameter. Easy and advanced session APIs return
`wirelog_error_t` directly where applicable.

Current guidance:

- Check every public API return value that can fail.
- Treat `WIRELOG_OK` as success and all other values as failure.
- Use `wirelog_error_string()` for user-facing or diagnostic text when a
  `wirelog_error_t` is available.
- Do not depend on logs being enabled, ordered, or present for correctness.

The enum is intentionally small today. Future work may refine taxonomy and
mapping, but this document does not freeze a full code-by-code policy.

## Diagnostics and WL_LOG

`WL_LOG` is a runtime, section-filtered diagnostic facility documented in the
README logging section. `WL_LOG_FILE` can redirect output to a file. If opening
`WL_LOG_FILE` fails, wirelog falls back to `stderr` with a one-time notice.

Safety policy:

- `WL_LOG` is not async-signal-safe.
- Do not call `wirelog_*` APIs from signal handlers.
- The emit path uses non-async-signal-safe operations such as formatting and
  file I/O, including `vsnprintf` and `fwrite`.
- Thresholds are written at `wl_log_init()`. Runtime threshold reads are
  lock-free byte loads, but logging output still goes through stdio.
- `WL_LOG` must remain diagnostic-only. Applications should not parse log
  output as an API.

Signal handlers should do only host-owned async-signal-safe work, such as
setting an atomic flag of the correct type for the host or writing a byte to a
host-owned file descriptor. Let normal host control flow call wirelog APIs after
the signal handler returns.

## Fork Constraints

wirelog does not install a `pthread_atfork` handler.

Prefer fork-then-exec. After `fork()`, a child that immediately calls
`execve()` or an equivalent exec-family function does not continue using
wirelog state across the fork boundary.

Fork-without-exec use is constrained by `docs/THREADING.md`. In particular,
child processes must not assume inherited sessions, registries, callbacks,
locks, adapters, or log sinks are safe to keep using. If a fork-without-exec
child changes the log sink, call `wl_log_init()` again before logging.

Adapter registry and plugin-loader fork concerns are covered by
`docs/io-adapters.md` and `docs/THREADING.md`.

## Process-Fatal Conditions and Restart Handoff

Process crashes and forced termination are not normal API errors. wirelog does
not provide a built-in WAL, checkpoint store, durable transaction log, or
automatic restart orchestration.

If restart recovery is needed, the host owns the durable source of truth:

- source program or generated program bundle,
- input facts and runtime events,
- snapshots or checkpoints maintained by the host,
- adapter and plugin configuration,
- output/export durability and partial-write policy.

On restart, rebuild wirelog state from those host-owned inputs: parse or reload
the program, recreate sessions or executors, reopen adapters, re-register
callbacks, and replay durable facts/events in a deterministic order.

The embedded-library threat model and optional crypto/export posture are
documented in `docs/SECURITY_MODEL.md`.

## Host Checklist

- Use `wirelog_error_t`, NULL returns, and facade return values for control
  flow.
- Use `WL_LOG` and `WL_LOG_FILE` only for diagnostics.
- Do not call `wirelog_*` APIs or `WL_LOG` from signal handlers.
- Prefer fork-then-exec. For fork-without-exec, follow `docs/THREADING.md`.
- If a forked child changes the log sink, call `wl_log_init()` again before
  logging.
- Keep restart durability in host-owned storage, not in wirelog process memory.
