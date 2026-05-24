# Embedded Integration Guide

wirelog is an in-process C11 library embedded inside a host application. The
host owns I/O, durable storage, process lifecycle, restart supervision, and
threading policy around each session.

This guide summarizes the current embedded posture. It is not a new API
surface; it points embedders to the existing public headers and operational
docs.

## Public Integration Surface

Use installed public headers for host integration:

- `wirelog/wirelog.h` for parser, program, executor, and result APIs.
- `wirelog/wirelog-easy.h` for the simple session facade.
- `wirelog/wirelog-advanced.h` for explicit sessions, backend selection, worker
  counts, callbacks, snapshots, and compound helpers.
- `wirelog/io/io_adapter.h` for public I/O adapter integration.

Internal `wl_*` headers and symbols are not an integration surface. Do not
include internal headers such as `wirelog/session.h`, and do not depend on
internal `wl_*` names, layout, or lifetime rules.

## Build Options

The Meson options below are user-visible for embedded builds:

| Option | Embedded guidance |
| --- | --- |
| `embedded` | Opt-in embedded build mode. Do not assume a specific dependency or binary-size reduction unless you verify it for the target build. |
| `tests` | Set `-Dtests=false` for release artifacts that do not need test binaries. |
| `documentation` | Documentation build toggle; currently described as not implemented in `meson_options.txt`. |
| `threads` | Selects threading backend: `native` auto-detects C11/POSIX, `posix` forces pthreads. See `docs/THREADING.md`. |
| `wirelog_log_max_level` | Release and embedded builds should usually set `-Dwirelog_log_max_level=error` to compile out higher-volume logging sites. |
| `mbedTLS` | Defaults to `disabled`. Keep disabled unless crypto built-ins are required; enabling changes dependency and export posture. See `docs/SECURITY_MODEL.md`. |
| `io_plugin_dlopen` | Enables optional CLI plugin loading through `dlopen` where supported. Most embedded hosts link adapters directly or own dynamic loading themselves. |
| `android` | Opt-in Android cross-build path. Source build support exists; packaged AAR/Prefab artifacts are deferred. |
| `ios` | Opt-in iOS build path. Source build support exists; packaged XCFramework artifacts are deferred. |
| `crc32_variant` | Selects the default CRC-32 variant, `ethernet` or `castagnoli`. |
| `enable_fuzz` | Maintainer-only fuzz build/smoke option, not an embedded runtime feature. |

Android and iOS platform artifact posture is tracked in
`docs/PLATFORM_SUPPORT.md`. Do not infer packaged mobile artifacts from the
source build options alone.

## Lifecycle and Restart

wirelog does not provide a daemon, network listener, restart supervisor, WAL,
checkpoint store, durable transaction log, or automatic crash recovery service.
The host decides when to create and destroy programs, sessions, executors,
adapters, and callbacks.

For restart recovery, persist host-owned inputs or snapshots and rebuild
wirelog state after process restart. See `docs/CRASH_RESTART.md` for the
recommended restart pattern and the list of non-durable in-memory state.

## Threading and Concurrency

Sessions, callbacks, and adapters run inside the host process. The host owns
thread creation, worker-count choices, synchronization around session use, and
callback workload limits.

Bound worker count and concurrency to match the target device. Avoid blocking
or unbounded callback work on latency-sensitive threads. See
`docs/THREADING.md` for the current threading backend, atomics, fork-safety, and
worker-session contracts.

## Resource Constraints

wirelog sessions, executors, generated plans, results, intern tables, and
compound arenas are in-memory and scoped to the host process. Intern IDs and
compound handles are session/process-local; do not persist them as durable
identifiers.

Embedded hosts should bound:

- source program size and parse frequency,
- input fact size and relation cardinality,
- recursion and query shapes accepted from users,
- worker counts and concurrent session activity,
- callback work and allocation behavior,
- adapter side effects, file handles, and external writes.

wirelog does not currently make a hard real-time, fixed-memory, or
allocation-free execution guarantee. Hosts with strict memory or latency budgets
should enforce their own limits at the integration boundary.

## I/O and Adapter Boundaries

The host owns I/O policy. wirelog adapters run in process and should be treated
as part of the host's trusted integration boundary. Reopen files, sockets owned
by the host, plugin handles, and adapter state during restart as needed.

For adapter patterns and the public adapter ABI, see `docs/io-adapters.md`.
Partial writes, export artifacts, and adapter side effects are the host or
adapter's responsibility.

## Logging and Security Posture

For release/embedded artifacts, prefer:

```sh
meson setup build-embedded --buildtype=release \
  -Dembedded=true \
  -Dtests=false \
  -Dwirelog_log_max_level=error \
  -DmbedTLS=disabled
```

The default `mbedTLS=disabled` build avoids the optional crypto/export posture.
If you enable mbedTLS-backed built-ins, review `docs/SECURITY_MODEL.md` and
your distribution requirements.

Detailed `WL_LOG` and future error-model safety taxonomy is intentionally not
duplicated here. Keep logging volume and log sinks under host control.
