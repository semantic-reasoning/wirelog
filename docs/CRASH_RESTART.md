# Crash and Restart Guide

wirelog is an in-process embedded library. It does not own the host process,
the host storage layer, or service orchestration. This guide describes the
current crash/restart posture so embedders can design their own durability
boundary.

## Guarantees

Within a live process, wirelog owns the memory behind the sessions, executors,
plans, results, intern tables, callback registrations, pending deltas, and
compound handles it creates. Normal API errors are reported through the API
surface for the active call path.

Process restart is different from in-process error handling. If the process
crashes or is killed, wirelog does not preserve in-memory engine state for
automatic recovery. Detailed error taxonomy and `WL_LOG` safety guidance are
tracked separately for the future error model work; see `docs/THREADING.md` for
current concurrency contracts and README logging caveats.

## Non-Guarantees

On process crash, the following in-memory state is lost:

- sessions and executors,
- generated plans and result objects,
- callback registrations,
- pending deltas,
- intern IDs,
- compound handles,
- adapter/plugin runtime state owned by the host process.

wirelog currently has no built-in WAL, checkpoint format, fsync discipline,
crash recovery store, durable transaction log, or automatic restart
orchestration. It also does not repair partial host writes or partial export
artifacts after a crash.

## Host-Owned Durability

If restart recovery is required, the host application owns the durable source
of truth. Common strategies include:

- storing the Datalog source program or generated program bundle,
- recording durable EDB facts or input events in a host-owned log,
- periodically writing host-owned snapshots,
- using host storage transactions and fsync policy appropriate for the
  deployment,
- recording adapter or plugin configuration needed to reopen inputs and
  outputs.

Partial writes, partially exported files, adapter side effects, plugin state,
and external file or database durability are host or adapter responsibilities.
See `docs/io-adapters.md` for adapter integration guidance.

## Safe Restart Pattern

A conservative restart path is:

1. Recreate or reload the source program.
2. Recreate the session or executor from that program.
3. Reopen adapters, files, and plugins owned by the host.
4. Re-register callbacks.
5. Replay host-owned durable EDB facts and runtime events in deterministic
   order.
6. Resume stepping, snapshotting, or exporting from the reconstructed state.

Inline `.dl` facts are not restored by a hidden recovery path. They are loaded
again through normal program/session construction and easy-facade lazy-build
semantics. See `docs/SEMANTICS.md` for inline fact materialization and Z-set
insert/remove semantics.

Choose one replay order and keep it stable. If the host keeps both snapshots
and an event log, define whether the event log is replayed from the snapshot
point or from process start; wirelog does not infer that boundary.

## Non-Durable Identifiers

Intern IDs and compound handles are session/process-local implementation
values. Do not persist them as durable identifiers, and do not expect them to be
stable across process restarts, session recreation, or program reloads.

Persist source strings, relation names, primary-key values, or host-owned
application identifiers instead. Re-intern strings and recreate compound values
after rebuilding the session.

## Security and Deployment Notes

Restart policy is part of the host application's threat model and operations
model. wirelog does not open network listeners or provide restart supervision.
For the embedded-library threat model, optional crypto posture, and export
self-classification, see `docs/SECURITY_MODEL.md`.
