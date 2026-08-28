# Scalar Function Addon

Status: design proposal only (Issue #912)

This document specifies a possible domain-neutral extension point for pure,
row-local scalar functions and predicates. It does not add an addon runtime,
parser syntax, public header, or installed API. Names and structures beginning
with `wirelog_` below are illustrative, non-normative pseudocode.
This issue delivers no runtime or public API.

## Current implementation boundary

Built-in expression functions currently follow the fixed path:

`lexer/parser -> AST -> IR -> serialized plan expression -> columnar evaluator`

The current string-function path includes `wirelog_str_fn_t`,
`WL_IR_EXPR_STR_FN`, and fixed evaluator opcodes. This issue leaves that path
and all built-in semantics unchanged. A future generic addon is an opaque
expression to the optimizer and must never store a raw function pointer in a
serialized plan.

## Recommended direction

Use a separately versioned, explicit registry whose immutable snapshot is a
registry snapshot captured during compilation or session creation. Parsing may retain a
namespace-qualified name without consulting a registry; resolution happens
before execution and fails before a plan is published if the function is
missing or its signature does not match. The snapshot owns a reference to each
descriptor and its user data. The sequence

`register -> snapshot -> unregister -> plan execution -> plugin unload`

must remain safe: unregister only affects future snapshots, while active
snapshots keep descriptors pinned. Unload is rejected or deferred until all
plans, sessions, callbacks, and destroy operations release the snapshot. A
snapshot must not retain an unprotected raw pointer.

Names are UTF-8, namespace-qualified, normalized once, and compared
case-sensitively. Built-in names win over addon names; duplicate addon names
are a registration error. Parse-time unknown names are allowed only if the
future generic syntax is explicitly documented; compile-time resolution is mandatory.
Serialized plans contain the normalized name and descriptor ABI
identity/version, never a process-local address. Loading a plan with a missing
or incompatible descriptor fails before execution.

## Candidate value and callback contract

The first candidate value set is limited to the existing `int64`, boolean, and
UTF-8/interned-string representations. These are future design choices, not
currently stable addon ABI types. Null, floating-point, compound, and relation
values are deferred. An interned-string result must either be copied into a
registry/session-owned allocation or explicitly retain the intern table; a
callback must not return a pointer whose lifetime ends at callback return.

A descriptor would declare namespace-qualified name, arity, argument and result
types, purity, determinism, external-state use, allocation behavior, and
thread-safety. Its callback receives an immutable argument view and opaque user
data. It cannot mutate relations, sessions, plans, registries, or evaluator
state. A successful scalar result, a false predicate result, and a callback error
are distinct outcomes. Invalid values, type errors, overflow, and
allocation failure are errors, not false predicates.

Output buffers use the host-provided allocator and the ownership rule is
explicit: the host releases returned storage through the matching destroy
operation. Callback failures map to the existing `WIRELOG_ERR_EXEC` in the
first design rather than silently expanding the current error enum; a later
dedicated error namespace requires a separate ABI decision. Error context
includes function name, expression/row context, and worker identity where
available. Callbacks must not throw, `longjmp`, terminate the process, or
write a global mutable error buffer.

`FILTER` callback errors are hard errors. They discard the in-flight evaluation;
they are not fail-closed false results. `MAP` and head-expression errors are
also hard errors. If one parallel worker fails, evaluation is cancelled,
uncommitted worker output is discarded, and the session delta is not published
until all callbacks succeed. The current snapshot remains unchanged; rollback
or replacement is atomic at the existing session boundary.

## Reentrancy, threading, and determinism

Callbacks are non-reentrant in v1: they may not call Wirelog APIs, re-enter the
same evaluator, alter a registry, unregister their descriptor, or trigger
recursive evaluation. Workers may invoke a thread-safe callback concurrently.
Without an explicit thread-safe declaration, the evaluator must serialize the
callback or reject worker execution. Pure/deterministic callbacks may not use
time, random state, process-global mutable state, or uncontrolled external
I/O. Native addons are trusted process-local code, not a sandbox.

Descriptor and user-data destruction runs only after all snapshot references
and in-flight callbacks end. Registration validates ABI version, name,
signature, callback, allocator, and capability fields before publication.

## Optimizer and serialization policy

Descriptors declare capabilities separately from the callback's purity flag.
An unknown or unverified capability disables predicate pushdown,
common-subexpression elimination, constant folding, reordering, duplicate
elimination, and assumptions about parallel evaluation. Generic addon calls
are otherwise opaque. Only a future proof of the relevant type, purity,
determinism, and algebraic law may enable a rewrite. Existing built-in
functions retain their current optimization and semantics.

The serialized representation contains a name, signature, and addon ABI
identity/version. It is portable only where the receiving registry supplies a
compatible descriptor. Function pointers, user-data addresses, allocator
addresses, and host-specific intern ids are never serialized.

## Future public ABI boundary and non-goals

Only after a prototype and cross-platform tests succeed should a public header
such as `wirelog/wirelog-function.h` be considered. It must use a separate
`WIRELOG_FUNCTION_ABI_VERSION`, never `WIRELOG_IO_ABI_VERSION`; public types
and functions use `wirelog_` and public macros/enums use `WIRELOG_`, while new
declarations use `WIRELOG_API`. The header must be added to
`wirelog_public_headers`, and the ABI symbol manifest, version/deprecation
policy, ownership, and unload guarantees must be reviewed together.

The existing I/O adapter registry is not reused: it has a different public ABI,
adapter lifetime, and synchronization contract. “Registry snapshot” here is a
future scalar-function design term only.

This issue adds no geometry function, GEOS/PROJ/GDAL dependency, dynamic loader,
native sandbox, stateful aggregate, relation operator, spatial index, optimizer
pushdown, parser keyword, generic runtime call, public header, ABI symbol, or
currently usable addon API. Existing built-in parser/evaluator and public API
tests remain authoritative and unchanged.

## Implementation decision gate

Before any runtime implementation, a prototype must test name collision and
resolution, snapshot/unregister/unload lifetime, all value/error outcomes,
allocation ownership, concurrent invocation, failure atomicity, serialized
round-trip, and optimizer conservatism on Linux, macOS, Windows, and ARM.
The prototype must also define whether unresolved names are accepted at parse
time and how version incompatibility is reported. Until that evidence exists,
the recommended output is this design document and its contract checker only.
