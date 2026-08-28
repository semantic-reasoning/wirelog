# Weighted and Semiring Evaluation Addon

Status: design proposal only (Issue #913)

This document describes a possible extension point for probabilistic and
other weighted logic. It does not add probabilistic semantics to Wirelog and
does not define an available API. The callback names below are illustrative,
non-normative pseudocode.

## Current normative baseline

Wirelog currently evaluates a signed-integer Z-set/differential algebra, not a
general semiring. An inserted EDB fact has multiplicity `+1`, a retraction has
`-1`, duplicate derivations are combined by addition, and a join extends
multiplicities by multiplication. A tuple whose consolidated multiplicity is
zero is absent. Deltas carry signed changes to this same state.

This baseline preserves deterministic replay, incremental retraction, and the
current optimizer equivalence tests. Antijoin/negation and aggregates retain
their current deterministic relational and weighted-Z-set meanings; this
proposal does not reinterpret either as probabilistic complement or
probabilistic aggregation.

## Problem and boundary with scalar addons

Issue #912 concerns row-local scalar functions. Such functions can calculate
`prob.mul(p, q)` or a threshold over ordinary columns, but cannot decide how
duplicate proofs combine, how recursive fixpoints converge, how retractions
undo a proof, how negation behaves, or how optimizer rewrites remain legal.
Those decisions belong to the evaluation algebra. A scalar addon therefore
does not implement this proposal.

Weights would eventually need an explicit attachment policy. The default
candidate is a weight on each EDB and IDB tuple, with each rule derivation and
delta carrying the resulting weight; a rule declaration or a derivation-only
weight must not be inferred implicitly. The policy for rule-local annotations
is deferred until a prototype demonstrates that it is necessary.

## Options and decision

Three approaches were considered:

1. **Generic weight-algebra addon.** This keeps one evaluator but makes every
   consolidation, join, delta, and optimizer law conditional on a callback
   descriptor. It offers reuse, but creates a large public ABI and makes
   unsupported laws easy to violate.
2. **Separate weighted backend.** A backend owns its representation and
   evaluation rules behind the existing backend boundary. This isolates
   probability, provenance, and approximation choices and lets the current
   Z-set backend remain unchanged, at the cost of duplicated execution work.
3. **External higher-layer inference.** Keep Wirelog as a deterministic
   relational engine and calculate probabilities/provenance outside it. This
   has the smallest ABI risk, but cannot provide algebra-aware incremental
   recursion inside the engine.

The recommended direction is **a separate weighted backend prototype first**,
followed by a decision gate. The prototype must demonstrate retraction,
recursive convergence, and optimizer equivalence before any generic callback
is standardized. If those properties cannot be demonstrated without exposing
backend internals, external higher-layer inference is preferred. A common
public addon ABI is not approved by this issue.

## Algebra and retraction contract

An arbitrary semiring is not enough for differential updates: most semirings
have no additive inverse. A future implementation must choose one of these
explicit modes before accepting deltas:

- an additive-inverse algebra, where retraction invokes the inverse operation;
- replacement/recomputation, where affected state is rebuilt atomically from
  surviving inputs; or
- provenance/derivation tracking, where each proof is retained sufficiently
  to remove only the retracted contribution.

Inverse-free callbacks must never silently treat a negative delta as a normal
weight. A failed inverse, recomputation, or provenance update aborts the whole
logical update, leaves the prior snapshot unchanged, and reports an error.
Partial callback output is discarded. The prototype must state whether EDB
weights, IDB weights, derivation weights, and deltas use the same representation
or explicit conversion operations.

Multiple proofs and shared evidence are not interchangeable: independence,
noisy-or, provenance, and simple addition produce different answers. The
backend must declare its proof-combination policy; no independence assumption
is implicit. Recursive strata require a deterministic convergence policy and a
bounded resource/failure rule. Negation remains the current deterministic antijoin
unless a future design supplies a separate closed-world/uncertainty
contract. Aggregates require an algebra-specific definition and must not reuse
the current aggregate merely because the value is numeric.

## Candidate future ABI (non-normative)

Only after the prototype and decision gate succeed could a separate namespace,
for example `WIRELOG_WEIGHT_ABI_VERSION`, be considered. It must not reuse
`WIRELOG_IO_ABI_VERSION`. A candidate descriptor would need to specify all of
the following before publication:

- an opaque context, immutable descriptor identity, lifecycle, and unload
  pinning; callbacks cannot outlive the registered descriptor;
- a stable weight representation, size, alignment, construction/destruction,
  ownership, allocator, and copy rules;
- `zero`, `one`, equality, and zero-test operations;
- addition/consolidation and multiplication/join-extension operations;
- retraction via inverse, recomputation, or provenance mode;
- a versioned, deterministic serialization format for snapshots and deltas;
- invalid values, overflow, underflow, NaN, infinity, precision, and exact vs
  approximate behavior;
- error codes, atomic failure behavior, callback reentrancy, and whether
  callbacks may allocate or call Wirelog;
- worker concurrency guarantees and required synchronization;
- deterministic replay requirements and a capability declaration for every
  optimizer law used by a rewrite.

Illustrative pseudocode such as `wirelog_weight_add_fn` is not a public API,
does not reserve names, and must not be copied into installed headers. If a
public header is eventually approved, its types/functions must use the
`wirelog_` prefix, macros/enums the `WIRELOG_` prefix, and new declarations
`WIRELOG_API`. The header must be added through `wirelog_public_headers`, the
ABI symbol manifest must be reviewed, and the version/deprecation policy must
be updated in the same change.

## Optimizer and execution legality

The backend must publish the algebraic laws it actually satisfies: associativity
and commutativity of addition, associativity/distributivity of multiplication,
identity and zero laws, idempotence if any, monotonicity, and determinism.
Optimizer rewrites that rely on an unavailable law are disabled, not guessed.
In particular, join reordering, duplicate elimination, aggregation pushdown,
magic-set transformations, fusion, and antijoin rewrites each need an explicit
capability check. Approximate or floating-point arithmetic must not claim
equivalence when rounding/order changes can alter results. The prototype must
compare each worker width and replay order before claiming equivalence.

## Decision gate and non-goals

The next implementation gate is an isolated backend prototype with executable
tests for duplicate proofs, shared evidence policy, insert/retract cycles,
recursive convergence, failure atomicity, negation, aggregates, and optimizer
legality. It must compare deterministic results across worker widths and
replay order. Only then may maintainers decide whether a public addon is worth
the ABI cost.

This issue adds no probability syntax, probability builtin, scalar addon,
public weight header, installed symbol, dependency on GEOS/PROJ/GDAL, core
probabilistic semantics, or compatibility promise. No probabilistic semantics
or public weight API is delivered by this issue. Existing signed Z-set tests
and optimizer-equivalence tests remain authoritative and are unchanged.
