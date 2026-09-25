# Issue #1937 size experiment plan

This document reserves a reviewable candidate for investigating the cumulative
`.text` size cost tracked by [issue #1937](https://github.com/semantic-reasoning/wirelog/issues/1937).
It makes no production change and claims no size reduction. The existing
baseline and historical measurements are context only; the experiment must
collect fresh, exact-profile evidence before selecting or accepting a code
change.

## Measurement sequence

1. Rebase this candidate onto current `main` after the trusted attribution
   tooling change is merged. Record the exact base commit, candidate head,
   workflow run, and report artifact for every measurement.
2. Run the initial base/head attribution using the supported Ubuntu 24.04
   release profile. Use its object/source attribution to identify a bounded
   production-size opportunity. A failed preflight, profile mismatch, missing
   evidence, or incomplete report is not a measurement pass.
3. Before testing reductions, record the independently reviewed reference
   candidate and its exact head as the anchor for that measurement series.
   Compare each subsequent reduction against that fixed reference and the
   current exact base/head. If the experiment's scope changes, declare a new
   reviewed series and establish a new reference; do not silently move the
   anchor.
4. For any production change, preserve admission accounting, typed denial,
   rollback behavior, and existing tests. Run the focused behavior and
   failure-path tests plus the applicable sanitizer, ABI, and performance
   validation. Accept a change only with reproducible reports and passing
   required size and behavior gates.

The separately reviewed, main-owned allowlist authorizes measurement of this
candidate PR only. It does not authorize a baseline increase, policy change,
weakened governor preflight, or a claim that this documentation-only draft
reduces binary size.
