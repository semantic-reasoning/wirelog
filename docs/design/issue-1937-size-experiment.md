# Issue #1937 size experiment plan

This document records the exact-profile experiment for the cumulative `.text`
size cost tracked by [issue #1937](https://github.com/semantic-reasoning/wirelog/issues/1937).
It makes no size-reduction claim until a production candidate is measured
against the fixed reference below.

## Reviewed reference

- Main/base SHA: `9887ace362dfd77c5dc9e7e370498569f84c6b3d`
- Reviewed reference candidate SHA: `e031010f27b01bbb54fe02988c0e03ca6e4e88ef`
- Initial trusted report: [workflow run 36123181728](https://github.com/semantic-reasoning/wirelog/actions/runs/36123181728)
- Exact-profile `.text`: base and reference both measured **382,829 bytes**;
  base/head delta **0 bytes**.
- The report confirms matching profile, policy-file hashes, binary hashes,
  repository identity, current `main`, PR base/head, and successful Ubuntu
  24.04 preflight.
- LTO's temporary partition names and missing final-binary DWARF make
  per-source byte attribution unavailable. Retained symbols identify
  `wl_columnar_join_op` and `wl_columnar_join_diff_op` as useful code-inspection
  starting points, not as evidence of additive source-byte savings.

## Bounded production experiment

Factor only the duplicated cached/governed right-filter setup shared by
ordinary and differential JOIN in `wirelog/columnar/join.c`. Preserve cache
eligibility and ownership, session-governor precedence, fallback behavior,
and caller-owned checked cleanup on denial. Leave delta/retraction selection,
materialization caching, cache publication, leases, and differential
transactions unchanged.

Keep `e031010f27b01bbb54fe02988c0e03ca6e4e88ef` as the fixed
`reference_sha` for every reduction measurement in this series. Record the
exact current base/head, workflow run, and report artifact each time. Do not
accept the refactor unless the trusted exact-profile comparison demonstrates
a useful `.text` reduction and the applicable size gate passes. If it does
not reduce size, discard the production experiment rather than changing the
budget or baseline.

For any accepted production change, preserve admission accounting, typed
denial, rollback behavior, and existing tests. Run focused behavior and
failure-path tests plus applicable sanitizer, ABI, and performance validation.

The separately reviewed, main-owned allowlist authorizes measurement of this
candidate PR only. It does not authorize a baseline increase, policy change,
or weakened governor preflight.
