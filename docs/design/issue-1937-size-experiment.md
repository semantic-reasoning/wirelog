# Issue #1937 size experiment plan

This document records the exact-profile experiments for the cumulative
`.text` size cost tracked by [issue #1937](https://github.com/semantic-reasoning/wirelog/issues/1937).
No production change is accepted without a useful reduction measured against
the fixed reference below.

## Reviewed reference

- Main/base SHA: `9887ace362dfd77c5dc9e7e370498569f84c6b3d`
- Fixed reference SHA: `e031010f27b01bbb54fe02988c0e03ca6e4e88ef`
- Initial trusted report: [workflow run 36123181728](https://github.com/semantic-reasoning/wirelog/actions/runs/36123181728)
- Exact-profile `.text`: base and reference both measured **382,829 bytes**;
  base/head delta **0 bytes**.
- The report confirms matching profile and policy-file hashes, binary and
  repository identity, current `main`, PR base/head, and successful Ubuntu
  24.04 preflight.
- LTO's temporary partition names and missing final-binary DWARF make
  per-source byte attribution unavailable. Retained symbols identify
  `wl_columnar_join_op` and `wl_columnar_join_diff_op` as code-inspection
  starting points, not as additive source-byte evidence.

## Rejected experiment: cached right-filter factoring

The experiment in candidate `0f4ff44fb19a8f99f34793232e49b187a60e14bf`
factored the duplicated cached/governed right-filter setup. The trusted
fixed-reference report [run 36128368885](https://github.com/semantic-reasoning/wirelog/actions/runs/36128368885)
measured base/reference at **382,829 bytes** and candidate at **382,834 bytes**
(**+5 bytes**). The profile fingerprints and policy-file hashes matched, and
all source identities were freshly verified. This experiment is rejected for
size acceptance and its production refactor is not retained.

For diagnostic review only, retained-symbol output changed `wl_columnar_join_op`
from 12,831 to 12,898 bytes, `wl_columnar_join_diff_op` from 10,710 to 10,430
bytes, and added the 1,946-byte `wl_columnar_join_filter_right` symbol. LTO
source mapping remained unavailable, so those per-symbol changes do not
explain additive source-level savings or the whole-library result.

## Current bounded experiment

Factor only the duplicated right-delta and retraction selection shared by
ordinary and differential JOIN. Return the selected right relation, whether a
delta was selected, and whether FORCE_DELTA requires an empty output. Keep
empty-result allocation and schema copying, stack ownership, filtering,
cleanup, and publication in their callers. Preserve FORCE_DELTA behavior for
absent versus empty deltas, seeded iteration-zero fallback, AUTO's size/subpass
and outbound-only rules, retraction fallback only when `$d$` is absent, and the
retraction-right-pass override including FORCE_FULL.

Keep `e031010f27b01bbb54fe02988c0e03ca6e4e88ef` as the fixed `reference_sha`
for every reduction measurement in this series. Record the exact current
base/head, workflow run, and report artifact each time. Do not accept the
refactor unless the trusted exact-profile comparison demonstrates a useful
`.text` reduction and the applicable size gate passes. If it does not reduce
size, discard the production experiment rather than changing the budget or
baseline.

For any accepted production change, preserve admission accounting, typed
denial, rollback behavior, and existing tests. Run focused behavior and
failure-path tests plus applicable sanitizer, ABI, and performance validation.

The separately reviewed, main-owned allowlist authorizes measurement of this
candidate PR only. It does not authorize a baseline increase, policy change, or
weakened governor preflight.
