# CRDT Performance Baseline

**Last Updated:** 2026-07-31

This document records the correctness sentinel and wall-time provenance for
the `crdt_perf_gate` test. The gate evaluates the complete Kleppmann sequence
CRDT workload with one worker and measures a full-session snapshot.

## Correctness boundary

Issue #914 fixed iteration state leaking from recursive strata into later
non-recursive strata. The fix, commit `61e2530`, changed the work performed
and the rows observable through a full snapshot, so measurements from before
that commit are not comparable to this baseline.

The old `1,301,914` sentinel counted callback rows aggregated across every
relation in the snapshot. It was not the cardinality of the user-facing
`result` relation. The gate now asserts only the `result` cardinality and
prints the aggregate snapshot count as a diagnostic. The aggregate was
`2,156,530` in the calibration below, but it is deliberately not a correctness
contract.

The shipped fixtures prove the expected result cardinality:

- `Insert_input.csv` has 182,315 insertion records and SHA-256
  `9147f2b44d4cb291336da9aa6c39c27feac8630361f2b36875474e5c30602b05`.
- `Remove_input.csv` has 77,463 removal records and SHA-256
  `b83fba0709b37259cd6ad00bfe3434abc3df3fec3dbccd3f5eedf273e9c921dc`.
- All 182,315 insertion `(ctr, node)` keys are unique, all 77,463 removal
  keys are unique, and every removal key occurs in the insertion key set.
  The fixture therefore has `182,315 - 77,463 = 104,852` visible sequence
  elements. An independent traversal check confirms that the insertion graph
  forms one rooted traversal and all 104,852 visible elements participate in
  one visible sequence. The `result` relation represents adjacent
  visible-element pairs, so its cardinality is `104,852 - 1 = 104,851`.

### Subset fixture (`bench/data/crdt-small`)

The full fixture takes roughly 23 seconds to evaluate, and about eight times
that in the `-O0` sanitizer legs, so it is too slow to validate on every
`meson test`. `scripts/perf/make_crdt_subset.py` derives a smaller one:

- Breadth-first descendant closure of the single root, restricted to insert
  counters `<= 2000`, with removals restricted to the kept keys. Taking a
  closure rather than a prefix is what keeps the subset valid: every kept
  node's parent is kept, so the tree stays connected and single-rooted.
- 1,674 insertion records, SHA-256
  `51ded7b31b061460dc13ceb044865c5cdbb8f4915ce2aa8fd9d22d4665959409`.
- 1,412 removal records, SHA-256
  `2eef4586fa3a043d52e255f4d555289e890fef4dee49ac7f5ca0895c97813386`.
- Expected `result` cardinality `1,674 - 1,412 - 1 = 261`, confirmed by
  running it.

The `- 1` is the root, which has no predecessor. Note the derivation above
rests on an independent traversal check of the *shipped* fixture, which does
not transfer to a subset; for the subset it holds because
`result(c1, c2, v) :- nextVisible(c1, _, c2, n2), currentValue(c2, n2, v)`
projects away `prev_node` and `assign(ctr, n, ctr, n, n)` forces `v == n2`,
so each tuple is `(c1, c2, n2)`. In a chain every visible node has exactly
one predecessor, so distinct edges carry distinct `(c2, n2)` and the
projection cannot collide.

`tests/meson.build` registers this as `crdt_correctness_small` in the default
suite (Issue #947), so the result cardinality is checked on every test run
rather than only on a machine with a pinned cpufreq governor.

## Measurement environment

The 2026-07-31 calibration used:

- Linux `7.1.4-arch1-1`, x86-64.
- Intel Xeon E5-2696 v4 at 2.20 GHz.
- GCC `16.1.1 20260728`, Meson `1.11.2`, and Ninja `1.13.2`.
- Source base `623e8cf` (`origin/main`) plus the gate instrumentation in this
  change.
- Meson options `buildtype=release`, `optimization=s`, `b_lto=true`,
  `wirelog_log_max_level=error`, and `tests=true`.
- Native threading, one Wirelog worker, CPU affinity fixed to CPU 0 with
  `taskset -c 0`, and cpufreq policy 0 fixed to the `performance` governor.
- `WIRELOG_PERF_GATE=1`, `WIRELOG_PERF_REQUIRE=1`, and
  `WIRELOG_CRDT_DATA_DIR=<source>/bench/data/crdt`.
- The canonical Meson test runner injected `MALLOC_PERTURB_=12` for the
  calibration and `MALLOC_PERTURB_=110` for verification. These runs should
  not be compared directly with a hand-run benchmark that omits Meson's test
  environment.

The dedicated build and strict gate were run as follows:

```sh
source .venv/bin/activate
meson setup build-crdt-perf \
  -Dbuildtype=release \
  -Doptimization=s \
  -Db_lto=true \
  -Dwirelog_log_max_level=error \
  -Dtests=true
meson compile -C build-crdt-perf test_crdt_perf_gate
governor_path=/sys/devices/system/cpu/cpufreq/policy0/scaling_governor
old_governor=$(tr -d '\n' < "$governor_path")
restore_governor() {
  printf '%s\n' "$old_governor" | sudo tee "$governor_path" >/dev/null
}
trap restore_governor EXIT INT TERM
printf '%s\n' performance |
  sudo tee "$governor_path"
taskset -c 0 env \
  WIRELOG_PERF_GATE=1 \
  WIRELOG_PERF_REQUIRE=1 \
  meson test -C build-crdt-perf crdt_perf_gate --verbose
restore_governor
trap - EXIT INT TERM
```

The previous governor was `schedutil` and was restored immediately after each
measurement.

## Calibration and target

The final instrumentation recorded one untimed warm-up and nine timed full
snapshots:

- Warm-up: `36,280.7 ms`.
- Raw timed samples, in run order:
  `36,189.0`, `36,251.7`, `36,303.0`, `36,257.8`, `36,372.9`,
  `36,529.4`, `36,455.5`, `36,223.9`, `36,462.9` ms.
- Median: `36,303.0 ms`.
- Mean: `36,338.5 ms`.
- Population standard deviation: `114.30 ms`.
- Coefficient of variation: `0.315%`.

The target is a 5% envelope over the median, rounded up to the next 10 ms:

```text
ceil((36,303.0 * 1.05) / 10) * 10 = 38,120 ms
```

The test skips rather than gates when CoV exceeds 3%. A stable run fails when
its median exceeds `38,120 ms`.

## Strict target verification

After rebuilding with the final `38,120 ms` target, a second strict run under
the same governor and CPU-affinity conditions passed:

- Warm-up: `36,413.0 ms`.
- Raw timed samples, in run order:
  `36,200.0`, `36,181.0`, `36,310.8`, `36,464.0`, `36,506.9`,
  `36,483.7`, `36,378.1`, `36,248.0`, `36,374.4` ms.
- Median: `36,374.4 ms`.
- Mean: `36,349.7 ms`.
- Population standard deviation: `115.40 ms`.
- Coefficient of variation: `0.317%`.
- Snapshot aggregate: `2,156,530` (diagnostic only).
- Result cardinality: `104,851` (expected).
- Iterations: `14,148`.

The test exited successfully, and policy 0 was restored from `performance` to
`schedutil`.
