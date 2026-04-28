# Stress Harness Baseline Protocol (Issue #594)

This document covers the K-Fusion arena stress harness that
ships in `tests/test_stress_harness.c`: how it is wired into CI,
how to run the release-tier (W=8) variant locally, and the flake-
baseline protocol the repo uses to triage intermittent failures.

## CI tier topology

The same `test_stress_harness` binary is registered twenty times in
`tests/meson.build` with different `env:` blocks and meson `suite:`
tags so a single configurable harness backs three CI tiers
without duplicating code.  (Eighteen in the table below plus
`stress_harness_nested_asan` and `stress_harness_daemon_asan` under
the `asan` suite -- omitted from this table because they ship
separately from the W/R/strategy axes.)

| Test name | Suite | Workload | W | R | Where it runs |
|---|---|---|---|---|---|
| `stress_harness_w2` | (default) | freeze-cycle | 2 | 200 | every PR (`ci-pr.yml` -> `meson test`) |
| `stress_harness_apply_roundtrip_pr` | (default) | apply-roundtrip | 2 | 500 | every PR |
| `stress_harness_rotation_pr` | (default) | rotation-vtable (standard) | 2 | 50 | every PR |
| `stress_harness_rotation_pr_mvcc` | (default) | rotation-vtable (mvcc) | 2 | 50 | every PR |
| `stress_harness_daemon_pr` | (default) | daemon-soak (standard) | 2 | 100 | every PR |
| `stress_harness_daemon_pr_mvcc` | (default) | daemon-soak (mvcc) | 2 | 100 | every PR |
| `stress_harness_w4` | `stress-nightly` | freeze-cycle | 4 | 500 | nightly (`perf-nightly.yml`, 04:00 UTC) |
| `stress_harness_apply_roundtrip_nightly` | `stress-nightly` | apply-roundtrip | 4 | 5000 | nightly |
| `stress_harness_rotation_nightly` | `stress-nightly` | rotation-vtable (standard) | 4 | 500 | nightly |
| `stress_harness_rotation_nightly_mvcc` | `stress-nightly` | rotation-vtable (mvcc) | 4 | 500 | nightly |
| `stress_harness_daemon_nightly` | `stress-nightly` | daemon-soak (standard) | 4 | 2000 | nightly |
| `stress_harness_daemon_nightly_mvcc` | `stress-nightly` | daemon-soak (mvcc) | 4 | 2000 | nightly |
| `stress_harness_w8` | `stress-release` | freeze-cycle | 8 | 1000 | release-tier (manual, see below) |
| `stress_harness_apply_roundtrip_release` | `stress-release` | apply-roundtrip | 8 | 50000 | release-tier (manual) |
| `stress_harness_rotation_release` | `stress-release` | rotation-vtable (standard) | 8 | 1000 | release-tier (manual) |
| `stress_harness_rotation_release_mvcc` | `stress-release` | rotation-vtable (mvcc) | 8 | 1000 | release-tier (manual) |
| `stress_harness_daemon_release` | `stress-release` | daemon-soak (standard) | 8 | 10000 | release-tier (manual) |
| `stress_harness_daemon_release_mvcc` | `stress-release` | daemon-soak (mvcc) | 8 | 10000 | release-tier (manual) |

The PR tier runs in the default suite, picked up by every
`meson test -C build` invocation in `ci-pr.yml`/`ci-main.yml`. The
nightly tier runs under TSan in `perf-nightly.yml`'s `stress` job.
The release tier has no GitHub workflow today; release engineers
invoke it manually before tagging.

## Workloads

### `freeze-cycle`

Verbatim lift of #582's freeze-cycle stress. Each cycle:

1. coordinator alloc()s a sentinel handle on `coord->compound_arena`,
2. coordinator freeze()s the arena,
3. submit `W` workers via `wl_workqueue_submit`; each worker does
   `wl_compound_arena_lookup` (must succeed) plus
   `wl_compound_arena_alloc` (must refuse while frozen),
4. wait_all,
5. coordinator unfreeze + `gc_epoch_boundary` advances the epoch.

`R` is the cycle count and is bounded by the arena's 4096 epoch cap;
the harness rejects `WL_STRESS_R >= max_epochs` with a hard FAIL.

### `apply-roundtrip`

Pre-rotation handles must remain valid post-apply (#594 acceptance
bullet). Sequence:

1. allocate `R` rows, each carrying a non-zero handle in column 0,
2. build a deterministic remap: `new_handle = old_handle XOR
   0xDEADBEEFCAFEBABE`,
3. call `wl_handle_remap_apply_columns` (#589) to rewrite the
   column in place,
4. assert every cell carries the expected post-apply handle.

`W` is accepted but ignored: `wl_handle_remap_apply_columns` is
single-mutator by contract; concurrent appliers would corrupt each
other's prefix. The W parameter stays in the harness signature for
CI-tier wiring uniformity. `R` is row count, only bounded by
available memory; release-tier exercises 50000 rows.

### `rotation-vtable`

Drives the #600 rotation strategy vtable
(`sess->rotation_ops->rotate_eval_arena` and
`sess->rotation_ops->gc_epoch_boundary`) under W/R stress. The
freeze-cycle and apply-roundtrip workloads call rotation/GC
primitives DIRECTLY (`wl_arena_reset`,
`wl_compound_arena_gc_epoch_boundary`); none exercise the indirect
function-pointer dispatch #600 introduced. This workload closes that
gap.

Per cycle:

1. allocate `K=64` handles in the compound arena's current epoch
   (oracle slice),
2. call `sess->rotation_ops->rotate_eval_arena(sess)` (vtable hook 1),
3. assert all `K` handles still resolve via
   `wl_compound_arena_lookup` with size-equality (the "all
   pre-rotation handles valid" acceptance bullet),
4. call `sess->rotation_ops->gc_epoch_boundary(sess)` (vtable hook 2);
   this closes the current epoch and the just-validated handles
   become unreachable -- by design (`compound_arena.c:332-344`).

The validity check sits between the two vtable calls because
`gc_epoch_boundary` clears the closed generation; that is GC behavior
owned by the arena, not the #596 vtable contract. Step (3) asserts
that `rotate_eval_arena` does not invalidate compound handles.

`W` is accepted but ignored (single-mutator: rotation hooks walk the
eval arena's bump pointer and the compound arena's per-epoch
generation table, neither concurrency-safe). The harness uses a
mock session (mirrors `nested-asan`'s `make_mock_session`) -- the
rotation hooks only touch `sess->eval_arena` and
`sess->compound_arena`, so a `calloc`'d `wl_col_session_t` with those
two fields plus `rotation_ops` is sufficient. No
parser/optimizer/plan link cost.

#### Strategy axis: `WIRELOG_ROTATION`

The rotation-vtable workload is the only one that honors the
`WIRELOG_ROTATION` environment variable, parsed at workload start
(unknown values hard-FAIL):

- `WIRELOG_ROTATION=standard` (default): selects
  `col_rotation_standard_ops`. Behavior matches the pre-#600 direct
  calls.
- `WIRELOG_ROTATION=mvcc`: selects `col_rotation_mvcc_ops`. The MVCC
  vtable is a placeholder today (per `rotation_mvcc.c`); behavior is
  identical to `standard` until the real MVCC implementation lands.

Both variants are registered at every CI tier because #596's contract
is the dispatch path itself, not the strategy semantics: the MVCC
placeholder must remain reachable through the function pointer under
stress. Coverage delta vs `tests/test_rotation_strategy.c` (#600's
correctness test): that file functional-tests selection
(default-is-standard, env-override-mvcc) and runs ONE rotate+gc
dispatch on a live session with no churn. The rotation-vtable
workload exercises the dispatch under R cycles of pre-rotation alloc
fan-out and per-handle post-rotate validity oracle.

`R` cap is 1500 (mirrors `WL_NESTED_ASAN_R_CAP`); higher values
hard-FAIL with a parseable diagnostic to keep epoch headroom under
the compound arena's 4096-epoch ceiling.

#### Baseline pass rate

100/100 PR-tier rotation-vtable runs pass on the baseline machine
(`stress_harness_rotation_pr` + `stress_harness_rotation_pr_mvcc`
combined). Healthy floor: any single CI failure is a real regression,
not a flake.

### `daemon-soak`

Long-running soak that proves bounded-RSS behavior under saturation-
driven rotation cadence and cumulative handle survival across many
`gc_epoch_boundary` calls within a rotation window. Differs from
`rotation-vtable` (#596) on three axes:

1. **Cadence is saturation-driven**, not loop-bounded `R`. When the
   compound arena's `current_epoch + 8 >= max_epochs` the workload
   calls `sess->rotation_ops->gc_epoch_boundary(sess)` explicitly to
   advance the epoch ahead of allocations. On absolute saturation
   (`current_epoch == max_epochs`, the sentinel from
   `compound_arena.c:351`) the arena is destroyed and recreated -- a
   deliberate scope discontinuity that resets the cycle-window
   survival oracle. The workload uses a tight `max_epochs=32`
   ceiling so the predicate fires every ~24 steps, which forces the
   soak's working-set ceiling to plateau early so VmHWM stays
   bounded across the run regardless of `R`.
2. **Per-step handle fan-out `K=1000`** (vs `rotation-vtable`'s
   `K=64`) so each step produces meaningful allocator pressure.
   `K` is a constant, not parameterized.
3. **Cumulative-survival oracle** every 100th step walks the LAST
   `WL_DAEMON_WINDOW=8000` handles (~8 epochs of allocations within
   a single arena lifetime) to exercise the `wl_compound_arena_lookup`
   path under a long soak. Lookups against closed-epoch handles are
   tolerated to return `NULL` by design (`compound_arena.c:332-344`
   zeroes the closed generation); the survival contract asserts
   crash-free behavior plus size-equality on non-`NULL` resolutions.
   ASan / TSan tiers add the orthogonal memory-safety axis.

`WL_STRESS_R` for this workload is the **step count**, not cycle
count. Hard cap is 10000 steps (issue spec); `WL_STRESS_R=10001`
hard-fails with a parseable diagnostic. `WL_STRESS_W` is accepted
but ignored: rotation hooks are single-mutator by contract.

Per step:

1. Saturation predicate: if `current_epoch + 8 >= max_epochs`,
   advance via `gc_epoch_boundary`; if the result is the
   `current_epoch == max_epochs` sentinel, destroy + recreate the
   arena and reset the survival oracle.
2. Allocate `K=1000` handles in `sess->compound_arena` via
   `wl_compound_arena_alloc(arena, 24)`. Record each in the
   window-local oracle.
3. Call `sess->rotation_ops->rotate_eval_arena(sess)` (vtable
   hook 1, must NOT touch `compound_arena` per the #596 contract).
4. Mid-rotation oracle: every just-allocated handle resolves via
   `wl_compound_arena_lookup` with size-equality, plus
   `wl_compound_arena_multiplicity == 1` (initial-alloc
   multiplicity per `compound_arena.h`).
5. Cumulative-survival check (every 100th step): walk last
   `min(WINDOW, oracle_n)` handles. Skipped when oracle was just
   reset by arena recreation.
6. Call `sess->rotation_ops->gc_epoch_boundary(sess)` (vtable
   hook 2) to close the epoch.

#### RSS gate

End-of-run RSS gate proves no leak by bounding the peak resident
working set growth. The cross-platform sampler lives in
`tests/test_rss_util.h` (mirrors `bench/bench_util.h:107-149`):

- **Linux**: `/proc/self/status` `VmHWM` parser, returns KB.
- **macOS**: `task_info` with `MACH_TASK_BASIC_INFO`, returns KB.
- **Windows**: `GetProcessMemoryInfo.PeakWorkingSetSize`, returns KB.
- Returns `-1` on platform sampler unavailability.

`getrusage(RUSAGE_SELF).ru_maxrss` is intentionally NOT used here
because of its KB-on-Linux / bytes-on-macOS divergence (a known
foot-gun).

The gate samples `rss_baseline_kb` immediately after mock-session
construction (BEFORE the first step) and `rss_final_kb` after the
last step's `gc_epoch_boundary` (BEFORE cleanup, so the heap has
not yet been deallocated). Gate formula:

```
rss_final_kb <= rss_baseline_kb + max(rss_baseline_kb / 10, 16384)
```

10% growth or 16 MB absolute floor (whichever larger). The floor
is the binding constraint for typical CI baselines (1-30 MB); the
percentage kicks in at very large baselines (>160 MB). On gate
failure the diagnostic prints baseline / final / delta / gate values
so a reviewer can read off the leak signal directly.

**Platform behavior:**

- Linux / macOS native: gate enforced.
- Windows or Linux containers without `/proc`: sampler returns -1;
  the gate is skipped with a `[rss-gate skipped: platform sampler
  unavailable]` diagnostic and the run continues.
- AddressSanitizer instrumentation: gate skipped at compile time
  (via `__SANITIZE_ADDRESS__` / `__has_feature(address_sanitizer)`)
  with a `[rss-gate skipped: AddressSanitizer instrumentation
  confounds VmHWM signal]` diagnostic. ASan's redzones, shadow
  memory, and quarantine freelist prevent the OS from reclaiming
  pages back below VmHWM after `free()`, so the no-leak signal is
  confounded under ASan; the ASan tier still validates the workload
  functionally and surfaces actual heap-buffer-overflow / use-after-
  free / leak via ASan's own reporting.

#### Baseline pass rate

100/100 PR-tier daemon-soak runs pass on the baseline machine
(`stress_harness_daemon_pr` + `stress_harness_daemon_pr_mvcc`
combined). Healthy floor: any single CI failure is a real
regression, not a flake.

## Running release-tier locally

There is no `release.yml` workflow yet (deferred follow-up). To
gate a release manually:

```bash
meson setup build-stress-tsan -Db_sanitize=thread -Db_lundef=false \
    -Dthreads=posix -Dtests=true --buildtype=debug
meson compile -C build-stress-tsan
TSAN_OPTIONS='halt_on_error=1' \
    meson test -C build-stress-tsan --suite stress-release \
    --print-errorlogs --num-processes 1
```

All six release-tier entries must pass:
`stress_harness_w8` (freeze-cycle, 1000 cycles),
`stress_harness_apply_roundtrip_release` (apply-roundtrip, 50000
rows), the two `stress_harness_rotation_release{,_mvcc}` entries
(rotation-vtable, 1000 cycles each, both strategy variants), and
the two `stress_harness_daemon_release{,_mvcc}` entries (daemon-
soak, 10000 steps each, both strategy variants). Wall time is sub-
minute per entry on a typical x86_64 laptop under TSan; the test()
entries set `timeout: 300-1800` as headroom (the daemon-soak
release tier sets 1800s to accommodate 10000 steps under TSan
slowdown).

## Flake baseline protocol

A "flake" is a TSan-clean test that fails non-deterministically
under stress instrumentation. The baseline distinguishes flakes
from real regressions.

### Capturing a baseline

After a stress test lands or its parameters change, capture a
baseline pass-rate over many runs to learn the natural failure
floor. From a clean working tree:

```bash
# 100-run baseline for the PR tier (cheap, sub-minute total).
N=100
fails=0
for i in $(seq 1 "$N"); do
    meson test -C build stress_harness_w2 \
        stress_harness_apply_roundtrip_pr >/dev/null 2>&1 \
        || fails=$((fails + 1))
done
echo "PR-tier baseline: $((N - fails)) / $N pass"
```

Replace the test-name list with `--suite stress-nightly` or
`--suite stress-release` for the heavier tiers.

A 100-run pass rate of:

- **100 / 100** -> healthy. Any single failure in CI is a real
  regression, not a flake.
- **99 / 100 - 95 / 100** -> the test has a tolerable flake floor.
  Document the rate inline at the top of the workload function in
  `test_stress_harness.c` and treat single CI failures as flakes
  for retry, but trigger investigation if two consecutive nightly
  runs fail.
- **< 95 / 100** -> the test is too flaky to gate on. Either
  reduce R (lower confidence) or fix the underlying race before
  re-enabling the gate.

### Triaging a CI failure against the baseline

When a CI run reports `stress_harness_*` FAIL:

1. Re-run the failed job once. Pass on retry + healthy baseline =
   flake; close-as-flaky.
2. Persistent failure across two consecutive runs OR a healthy
   baseline (100/100) PROVES it's a regression. Open a P1 issue
   referencing the SHA, attach `meson-logs/testlog.txt` and the
   relevant TSan/ASAN diagnostic, and bisect.

### Re-baselining

Re-run the baseline capture whenever:

- The harness's `R` defaults change.
- A new workload is added.
- A worker_fn or apply pass implementation changes (the upstream
  primitives in `wl_handle_remap_apply_columns`,
  `wl_compound_arena_lookup`, etc.).
- TSan / sanitizer toolchain version changes in CI image.

Record the new baseline pass-rate in the file header docstring of
`test_stress_harness.c` so future on-callers do not have to
re-derive it.

## Per-rotation RSS-bounded gate (Issue #598)

`tests/test_rss_bounded.c` is the residual-scope companion to #597's
end-of-run daemon-soak gate. It exercises three signals NOT covered
by the daemon-soak workload:

1. **Per-rotation sampling granularity**, not just end-of-run. The
   sampler is `test_current_rss_kb()` (Linux `/proc/self/status:VmRSS`,
   macOS `task_info` `resident_size`, Windows
   `PROCESS_MEMORY_COUNTERS.WorkingSetSize`), which returns the
   CURRENT working-set size. The companion `test_peak_rss_kb()` (used
   by daemon-soak) returns the monotonic VmHWM peak and is unfit for
   per-rotation deltas because `VmHWM(N+1) - VmHWM(N) >= 0` always,
   independent of memory pressure. Per-rotation gates require VmRSS;
   end-of-run gates use VmHWM.
2. **Cumulative cap with no 4 MB floor.** The PR-tier gate is
   `rss_after <= rss_warmup + max(rss_warmup / 10, 1024 KB)` -- 10
   percent growth or a 1 MB absolute floor (whichever larger),
   strictly tighter than daemon-soak's `max(10 percent, 4 MB)`. The
   wider 4 MB floor is appropriate for an end-of-run signal at small
   baselines (~2 MB) where 10 percent would be too tight; the
   per-rotation gate is the canary for incremental drift and benefits
   from the tighter floor.
3. **Per-rotation `mem_ledger.current_bytes` snapshot.** Production
   rotation hooks (`rotation_standard.c`) only call
   `wl_arena_reset`; they do NOT mutate `wl_mem_ledger`. The gate
   asserts `ledger.current_bytes` is EXACTLY equal before and after
   each rotation -- production rotation does not allocate via the
   ledger; any change is a regression. Today the assertion is
   tautological (catches future regressions that introduce ledger
   churn into the rotation hot path).

The strict per-rotation delta gate
`rss_after - rss_prev <= rss_prev / 10` (skip when delta < 256 KB
page-size noise floor) is opt-in via `WL_RSS_BOUNDED_STRICT=1` and
runs only in the nightly/release tiers. Localhost machines that fail
the warmup CoV self-check (>0.05) exit 77 (meson SKIP); the pattern
mirrors `test_log_perf_gate.c`.

The workload uses a saturation-driven arena recycle pattern
(max_epochs=32, epoch_buffer=8) so the working set plateaus at the
arena ceiling, mirroring daemon-soak. Without recycle, R=1000 with
K=64 handles per epoch would accumulate legitimate per-epoch
generation overhead and trip the cumulative gate.

### CI tier topology

| Test name | Suite | R | Strict | Where it runs |
|---|---|---|---|---|
| `rss_bounded_pr` | (default) | 100 | off | every PR |
| `rss_bounded_nightly` | `stress-nightly` | 500 | on | nightly |
| `rss_bounded_release` | `stress-release` | 1000 | on | release-tier (manual) |
| `rss_bounded_asan` | `asan` | 100 | off | every PR (build-san) |

ASan tier compile-time skips the RSS portion (ASan instrumentation
confounds VmRSS the same way it confounds VmHWM -- redzones, shadow
memory, and quarantine prevent reclaim). Ledger and rotation
assertions still run to surface UAF/heap-overflow on the rotation
hot path via ASan's own reporting.

### Baseline pass rate

100/100 PR-tier `rss_bounded_pr` runs pass on the baseline machine.
Healthy floor: any single CI failure is a real regression, not a
flake.

## deep_copy ledger-charge canary (Issue #598)

`tests/test_deep_copy_ledger_canary.c` is the destroy-side companion
to `test_col_rel_deep_copy.c::test_deep_copy_mem_ledger_null`
(Issue #554). The existing test already pins:

1. `dst->mem_ledger == NULL` after `col_rel_deep_copy`.
2. Source ledger counters are unchanged by the deep copy itself.

The canary adds the residual #598 acceptance bullet:

- `col_rel_destroy(dst)` does NOT double-charge the source session's
  ledger. Because `dst->mem_ledger` is NULL,
  `col_rel_free_contents` must not accidentally consult
  `src->mem_ledger` when it walks dst's columns.

**Vacuous in production today**: `col_rel_deep_copy` has NO
production callers in rotation paths today (verified by grep across
`wirelog/`). Only `tests/test_col_rel_deep_copy.c` and
`tests/col_rel_deep_copy_fixture.c` invoke it. The canary becomes
load-bearing when #550-C `wl_session_rotate` ships: the cross-arena
rotation helper will deep-copy live relations into the new arena,
and any ledger churn would corrupt per-session memory accounting.

Registered in the default suite as `deep_copy_ledger_canary`.

## Rotate-latency p50/p99/p999 gate (Issue #599)

Companion to #598's RSS-bounded gate. #598 asserts that the working
set is bounded across rotations; #599 asserts that each rotation is
fast. Both gate the same rotation_ops vtable
(`col_rotation_standard_ops`) on an identical workload (K=64
handles, 24-byte payload, `compound_arena`), so a divergence in
either lights up a real regression rather than a workload-shape
change.

### Recalibration: ms in the issue body, us in the gate

The original Issue #599 body specified targets of "p50 < 10ms,
p99 < 100ms, p999 < 500ms". Those numbers were transcription
errors by ~1000x: the rotation hot path is
`wl_arena_reset` (one bump-pointer reset) plus a per-epoch
generation table walk -- 30-50 nanoseconds per rotation in
steady state on x86_64. Gating at the literal millisecond
values would have allowed five orders of magnitude of slowdown
to slip through.

The shipped gate uses **microsecond budgets**:

| Percentile | Budget    | Observed (local Linux/x86_64) | Headroom |
|------------|-----------|-------------------------------|----------|
| p50        | 10 us     | 31-36 ns                      | ~280-320x |
| p99        | 100 us    | 37-62 ns                      | ~1600-2700x |
| p999       | 500 us    | 60-187 ns                     | ~2700-8300x |

The 100 us p99 budget is the load-bearing "1.5x regression gate":
it is intentionally wider than observed steady-state to absorb
dispatcher noise on shared CI runners and slow CI hardware,
without masking a real 1.5x regression on the rotation hot path.
Tighten if the rotation path is ever benchmarked on dedicated
perf hardware and a realistic budget is established.

### CI tier topology

| Test name | Suite | N | Strict (p999) | Strategy | Where it runs |
|---|---|---|---|---|---|
| `rotate_latency_pr` | `perf` | 1000 | off | standard | nightly only (perf-nightly.yml) |
| `rotate_latency_pr_mvcc` | `perf` | 1000 | off | mvcc | nightly only |
| `rotate_latency_nightly` | `perf` | 10000 | on | standard | nightly only |
| `rotate_latency_nightly_mvcc` | `perf` | 10000 | on | mvcc | nightly only |
| `rotate_latency_release` | `stress-release` | 100000 | on | standard | release-tier (manual) |
| `rotate_latency_release_mvcc` | `stress-release` | 100000 | on | mvcc | release-tier (manual) |

The `perf` suite is invoked only by `.github/workflows/perf-nightly.yml`
(via `meson test --suite perf`); `ci-pr.yml` does NOT pass `--suite perf`
so the rotate-latency gate does not run on per-PR CI. This mirrors
`test_log_perf_gate.c`'s nightly-only cadence: rotation latency is a
dedicated-hardware signal (cpufreq governor pre-check, opt-in env)
and shared per-PR runners cannot supply the stability budget. The
release-tier entries (`stress-release` suite) have no automation
today; release engineers invoke them manually.

All entries set `WIRELOG_PERF_GATE=1` in the meson env so the binary
opts in. Bare invocation (`./test_rotate_latency`) without the env
var exits 77 (SKIP).

Strategy axis: every tier registers both `standard` and `mvcc`
strategy variants (mirrors the `stress_harness_rotation_*` tier
layout). The MVCC vtable is a placeholder today (per
`rotation_mvcc.c`) and behaves identically to standard until the
real implementation lands; the variants exist now so the dispatch
path is gated under the latency budget at every tier the moment a
non-trivial MVCC implementation lands.

### Gate sensitivity

The original Issue #599 acceptance criterion was "p99 > 1.5x baseline
blocks merge". The shipped gate is an **absolute budget** (100 us p99)
chosen ~1600x above the observed steady-state p99 (37-62 ns). That
recalibration was deliberate but the headroom drifted from "1.5x" to
roughly three orders of magnitude during recalibration; the gate as
shipped is a **catastrophic-regression detector**, not a 1.5x
trend-tracker.

Concretely:

- A 2-10x regression on the rotation hot path (e.g. 30 ns -> 300 ns)
  passes this gate cleanly. The absolute p99 budget (100 us / 100 000 ns)
  has ~1600x headroom over steady-state, so the gate is silent across
  multiple orders of magnitude of incremental drift.
- A ~1000x regression (300 ns -> 30 us) is flagged. By that point the
  rotation hot path has lost most of its headroom over a parser parse
  call, which is the regression class the gate was specified to
  detect after recalibration.

Catching incremental 2-10x drift requires a separate trend-tracking
job that compares against a moving baseline rather than a fixed
absolute threshold. **Out of scope for #599** -- a sibling issue
should track that work, including baseline JSON schema, percentile
deviation thresholds, and CI integration. The current gate stays
honest: it gates catastrophic regressions and does NOT pretend to
catch trend drift.

### Workload shape

K=64 handles per cycle, 24-byte payload, `compound_arena` --
identical to #598's RSS-bounded gate so divergence between the
two is impossible without a deliberate edit. W=1 mandated:
rotation hooks walk the eval arena's bump pointer and the
per-epoch generation table, neither concurrency-safe (see the
"Rotation hooks" reasoning early in this doc).

### Steady-state vs recycle

The compound arena caps `max_epochs` at
`WL_COMPOUND_DEFAULT_MAX_EPOCHS` (4096; see
`wirelog/arena/compound_arena.c`). Larger N would otherwise hit
the saturation sentinel. The test sizes the arena to the platform
max and recycles (full destroy + recreate) **outside the timed
window** when `current_epoch >= max_epochs - 64`. Only
`rotate_eval_arena + gc_epoch_boundary` is timed, so the
measurement observes only the steady-state hot path; arena
recycle is a workload-shape event that #598's RSS-bounded gate
covers separately.

### Skip behavior

The gate exits 77 (Meson SKIP) on any of:

- **Sanitizer build** (compile-time): wall-clock signal is
  invalidated by ASan/TSan instrumentation.
- **`WIRELOG_PERF_GATE` not `1`**: opt-in gate; default-suite
  invocation (no env) is silent.
- **Linux: cpufreq governor != "performance"**: mirrors
  `test_log_perf_gate.c`. Set with
  `echo performance | sudo tee /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor`.
- **macOS / BSD / Windows**: no shipped stability design today --
  `wl_perf_stability_env_ok()` returns 0.
- **Steady-state CoV > 0.50**: jittery host. The CoV ceiling is
  wider than `test_rss_bounded.c`'s 0.05 because at ~30 ns per
  sample the measurement is within an order of magnitude of
  `clock_gettime` resolution; sub-ns jitter trivially yields
  25-40% CoV at this scale even on a quiet dedicated host.

### Failure UX

On gate breach the test prints, in addition to the always-printed
distribution line `[rotate-latency] N=... p50=... p95=... p99=...
p999=... max=... cov=... rotation=...`, the 5 slowest sample
positions from the un-sorted measurement buffer. Clustered indices
(consecutive) point at a cold-cache or recycle artifact;
scattered indices point at dispatcher jitter. There is no
baseline JSON file -- the gate is a hard absolute budget, not a
deviation-from-baseline test (#599 is the regression alarm; a
baseline-tracking test belongs in a separate issue if ever
needed).

## Out of scope (#594 follow-up tracking)

- **Release-tier CI workflow**: no `release.yml` exists yet. The
  W=8 invocation runs manually per the script above. Adding an
  automated release pipeline is a separate (follow-up) issue.
- **Cross-arena rotation workload**: this harness exercises the
  *in-place* apply pass (#589 / #590). Cross-arena rotation needs
  the #550 Option C `wl_session_rotate` helper, which has not
  landed; a sibling workload will be added when #550-C ships.
- **Existing hardcoded canaries**: `test_compound_arena_freeze_
  cycle_stress.c` (#582), `test_gc_freeze_alloc_race.c` (#584),
  and `test_worker_borrow_w2_tsan.c` (#592) are intentionally
  retained un-refactored. They serve as fixed-W canaries; the
  parameterizable harness is in addition to, not in place of,
  them.
