# DOOP performance-nightly contract

The scheduled `perf-nightly` workflow has two different responsibilities:

* `perf-stable` is the required execution and correctness lane. It runs on
  `ubuntu-latest`, executes the pinned DOOP workload with `W=8` and
  `repeat=5`, and fails for missing data, an oracle or manifest mismatch,
  incorrect tuple/iteration sentinels, an incomplete result, or a non-zero
  benchmark exit.
* A provisioned runner may additionally use
  `WIRELOG_DOOP_PERF_MODE=strict-stable` to enforce cpufreq and a calibrated
  five-repetition median target. That mode is not used by the hosted lane,
  because a GitHub-hosted runner does not provide stable CPU-frequency or
  affinity guarantees.

Hosted timing is therefore recorded as `timing=advisory` in the DOOP result
and in the retained `perf-doop-evidence` artifact. A configured
`WL_DOOP_PERF_GATE_TARGET_MS` is recorded for comparison but is not treated as
a stable-runner regression threshold in hosted mode. The artifact retains the
candidate commit, workflow run, runner image, permitted CPU affinity, governor
observation, oracle row, facts manifest, raw Meson log, and the requested
`workers=8` / `repeat=5` contract.

The correctness oracle is the `doop` row in
`scripts/release/downstream-matrix-oracles.tsv`. Its tuple count, iteration
count, and dataset manifest are independent of timing calibration. A new
strict-stable target must be produced by an independent five-repetition
calibration and must include its host, compiler, commit, dataset manifest,
median, and margin; the README's single-run number is not a calibration.
