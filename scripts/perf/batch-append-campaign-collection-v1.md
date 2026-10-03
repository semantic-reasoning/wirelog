# Batch append campaign collection admission v1

`collect-batch-append-campaign.py` admits a previously frozen command artifact
and creates a durable, empty journal directory. It does not launch benchmark
processes. The supplied `command-freeze.json` is revalidated against the
current plans, profiles, calibration evidence, overlay, source/build trees,
binaries, and command schedule; its bytes must match the reconstructed frozen
artifact exactly. Command arguments and their preassigned `HOME` and `TMPDIR`
paths remain those in the freeze artifact.

The collection directory must be a new child of the command-freeze directory,
under `HOME`. It contains `collector-preflight.json`, `status.json`, and an
empty `journal.jsonl`. Publication stages and fsyncs these files, revalidates
the inputs before and after the atomic directory rename, and removes the
collection directory if either validation detects drift. The status remains
`ready` with `benchmark_launches_performed: 0`; no performance verdict is
produced.

The preflight records a current host snapshot and the selected frozen CPUs. It
checks host identity, sibling topology, and that every selected CPU remains
online and in the current affinity mask. Host telemetry is recorded only; it
does not qualify campaign samples.

Example comparison admission:

```sh
python scripts/perf/collect-batch-append-campaign.py \
  --mode comparison \
  --overlay-patch "$HOME/evidence/overlay.patch" \
  --freeze-artifact "$HOME/evidence/freeze/command-freeze.json" \
  --plan-a "$HOME/evidence/plan-a/campaign-plan.json" \
  --profile-a "$HOME/evidence/profile-a/execution-preflight.json" \
  --calibration-a "$HOME/evidence/calibration" \
  --plan-b "$HOME/evidence/plan-b/campaign-plan.json" \
  --profile-b "$HOME/evidence/profile-b/execution-preflight.json" \
  --output-dir "$HOME/evidence/freeze/collection"
```

Use `--mode aa_control`, `--plan`, `--profile`, `--calibration`,
`--calibration-origin-plan`, and `--calibration-origin-profile` for the A/A
control form.
