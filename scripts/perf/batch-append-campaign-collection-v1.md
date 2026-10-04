# Batch append campaign collection admission v1

`collect-batch-append-campaign.py` admits a previously frozen command artifact
and creates a durable, empty journal directory. Admission is the default and
does not launch benchmark processes. The supplied `command-freeze.json` is
revalidated against the current plans, profiles, calibration evidence, overlay,
source/build trees, binaries, and command schedule; its bytes must match the reconstructed frozen
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

After reviewing the admission and frozen schedule, run the separate explicit
execution mode by passing the same inputs and `--run`. It accepts only the
fresh ready collection, creates a durable one-time run lock, and executes the
frozen commands in order. It does not resume or retry. Before each launch it
revalidates inputs and writes/fsyncs the complete `started` journal record;
after each process it stores raw stdout/stderr, hashes them, records host
telemetry and strict benchmark-v2 output validation, then fsyncs a result
record. Process, output, host-identity, or provenance failures leave a typed
`incomplete_capture` status. Telemetry-ineligible rows are retained and the
schedule continues. Only a complete schedule receives `complete_capture`.
Neither state contains a performance verdict or ratio.

The frozen per-command `HOME`, `TMPDIR`, argv, cwd, and environment are used
unchanged. SIGINT, SIGTERM, and SIGHUP trigger process-group cleanup; the
result and incomplete status are persisted before the command returns.
