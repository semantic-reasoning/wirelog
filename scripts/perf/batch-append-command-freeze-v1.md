# Batch append command freeze v1

`freeze-batch-append-execution.py` writes a provenance-bound command artifact
without running benchmarks. Comparison mode consumes two plan-v3 files,
their profile-v2 artifacts, and one completed calibration bound to plan/profile
A. A/A mode
consumes one plan/profile/calibration set and accepts only explicit
`aa_control` product `pre`.

Each profile is recomputed against its plan, source checkouts, staged overlay,
Meson metadata, no-pending-rebuild state, and both executable binary hashes.
Comparison plan B must be a semantic copy of plan A with a distinct seed; only
the seed, derived schedule, and creation time may differ. Profile B may differ
only in the exact plan hash it binds. Calibration A must bind the raw bytes of
plan/profile A. Its validated iteration counts are applied to both schedules;
the freezer does not calibrate a second time for plan B.

The freezer reconstructs all calibration attempts from the started/result
records and raw stdout/stderr files. It verifies every file hash, parses each
raw benchmark-v2 stream, recomputes PSI/cgroup/SMT eligibility, checks child
affinity and binary identity, and confirms the complete iteration-scaling
chain stopped at the first eligible sample of at least 250 ms. It rejects
missing or extra evidence files and does not infer performance verdicts from
calibration telemetry.

Comparison output contains exactly 216 planned commands: 108 for each plan,
with adjacent AB or BA pairs, nine pairs of each order per case, and identical
iteration counts across both plans. A/A-pre output contains 108 commands.
Every row binds its absolute executable, hash, argv, build working directory,
scrubbed environment, selected CPU and host identity, source/product identity,
and plan/profile/calibration hashes. HOME and TMPDIR values are reserved under
the future output directory. The artifact states
`benchmark_launches_performed=0` and has no ratio or performance verdict.

All input and evidence hashes are revalidated immediately before the artifact
is atomically published under a new HOME directory with file and directory
fsyncs. Output paths overlapping evidence, source, or build directories are
rejected. The command artifact is a freeze only; a later runner must separately
validate runtime eligibility before launching any command.
