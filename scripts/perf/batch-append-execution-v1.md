# Batch append execution preflight v1

`prepare-batch-append-execution.py` consumes a plan-v2 manifest, its immutable
overlay patch, and two pre-existing Meson build directories. It does not run
Meson setup/build, launch `bench_batch_append`, read performance samples, or
create execution arguments. Its artifact has `source_build_verified=true` and
`not_executable=true`.

Before and after profile inspection it strictly parses and hashes the plan,
recomputes the plan and frozen 108-launch schedule, and revalidates source
wrapper/parent/tree identities, clean and non-shallow repositories, staged
overlay bytes, and unchanged helper Git blob IDs and SHA-256 values. Build
roots, plan, overlay, and output must be under HOME and outside source trees.

For each side it checks Meson's recorded source/build mapping, a clean Ninja
`-n` result, complete normalized resolved options, compiler/linker and machine
metadata, compiler/linker executable hashes, dependencies, target source and
flag metadata, CPU/link flags, introspection file hashes, and benchmark binary
SHA-256. It also hashes the retained Meson configure log and
`compile_commands.json` as build provenance evidence; these identify the
recorded configure/build inputs, while the current binary hash and repeated
no-op Ninja dry-run establish the checked build outputs are present and clean.
The normalized profile hashes must match across sides. Profile inputs are
re-read after inspection to detect drift. The tool rejects shallow sources and
any tracked, ignored, or untracked working-tree content. Profile/build reads
and Ninja dry-runs are read-only. The only staged source changes permitted are
the four overlay files already named and verified by plan v2. A regression test snapshots both build trees
before and after preflight. The output JSON is atomically written, fsynced,
renamed, and its containing directories are fsynced.

This artifact is deliberately not executable. A later calibration contract
must bind baseline raw stdout/stderr and provenance, accept at least 250 ms per
case, and freeze all per-case iteration counts and the exact 108 launch argv
records. A runner must refuse this profile-only artifact until that unit has
completed. Calibration qualification, launch scheduling, telemetry, ratios,
and performance verdicts do not belong to this step.
