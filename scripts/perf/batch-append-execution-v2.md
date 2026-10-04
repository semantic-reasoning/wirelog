# Batch append execution preflight v2

`prepare-batch-append-execution.py` accepts only plan v3, its immutable overlay
patch, and two pre-existing Meson build directories. V2 plans are rejected.
The profile artifact schema is v2 and remains `not_executable`; this tool does
not build, launch benchmarks, qualify calibration, or create execution argv.

Before and after profile inspection it hashes and recomputes the plan and
schedule, revalidates the tracked manifest hash and fixed anchor/product delta,
the direct-anchor/synthetic-product checkout identities, staged overlay and
execution trees, and helper blob identities. Sources must be complete and
clean apart from the exact staged overlay. Build roots, plan, overlay, and
artifact output must be under HOME and outside source and build trees.

For each side it checks Meson's source/build mapping, clean Ninja dry-runs for
both the default graph and explicit `bench/bench_batch_append` target,
normalized build options, compiler/linker/machine/dependency metadata, tool
hashes, target and flags, introspection hashes, Meson log and compilation
database hashes, and benchmark binary hash. The profile and binary are checked
again before output. It writes the artifact atomically with file and directory
fsyncs. The fixture test snapshots build trees to confirm preflight performs
no writes or benchmark launches.

The separate Unit 3A collector binds raw baseline stdout/stderr and provenance
and accepts at least 250 ms per case. Unit 3B then validates every calibration
file and freezes either 216 comparison commands or 108 A/A-pre commands in
`batch-append-command-freeze-v1.md`. This profile remains non-executable;
`collect-batch-append-campaign.py --run` revalidates host identity before each
launch and records fresh per-run eligibility after each process, as described
in `batch-append-campaign-collection-v1.md`. Neither preflight nor freezer
launches a benchmark.
