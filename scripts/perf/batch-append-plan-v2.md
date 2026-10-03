# Batch append campaign plan v2

`prepare-batch-append-campaign.py` writes a provenance-checked v2
`campaign-plan.json`. V1 plans do not satisfy this contract. It does not build
or launch the benchmark, qualify the host, or produce a performance verdict.
Build-profile and host qualification are separate later steps.

The caller supplies read-only wrapper worktrees. Each wrapper commit must have
exactly one parent, and the parent tree, wrapper tree, and declared upstream tree
must match. Repositories must be non-shallow, and the parent commit must be
present. The plan records wrapper and parent commit/tree IDs. Each worktree must
have the same four benchmark overlay files staged, no unstaged or untracked
files, and the staged binary diff must byte-match the supplied overlay patch.
The plan records resulting staged tree IDs and the overlay SHA-256. A/A mode
requires identical upstream trees; comparison mode requires different trees.
The unchanged `bench/bench_util.h` and `tests/test_perf_util.h` are bound by
their Git blob IDs and SHA-256 values and must match between both sources.

The case contract records `ncols`, rows per call, capacity, and initial default
iteration counts: 1x1 uses 2,000,000; 1x256 and 32x256 use 10,000. These are
starting points only. Final counts stay unresolved until baseline calibration.
The plan binds benchmark header, sample, summary, and case records to output
schema v2. Successful sample and case records carry `denied=0`,
`row_count_check=OK`, `capacity_check=OK`, `value_check=OK`, and
`distinct_input_probe=OK`; the benchmark emits these fields only after its
correctness checks pass. The plan contains no performance verdict field.

The schedule has 54 adjacent pairs: each of `1x1`, `1x256`, and `32x256` has
nine AB and nine BA pairs. Pair descriptors are ordered by SHA-256 of
`wirelog-batch-append-plan-v2\0<seed>\0<pair-id>`; ties use pair ID. This makes
the schedule reproducible without relying on Python's random implementation.
Each pair's two calls stay adjacent. Each future process is specified to use
two internal warmups and one measured sample, for 108 total launches.

The output directory must be a new directory under HOME, and its parent must
already exist. The manifest is written to a temporary file, fsynced, renamed
atomically, and the containing directory is fsynced before success is reported.
Paths under `/tmp` and `/dev/shm` are rejected.
