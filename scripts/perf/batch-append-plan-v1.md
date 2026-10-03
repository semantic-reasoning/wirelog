# Batch append campaign plan v1

`prepare-batch-append-campaign.py` writes a provenance-checked, immutable
`campaign-plan.json`. It does not build or launch the benchmark, qualify the
host, or produce a performance verdict. Build-profile and host qualification
are separate later steps.

The caller supplies read-only wrapper worktrees. Each wrapper commit's tree
must equal its declared upstream tree. Repositories must be non-shallow, and each
wrapper parent must be present; parent IDs are recorded. Each worktree must have
the same four benchmark overlay files staged, no unstaged or untracked files, and the staged
binary diff must byte-match the supplied overlay patch. The plan records both
wrapper commit IDs and parents, upstream tree IDs, resulting staged tree IDs,
and the overlay SHA-256. For an A/A control, upstream tree IDs must match.

The schedule has 54 adjacent pairs: each of `1x1`, `1x256`, and `32x256` has
nine AB and nine BA pairs. Pair descriptors are ordered by SHA-256 of
`wirelog-batch-append-plan-v1\0<seed>\0<pair-id>`; ties use pair ID. This makes
the schedule reproducible without relying on Python's random implementation.
Each pair's two calls stay adjacent. Each future process is specified to use
two internal warmups and one measured sample, for 108 total launches.

The output directory must not exist and its parent must already exist. The
manifest file and directory are fsynced before success is reported. Temporary
paths under `/tmp` and `/dev/shm` are rejected.
