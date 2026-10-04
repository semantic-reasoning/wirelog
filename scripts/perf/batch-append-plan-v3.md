# Batch append campaign plan v3

`prepare-batch-append-campaign.py` writes only a provenance-checked v3
`campaign-plan.json`. The v2 plan is rejected. The builder reads existing
source checkouts and never creates or changes commits, builds, or launches.

The tracked `batch-append-revision-manifest-v1.json` is pinned by the builder's
SHA-256. It identifies anchor commit `64bd12f499b357d60d1928d5dba08326af58426f`
and tree `4c10e21ab4ba034dbc50abd92a59d22e8b975130`, product pre tree equal to
that anchor tree, and post tree `9fa822ba32dd3b89e71d0158efa27422e49a06d4`.
The only product delta is the exact two modified paths
`tests/test_relation_generations.c` and `wirelog/columnar/relation.c`, bound by
the canonical, unrestricted Git diff SHA-256 in the manifest. The validator
checks the complete pre-to-post tree status set and full diff with renames,
external diff drivers, and text conversion disabled; extra changed paths fail
validation. Product paths are disjoint from the four benchmark overlay paths.

Comparison mode requires the base HEAD to be the direct anchor commit and the
candidate HEAD to be a pre-created synthetic commit with exactly one parent:
the anchor. The parent tree must be the pre tree and the commit tree must be
the post tree. A/A mode requires an explicit `--aa-product pre` or
`--aa-product post`; both checkouts must resolve to that same product tree.
Checkout commit/tree, product tree/delta, and staged-overlay execution tree are
recorded as separate identities. The builder does not synthesize the product
commit or modify either source checkout.

Each source must have exactly the four benchmark overlay files staged, with
the staged diff byte-matching the supplied overlay patch. Unstaged changes and
other tracked, ignored, or untracked files are rejected. The unchanged
`bench/bench_util.h` and `tests/test_perf_util.h` are bound by Git blob IDs and
SHA-256 values and must match across both sides.

The case contract records `ncols`, rows per call, capacity, and initial default
iteration counts: 1x1 uses 2,000,000; 1x256 and 32x256 use 10,000. Counts are
starting points only and remain unresolved until baseline calibration. The
plan binds benchmark records to output schema v2 and has no performance
verdict. Its 108 launches are 54 adjacent pairs; each case has nine AB and
nine BA pairs. Pair order uses SHA-256 hash-sort with the v3 seed prefix. Each
future process uses two internal warmups and one measured sample.

The manifest output directory must be fresh and under HOME. The file is
fsynced and atomically renamed, then its containing directories are fsynced.
Paths under `/tmp` and `/dev/shm` are rejected.

## Clean-clone source preparation

The anchor commit and candidate tree are not guaranteed to exist in a fresh
checkout. `batch-append-anchor-v1.bundle` contains the complete pinned history,
and `batch-append-product-delta-v1.patch` reconstructs the exact candidate
tree. The planner verifies their pinned size, hashes, ref, and tree identities,
then reconstructs the product delta in a disposable repository under
`$HOME/.tmp`. It does not read or write the caller checkout's Git object
database.

Hydrate and validate a local Meson fallback seed before preparing a campaign
from a fresh clone, then stage the exact four benchmark overlay paths. The
source materializer requires that seed explicitly and never downloads or
changes dependencies. For example:

```sh
git diff --cached --binary HEAD -- \
  bench/bench_batch_append.c bench/meson.build tests/meson.build \
  tests/test_bench_batch_append.c > "$HOME/.tmp/batch-append-overlay.patch"
uv run python scripts/perf/materialize-batch-append-sources.py \
  --output-root "$HOME/.tmp/batch-append-sources-run-001" \
  --mode comparison \
  --overlay-patch "$HOME/.tmp/batch-append-overlay.patch" \
  --fallback-seed "$HOME/src/wirelog-fallback-seed" \
  --fallback-seed-mode overlay_staged
```

Use `--fallback-seed-mode clean` only when the seed contains exactly the three
validated ignored fallback roots and no staged overlay. The materializer
creates detached `base-source` and `candidate-source` repositories, stages the
same overlay in both, copies only the three validated fallback roots, then
rechecks source and fallback provenance. Pass those two directories to the
campaign planner. The generated sources remain available for build and profile
steps; choose a new, absent output root for each materialization.
For an A/A control, pass `--mode aa_control --aa-product pre` or
`--mode aa_control --aa-product post`; each generated pair then has the same
product tree on both sides.
