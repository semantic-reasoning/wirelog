# 0.30.0 → 1.0 upgrade matrix

The release-tag workflow runs `scripts/upgrade/run-upgrade-matrix.sh` before
the aggregate release gate. It extracts the exact `v0.30.0` commit and the
candidate tag into isolated trees, builds both with Meson, and compiles three
representative consumers against each installed header/library set.

The matrix deliberately keeps legacy and migrated source files separate where
the public migration requires a rename:

| Consumer | v0.30.0 source | Candidate source | Contract |
| --- | --- | --- | --- |
| easy | `tests/upgrade/easy/legacy.c` (`wl_easy_*`) | `tests/upgrade/easy/migrated.c` (`wirelog_easy_*`) | identical sorted `reach` tuples |
| session | `tests/upgrade/session/legacy.c` (legacy easy facade) | `tests/upgrade/session/migrated.c` (`wirelog_session_*`) | identical sorted `reach` tuples |
| executor | `tests/upgrade/executor/consumer.c` | same source | candidate runs; v0.30.0 is `EXPECTED_UNSUPPORTED` |

The candidate is tested from its checked-out git tree because the release
tarball is created from that same tree only after the verification gate. This
avoids a dependency cycle while preserving the source-tree identity of the
artifact. The runner pins both commit IDs, uses isolated temporary prefixes,
sets runtime library search paths, and stores compiler/output evidence in
`UPGRADE_MATRIX_LOG_DIR` when supplied.

The six full downstream workload rebuilds requested by #752 are not implied by
this synthetic matrix. They require a provisioned runner, pinned datasets,
and workload-specific tuple/iteration oracles; that work is tracked in #1156.

The executor row is deliberately not a compatibility pass. The v0.30.0
executor/result API is unsupported and no compatibility shim is promised for
the 1.0 migration. The runner derives the executor/result declaration
inventory from the selected public header, verifies that none is exported by
the v0.30.0 library, and requires the old consumer to fail at link time with
every missing-symbol diagnostic. It records the inventory in
`executor-abi.tsv`, the exact missing-symbol result in `executor-status.json`,
and the linker diagnostic in `executor-old-link.log`. The candidate consumer
must still compile, link, run, and produce its expected output.

This is the resolved policy for #1157: consumers using the v0.30.0 executor
facade must migrate to the 1.0 API; the historical v0.30.0 executor artifact
is not a supported compatibility target. #752 remains open for the separate
downstream workload evidence tracked in #1156.

The executor ABI comparison is a Linux release-gate check. It uses `nm -D`,
GNU linker diagnostics, and `sha256sum`; it is not a cross-platform ABI claim.
