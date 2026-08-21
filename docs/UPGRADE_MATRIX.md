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

The executor row is also not a compatibility pass: v0.30.0 declares the
executor/result functions but does not export their implementations. The
runner records the exact missing-symbol inventory and linker diagnostic in
`executor-status.json` and `executor-old-link.log`; the compatibility policy
and the decision whether a shim is required are tracked in #1157. #752 stays
open until that policy is resolved.
