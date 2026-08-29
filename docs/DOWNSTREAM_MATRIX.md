# GA downstream rebuild matrix

Issue #1163 closes the repository-side portion of the six-workload GA gate.
The release tag workflow runs `scripts/release/run-downstream-matrix.sh` from
an exact source tarball and fails on missing data, a non-zero benchmark exit,
or a tuple/iteration mismatch.

## Runner contract

The job uses the isolated self-hosted label set
`[self-hosted, linux, x64, wirelog-ga]`. The named runner must be Ubuntu 24.04
x86_64 with GCC, Meson, Ninja, GNU tar, curl, unzip, and timeout; it needs at
least 8 logical CPUs, 64 GiB RAM, 250 GiB free local storage, and a single
matrix job at a time. The CPU
governor must be `performance`. The workflow has a 360-minute timeout; DOOP
is the resource driver (about 40 GiB RAM and 23 minutes at W=1).

The runner must not be shared with untrusted jobs. The workflow removes its
candidate archive/evidence directories after artifact upload, and the matrix
script removes its temporary extraction directory after each run. Host identity,
kernel, architecture, CPU count, memory, candidate SHA, and completion time
are retained in the uploaded evidence.

## Dataset and oracle policy

The five repository-tracked datasets are identified by the sorted-file
manifest hashes and the `candidate-commit:<path>` provenance identifiers in
`scripts/release/downstream-matrix-oracles.tsv`; changing one requires an
intentional oracle update. The acquisition column is a declarative recipe ID,
not shell text and is never evaluated. DOOP is fetched from the FlowLog
zxing archive and `bench/data/doop/download.sh` rejects any archive other than
SHA256
`154593343fefd18306d4098ba9f6286947b134b56ebcf83d8e8eae368d5867e7`.

The DOOP archive URL is pinned to Hugging Face dataset revision
`da9e91b3ff75d94604f57ba2b21ef3aa97e241ec`. Its `zxing.zip` blob is 72,025,076
bytes with ETag/SHA256
`154593343fefd18306d4098ba9f6286947b134b56ebcf83d8e8eae368d5867e7`; the
extracted `.facts` manifest is recorded in the oracle file. The production
matrix rejects a `DOOP_ZXING_URL` override so the declared provenance and
effective download source cannot diverge. The downloader retains its override
variables only for isolated negative fixtures and is not a production policy
boundary by itself.

The pinned W=1 oracle is:

The correctness matrix uses one serial run per workload (`workers=1`,
`repeat=1`). Timing is retained as evidence, but repetition for performance
calibration belongs to the separate perf issues and is not a GA correctness
requirement.

| Workload | Tuples | Iterations |
| --- | ---: | ---: |
| CSPA (`cspa-fast`) | 20,381 | 6 |
| Galen | 5,568 | 23 |
| Polonius | 1,983 | 23 |
| DDISASM | 704 | 0 |
| CRDT | 2,152,328 | 14,148 |
| DOOP (zxing) | 13,828,835 | 153 |

The workflow uploads the candidate tarball checksums, host metadata, the
complete result table, data-provenance.tsv, and one log per workload. A successful run is the
external evidence needed to close #1156/#1163 and must be linked from the
release notes and the corresponding issue.
