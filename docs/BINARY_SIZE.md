# Binary size monitoring and admission

`tests/baseline_size.txt` records the current canonical Ubuntu x86_64 GCC
measurement (377286 bytes), from source revision
`03a97843a8ca3c740d7ab1df05097aa512f7fbcf`. The earlier 354887-byte value was
legacy and unverified. The refreshed value is tied to the main CI artifact in
`tests/baseline_size.provenance.json` and is accepted only after the verifier
reproduces the profile and `.text` size from that source. The artifact digest
and reported library digest remain cross-checked against the sidecar; a rebuilt
library may have different non-`.text` bytes due to LTO metadata.

The production limit remains 5120 bytes. PR CI compares the production shared
library from the exact `pull_request.base.sha` tree with the library from the
tested merge SHA. It verifies that the tested SHA is the merge commit and that
its first parent is the event base. Both libraries are configured and built on
the same runner with `-Dtests=true -DmbedTLS=disabled`, and the resolved Meson
options, compiler/linker identity and version, target, platform, and effective
wirelog build commands must match. A profile mismatch is an error. The
candidate's baseline file is never used for its own admission decision.

Normally the head may be at most baseline + 5120 bytes. If the measured base
already exceeds that ceiling, the head may not exceed the measured base. A
docs-only change receives no special exemption; equal measured binaries pass
because they add no size to the base.

Main-branch measurement is advisory. Its always-run reporting step records
commit and run identity, measurement, baseline, delta, profile, and status in a
warning annotation, the job summary, and a platform-specific
`wirelog-size-monitor-${{ matrix.os }}` artifact. Baseline provenance accepts
only `wirelog-size-monitor-ubuntu-latest`; ARM measurements cannot authorize
the canonical x86 baseline.
If setup, profile capture, library lookup, or measurement fails, the report is
`monitoring-error`; missing data is never replaced with zero or described as a
pass. A monitor report can still carry a complete production-library
measurement when the later full build fails. Such an artifact is eligible for
baseline provenance only when the main workflow run succeeded, configuration
succeeded, the report contains the exact measured library/profile evidence,
and the verifier independently reproduces all of it. This does not declare
the failed full build successful.

## Baseline updates

Do not replace the baseline with a local build measurement. A numeric update is
accepted only when its sidecar identifies a report artifact from a successful
`ci-main.yml` push run in this repository, the source commit is an ancestor of
the PR base, the Actions artifact digest and report agree with the sidecar, and
the current runner can reproduce the profile and section size from that
eligible source. The artifact and report library digests must match the
sidecar, but rebuilds need not be byte-identical outside the measured section.
A `monitoring-error` artifact qualifies only for the
narrow later-full-build failure case described above. PR admission still uses the event base's
baseline, so changing source and baseline in one PR cannot grant additional
budget. If artifact access, toolchain reproduction, or provenance validation
fails, the update is rejected. No workflow writes or commits baseline changes.

Required PR check/job names are unchanged. The main monitoring step remains
non-blocking and the repository grants no write permission for this process.
