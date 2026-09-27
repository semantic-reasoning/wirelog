# Binary size monitoring and admission

`tests/baseline_size.txt` records 404282 bytes, the canonical Ubuntu x86_64
GCC `.text` measurement from PR #1961's reviewed production size job at source
revision `418116cf14e915f6391082ea37b2884b67edd960`. This is a one-time,
maintainer-approved baseline reset based on that reproducible measurement; it
does not reduce the binary's size. Its exact PR, base, merge, run, job, log,
profile, prior baseline, and measurement values are pinned in
`tests/baseline_size.provenance.json`. The verifier accepts the record only
after checking those identities and reproducing the canonical profile and
`.text` size. The fixed 5120-byte allowance is unchanged, so the resulting
maximum is 409402 bytes. A rebuilt library may have different non-`.text`
bytes due to LTO metadata.

The production limit remains 5120 bytes. PR CI compares the production shared
library from the exact `pull_request.base.sha` tree with the library from the
tested merge SHA. It verifies that the tested SHA is the merge commit and that
its first parent is the event base. Both libraries are configured and built on
the same runner with `-Dtests=true -DmbedTLS=disabled`, and the resolved Meson
options, compiler/linker identity and version, target, platform, and effective
wirelog build commands must match. A profile mismatch is an error. The
candidate's baseline file is used only after its main CI provenance is verified.

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

Do not replace the baseline with a local build measurement. Normally, a numeric
update is accepted only when its sidecar identifies a report artifact from a successful
`ci-main.yml` push run in this repository, the source commit is the PR base or
an earlier ancestor, the Actions artifact digest and report agree with the sidecar, and
the current runner can reproduce the profile and section size from that
eligible source. The artifact and report library digests must match the
sidecar, but rebuilds need not be byte-identical outside the measured section.
A `monitoring-error` artifact qualifies only for the
narrow later-full-build failure case described above. A candidate baseline can
grant additional budget only when the verified measurement comes from an eligible
main revision distinct from the candidate. If artifact access, toolchain
reproduction, or provenance validation fails, the update is rejected. No
workflow writes or commits baseline changes. The only reviewed-PR exceptions
are the exact, one-time records for PRs #1959 and #1961 embedded in the verifier;
each is restricted to its recorded measurement and repair paths. They grant no
general PR-based baseline eligibility and retain the same 5120-byte allowance.

Required PR check/job names are unchanged. The main monitoring step remains
non-blocking and the repository grants no write permission for this process.
