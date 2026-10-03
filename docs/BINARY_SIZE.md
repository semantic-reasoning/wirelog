# Binary size monitoring and admission

## Pre-stable policy mode

Until the user explicitly declares a stable version, the 5120-byte allowance
and resulting `.text` ceiling are reference values. PR CI reports a valid
overage as `over-budget` but exits successfully. Measurement failures,
production profile mismatches, malformed provenance, and tested-merge identity
failures remain blocking. The comparison reads the policy mode from the event
base; older bases without `tests/size_policy_mode.txt` use advisory mode. A
future switch to `enforced` requires the user's stable-version declaration and
a separate reviewed policy change.

`tests/baseline_size.txt` records 419135 bytes, the canonical Ubuntu x86_64
GCC `.text` measurement from eligible successful main ancestor
`c6e263d492828d1208fa11005c18d19b05342233`, rather than current main or PR #2032.
The successful
[main workflow run 36673112247](https://github.com/semantic-reasoning/wirelog/actions/runs/36673112247)
produced `wirelog-size-monitor-ubuntu-latest` artifact `11079113586`.
Its source, run, artifact, profile, library digests and measurement are pinned
in `tests/baseline_size.provenance.json`. The unchanged verifier checks those
identities and independently rebuilds that source to reproduce its profile and
`.text` size through the normal `trusted-ci-artifact` policy.

The report records `configure`, `build` and `test` each as success, with status
`within-budget` and a 5084-byte delta against the previous 414051-byte baseline.
The previous baseline measured main ancestor `863e011e`; subsequent changes
on main contribute to the new ancestor measurement. After #2040 found no
coherent reduction sufficient to admit PR #2032, the maintainer authorized
this measured reset. It grants fresh headroom rather than claiming a size
reduction, and does not change TDD eligibility. The fixed 5120-byte allowance
is unchanged, so the resulting ceiling is 424255 bytes. A rebuilt library may
have different non-`.text` bytes due to LTO metadata.

Main's measurement at any given tip is a moving figure and is deliberately
not tracked here. Read it from the most recent concluded `main` run's
`wirelog-size-monitor-ubuntu-latest` artifact, and compute headroom as 424255
minus that measurement. Recording an ancestor stays valid as main advances:
the verifier requires that source to be an ancestor of the event base. The
artifact must remain unexpired, and its source must still reproduce with the
recorded toolchain profile.

Historically, the 408989-byte baseline at `13d9244a` rose to 414051 bytes at
`863e011e`, consuming 5062 bytes of the same fixed allowance. This reset
replaces that ancestor measurement with the authenticated 419135-byte figure.

The production reference allowance remains 5120 bytes. PR CI compares the
production shared library from the exact `pull_request.base.sha` tree with the library from the
tested merge SHA. It verifies that the tested SHA is the merge commit and that
its first parent is the event base. Both libraries are configured and built on
the same runner with `-Dtests=true -DmbedTLS=disabled`, and the resolved Meson
options, compiler/linker identity and version, target, platform, and effective
wirelog build commands must match. A profile mismatch is an error. The
candidate baseline is used only after its CI provenance is verified.

The reference allowance normally places the head at baseline + 5120 bytes. If
the measured base already exceeds that ceiling, the reference size is the
measured base. In enforced mode, a head above that allowed size fails. A
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
