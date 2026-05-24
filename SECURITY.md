# Security Policy

## Supported Versions

wirelog is still pre-1.0. Security fixes are currently made on `main`; release
branch backports begin with supported 1.x release lines.

| Version line | Security support |
| --- | --- |
| `main` / current pre-1.0 development | Supported for security fixes on `main` |
| Current supported 1.x line, when available | Supported |
| Older supported 1.x lines, when explicitly announced | Backports considered under the policy below |
| Unmaintained 0.x release tags and unsupported old branches | Not supported |

## Reporting Vulnerabilities

Please report suspected vulnerabilities privately by email:

**inquiry@cleverplant.com**

Do not report security vulnerabilities through public GitHub issues,
discussions, or pull requests. Public reports can expose users before a fix is
available.

We aim to acknowledge vulnerability reports within a few business days. The
acknowledgment target is not a guarantee of a fix timeline; investigation,
coordination, and release timing depend on the issue and affected versions.

Commercial licensing, general support, feature requests, and ordinary bug
reports are separate from vulnerability intake, even if they use the same
published contact address.

## Report Contents

Please include as much of the following as you can:

- A short summary of the vulnerability and suspected impact.
- Affected version, branch, commit, build options, and platform.
- Whether the default build or `mbedTLS=enabled` build is affected.
- Reproduction steps, proof of concept, crash input, or minimized test case.
- Relevant source paths, stack traces, sanitizer output, logs, or debugger
  notes.
- Whether the issue is already public or has been shared with other parties.
- Any disclosure timing constraints you need us to know about.

Reports do not need to be perfect. If you are unsure whether something is a
security issue, report it privately and we will triage it.

## GPG

Encrypted communication is optional. If you want to encrypt sensitive report
details, use the maintainer signing key below and include a non-sensitive email
subject so we can route the report.

- Full fingerprint: `639EB6441B3C8A2E083ACED75BFC926EFD323A4A`
- Key ID: `5BFC926EFD323A4A`

## CVE and Advisory Process

After triage, we will classify the report, identify affected versions and build
configurations, and prepare a fix or mitigation when warranted.

For confirmed vulnerabilities with user impact, we will coordinate disclosure
and request CVE assignment when warranted. The project does not claim CNA
status. Advisory content may include affected versions, severity, impact,
mitigations, fixed versions, and reporter credit when requested and appropriate.

## Severity Rubric

Severity depends on exploitability, affected build options, host-application
exposure, and impact. wirelog is an embedded Datalog engine; the host
application owns the I/O boundary, so the same bug may have different severity
depending on whether the host feeds untrusted programs or data into wirelog.

| Severity | Examples |
| --- | --- |
| Critical | Reliable arbitrary code execution, memory corruption with practical control of execution, or a sandbox escape in a common host integration where untrusted input reaches wirelog. |
| High | Denial of service from untrusted input, out-of-bounds read or write with credible data exposure or corruption, parser or evaluator behavior that can violate host security assumptions, or crypto-built-in misuse that materially weakens security when `mbedTLS=enabled`. |
| Medium | Crashes requiring unusual configuration, resource exhaustion with limited exploitability, incorrect query results that can affect authorization or policy decisions in a plausible integration, or information disclosure with constrained scope. |
| Low | Hard-to-trigger crashes in non-default paths, diagnostic leaks without sensitive data, build or packaging issues that do not directly expose users, or defense-in-depth hardening findings. |

## Backport Policy

Before 1.0, security fixes land on `main`. Pre-1.0 release tags and old
development snapshots are not maintained as long-term security branches unless
a release note explicitly says otherwise.

For supported 1.x lines, we will consider backports for confirmed security
fixes when the fix can be applied with acceptable risk. Critical and High
issues are the strongest candidates for backporting. Medium and Low issues may
be fixed in the next regular release unless impact or operational risk justifies
a backport.

Unsupported old versions may receive public mitigation notes, but users should
upgrade to a supported version or to `main` when directed during the pre-1.0
period.

## Security Model

The threat model, optional crypto posture, mbedTLS build-option implications,
and export-control self-classification are documented in
[`docs/SECURITY_MODEL.md`](docs/SECURITY_MODEL.md).
