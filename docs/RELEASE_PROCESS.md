# wirelog Release Process

This document is the **canonical procedure for cutting a wirelog
release tag and publishing the release notes that accompany it**.
It is the operational counterpart of:

- `CONTRIBUTING.md` — Changelog Conventions (the per-PR rules).
- `CHANGELOG.md` — the source-of-truth changelog.
- `docs/MIGRATION.md` — version-to-version migration recipes.
- `docs/SECURITY_MODEL.md` — vulnerability-disclosure policy.
- `stable-release-plan.md` — long-form roadmap to v1.0 GA.

> **Note on `stable-release-plan.md`.**  Unlike the other documents
> listed above, the long-form release plan is intentionally **not**
> tracked in the source tree — it is a working planning artifact that
> maintainers keep locally during release preparation.  The public-
> header SoT gate `scripts/ci/check-public-header-surface.py` knows
> this: its `parse_plan_section()` helper returns `None` when the
> file is absent, which is the case in every CI checkout and clean
> clone today.  The §3.1 cross-check therefore activates only for
> contributors who keep a working copy of the plan locally; drift
> errors against that copy (e.g. a public-header rename that the
> local plan has not yet picked up) are local-developer noise and do
> not gate CI.  If the team later decides to track the plan in-tree,
> the existing gate immediately becomes a hard mirror for §3.1 with
> no script change required.

It is the deliverable for issue **#772** (release-note template +
publication procedure) and is referenced by **#687** (v1.0.0 GA
epic) which lists "CHANGELOG `[1.0.0]` section merged" as an exit
condition without specifying the publication side.

- Platform artifact policies for **Android** and **iOS** are tracked in
  [`docs/PLATFORM_SUPPORT.md`](PLATFORM_SUPPORT.md).

---

## 1. Release-note template

Every release tag carries a release note authored from this
template.  The note is published in **two places** that must agree:

1. The versioned section of `CHANGELOG.md` (under
   `## [X.Y.Z] - YYYY-MM-DD`).
2. The GitHub Releases body for the corresponding tag.

The CI gate `scripts/ci/check-release-template.sh` (registered as
`meson test --suite abi:release_template`) diffs the two sources
when a published release exists for the tag at HEAD; it SKIPs
cleanly on PR builds and on tags whose release does not yet
exist.

### ABI manifest update

The libabigail-backed `abi/libwirelog-1.0.abi.json` baseline pins struct
layouts, function signatures, and visibility attributes that the
`abi/libwirelog-1.0.symbols` allowlist cannot see (Issue #786 /
`meson test --suite abi:abi_manifest`).  On a release PR that
deliberately reshapes the ABI, regenerate the baseline with
`scripts/release/regenerate-abi-manifest.sh build` and commit the diff
alongside the API change.  The gate SKIPs cleanly when libabigail is
not installed or when the baseline is absent — first-time seeding only
requires running the regeneration script once on a Linux/x86_64 host.

The v1.0 libabigail baseline is intentionally Linux/x86_64-only.  On
Linux arm64, v1.0 ABI coverage is the arch-agnostic
`abi/libwirelog-1.0.symbols` gate (`meson test --suite
abi:abi_symbols`); `abi_manifest` skips because libabigail treats the
ELF architecture difference between the x86_64 baseline and an arm64
build as an ABI break.  Per issue #824, arm64 per-architecture
libabigail baselines are out of scope for #681 / v1.0 unless a later
policy issue adds an arm64 baseline file and matching regeneration
workflow.

macOS and Windows export-surface checks are warning-only for v1.0
(Issue #788).  They run in the `abi_advisory` suite as
`abi_symbols_macos` and `abi_symbols_windows`, compare normalized
public `wirelog_*` exports against `abi/libwirelog-1.0.macos.symbols`
and `abi/libwirelog-1.0.windows.symbols`, and emit GitHub `::warning::`
annotations instead of failing the build.  The
platform baselines intentionally normalize Mach-O's leading `_`
symbol spelling and inspect DLL exports rather than Windows import
library `__imp_*` thunks; richer Mach-O / PE manifests remain a v1.x
follow-up.

Run the advisory suite with `meson test -C builddir --suite
abi_advisory`, or run an individual check with `meson test -C builddir
abi_symbols_macos` / `abi_symbols_windows`.

After a deliberate public ABI addition, regenerate the advisory
allowlists on the matching hosts:

```bash
nm -gU builddir/libwirelog.1.dylib \
  | awk '{name = $NF; sub(/^_/, "", name); if (name ~ /^wirelog_/) print name}' \
  | LC_ALL=C sort -u > abi/libwirelog-1.0.macos.symbols
```

```powershell
$symbols = dumpbin /EXPORTS builddir\wirelog-1.dll |
  Select-String '^\s*\d+\s+[0-9A-Fa-f]+\s+[0-9A-Fa-f]+\s+([A-Za-z_][A-Za-z0-9_]*)\b' |
  ForEach-Object { $_.Matches[0].Groups[1].Value } |
  Where-Object { $_ -like 'wirelog_*' }
$set = [System.Collections.Generic.SortedSet[string]]::new(
  [System.StringComparer]::Ordinal)
$symbols | ForEach-Object { [void]$set.Add($_) }
$set | Set-Content abi\libwirelog-1.0.windows.symbols
```

### Template (Markdown)

```md
# wirelog X.Y.Z (YYYY-MM-DD)

## Headline

<1-3 sentences summarising what this release brings to users.>

## Highlights

- <Bullet 1>
- <Bullet 2>
- <Bullet 3>

## Changes

### Added
- <items from CHANGELOG[X.Y.Z]/Added>

### Changed
- <items from CHANGELOG[X.Y.Z]/Changed>

### Deprecated
- <items from CHANGELOG[X.Y.Z]/Deprecated, if any>

### Removed
- <items from CHANGELOG[X.Y.Z]/Removed, if any>

### Fixed
- <items from CHANGELOG[X.Y.Z]/Fixed>

### Performance
- <items from CHANGELOG[X.Y.Z]/Performance, if any>

### Security
- <items from CHANGELOG[X.Y.Z]/Security, if any>

### Documentation
- <items from CHANGELOG[X.Y.Z]/Documentation, if any>

## Migration

See [`docs/MIGRATION.md`](../docs/MIGRATION.md) section
`X.Y → X.Y'.0` for upgrade recipes.

## Performance

See [`docs/PERFORMANCE.md`](../docs/PERFORMANCE.md) for the
release-pinned benchmark snapshot.

## Security

<Inline CVE refs (if any), or "No advisories.">

## Verification artefacts

- **Signed tag**: `vX.Y.Z` (GPG fingerprint <maintainer>;
  `git verify-tag` clean).
- **Tarball**: `wirelog-X.Y.Z.tar.gz` (SHA256 / BLAKE3
  checksums committed alongside).
- **SBOM**: `wirelog-X.Y.Z.spdx.json` (SPDX 2.3) +
  `wirelog-X.Y.Z.cdx.json` (CycloneDX 1.5).
- **ABI manifest**: `abi/libwirelog-1.0.symbols` on Linux x86_64 and
  arm64; `abi/libwirelog-1.0.abi.json` (libabigail) on Linux x86_64.

## Acknowledgments

<Optional: contributors of note for this release.>
```

---

## 2. Publication procedure

When cutting a release tag:

1. **Open a release PR** that:
   - Moves the entire `[Unreleased]` block in `CHANGELOG.md`
     under a new heading `## [X.Y.Z] - YYYY-MM-DD`.
   - Resets `[Unreleased]` to the placeholder shape (empty
     category headings).
   - Bumps `project_version` in `meson.build`.
   - Updates `docs/MIGRATION.md` with any release-specific
     entries (per-issue MIGRATION entries land alongside their
     PRs; the release PR consolidates).
2. **Wait for the full CI matrix** to pass on the release PR
   (default + `--suite abi` + `--suite asan` + `--suite tsan`
   + `--suite perf` on a `WIRELOG_PERF_REQUIRE=1` runner per
   #695 B8, plus the `mbedtls-enabled / ubuntu-latest / gcc`
   validation check once #843 lands).
3. **Merge** the release PR via rebase (no squash, no merge
   commit; matches project convention).
4. **Tag** on `main`:
   ```bash
   git checkout main && git pull --ff-only
   git tag -s vX.Y.Z -m "wirelog X.Y.Z"
   git push origin vX.Y.Z
   ```
   The `release-tag.yml` workflow (#749 B19, when shipped) re-runs
   the full CI matrix on the tagged commit and produces the
   verification artefacts (signed tarball, checksums, SBOM, ABI
   manifest, SLSA provenance attestation).
5. **Author the GitHub Release**:
   ```bash
   gh release create vX.Y.Z \
       --title "wirelog X.Y.Z" \
       --notes-file <(./scripts/release/extract-changelog-section.sh X.Y.Z)
   ```
   The release body MUST be the verbatim CHANGELOG section for
   `[X.Y.Z]`.  The CI gate enforces equality.
6. **Attach release artefacts** produced by the at-tag workflow:
   tarball + checksums + SBOM + ABI manifest + provenance file.

### Perf-suite graph gate

The `sub_ms_graph_perf_gate` entry in `--suite perf` covers the Reach,
SSSP, SG, and Bipartite W=1 sub-ms graph workloads.  Like the other
release perf gates, it is opt-in via `WIRELOG_PERF_GATE=1` and is meant
for stable perf runners, not normal shared local/default runs.

Use a release/perf build with `-Dwirelog_log_max_level=error` and a
stable timing host, including the `performance` CPU governor where the
platform exposes one.  `WIRELOG_PERF_REQUIRE=1` turns host or build
misconfiguration from SKIP into FAIL for release runners.

Current enforcement is correctness sentinels plus median/mean/stdev/CoV
reporting with a CoV <= 5% noise ceiling.  There is no absolute
wall-clock regression budget for these graph workloads yet; per-workload
median targets should be added only after stable-run provenance exists.

---

## 3. Branch-protection and freeze rules

### After 1.0 cut (post-#685 v1.0 RC1)

- The `[Unreleased]` section in `CHANGELOG.md` is **frozen** on
  `1.0` between rc1 and GA.  Hotfixes update the `[1.0.0]`
  versioned section, not `[Unreleased]`.  Mechanically enforced
  by `scripts/ci/check-changelog-rc.sh` (always-emitting check context
  name: `RC changelog freeze gate`) under #747 B18.  The check enforces
  only when a PR base branch is `1.0`; non-`1.0` PRs SKIP/pass.

### Branch-protection (1.0, #746 B17)

#### Phase A — preparatory (repo-side readiness only)

- Keep `.github/workflows/ci-pr.yml` subscribed to PR targets
  `[main, 1.0]` so the same CI contexts are emitted as soon as
  `1.0` exists.
- Do **not** configure GitHub branch protection yet; the branch does
  not exist before the last-moment RC1 cutover.
- This phase is intentionally incomplete by itself: final #746
  acceptance remains gated on Phase B.

#### Phase B — final RC1 cutover (last moment)

Execute only after `1.0` is created during the RC1 cutover and
the cutover commit sets `project_version` to `1.0.0-rc1`.

- At least one CODEOWNER review.
- CLA signoff required via branch protection on the stable
  `CLA signoff gate` check.  This repository-owned gate verifies the
  hosted cla-assistant external status context `license/cla`; do not
  make `license/cla` itself the required protected check.
- Linear history (no force-push, no merge commits).
- Signed commits (GPG verified).
- Tag protection on `v1.x.*`.
- Required status checks:
  - Stable policy plan: `default`, `abi`, `asan`, `tsan`, `perf`,
    plus `mbedtls-enabled / ubuntu-latest / gcc`.
  - `perf` must be represented by a dedicated always-emitting release
    perf context that runs with `WIRELOG_PERF_REQUIRE=1`; do not rely on
    path-filtered contexts for branch protection.
  - Currently known concrete PR contexts to require at cutover (unless
    replaced by a later policy-consolidation change): `lint / EditorConfig check`,
    `lint / uncrustify check`,
    `Build / ubuntu-latest / gcc`, `Build / ubuntu-24.04-arm / gcc`,
    `RC changelog freeze gate`, `CLA signoff gate`,
    `mbedtls-enabled / ubuntu-latest / gcc`,
    `Build / ubuntu-latest / clang`, `Build / macos-latest / clang`,
    `Build / windows-latest / msvc`,
    `Sanitizers / ubuntu-latest / gcc`,
    `Sanitizers / ubuntu-latest / clang`,
    `Sanitizers / macos-latest / clang`,
    `TSan / ubuntu-latest / gcc`.

#### Synthetic PR verification plan (Phase B)

After branch creation and protection rules are configured, open a
synthetic PR targeting `1.0` and verify:

1. The required check contexts above are present and enforced.
2. Merge is blocked until at least one CODEOWNER review is granted.
3. Merge is blocked until `CLA signoff gate` passes by observing
   hosted `license/cla: success` on the PR head SHA.
4. Merge is blocked while any required check is failing or pending.
5. Linear-history and signed-commit constraints are enforced.

Phase B prerequisites for final #746 acceptance are not fully present in
this repository state yet: `CODEOWNERS` and the always-emitting
`CLA signoff gate` workflow are present, but final synthetic PR
validation remains a Phase B cutover task after `1.0` branch protection
is configured.

Close the synthetic PR after capturing verification evidence for #746.
Final #746 acceptance is deferred until this Phase B verification passes
on the real `1.0` branch at the `1.0.0-rc1` cutover.

### Perf nightly monitoring posture

For v1.0, `.github/workflows/perf-nightly.yml` remains
`continue-on-error: true`.  GitHub-hosted perf runners are noisy enough
that nightly performance checks should not directly block merges,
release PRs, or release tags.

The nightly `meson test --suite perf` run and the `bench_flowlog`
portfolio artifacts/current-run summaries are monitoring evidence, not
branch-protection gates.  The portfolio artifacts and current-run
summary are required observability outputs for the nightly job so
maintainers can inspect workload coverage and current results.

The workflow also produces best-effort 30-day/current-available
SKIP-rate monitoring as a workflow summary and a
`perf-portfolio-skip-rate` artifact.  This detects silently degraded
coverage without adding thresholds, gates, or paging.  Promoting perf
checks to blocking status requires a dedicated stable perf runner and a
separate policy change.

### mbedTLS-enabled validation policy

The stable check name for optional crypto validation is
`mbedtls-enabled / ubuntu-latest / gcc`.  The check is validation
coverage only: it proves that `-DmbedTLS=enabled` still configures,
builds, and runs the crypto tests against a system PSA Crypto provider.
It does **not** change the default release artifact posture, which
remains `mbedTLS=disabled` unless a separate release-artifact issue
changes that policy.

- PRs to `main`: #843 must add this as a blocking PR check using
  `-DmbedTLS=enabled`, not `auto`, so dependency absence cannot silently
  downgrade coverage.
- Pushes to `main`: `ci-main.yml` may mirror the same coverage as
  monitoring only, consistent with its existing `continue-on-error`
  posture.
- `1.0`: #746 branch-protection work should require the same
  stable check name once the branch is cut.
- Release tags: #749 tag-time verification should include this enabled
  crypto validation at minimum by running `cryptographic_hashes` under
  `-DmbedTLS=enabled`; a full enabled suite may replace that minimum if
  runtime stays acceptable.

---

## 4. Per-release security review

- License/export-control reclassification per
  `docs/SECURITY_MODEL.md` — see #715 (subprojects pin
  verification) and the per-release checklist tracked under
  v1.0.0 GA.
- CVE intake / PSIRT activation per #698 B11.
- Subprojects pin SHA256 verification at release time.

---

## 5. Internal references

- #687 — v1.0.0 GA epic (this document is the publication side
  of its "CHANGELOG [1.0.0] section merged" exit condition).
- #471 — CHANGELOG infrastructure (format gate; landed via
  #777).
- #772 — this document.
- #745 — `docs/MIGRATION.md` 0.30 → 1.0 entry.
- #749 (B19) — at-tag CI re-verification workflow.
- #750 — GPG + cosign-keyless signing pipeline.
- #751 — tarball + SHA256 + BLAKE3 + provenance attestation.
- #752 — GA release-facing deferrals/readiness follow-up in release process docs.
- #753 — GA PSIRT/CVE-intake activation and 1.x support-window declaration deferral.
- #744 — SBOM automation (SPDX + CycloneDX).
- `stable-release-plan.md` §12.8 — GA exit conditions.
