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
  | sort -u > abi/libwirelog-1.0.macos.symbols
```

```powershell
dumpbin /EXPORTS builddir\wirelog-1.dll |
  Select-String '^\s*\d+\s+[0-9A-Fa-f]+\s+[0-9A-Fa-f]+\s+([A-Za-z_][A-Za-z0-9_]*)\b' |
  ForEach-Object { $_.Matches[0].Groups[1].Value } |
  Where-Object { $_ -like 'wirelog_*' } |
  Sort-Object -Unique |
  Set-Content abi\libwirelog-1.0.windows.symbols
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
   #695 B8).
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

---

## 3. Branch-protection and freeze rules

### After release-1.x cut (post-#685 v1.0 RC1)

- The `[Unreleased]` section in `CHANGELOG.md` is **frozen** on
  `release-1.x` between rc1 and GA.  Hotfixes update the `[1.0.0]`
  versioned section, not `[Unreleased]`.  Mechanically enforced
  by the per-branch CI rule under #747 B18.

### Branch-protection (release-1.x)

Per #746 B17, `release-1.x` requires:

- At least one CODEOWNER review.
- Required status checks (default + abi + asan + tsan + perf).
- Linear history (no force-push, no merge commits).
- Signed commits (GPG verified).
- Tag protection on `v1.x.*`.

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
- #744 — SBOM automation (SPDX + CycloneDX).
- `stable-release-plan.md` §12.8 — GA exit conditions.
