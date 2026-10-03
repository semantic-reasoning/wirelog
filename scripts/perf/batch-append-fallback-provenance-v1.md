# Batch append fallback provenance v1

`batch_append_fallbacks.py` is the shared authority used by campaign planning
and profile preflight. Its per-source result is embedded under
`plan.sources.{base,candidate}.fallback_dependencies` and at the profile
artifact’s `fallback_dependencies`. Every plan revalidation reconstructs this
result. Profile preflight verifies it before and after Meson/build reads and
again before and after profile publication. Calibration and command freezing
continue to bind exact plan/profile bytes, so a changed fallback cannot be
reused downstream.

The outer source status allowlist is exact: the four staged benchmark overlay
paths and ignored `subprojects/nanoarrow/`, `subprojects/xxHash-0.8.4/`, and
`subprojects/packagecache/` directories. Any other tracked, untracked, or
ignored path is rejected. Staged overlay bytes must still match the supplied
patch.

Nanoarrow must be the non-shallow nested checkout at commit
`ec8a58cae18beaa241c7fea7cb26816ac27c280f`, tree
`1d477a02d0f027659c19afd95528bace072107a3`. Its tracked wrap blob, URL, and
revision are pinned. The complete nested filesystem must equal tracked Git
entries plus the one exact wrap-hash marker. The sole tracked symlink is
`python/subprojects/arrow-nanoarrow`, with pinned blob
`c25bddb6dd4666c6eb8cc92e33f1d60f64c3162b`, target `../..`, and a resolved
target equal to the checkout root. Other symlinks, special entries, dirty
tracked files, and additional files/directories are rejected.

The xxHash wrap and archive SHA are pinned. The package cache must contain only
the regular `xxHash-0.8.4.tar.gz` archive. The verifier parses it without
extracting, rejects unsafe, duplicate, linked, and special members, then builds
a complete expected manifest from archive contents plus the exact tracked
`subprojects/packagefiles/xxhash-0.8.4` overlay. The extracted source directory
must match every expected path, type, permission mode, byte hash, and the
single exact wrap-hash marker. Manifests and hashes are recorded for both
sources in plans and profile artifacts.
