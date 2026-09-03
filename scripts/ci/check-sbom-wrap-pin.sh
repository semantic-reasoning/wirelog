#!/usr/bin/env bash
# Assert sbom/snapshot.txt's recorded nanoarrow commit matches the wrap pin.
#
# generate-sbom.sh appends the resolved subproject commit as a comment:
#
#   # nanoarrow-resolved-sha: 3f824063...
#
# and subprojects/nanoarrow.wrap declares the pin the build actually resolves:
#
#   revision = 3f824063...
#
# Nothing compared them. check-sbom-snapshot.sh cannot: it diffs the baseline
# against `syft | jq | sort` output, which is one package per line and never
# emits a `#` line, so the sha is metadata inside a file whose format has no
# room for it. The `grep -v '^#'` there is a consequence, not the cause -- do
# not "fix" this by removing that strip, it would compare a comment against
# package lines and fail every run.
#
# Why it matters despite the entry being present: the compared line is
# `nanoarrow@0.9.0.9000:Apache`. The `.9000` suffix is the
# development-version convention meaning "after 0.9.0, unreleased", so that
# string is constant across every commit in that window -- a wrap revision bump
# produces a byte-identical snapshot line and the gate stays green while the
# recorded sha and the actual pin have diverged. The windows are long enough to
# matter: the previous one, 0.8.0.9000, spanned 58 nanoarrow commits over about
# six months.
#
# Scope, so triage does not read this as a supply-chain hole: the wrap ships
# inside the source tarball with its revision, and that tarball is checksummed,
# attested and Sigstore-signed, so the dependency IS pinned for consumers. What
# this closes is CI never noticing snapshot/wrap divergence. (#1339)
#
# Deliberately a separate script rather than a branch of check-sbom-snapshot.sh:
# that one SKIPs when syft is absent, and this comparison needs only two
# committed files. Folding it in would make it skip for a reason that does not
# apply to it -- the defect class #1303 exists to remove.
set -euo pipefail

root=${1:-$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)}
snapshot="$root/sbom/snapshot.txt"
wrap="$root/subprojects/nanoarrow.wrap"

for f in "$snapshot" "$wrap"; do
    [ -f "$f" ] || {
        echo "check-sbom-wrap-pin: FAIL: missing $f" >&2
        exit 1
    }
done

# Both extractions require exactly one match, and require it to be present. A missing
# sha line is a failure, not a skip: generate-sbom.sh omits it when the
# subproject is not checked out, so its absence means the committed snapshot was
# regenerated from a tree that could not see the dependency it claims to
# describe.
# The wrap revision is read from the [wrap-git] SECTION, not from the first
# `revision =` anywhere in the file. meson resolves the one under [wrap-git];
# an unanchored match reports OK against a `revision` sitting in [provide],
# while the build uses a different commit. Verified against meson's own parser
# (ConfigParser, interpolation=None): that wrap parses cleanly and resolves to
# the [wrap-git] value. A false green in the gate whose only job is catching
# divergence is the one direction that must not be possible.
#
# [0-9a-fA-F], matching check-wrap-revisions.sh's accepted alphabet. A
# lowercase-only class turned a legal uppercase revision into "no hex revision
# found", which is an actively misleading diagnostic when one is plainly there.
snapshot_shas=$(sed -n 's/^# nanoarrow-resolved-sha:[[:space:]]*\([0-9a-fA-F]\{7,\}\).*/\1/p' \
    "$snapshot")
wrap_shas=$(awk '
    /^[[:space:]]*\[/ { in_git = ($0 ~ /^[[:space:]]*\[wrap-git\][[:space:]]*$/) ; next }
    in_git && /^[[:space:]]*revision[[:space:]]*=/ {
        line = $0
        sub(/^[[:space:]]*revision[[:space:]]*=[[:space:]]*/, "", line)
        sub(/[^0-9a-fA-F].*$/, "", line)
        if (length(line) >= 7) print line
    }' "$wrap")

# Exactly one of each. A second, diverging sha line is not reachable through
# generate-sbom.sh, which truncates then appends once -- but it is reachable
# through a bad merge, and taking the first match would leave the gate green on
# the stale one.
count_of() { printf '%s\n' "$1" | grep -c . || true; }
for pair in "snapshot:$snapshot_shas" "wrap:$wrap_shas"; do
    label=${pair%%:*}
    n=$(count_of "${pair#*:}")
    if [ "$n" -gt 1 ]; then
        echo "check-sbom-wrap-pin: FAIL: $n conflicting revisions in the $label" >&2
        printf '%s\n' "${pair#*:}" | sed 's/^/    /' >&2
        exit 1
    fi
done
# Compared case-insensitively: the same commit written 3F82... and 3f82... is
# the same commit, and git accepts either. Comparing raw would report a
# disagreement between two spellings of one sha -- fail-closed, but a false
# failure, and the diagnostic would show two shas that look identical to a
# reader skimming them.
lower() { printf '%s\n' "$1" | tr 'A-F' 'a-f'; }
snapshot_sha=$(lower "$(printf '%s\n' "$snapshot_shas" | sed -n 1p)")
wrap_sha=$(lower "$(printf '%s\n' "$wrap_shas" | sed -n 1p)")

if [ -z "$snapshot_sha" ]; then
    echo "check-sbom-wrap-pin: FAIL: no '# nanoarrow-resolved-sha:' in $snapshot" >&2
    echo "  regenerate with scripts/release/generate-sbom.sh from a tree where" >&2
    echo "  subprojects/nanoarrow is checked out" >&2
    exit 1
fi
if [ -z "$wrap_sha" ]; then
    echo "check-sbom-wrap-pin: FAIL: no hex 'revision =' under [wrap-git] in $wrap" >&2
    exit 1
fi

if [ "$snapshot_sha" != "$wrap_sha" ]; then
    echo "check-sbom-wrap-pin: FAIL: the SBOM snapshot and the wrap disagree" >&2
    echo "  sbom/snapshot.txt records:      $snapshot_sha" >&2
    echo "  subprojects/nanoarrow.wrap pins: $wrap_sha" >&2
    echo "  The snapshot's nanoarrow@ line cannot show this: 0.9.0.9000 is a" >&2
    echo "  development version constant across the window, so regenerate the" >&2
    echo "  snapshot after a wrap bump even when its diff looks empty." >&2
    exit 1
fi

echo "check-sbom-wrap-pin: OK; snapshot and wrap agree on $wrap_sha"
