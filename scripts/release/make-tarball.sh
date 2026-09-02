#!/usr/bin/env bash
# Build the deterministic source archive and its public checksums.
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
out_dir=${1:-"$repo_root/dist"}
ref=${2:-HEAD}
commit=$(git -C "$repo_root" rev-parse --verify "$ref^{commit}")
# Read the file into a variable, then match it: no pipeline, so nothing can
# stop reading early and SIGPIPE its producer.
#
# Two forms were tried and rejected, and the thresholds are much lower than
# they look. `sed ... | head -1` dies once sed crosses its first 4 KiB stdout
# flush after head has exited -- about 700 matching `version:` lines, measured
# 4/5 at 700 and 5/5 at 800, not the 10^5 first assumed. Replacing head with
# sed's own `q` moves the early exit one stage left, and its threshold is the
# whole blob against ~70 KB -- the 64 KiB pipe capacity plus sed's 4 KiB read
# block -- rather than a match count: this meson.build is already 25 KiB, so
# that form becomes reachable at under 3x growth of the file. Removing the pipe
# is the only form with no threshold.
#
# The `q` stays inside the address block. A bare `s/.../p;q` quits after the
# FIRST LINE rather than the first match, and the version is on line 4 -- that
# form returns empty and fails the check below with "invalid project version:".
# The narrowing this accepts: block-`q` stops at the first line matching the
# ADDRESS even if the substitution fails, so a malformed `  version: '` with no
# closing quote now masks a valid line below it. That is invalid meson and it
# fails closed, but it is a behaviour change from the piped form.
meson_build=$(git -C "$repo_root" show "$commit:meson.build")
version=$(sed -n "/^  version: '/{s/^  version: '\([^']*\)'.*/\1/p;q;}" <<<"$meson_build")
version=${version%%-*}
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
  echo "invalid project version: $version" >&2
  exit 1
}
command -v sha256sum >/dev/null || { echo 'sha256sum is required' >&2; exit 1; }
command -v b3sum >/dev/null || { echo 'b3sum is required' >&2; exit 1; }

# `--`: an out_dir beginning with `-` is otherwise parsed as options here, and
# mkdir fails before the manifest subshells below are ever reached -- which is
# why hardening only those two would have left `--` untestable. (#1316)
mkdir -p -- "$out_dir"
archive="$out_dir/wirelog-$version.tar.gz"
prefix="wirelog-$version/"

# git-archive uses the explicitly supplied tag/ref tree and preserves only tracked source;
# gzip -n removes wall-clock metadata so repeated builds are byte-identical.
git -C "$repo_root" archive --format=tar --prefix="$prefix" "$commit" \
  | gzip -n -9 > "$archive"
# Write each manifest from inside the archive's directory, against the bare
# basename, so no path is embedded in the hash line. Three hardenings, each with
# a distinct failure this repository has already seen on the reading side
# (#1311):
#
#   CDPATH=  With CDPATH set, `cd` on a RELATIVE path searches CDPATH and lands
#            in whichever entry matched first, not the intended directory -- so
#            the manifest is written in the wrong place, or sha256sum finds no
#            archive there and the run aborts. It also echoes the resolved path
#            on ITS OWN stdout, which the archive_dir capture below would take
#            as part of the value. Note it does NOT corrupt the manifest: the
#            redirection binds to sha256sum, not to cd. release-tag.yml:274,344
#            pass a relative out_dir, so this is the production shape.
#   --       An out_dir beginning with `-` is parsed as options -- first by the
#            mkdir above, which is why that line needs it too, then by dirname,
#            basename and cd here -- all three receive the value.
#   -P       `cd` is logical, so an out_dir reached through a symlink followed
#            by `..` resolves differently for the shell than for the kernel:
#            with `link -> y/target`, `x/dir/link/..` is `y` (the link target's
#            PARENT) where the archive is written, but `x/dir` to a logical cd.
#            The manifest would land beside a different file, or the subshell
#            fails outright.
archive_dir=$(CDPATH= cd -P -- "$(dirname -- "$archive")" && pwd -P)
archive_base=$(basename -- "$archive")
(CDPATH= cd -P -- "$archive_dir" && sha256sum -- "$archive_base" > "$archive_base.sha256")
(CDPATH= cd -P -- "$archive_dir" && b3sum -- "$archive_base" > "$archive_base.blake3")

printf '%s\n' "$archive" "$archive.sha256" "$archive.blake3"
