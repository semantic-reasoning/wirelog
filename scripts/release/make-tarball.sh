#!/usr/bin/env bash
# Build the deterministic source archive and its public checksums.
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
out_dir=${1:-"$repo_root/dist"}
ref=${2:-HEAD}
commit=$(git -C "$repo_root" rev-parse --verify "$ref^{commit}")
version=$(git -C "$repo_root" show "$commit:meson.build" \
  | sed -n "s/^  version: '\([^']*\)'.*/\1/p" | head -1)
version=${version%%-*}
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
  echo "invalid project version: $version" >&2
  exit 1
}
command -v sha256sum >/dev/null || { echo 'sha256sum is required' >&2; exit 1; }
command -v b3sum >/dev/null || { echo 'b3sum is required' >&2; exit 1; }

mkdir -p "$out_dir"
archive="$out_dir/wirelog-$version.tar.gz"
prefix="wirelog-$version/"

# git-archive uses the explicitly supplied tag/ref tree and preserves only tracked source;
# gzip -n removes wall-clock metadata so repeated builds are byte-identical.
git -C "$repo_root" archive --format=tar --prefix="$prefix" "$commit" \
  | gzip -n -9 > "$archive"
(cd "$(dirname "$archive")" && sha256sum "$(basename "$archive")" > "$(basename "$archive").sha256")
(cd "$(dirname "$archive")" && b3sum "$(basename "$archive")" > "$(basename "$archive").blake3")

printf '%s\n' "$archive" "$archive.sha256" "$archive.blake3"
