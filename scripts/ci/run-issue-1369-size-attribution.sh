#!/usr/bin/env bash
# Disposable, timing-free attribution of the #1369 production .text delta.
set -euo pipefail

base_sha=${1:?usage: $0 BASE_SHA CANDIDATE_SHA OUTPUT_DIR}
candidate_sha=${2:?usage: $0 BASE_SHA CANDIDATE_SHA OUTPUT_DIR}
output_dir=${3:?usage: $0 BASE_SHA CANDIDATE_SHA OUTPUT_DIR}
repo=$(git rev-parse --show-toplevel)
mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd -P)
temp=$(mktemp -d "${RUNNER_TEMP:-/tmp}/wirelog-1369-size.XXXXXX")
cleanup() {
  git -C "$repo" worktree remove --force "$temp/base-source" >/dev/null 2>&1 || true
  git -C "$repo" worktree remove --force "$temp/candidate-source" >/dev/null 2>&1 || true
  rm -rf "$temp"
}
trap cleanup EXIT HUP INT TERM

for sha in "$base_sha" "$candidate_sha"; do
  [[ "$sha" =~ ^[0-9a-f]{40}$ ]]
  git -C "$repo" cat-file -e "$sha^{commit}"
done
git -C "$repo" worktree add --detach "$temp/base-source" "$base_sha"
git -C "$repo" worktree add --detach "$temp/candidate-source" "$candidate_sha"

cat >"$output_dir/metadata.txt" <<EOF
base_sha=$base_sha
candidate_sha=$candidate_sha
dispatch_sha=$(git -C "$repo" rev-parse HEAD)
gcc=$(gcc -dumpfullversion -dumpversion)
meson=$(meson --version)
ninja=$(ninja --version)
kernel=$(uname -a)
EOF

build_one() {
  local label=$1 sha=$2 source=$3
  local build="$temp/$label-build"
  local map_build="$temp/$label-map-build"
  local log="$output_dir/$label-build.log"
  local map_log="$output_dir/$label-map-build.log"
  local library="$build/libwirelog.so"
  local map_library="$map_build/libwirelog.so"

  # Match ci-pr's default production profile: release, optimization=s, LTO,
  # trace ceiling, tests enabled, and mbedTLS disabled.
  meson setup "$build" "$source" --buildtype=release -Doptimization=s \
    -Db_lto=true -Dwirelog_log_max_level=trace -Dtests=true \
    -DmbedTLS=disabled 2>&1 | tee "$log"
  meson compile -C "$build" wirelog 2>&1 | tee -a "$log"
  meson introspect --buildoptions "$build" >"$output_dir/$label-options.json"
  python scripts/ci/size-profile.py capture --build-dir "$build" \
    --source-dir "$source" --source-sha "$sha" \
    --output "$output_dir/$label-profile.json" 2>&1 | tee -a "$log"
  scripts/ci/check-text-size.sh "$library" --measure-only \
    --json "$output_dir/$label-size.json" --source-sha "$sha" \
    --profile "$output_dir/$label-profile.json" 2>&1 | tee -a "$log"
  nm -S --size-sort --demangle --defined-only "$library" >"$output_dir/$label-nm.txt"
  sha256sum "$library" >"$output_dir/$label-library.sha256"

  # A separate map-enabled build is diagnostic only. Confirm its linked
  # binary is byte-for-byte equal to the untouched build before trusting map
  # attribution; otherwise retain the map but mark it non-comparable.
  meson setup "$map_build" "$source" --buildtype=release -Doptimization=s \
    -Db_lto=true -Dwirelog_log_max_level=trace -Dtests=true -DmbedTLS=disabled \
    "-Dc_link_args=-Wl,-Map=$output_dir/$label.map" 2>&1 | tee "$map_log"
  meson compile -C "$map_build" wirelog 2>&1 | tee -a "$map_log"
  sha256sum "$map_library" >"$output_dir/$label-map-library.sha256"
  if cmp -s "$library" "$map_library"; then
    printf 'map_binary_matches_authoritative=yes\n' >"$output_dir/$label-map-status.txt"
  else
    printf 'map_binary_matches_authoritative=no\n' >"$output_dir/$label-map-status.txt"
  fi
  size --format=sysv "$map_library" >"$output_dir/$label-map-size.txt"
}

build_one base "$base_sha" "$temp/base-source"
build_one candidate "$candidate_sha" "$temp/candidate-source"
python scripts/ci/size-profile.py compare \
  "$output_dir/base-profile.json" "$output_dir/candidate-profile.json" \
  | tee "$output_dir/profile-comparison.txt"
sha256sum "$output_dir"/* >"$output_dir/SHA256SUMS"
