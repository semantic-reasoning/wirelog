#!/usr/bin/env bash
# test-check-changelog-rc.sh - self-test harness for check-changelog-rc.sh.
#
# Coverage:
#   1) no relevant changes => pass
#   2) [Unreleased] mutation => fail
#   3) [1.0.0] mutation => pass
#   4) version 1.0.0-rc1 => pass
#   5) version 1.1.0 => fail

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
gate="$script_dir/check-changelog-rc.sh"

if [ ! -x "$gate" ]; then
    echo "test-check-changelog-rc: FAIL: missing executable gate script: $gate" >&2
    exit 1
fi

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

base_changelog="$workdir/base-CHANGELOG.md"
head_same="$workdir/head-same.md"
head_unreleased_mut="$workdir/head-unreleased-mut.md"
head_100_mut="$workdir/head-1.0.0-mut.md"

cat > "$base_changelog" <<'EOF'
# Changelog

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Performance

### Security

### Documentation

- Placeholder.

## [1.0.0] - 2026-05-27

### Added

- Baseline entry.
EOF

cp "$base_changelog" "$head_same"

cat > "$head_unreleased_mut" <<'EOF'
# Changelog

## [Unreleased]

### Added

- This mutates frozen unreleased.

### Changed

### Deprecated

### Removed

### Fixed

### Performance

### Security

### Documentation

- Placeholder.

## [1.0.0] - 2026-05-27

### Added

- Baseline entry.
EOF

cat > "$head_100_mut" <<'EOF'
# Changelog

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Performance

### Security

### Documentation

- Placeholder.

## [1.0.0] - 2026-05-27

### Added

- Baseline entry.
- RC fix landed.
EOF

make_meson() {
    local out="$1"
    local version="$2"
    cat > "$out" <<EOF
project(
  'wirelog',
  'c',
  version: '$version',
)
EOF
}

run_expect() {
    local expect="$1"
    local label="$2"
    local head_file="$3"
    local version="$4"
    local meson_file="$workdir/meson-$label.build"

    make_meson "$meson_file" "$version"

    set +e
    "$gate" \
      --base-ref 1.0 \
      --base-changelog "$base_changelog" \
      --head-changelog "$head_file" \
      --meson-file "$meson_file" \
      >"$workdir/$label.stdout" 2>"$workdir/$label.stderr"
    rc=$?
    set -e

    if [ "$expect" = "pass" ] && [ "$rc" -eq 0 ]; then
        echo "PASS: $label"
        return 0
    fi
    if [ "$expect" = "fail" ] && [ "$rc" -ne 0 ]; then
        echo "PASS: $label (expected failure)"
        return 0
    fi

    echo "FAIL: $label (expect=$expect rc=$rc)" >&2
    if [ -s "$workdir/$label.stdout" ]; then
        echo "--- stdout ---" >&2
        cat "$workdir/$label.stdout" >&2
    fi
    if [ -s "$workdir/$label.stderr" ]; then
        echo "--- stderr ---" >&2
        cat "$workdir/$label.stderr" >&2
    fi
    exit 1
}

run_expect pass "no-relevant-changes" "$head_same" "1.0.0"
run_expect fail "unreleased-mutation-fails" "$head_unreleased_mut" "1.0.0"
run_expect pass "1.0.0-mutation-passes" "$head_100_mut" "1.0.0"
run_expect pass "version-1.0.0-rc1-passes" "$head_same" "1.0.0-rc1"
run_expect fail "version-1.1.0-fails" "$head_same" "1.1.0"

echo "test-check-changelog-rc: OK"
