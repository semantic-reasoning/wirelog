#!/usr/bin/env bash
# Self-test for check-bash-constructs.py. Keep this harness Bash 3.2-safe.
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
production_analyzer=$script_dir/check-bash-constructs.py
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
root=$tmp/root
build=$tmp/build
mkdir -p "$root/ci" "$root/release" "$root/tier2" "$build/meson-info"

write_fixture() {
    name=$1
    body=$2
    printf '%s\n' "$body" >"$root/$name"
}

: >"$root/ci/seed.sh"
printf '%s\n' 'script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)' >>"$root/ci/seed.sh"
printf '%s\n' 'script_dir_zero=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)' >>"$root/ci/seed.sh"
printf '%s\n' 'helper="$script_dir/helper.sh"' >>"$root/ci/seed.sh"
printf '%s\n' 'helper_zero="$script_dir_zero/helper.sh"' >>"$root/ci/seed.sh"
printf '%s\n' 'changelog=$("$script_dir/../release/extract-changelog-section.sh")' >>"$root/ci/seed.sh"
printf '%s\n' 'source "$script_dir/floor01.sh"' >>"$root/ci/seed.sh"
printf '%s\n' '"$helper"' >>"$root/ci/seed.sh"
write_fixture ci/helper.sh 'printf "%s\\n" ok'
write_fixture ci/compound_only.sh 'printf "%s\\n" compound'
write_fixture ci/source_only.sh 'printf "%s\\n" source'
write_fixture ci/env_only.sh 'printf "%s\\n" env'
write_fixture release/extract-changelog-section.sh 'printf "%s\\n" ok'
write_fixture tier2/ignored.sh 'printf "%s\\n" '\''mapfile -t values'\'''
for floor in 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29; do
    next=$(printf '%02d' "$((10#$floor + 1))")
    printf 'source "$script_dir/floor%s.sh"\n' "$next" >"$root/ci/floor$floor.sh"
done
write_fixture ci/floor30.sh 'printf "%s\\n" floor'
printf '%s\n' '[{"cmd":["/bin/bash","'"$root"'/ci/seed.sh"]}]' >"$build/meson-info/intro-tests.json"

expect_status() {
    expected=$1
    shift
    set +e
    "$@" >"$tmp/out" 2>"$tmp/err"
    actual=$?
    set -e
    [ "$actual" -eq "$expected" ] || {
        cat "$tmp/out" "$tmp/err" >&2
        exit 1
    }
}

# Exercise the real floor once, propagating unsupported-platform SKIP (77).
# Missing fixture inputs must fail here, not become an analyzer SKIP.
[ -d "$build" ]
[ -f "$build/meson-info/intro-tests.json" ]
python3 "$production_analyzer" "$build" "$root"

# The mutations exercise parsing and traversal, not the production count floor.
# Avoid rechecking 30 padding scripts with bash -n for every mutation. Retain
# the production-floor rejection check before using a byte-identical analyzer
# copy with private fixture floors; checked-in policy remains unchanged.
sed '/^source "\$script_dir\/floor01\.sh"$/d' "$root/ci/seed.sh" >"$tmp/seed.good"
cp "$tmp/seed.good" "$root/ci/seed.sh"
expect_status 1 python3 "$production_analyzer" "$build" "$root"
grep -Fq 'tier-1 closure has 3 files; minimum is 30' "$tmp/out" "$tmp/err"
mkdir "$tmp/mutations"
analyzer=$tmp/mutations/check-bash-constructs.py
cp "$production_analyzer" "$analyzer"
printf '%s\n' 'linux 1' 'darwin 1' >"$tmp/mutations/bash-tier1-minimum.txt"
expect_status 0 python3 "$analyzer" "$build" "$root"
printf '%s\n' '[{"cmd":["'"$root"'/ci/seed.sh"]}]' >"$build/meson-info/intro-tests.json"
expect_status 0 python3 "$analyzer" "$build" "$root"
printf '%s\n' '[{"cmd":["/bin/bash","'"$root"'/ci/seed.sh"]}]' >"$build/meson-info/intro-tests.json"
write_fixture ci/bad.sh 'if true; then'
printf '%s\n' 'source "$script_dir/bad.sh"' >>"$root/ci/seed.sh"
expect_status 1 python3 "$analyzer" "$build" "$root"
grep -Fq 'PATH bash -n syntax validation failed' "$tmp/out" "$tmp/err"
grep -Fq 'bad.sh' "$tmp/out" "$tmp/err"
cp "$tmp/seed.good" "$root/ci/seed.sh"
printf '%s\n' '"$DYNAMIC_SCRIPT"' >>"$root/ci/seed.sh"
expect_status 1 python3 "$analyzer" "$build" "$root"
grep -Fq 'unresolved dynamic command operand' "$tmp/out" "$tmp/err"
cp "$tmp/seed.good" "$root/ci/seed.sh"
for construct in \
    'if bash "$script_dir/compound_only.sh"; then :; fi' \
    'printf x | bash "$script_dir/compound_only.sh"' \
    'FOO= "$script_dir/compound_only.sh"' \
    'env FOO=x "$script_dir/compound_only.sh"'; do
    cp "$tmp/seed.good" "$root/ci/seed.sh"
    printf '%s\n' "$construct" >>"$root/ci/seed.sh"
    expect_status 0 python3 "$analyzer" "$build" "$root"
done
cp "$tmp/seed.good" "$root/ci/seed.sh"
for construct in \
    'if source "$script_dir/source_only.sh"; then :; fi' \
    '. "$script_dir/source_only.sh" | cat'; do
    cp "$tmp/seed.good" "$root/ci/seed.sh"
    printf '%s\n' "$construct" >>"$root/ci/seed.sh"
    expect_status 0 python3 "$analyzer" "$build" "$root"
done
cp "$tmp/seed.good" "$root/ci/seed.sh"
write_fixture ci/source_only.sh 'mapfile -t values'
printf '%s\n' 'if source "$script_dir/source_only.sh"; then :; fi' >>"$root/ci/seed.sh"
expect_status 1 python3 "$analyzer" "$build" "$root"
grep -Fq 'source_only.sh' "$tmp/out" "$tmp/err"
write_fixture ci/source_only.sh 'printf "%s\\n" source'
cp "$tmp/seed.good" "$root/ci/seed.sh"
for construct in \
    'source "$script_dir/missing-compound.sh"' \
    '. "/tmp/outside-compound.sh"'; do
    printf '%s\n' "$construct" >>"$root/ci/seed.sh"
    expect_status 1 python3 "$analyzer" "$build" "$root"
    cp "$tmp/seed.good" "$root/ci/seed.sh"
done
for construct in \
    'command bash "$(provider)"' \
    'exec /usr/bin/bash "$(provider)"'; do
    printf '%s\n' "$construct" >>"$root/ci/seed.sh"
    expect_status 1 python3 "$analyzer" "$build" "$root"
    grep -Fq 'unresolved dynamic script operand' "$tmp/out" "$tmp/err"
    cp "$tmp/seed.good" "$root/ci/seed.sh"
done
for construct in \
    'command bash "$script_dir/compound_only.sh"' \
    'exec /usr/bin/bash "$script_dir/compound_only.sh"' \
    'env bash "$script_dir/env_only.sh"' \
    'env FOO=bar /usr/bin/bash "$script_dir/env_only.sh"' \
    'env FOO=bar "$script_dir/env_only.sh"'; do
    printf '%s\n' "$construct" >>"$root/ci/seed.sh"
    expect_status 0 python3 "$analyzer" "$build" "$root"
    cp "$tmp/seed.good" "$root/ci/seed.sh"
done
write_fixture ci/env_only.sh 'mapfile -t values'
printf '%s\n' 'env FOO=bar "$script_dir/env_only.sh"' >>"$root/ci/seed.sh"
expect_status 1 python3 "$analyzer" "$build" "$root"
grep -Fq 'env_only.sh' "$tmp/out" "$tmp/err"
write_fixture ci/env_only.sh 'printf "%s\\n" env'
cp "$tmp/seed.good" "$root/ci/seed.sh"
write_fixture ci/compound_only.sh 'mapfile -t values'
printf '%s\n' 'if bash "$script_dir/compound_only.sh"; then :; fi' >>"$root/ci/seed.sh"
expect_status 1 python3 "$analyzer" "$build" "$root"
grep -Fq 'compound_only.sh' "$tmp/out" "$tmp/err"
write_fixture ci/compound_only.sh 'printf "%s\\n" compound'
cp "$tmp/seed.good" "$root/ci/seed.sh"
for construct in \
    'if bash "$script_dir/helper.sh"; then :; fi' \
    'printf x | bash "$script_dir/helper.sh"' \
    'FOO=x "$script_dir/helper.sh"'; do
    cp "$tmp/seed.good" "$root/ci/seed.sh"
    printf '%s\n' "$construct" >>"$root/ci/seed.sh"
    expect_status 0 python3 "$analyzer" "$build" "$root"
done
cp "$tmp/seed.good" "$root/ci/seed.sh"
for construct in \
    'result=$("$SCRIPT_PROVIDER")' \
    'helper=$("$SCRIPT_PROVIDER")' \
    'if bash "$(provider)"; then :; fi' \
    'if [[ -n "$x" ]]; then bash "$(provider)"; fi' \
    'printf x | bash "$(provider)"' \
    'bash "$(script_provider)"' \
    'bash -n "$("$PROVIDER")"' \
    '/usr/bin/bash "$(provider)"' \
    'FOO=x bash "$(provider)"' \
    'env bash "$(provider)"'; do
    write_fixture ci/seed.sh "$construct"
    expect_status 1 python3 "$analyzer" "$build" "$root"
    grep -Fq 'unresolved dynamic script operand' "$tmp/out" "$tmp/err"
    cp "$tmp/seed.good" "$root/ci/seed.sh"
done
write_fixture ci/seed.sh 'if [[ -n "$x" ]]; then "$("$SCRIPT_PROVIDER")"; fi'
expect_status 1 python3 "$analyzer" "$build" "$root"
grep -Fq 'unresolved dynamic command operand' "$tmp/out" "$tmp/err"
cp "$tmp/seed.good" "$root/ci/seed.sh"
for construct in \
    'FOO= "$(provider)"' \
    'FOO=x "$(provider)"' \
    'env FOO= "$(provider)"'; do
    write_fixture ci/seed.sh "$construct"
    expect_status 1 python3 "$analyzer" "$build" "$root"
    grep -Fq 'unresolved dynamic command operand' "$tmp/out" "$tmp/err"
    cp "$tmp/seed.good" "$root/ci/seed.sh"
done
: >"$root/ci/seed.sh"
printf '%s\n' '"$("$SCRIPT_PROVIDER")"' >>"$root/ci/seed.sh"
expect_status 1 python3 "$analyzer" "$build" "$root"
grep -Fq 'unresolved dynamic command operand' "$tmp/out" "$tmp/err"
cp "$tmp/seed.good" "$root/ci/seed.sh"
: >"$root/ci/seed.sh"
printf '%s\n' "bash \\" >>"$root/ci/seed.sh"
printf '%s\n' '  "$(provider)"' >>"$root/ci/seed.sh"
expect_status 1 python3 "$analyzer" "$build" "$root"
grep -Fq 'unresolved dynamic script operand' "$tmp/out" "$tmp/err"
cp "$tmp/seed.good" "$root/ci/seed.sh"
: >"$root/ci/seed.sh"
printf '%s\n' "/usr/bin/bash \\" >>"$root/ci/seed.sh"
printf '%s\n' '  "$(provider)"' >>"$root/ci/seed.sh"
expect_status 1 python3 "$analyzer" "$build" "$root"
grep -Fq 'unresolved dynamic script operand' "$tmp/out" "$tmp/err"
cp "$tmp/seed.good" "$root/ci/seed.sh"
write_fixture ci/helper.sh 'bash -c '\''printf "%s\\n" ok'\'''
expect_status 0 python3 "$analyzer" "$build" "$root"
write_fixture ci/helper.sh 'printf "%s\\n" ok'
write_fixture ci/helper.sh '((value << 1))'
expect_status 0 python3 "$analyzer" "$build" "$root"
write_fixture ci/helper.sh 'printf "%s\\n" ok'
write_fixture release/extract-changelog-section.sh 'mapfile -t values'
expect_status 1 python3 "$analyzer" "$build" "$root"
write_fixture release/extract-changelog-section.sh 'printf "%s\\n" ok'

for construct in \
    'mapfile -t values' 'readarray values' 'coproc worker' \
    'declare -A values' 'local -A values' 'typeset -A values' \
    'wait -n' 'shopt -s globstar' 'printf x |& cat' \
    'printf x &>>out' 'case x in x) : ;;& esac' \
    'printf "%s" "${value^^pattern}"' 'printf "%s" "${value,,pattern}"' \
    'printf "%s" "${value^pattern}"' 'printf "%s" "${value,pattern}"' \
    'printf "%s" "${value@Q}"' 'printf "%s" "${1^^}"' \
    'printf "%s" "${1,,}"' 'printf "%s" "${1^}"' \
    'printf "%s" "${1,}"' 'printf "%s" "${value^^${pattern}}"'; do
    write_fixture ci/helper.sh "$construct"
    expect_status 1 python3 "$analyzer" "$build" "$root"
done

: >"$root/ci/helper.sh"
printf '%s\n' "printf '%s' '<<EOF'" >>"$root/ci/helper.sh"
printf '%s\n' 'mapfile -t values' >>"$root/ci/helper.sh"
expect_status 1 python3 "$analyzer" "$build" "$root"

write_fixture ci/helper.sh 'shopt -s nullglob globstar'
expect_status 1 python3 "$analyzer" "$build" "$root"
grep -Fq 'shopt -s globstar' "$tmp/out" "$tmp/err"
if grep -Fq nullglob "$tmp/out" "$tmp/err"; then
    echo 'self-test: nullglob must not be reported' >&2
    exit 1
fi

: >"$root/ci/helper.sh"
printf '%s\n' '# mapfile -t values' >>"$root/ci/helper.sh"
printf '%s\n' 'declare -a values=()' >>"$root/ci/helper.sh"
printf '%s\n' 'local -a other=()' >>"$root/ci/helper.sh"
printf '%s\n' 'printf "%s" "mapfile"' >>"$root/ci/helper.sh"
printf '%s\n' 'echo mapfile' >>"$root/ci/helper.sh"
printf '%s\n' 'printf "%s" mapfile' >>"$root/ci/helper.sh"
printf '%s\n' 'echo shopt -s globstar' >>"$root/ci/helper.sh"
printf '%s\n' 'printf "%s" '\''cat <<PSEUDO'\''' >>"$root/ci/helper.sh"
printf '%s\n' 'cat <<EOF' >>"$root/ci/helper.sh"
printf '%s\n' 'mapfile -t values' >>"$root/ci/helper.sh"
printf '%s\n' 'EOF' >>"$root/ci/helper.sh"
printf '%s\n' 'cat <<A <<'"'"'B'"'"' <<-C' >>"$root/ci/helper.sh"
printf '%s\n' 'mapfile -t values' >>"$root/ci/helper.sh"
printf '%s\n' 'A' >>"$root/ci/helper.sh"
printf '%s\n' 'readarray values' >>"$root/ci/helper.sh"
printf '%s\n' 'B' >>"$root/ci/helper.sh"
printf '%s\n' 'coproc worker' >>"$root/ci/helper.sh"
printf '\tC\n' >>"$root/ci/helper.sh"
expect_status 0 python3 "$analyzer" "$build" "$root"

write_fixture ci/helper.sh 'printf "%s\\n" ok'
printf '%s\n' '[{"cmd":["/bin/bash","'"$root"'/ci/seed.sh"]}]' >"$build/meson-info/intro-tests.json"
expect_status 0 python3 "$analyzer" "$build" "$root"
printf '%s\n' '[{"cmd":["/bin/bash","'"$root"'/tier2/ignored.sh"]}]' >"$build/meson-info/intro-tests.json"
expect_status 1 python3 "$production_analyzer" "$build" "$root"
grep -Fq 'tier-1 closure has 1 files; minimum is 30' "$tmp/out" "$tmp/err"

write_fixture ci/seed.sh 'source "$script_dir/missing.sh"'
printf '%s\n' '[{"cmd":["/bin/bash","'"$root"'/ci/seed.sh"]}]' >"$build/meson-info/intro-tests.json"
expect_status 1 python3 "$analyzer" "$build" "$root"
printf '%s\n' '[]' >"$build/meson-info/intro-tests.json"
expect_status 1 python3 "$analyzer" "$build" "$root"
printf '%s\n' '[{"cmd":["/bin/bash","/tmp/outside.sh"]}]' >"$build/meson-info/intro-tests.json"
expect_status 1 python3 "$analyzer" "$build" "$root"
printf '%s\n' '[{"cmd":["/bin/bash","'"$root"'/ci/seed.sh"]}]' >"$build/meson-info/intro-tests.json"
write_fixture ci/seed.sh 'source "$DYNAMIC_SCRIPT"'
expect_status 1 python3 "$analyzer" "$build" "$root"

rm "$build/meson-info/intro-tests.json"
expect_status 77 python3 "$analyzer" "$build" "$root"

analyzer_copy=$tmp/check-bash-constructs.py
cp "$analyzer" "$analyzer_copy"
cp "$tmp/seed.good" "$root/ci/seed.sh"
printf '%s\n' '[{"cmd":["/bin/bash","'"$root"'/ci/seed.sh"]}]' >"$build/meson-info/intro-tests.json"
printf '%s\n' 'linux nope' 'darwin 30' >"$tmp/bash-tier1-minimum.txt"
expect_status 1 python3 "$analyzer_copy" "$build" "$root"
grep -Fq 'invalid floor record' "$tmp/out" "$tmp/err"
printf '%s\n' 'linux 999999' 'darwin 999999' >"$tmp/bash-tier1-minimum.txt"
expect_status 1 python3 "$analyzer_copy" "$build" "$root"
grep -Fq 'minimum is 999999' "$tmp/out" "$tmp/err"

write_fixture ci/seed.sh 'source "$DYNAMIC_SCRIPT"'
expect_status 77 python3 -c \
    'import importlib.util, sys; spec = importlib.util.spec_from_file_location("gate", sys.argv[1]); gate = importlib.util.module_from_spec(spec); sys.modules["gate"] = gate; spec.loader.exec_module(gate); gate.sys.platform = "win32"; sys.exit(gate.main([sys.argv[1], sys.argv[2], sys.argv[3]]))' \
    "$analyzer" "$analyzer" "$build" "$root"

# A child stops at its first analyzer call, before reaching these assertions.
# Preserve both unsupported-platform skips and genuine analyzer failures.
expect_status 77 bash -c \
    'python3() { return 77; }; export -f python3; bash "$1"' \
    bash "$script_dir/test-check-bash-constructs.sh"
expect_status 1 bash -c \
    'python3() { return 1; }; export -f python3; bash "$1"' \
    bash "$script_dir/test-check-bash-constructs.sh"

bash -n "$script_dir/test-check-bash-constructs.sh"
echo "test-check-bash-constructs: OK"
