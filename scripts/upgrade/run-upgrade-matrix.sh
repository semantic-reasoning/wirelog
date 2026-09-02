#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage: run-upgrade-matrix.sh [--old-ref REF] [--candidate-ref REF]

REF must resolve to a commit in the current repository. The candidate is
tested from its checked-out git tree; the release archive is created from the
same tree after this gate passes.
EOF
}

old_ref=v0.30.0
candidate_ref=HEAD
while (($#)); do
    case "$1" in
        --old-ref) old_ref=${2:?missing value for --old-ref}; shift 2 ;;
        --candidate-ref) candidate_ref=${2:?missing value for --candidate-ref}; shift 2 ;;
        -h|--help) usage >&2; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done

root=$(git rev-parse --show-toplevel)
old_commit=$(git rev-parse --verify "${old_ref}^{commit}")
candidate_commit=$(git rev-parse --verify "${candidate_ref}^{commit}")
[[ -n "$old_commit" && -n "$candidate_commit" ]] || {
    echo 'upgrade matrix: refs must resolve to commits' >&2
    exit 2
}

tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-upgrade.XXXXXX")
log_dir=${UPGRADE_MATRIX_LOG_DIR:-$tmp/evidence}
if [[ -e "$log_dir" && -n "$(find "$log_dir" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
    echo "upgrade matrix: evidence directory must be empty: $log_dir" >&2
    exit 2
fi
mkdir -p "$log_dir"
trap 'rm -rf "$tmp"' EXIT

extract() {
    local commit=$1 destination=$2
    mkdir -p "$destination"
    git archive --format=tar "$commit" | tar -xf - -C "$destination"
}

build_tree() {
    local source=$1 name=$2 prefix="$tmp/prefix-$2" build="$tmp/build-$2"
    meson setup "$build" "$source" --buildtype=release -Dtests=false \
        -Dprefix="$prefix" >"$log_dir/$name-configure.log" 2>&1
    meson compile -C "$build" >"$log_dir/$name-build.log" 2>&1
    meson install -C "$build" >"$log_dir/$name-install.log" 2>&1
    local lib
    lib=$(find -L "$prefix" \( -name 'libwirelog.so' -o -name 'libwirelog.dylib' \) -print -quit)
    [[ -n "$lib" ]] || {
        echo "upgrade matrix: installed libwirelog not found for $name" >&2
        exit 1
    }
    # Record the shared libraries this tree builds for itself -- libwirelog
    # plus the subproject libraries it carries as DT_NEEDED.  The closure
    # check in compile_and_run uses these basenames to tell "loaded from the
    # release prefix" apart from "loaded from whatever the host had lying
    # around"; see scripts/ci/check-shared-library-closure.sh.  No -maxdepth:
    # a subproject that relocates its build output must not drop silently out
    # of the set.  ! -type d keeps meson's libwirelog.so.N.p object
    # directories, which match *.so.*, from being mistaken for libraries.
    # The grep keeps the match to real library names: '*.so.*' also catches
    # build by-products such as libnanoarrow.so.symbols.
    local owned="$tmp/owned-$name.txt"
    find "$build" ! -type d \
        \( -name '*.so' -o -name '*.so.*' -o -name '*.dylib' \) -print |
        sed 's|.*/||' |
        { grep -E '\.(so|dylib)(\.[0-9]+)*$' || true; } |
        LC_ALL=C sort -u >"$owned"
    [[ -s "$owned" ]] || {
        echo "upgrade matrix: $name built no shared libraries to verify" >&2
        exit 1
    }
    cp "$owned" "$log_dir/$name-owned-sonames.txt"
    printf '%s\n' "$prefix|$(dirname "$lib")"
}

compile_and_run() {
    local label=$1 prefix=$2 libdir=$3 source=$4 program=$5 output=$6 owned=$7
    local exe="$tmp/$label"
    local runtime_log="$log_dir/$label-runtime.log"
    local trace="$tmp/$label-loader.trace"
    "${CC:-cc}" -std=c11 -Wall -Wextra -Werror -I"$prefix/include" \
        "$source" -L"$libdir" -lwirelog -Wl,-rpath,"$libdir" -o "$exe" \
        >"$log_dir/$label-compile.log" 2>&1
    # Ask the loader to record what it opens during the real run.  A separate
    # ldd pass would re-resolve in its own environment and could describe a
    # different closure than the one that actually executed.
    if [[ "$(uname -s)" == Darwin ]]; then
        DYLD_LIBRARY_PATH="$libdir${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" \
            DYLD_PRINT_LIBRARIES=1 \
            "$exe" "$program" >"$output" 2>"$trace"
        # dyld shares stderr with the program, so keep the consumer's own
        # diagnostics readable in the evidence artifact.
        grep -v '^dyld' "$trace" >"$runtime_log" || true
    else
        # glibc appends .PID to LD_DEBUG_OUTPUT; without it the trace would go
        # to stderr and bury the consumer's diagnostics.
        local trace_prefix="$tmp/$label-loader"
        rm -f "$trace_prefix".[0-9]*
        LD_LIBRARY_PATH="$libdir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
            LD_DEBUG=libs LD_DEBUG_OUTPUT="$trace_prefix" \
            "$exe" "$program" >"$output" 2>"$runtime_log"
        cat "$trace_prefix".[0-9]* >"$trace" 2>/dev/null || : >"$trace"
    fi
    "$root/scripts/ci/check-shared-library-closure.sh" \
        "$trace" "$prefix" "$owned" "$log_dir/$label-closure.txt"
    [[ -s "$output" ]] || {
        echo "upgrade matrix: $label produced no output" >&2
        exit 1
    }
}

old_source=$tmp/old
candidate_source=$tmp/candidate
extract "$old_commit" "$old_source"
extract "$candidate_commit" "$candidate_source"
old_install=$(build_tree "$old_source" old)
candidate_install=$(build_tree "$candidate_source" candidate)
old_prefix=${old_install%%|*}
old_libdir=${old_install#*|}
candidate_prefix=${candidate_install%%|*}
candidate_libdir=${candidate_install#*|}
old_owned=$tmp/owned-old.txt
candidate_owned=$tmp/owned-candidate.txt

run_case() {
    local name=$1 old_program=$2 candidate_program=$3 old_consumer=$4 candidate_consumer=$5
    local old_out="$log_dir/$name-old.out" candidate_out="$log_dir/$name-candidate.out"
    compile_and_run "$name-old" "$old_prefix" "$old_libdir" \
        "$old_consumer" "$old_program" "$old_out" "$old_owned"
    compile_and_run "$name-candidate" "$candidate_prefix" "$candidate_libdir" \
        "$candidate_consumer" "$candidate_program" "$candidate_out" "$candidate_owned"
    cmp -s "$old_out" "$candidate_out" || {
        echo "upgrade matrix: $name output mismatch" >&2
        diff -u "$old_out" "$candidate_out" >&2 || true
        exit 1
    }
    printf 'PASS %s (%s -> %s)\n' "$name" "$old_commit" "$candidate_commit"
}

run_executor_case() {
    local program=$1 consumer=$2 probe=$3 old_lib candidate_out old_lib_symbols
    candidate_out="$log_dir/executor-candidate.out"
    compile_and_run executor-candidate "$candidate_prefix" "$candidate_libdir" \
        "$consumer" "$program" "$candidate_out" "$candidate_owned"

    old_lib=$(find -L "$old_libdir" -maxdepth 1 -name 'libwirelog.so' -print -quit)
    [[ -n "$old_lib" ]] || { echo 'upgrade matrix: old library missing' >&2; exit 1; }
    old_lib_symbols="$log_dir/executor-old-exported-symbols.txt"
    nm -D --defined-only "$old_lib" >"$old_lib_symbols"
    local abi_report="$log_dir/executor-abi.tsv"
    local declared_symbols="$tmp/executor-declared-symbols.txt"
    "$root/scripts/ci/check-executor-abi.sh" \
        "$old_prefix/include/wirelog/wirelog.h" "$old_lib" \
        "$abi_report" "$declared_symbols"
    mapfile -t executor_symbols <"$declared_symbols"

    local old_obj="$tmp/executor-old-probe.o" old_exe="$tmp/executor-old.so"
    local old_compile_log="$log_dir/executor-old-probe-compile.log"
    local old_link_log="$log_dir/executor-old-link.log"
    "${CC:-cc}" -std=c11 -Wall -Wextra -Werror -I"$old_prefix/include" \
        -c "$probe" -o "$old_obj" >"$old_compile_log" 2>&1
    if "${CC:-cc}" -shared "$old_obj" -L"$old_libdir" -lwirelog \
        -Wl,--no-undefined -Wl,-z,defs -Wl,-rpath,"$old_libdir" -o "$old_exe" \
        >"$old_link_log" 2>&1; then
        echo 'upgrade matrix: v0.30.0 executor unexpectedly linked' >&2
        exit 1
    fi
    for symbol in "${executor_symbols[@]}"; do
        grep -Fq "$symbol" "$old_link_log" || {
            echo "upgrade matrix: expected missing-symbol diagnostic absent: $symbol" >&2
            exit 1
        }
    done
    [[ "$(cat "$candidate_out")" == 'reach-count 3' ]] || {
        echo 'upgrade matrix: candidate executor output changed' >&2
        exit 1
    }
    local linker_digest
    linker_digest=$(sha256sum "$old_link_log" | awk '{print $1}')
    missing_json=$(printf '"%s",' "${executor_symbols[@]}" | sed 's/,$//')
    printf '{"status":"EXPECTED_UNSUPPORTED","old_commit":"%s","candidate_commit":"%s","missing_symbols":[%s],"candidate_output":"reach-count 3","linker_diagnostic_file":"executor-old-link.log","linker_diagnostic_sha256":"%s"}\n' \
        "$old_commit" "$candidate_commit" "$missing_json" "$linker_digest" >"$log_dir/executor-status.json"
    "$root/scripts/ci/validate-executor-status.sh" \
        "$log_dir/executor-status.json" "$old_link_log" \
        "$old_commit" "$candidate_commit" "$candidate_out" "$declared_symbols" "$abi_report"
    printf 'EXPECTED_UNSUPPORTED executor (v0.30.0 declarations are not exported; candidate passed)\n'
}

run_case easy \
    "$candidate_source/tests/upgrade/easy/program.dl" \
    "$candidate_source/tests/upgrade/easy/program.dl" \
    "$candidate_source/tests/upgrade/easy/legacy.c" \
    "$candidate_source/tests/upgrade/easy/migrated.c"
run_case session \
    "$candidate_source/tests/upgrade/session/program.dl" \
    "$candidate_source/tests/upgrade/session/program.dl" \
    "$candidate_source/tests/upgrade/session/legacy.c" \
    "$candidate_source/tests/upgrade/session/migrated.c"
run_executor_case \
    "$candidate_source/tests/upgrade/executor/program.dl" \
    "$candidate_source/tests/upgrade/executor/consumer.c" \
    "$candidate_source/tests/upgrade/executor/link-probe.c"

printf 'Upgrade matrix passed: old=%s candidate=%s\n' "$old_commit" "$candidate_commit"
