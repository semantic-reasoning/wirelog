#!/usr/bin/env bash
# Deterministic fixture for the observable DOOP calibration contract.
set -euo pipefail

root=${1:-$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)}
tmp=$(CDPATH= cd -- "$(mktemp -d "${TMPDIR:-/tmp}/wirelog-doop-validation.XXXXXX")" && pwd)
trap 'rm -rf "$tmp"' EXIT

fixture="$tmp/root"
mkdir -p "$fixture/scripts/release" "$fixture/build/bench" "$tmp/data" "$tmp/cgroup"
printf 'memory\n' > "$tmp/cgroup/cgroup.controllers"
: > "$tmp/cgroup/cgroup.procs"
cp "$root/scripts/run_doop_validation.sh" "$fixture/scripts/run_doop_validation.sh"
chmod +x "$fixture/scripts/run_doop_validation.sh"
for n in ActualParam $(seq 1 34); do
    printf 'fixture\n' > "$tmp/data/$n.facts"
done
facts_manifest=$(CDPATH= cd -- "$tmp/data" && sha256sum -- *.facts | LC_ALL=C sort -k2 | sha256sum | awk '{print $1}')
printf '%s\n' '# schema=2 workload tuple_oracle iteration_oracle data_path data_manifest_sha256 provenance_id acquisition_command' \
    > "$fixture/scripts/release/downstream-matrix-oracles.tsv"
printf 'doop\t7\t3\tbench/data/doop\tarchive:%064d;files:%s\tfixture\tfixture\n' 0 "$facts_manifest" \
    | sed 's/archive:0000000000000000000000000000000000000000000000000000000000000000/archive:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/' \
    >> "$fixture/scripts/release/downstream-matrix-oracles.tsv"

cat > "$fixture/build/bench/bench_flowlog" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
progress=
while (($#)); do
    if [[ $1 == --repeat-progress ]]; then progress=$2; shift 2; else shift; fi
done
mode=${DOOP_FIXTURE_MODE:-success}
if [[ -z "$(<"${WIRELOG_DOOP_CGROUP_DIR:?}/cgroup.procs")" ]]; then
    exit 91
fi
[[ "$(<"$WIRELOG_DOOP_CGROUP_DIR/memory.max")" == 1048576 ]] || exit 92
printf '%s\n' "${WIRELOG_DOOP_CGROUP_MARKER:?}" > "${WIRELOG_DOOP_CGROUP_MARKER}"
case "$mode" in
success)
    for r in 1 2 3 4 5; do
        printf 'repeat_start\tworkload=doop\trepetition=%s\trepeat=5\tworkers=8\n' "$r" >> "$progress"
        printf 'repeat_complete\tworkload=doop\trepetition=%s\trepeat=5\tworkers=8\tduration_ms=1.0\trss_kb=10\trc=0\tstatus=OK\ttuples=7\titerations=3\n' "$r" >> "$progress"
    done
    printf 'DONE\tworkload=doop\trepeat=5\tworkers=8\tstatus=OK\n' >> "$progress"
    printf 'doop\tfixture\t35\t8\t5\t1.0\t1.0\t1.0\t10\t7\t3\tOK\n'
    ;;
malformed|missing|nonsequential)
    printf 'repeat_start\tworkload=doop\trepetition=1\trepeat=5\tworkers=8\n' >> "$progress"
    if [[ $mode != missing ]]; then
        r=2; [[ $mode == nonsequential ]] && r=4
        printf 'repeat_complete\tworkload=doop\trepetition=%s\trepeat=5\tworkers=8\tduration_ms=1.0\trss_kb=10\trc=0\tstatus=OK\n' "$r" >> "$progress"
    fi
    printf 'doop\tfixture\t35\t8\t5\t1.0\t1.0\t1.0\t10\t7\t3\tOK\n'
    ;;
partial)
    printf 'repeat_start\tworkload=doop\trepetition=1\trepeat=5\tworkers=8\n' >> "$progress"
    printf 'repeat_complete\tworkload=doop\trepetition=1\trepeat=5\tworkers=8\tduration_ms=1.0\trss_kb=10\trc=0\tstatus=OK\n' >> "$progress"
    ;;
oom)
    printf 'oom 1\noom_kill 0\n' > "$WIRELOG_DOOP_CGROUP_DIR/memory.events"
    exit 0
    ;;
failed-repetition)
    printf 'repeat_start\tworkload=doop\trepetition=1\trepeat=5\tworkers=8\n' >> "$progress"
    printf 'repeat_complete\tworkload=doop\trepetition=1\trepeat=5\tworkers=8\tduration_ms=1.0\trss_kb=10\trc=1\tstatus=FAIL\tfailure_reason=fixture_failure\n' >> "$progress"
    exit 1
    ;;
timeout)
    (trap '' TERM; sleep 1000) & printf '%s\n' "$!" > "${DOOP_FIXTURE_CHILD:?}"
    sleep 1000
    ;;
early-leader)
    for r in 1 2 3 4 5; do
        printf 'repeat_start\tworkload=doop\trepetition=%s\trepeat=5\tworkers=8\n' "$r" >> "$progress"
        printf 'repeat_complete\tworkload=doop\trepetition=%s\trepeat=5\tworkers=8\tduration_ms=1.0\trss_kb=10\trc=0\tstatus=OK\ttuples=7\titerations=3\n' "$r" >> "$progress"
    done
    printf 'DONE\tworkload=doop\trepeat=5\tworkers=8\tstatus=OK\n' >> "$progress"
    printf 'doop\tfixture\t35\t8\t5\t1.0\t1.0\t1.0\t10\t7\t3\tOK\n'
    (trap '' TERM; sleep 1000) & printf '%s\n' "$!" > "${DOOP_FIXTURE_CHILD:?}"
    exit 0
    ;;
false-success)
    for r in 1 2 3 4 5; do
        printf 'repeat_start\tworkload=doop\trepetition=%s\trepeat=5\tworkers=8\n' "$r" >> "$progress"
        printf 'repeat_complete\tworkload=doop\trepetition=%s\trepeat=5\tworkers=8\tduration_ms=1.0\trss_kb=10\trc=0\tstatus=OK\n' "$r" >> "$progress"
    done
    printf 'DONE\tworkload=doop\trepeat=5\tworkers=8\tstatus=OK\n' >> "$progress"
    printf 'doop\tfixture\t35\t8\t5\t1.0\t1.0\t1.0\t10\t7\t3\tOK\n'
    exit 7
    ;;
esac
EOF
chmod +x "$fixture/build/bench/bench_flowlog"

run_case() {
    local name=$1 mode=$2 want=$3 wall_timeout=${4:-2}
    local evidence="$tmp/evidence-$name" output status
    set +e
    output=$(DOOP_FIXTURE_MODE="$mode" DOOP_DATA_DIR="$tmp/data" DOOP_CGROUP_ROOT="$tmp/cgroup" \
        DOOP_FIXTURE_CHILD="$tmp/child.pid" DOOP_CGROUP_MARKER="$evidence/cgroup-attached" \
        "$fixture/scripts/run_doop_validation.sh" --calibration \
        --memory-limit-kb 1024 --evidence-dir "$evidence" --wall-timeout "$wall_timeout" 2>&1)
    status=$?
    set -e
    [[ "$status" == "$want" ]] || {
        printf 'test-doop-validation: FAIL %s (want %s got %s)\n%s\n' \
            "$name" "$want" "$status" "$output" >&2
        find "$evidence" -type f -maxdepth 1 -print -exec sed 's/^/    /' {} \; 2>/dev/null || true
        return 1
    }
    printf 'test-doop-validation: ok %s\n' "$name"
    printf '%s' "$output" > "$tmp/$name.out"
}

# The runner's child starts in the runner's process group and setsid(1)
# moves it out only once it runs.  These shims replay the losing order
# without timing luck: setsid waits until ps has been asked about the child,
# so the first answer always names the runner's own group.  In "never" mode
# setsid does not move the child at all; in "group-only" mode the child leads
# a new group but stays in the runner's session, as when setsid forks.  The
# runner runs in a session of its own under a leader that records any TERM
# sent to that group, so a runner that signals the group it saw is caught
# even when its own trap survives the signal, and never reaches this script.
real_ps=$(command -v ps)
real_setsid=$(command -v setsid)
mkdir -p "$tmp/race-bin"
cat > "$tmp/race-bin/ps" <<EOF
#!/usr/bin/env bash
target=
args=("\$@")
for ((i = 0; i < \${#args[@]}; i++)); do
    [[ \${args[i]} == -p ]] && target=\${args[i + 1]}
done
pid=\$\$
while [[ -n \$target && \$pid -gt 1 ]]; do
    [[ \$pid == "\$target" ]] && exec "$real_ps" "\$@"
    read -r _ _ _ pid _ < "/proc/\$pid/stat" || break
done
# Answer first, then let setsid run: the child cannot have moved yet.
out=\$("$real_ps" "\$@")
rc=\$?
[[ -n \$target ]] && : > "$tmp/race-asked"
printf '%s\\n' "\$out"
exit "\$rc"
EOF
cat > "$tmp/race-bin/setsid" <<EOF
#!/usr/bin/env bash
for _ in \$(seq 1 200); do
    [[ -e "$tmp/race-asked" ]] && break
    sleep 0.05
done
[[ \$1 == --wait ]] && shift
if [[ -e "$tmp/race-never" ]]; then
    exec "\$@"
fi
if [[ -e "$tmp/race-group-only" ]]; then
    exec python3 -c 'import os, sys; os.setpgid(0, 0); os.execvp(sys.argv[1], sys.argv[1:])' "\$@"
fi
exec "$real_setsid" "\$@"
EOF
chmod +x "$tmp/race-bin/ps" "$tmp/race-bin/setsid"

race_case() {
    local name=$1 want=$2 evidence="$tmp/evidence-$1" status
    rm -f "$tmp/race-asked" "$tmp/race-signalled"
    set +e
    DOOP_FIXTURE_MODE=success DOOP_DATA_DIR="$tmp/data" DOOP_CGROUP_ROOT="$tmp/cgroup" \
        DOOP_FIXTURE_CHILD="$tmp/child.pid" DOOP_CGROUP_MARKER="$evidence/cgroup-attached" \
        DOOP_CALIBRATION_ISOLATE_POLLS=20 PATH="$tmp/race-bin:$PATH" \
        "$real_setsid" --wait bash -c '
            trap ": > \"\$0\"" TERM
            "$@" &
            child=$!
            while :; do
                wait "$child"
                rc=$?
                kill -0 "$child" 2>/dev/null || break
            done
            exit "$rc"' "$tmp/race-signalled" \
        "$fixture/scripts/run_doop_validation.sh" --calibration \
        --memory-limit-kb 1024 --evidence-dir "$evidence" --wall-timeout 10 \
        > "$tmp/$name.out" 2>&1
    status=$?
    set -e
    if [[ ! -e "$tmp/race-asked" || -e "$tmp/race-signalled" || "$status" != "$want" ]]; then
        printf 'test-doop-validation: FAIL %s (want %s got %s, child queried: %s, group signalled: %s)\n' \
            "$name" "$want" "$status" "$([[ -e "$tmp/race-asked" ]] && echo yes || echo no)" \
            "$([[ -e "$tmp/race-signalled" ]] && echo yes || echo no)" >&2
        sed 's/^/    /' "$tmp/$name.out" >&2
        return 1
    fi
}

race_case setsid_race 0
grep -q 'RESULT: PASS' "$tmp/setsid_race.out"
[[ -s "$tmp/evidence-setsid_race/cgroup-attached" ]]
printf 'test-doop-validation: ok setsid_race\n'

# A child that never leads its own session must be refused without
# signalling any group, because the only one seen is the runner's own.
: > "$tmp/race-never"
race_case setsid_never 1
rm -f "$tmp/race-never"
grep -q 'could not isolate calibration process group' "$tmp/setsid_never.out"
[[ ! -e "$tmp/evidence-setsid_never/cgroup-attached" ]]
[[ ! -e "$tmp/evidence-setsid_never/final.tsv" ]]
printf 'test-doop-validation: ok setsid_never\n'

command -v python3 > /dev/null || {
    echo 'test-doop-validation: FAIL setsid_group_only (python3 not found)' >&2
    exit 1
}
: > "$tmp/race-group-only"
race_case setsid_group_only 1
rm -f "$tmp/race-group-only"
grep -q 'could not isolate calibration process group' "$tmp/setsid_group_only.out"
[[ ! -e "$tmp/evidence-setsid_group_only/cgroup-attached" ]]
printf 'test-doop-validation: ok setsid_group_only\n'

# The poll count reaches bash arithmetic, which would run a subscript, and
# zero polls, or a count that wraps in bash arithmetic, would refuse before
# the child has run.  All are rejected before anything is launched.
for polls in 0 9223372036854775808 "a[\$(: > $tmp/polls-ran)]"; do
    rm -rf "$tmp/evidence-polls"
    set +e
    DOOP_FIXTURE_MODE=success DOOP_DATA_DIR="$tmp/data" DOOP_CGROUP_ROOT="$tmp/cgroup" \
        DOOP_CALIBRATION_ISOLATE_POLLS="$polls" \
        timeout 20 "$fixture/scripts/run_doop_validation.sh" --calibration --memory-limit-kb 1024 \
        --evidence-dir "$tmp/evidence-polls" --wall-timeout 2 > "$tmp/polls.out" 2>&1
    status=$?
    set -e
    [[ "$status" == 1 && ! -e "$tmp/polls-ran" && ! -e "$tmp/evidence-polls/cgroup-attached" ]] &&
        grep -q 'DOOP_CALIBRATION_ISOLATE_POLLS must be an integer from 1 to 999999' "$tmp/polls.out" || {
        printf 'test-doop-validation: FAIL isolate_polls_rejected (%s: want 1 got %s)\n' \
            "$polls" "$status" >&2
        sed 's/^/    /' "$tmp/polls.out" >&2
        exit 1
    }
done
printf 'test-doop-validation: ok isolate_polls_rejected\n'

run_case success success 0 10
grep -q 'RESULT: PASS' "$tmp/success.out"
[[ $(wc -l < "$tmp/evidence-success/repeat-progress.tsv") -eq 11 ]]
[[ -s "$tmp/evidence-success/final.tsv" ]]
[[ -s "$tmp/evidence-success/cgroup-attached" ]]
[[ -z "$(find "$tmp/cgroup" -mindepth 1 -maxdepth 1 -type d -name 'wirelog-doop-*' -print -quit)" ]]

printf 'doop\tfixture\t35\t8\t5\t1.0\t1.0\t1.0\t10\t999\t3\tOK\n' > "$tmp/baseline.tsv"
set +e
DOOP_FIXTURE_MODE=success DOOP_DATA_DIR="$tmp/data" DOOP_CGROUP_ROOT="$tmp/cgroup" \
    DOOP_FIXTURE_CHILD="$tmp/child.pid" DOOP_CGROUP_MARKER="$tmp/evidence-late/cgroup-attached" \
    "$fixture/scripts/run_doop_validation.sh" --calibration --memory-limit-kb 1024 \
    --baseline "$tmp/baseline.tsv" --evidence-dir "$tmp/evidence-late" --wall-timeout 10 > "$tmp/late.out" 2>&1
status=$?
set -e
[[ "$status" == 1 ]]
[[ ! -e "$tmp/evidence-late/final.tsv" ]]
[[ -z "$(compgen -G "$tmp/evidence-late/final.tsv.tmp.*" || true)" ]]
printf 'test-doop-validation: ok late_validation_failure\n'

set +e
DOOP_FIXTURE_MODE=success DOOP_DATA_DIR="$tmp/data" DOOP_CGROUP_ROOT="$tmp/missing-cgroup" \
    "$fixture/scripts/run_doop_validation.sh" --calibration --memory-limit-kb 1024 \
    --evidence-dir "$tmp/evidence-refused" --wall-timeout 2 > "$tmp/refused.out" 2>&1
status=$?
set -e
[[ "$status" == 1 ]]
grep -q 'memory_bound_refused' "$tmp/evidence-refused/metadata.tsv"
[[ ! -e "$tmp/evidence-refused/final.tsv" ]]
printf 'test-doop-validation: ok cgroup_refusal\n'

run_case malformed malformed 1
run_case missing missing 1
run_case nonsequential nonsequential 1
run_case partial partial 1
[[ -s "$tmp/evidence-partial/repeat-progress.tsv" ]]
[[ ! -e "$tmp/evidence-partial/final.tsv" ]]

run_case failed_repetition failed-repetition 1
grep -q $'status=FAIL\tfailure_reason=fixture_failure' \
    "$tmp/evidence-failed_repetition/repeat-progress.tsv"
[[ ! -e "$tmp/evidence-failed_repetition/final.tsv" ]]

run_case timeout timeout 1
grep -q $'calibration_failure\tworkload=doop\trepeat=5\tworkers=8\trc=124\tstatus=FAIL\tfailure_reason=timeout' \
    "$tmp/evidence-timeout/repeat-progress.tsv"
child=$(<"$tmp/child.pid")
! kill -0 "$child" 2>/dev/null

run_case early_leader early-leader 1
grep -q $'calibration_failure\tworkload=doop\trepeat=5\tworkers=8\trc=125\tstatus=FAIL\tfailure_reason=leader_exit' \
    "$tmp/evidence-early_leader/repeat-progress.tsv"
child=$(<"$tmp/child.pid")
! kill -0 "$child" 2>/dev/null

run_case oom oom 1
grep -q $'calibration_failure\tworkload=doop\trepeat=5\tworkers=8\trc=137\tstatus=FAIL\tfailure_reason=memory_oom' \
    "$tmp/evidence-oom/repeat-progress.tsv"
grep -q $'memory_events_oom_delta\t1' "$tmp/evidence-oom/metadata.tsv"

run_case false_success false-success 1
! grep -q 'RESULT: PASS' "$tmp/false_success.out"
[[ ! -e "$tmp/evidence-false_success/final.tsv" ]]

printf 'test-doop-validation: all cases passed\n'
