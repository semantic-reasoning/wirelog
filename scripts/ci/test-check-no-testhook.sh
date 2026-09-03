#!/usr/bin/env bash
# Self-test for check-no-testhook-in-libwirelog.sh.
#
# Issue #1302. The gate makes two claims: (a) no testhook-branded symbols are
# defined in libwirelog, and (b) wl_log_emit originates from log_emit.c rather
# than log_testhook.c. Claim (a) ran. Claim (b) never did, on any platform,
# because its awk predicate could not be satisfied:
#
#   $NF ~ /:[0-9]+$/ && $0 ~ / wl_log_emit$/
#
# `nm --line-numbers` prints "<addr> T wl_log_emit\t<file>:<line>". With the
# location present $NF matches but $0 no longer ends in the symbol name; with it
# absent $0 matches but $NF is the symbol name. The two are mutually exclusive,
# so PROV was always empty and the gate always took its "provenance check
# skipped; exit 0" branch -- a security-adjacent check reporting success without
# ever running.
#
# nm is stubbed here rather than built against, so every branch is reachable in
# milliseconds; the one case that needs a real library is guarded on the build
# tree existing.
set -euo pipefail

case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux|Darwin) ;;
    *) echo "test-check-no-testhook: SKIP: needs a POSIX host"; exit 77 ;;
esac

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
gate="$root/scripts/ci/check-no-testhook-in-libwirelog.sh"
[[ -x "$gate" ]] || { echo "test-check-no-testhook: not executable: $gate" >&2; exit 1; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-testhook.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
# Exact status: `!` would conflate 1 (a real leak) with 77 (could not check),
# which is the distinction this issue exists to restore.
expect_status() {
    local name=$1 want=$2 got=0
    shift 2
    "$@" >"$tmp/out" 2>"$tmp/err" || got=$?
    if [[ "$got" == "$want" ]]; then
        printf 'test-check-no-testhook: ok %s\n' "$name"
    else
        printf 'test-check-no-testhook: FAIL %s (want exit %s, got %s)\n' "$name" "$want" "$got" >&2
        sed 's/^/    /' "$tmp/out" "$tmp/err" >&2
        failures=$((failures + 1))
    fi
}
# The negative form. Without it, asserting only that a message IS present
# cannot rule out a second, contradictory message alongside it.
refute_says() {
    local name=$1 needle=$2
    if grep -qF -e "$needle" "$tmp/out" "$tmp/err"; then
        printf 'test-check-no-testhook: FAIL %s (unexpected %q in output)\n' "$name" "$needle" >&2
        failures=$((failures + 1))
    else
        printf 'test-check-no-testhook: ok %s\n' "$name"
    fi
}

expect_says() {
    local name=$1 needle=$2
    if grep -qF -e "$needle" "$tmp/out" "$tmp/err"; then
        printf 'test-check-no-testhook: ok %s\n' "$name"
    else
        printf 'test-check-no-testhook: FAIL %s (no %q in output)\n' "$name" "$needle" >&2
        failures=$((failures + 1))
    fi
}

# A stub nm driven by two files: NM_PLAIN is what `nm --defined-only` prints,
# NM_LINES what `nm --line-numbers --defined-only` prints. Writing NM_NO_LINES
# makes the stub reject --line-numbers the way BSD nm does.
bin="$tmp/bin"; mkdir -p "$bin"
cat > "$bin/nm" <<'EOS'
#!/usr/bin/env bash
# NM_FAIL models the real failure -- nm exiting non-zero with no output, which
# is what an unreadable or non-object file produces. Reading an empty file
# instead exits 0 and only models "nm ran and found nothing", a different case.
[ -n "${NM_FAIL:-}" ] && { echo "nm: could not read file" >&2; exit 1; }
want_lines=0
for a in "$@"; do [ "$a" = --line-numbers ] && want_lines=1; done
if [ "$want_lines" = 1 ]; then
    [ -n "${NM_NO_LINES:-}" ] && { echo "nm: unrecognized option '--line-numbers'" >&2; exit 1; }
    cat "$NM_LINES"
else
    cat "$NM_PLAIN"
fi
EOS
chmod +x "$bin/nm"

build="$tmp/build"; mkdir -p "$build"
: >"$build/libwirelog.so"          # presence is all the gate checks

clean_syms="$tmp/clean.txt"
cat > "$clean_syms" <<'EOF'
0000000000001100 T wl_log_init
0000000000001119 T wl_log_emit
EOF

run() { PATH="$bin:$PATH" NM_PLAIN="$1" NM_LINES="$2" "$gate" "${@:3}" "$build"; }

# --- (a) the leak check ------------------------------------------------------
leaky="$tmp/leaky.txt"
cat > "$leaky" <<'EOF'
0000000000001100 T wl_log_init
0000000000001180 T wl_log_test_last
EOF
expect_status 'a testhook symbol is a leak' 1 run "$leaky" "$clean_syms" --check=leak
expect_says   'the leak names the symbol' 'wl_log_test_last'
expect_status 'a clean library passes the leak check' 0 run "$clean_syms" "$clean_syms" --check=leak

# --- (b) the provenance check ------------------------------------------------
# The regression itself: with line info present the check must reach a verdict.
# Under the old predicate PROV was empty here and the gate exited 0 without
# checking, so this case is what proves the fix.
good="$tmp/good.txt"
printf '0000000000001119 T wl_log_emit\t%s/wirelog/util/log_emit.c:42\n' "$root" > "$good"
expect_status 'provenance in log_emit.c passes' 0 run "$clean_syms" "$good" --check=provenance
expect_says   'the pass names the file' 'log_emit.c'

# The case the gate exists for, and which has never run.
bad="$tmp/bad.txt"
printf '0000000000001119 T wl_log_emit\t%s/wirelog/util/log_testhook.c:17\n' "$root" > "$bad"
expect_status 'provenance in log_testhook.c FAILS' 1 run "$clean_syms" "$bad" --check=provenance
# The specific diagnosis, not just the filename: deleting the log_testhook.c
# case makes it fall through to the "unrecognized source" branch, which also
# exits 1 and also happens to print the path -- so a needle of "log_testhook.c"
# alone cannot tell a correct leak diagnosis from an accidental one. For a gate
# whose purpose is naming this exact leak, that distinction is the point.
expect_says   'it is diagnosed as a testhook leak, not merely unrecognized' \
    'originates from log_testhook.c'

# No line info: a stripped or release build genuinely cannot answer. That is a
# skip, not a pass -- meson must not record it as a verdict (#1288).
nolines="$tmp/nolines.txt"
printf '0000000000001119 T wl_log_emit\n' > "$nolines"
expect_status 'a build without line info SKIPs' 77 run "$clean_syms" "$nolines" --check=provenance
expect_says   'the skip says why' 'no line-number information'

# BSD nm has no --line-numbers at all. Same verdict, different cause.
bsd_skips() {
    PATH="$bin:$PATH" NM_PLAIN="$clean_syms" NM_LINES="$good" NM_NO_LINES=1 \
        "$gate" --check=provenance "$build"
}
expect_status 'an nm without --line-numbers SKIPs' 77 bsd_skips

# Provenance somewhere unexpected must reach a verdict, not fall through to 0.
odd="$tmp/odd.txt"
printf '0000000000001119 T wl_log_emit\t/somewhere/else/mystery.c:3\n' > "$odd"
expect_status 'unrecognized provenance FAILS rather than passing' 1 run "$clean_syms" "$odd" --check=provenance
expect_says   'the unrecognized verdict names the file' 'mystery.c'

# --- parsing edge cases ------------------------------------------------------
# The location is tab-separated, so a path containing spaces must still parse.
# Splitting on whitespace and taking $NF would truncate it.
spaced="$tmp/spaced.txt"
printf '0000000000001119 T wl_log_emit\t/build dir/wirelog/util/log_emit.c:42\n' > "$spaced"
expect_status 'a path containing spaces still resolves' 0 run "$clean_syms" "$spaced" --check=provenance

# A longer symbol that merely starts with the name must not be mistaken for it.
# This is shape 2b under #1305 -- the listing carries a location, and no symbol
# matching the emitter pattern is defined -- so the verdict is 1, not the 77 it
# was before. Assert the MESSAGE as well: after the flip the exit code alone no
# longer distinguishes "the pattern correctly rejected _v2" from "the pattern
# wrongly matched _v2 and found testhook provenance", which is what this case
# exists to rule out.
prefix="$tmp/prefix.txt"
# A local symbol is present so this is shape 2b (the local table survived and
# the emitter is not in it), not 2c (locals stripped, absence proves nothing).
# Real libraries have locals; without one this fixture would take the 2c skip
# and stop testing what it names.
printf '0000000000001119 T wl_log_emit_v2\t/x/log_testhook.c:1\n0000000000002220 t a_local\t/x/o.c:2\n' > "$prefix"
expect_status 'a prefix symbol is not mistaken for wl_log_emit' 1 run "$clean_syms" "$prefix" --check=provenance
expect_says 'and is reported as a missing emitter, not as testhook provenance' \
    'defines no wl_log_emit'
refute_says 'and does not claim provenance in log_testhook.c' 'originates from log_testhook.c'

# --- independence (criterion 5) ---------------------------------------------
# A provenance skip must not hide the leak check's real result, and a leak must
# not be reported as a skip.
# --check=both composes the two, and had no coverage: its exit logic could be
# deleted entirely and every other assertion still passed. It is also the mode a
# developer types by hand.
expect_status 'both: a leak fails even when provenance cannot be checked' 1 \
    run "$leaky" "$nolines" --check=both
expect_status 'both: a provenance skip alone is not a failure' 0 \
    run "$clean_syms" "$nolines" --check=both
expect_status 'both: bad provenance fails even with no leak' 1 \
    run "$clean_syms" "$bad" --check=both
expect_status 'both: all clear passes' 0 run "$clean_syms" "$good" --check=both

# A readable symbol table but an unusable line-number listing: the leak check
# passes and provenance returns 2. This is the case that distinguishes
# propagating the real status from the older "anything that is not 1 is fine",
# which would report success on a status it never considered.
both_prov_error() {
    local e="$tmp/empty-lines.txt"; : >"$e"
    run "$clean_syms" "$e" --check=both
}
expect_status 'both: a non-1 provenance error is not treated as a pass' 2 both_prov_error

# An unreadable or non-object file makes nm print nothing, which used to be
# indistinguishable from a clean library -- the gate said "OK: no testhook
# symbols" about a file it could not read.
empty_syms="$tmp/empty.txt"; : >"$empty_syms"
expect_status 'nm producing nothing is an error, not a clean bill of health' 2 \
    run "$empty_syms" "$empty_syms" --check=leak
expect_says   'the unreadable case says why' 'not an object file'
expect_status 'nm producing nothing is an error for provenance too' 2 \
    run "$empty_syms" "$empty_syms" --check=provenance

# ...and the case that actually occurs: nm exits non-zero having printed
# nothing. The empty-file fixtures above exercise a stub that exits 0, so they
# never reached the code that handles a genuine nm failure -- the gate got this
# wrong in the meson-registered mode while every assertion here passed.
nm_fails() { PATH="$bin:$PATH" NM_PLAIN="$clean_syms" NM_LINES="$good" NM_FAIL=1 "$gate" "$@" "$build"; }
expect_status 'nm failing outright is an error (leak)' 2 nm_fails --check=leak
expect_status 'nm failing outright is an error (provenance)' 2 nm_fails --check=provenance
expect_says   'the provenance failure blames the file, not BSD nm' 'cannot read'
expect_status 'nm failing outright is an error (both)' 2 nm_fails --check=both

# Mach-O symbols carry a leading underscore.
macho="$tmp/macho.txt"
printf '0000000000001119 t _wl_log_emit\t/x/wirelog/util/log_emit.c:51\n' > "$macho"
expect_status 'a Mach-O underscored symbol resolves' 0 run "$clean_syms" "$macho" --check=provenance

# An optimised build can emit only a clone. build-erasure-check contains
# wl_log_emit.constprop.0 and no plain wl_log_emit, so without this the gate
# silently SKIPs there -- a registered check that can never reach a verdict,
# which is the shape of the bug this issue is about.
clone="$tmp/clone.txt"
printf '0000000000001119 t wl_log_emit.constprop.0\t/x/wirelog/util/log_emit.c:51\n' > "$clone"
expect_status 'a GCC clone suffix resolves' 0 run "$clean_syms" "$clone" --check=provenance

# ...but a different symbol that merely shares the prefix must not. The dot is
# what separates a clone from an unrelated function.
# Shape 2b as above, so 1 rather than 77, with the message carrying the proof.
notclone="$tmp/notclone.txt"
printf '0000000000001119 t wl_log_emit_v2\t/x/wirelog/util/log_testhook.c:1\n0000000000002220 t a_local\t/x/o.c:2\n' > "$notclone"
expect_status 'a prefix symbol is still not mistaken for a clone' 1 \
    run "$clean_syms" "$notclone" --check=provenance
expect_says 'and reports a missing emitter' 'defines no wl_log_emit'
refute_says 'and the clone rule did not match _v2 either' 'originates from log_testhook.c'

# --- #1305: the three no-provenance shapes are distinguished --------------
#
# Before #1305 all three printed the stripped-build message, so a library with
# 1,261 lines of line-number information and wl_log_emit renamed away was
# byte-identical to a genuinely stripped build.
#
# (1) no locations anywhere -- the build cannot answer. SKIP 77.
nodwarf="$tmp/nodwarf.txt"
printf '0000000000001119 t wl_log_emit\n0000000000002220 T other_symbol\n' > "$nodwarf"
expect_status 'no locations anywhere is the stripped-build skip' 77 \
    run "$clean_syms" "$nodwarf" --check=provenance
expect_says 'and names a stripped or non-debug build' 'stripped or non-debug build'

# (2a) locations present, and the emitter IS defined but carries none. The
# library is intact; the debug info is incomplete for one symbol. SKIP 77, but
# it must not blame a strip that did not happen.
partial="$tmp/partial.txt"
printf '0000000000001119 t wl_log_emit\n0000000000002220 T other_symbol\t/x/other.c:9\n' > "$partial"
expect_status 'emitter present without a location is a distinct skip' 77 \
    run "$clean_syms" "$partial" --check=provenance
expect_says 'and names the missing location, not a strip' 'carries no location for it'
refute_says 'and does not blame a stripped build' 'stripped or non-debug build'

# (2b) locations present, LOCAL SYMBOLS PRESENT, and still no symbol matching
# the emitter pattern. The local table survived and the emitter is not in it, so
# it was renamed, relinked or removed. FAIL 1. The locals precondition is what
# separates this from (2c) below -- without it, a legitimately `strip -x`ed
# library with a separate debug file would be failed as tampered.
gone="$tmp/gone.txt"
printf '0000000000002220 T other_symbol\t/x/other.c:9\n0000000000003330 t a_local\t/x/o.c:2\n' > "$gone"
expect_status 'DWARF present with no emitter at all is a failure' 1 \
    run "$clean_syms" "$gone" --check=provenance
expect_says 'and says the emitter was renamed, relinked or removed' 'defines no wl_log_emit'
refute_says 'and does not report it as a stripped build' 'stripped or non-debug build'

# (2c) Locations present, but NO local symbols at all. wl_log_emit is local, so
# its absence is not evidence. Reachable legitimately: `strip -x` plus
# `objcopy --add-gnu-debuglink` produces exactly this under GNU nm -- measured
# on a real library at 96 symbols, 96 locations, zero locals, no emitter. This
# case is why (2b) is not simply "DWARF present and no emitter".
nolocals="$tmp/nolocals.txt"
printf '0000000000002220 T other_symbol\t/x/other.c:9\n0000000000003330 T third\t/x/third.c:4\n' > "$nolocals"
expect_status 'no local symbols is a skip, not a missing emitter' 77 \
    run "$clean_syms" "$nolocals" --check=provenance
expect_says 'and says why the absence is not evidence' 'not evidence'
refute_says 'and does not accuse a rename' 'renamed, relinked or removed'

# A GLOBAL ifunc must not be counted as a surviving local. GNU nm prints ELF
# IFUNC as 'i' and unique-global as 'u' regardless of binding, and a global
# ifunc survives `strip -x` -- measured on a real two-line ifunc library, where
# 'i' is the ONLY lowercase symbol left after stripping. Counting it as a local
# would re-arm the false failure (2c) exists to remove, on a project that
# already builds with -mavx2/-msse4.2 and could plausibly add CPU dispatch.
ifunc="$tmp/ifunc.txt"
printf '0000000000002220 T other_symbol\t/x/other.c:9\n0000000000003330 i dispatched\t/x/i.c:2\n' > "$ifunc"
expect_status 'a global ifunc does not count as a surviving local' 77 \
    run "$clean_syms" "$ifunc" --check=provenance
expect_says 'and is reported as locals stripped' 'no local symbols'

# emitter_symbols must recognise the same shapes provenance_of does, or a
# legitimate build flips from skip to failure. /tmp/b-err in this repo defines
# ONLY wl_log_emit.constprop.0 and no plain wl_log_emit, so the clone
# alternation is load-bearing, not decorative.
clone_noloc="$tmp/clone_noloc.txt"
printf '0000000000001119 t wl_log_emit.constprop.0\n0000000000002220 T other\t/x/other.c:9\n' > "$clone_noloc"
expect_status 'an unlocated clone is 2a, not a missing emitter' 77 \
    run "$clean_syms" "$clone_noloc" --check=provenance
refute_says 'and the clone is not reported as removed' 'renamed, relinked or removed'

macho_noloc="$tmp/macho_noloc.txt"
printf '0000000000001119 t _wl_log_emit\n0000000000002220 T other\t/x/other.c:9\n' > "$macho_noloc"
expect_status 'an unlocated Mach-O _wl_log_emit is 2a' 77 \
    run "$clean_syms" "$macho_noloc" --check=provenance
refute_says 'and the Mach-O form is not reported as removed' 'renamed, relinked or removed'

# The 2b diagnostic must survive a large listing. Without this the `nearby`
# computation could regress to a pipeline -- which fails on exactly this input
# and prints nothing -- and no case would notice.
bigb="$tmp/bigb.txt"
{
    printf '0000000000002220 T other_symbol\t/x/other.c:9\n'
    i=0
    while [ "$i" -lt 9000 ]; do
        printf '00000000000%05d t wl_log_filler_%d\t/x/f.c:%d\n' "$i" "$i" "$i"
        i=$((i + 1))
    done
} > "$bigb"
# The point is sort's OUTPUT exceeding the 64 KB pipe buffer, not the fixture's
# size, so guard it: shortening the filler names would make this pass vacuously
# while no longer exercising the regression it exists to catch.
bigb_sorted_bytes=$(awk '{ for (i = 1; i <= NF; i++) if ($i ~ /wl_log/) n += length($i) + 1 } END { print n + 0 }' "$bigb")
if [ "$bigb_sorted_bytes" -lt 100000 ]; then
    printf 'test-check-no-testhook: FAIL 2b fixture too small (%s bytes of matches; need >100000 to clear the pipe buffer)\n' \
        "$bigb_sorted_bytes" >&2
    failures=$((failures + 1))
else
    printf 'test-check-no-testhook: ok the 2b fixture is large enough to exercise the pipe buffer\n'
fi
expect_status 'a large listing with no emitter still fails' 1 \
    run "$clean_syms" "$bigb" --check=provenance
expect_says 'and still lists the wl_log-like symbols it did find' \
    'wl_log-like symbols present'

# A tab-separated trailing field that is not a file:line must not be taken as a
# location; without the :<line> guard this would be reported as provenance.
notloc="$tmp/notloc.txt"
printf '0000000000001119 t wl_log_emit\tsome trailing note\n' > "$notloc"
expect_status 'a trailing field that is not file:line is not a location' 77 \
    run "$clean_syms" "$notloc" --check=provenance

# --- output larger than the pipe buffer -------------------------------------
# Every fixture above is one or two lines, which cannot reach the failure these
# cover. A consumer that stops reading early -- `grep -q`, or awk's `exit` --
# SIGPIPEs its producer once the output passes the pipe buffer (~64 KB); under
# `set -o pipefail` that surfaces as 141, and in the leak check it silently
# inverted the verdict. Both were live in this gate: the leak check reported
# "OK: no testhook symbols" on a leaking library 5/5 times at this size, and the
# provenance check exited 141. Whether either fired depended only on where the
# linker placed the symbol, so a relink was enough to change the answer.
big_leak="$tmp/big-leak.txt"
big_prov="$tmp/big-prov.txt"
{
    # The interesting symbol FIRST: that is what makes the consumer stop while
    # the producer still has ~1 MB to write.
    printf '0000000000001180 T wl_log_test_last\n'
    awk 'BEGIN { for (i = 1; i <= 20000; i++)
        printf "00000000000%05d t filler_%d\t/some/path/file_%d.c:%d\n", i, i, i, i }'
} > "$big_leak"
{
    printf '0000000000001119 t wl_log_emit\t/x/wirelog/util/log_emit.c:51\n'
    awk 'BEGIN { for (i = 1; i <= 20000; i++)
        printf "00000000000%05d t filler_%d\t/some/path/file_%d.c:%d\n", i, i, i, i }'
} > "$big_prov"

# Sanity: the fixtures must actually exceed the pipe buffer, or these assertions
# hold vacuously and would keep passing after the guard was removed.
big_enough() { [[ "$(wc -c <"$big_leak")" -gt 262144 && "$(wc -c <"$big_prov")" -gt 262144 ]]; }
expect_status 'the large fixtures exceed the pipe buffer' 0 big_enough

expect_status 'a leak is still caught in output larger than the pipe buffer' 1 \
    run "$big_leak" "$clean_syms" --check=leak
expect_says   'the large-output leak is named, not swallowed' 'wl_log_test_last'
expect_status 'provenance still resolves in output larger than the pipe buffer' 0 \
    run "$clean_syms" "$big_prov" --check=provenance

# --- argument handling -------------------------------------------------------
missing_lib() { PATH="$bin:$PATH" NM_PLAIN="$clean_syms" NM_LINES="$good" "$gate" "$tmp/nonexistent"; }
expect_status 'a missing library is an error, not a skip' 2 missing_lib
bad_flag() { PATH="$bin:$PATH" NM_PLAIN="$clean_syms" NM_LINES="$good" "$gate" --check=bogus "$build"; }
expect_status 'an unknown --check value is rejected' 2 bad_flag

# --- against the real library (criterion 1) ---------------------------------
# The stubs above prove the parsing; this proves the parsing matches what this
# platform's nm actually emits, which is the part a stub cannot establish.
# meson passes its build root as $1 so this does not depend on a directory
# guessed by name; without it a CI build dir called anything else silently
# skipped the only assertion a stub cannot substitute for.
# Try each candidate and keep going until one actually carries line info: meson
# passes its build root, which defaults to buildtype=release and cannot answer,
# and stopping there would skip the assertions a stub cannot substitute for.
# The dylib name is used, not just found -- the previous version located a
# dylib and then ran nm on a hardcoded libwirelog.so, so the macOS path could
# never run.
real=""
real_lib=""
real_nm=""
for d in ${1:+"$1"} "$root/build-debug" "$root/build"; do
    # Mirror the gate's own search order and take the FIRST match, rather than
    # the first that happens to carry line info. Picking a different library
    # than the gate would makes the assertions below judge an artifact the gate
    # never looks at: on a tree holding a stripped .so and a debug .dylib, the
    # probe chose the dylib, the gate chose the .so, and they disagreed.
    cand=""
    for c in "$d"/libwirelog.so "$d"/libwirelog.so.* \
             "$d"/libwirelog.dylib "$d"/libwirelog.*.dylib "$d"/libwirelog.a; do
        [[ -f "$c" ]] && { cand=$c; break; }
    done
    if [[ -n "$cand" ]]; then
        probe=$(nm --line-numbers --defined-only "$cand" 2>/dev/null || true)
        # Same clone-aware match the gate uses; a stricter probe here would opt
        # out of exactly the builds the gate handles correctly.
        if [[ -n "$probe" ]] && awk -F'\t' 'NF >= 2 {
                split($1, f, /[ \t]+/)
                if (f[3] ~ /^_?wl_log_emit(\.[A-Za-z0-9_.]+)?$/ && $NF ~ /:[0-9]+$/) found = 1
            } END { exit found ? 0 : 1 }' <<<"$probe"; then
            real=$d; real_lib=$cand; real_nm=$probe
            break
        fi
    fi
done
# real_nm is the probe's own output, captured in the loop above -- not re-read
# here. An earlier version re-read a hardcoded libwirelog.so at this point,
# which discarded the candidate the loop had just chosen and reintroduced the
# "find a dylib, then run nm on a .so" mismatch it exists to avoid.
#
# It is captured rather than piped for the reason that cost the most time here:
# `nm | grep -q` looks equivalent but is not -- grep -q exits at the first
# match, nm takes SIGPIPE and returns 141, and `set -o pipefail` propagates it,
# so the guard read "no line info" on a library that has it and silently
# downgraded this case to a skip.
if [[ -n "$real_lib" ]]; then
    real_ok() { "$gate" --check=provenance "$real"; }
    expect_status 'the real library resolves its own provenance' 0 real_ok
    expect_says   'the real provenance is log_emit.c' 'log_emit.c'
    # A stub cannot establish this: the symbol is local ('t'), not exported
    # ('T'). The parser deliberately ignores the type column; this asserts the
    # fact that makes that deliberate, so a future "tighten it to ^T$" cannot
    # look harmless -- it would pass every fixture here and still miss the
    # shipped library.
    # Here-string, and no early `exit` in awk: any consumer that stops reading
    # early SIGPIPEs its producer, which under pipefail surfaces as 141. That is
    # what the comment above is about, and it bites here too.
    local_type() {
        local t
        t=$(awk '$3 ~ /^_?wl_log_emit(\.[A-Za-z0-9_.]+)?$/ && !seen { print $2; seen = 1 }' <<<"$real_nm")
        [[ "$t" == t ]]
    }
    expect_status 'wl_log_emit is local, so the parser must not require ^T$' 0 local_type
else
    printf 'test-check-no-testhook: SKIPPED real-library case (no debug build present)\n'
fi

if ((failures)); then
    printf 'test-check-no-testhook: %d case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'test-check-no-testhook: all cases passed\n'
