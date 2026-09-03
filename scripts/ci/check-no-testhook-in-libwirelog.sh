#!/usr/bin/env bash
# Issue #287: libwirelog must NOT contain testhook symbols, and its wl_log_emit
# must originate from log_emit.c (not log_testhook.c).
#
# The two claims are separate tests (#1302).  They fail for different reasons
# and, more importantly, one of them can be unanswerable on a build where the
# other still holds: a stripped or release build carries no line numbers, so
# provenance cannot be determined, while the symbol-leak check is unaffected.
# Reporting one status for both meant a provenance skip would have masked a real
# leak verdict, so meson registers them separately.
#
# usage: check-no-testhook-in-libwirelog.sh [--check=leak|provenance|both] [BUILD_DIR]
#
# Exit codes:
#    0 - the selected check passed.
#    1 - violation: testhook symbols present, or wl_log_emit does not come from
#        log_emit.c.
#    2 - usage error; libwirelog is not built under BUILD_DIR; or nm cannot
#        read it / reports no symbols at all.  The last is distinct from a
#        clean library: silence from nm must not read as "nothing to find".
#   77 - SKIP; provenance cannot be determined on this build or with this nm.
#        Reported to meson as a skip, not a pass -- see #1288.
set -euo pipefail

SKIP_EXIT=77

usage() {
    echo "usage: check-no-testhook-in-libwirelog.sh [--check=leak|provenance|both] [BUILD_DIR]" >&2
}

CHECK=both
BUILD_DIR=""
while [ $# -gt 0 ]; do
    case "$1" in
        --check=*) CHECK="${1#--check=}" ;;
        -h|--help) usage 2>&1; exit 0 ;;
        --*)       echo "unknown option: $1" >&2; usage; exit 2 ;;
        *)         BUILD_DIR="$1" ;;
    esac
    shift
done
case "$CHECK" in
    leak|provenance|both) ;;
    *) echo "unknown --check value: $CHECK" >&2; usage; exit 2 ;;
esac

# Only consult git when we actually need the default; `git rev-parse` outside a
# checkout exits 128 and would abort the script under set -e even though the
# caller supplied an explicit build directory.
[ -n "$BUILD_DIR" ] || BUILD_DIR="$(git rev-parse --show-toplevel)/build"

LIB=""
for candidate in "$BUILD_DIR/libwirelog.so" "$BUILD_DIR"/libwirelog.so.* \
                 "$BUILD_DIR/libwirelog.dylib" "$BUILD_DIR"/libwirelog.*.dylib \
                 "$BUILD_DIR/libwirelog.a"; do
    if [[ -f "$candidate" ]]; then LIB="$candidate"; break; fi
done
if [[ -z "$LIB" ]]; then
    echo "ERROR: libwirelog not built under $BUILD_DIR; run 'meson compile -C $BUILD_DIR' first" >&2
    exit 2
fi

# Portable "defined symbols" wrapper. GNU binutils nm supports --defined-only;
# BSD nm (macOS) does not. On BSD, undefined symbols have 'U' in the type
# column and can be filtered via awk. Works on both toolchains.
nm_defined() {
    if nm --defined-only "$1" 2>/dev/null; then
        return 0
    fi
    nm "$1" 2>/dev/null | awk '$2 != "U" && $2 != "u"'
}

TESTHOOK_RE='wl_log_test_last|wl_log_test_count|wl_log_testhook|__log_testhook'

# Capture once, then match the variable.  `nm_defined "$LIB" | grep -q ...` is
# not equivalent: grep -q exits at the first match, nm takes SIGPIPE and exits
# 141, and `set -o pipefail` makes the `if` false -- so a library that DOES leak
# is reported "OK: no testhook symbols".  Silent green on the one check that
# exists to keep a test hook out of a shipped artifact.  Reproduced 5/5 at
# 20,000 symbols; today's .so is under the pipe-buffer threshold, but the
# library grows and this gate also accepts libwirelog.a.
check_leak() {
    local syms hits
    syms=$(nm_defined "$LIB" || true)
    # An unreadable or non-object file makes nm print nothing and exit non-zero
    # into 2>/dev/null, which is indistinguishable from a library with no
    # matching symbols -- so silence has to be an error, not a pass.
    if [ -z "$syms" ]; then
        echo "ERROR: nm produced no symbols for $LIB; not an object file, or unreadable" >&2
        return 2
    fi
    hits=$(grep -E "$TESTHOOK_RE" <<<"$syms" || true)
    if [ -n "$hits" ]; then
        echo "LEAK: testhook symbols present in $LIB" >&2
        printf '%s\n' "$hits" >&2
        return 1
    fi
    echo "OK: no testhook symbols in $LIB"
    return 0
}

# `nm --line-numbers` prints "<addr> <type> <name>\t<file>:<line>".
#
# The predicate this replaces was `$NF ~ /:[0-9]+$/ && $0 ~ / wl_log_emit$/`,
# which can never hold: when the location is present $0 ends with it rather
# than with the symbol name, and when it is absent $NF *is* the symbol name.
# So the provenance check never ran, on any platform, and always reported a
# pass it had not earned (#1302).
#
# The TAB is the field separator, not whitespace: a build path containing a
# space would otherwise truncate the location.  The type column is deliberately
# NOT examined: wl_log_emit is a local symbol ('t') in this library rather than
# an exported one ('T'), so a predicate keyed on an uppercase type would match
# every hand-written fixture and still miss the shipped library.
provenance_of() {
    awk -F'\t' '
        NF < 2 { next }
        {
            split($1, f, /[ \t]+/)
            # ^_? for Mach-O, where symbols carry a leading underscore.  The
            # optional dotted tail accepts GCC clone suffixes such as
            # wl_log_emit.constprop.0: an optimised build can emit only the
            # clone, and a clone of wl_log_emit still originates from the file
            # we are asking about.  The dot matters -- it keeps this from
            # matching a different symbol like wl_log_emit_v2.
            if (f[3] ~ /^_?wl_log_emit(\.[A-Za-z0-9_.]+)?$/ && $NF ~ /:[0-9]+$/) { print $NF; exit }
        }'
}

# Symbols matching the emitter pattern, WHETHER OR NOT they carry a location.
# provenance_of answers "where is it defined"; this answers "is it defined at
# all", and #1305 turns on the difference.
emitter_symbols() {
    awk -F'\t' '
        {
            split($1, f, /[ \t]+/)
            if (f[3] ~ /^_?wl_log_emit(\.[A-Za-z0-9_.]+)?$/) { print f[3] }
        }'
}

check_provenance() {
    local nm_out prov
    if ! nm_out=$(nm --line-numbers --defined-only "$LIB" 2>/dev/null); then
        # Two different causes, and calling both "BSD nm" would file an
        # unreadable file under a benign skip with the wrong reason.
        if ! nm "$LIB" >/dev/null 2>&1; then
            echo "ERROR: nm cannot read $LIB; not an object file, or unreadable" >&2
            return 2
        fi
        echo "SKIP: this nm does not support --line-numbers (BSD nm); cannot verify wl_log_emit provenance in $LIB" >&2
        return "$SKIP_EXIT"
    fi
    if [ -z "$nm_out" ]; then
        echo "ERROR: nm produced no symbols for $LIB; not an object file, or unreadable" >&2
        return 2
    fi
    # Here-string, not a pipe: provenance_of's awk stops at the first match, and
    # a pipe would SIGPIPE the producer once the output exceeds the pipe buffer
    # -- making the exit status depend on where the linker happened to place
    # wl_log_emit.  Verified: with the symbol first in 20k lines of real nm
    # output, the piped form exits 141.
    prov=$(provenance_of <<<"$nm_out")
    if [ -z "$prov" ]; then
        # Three causes, and they are not equally benign. Distinguishing them is
        # the whole of #1305: before this, all three printed the stripped-build
        # message, so a library with 1,261 lines of line-number information and
        # wl_log_emit renamed away was byte-identical to a genuinely stripped
        # build.
        local any_loc emitter_syms
        any_loc=$(grep -cE ':[0-9]+$' <<<"$nm_out" || true)
        emitter_syms=$(emitter_symbols <<<"$nm_out")

        if [ "$any_loc" -eq 0 ]; then
            # (1) No locations anywhere: the build cannot answer. Unchanged.
            echo "SKIP: no line-number information for wl_log_emit in $LIB (stripped or non-debug build); cannot verify provenance" >&2
            return "$SKIP_EXIT"
        fi
        if [ -n "$emitter_syms" ]; then
            # (2a) The emitter IS defined; this listing gives it no location.
            # That is all that is observable -- deliberately not narrated as
            # "the library is intact", because a substituted log_testhook.c
            # compiled without -g produces exactly this shape, and a comment
            # telling the maintainer to stop looking would be the same sin this
            # change removes from the branch above. Skip because there is
            # nothing to verify against, not because nothing is wrong.
            echo "SKIP: $LIB defines wl_log_emit but this listing carries no location for it; cannot verify provenance" >&2
            echo "  symbol(s) found: $(tr '\n' ' ' <<<"$emitter_syms")" >&2
            return "$SKIP_EXIT"
        fi
        # No emitter symbol at all. Whether that is evidence depends on
        # whether the LOCAL symbol table survived, because wl_log_emit is a
        # local ('t') symbol in this library.
        local locals_present
        # /^[tdbrns]$/, not any lowercase: nm prints ELF IFUNC as 'i' and
        # unique-global as 'u' REGARDLESS of binding, so a GLOBAL ifunc survives
        # strip -x and would read as a surviving local -- re-arming the false
        # failure this branch exists to remove. Measured on a two-line ifunc
        # library: after strip -x the only lowercase symbol left is 'i'. This
        # project already builds with -mavx2/-msse4.2, so a CPU-dispatch ifunc
        # is a plausible addition rather than a hypothetical.
        locals_present=$(awk '$2 ~ /^[tdbrns]$/ { n++ } END { print n + 0 }' <<<"$nm_out")
        if [ "$locals_present" -eq 0 ]; then
            # (2c) No local symbols anywhere: the local symbol table was
            # discarded, so a missing LOCAL wl_log_emit is not evidence of
            # anything. This is reachable legitimately -- `strip -x` followed by
            # `objcopy --add-gnu-debuglink` yields exactly this, because GNU nm
            # takes the symbol list from the stripped file while resolving line
            # numbers through the separate debug file. Measured: 96 symbols, 96
            # locations, no locals, no emitter. Calling that tampering would be
            # a false failure in the one gate that must not cry wolf.
            # Accepted detection boundary, stated so nobody "tightens" it back
            # into the false failure: an attacker who strips locals from a
            # tampered library gets a skip rather than a failure. That is
            # unavoidable -- a local symbol's absence genuinely proves nothing
            # here -- and it is no weaker than branch (1) or the nm-empty exit
            # above. The gate declines to answer instead of answering wrongly.
            echo "SKIP: $LIB carries line-number information but no local symbols (locals stripped, e.g. strip -x with a separate debug file); wl_log_emit is local, so its absence is not evidence" >&2
            return "$SKIP_EXIT"
        fi
        # (2b) Locations present, local symbols present, and still no emitter.
        # The local table survived and the emitter is not in it, so something
        # removed it specifically: a rename, a relink against a different
        # translation unit, or a substitution. All three are what this gate
        # exists to catch and all three need a human -- the same condition the
        # unrecognized-provenance branch below answers with 1.
        #
        # This verdict does not rest on line-number quality. "No symbol matches
        # the pattern, and locals are present" is a statement about the SYMBOL
        # TABLE, which every nm vintage reports alike; the line table only
        # establishes that the build could have answered, and misreading it
        # under-detects DWARF, which downgrades this to a skip -- the safe
        # direction.
        echo "FAIL: $LIB carries line-number information but defines no wl_log_emit; the emitter has been renamed, relinked or removed" >&2
        # At 2am the next question is "then what IS in there", so answer it.
        # One awk, no pipeline. `grep | sort -u | head -8` fails two independent
        # ways, both measured on this host: grep exits 1 when nothing matches
        # (the common case here -- most libraries have no stray wl_log symbol),
        # and sort is SIGPIPE-killed at 141 once head stops reading a large
        # listing. pipefail surfaces either, and set -e then aborts before this
        # message is printed -- silently costing the diagnostic this branch
        # exists to give. Two mechanisms and a status that varies by tool is
        # why the fix is removing the pipeline, not tolerating a code.
        local nearby
        nearby=$(awk '
            { for (i = 1; i <= NF; i++)
                  if ($i ~ /wl_log/ && !seen[$i]++ && n < 8) { out = out " " $i; n++ } }
            END { print out }' <<<"$nm_out")
        echo "  wl_log-like symbols present:${nearby:- (none)}" >&2
        return 1
    fi
    case "$prov" in
        *log_testhook.c*)
            echo "LEAK: wl_log_emit in $LIB originates from log_testhook.c ($prov)" >&2
            return 1
            ;;
        *log_emit.c*)
            echo "OK: wl_log_emit provenance = $prov"
            return 0
            ;;
        *)
            # Reaching a verdict, not falling through to 0. An unrecognized
            # origin means either a rename this gate has not been taught about
            # or a genuinely unexpected definition; both need a human, and
            # passing silently is what #1302 is about.
            echo "FAIL: wl_log_emit in $LIB resolves to an unrecognized source ($prov); expected log_emit.c" >&2
            return 1
            ;;
    esac
}

case "$CHECK" in
    leak)       check_leak ;;
    provenance) check_provenance ;;
    both)
        # Run both and report the stronger verdict. A provenance SKIP does not
        # mask the leak result, which is why meson registers them separately;
        # this mode exists for running the gate by hand.
        leak_rc=0; check_leak || leak_rc=$?
        prov_rc=0; check_provenance || prov_rc=$?
        # Propagate the actual status rather than folding everything into 1.
        # Testing `prov_rc = 1` would have treated 2 (unreadable) and 141 as
        # passes -- the same "anything I did not name is fine" mistake as the
        # predicate this issue fixes.
        [ "$leak_rc" = 0 ] || exit "$leak_rc"
        [ "$prov_rc" = 0 ] || [ "$prov_rc" = "$SKIP_EXIT" ] || exit "$prov_rc"
        exit 0
        ;;
esac
