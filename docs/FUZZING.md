# Fuzzing

Current `#743` scaffolding includes parser, CSV reader, intern, and compound
arena targets.

- `parser_fuzz` exercises `wl_parser_parse_string()` via `LLVMFuzzerTestOneInput`.
- `csv_reader_fuzz` exercises `wl_csv_parse_line()` and `wl_csv_parse_line_ex()`
  at the line level.
- `intern_fuzz` exercises `wl_intern_*()` APIs with randomized symbol tokens.
- `compound_arena_fuzz` exercises bounded `wl_compound_arena_*()` allocation,
  lookup, retain, freeze/unfreeze, GC-boundary, and live-handle paths.

## libFuzzer Smoke

```sh
CC=clang meson setup build-fuzz -Denable_fuzz=true -Db_sanitize=address --wipe
meson compile -C build-fuzz parser_fuzz csv_reader_fuzz intern_fuzz compound_arena_fuzz
meson test -C build-fuzz --suite fuzz --print-errorlogs
```

The default profile remains unchanged. `-Denable_fuzz=true` gates the extra
targets and requires a Clang + libFuzzer-capable toolchain. The Meson smoke
tests use `tests/fuzz/run_fuzz_smoke.sh`, which copies each tracked seed corpus
from `tests/fuzz/corpus/` into a build-tree work directory before running the
target. Source corpora are not mutated.

## libFuzzer Soak Runner

Issue `#874` adds a release-evidence runner for longer libFuzzer campaigns.
It uses the same compiled fuzz binaries and copies committed seeds into
writable per-target corpus directories before each run.

Short local validation:

```sh
scripts/fuzz/run-libfuzzer-soak.sh \
  --build-dir build-fuzz --target parser --duration 5s
```

Release soak invocation:

```sh
scripts/fuzz/run-libfuzzer-soak.sh \
  --build-dir build-fuzz --all --duration 24h
```

The runner writes per-target logs, artifacts, and metadata under its work
directory. The `24h` command is the intended release evidence producer for
`#684` / `#694`, but the runner itself does not prove that a soak happened
until that command has actually been executed and its artifacts retained.

## AFL++

The `scripts/afl/*.sh` entrypoints run AFL++ against the same four target
surfaces:

```sh
scripts/afl/parser.sh --binary build-fuzz/tests/parser_fuzz
scripts/afl/csv_reader.sh --binary build-fuzz/tests/csv_reader_fuzz
scripts/afl/intern.sh --binary build-fuzz/tests/intern_fuzz
scripts/afl/compound_arena.sh --binary build-fuzz/tests/compound_arena_fuzz
```

Each script assumes the target binary already exists from a fuzz build or a
compatible AFL-instrumented build. The scripts do not rebuild the project. For
normal AFL++ campaigns, use AFL-compatible instrumentation unless deliberately
testing one of AFL++'s uninstrumented modes.

Like the Meson smoke helper, each AFL++ script copies the tracked seed corpus
into a writable work directory before starting `afl-fuzz`, so source corpora
under `tests/fuzz/corpus/` are not mutated. By default, work and output
directories are created under `${TMPDIR:-/tmp}/wirelog-afl/`; pass `--work-dir`
or `--out-dir` to override that location.

## Validation

Concise local validation for fuzz scaffolding changes:

```sh
git diff --check origin/main..HEAD
bash -n tests/fuzz/run_fuzz_smoke.sh scripts/afl/*.sh
meson test -C build parser --print-errorlogs
CC=gcc meson setup build-fuzz-gcc -Denable_fuzz=true
```

When `clang` is unavailable, record libFuzzer smoke as a local toolchain gap.
When `afl-fuzz` is unavailable, record AFL++ campaign execution as a local
tooling gap and verify the script help and missing-tool error path instead.

## Follow-Ups

Issue `#743` lands the harnesses, seed corpora, AFL++ entrypoints, and
libFuzzer smoke scaffold only. Longer 24h soak runs, corpus workflow, and B7
GitHub comment/reporting integration remain follow-ups under `#694` / `#874`.
