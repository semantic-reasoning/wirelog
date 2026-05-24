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

## libFuzzer Corpus Minimization

After a successful soak, stage a minimized corpus into a separate output root:

```sh
scripts/fuzz/minimize-libfuzzer-corpus.sh \
  --build-dir build-fuzz --input-dir path/to/soak-root \
  --out-dir path/to/minimized-root --all
```

For a single target, `--input-dir` may point directly at that target's corpus.
The output layout mirrors the soak evidence shape:
`<out-dir>/<target>/corpus`, `<out-dir>/<target>/logs/minimize.log`,
`<out-dir>/<target>/artifacts`, `<out-dir>/<target>/metadata.txt`, plus a
top-level `manifest.txt`.

Minimized corpora are not committed by the script. Committing any corpus update
is a later reviewed step after successful soak and minimization evidence has
been retained for `#874` / `#684`.

## Manual Fuzz Evidence Workflow

`.github/workflows/fuzz-evidence.yml` is a manual `workflow_dispatch` workflow
for release-candidate evidence. It is not scheduled, it is not triggered on PR
or push, and it is not a required CI gate.

Launch it before an RC from GitHub Actions with:

- `target`: `all`, `parser`, `csv_reader`, `intern`, or `compound_arena`
  (default: `all`).
- `duration`: per-target libFuzzer duration such as `60s`, `10m`, or `24h`
  (default: `60s`).
- `runs_on`: JSON runner labels for the Actions job
  (default: `["ubuntu-latest"]`).
- `timeout_minutes`: GitHub Actions job timeout in minutes, separate from the
  per-target libFuzzer duration (default: `360`).

The hosted default runner is only for smoke/evidence-path validation. For a
hosted smoke run, leave `runs_on=["ubuntu-latest"]`, `duration=60s`, and
`timeout_minutes=360`. That default path does not produce release evidence.
Release evidence for `#874` / `#684` / `#694` requires an explicit long run,
normally `duration=24h`, and a runner whose runtime policy supports the
requested duration. Use `runs_on` to select appropriate self-hosted or larger
runner labels for planned 24h evidence.

For a single-target 24h release run, select one target such as `parser`, set
`duration=24h`, set `runs_on` to the long-running runner labels, and set
`timeout_minutes` above 1440 plus setup/minimization/upload buffer; `1500` is a
reasonable starting point. The workflow validates the timeout estimate before
building.

The duration is per target. `target=all` runs the four targets sequentially for
that duration each, so release evidence should usually be launched as separate
per-target workflow runs or on a runner whose runtime policy covers the full
all-target runtime. `target=all` with `duration=24h` is about 96h plus
overhead. The workflow rejects runs whose estimated runtime exceeds
`timeout_minutes`, and rejects the hosted default `["ubuntu-latest"]` runner
labels whenever the estimate exceeds the hosted 6h limit.

The workflow builds the four libFuzzer targets with Clang, writes soak evidence
under `fuzz-evidence/soak`, and, only if the soak step succeeds, writes
minimized corpora under `fuzz-evidence/minimized`. Artifacts are uploaded with
`if: always()`, so crashes and nonzero exits should still retain available soak
logs and reproducers even when minimization does not run. Artifact upload after
runner cancellation or timeout is best-effort, and long-run artifact upload,
token, and runtime policies are runner-dependent. Planned release evidence
should use a runner that is not expected to time out or be cancelled.

Retained release evidence should include:

- the GitHub workflow run URL,
- the uploaded `fuzz-evidence` artifact bundle,
- `soak/manifest.txt`,
- each target's soak `metadata.txt` and `logs/libfuzzer.log`,
- crash/reproducer artifacts under each target's `artifacts/`,
- `minimized/manifest.txt` and minimized target corpus roots when soak succeeds.

Pass/fail policy: any libFuzzer crash or nonzero target exit blocks the RC.
Keep the uploaded reproducers/logs, file a follow-up with the failing target and
artifact paths, fix the issue, and rerun the evidence workflow before retrying
the RC.

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
bash -n tests/fuzz/run_fuzz_smoke.sh scripts/afl/*.sh scripts/fuzz/*.sh
meson test -C build parser --print-errorlogs
CC=gcc meson setup build-fuzz-gcc -Denable_fuzz=true
```

When `clang` is unavailable, record libFuzzer smoke as a local toolchain gap.
When `afl-fuzz` is unavailable, record AFL++ campaign execution as a local
tooling gap and verify the script help and missing-tool error path instead.

## Follow-Ups

Issue `#743` landed the initial harnesses, seed corpora, AFL++ entrypoints, and
libFuzzer smoke scaffold. Issue `#874` adds the reproducible libFuzzer soak
runner, corpus minimizer, and manual evidence workflow in this branch.

Actual pre-RC 24h evidence still requires executing the workflow, retaining the
uploaded artifacts, and linking the run evidence before release. B7 / `#694`
reporting also remains follow-up work until those retained artifacts are
referenced from the release evidence trail.
