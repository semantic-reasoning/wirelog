# PR #2364: Windows agent handoff (unfinished Draft)

This is a checkpoint for continuing work on Windows. It does not close #2367, #2368 or #2300 and does not establish PR readiness. #2369 RUNNING/FINISH and #2370 global accounting remain separate. Existing PR #2364 is the single intended implementation bundle for #2367 and #2368.

## Published source and preserved history

- Rebase base: `60e28c6fa7300c51fc2b2bc150daca44f7fce25c` (25 main commits after the old `a8dfa5b3`).
- Rebased original work: 16 atomic commits, ending at `93bd499a018801aeb6cb75a20a22e046783e385e`; none squashed or dropped and no merge commits added.
- Active code checkpoint: `5c7b6e879703c92c8d3ac0790af37df6f54416c5` (source-only snapshot tree; published commit is the current Draft HEAD).
- Final fresh validation: whole Clang build PASS; focused eight-target build PASS; focused 8 PASS /1 FAIL (expression_reduce Native mixed0 consumed3817 versus expected3560, unchanged oracle); all65 changed C/H format PASS; diff-check and threading inventory621 PASS. The complete fresh720-test run finished:705 OK/13 SKIP/2 FAIL. LLVM22 tidy ratchet PASS. Failure1: expression_reduce consumed3817 vs3560 at line194. Failure2: evaluation_empty_expression_sources cleanup/rollback assertion at line1180 (SIGABRT). See [FRESH_VALIDATION.json](FRESH_VALIDATION.json). Both are real unfinished integration/accounting failures, not declared flakes; expected values and test oracles were not changed.
- Protected original main checkout was not changed. A named original stash and original snapshots were retained during verification.

Original-to-rebased mapping is in [COMMIT_MAPPING.json](COMMIT_MAPPING.json). Rebase retained main's newer physical-row filtered JOIN chains, 256-work scalar packets and intern-only scalar context. Unique wide/high-row64/NaN/error tests, generated 65-row recovery tests, 2050-byte string/retained-owner tests and non-COUNT STRING/intern-ranking work were preserved. Obsolete internal checkpoint 50/51 expectations were rebound to actual main phases, not restored in the engine. Main's recursive cursor, private seed/radix/timestamp/schema/rollback fixes were retained alongside the lifecycle changes.

## Active implementation material

The active checkpoint includes staged controlled-operator ownership/initialization and Original/MAIN funding; retained observer/transaction lineage across ended attempts and successor calls; unary/JOIN Source error-forwarding; differential/round/private-object physical retirement before credit return; retained reports and finite acknowledgement; managed merge acknowledgement; actual Session Snapshot/CLOSE/reset/frontier/bridge recovery fixtures; and genuine pre/post-action financial cancellation coverage. Tests preserve independent signed outputs, reports, old public heads, credit/ref baselines and non-replay assertions.

No experimental Native `9caf60fd` or four-FINAL Source `53d56cb5` was automatically ported into active code. Those isolated experiments are archival references only. Historical active Source hash `6b5196c1`, Native `79ac1268` and tree `3b547` identify the pre-rebase snapshot, not the current code checkpoint. Rebase/format changes invalidate those old source identities for new qualification.

## Portable archive

Download [2364-windows-handoff.tar.gz](2364-windows-handoff.tar.gz).

- SHA256: `87e16e6ee73e1c9626e9174c8a8355b14177543e59d07a84d37b17d5f6cc8ea2`.
- Size: 2,226,729 bytes; 152 regular files, no symlinks.
- All 151 manifest-listed file hashes were independently extracted and verified.
- The included original Git bundle plus full binary WIP patch reconstructed EXACT tree `3b54717ab11570eb3db4d8c89b9f75c947ee08b0` in a disposable checkout: all 16 old commits, all 53 WIP paths and all nine added files.
- Includes critical own method code/fixtures, review receipts and source-bound results; original versioned Native/Source experiments are under `ARCHIVAL-NOT-ACTIVE`.
- Excludes the 92-GiB local dataset, large AST/LLVM/preprocessed captures, SDK/CRT headers, third-party DLLs, portable PowerShell binary, environments, caches, credentials and signed URLs. The compact archive is not the complete old sealed dataset.

```powershell
Get-FileHash .\2364-windows-handoff.tar.gz -Algorithm SHA256
New-Item -ItemType Directory -Force .\handoff-reference | Out-Null
tar -xzf .\2364-windows-handoff.tar.gz -C .\handoff-reference
```

Extract into a separate directory. Do not overlay archival Source/Native files or apply the old patch on top of the rebased PR. Original scripts retain Linux paths and pins deliberately; they require fresh genuine Windows compiler/SDK/path/source binding, not blind path edits presented as old proof.

To reconstruct only the old historical context in an INDEPENDENT clone, fetch main (contains prerequisite `a8dfa5b3`), fetch `original-snapshot/pre-rebase-commits.bundle` HEAD, checkout detached `9914e9b4`, then `git apply --check` and `git apply` `original-snapshot/pre-rebase-wip.patch`. The active continuation baseline is the rebased Draft instead.

## First actions on Windows

Use an x64 Visual Studio Developer PowerShell, a short checkout path and the existing PR branch. Record exact cl/link/toolset/SDK and Meson configuration. Disable checkout newline conversion before creating the worktree so source hashes remain comparable. Follow repository AGENTS.md and use uv virtualenv for every Python script.

```powershell
git -c core.autocrlf=false clone --branch codex/2300-operator-recovery-next https://github.com/semantic-reasoning/wirelog.git C:\src\wirelog-2364
Set-Location C:\src\wirelog-2364
git config core.autocrlf false
git fetch origin main
git rev-parse HEAD
uv venv .venv
. .\.venv\Scripts\Activate.ps1
uv pip install meson==1.12.0 ninja
meson --version
ninja --version
(Get-Command cl.exe -CommandType Application | Select-Object -First 1).Source
meson setup build-msvc -Dtests=true -DmbedTLS=disabled
meson compile -C build-msvc
meson test -C build-msvc --no-rebuild --print-errorlogs
```

Start with the actual integrated MSVC build/tests, not another general-purpose provider-analysis framework. Preserve `build-msvc/meson-logs/meson-log.txt`, actual C11 create/join probe command/stderr, selected backend, compile commands and full actor layout. In a separate build directory, try genuine `-Dc_std=c11`; do not force `WL_HAVE_C11_THREADS` or replace thrd_t with DWORD. A default native success is not a C11 success. Preserve full thrd_t return and BOTH equality arguments, including Handle/Tid and real sret/byval/copy endpoints. Do not raise old 392/256 guards as a substitute for justified finite stages.

Useful focused domains include evaluation_serial, evaluation_serial_join_batch_env, evaluation_observer_error_metadata, controlled_recovery, evaluation_differential, evaluation_observer_resources, evaluation_physical_retirement, evaluation_join_scalar_recovery, generated filtered numeric/scalar probe recovery, managed_merge_staged_ack, expression_reduce and recursive_cursor_parity. Confirm actual Meson names with `meson test -C build-msvc --list`. Preserve exact/one-short accounting oracles; do not replace a failing expected value with an observed magic number.

## Actual Windows diagnostic results (not engine qualification)

Temporary branch: `diagnostic/windows-thread-provider-v1`, head `64ab0edd2096a4d73ac5d716d5fc57e850c3bd88`. It has no PR and must not be merged into the implementation branch.

- Run [43541685883](https://github.com/semantic-reasoning/wirelog/actions/runs/43541685883), commit `b2c5c17a`, failed PowerShell parsing before collection. Local isolated official PowerShell 7.7 reproduced 38 errors. Moving seven logical operators to preceding line ends produced zero errors; independent RAW verified the fix. Real function-only Linux process-wrapper tests passed positive/nonzero/throw/timeout/output-bound checks. This is not Windows MSVC data.
- Run [43543099572](https://github.com/semantic-reasoning/wirelog/actions/runs/43543099572), commit `64ab0edd`, parsed but failed BEFORE compiler/toolchain collection: `(Get-Command git.exe -CommandType Application).Source` returned three paths, then `[string]` concatenation passed them as one executable name to Process.Start.
- Artifact `14092384850`, ZIP SHA256 `a686b4b8033a04994d90750f91af8aefa3967b6905be84e424adc1df20eeda2c`, yielded an explicit failure manifest, archived here. No C11 selection, actor/runtime or provider qualification was obtained.
- If reusing that diagnostic, resolve exactly one application path (for git/cl/link/dumpbin/uv/python) using `Select-Object -First 1` and verify selected VS paths. This is an identified NEXT FIX, not applied in the handoff.
- Historical successful main run `43347427613`, Windows job `131635604437`, cl19.44.35228/link14.44.35228, selected Windows-native after real C11 create/join links:NO. Failure cause was not in its published log. Conditional MSVC-C11 is not proven unsupported and cannot be silently waived.

## Remaining method/resource work

1. **Prepared Native initialization.** Isolated tree `4a449288`, Native `9caf60fd`, header `a7805f1a`, CoreControl `777940e6`, CoreSession `9e3dda26` has finite initializer/owned-return experiments; Root CoreSession `89479e79` is a DIFFERENT historical authority. Do not misattribute 777940 to session.c. Actual 28 returns/23 owned exits, all 40 inert checks and 12 tuple fields, initializer/clear/ACK/helper/ABI effects, chronological positive payers and true claim-release branches need complete qualification. Old linear/partial counts are not maxima. Root ABORT1920, NC3072 and unary4096 capacities were real old caller limits; unchanged current policies and quantum256 must be reverified rather than raised.
2. **Trusted thread identity boundary.** Exact adapter v2 has scoped default-OFF approval and 23 independent focused checks. It keeps ordinary engine/helper/argument/result/ABI work in C and visible trusted out-of-line standard identity calls in P. OS/library interiors are EXCLUDED by the declared cooperative-service theorem, not C0/C1 or an OS latency bound. Actual O2 header-inline equality is C with no invented equality P. Missing provider/caller/ABI facts remain UNQUALIFIED with null accepted uppers; one Domain/Counter pair is single-use.
3. **Current actual registration blocker.** Four genuine O2 Linux TUs (C11/POSIX × Native/Serial) compile, but optimization removes standalone framed_thread/prepared_thread IR bodies. All eight registrations reject `actual caller LLVM body unavailable`; no completed measurement or upper exists. Exact containing IR functions/current calls are in HANDOFF-FROZEN.json. Multi-TU constructor composition is an additional source-inspection limitation, not an executed reproducer. Next work is the smallest explicitly reviewed optimized containing-caller/multi-TU binding under the same source-interface theorem (also reassess whether a standalone-helper IR gate is necessary); do not use O0/noinline flags or name whitelists to manufacture success.
4. **Const-record scalar method.** The default-OFF immediate-value-conversion extension is independently approved:272 genuine four-profile fixture groups,252 legacy comparisons,two real MSVC thrd_t16/8 aggregate refusals and four API checks. It preserves actual field/record/enum/conversion/consumer IDs, truth/connectors/call/ABI and all old exclusions. Mere regex broadening or stripping const from Source/AST is forbidden. Composition with the thread adapter still needs exact fresh independent binding/review.
5. **Actual Source matrix.** Requalify the eleven CURRENT224 observer-error FINAL actions plus the actual outer Snapshot metadata144 action and full 208/body/80/caller/denied/report/unlock/return/cleanup intervals. Old raw AST totals225/254/259 are NOT contextual scalar lower bounds and do NOT justify adding stages. Old original-Root quiet(parent=false) had a selected source UPPER230 (refusal228); parent=true stopped at thrd_current. New method fixtures are not new production bounds. Four-FINAL Source experiment `70581393`/`53d56cb5` is archival; its individual80/93 source paths were not full224/caller maxima or a reason to port four phases.
6. **Lifetime/ownership.** Finite Root3b547 C1 caller-liveness classification is independently approved: main actors remain alive across helper returns; observer worker drains terminal0/ACK; reset migration is metadata-only; foreign peers join while owner lives. That is not a new public no-exit rule, a control-ref OS-thread pin or a universal certificate. New9caf changed register/init/ACK/auto-maintain producers still need lineage/terminal-drain proof. All old receipts must be rebound after this rebase, alongside actual reference/Original/report/resource ownership.

Do not expand into unrelated #2369/#2370 completion merely because old whole-Root audits remain blocked. Qualify the routes this bundle actually enables and make the sibling handoff explicit. No accepted upper/Ready/issue-closing claim follows from method approval, Clang cross-target syntax, historical Linux success or standalone Windows probes.

## Validation history and workflow

Historical PRE-REBASE tree3b547 on old main a8dfa: full Clang699 tests =686 OK/13 SKIP/0 FAIL; required LLVM22 tidy ratchet and production/hooks checks passed. GCC, ASan/UBSan and current POSIX serial/batch/metadata/JOIN each passed4/4. Those are useful regression context only and are not results for the new code checkpoint. Fresh results are listed at the top and in the PR body, including any real failures/unavailable MSVC checks.

Use the implementation skill through an actual uv environment; record selected skills and exact candidates. The nontrivial harness selected inspect/classify/plan/critique/implement/focused+broad/RAW/final-design/final-risk/atomic-commit/report, no blocked skill. Independent RAW must precede distinct final design/risk for the same tree; method receipts are not engine final validation. Do not bypass hooks, normalize a failed exact oracle or promote partial/failed ASTs to admitted count inputs. Keep this PR Draft and use Refs #2367/#2368/#2300 until their full acceptance passes.
