#wirelog - Claude Code Project Configuration

## Git Commit Configuration

**Important**: wirelog commits should NOT include Co-Authored-By information.

When committing to this project:
- Do NOT add "Co-Authored-By: Claude" lines
- Commit message format should be clean author attribution only
- Do NOT use emojis in commit messages (clean text only)

### Reason
wirelog is a professional open-source project with dual licensing (LGPL-3.0 and commercial).
Commits should maintain clear authorship attribution to human developers and the CleverPlant organization.
Commit messages should be professional and emoji-free.

## Development Methodology (Phase 3C onwards)

**Test-Driven Development (TDD):**
- Write tests FIRST, before implementation code
- Each feature/module must have accompanying unit tests
- Regression test suite must pass after every change (20/20 tests minimum)
- Use test-driven approach for all feature work

**Atomic Commits:**
- Each commit should be logically independent and compilable
- Include test changes in the same commit as implementation
- Before committing:
  1. Verify `git diff` shows only logical changes (no formatting-only changes)
  2. Run full test suite: `meson test -C build`
  3. Confirm TSan/ASAN clean (when applicable)

**Peer Review:**
- All implementation changes must be reviewed by a peer before merge
- Code review should cover: correctness, memory safety, performance impact
- Use internal consensus (no formal PRs), but documented sign-off required
- Both implementation author and reviewer sign off on commits

## Project Guidelines

- Language: C11 (strict C11 compliance)
- Build: Meson
- License: LGPL-3.0 + Commercial dual license

See `docs/ARCHITECTURE.md` for design details and `AGENTS.md` for agent-specific guidelines (Python).

## Runtime Diagnostics (Issue #287)

`WL_LOG` is the canonical structured-logger surface. Syntax
`WL_LOG=SECTION:LEVEL[,...]`; sections include `JOIN`, `CONSOLIDATION`,
`ARRANGEMENT`, `EVAL`, `SESSION`, `IO`, `PARSER`, `PLUGIN`, `GENERAL`;
levels are `0..5` (NONE/ERROR/WARN/INFO/DEBUG/TRACE). `*` is a
wildcard; later entries override earlier ones. Examples:

```
WL_LOG=JOIN:4 ./app
WL_LOG=*:2,JOIN:5 WL_LOG_FILE=/tmp/wl.log ./app
```

Release builds should pass `-Dwirelog_log_max_level=error` so the
compile-time ceiling strips disabled sites entirely (no `.text`
bytes, no argument evaluation). `meson test --suite abi` verifies
this via `scripts/ci/check-log-erasure.sh`. `meson test --suite perf`
runs the release-mode microbench gate (skips unless cpufreq governor
= `performance`).

Legacy presence flags `WL_DEBUG_JOIN` and `WL_CONSOLIDATION_LOG` (from
#277) are still honored: any value — including `0` — enables TRACE on
the matching section. `WL_LOG` overrides the shim, including explicit
silence via `WL_LOG=JOIN:0`.

`WL_LOG` is NOT async-signal-safe; do not call from signal handlers.
After fork, call `wl_log_init()` again if the child changes the sink.

The header at `wirelog/util/log.h` is internal — enforced by
`scripts/check_log_header_not_public.sh`. Never include it from a
public header.
