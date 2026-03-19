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
