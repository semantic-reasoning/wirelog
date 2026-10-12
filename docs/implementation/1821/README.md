# Issue 1821: unfinished checkpoint

This branch preserves 51 preceding implementation commits and unfinished
observer/seed work at the user's explicit request. It is a draft handoff,
not a completed implementation or an approval to merge. Issue #1821 stays open.

- [Handoff and remaining work](HANDOFF.md)
- [All preceding commits and changed paths](commits.md)
- [Preserved plans, reviews, and test evidence](evidence/)
- [Current post-format normal validation](evidence/1821-checkpoint-root-validation.json)
- [Independent handoff completeness audit](evidence/1821-checkpoint-architect-audit.json)

Start with the accepted but unimplemented F1 installed-target credit check in
the typed seed cleanup footer. Complete lazy/private-growth/COW/arena seed
branches before enabling the observed STEP caller. Default differential,
legacy retry/snapshot tests, complete checkpoint inventory, latest-main
integration, and native Windows validation remain required.

The latest normal focused check passed after formatting. The latest historical
ASan check timed out under the normal 120-second deadline; a 240-second
diagnostic passed on an earlier source epoch. Current full-profile validation
and production review remain outstanding.

Create a fresh checkout and build environment on another machine. Historical
absolute HOME paths in receipts are provenance, not required destination paths.
Use a uv Python environment, HOME TMPDIR, at most eight compiler jobs, one LTO
thread, and sequential profiles. Existing enabled-profile caches require an
explicit `-Db_lto_threads=1` before reuse. The implementation agents have stopped;
the next agent should take sole source ownership before resuming.
