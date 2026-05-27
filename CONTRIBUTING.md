# Contributing to wirelog

First off, thank you for considering contributing to wirelog! It's people like you that make wirelog such a great tool for the community.

## Code of Conduct

This project and everyone participating in it is governed by the [wirelog Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to inquiry@cleverplant.com.

## Dual Licensing and CLA

wirelog uses a dual-licensing model:
1. **Open Source**: GNU Lesser General Public License v3.0 (LGPL-3.0)
2. **Commercial**: Proprietary license by CleverPlant

Because of this dual licensing, **all contributors must agree to the [Contributor License Agreement (CLA)](CLA.md)**. By submitting a pull request or patch to this project, you indicate your agreement to the terms in the CLA.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the existing issues as you might find out that you don't need to create one. When you are creating a bug report, please include as many details as possible:
* Use a clear and descriptive title.
* Describe the exact steps which reproduce the problem in as many details as possible.
* Provide specific examples to demonstrate the steps.
* Describe the behavior you observed after following the steps and point out what exactly is the problem with that behavior.
* Explain which behavior you expected to see instead and why.

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, please provide the following information:
* Use a clear and descriptive title.
* Provide a step-by-step description of the suggested enhancement.
* Provide specific examples to demonstrate the steps.
* Describe the current behavior and explain which behavior you expected to see instead.
* Explain why this enhancement would be useful to most users.

### Pull Requests

1. Fork the repo and create your branch from `main`.
2. Ensure you have the `meson` build system installed, along with a C11 compiler and Rust (for Differential Dataflow).
3. Build the project:
   ```bash
   meson setup builddir
   cd builddir
   meson compile
   ```
4. If you've added code that should be tested, add tests.
5. Ensure the test suite passes:
   ```bash
   meson test
   ```
6. Issue that pull request!

## Code Style

wirelog uses automated formatting and linting. The `uncrustify.cfg` file is the source of truth for code style.

### Key Conventions

* **Language:** C11 strict (`-std=c11`)
* **Indentation:** 4 spaces, no tabs
* **Braces:** K&R for control flow, next-line for functions
* **Return type:** Separate line (GNU style)
* **Naming:** `wl_` prefix (internal), `wirelog_` prefix (public API), `_t` suffix (types), `UPPER_SNAKE_CASE` (macros)
* **Comments:** C-style `/* */` only (see NOLINT exception below)
* **Pointer style:** `char *ptr` (Right-aligned)

### Running Checks Locally

**Format check (dry-run):**
```sh
find wirelog/ tests/ -name '*.c' -o -name '*.h' | xargs uncrustify -c uncrustify.cfg --check
```

**Auto-format:**
```sh
find wirelog/ tests/ -name '*.c' -o -name '*.h' | xargs uncrustify -c uncrustify.cfg --replace --no-backup
```

**clang-tidy:**
```sh
meson setup builddir-tidy --reconfigure -Dtests=true 2>/dev/null || meson setup builddir-tidy -Dtests=true
run-clang-tidy-18 -p builddir-tidy wirelog/ tests/
```

### Suppression Policy

* Use `// NOLINTNEXTLINE(check-name)` with a brief reason explaining why
* `// NOLINT` and `// NOLINTNEXTLINE` are the **only permitted** `//`-style comments (tool-required pragmas, not code comments)
* No blanket suppressions without specifying the check name

### Pre-commit Hook (Optional)

You can optionally set up a pre-commit hook for auto-formatting:
```sh
echo '#!/bin/sh
find wirelog/ tests/ -name "*.c" -o -name "*.h" | xargs uncrustify -c uncrustify.cfg --replace --no-backup
git add -u' > .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

See [docs/LINTING.md](docs/LINTING.md) for complete linting documentation, check rationale, and rollout plan.

### Perf-suite gate for heap-touching edits

The CRDT median-time gate at `tests/test_crdt_perf_gate.c` derives its
win from a leading-key shadow array in `col_rel_compact_runs`'s K-way
merge heap (`wirelog/columnar/ops.c`). Because `compact_runs` is
shared with every recursive workload (DOOP, CSPA, Galen, Polonius),
a regression in the heap path silently regresses all of them.

A required-check workflow
(`.github/workflows/perf-suite-required.yml`) reruns
`meson test --suite perf` on any PR that touches `wirelog/columnar/ops.c`
or `wirelog/columnar/internal.h`. The workflow is best-effort on
shared-runner cpufreq stability:

* If `cpufreq=performance` cannot be set, the test self-SKIPs (exit 77).
* If a real regression slips through, the gate FAILs and blocks the PR.

Local reproduction:

```sh
meson setup build-perf --buildtype=release -Dwirelog_log_max_level=trace -Dtests=true
meson compile -C build-perf
sudo cpupower frequency-set -g performance     # if available
taskset -c 0 WIRELOG_PERF_GATE=1 meson test -C build-perf --suite perf --print-errorlogs
```

### Internal commit-series labels in source comments

Internal commit-series labels (such as `Phase 2A`, `Phase 3C-001`,
`Phase 4B`) are an artifact of how individual contributors decompose
work into commit batches. They are **not** meaningful to readers of
the codebase and must not appear in source comments without a public
issue cross-reference. The rule:

* If a source comment in `wirelog/**/*.{c,h}` references a phase label
  matching `Phase \d+[A-Z]`, the same line must include a public issue
  reference (`#NNN`, `Issue NNN`, or `US-N-NNN`).
* If the label refers to historical work whose context is no longer
  necessary, **remove** the bare phase token rather than retroactively
  inventing an issue link.
* Allowlisted historical entries are listed in
  `scripts/ci/phase-label-allowlist.txt`. The CI gate
  (`meson test --suite abi:phase_labels` /
  `scripts/ci/check-phase-labels.sh`) fails if a new unanchored label
  appears outside that allowlist. The allowlist may shrink as cleanup
  PRs land; it should not grow.

This is the source-comment counterpart to the rule (recorded in
project-wide assistant config) that forbids internal phase labels in
commit messages and PR descriptions.

### `Status: Future` entries in docs/SEMANTICS.md

`docs/SEMANTICS.md` is the canonical reference for semantic-model
decisions and labels each entry `Status: Current` or `Status: Future`.
A 1.0 stable contract that says "this is Future" with no schedule is
an unbounded liability; external embedders cannot estimate when a
Future entry becomes Current.

The rule:

* Every `Status: Future` heading in `docs/SEMANTICS.md` MUST cite a
  milestone or issue reference within the 8-line window starting at
  the heading. Acceptable anchors are `#NNN`, `Milestone vX.Y`, or
  `Target: milestone vX.Y`.
* The gate `meson test --suite abi:semantics_future` /
  `scripts/ci/check-semantics-future.sh` enforces this. It exits 0
  cleanly when there are no Future entries (i.e. when the doc is
  fully Current).

## General Styleguides

* Write clear, self-documenting code.
* Add comments for complex logic or design decisions.
* When adding documentation or updating existing texts, adhere to Markdown formatting.

## Changelog Conventions

`CHANGELOG.md` follows a Keep-a-Changelog-style format with a project-
specific format gate enforced by
`scripts/ci/check-changelog-format.py` (registered as
`meson test --suite abi:changelog_format`).  Every PR that introduces
user-visible behaviour MUST add a bullet to the `[Unreleased]` section
under the appropriate category, with a reference to the PR or
originating issue.

### Allowed categories

Each `[Unreleased]` bullet sits under exactly one of:

* `### Added` — new public-facing functionality
* `### Changed` — changes to existing behaviour (incl. ABI breaks)
* `### Deprecated` — soon-to-be-removed features (warning fired)
* `### Removed` — features removed in this release
* `### Fixed` — bug fixes
* `### Performance` — measurable performance changes (gated by
  `meson test --suite perf`)
* `### Security` — vulnerability fixes (CVE refs encouraged)
* `### Documentation` — doc-only changes that nonetheless affect a
  user-visible contract

### Required content per bullet

Every bullet MUST include at least one PR or issue reference (`#N`).
The format gate trips when an `[Unreleased]` bullet lacks any `#N`
match.

Example (good):

```md
### Added

- **Foo widget** (#123): introduces the `wirelog_foo_*` API for
  bar-style integration.  See `docs/FOO.md` for usage.
```

Example (will fail the gate):

```md
### Added

- Foo widget: introduces the wirelog_foo_* API
```

### Cutover procedure

When cutting a release tag (`vX.Y.Z`):

1. Move the entire `[Unreleased]` block under a new heading
   `## [X.Y.Z] - YYYY-MM-DD`.  The format gate enforces the date
   format on versioned sections.
2. Reset `[Unreleased]` to the placeholder shape:
   ```md
   ## [Unreleased]

   ### Added

   ### Changed

   ...
   ```
   Empty category bullets are fine; the gate only fails on bullets
   that lack a #N reference, not on empty categories.
3. Open the release PR; CI will refuse to merge if the gate fails.

### Freeze rule (post-1.0 cut)

After `1.0` is cut for v1.0 RC1 (#685), the `[Unreleased]`
section on `1.0` is **frozen** — every hotfix landing on
`1.0` between rc1 cut and GA tag updates the versioned
section, not `[Unreleased]`.  This is mechanically enforced by
the per-branch CI rule landed under #747 (B18).
