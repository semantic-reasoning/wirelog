#!/usr/bin/env python3
"""Decide whether a pull request must run the required Perf Suite (#2084).

The required check `Perf Suite (col_rel_compact_runs / heap surfaces)`
protects the K-way merge compaction (`col_rel_compact_runs`,
`col_rel_compact_runs_prepared`, `COMPACT_SIFT_DOWN`) that #738 put under a
per-PR perf gate.  That code moved from wirelog/columnar/ops.c to
wirelog/columnar/merge.c (9c26ca49).  The workflow's path filter still fires
on ops.c, the code's former home, so this classifier lets an ops.c edit that
cannot alter the compaction implementation skip the strict suite, while
every change to the protected surface still runs it.

What "cannot alter" means here is narrow and syntactic.  An ops.c edit is
judged unrelated only when every changed line, on both sides of the diff,
read the way the compiler reads it (backslash-newlines spliced first),

  - is a comment, or sits inside a function body at every character of the
    line;
  - is not a preprocessor directive;
  - uses no `asm`, no `_Pragma`, no `[[...]]` attribute and no reserved
    `__` identifier (so no `__attribute__`, `__asm__` or `__declspec` in any
    spelling), through which a line inside a body can still define a
    global symbol or load-time code; and
  - does not name `col_rel_compact_runs`, `col_rel_compact_runs_prepared` or
    `COMPACT_SIFT_DOWN`.

Every unchanged line must also keep its kind, so an edit cannot re-scope
the code around it (say, nesting the next function inside this one).

Such an edit can still change what ops.c does at run time, including how
often it calls into code that compacts; it cannot change the compaction code
itself, which is what the check protects.  The file must also contain no
lone carriage return, trigraph or digraph, whose line or token structure the
scanner does not model.

The verdict is computed from the complete pull-request diff, merge-base of
the two SHAs to the head SHA, read straight from git objects.  It is
fail-closed: anything the classifier cannot prove unrelated -- a protected
path on either side of a rename or copy, a rename/deletion/type change or
content-less change of ops.c, an unknown change type, missing history,
malformed input, or an internal error -- yields `applicable=true`.

The workflow runs this script as it exists at the pull request's base SHA,
so a pull request cannot change these rules by editing this file.  A
pull_request workflow does run the workflow definition from the pull request
itself, so a pull request that edits the workflow can still change how it is
judged; the workflow is a protected path, and reviewing such edits is the
backstop.

Usage:
    classify-perf-surface.py --repo DIR --base SHA --head SHA [--summary FILE]

Prints exactly one line, `applicable=true` or `applicable=false`, on stdout
and exits 0.  A Markdown report goes to --summary when given.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path


# Paths whose change always runs the strict suite, in GitHub's `paths` glob
# syntax (`**/` matches zero or more directories).  The workflow's
# `pull_request.paths` filter must list each of these plus CANDIDATE;
# test-classify-perf-surface.py checks that the two stay in step, and that
# every in-repo file merge.c and the CRDT/CSPA gates include, directly or
# not, is covered.
PROTECTED = (
    "wirelog/columnar/merge.c",
    "wirelog/**/*.h",
    "wirelog/**/*.h.in",
    "config.h.in",
    "tests/test_crdt_perf_gate.c",
    "tests/test_cspa_perf_gate.c",
    "tests/test_perf_util.h",
    "bench/bench_crdt_workload.h",
    "bench/data/crdt/**",
    "bench/data/cspa/**",
    "meson.build",
    "meson_options.txt",
    "tests/meson.build",
    "wirelog/meson.build",
    ".github/workflows/perf-suite-required.yml",
    ".github/actions/setup-meson/**",
    "scripts/ci/classify-perf-surface.py",
    "scripts/ci/test-classify-perf-surface.py",
)

# The one watched path that can be judged unrelated: the former home of the
# compaction code.
CANDIDATE = "wirelog/columnar/ops.c"

PROTECTED_IDENTIFIERS = (
    "col_rel_compact_runs",
    "col_rel_compact_runs_prepared",
    "COMPACT_SIFT_DOWN",
)

SHA_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")
IDENT_RE = re.compile(r"\b(?:" + "|".join(PROTECTED_IDENTIFIERS) + r")\b")
TRIGRAPH_RE = re.compile(r"\?\?[=/'()!<>\-]")
# Extensions that let a line inside a function body place code or data at
# file scope or in load-time sections: inline assembly can define global
# symbols, an attribute can put a static in .init_array, and a pragma can
# change code generation.  Every reserved `__` identifier is included, so
# spellings such as `__attribute` or `__asm` need no list of their own, and
# so are standard `[[...]]` attributes.
EXTENSION_RE = re.compile(r"\b(?:asm|_Pragma|__\w*)|\[\[")
DIGRAPHS = ("<%", "%>", "<:", ":>", "%:")


def glob_regex(pattern: str) -> re.Pattern[str]:
    """Translate a GitHub `paths` glob into an anchored regex."""
    out = []
    i = 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif pattern.startswith("**", i):
            out.append(".*")
            i += 2
        elif pattern[i] == "*":
            out.append("[^/]*")
            i += 1
        elif pattern[i] == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(pattern[i]))
            i += 1
    return re.compile("^" + "".join(out) + "$")


PROTECTED_RES = tuple(glob_regex(pattern) for pattern in PROTECTED)


class Uncertain(Exception):
    """The classifier cannot prove the change unrelated."""


@dataclass
class Verdict:
    applicable: bool = False
    reasons: list[str] = field(default_factory=list)
    unrelated: list[str] = field(default_factory=list)
    ignored: list[str] = field(default_factory=list)
    merge_base: str = "unknown"

    def require(self, reason: str) -> None:
        self.applicable = True
        self.reasons.append(reason)


def is_protected(path: str) -> bool:
    return any(regex.match(path) for regex in PROTECTED_RES)


def git(repo: Path, *args: str) -> str:
    """Run git and return its output with every byte kept.

    Decoding without universal newlines keeps a carriage return as a
    character, so line splitting below sees the same line structure as the
    `@@` hunk numbers, which count only line feeds.
    """
    result = subprocess.run(["git", "-C", str(repo), *args],
                            capture_output=True, check=False)
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", "replace").strip()
        raise Uncertain(f"git {' '.join(args[:2])} failed: "
                        f"{detail or result.returncode}")
    return result.stdout.decode("utf-8", "surrogateescape")


def commit(repo: Path, sha: str, role: str) -> str:
    if not SHA_RE.match(sha):
        raise Uncertain(f"{role} SHA {sha!r} is not a full hexadecimal object id")
    try:
        return git(repo, "rev-parse", "--verify", "--quiet",
                   f"{sha}^{{commit}}").strip()
    except Uncertain:
        raise Uncertain(f"{role} commit {sha} is not in the local history")


def name_status(repo: Path, base: str, head: str) -> list[tuple[str, list[str]]]:
    """Parse `git diff --name-status -z -M -C` into (status, paths)."""
    raw = git(repo, "diff", "--name-status", "-z", "-M", "-C", "--no-color",
              base, head)
    fields = raw.split("\0")
    if fields and fields[-1] == "":
        fields.pop()
    entries = []
    i = 0
    while i < len(fields):
        status = fields[i]
        kind = status[:1]
        if not kind:
            raise Uncertain("empty name-status record")
        npaths = 2 if kind in ("R", "C") else 1
        paths = fields[i + 1:i + 1 + npaths]
        if len(paths) != npaths or any(not p for p in paths):
            raise Uncertain(f"malformed name-status record {status!r}")
        entries.append((status, paths))
        i += 1 + npaths
    return entries


@dataclass
class Scan:
    kinds: list[str]           # per physical line: pp, comment, body, top
    logical_text: list[str]    # per physical line: its spliced logical line


def scan_c(text: str) -> Scan:
    """Classify each physical line of a C file the way the compiler reads it.

    Backslash-newlines are spliced into logical lines first (translation
    phase 2), so a `//` comment or an identifier continued onto the next line
    is seen whole.  A logical line is 'pp' when its first token is `#`,
    'comment' when it holds no code, 'body' when every character lies inside
    a function body -- a brace pair opened at file scope right after `)` --
    and 'top' otherwise.  Raises Uncertain on anything the model does not
    cover: a lone carriage return, a trigraph or digraph, or unbalanced
    braces, comments or literals.
    """
    if re.search(r"\r(?!\n)", text):
        raise Uncertain("a lone carriage return changes the line structure")
    physical = [line[:-1] if line.endswith("\r") else line
                for line in text.split("\n")]
    if TRIGRAPH_RE.search(text):
        raise Uncertain("trigraph in the file")
    logical: list[str] = []
    owner: list[int] = []
    pending = ""
    continued = False
    for line in physical:
        owner.append(len(logical))
        if line.endswith("\\"):
            pending += line[:-1]
            continued = True
            continue
        logical.append(pending + line)
        pending = ""
        continued = False
    if continued:
        raise Uncertain("backslash continuation runs off the end")

    logical_kinds = []
    depth = 0
    function_body = False      # the outermost open brace is a function body
    last_code = ""             # last code character seen at file scope
    in_block_comment = False
    for number, line in enumerate(logical, start=1):
        i = 0
        quote = None
        has_code = False
        first_code = None
        inside_throughout = depth > 0 and function_body
        while i < len(line):
            ch = line[i]
            nxt = line[i + 1] if i + 1 < len(line) else ""
            if in_block_comment:
                if ch == "*" and nxt == "/":
                    in_block_comment = False
                    i += 2
                    continue
                i += 1
                continue
            if quote:
                if ch == "\\":
                    i += 2
                    continue
                if ch == quote:
                    quote = None
                i += 1
                continue
            if ch == "/" and nxt == "*":
                in_block_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "/":
                break
            if ch.isspace():
                i += 1
                continue
            if ch + nxt in DIGRAPHS:
                raise Uncertain(f"digraph {ch + nxt!r} at logical line {number}")
            has_code = True
            if first_code is None:
                first_code = ch
            if ch in "\"'":
                quote = ch
            elif ch == "{":
                if depth == 0:
                    function_body = last_code == ")"
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth < 0:
                    raise Uncertain(f"unbalanced '}}' at logical line {number}")
            if depth == 0:
                inside_throughout = False
                if ch != "}":
                    last_code = ch
            i += 1
        if quote:
            raise Uncertain(f"unterminated literal at logical line {number}")
        if first_code == "#":
            logical_kinds.append("pp")
        elif not has_code:
            logical_kinds.append("comment")
        elif inside_throughout:
            logical_kinds.append("body")
        else:
            logical_kinds.append("top")
    if in_block_comment:
        raise Uncertain("unterminated block comment")
    if depth != 0:
        raise Uncertain(f"braces do not balance (depth {depth} at end)")
    return Scan(kinds=[logical_kinds[o] for o in owner],
                logical_text=[logical[o] for o in owner])


def changed_lines(repo: Path, base: str, head: str, path: str):
    """Return the changed (side, line) pairs of @path and their hunks.

    A hunk is (old_start, old_count, new_start, new_count) in `-U0` form,
    where a count of zero puts the start just before the change.
    """
    diff = git(repo, "diff", "-U0", "--no-color", "--no-ext-diff",
               "--no-textconv", base, head, "--", path)
    changed = []
    hunks = []
    old_no = new_no = 0
    in_hunk = False
    for line in diff.split("\n"):
        match = HUNK_RE.match(line)
        if match:
            old_no = int(match.group(1))
            new_no = int(match.group(3))
            old_count = int(match.group(2) or "1")
            new_count = int(match.group(4) or "1")
            hunks.append((old_no, old_count, new_no, new_count))
            in_hunk = True
            continue
        if not in_hunk or not line:
            continue
        if line.startswith("-"):
            changed.append(("-", old_no))
            old_no += 1
        elif line.startswith("+"):
            changed.append(("+", new_no))
            new_no += 1
        elif line.startswith("\\"):
            continue
        else:
            raise Uncertain(f"unexpected diff line {line[:40]!r}")
    return changed, hunks


def unchanged_pairs(hunks, old_len: int, new_len: int):
    """Yield (old_line, new_line) for every line the diff leaves alone."""
    old_no = new_no = 1
    for old_start, old_count, new_start, new_count in hunks:
        # With a zero count, git names the line before the change.
        old_stop = old_start if old_count else old_start + 1
        while old_no < old_stop:
            yield old_no, new_no
            old_no += 1
            new_no += 1
        old_no += old_count
        new_no += new_count
    while old_no <= old_len:
        yield old_no, new_no
        old_no += 1
        new_no += 1
    if new_no != new_len + 1:
        raise Uncertain("diff hunks do not account for every line")


def classify_candidate(repo: Path, base: str, head: str, path: str,
                       verdict: Verdict) -> None:
    scans = {"-": scan_c(git(repo, "show", f"{base}:{path}")),
             "+": scan_c(git(repo, "show", f"{head}:{path}"))}
    changed, hunks = changed_lines(repo, base, head, path)
    for old_no, new_no in unchanged_pairs(hunks, len(scans["-"].kinds),
                                          len(scans["+"].kinds)):
        if scans["-"].kinds[old_no - 1] != scans["+"].kinds[new_no - 1]:
            verdict.require(f"{path}:{new_no} (new) is unchanged but moves "
                            f"from {scans['-'].kinds[old_no - 1]} to "
                            f"{scans['+'].kinds[new_no - 1]}")
    seen = 0
    for side, number in changed:
        seen += 1
        scan = scans[side]
        if number < 1 or number > len(scan.kinds):
            raise Uncertain(f"{path}: hunk line {number} is outside the file")
        kind = scan.kinds[number - 1]
        where = f"{path}:{number} ({'old' if side == '-' else 'new'})"
        if IDENT_RE.search(scan.logical_text[number - 1]):
            verdict.require(f"{where} names a protected compaction symbol")
        elif EXTENSION_RE.search(scan.logical_text[number - 1]):
            verdict.require(f"{where} uses assembly, an attribute or a "
                            "pragma")
        elif kind == "pp":
            verdict.require(f"{where} changes a preprocessor line")
        elif kind == "top":
            verdict.require(f"{where} changes code outside a function body")
    if seen == 0:
        raise Uncertain(f"{path}: modified with no textual change "
                        "(mode or encoding change)")


def classify(repo: Path, base_sha: str, head_sha: str) -> Verdict:
    verdict = Verdict()
    base = commit(repo, base_sha, "base")
    head = commit(repo, head_sha, "head")
    merge_base = git(repo, "merge-base", base, head).strip()
    if not SHA_RE.match(merge_base):
        raise Uncertain(f"no merge base between {base} and {head}")
    verdict.merge_base = merge_base
    candidate_examined = False
    for status, paths in name_status(repo, merge_base, head):
        kind = status[:1]
        if kind not in "MADRCT":
            verdict.require(f"unknown change type {status!r} for {paths}")
            continue
        protected = [p for p in paths if is_protected(p)]
        if protected:
            verdict.require(f"{status} touches protected {', '.join(protected)}")
            continue
        if CANDIDATE in paths:
            if kind != "M" or len(paths) != 1:
                verdict.require(f"{status} of {CANDIDATE} (only an in-place "
                                "edit can be judged unrelated)")
                continue
            before = len(verdict.reasons)
            classify_candidate(repo, merge_base, head, CANDIDATE, verdict)
            if len(verdict.reasons) == before:
                verdict.unrelated.append(CANDIDATE)
            candidate_examined = True
            continue
        verdict.ignored.append(f"{status} {' -> '.join(paths)}")
    if not verdict.applicable and not candidate_examined:
        verdict.require("no protected path or ops.c edit in the diff, so the "
                        "path filter fired for a reason this classifier "
                        "cannot see")
    return verdict


def md(text: str) -> str:
    """Render untrusted text as inline code that cannot break the summary."""
    cleaned = re.sub(r"[`\r\n<>]", "?", text)
    return f"`{cleaned}`"


def summary_text(verdict: Verdict, base: str, head: str) -> str:
    lines = ["## Perf Suite applicability (#2084)", "",
             f"- base: {md(base)}",
             f"- head: {md(head)}",
             f"- merge base: {md(verdict.merge_base)}",
             f"- verdict: **{'applicable' if verdict.applicable else 'not applicable'}**",
             ""]
    if verdict.reasons:
        lines.append("Runs the strict suite because:")
        lines.extend(f"- {md(reason)}" for reason in verdict.reasons)
    else:
        lines.append("Every change to the watched paths is an ops.c edit "
                     "inside function bodies that names no compaction "
                     "symbol and uses no assembly, attribute or pragma, so "
                     "it cannot alter the compaction code:")
        lines.extend(f"- {md(path)}" for path in verdict.unrelated)
    if verdict.ignored:
        lines += ["", "Outside the protected surface (does not trigger the check):"]
        lines.extend(f"- {md(entry)}" for entry in verdict.ignored[:50])
        if len(verdict.ignored) > 50:
            lines.append(f"- ... and {len(verdict.ignored) - 50} more")
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", required=True)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--summary")
    args = parser.parse_args(argv)
    try:
        verdict = classify(Path(args.repo), args.base, args.head)
    except Uncertain as error:
        verdict = Verdict()
        verdict.require(f"cannot classify: {error}")
    except Exception as error:  # noqa: BLE001 -- any defect must fail closed
        verdict = Verdict()
        verdict.require(f"classifier error: {type(error).__name__}: {error}")
    if args.summary:
        try:
            with open(args.summary, "a", encoding="utf-8") as handle:
                handle.write(summary_text(verdict, args.base, args.head))
        except OSError as error:
            print(f"warning: cannot write summary: {error}", file=sys.stderr)
    print(f"applicable={'true' if verdict.applicable else 'false'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
