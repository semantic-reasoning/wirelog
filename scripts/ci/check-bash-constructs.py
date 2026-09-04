#!/usr/bin/env python3
"""Check Bash 4+ constructs in Meson's tier-1 shell-script closure.

Tier 1 is the comment-stripped transitive closure of scripts named by
Meson's intro-tests.json. Scripts outside that closure are tier 2.

The file-by-file `bash -n` check uses the Bash executable on PATH; it does not
directly guarantee Bash 3.2 syntax. Actual Bash 3.2 validation runs in the
macOS CI environment. This gate intentionally does not catch the Bash 3.2
set -u empty-array behaviour defects from #1320 and #1324. Also, ${arr[*]} is
not a safe Bash 3.2 rewrite of ${arr[@]}.

The recorded platform floor in bash-tier1-minimum.txt applies only to tier 1;
scripts outside the intro-tests closure are tier 2 and are not scanned by this
static gate. Static expansion and reference tracking are deliberately
fail-closed, but cannot prove runtime-generated paths or shell evaluation
semantics; those remain execution-test responsibilities.
"""
from __future__ import annotations

import json
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

DENY_EXPANSIONS = ("@Q", "^^", ",,", "^", ",")
PARAMETER_OPERATOR = re.compile(r"^(?:[A-Za-z_][A-Za-z0-9_]*|[0-9]+)(\^\^|,,|\^|,)")
DENY_WORDS = {"mapfile", "readarray", "coproc"}
DENY_OPERATORS = {"|&", "&>>", ";;&"}


class GateError(Exception):
    pass


@dataclass
class Token:
    text: str
    line: int
    quoted: bool = False
    kind: str = "word"


def fail(message: str) -> int:
    print(f"check-bash-constructs: FAIL: {message}", file=sys.stderr)
    return 1


def skip(message: str) -> int:
    print(f"check-bash-constructs: SKIP: {message}")
    return 77


def strip_comments(text: str) -> str:
    """Remove unquoted comments and here-document bodies."""
    out: list[str] = []
    quote = ""
    heredocs: list[tuple[str, bool]] = []
    for original in text.splitlines(keepends=True):
        line = original.rstrip("\r\n")
        newline = original[len(line):]
        if heredocs:
            heredoc, heredoc_strip = heredocs[0]
            candidate = line.lstrip("\t") if heredoc_strip else line
            out.append(newline)
            if candidate == heredoc:
                heredocs.pop(0)
            continue
        chars: list[str] = []
        new_heredocs: list[tuple[str, bool]] = []
        escaped = False
        token_start = True
        i = 0
        while i < len(line):
            ch = line[i]
            if escaped:
                chars.append(ch)
                escaped = False
                token_start = False
                i += 1
                continue
            if ch == "\\" and quote != "'":
                chars.append(ch)
                escaped = True
                token_start = False
                i += 1
                continue
            if quote:
                chars.append(ch)
                if ch == quote:
                    quote = ""
                i += 1
                continue
            if ch in "'\"":
                quote = ch
                chars.append(ch)
                token_start = False
                i += 1
                continue
            if ch == "<" and i + 1 < len(line) and line[i + 1] == "<":
                # This is shell syntax only outside quotes.  A here-string
                # (<<<) is not a here-document and must not enqueue a body.
                if i + 2 < len(line) and line[i + 2] != "<":
                    cursor = i + 2
                    strip_tabs = cursor < len(line) and line[cursor] == "-"
                    if strip_tabs:
                        cursor += 1
                    while cursor < len(line) and line[cursor] in " \t":
                        cursor += 1
                    delimiter = ""
                    if cursor < len(line) and line[cursor] in "'\"":
                        delimiter_quote = line[cursor]
                        cursor += 1
                        start = cursor
                        while cursor < len(line) and line[cursor] != delimiter_quote:
                            cursor += 1
                        if cursor < len(line):
                            delimiter = line[start:cursor]
                    else:
                        start = cursor
                        while cursor < len(line) and line[cursor] not in " \t;|&<>":
                            cursor += 1
                        delimiter = line[start:cursor]
                    arithmetic_shift = line.rfind("((", 0, i) > line.rfind("))", 0, i)
                    if delimiter and not arithmetic_shift:
                        new_heredocs.append((delimiter, strip_tabs))
            if ch == "#" and token_start:
                chars.extend(" " * (len(line) - i))
                break
            chars.append(ch)
            token_start = ch.isspace() or ch in ";|&()<>"
            i += 1
        cleaned = "".join(chars)
        out.append(cleaned + newline)
        heredocs.extend(new_heredocs)
    return "".join(out)


def lex(text: str) -> tuple[list[Token], list[tuple[int, str]]]:
    tokens: list[Token] = []
    expansions: list[tuple[int, str]] = []
    i = 0
    line = 1
    while i < len(text):
        ch = text[i]
        if ch == "\n":
            tokens.append(Token("\n", line, kind="newline"))
            line += 1
            i += 1
            continue
        if ch.isspace():
            i += 1
            continue
        op = next((item for item in ("&>>", ";;&", "|&") if text.startswith(item, i)), None)
        if op:
            tokens.append(Token(op, line, kind="operator"))
            i += len(op)
            continue
        start_line = line
        pieces: list[str] = []
        quoted = False
        quote = ""
        unquoted = False
        while i < len(text):
            ch = text[i]
            if not quote and (ch.isspace() or ch in "|&;()<> \n"):
                break
            if not quote and next((item for item in ("&>>", ";;&", "|&") if text.startswith(item, i)), None):
                break
            if ch == "\n":
                line += 1
                pieces.append(ch)
                i += 1
                continue
            if not quote and ch in "'\"":
                quote = ch
                quoted = True
                i += 1
                continue
            if quote and ch == quote:
                quote = ""
                i += 1
                continue
            if ch == "\\" and quote != "'" and i + 1 < len(text):
                pieces.append(text[i + 1])
                unquoted = not quote
                i += 2
                continue
            if ch == "$" and quote != "'" and i + 1 < len(text) and text[i + 1] == "{":
                end = parameter_end(text, i)
                if end >= 0:
                    expansions.append((start_line, text[i + 2:end]))
            pieces.append(ch)
            if not quote:
                unquoted = True
            i += 1
        if pieces:
            tokens.append(Token("".join(pieces), start_line, quoted and not unquoted))
        else:
            tokens.append(Token(ch, line, kind="operator"))
            i += 1
    return tokens, expansions


def parameter_end(text: str, start: int) -> int:
    """Find the matching brace for ${...}, including nested ${...} forms."""
    depth = 1
    i = start + 2
    while i < len(text):
        if text.startswith("${", i):
            depth += 1
            i += 2
            continue
        if text[i] == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def command_positions(tokens: list[Token]) -> set[int]:
    positions: set[int] = set()
    at_command = True
    in_for_list = False
    for index, token in enumerate(tokens):
        if token.kind == "newline" or token.text in ("|", "||", "|&", ";", ";;", ";;&", "&", "&>>"):
            at_command = True
            continue
        if token.kind == "operator":
            if token.text in ("(", ")"):
                at_command = token.text == "("
            continue
        if token.kind != "word":
            continue
        if in_for_list:
            if token.text == "do":
                in_for_list = False
                at_command = True
            continue
        if at_command and re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", token.text):
            continue
        if at_command:
            positions.add(index)
            at_command = False
        if token.text == "in":
            in_for_list = True
            at_command = False
        elif token.text in ("if", "then", "do", "else", "elif", "case"):
            at_command = True
    return positions


def inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def static_expand(value: str, current: Path, variables: dict[str, str]) -> str | None:
    value = value.strip()
    current_dir = str(current.parent)
    value = re.sub(
        r"\$\((?:[A-Z_][A-Z0-9_]*=\s*)?cd\s+(?:-P\s+--\s+)?\"?\$\(dirname(?:\s+--)?\s+\"(?:\$0|\$\{BASH_SOURCE\[0\]\})\"?\)\"?\s+&&\s+pwd(?:\s+-P)?\)",
        current_dir,
        value,
    )
    value = re.sub(r"\$\(dirname(?:\s+--)?\s+\"(?:\$0|\$\{BASH_SOURCE\[0\]\})\"?\)", current_dir, value)
    if re.search(r"\$\([^)]*\)", value):
        return None
    value = value.replace("${BASH_SOURCE[0]}", str(current)).replace("$0", str(current))
    for _ in range(8):
        before = value
        value = re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", lambda m: variables.get(m.group(1), m.group(0)), value)
        value = re.sub(r"\$([A-Za-z_][A-Za-z0-9_]*)", lambda m: variables.get(m.group(1), m.group(0)), value)
        if value == before:
            break
    if "$" in value:
        return None
    try:
        parts = shlex.split(value, comments=False, posix=True)
    except ValueError:
        return None
    return parts[0] if len(parts) == 1 else None


def source_path(value: str, current: Path, root: Path, variables: dict[str, str]) -> Path | None:
    expanded = static_expand(value, current, variables)
    if expanded is None or not expanded.endswith(".sh"):
        return None
    path = Path(expanded)
    if not path.is_absolute():
        path = current.parent / path
    return path.resolve(strict=False)


def dynamic_script_operand(word: str, provenance: dict[str, str]) -> bool:
    """Recognize an unresolved operand in a statically script-shaped path."""
    if ".sh" in word:
        names = re.findall(r"\$\{?([A-Za-z_][A-Za-z0-9_]*)", word)
        return any(provenance.get(name) != "loop" for name in names)
    return False


def dynamic_command_substitution(line: str) -> tuple[str, str] | None:
    """Return the diagnostic kind for dynamic command substitutions.

    A command substitution used as a bash/sh script operand cannot identify a
    closure member statically.  `bash -c` is a program-string invocation,
    rather than a script reference, so it is intentionally excluded.
    """
    stripped = line.strip()
    if "[[" in stripped:
        return None
    leading_assignment = re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", stripped)
    assignment_prefix = re.match(
        r"^[A-Za-z_][A-Za-z0-9_]*=(?:\s+|[^\s\"']+\s+)", stripped
    )
    if leading_assignment and not assignment_prefix:
        return None
    if assignment_prefix:
        remainder = stripped[assignment_prefix.end():]
        if not re.match(r"^(?:env\s+)?(?:(?:/[^\s/]*)+/)?(?:bash|bash\.exe|sh|sh\.exe)(?:\s|$)", remainder, re.IGNORECASE):
            assigned_value = assignment_prefix.group(0).split("=", 1)[1].strip()
            if assigned_value.startswith(("$(", "${")):
                return None
            if remainder.startswith("$(") or remainder.startswith('"$('):
                return "command", "unresolved dynamic command operand"
            return None
    for candidate in reversed(re.split(r"\|\||&&|[|;]", stripped)):
        candidate = re.sub(r"^(?:if|then|!)+\s+", "", candidate.strip())
        result = dynamic_command_substitution_one(candidate)
        if result:
            return result
    return None


def dynamic_command_substitution_one(stripped: str) -> tuple[str, str] | None:
    had_assignment = False
    while True:
        assignment = re.match(r"^[A-Za-z_][A-Za-z0-9_]*=\S+\s+", stripped)
        if not assignment:
            break
        had_assignment = True
        stripped = stripped[assignment.end():]
    had_env = bool(re.match(r"^env(?:\s|$)", stripped))
    if had_env:
        stripped = stripped[3:].lstrip()
        while True:
            prefix = re.match(r"^(?:-[^\s]+\s+|[A-Za-z_][A-Za-z0-9_]*=\S*\s+)", stripped)
            if not prefix:
                break
            stripped = stripped[prefix.end():]
    interpreter = re.match(r"^(?:(?:/[^\s/]*)+/)?(?:bash|bash\.exe|sh|sh\.exe)(?:\s+|$)", stripped, re.IGNORECASE)
    if interpreter:
        args = stripped[interpreter.end():]
        if re.search(r"(?:^|\s)-c(?:\s|$)", args):
            return None
        if "$(" in args:
            return "script", "unresolved dynamic script operand"
        return None
    if had_assignment or had_env:
        if stripped.startswith("$(") or stripped.startswith('"$('):
            return "command", "unresolved dynamic command operand"
        return None
    if stripped.startswith("$(") or stripped.startswith('"$('):
        return "command", "unresolved dynamic command operand"
    return None


def dynamic_assignment_substitution(value: str, provenance: dict[str, str]) -> bool:
    match = re.search(r"\$\(\s*\"?\$\{?([A-Za-z_][A-Za-z0-9_]*)", value)
    if not match:
        return False
    return provenance.get(match.group(1)) not in ("static", "interpreter", "loop")


def references(path: Path, root: Path) -> tuple[list[Path], list[str]]:
    cleaned = strip_comments(path.read_text(encoding="utf-8"))
    variables = {"script_dir": str(path.parent), "repo_root": str(root), "root": str(root), "ROOT": str(root), "repo": str(root)}
    provenance = {name: "static" for name in variables}
    provenance["BASH"] = "interpreter"
    found: list[Path] = []
    errors: list[str] = []
    in_conditional = False
    in_continuation = False
    pending_interpreter = False
    for line_no, raw in enumerate(cleaned.splitlines(), 1):
        line = raw.strip()
        if not line:
            continue
        dynamic_substitution = (
            None
            if in_conditional or in_continuation
            else dynamic_command_substitution(line)
        )
        if "[[" in line and "]]" not in line:
            in_conditional = True
        if "]]" in line:
            in_conditional = False
        in_continuation = line.endswith("\\")
        if pending_interpreter and "$(" in line:
            dynamic_substitution = ("script", "unresolved dynamic script operand")
            pending_interpreter = False
        elif re.search(r"(?:^|\s)(?:(?:/[^\s/]*)+/)?(?:bash|bash\.exe|sh|sh\.exe)\s+\\$", line, re.IGNORECASE):
            pending_interpreter = True
        elif not line.endswith("\\"):
            pending_interpreter = False
        if dynamic_substitution:
            _, diagnostic = dynamic_substitution
            errors.append(f"{path}:{line_no}: {diagnostic} {line!r}")
            continue
        assignment = re.match(r"^(?:local\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
        for parameter in re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)=\$[0-9]+", line):
            provenance[parameter] = "parameter"
        if assignment:
            value = static_expand(assignment.group(2), path, variables)
            if value is not None:
                if value.startswith(assignment.group(1) + "="):
                    value = value[len(assignment.group(1)) + 1:]
                variables[assignment.group(1)] = value
                provenance[assignment.group(1)] = "static"
            else:
                provenance.setdefault(
                    assignment.group(1),
                    "unknown_script" if ".sh" in assignment.group(2) else "unknown",
                )
            # Commands embedded in assignments are still executable script
            # references.  In particular check-release-template.sh invokes
            # extract-changelog-section.sh inside $(...).  Do not let shlex's
            # inability to parse nested quotes hide that edge of the closure.
            if "$(" in assignment.group(2):
                if dynamic_assignment_substitution(assignment.group(2), provenance) and ".sh" not in assignment.group(2):
                    errors.append(
                        f"{path}:{line_no}: unresolved dynamic script operand "
                        f"{assignment.group(2)!r}"
                    )
                for literal in re.findall(r"(?:\$[A-Za-z_][A-Za-z0-9_]*(?:/[^ \t\"'()\\]+)?|[./][^ \t\"'()\\]+)\.sh", assignment.group(2)):
                    expanded = static_expand(literal, path, variables)
                    if expanded is None:
                        errors.append(f"{path}:{line_no}: unresolved script reference {literal!r}")
                    else:
                        candidate = Path(expanded)
                        if not candidate.is_absolute():
                            candidate = path.parent / candidate
                        found.append(candidate.resolve(strict=False))
        try:
            words = shlex.split(line, comments=False, posix=True)
        except ValueError:
            # A valid shell command may contain nested command substitutions
            # that shlex (which is not a shell parser) cannot balance.  Such a
            # line is still covered by the lexer for denylist scanning; only
            # the bounded reference forms that shlex can unambiguously split
            # are considered for closure here.
            words = []
            source_match = re.match(r"^(source|\.)\s+(.+)$", line)
            if source_match:
                candidate = source_path(source_match.group(2), path, root, variables)
                if candidate is None:
                    errors.append(f"{path}:{line_no}: unresolved source reference {source_match.group(2)!r}")
                else:
                    found.append(candidate)
            continue
        if not words:
            continue
        command = words[0]
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", command):
            continue
        loop = re.match(r"for\s+([A-Za-z_][A-Za-z0-9_]*)\s+in\b", line)
        if loop:
            provenance[loop.group(1)] = "loop"
        if command in ("source", "."):
            if len(words) < 2:
                errors.append(f"{path}:{line_no}: unresolved source reference")
                continue
            candidate = source_path(words[1], path, root, variables)
            if candidate is None:
                errors.append(f"{path}:{line_no}: unresolved source reference {words[1]!r}")
            else:
                found.append(candidate)
            continue
        command_base = re.split(r"[\\/]", command)[-1].lower()
        if command_base in ("bash", "bash.exe", "sh", "sh.exe"):
            for word in words[1:]:
                expanded = static_expand(word, path, variables)
                if expanded and expanded.endswith(".sh"):
                    candidate = Path(expanded)
                    if not candidate.is_absolute():
                        candidate = path.parent / candidate
                    found.append(candidate.resolve(strict=False))
                elif word.endswith(".sh") or word.startswith("$"):
                    variable_name = word[1:].split("/", 1)[0].strip("{}") if word.startswith("$") else ""
                    if dynamic_script_operand(word, provenance) or provenance.get(variable_name) == "unknown_script":
                        errors.append(f"{path}:{line_no}: unresolved script reference {word!r}")
                    elif re.fullmatch(r"\$\{?[A-Za-z_][A-Za-z0-9_]*\}?", word) and provenance.get(variable_name) not in (None, "loop", "parameter", "interpreter"):
                        errors.append(f"{path}:{line_no}: unresolved dynamic script operand {word!r}")
            continue
        expanded_command = static_expand(command, path, variables)
        if expanded_command and expanded_command.endswith(".sh") and ("/" in command or command.startswith((".", "$"))):
            candidate = Path(expanded_command)
            if not candidate.is_absolute():
                candidate = path.parent / candidate
            found.append(candidate.resolve(strict=False))
        elif command.startswith("$") and re.fullmatch(r"\$\{?[A-Za-z_][A-Za-z0-9_]*\}?", command):
            variable_name = command[1:].split("/", 1)[0].strip("{}")
            if variable_name not in ("@", "*") and provenance.get(variable_name) not in (None, "loop", "parameter", "interpreter") and (
                dynamic_script_operand(command, provenance)
                or provenance.get(variable_name) == "unknown_script"
            ):
                errors.append(f"{path}:{line_no}: unresolved script reference {command!r}")
            elif variable_name not in ("@", "*") and provenance.get(variable_name) is None:
                errors.append(f"{path}:{line_no}: unresolved dynamic command operand {command!r}")
    tokens, _ = lex(cleaned)
    positions = command_positions(tokens)
    for index in sorted(positions):
        token = tokens[index]
        effective = index
        if token.text in ("command", "exec", "env"):
            effective += 1
            while (
                effective < len(tokens)
                and tokens[effective].kind == "word"
                and (
                    tokens[effective].text.startswith("-")
                    or (token.text == "env" and re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", tokens[effective].text))
                )
            ):
                effective += 1
            if effective >= len(tokens) or tokens[effective].kind != "word":
                continue
        command_token = tokens[effective]
        command_base = re.split(r"[\\/]", command_token.text)[-1].lower()
        if command_token.text in ("source", "."):
            operand = effective + 1
            if operand >= len(tokens) or tokens[operand].kind != "word":
                errors.append(f"{path}:{command_token.line}: unresolved source reference")
            else:
                candidate = source_path(tokens[operand].text, path, root, variables)
                if candidate is None:
                    errors.append(
                        f"{path}:{command_token.line}: unresolved source reference "
                        f"{tokens[operand].text!r}"
                    )
                else:
                    found.append(candidate)
            continue
        if command_base in ("bash", "bash.exe", "sh", "sh.exe"):
            option = effective + 1
            while option < len(tokens) and tokens[option].kind == "word":
                operand = tokens[option].text
                if operand == "-c":
                    break
                if "$(" in operand:
                    errors.append(
                        f"{path}:{token.line}: unresolved dynamic script operand {operand!r}"
                    )
                expanded_operand = static_expand(operand, path, variables)
                if expanded_operand and expanded_operand.endswith(".sh"):
                    candidate = Path(expanded_operand)
                    if not candidate.is_absolute():
                        candidate = path.parent / candidate
                    found.append(candidate.resolve(strict=False))
                option += 1
        elif command_token.text.startswith("$("):
            previous = tokens[effective - 1] if effective else None
            if not previous or previous.kind == "newline" or (
                previous.line == command_token.line
                and previous.text in ("then", "do", "else", "elif", "|", ";")
            ):
                errors.append(
                    f"{path}:{command_token.line}: unresolved dynamic command operand "
                    f"{command_token.text!r}"
                )
        expanded = static_expand(command_token.text, path, variables)
        if expanded and expanded.endswith(".sh") and ("/" in command_token.text or command_token.text.startswith((".", "$"))):
            candidate = Path(expanded)
            if not candidate.is_absolute():
                candidate = path.parent / candidate
            found.append(candidate.resolve(strict=False))
    return found, errors


def seeds_from_intro(intro: Path, root: Path) -> list[Path]:
    try:
        data = json.loads(intro.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GateError(f"cannot read {intro}: {exc}") from exc
    seeds: list[Path] = []
    for item in data:
        command = item.get("cmd") if isinstance(item, dict) else None
        if not isinstance(command, list) or not command:
            continue
        first = str(command[0])
        candidate: str | None = None
        if first.lower().endswith((".sh", ".sh.exe")):
            candidate = first
        elif re.split(r"[\\/]", first)[-1].lower() in ("bash", "bash.exe", "sh", "sh.exe") and len(command) > 1:
            second = str(command[1])
            if second.lower().endswith((".sh", ".sh.exe")):
                candidate = second
        if candidate is None:
            continue
        path = Path(candidate)
        if not path.is_absolute():
            path = root / path
        resolved = path.resolve(strict=False)
        if not inside(resolved, root):
            raise GateError(f"introspection seed escapes source root: {candidate}")
        if not resolved.is_file():
            raise GateError(f"introspection seed does not exist: {resolved}")
        seeds.append(resolved)
    return sorted(set(seeds))


def violations(path: Path) -> list[str]:
    cleaned = strip_comments(path.read_text(encoding="utf-8"))
    tokens, expansions = lex(cleaned)
    result: list[str] = []
    for line, expansion in expansions:
        if PARAMETER_OPERATOR.match(expansion) or expansion.endswith("@Q"):
            result.append(f"{path}:{line}: ${{{expansion}}}")
    command_positions: set[int] = set()
    at_command = True
    for index, token in enumerate(tokens):
        if token.kind == "newline" or token.kind == "operator":
            at_command = True
            continue
        if token.kind != "word":
            continue
        if at_command and re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", token.text):
            continue
        if at_command:
            command_positions.add(index)
            at_command = False
        if token.text in ("then", "do", "else", "elif", "case", "in"):
            at_command = True

    for index, token in enumerate(tokens):
        if token.kind == "operator" and token.text in DENY_OPERATORS:
            result.append(f"{path}:{token.line}: {token.text}")
        if token.kind != "word" or token.quoted or index not in command_positions:
            continue
        if token.text in DENY_WORDS:
            result.append(f"{path}:{token.line}: {token.text}")
        if token.text == "shopt" and index + 1 < len(tokens) and tokens[index + 1].text == "-s":
            option = index + 2
            while option < len(tokens) and tokens[option].kind == "word":
                if tokens[option].text == "globstar":
                    result.append(f"{path}:{tokens[option].line}: shopt -s globstar")
                option += 1
        if token.text in ("declare", "local", "typeset") and index + 1 < len(tokens):
            if tokens[index + 1].text == "-A":
                result.append(f"{path}:{token.line}: {token.text} -A")
        if token.text == "wait" and index + 1 < len(tokens) and tokens[index + 1].text == "-n":
            result.append(f"{path}:{token.line}: wait -n")
    return result


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        return fail("usage: check-bash-constructs.py <builddir> <source-root>")
    build = Path(argv[1]).resolve()
    root = Path(argv[2]).resolve()
    if sys.platform not in ("linux", "darwin"):
        return skip(f"unsupported platform for tier-1 floor: {sys.platform}")
    intro = build / "meson-info" / "intro-tests.json"
    if not build.is_dir():
        return skip(f"meson introspection unavailable: build directory missing: {build}")
    if not intro.is_file():
        return skip(f"meson introspection unavailable: {intro} missing")
    try:
        seeds = seeds_from_intro(intro, root)
        if not seeds:
            raise GateError("introspection contained no source-tree shell test seeds")
        closure = set(seeds)
        queue = list(seeds)
        errors: list[str] = []
        while queue:
            current = queue.pop(0)
            refs, ref_errors = references(current, root)
            errors.extend(ref_errors)
            for ref in refs:
                if not inside(ref, root):
                    errors.append(f"{current}: script reference escapes source root: {ref}")
                elif not ref.is_file():
                    errors.append(f"{current}: script reference does not exist: {ref}")
                elif ref not in closure:
                    closure.add(ref)
                    queue.append(ref)
        if errors:
            return fail("\n".join(sorted(set(errors))))
        syntax_errors: list[str] = []
        for path in sorted(closure):
            checked = subprocess.run(
                ["bash", "-n", str(path)],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            if checked.returncode:
                detail = checked.stderr.strip() or "bash -n returned non-zero"
                syntax_errors.append(
                    f"{path}: PATH bash -n syntax validation failed "
                    f"(Bash 3.2 is exercised by macOS CI):\n{detail}"
                )
        if syntax_errors:
            return fail("\n".join(syntax_errors))
        minimum_path = Path(__file__).with_name("bash-tier1-minimum.txt")
        try:
            platform = sys.platform
            floors = {}
            for record in minimum_path.read_text(encoding="utf-8").splitlines():
                record = record.split("#", 1)[0].strip()
                if not record:
                    continue
                fields = record.split()
                if len(fields) != 2 or not fields[1].isdigit():
                    raise GateError(f"invalid floor record: {record!r}")
                floors[fields[0]] = int(fields[1])
            minimum = floors[platform]
        except (OSError, ValueError, KeyError) as exc:
            return fail(f"invalid {minimum_path}: {exc}")
        if len(closure) < minimum:
            return fail(f"tier-1 closure has {len(closure)} files; minimum is {minimum}")
        found: list[str] = []
        for path in sorted(closure):
            found.extend(violations(path))
        if found:
            return fail("\n".join(found))
        print(f"check-bash-constructs: OK; tier-1 closure has {len(closure)} files, 0 violations, 0 exemptions")
        return 0
    except (OSError, UnicodeError, GateError) as exc:
        return fail(str(exc))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
