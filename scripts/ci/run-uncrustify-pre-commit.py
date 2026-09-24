#!/usr/bin/env python3
"""Run the generated Uncrustify hook without partial worktree/index updates."""

from __future__ import annotations

import hashlib
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from os import open as open_fd
from pathlib import Path


def git(root: Path, args: list[str], *, env: dict[str, str] | None = None,
        check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(root), *args], check=check,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        env=env, timeout=60)


def _identity(path: Path, root: Path) -> tuple[int, int, int, int, int, int]:
    try:
        path.resolve(strict=True).relative_to(root)
    except (OSError, ValueError) as error:
        raise RuntimeError(f"staged path escapes the repository: {path}") from error
    cursor = root
    for part in path.relative_to(root).parts[:-1]:
        cursor = cursor / part
        if cursor.is_symlink():
            raise RuntimeError(f"refusing symlinked parent directory: {cursor}")
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise RuntimeError(f"refusing non-regular staged source: {path}")
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            info.st_ctime_ns, stat.S_IMODE(info.st_mode))


def _write_sibling(path: Path, data: bytes, mode: int, label: str) -> Path:
    fd, name = tempfile.mkstemp(prefix=f".wirelog-{label}-", dir=path.parent)
    temp = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temp, mode)
        return temp
    except BaseException:
        try:
            temp.unlink()
        except OSError:
            pass
        raise


def _cleanup(paths: list[Path]) -> list[str]:
    errors: list[str] = []
    for path in paths:
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        except OSError as error:
            errors.append(f"{path}: {error}")
    return errors


def _root() -> Path:
    result = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                            check=True, stdout=subprocess.PIPE, timeout=5)
    return Path(os.fsdecode(result.stdout.rstrip(b"\n"))).resolve()


def run() -> int:
    root = _root()
    env = os.environ.copy()
    listing = git(root, ["diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z"],
                  env=env, capture=True)
    staged_paths = [os.fsdecode(item) for item in listing.stdout.split(b"\0") if item]
    paths = [path for path in staged_paths if path.endswith((".c", ".h"))]
    if not paths:
        return 0

    formatter = shutil.which("uncrustify")
    if formatter is None:
        print("Error: uncrustify is not installed", file=sys.stderr)
        return 1
    config = root / "uncrustify.cfg"
    if not config.is_file():
        print(f"Error: uncrustify.cfg not found at {config}", file=sys.stderr)
        return 1

    source_paths: list[Path] = []
    originals: list[bytes] = []
    identities: list[tuple[int, int, int, int, int, int]] = []
    for relative in paths:
        completed = git(root, ["diff", "--quiet", "--", relative], env=env,
                        check=False, capture=True)
        if completed.returncode not in (0, 1):
            print(f"Error: unable to inspect staged path {relative}", file=sys.stderr)
            return 1
        if completed.returncode == 1:
            print(f"Error: {relative} has unstaged changes; stage the whole file before committing",
                  file=sys.stderr)
            return 1
        path = root / relative
        try:
            identity = _identity(path, root)
            data = path.read_bytes()
        except (OSError, RuntimeError) as error:
            print(f"Error: cannot safely format {relative}: {error}", file=sys.stderr)
            return 1
        source_paths.append(path)
        originals.append(data)
        identities.append(identity)

    index_env = env.get("GIT_INDEX_FILE")
    index_path = Path(index_env) if index_env else Path(
        os.fsdecode(git(root, ["rev-parse", "--git-path", "index"], env=env,
                        capture=True).stdout.rstrip(b"\n")))
    if not index_path.is_absolute():
        index_path = root / index_path
    index_path = index_path.resolve()
    lock_path = Path(str(index_path) + ".lock")
    lock_fd = None
    lock_created = False
    temp_paths: list[Path] = []
    backups: list[Path | None] = [None] * len(paths)
    applied: list[int] = []
    index_installed = False
    try:
        lock_fd = open_fd(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
        lock_created = True
        if not index_path.is_file():
            raise RuntimeError("active Git index is missing")
        original_index = index_path.read_bytes()
        fd, temp_index_name = tempfile.mkstemp(prefix=".wirelog-index-", dir=index_path.parent)
        temp_index = Path(temp_index_name)
        temp_paths.append(temp_index)
        os.close(fd)
        shutil.copyfile(index_path, temp_index)

        locked_listing = git(root, ["diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z"],
                             env=env, capture=True)
        locked_staged = [os.fsdecode(item) for item in locked_listing.stdout.split(b"\0") if item]
        locked_paths = [path for path in locked_staged if path.endswith((".c", ".h"))]
        if locked_paths != paths:
            raise RuntimeError("staged C/H file list changed while acquiring the index lock")
        for relative, path, identity, original in zip(paths, source_paths, identities, originals):
            diff = git(root, ["diff", "--quiet", "--", relative], env=env,
                       check=False, capture=True)
            if diff.returncode != 0 or _identity(path, root) != identity or path.read_bytes() != original:
                raise RuntimeError(f"staged source changed while acquiring the index lock: {relative}")

        with tempfile.TemporaryDirectory(prefix="wirelog-uncrustify-") as work_dir:
            work = Path(work_dir)
            formatted: list[bytes] = []
            for number, (relative, path, identity) in enumerate(
                    zip(paths, source_paths, identities)):
                if _identity(path, root) != identity or hashlib.sha256(path.read_bytes()).digest() != \
                        hashlib.sha256(originals[number]).digest():
                    raise RuntimeError(f"staged source changed while formatting: {relative}")
                output_path = work / f"formatted-{number}"
                with open(output_path, "wb") as output:
                    result = subprocess.run(
                        [formatter, "-c", str(config), "-f", str(path)],
                        stdout=output, stderr=subprocess.PIPE, check=False, timeout=120)
                if result.returncode != 0:
                    detail = os.fsdecode(result.stderr).strip()
                    raise RuntimeError(
                        f"uncrustify failed for {relative} (exit {result.returncode})"
                        + (f": {detail}" if detail else ""))
                formatted.append(output_path.read_bytes())

            # Re-check every source after all external formatter processes have run.
            for relative, path, identity, original in zip(paths, source_paths, identities, originals):
                if _identity(path, root) != identity or path.read_bytes() != original:
                    raise RuntimeError(f"staged source changed while formatting: {relative}")

            # Prepare same-directory backups and replacements before the first mutation.
            replacements: list[Path] = []
            for number, (path, data, identity) in enumerate(
                    zip(source_paths, formatted, identities)):
                mode = identity[-1]
                backups[number] = _write_sibling(path, originals[number], mode, "backup")
                replacements.append(_write_sibling(path, data, mode, "formatted"))
                temp_paths.append(replacements[-1])

            # Stage into a copy of the index. No actual index entries change until all
            # worktree replacements and git-add operations have succeeded.
            temp_env = env.copy()
            temp_env["GIT_INDEX_FILE"] = str(temp_index)
            for number, path in enumerate(source_paths):
                os.replace(replacements[number], path)
                applied.append(number)
            for relative in paths:
                result = git(root, ["add", "--", relative], env=temp_env,
                             check=False, capture=True)
                if result.returncode != 0:
                    detail = os.fsdecode(result.stderr).strip()
                    raise RuntimeError(f"git add failed for {relative}" +
                                       (f": {detail}" if detail else ""))

            # Prepare the Git index lock while backups are still available. The actual
            # index remains unchanged until the following atomic rename.
            if index_path.read_bytes() != original_index:
                raise RuntimeError("active Git index changed while formatting")
            with os.fdopen(lock_fd, "wb") as lock_stream:
                lock_fd = None
                lock_stream.write(temp_index.read_bytes())
                lock_stream.flush()
                os.fsync(lock_stream.fileno())
            temp_index.unlink()
            temp_paths.remove(temp_index)

        # Leaving the private formatting directory can itself fail. Do that before
        # installing the index so any error still rolls the worktree back safely.
        os.replace(lock_path, index_path)
        index_installed = True

        return 0
    except BaseException as error:
        restore_errors: list[str] = []
        if not index_installed:
            for number in reversed(applied):
                backup = backups[number]
                if backup is None:
                    continue
                try:
                    os.replace(backup, source_paths[number])
                    backups[number] = None
                except OSError as restore_error:
                    restore_errors.append(f"{source_paths[number]}: {restore_error}")
        message = f"Error: transactional Uncrustify hook failed: {error}"
        if restore_errors:
            message += "\nError: worktree rollback also failed: " + "; ".join(restore_errors)
        print(message, file=sys.stderr)
        return 1
    finally:
        if lock_fd is not None:
            try:
                os.close(lock_fd)
            except OSError as error:
                print(f"Warning: unable to close temporary index lock: {error}", file=sys.stderr)
        if lock_created:
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass
            except OSError as error:
                print(f"Warning: unable to remove temporary index lock: {error}", file=sys.stderr)
        cleanup_errors = []
        for paths_to_clean in (
                [path for path in temp_paths if path.exists()],
                [path for path in backups if path is not None and path.exists()]):
            try:
                cleanup_errors.extend(_cleanup(paths_to_clean))
            except OSError as error:
                cleanup_errors.append(str(error))
        if cleanup_errors:
            print("Warning: unable to remove transactional hook temporary files: " +
                  "; ".join(cleanup_errors), file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(run())
