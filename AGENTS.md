# wirelog - Agent Guidelines

## Python

Always create a virtual environment with `uv` and run Python scripts inside it.

```bash
uv venv
source .venv/bin/activate
python script.py
```

## Linting

Always use **clang-format version 18** for C code formatting.
Run `clang-format --version` and verify it starts with `18.` before formatting.

```bash
clang-format --version   # must start with "clang-format version 18."
clang-format --style=file -i <files>
```
