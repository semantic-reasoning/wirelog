# wirelog - Agent Guidelines

## Python

Always create a virtual environment with `uv` and run Python scripts inside it.

```bash
uv venv
source .venv/bin/activate
python script.py
```

## Naming Convention

### Prefix Rules

| Scope | Function/Type Prefix | Macro/Enum Prefix |
|-------|---------------------|-------------------|
| **Public API** (installed headers) | `wirelog_` | `WIRELOG_` |
| **Internal** (not installed) | `wl_` | `WL_` |

**Public API headers** (installed, user-facing):
`wirelog/wirelog.h`, `wirelog/wirelog-types.h`, `wirelog/wirelog-ir.h`, `wirelog/wirelog-parser.h`, `wirelog/wirelog-optimizer.h`

**Internal headers** (not installed):
Everything under `wirelog/parser/`, `wirelog/ir/`, `wirelog/ffi/`, `wirelog/io/`, and `wirelog/backend.h`, `wirelog/session.h`, `wirelog/intern.h`

### Subdirectory Encoding

Internal symbols must encode their subdirectory path in the prefix:

```
wirelog/{subdir}/{file}.h  →  wl_{subdir}_{file}_*
```

Examples:
- `wirelog/parser/ast.h` → `wl_parser_ast_node_t`, `wl_parser_ast_node_create()`
- `wirelog/ir/stratify.h` → `wl_ir_stratify_dep_graph_t`
- `wirelog/ffi/dd_ffi.h` → `wl_ffi_plan_t`, `wl_ffi_op_t`

Top-level internal headers use the file name: `wl_backend_*`, `wl_session_*`, `wl_intern_*`

### Include Guards

```c
/* Public: wirelog/wirelog.h */
#ifndef WIRELOG_H
#define WIRELOG_H

/* Internal: wirelog/parser/ast.h */
#ifndef WL_PARSER_AST_H
#define WL_PARSER_AST_H
```

See [issue #75](https://github.com/justinjoy/wirelog/issues/75) for the full rename plan.

## Linting

Always use **clang-format version 18** for C code formatting.
Run `clang-format --version` and verify it starts with `18.` before formatting.

```bash
clang-format --version   # must start with "clang-format version 18."
clang-format --style=file -i <files>
```
