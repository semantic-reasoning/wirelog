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
`wirelog/wirelog.h`, `wirelog/wirelog-types.h`, `wirelog/wirelog-ir.h`, `wirelog/wirelog-parser.h`, `wirelog/wirelog-optimizer.h`, `wirelog/wirelog-export.h`, `wirelog/wl_easy.h`, `wirelog/wirelog-advanced.h`, `wirelog/io/io_adapter.h`

> Single source of truth: `wirelog_public_headers` in `meson.build`
> (plus the standalone `install_headers('wirelog/io/io_adapter.h', ...)`
> call). Edit there first; this list is enforced by
> `scripts/ci/check-public-header-surface.py` (`meson test --suite abi`).
> The generated `wirelog/wirelog-version.h` is also installed but is
> tracked separately by `check-version-sync.py`.

**Internal headers** (not installed):
Everything under `wirelog/parser/`, `wirelog/ir/`, `wirelog/backend/`, and `wirelog/backend.h`, `wirelog/session.h`, `wirelog/intern.h`

### Subdirectory Encoding

Internal symbols must encode their subdirectory path in the prefix:

```
wirelog/{subdir}/{file}.h  →  wl_{subdir}_{file}_*
```

Examples:
- `wirelog/parser/ast.h` → `wl_parser_ast_node_t`, `wl_parser_ast_node_create()`
- `wirelog/ir/stratify.h` → `wl_ir_stratify_dep_graph_t`
- `wirelog/exec_plan.h` → `wl_plan_t`, `wl_plan_op_t` (backend-agnostic execution plan types)
- `wirelog/backend.h` → `wl_compute_backend_t` (columnar backend abstraction)

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

See [issue #75](https://github.com/semantic-reasoning/wirelog/issues/75) for the full rename plan.
