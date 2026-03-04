# Open Questions

## ir-implementation - 2026-02-22

- [ ] Should `wirelog_program_t` retain the raw AST after IR conversion, or free it immediately? -- Retaining it aids debugging/error reporting but doubles memory usage during the program lifetime.
- [ ] The `wirelog_optimize` signature differs between `wirelog.h` (`wirelog_program_t*, wirelog_error_t*`) and `wirelog-optimizer.h` (`void*, int*`). Which is canonical? -- Must be resolved before optimizer phase, but does not block IR implementation.
- [ ] How should wildcard variables in head arguments be handled during PROJECT? -- e.g., `r(_) :- a(x).` is unusual but syntactically valid. Current plan treats `_` as anonymous and excludes from projections.
- [ ] Should multiple rules for the same head always be UNIONed, or should recursive rules be marked differently for fixpoint computation? -- Stratification phase will need to distinguish recursive vs non-recursive, but for IR-only scope we UNION all same-head rules uniformly.
- [ ] What is the correct IR representation for a rule where the head has fewer variables than the body? -- e.g., `count_nodes(count(x)) :- node(x).` has zero group-by columns. Plan treats this as a full aggregation (no grouping), producing a single-row result.
- [ ] The `wirelog_program_get_stratum_count` and `wirelog_program_is_stratified` functions are declared in the public API but stratification is deferred. -- Plan returns 1 stratum and `true` as defaults. These will be properly implemented in the stratification phase.
- [ ] Should constant-only atoms be treated as facts or scans? -- e.g., `r(1, 2).` (a fact) vs `r(x) :- a(1).` (scan with constant filter). Current plan treats all body atoms as SCANs; fact insertion is an I/O concern.

## jemalloc-evaluation - 2026-02-23

- [ ] jemalloc의 ARM/RISC-V 크로스 컴파일 시 실제 바이너리 크기 증가량은? -- 임베디드 500KB-2MB 목표 달성 가능 여부 판단에 필수
- [ ] wirelog의 향후 멀티스레드 사용 범위는? (현재 "Optional pthreads", 기본 단일 스레드) -- jemalloc/mimalloc의 thread-local cache 이점이 실현되려면 멀티스레드 워크로드가 전제
- [ ] Arena allocator 구현 시 shared pointer 패턴 (schema -> relation 공유)을 어떻게 처리할지? -- 현재 `wl_program_build_schemas()`에서 relation의 name/columns를 shared pointer로 참조
- [ ] Allocator 인터페이스 도입 시점을 Phase 0에 포함할지, Phase 1 이후로 미룰지? -- 94개 호출 지점 수정은 코드베이스가 커지기 전에 하는 것이 비용이 적으나, Phase 0의 현재 우선순위(IR/DD 번역기)와 충돌 가능
- [ ] DD(Differential Dataflow) Rust FFI 경계에서의 메모리 소유권 전략이 allocator 선택에 영향을 주는가? -- ARCHITECTURE.md의 "FFI boundary (memory ownership)" TODO 항목과 연관
- [ ] 엔터프라이즈 타겟에서 DD가 Rust 측에서 자체 메모리 관리를 하므로, C11 측 allocator 최적화의 실질적 영향 범위는 파서/옵티마이저에 한정되는가? -- 실행(execution)은 DD가 담당하므로 C11 allocator의 영향 범위가 제한적일 수 있음

## lint-rules-setup - 2026-02-23

- [ ] Which LLVM major version to pin? Recommendation is clang-18 (widely available on Ubuntu latest), but confirm this matches local developer environments. -- Affects reproducibility of format and tidy results across CI and local dev.
- [ ] Should test files (`tests/*.c`) be included in clang-tidy scope in Phase 1? Tests contain long macros and custom assert helpers that may generate noise. -- Affects tidy finding count and Phase 2 transition timeline.
- [ ] Is Linux-only lint acceptable? Recommendation is yes (clang-tidy is clang-based; MSVC correctness is covered by build matrix). -- Clarifies whether lint workflow needs Windows/macOS runners.
- [ ] What is the deadline for Phase 2 (blocking) transition? Recommendation is 4 weeks after Phase 1 merge. -- Prevents indefinite report-only phase.
- [ ] What suppression policy for `NOLINT` annotations? Recommendation: require reason comment, periodic review. -- Prevents suppressions from becoming permanent technical debt.
- [ ] Should `SortIncludes` be enabled or remain disabled? Current include order (own header first, system alphabetical) is consistent but manual. Enabling risks reordering across the codebase. -- Affects format diff size and include-order enforcement.
- [ ] ColumnLimit: 80 vs 100? Recommendation is 100 to avoid mass reflow (~68 lines currently exceed 80). Confirm maintainer preference. -- Affects baseline formatting PR diff size.
- [ ] Will branch protection require lint status checks in Phase 2? -- Determines whether lint can actually block merges.
- [ ] Should conditional compilation paths (embedded=true, threads=disabled) get a separate clang-tidy run with their own compile_commands.json? -- Currently only default config is analyzed; embedded-specific code paths may have uncaught issues.
- [ ] Should a `.clang-format-ignore` file be created to exclude generated headers, or is CI scope restriction (find command) sufficient? -- Affects whether developers running format locally also get correct exclusions.

## stratification-scc - 2026-02-24

- [ ] Should EDB-only relations (declared with `.input` but never appearing as a rule head) be assigned to a virtual stratum -1 or simply excluded from strata? -- Affects whether `wirelog_program_get_stratum()` returns info about EDB relations. Current plan: exclude them (only rule-defined relations get strata).
- [ ] Should `wl_program_stratify()` be called automatically inside `wirelog_parse_string()` or left as an explicit step the caller invokes? -- Affects API ergonomics vs separation of concerns. Current plan: call automatically (replace `wl_program_build_default_stratum()` call in `api.c`).
- [ ] For the `wirelog_stratum_t.rule_names[]` field: should it store per-rule head names (with duplicates for multi-rule relations like TC) or unique relation names per stratum? -- Current `wl_program_build_default_stratum()` stores per-rule heads (duplicates possible). Plan preserves this convention.
- [ ] Should an unstratifiable program cause `wirelog_parse_string()` to return NULL (parse error) or return a valid program with `is_stratified = false`? -- Affects error handling strategy. Current plan: return valid program with `is_stratified = false`, letting callers check `wirelog_program_is_stratified()`.
- [ ] Dependency extraction from IR trees vs AST: the plan walks IR trees (SCAN/ANTIJOIN nodes) to build the dependency graph. An alternative is to walk the AST directly (WL_NODE_ATOM / WL_NODE_NEGATION). IR walking is preferred since it operates on the canonical representation, but AST walking would be simpler. -- Current plan: IR tree walking (consistent with IR-layer placement of stratify.c).

## dd-plan-translator - 2026-02-24

- [ ] FLATMAP decomposition fidelity: the IR FLATMAP node is a fused join+map+filter. Decomposing it back to 3 separate DD ops may lose the fusion optimization benefit. Should the DD plan preserve a fused FLATMAP op type for the future Rust executor to handle as a single closure? -- Affects whether the executor can take advantage of DD's `flat_map` operator directly.
- [ ] Borrowed vs. owned filter expressions: the plan currently borrows `wl_ir_expr_t*` pointers from the IR tree (not deep-copied). This means the plan's lifetime is tied to the program's lifetime. Is this acceptable, or should the plan be fully self-contained for serialization? -- Affects future FFI serialization design and memory ownership model.
- [ ] DD `Variable` semantics for mutual recursion: when a stratum has multiple mutually recursive relations (e.g., a and b in the same SCC), DD requires all of them to be `Variable`s inside a single `iterate` scope. The current plan marks the stratum as recursive and lists all relations, but does not explicitly model the DD `iterate` scope's variable bindings. -- Affects correctness of mutual recursion execution.
- [ ] Stratum plan representation: the current design uses parallel arrays (`ops[i]` / `op_counts[i]` per relation). An alternative is a flat op sequence with relation markers. Which is better for the future Rust FFI consumer? -- Affects serialization format and executor implementation complexity.
- [ ] CONSOLIDATE placement: currently CONSOLIDATE is emitted after every CONCAT (union). DD may not require explicit consolidation in all cases (e.g., within an iterate loop, DD handles this automatically). Should CONSOLIDATE be conditional? -- Affects plan accuracy and executor performance.

## rust-dd-executor - 2026-02-26

- [ ] **Worker pool model: per-call vs persistent Timely computation** -- Per-call (Option A) is simpler but re-creates threads on each `wl_dd_execute`. If execution frequency is high, persistent workers (Option B with channels) will be needed. Decision affects worker.rs design and integration test expectations.

- [ ] **EDB data loading API shape** -- The current FFI declares `wl_dd_execute` but has no way to feed input data (EDB facts) to the Rust executor. The plan proposes a minimal `wl_dd_load_edb()` with `int64_t*` rows, but real programs need typed columns (int32, string, bool). Should the initial API support only int64, or include the full `wirelog_column_type_t` type set from the start?

- [ ] **Result collection API** -- `wl_dd_execute` returns `int` (success/error) but the C side has no way to read computed IDB results back. A result-reading API (`wl_dd_get_results()` or callback-based) is needed for any real use. Should this be part of this plan or a follow-up?

- [ ] **DD tuple representation** -- Using `Vec<Value>` as the DD collection element type is flexible but has heap allocation overhead. An alternative is fixed-width tuples or a columnar representation. This affects performance but not correctness. Decision can be deferred to optimization phase.

- [ ] **`bool` ABI across C11 and Rust** -- `wl_ffi_stratum_plan_t.is_recursive` is C `bool`. On all target platforms (macOS ARM64, Linux x86-64), `_Bool` and Rust `bool` are both 1 byte. A `_Static_assert(sizeof(bool) == 1, ...)` in a C test would guard against surprises. Should this be added to existing FFI tests?

- [ ] **Cross-compilation for embedded ARM targets** -- Cargo cross-compilation to ARM/RISC-V requires a Rust target triple and possibly a cross-linker. Meson's cross-file mechanism must coordinate with Cargo's target. Not blocking for initial x86-64/ARM64 macOS development but needed before Phase 2 embedded validation.

- [ ] **Differential Dataflow version pinning** -- The plan uses DD 0.12 from crates.io. The homebrew package `differential-dogs3` is v0.19.1 but provides .rlib files. Need to confirm DD 0.12 is available on crates.io and compatible with Timely 0.12. If not, version numbers need adjustment.

## subplan-sharing-tdd (#61) - 2026-03-01

- [ ] Per-stratum vs global scope -- Current plan operates globally since UNION children are inherently within one relation (and therefore one stratum). If future strata logic changes, this assumption should be revisited.
- [ ] Partial prefix sharing (SCAN+FILTER CSE when remainder differs) -- Deferred to future work. The current plan only eliminates fully duplicate UNION children. Partial prefix extraction requires either (a) creating intermediate relations at IR level or (b) DD-plan-level CSE, both of which are more invasive.
- [ ] FLATMAP-aware comparison -- If the pass ever runs after fusion, `wl_ir_node_subtree_equal` needs to compare FLATMAP nodes (filter_expr + project_indices + child). Current plan avoids this by running sharing before fusion.
- [ ] Should `wl_ir_expr_equal` and `wl_ir_node_subtree_equal` be promoted to `ir.h` for reuse by other passes? -- Currently scoped to `subplan_sharing.h` to minimize blast radius. Can be refactored later.

## issue-20-arith-projection-tdd - 2026-02-28

- [ ] Should `project_indices[i]` be set to a meaningful value (e.g., 0) or a sentinel (e.g., UINT32_MAX) for columns that use expression evaluation instead of index lookup? -- Affects whether the Rust side can distinguish "use index" from "use expression" without checking the exprs array. Using UINT32_MAX as sentinel is cleaner but adds a magic constant.
- [ ] Should `SafeOp::Map` use `Option<Vec<Option<Vec<ExprOp>>>>` for the exprs field, or a per-column enum like `MapColumn::Index(u32) | MapColumn::Expr(Vec<ExprOp>)`? -- The enum is more type-safe but changes the Map variant signature more significantly. The Option-of-Options approach is less invasive.
- [ ] For the Rust `eval_expr_int()` function: should it be a separate function or should `eval_filter()` be generalized to `eval_expr()` returning `Value` with callers checking the type? -- Affects API surface in expr.rs. A unified evaluator is cleaner long-term but changes the existing API.
- [ ] What happens when an arithmetic expression in a head position references a variable not bound in the body? e.g., `b(x, z + 1) :- a(x, y).` where z is unbound. -- The parser/IR should reject this, but need to verify. If it reaches dd_plan.c, the `rewrite_expr_vars()` call will leave the variable name unrewritten, causing a runtime error in Rust.

## delta-query-support - 2026-03-03

- [ ] Should `wl_session_step()` block until the dataflow quiesces, or should it support async stepping with a progress callback? -- Affects API design for long-running computations on large graphs.
- [ ] What is the maximum session lifetime expectation? Should sessions support checkpointing/serialization for crash recovery? -- Determines whether we need persistence beyond in-memory state.
- [ ] Should the delta callback receive the DD timestamp (epoch) along with the diff? -- Useful for debugging and ordering but adds FFI complexity.
- [ ] For Phase 2 nanoarrow: should the semi-naive evaluator support multi-threaded evaluation within a single session, or is single-threaded sufficient for embedded targets? -- Affects evaluator architecture significantly.
- [ ] Should `wl_session_update_edb()` validate that the relation name and column count match the original plan, or silently ignore mismatches? -- Strict validation is safer but adds overhead per update call.
- [ ] Is there a need for a `wl_session_query()` API that returns the current full materialized state (not just deltas) of a relation? -- Common use case: "give me everything" after a series of incremental updates.
- [ ] Should the `wl_backend_t` vtable be extensible (reserved function pointer slots) or versioned (version field + runtime capability check)? -- Affects ABI stability for third-party backends.

## phase2a-dd-vs-columnar-consensus - 2026-03-04

- [ ] What is the concrete customer/user demand timeline for multi-worker sessions? -- If no near-term demand, multi-worker can be deprioritized within Option A, reducing Rust complexity.
- [ ] Should non-monotone aggregation (COUNT/SUM/AVG) in recursive strata use DD lattice semantics or stratified-negation-style re-stratification? -- Lattice approach is DD-native but complex; re-stratification moves aggregation to a non-recursive stratum but may not always be possible.
- [ ] What is the acceptable Rust LOC growth budget for Phase 2A? -- Current: 5045 LOC. If a ceiling (e.g., 6000 LOC) is exceeded, should scope be trimmed or should the decision be revisited?
- [ ] For multi-worker sessions: `timely::Configuration::Process(N)` vs `Configuration::Thread`? -- Process uses OS threads with full communication channels; Thread uses lighter cooperative scheduling. Affects performance and debugging.
- [ ] Should incremental GC be explicit (`session_compact()` API) or automatic (triggered after N retractions)? -- Explicit gives the C caller control; automatic is simpler but may compact at inopportune times.
- [ ] Is there an existing Datalog engine (Souffle, Crepe, Ascent) that can serve as a reference oracle for non-monotone aggregation in recursive programs? -- Critical for validating correctness of whichever option is chosen.

## phase2a-rust-removal-columnar - 2026-03-04

- [ ] Are 2 C engineers available for 30 days of dedicated parallel work? -- The accelerated timeline depends on this. Single-engineer fallback is ~40 days.
- [ ] Should `wl_relation_t` use row-major (array of tuples) or column-major (array of columns) storage initially? -- Row-major is simpler to implement; column-major enables future nanoarrow/SIMD optimization. Recommendation: row-major first, refactor to columnar in Phase 2B.
- [ ] What is the acceptable performance gap vs DD at Phase 2A ship? -- If 5x slowdown on DOOP is acceptable for correctness, the timeline holds. If parity is required, add 5-10 days for join optimization.
- [ ] Should the plan types be renamed when Rust is removed (wl_dd_plan_t -> wl_exec_plan_t)? -- Low priority but affects API clarity. Recommendation: rename in a separate follow-up commit.
- [ ] How should `wl_ffi_plan_t` (the marshaled plan for Rust FFI) be handled? -- The columnar backend can read `wl_dd_plan_t` directly without marshaling. The FFI marshal layer (`dd_marshal.c`) becomes unnecessary. Should we skip marshaling entirely and have the evaluator read the DD plan struct directly?
- [ ] Non-monotone aggregation in recursive strata: implement via lattice semantics or defer entirely to Phase 2B? -- Lattice-based COUNT/SUM in recursive strata is a research-grade problem. Recommendation: defer, support only monotone (MIN/MAX) in recursive strata for Phase 2A.
- [ ] What is the CI impact of removing Rust? -- Current CI likely has Rust toolchain installation steps. Need to audit CI configuration before Rust removal to avoid breaking the pipeline.
- [ ] Should the DD backend code be preserved on a branch for reference/rollback? -- Recommendation: yes, tag the last DD commit before deletion for easy recovery if needed.
