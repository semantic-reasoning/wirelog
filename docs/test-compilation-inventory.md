# Test Compilation Inventory — Issue #1572

This document is the target/configuration inventory required by
[issue #1572](https://github.com/semantic-reasoning/wirelog/issues/1572).
It records which test executables in `tests/meson.build` now reuse the
shared production static library `testlib_prod`, why each reuse group is
build-configuration equivalent (and therefore safe), and why every
retained target keeps its independent compilation.

## 1. What changed

One internal static library was added to `tests/meson.build`,
immediately after `thread_src = wirelog_thread_src`:

```meson
testlib_prod = static_library(
  'testlib_prod',
  files( <80 production .c files> ),
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)
```

- 81 archive members = 80 distinct production translation units (no
  test `.c`, no duplicates) + the platform thread source
  (`thread_posix.c` / `thread_c11.c` / `thread_msvc.c` per platform,
  via the same `wirelog_thread_src` the targets used before).
- Compiled once per build directory with the project defaults
  (`buildtype=release`, `optimization=s`, `b_lto=true`, no sanitizers)
  and **without** `-DWIRELOG_BUILDING` / `config.h` — exactly the
  translation set and flags the 155 converted targets previously used
  to compile these TUs themselves. On Windows, `threads_dep` is wrapped
  with `compile_args: ['-DWIRELOG_STATIC']` so public API declarations in
  wirelog headers expand with plain static linkage rather than
  `__declspec(dllimport)`, avoiding unresolved `__imp_*` symbols during
  static archive extraction.
- 155 converted targets: their `*_src` bundle arguments were removed and
  `link_with: [testlib_prod]` added. Their `files()`,
  `include_directories`, and `dependencies` are byte-identical to the
  pre-change form, and they carry no per-target `c_args` / `c_link_args`
  / `link_args` / `scc_args` / `override_options` / `wrap=` / `env:`.
- 105 targets retained (table in §3), each with an individual
  justification.
- No `test()` registration changed: 377 before and after, and the
  normalized `test()` bodies are identical multisets.
- No target added or removed: 260 `executable()` targets before and
  after (the file's 261st `executable(` occurrence is inside a comment).
- No production `.c`, no public/installed header, no test `.c`, and no
  install rule is touched. The change is `tests/meson.build` plus this
  document.

## 2. Why the reuse is safe

1. **Equivalence by construction.** A target is converted only if its
   pre-change production translation set is a subset of `testlib_prod`'s
   members, its `dependencies:` list is exactly the full-5 set the
   library is built with, its `include_directories:` are
   `[wirelog_inc, wirelog_src_inc]`, and it has no per-target flags.
   Every object the library contains is therefore bit-for-bit the
   object that target used to build for itself.
2. **Static-archive lazy extraction.** `testlib_prod` links as a plain
   archive (no `--whole-archive`). A member is pulled in only to satisfy
   an undefined symbol. Each converted target references the same
   symbols as before, resolved by the same objects as before; unreferenced
   members are never extracted.
3. **No duplicate symbols.** Each of the 81 members appears exactly once
   (no two members define the same global symbol — verified with `nm` on
   baseline objects: 0 cross-TU global duplicates), and no member defines
   `int main` or uses `__attribute__((constructor))` / other
   self-registration (grep-verified), so extraction order cannot change
   behavior. This single-library shape is load-bearing: the legacy
   `*_src` bundles overlapped — `columnar/memory_governor.c` was a member
   of 5 bundles, `util/log.c` + `util/log_emit.c` of 2, and
   `extension_registry.c`, `intern.c`, `csv_reader.c`, `string_ops.c`,
   `crc32.c`, `util/lockfree_queue.c` of 2 — so per-bundle archives would
   have contained identical members and made dedup linker-order dependent.
   `testlib_prod` contains each such TU exactly once, which removes the
   duplicate by construction (and keeps the LTO bitcode job free of
   duplicate modules). No archive member may be re-listed directly in a
   converted target, and none is: a target that compiles bundles directly
   is retained and does not link the archive at all.  The TU union of the
   converted targets still equals the library set -- #1720 moved
   test_join_right_filter_cleanup back out of the converted set, but every
   bundle it used (parser/ir/optimizer/backend/arena/io/thread/workqueue)
   is still contributed by other converted targets, so the union is
   unchanged.
4. **Single-archive link resolution.** The converted targets link exactly
   one shared archive in addition to their own objects, so the linker's
   iterative archive rescan resolves every intra-library dependency
   regardless of member order; no `--start-group`/`--end-group` is
   needed and none is emitted. (Cross-archive single-pass ordering would
   be a real hazard — it is eliminated by design here.)
5. **LTO.** With `b_lto=true` the archive holds each module exactly once,
   so an LTO link job never sees a duplicated module.
6. **Sanitizer isolation.** The library inherits each build directory's
   `-Db_sanitize`, so sanitizer and normal builds each compile their own
   copy in their own build directory. No artifact is shared across
   sanitizer boundaries.
7. **Cross-platform / cross-compiler.** The design uses the existing
   `wirelog_thread_src` selection and no host conditionals; CI exercises
   GCC/Clang on Linux/macOS/Windows.

## 3. Retained targets (105) and why

| target | reason for independent compilation |
|---|---|
| compound_arena_fuzz | dep subset `(none)` |
| csv_reader_fuzz | dep subset `threads_dep` |
| intern_fuzz | dep subset `threads_dep` |
| parser_fuzz | dep subset `(none)` |
| test_arena_observability | dep subset `(none)` |
| test_bench_argv | dep subset `(none)` |
| test_bench_compound_smoke | dep subset `(none)` |
| test_bench_intern_smoke | dep subset `(none)` |
| test_bitwise_parser | dep subset `nanoarrow_dep, threads_dep` |
| test_bitwise_types | dep subset `nanoarrow_dep, threads_dep` |
| test_column_names_oom | `b_lto=false`; dep subset `threads_dep` |
| test_compound_arena_freeze_cycle_stress | dep subset `math_dep, threads_dep` |
| test_compound_arena_trace | dep subset `math_dep, threads_dep` |
| test_consolidate_incremental_delta | test hook `WL_TEST_CONSOLIDATE_HOOK` is consumed by production `columnar/merge.c` and `columnar/relation.c`; this target must compile its own columnar TUs with the define (hook added on main) |
| test_consolidate_kway_merge | test hook `WL_TEST_CONSOLIDATE_ALLOC_HOOK` is consumed by production `columnar/merge.c`, `columnar/relation.c`, and `columnar/internal.h`; this target must compile its own columnar TUs with the define (hook added on main) |
| test_crc32_castagnoli | dep subset `(none)` |
| test_crc32_ethernet | dep subset `(none)` |
| test_crc32_eval | perf-gate source pair (`optimization=3` sibling) |
| test_crc32_hw_equivalence | dep subset `(none)` |
| test_crc32_parser | dep subset `nanoarrow_dep, threads_dep` |
| test_crc32_windows_fallback | dep subset `(none)` |
| test_cse_cache_hit_rate | dep subset `(none)` |
| test_cspa_correctness | `optimization=3` |
| test_cspa_perf_gate | perf-gate source pair (`optimization=3` sibling) |
| test_csv | dep subset `threads_dep` |
| test_csv_streaming | dep subset `threads_dep` |
| test_diff_arrangement_deep_copy_buckets | dep subset `threads_dep` |
| test_diff_trace | dep subset `(none)` |
| test_extension_registry | defines `WIRELOG_BUILDING`; `WIRELOG_BUILDING` plugin build; dep subset `threads_dep` |
| test_frontier | dep subset `(none)` |
| test_frontier_filtering | dep subset `(none)` |
| test_frontier_integration | dep subset `(none)` |
| test_frontier_skip | dep subset `(none)` |
| test_frontier_skip_integration | dep subset `(none)` |
| test_fusion | dep subset `threads_dep` |
| test_gc_freeze_alloc_race | dep subset `math_dep, threads_dep` |
| test_hash_parser | dep subset `nanoarrow_dep, threads_dep` |
| test_hash_types | dep subset `nanoarrow_dep, threads_dep` |
| test_intern | dep subset `threads_dep` |
| test_io_adapter | defines `WIRELOG_BUILDING`; `WIRELOG_BUILDING` plugin build; dep subset `threads_dep` |
| test_io_adapter_asan | defines `WIRELOG_BUILDING`; `WIRELOG_BUILDING` plugin build; dep subset `threads_dep` |
| test_io_adapter_concurrent | defines `WIRELOG_BUILDING`; `WIRELOG_BUILDING` plugin build; dep subset `threads_dep` |
| test_io_ctx | defines `WIRELOG_BUILDING`; `WIRELOG_BUILDING` plugin build; dep subset `threads_dep` |
| test_io_dispatch | defines `TEST_DISPATCH_PRESENT, ` WIRELOG_BUILDING`; `WIRELOG_BUILDING` plugin build; dep subset `threads_dep` |
| test_ir | `b_lto=false`; dep subset `threads_dep` |
| test_join_hash_probe | dep subset `(none)` |
| test_join_right_filter_cleanup | `b_lto=false` + `-Wl,--wrap=calloc` on Linux (#1720 allocation-failure coverage) |
| test_jpp | `b_lto=false`; dep subset `threads_dep` |
| test_k_fusion_adaptive | dep subset `(none)` |
| test_k_fusion_dispatch | dep subset `(none)` |
| test_k_fusion_memory_nofusion | defines `ENABLE_K_FUSION=0` |
| test_k_fusion_merge | dep subset `(none)` |
| test_lexer | dep subset `(none)` |
| test_lockfree_queue | dep subset `threads_dep` |
| test_log_gating | dep subset `(none)` |
| test_log_integration | dep subset `(none)` |
| test_log_legacy_shim | dep subset `(none)` |
| test_log_parse | dep subset `(none)` |
| test_log_perf_gate | dep subset `math_dep` |
| test_log_threshold_race | dep subset `threads_dep` |
| test_mem_instrumentation_ | perf-gate source pair (`optimization=3` sibling) |
| test_mem_ledger | dep subset `threads_dep` |
| test_memory_admission_arena | dep subset `threads_dep` |
| test_memory_admission_compound | dep subset `threads_dep` |
| test_memory_admission_delta | dep subset `threads_dep` |
| test_memory_admission_intern | dep subset `threads_dep` |
| test_memory_governor | defines `WL_COLUMNAR_MEMORY_TEST_HOOKS=1`; dep subset `threads_dep` |
| test_option2_cse | defines `ENABLE_K_FUSION=1` |
| test_parse_duplicate_decl_no_leak | dep subset `threads_dep` |
| test_parser | dep subset `(none)` |
| test_parser_arithmetic_oom | `b_lto=false`; dep subset `(none)` |
| test_phase4_frontier_array | dep subset `(none)` |
| test_plan_gen | defines `WIRELOG_TEST_DELTA_NAME, ` WIRELOG_TEST_EXTENSION_SERIALIZATION` |
| test_plan_gen_oom | `b_lto=false` |
| test_plugin_loader | defines `WIRELOG_BUILDING, ` WL_HAVE_PLUGIN_LOADER`; `WIRELOG_BUILDING` plugin build; dynamic-load plugin; dep subset `threads_dep` |
| test_program | dep subset `threads_dep` |
| test_program_oom | `b_lto=false`; dep subset `threads_dep` |
| test_r9_self_join_delta | dep subset `(none)` |
| test_recursive_agg_contract_nofusion | defines `ENABLE_K_FUSION=0` |
| test_recursive_scc_ | per-`scc_args` conditional defines |
| test_relation_generations | defines `WL_TEST_APPEND_HOOK=1, ` WL_TEST_SET_HOOK=1` |
| test_relation_generations_oom | defines `WL_TEST_ALLOC_WRAP=1, ` WL_TEST_APPEND_HOOK=1, ` WL_TEST_SET_HOOK=1`; `b_lto=false` |
| test_session | defines `WL_SESSION_TEST_HOOKS=1` |
| test_session_admission | defines `WIRELOG_BUILDING`; `WIRELOG_BUILDING` plugin build; dep subset `threads_dep` |
| test_session_extension_snapshot | defines `WIRELOG_BUILDING`; `WIRELOG_BUILDING` plugin build; dep subset `threads_dep` |
| test_session_options | defines `WIRELOG_BUILDING`; `WIRELOG_BUILDING` plugin build; dep subset `threads_dep` |
| test_sip | dep subset `threads_dep` |
| test_source_access | dep subset `threads_dep` |
| test_standalone_ | dep subset `(none)` |
| test_standalone_wirelog_deprecated_macro | dep subset `(none)` |
| test_stratify | `b_lto=false`; dep subset `threads_dep` |
| test_stratify_scc_bounds | dep subset `threads_dep` |
| test_string_ops | dep subset `threads_dep` |
| test_string_parser | dep subset `threads_dep` |
| test_tdd_decision_stats | defines `WL_COLUMNAR_EVAL_TEST_SUBMISSION` |
| test_tdd_decision_stats_nofusion | defines `ENABLE_K_FUSION=0, ` WL_COLUMNAR_EVAL_TEST_SUBMISSION` |
| test_tdd_multi_batch_protocol | dep subset `threads_dep` |
| test_thread_mach_after | dep subset `threads_dep` |
| test_thread_mach_before | dep subset `threads_dep` |
| test_wirelog_advanced | defines `WL_SESSION_TEST_HOOKS=1` |
| test_wirelog_easy | defines `WL_SESSION_TEST_HOOKS=1` |
| test_wirelog_public_api | dep subset `(none)` |
| test_wirelog_result_oom | defines `WIRELOG_BUILDING`; `b_lto=false`; `zlib_dep`; `WIRELOG_BUILDING` plugin build; dep subset `math_dep, mbedtls_dep, nanoarrow_dep, threads_dep, xxhash_dep, zlib_dep` |
| test_workqueue_capacity | dep subset `threads_dep` |
| test_workqueue_drain | dep subset `threads_dep` |

Category counts: dependency-subset targets 58; per-target flag targets
44 (allocator `--wrap` fault injection, `ENABLE_K_FUSION` A/B pairs,
`WL_SESSION_TEST_HOOKS` / test hooks, crc32 architecture variants,
`b_lto=false`, `optimization=3` perf-gate sibling, `zlib_dep`,
`WIRELOG_BUILDING` plugin builds, dynamic-load plugin); cspa perf-gate
source pair 1.

## 4. Measurements (this machine)

Environment: 16-core x86_64, GCC 16.2.1, Meson 1.12.0, ninja 1.13.2,
`buildtype=release`, `optimization=s`, `b_lto=true`, `b_sanitize=none`,
no ccache. Both sides: fresh never-built build directories, default
options, same checkout (before: clean `c5b7017`; after: this change),
`ninja -j16`.

| phase | before | after | Δ steps | Δ wall |
|---|---|---|---|---|
| cold (full) build | 15,154 steps, 404 s | 3,901 steps, 236 s | **−74.3%** | **−41.6%** |
| warm: touch `wirelog/parser/ast.c`, rebuild | 333 steps, 179 s | 263 steps, 184 s | −21.0% | +2.8% (noise) |

Cold builds — the case the issue targets (fresh checkout/PR builds of
~15k steps) — drop 74% of ninja steps and 42% of wall time. In the warm
single-TU case the 155 converted targets pay only relink cost; the
removed work was compiler-parallel, so on a 16-core box the remaining
LTO relinks dominate both sides and wall time is flat.

## 5. Test evidence

- **Default options** (after): 362 Ok / 3 Fail / 18 Skip. The 3 failures
  (`log_abi_compile_erasure`, `log_abi_header_not_public`,
  `downstream_matrix_contract`) are pre-existing/environmental: they
  fail on the unmodified baseline in a `/tmp` checkout whose script tests
  need a git context, and all 3 pass in the worktree build
  (`build/meson-logs/testlog.txt`: Ok 3 / Fail 0).
- **Baseline** (before): 361 Ok / 4 Fail — the extra failure
  (`doop_validation`) is a flaky SIGTERM timeout that passes in the
  after build.
- **Sanitizer** (`-Db_sanitize=address,undefined`, separate build dir,
  after): 360 Ok / 3 Fail — the same 3 environmental failures, no new
  sanitizer findings from the shared archive.
- **ABI surface**: no installed header changed; the
  `check-public-header-surface.py` gate is unaffected.

## 6. Converted targets (155)

bench_incremental_frontier, incremental_insertion, test_2d_frontier_init, test_2d_frontier_skip
test_affected_rules, test_affected_strata, test_affected_strata_rewrites, test_arithmetic_overflow
test_arr_hash_rows_batch, test_arrangement, test_arrangement_cache_reuse, test_arrangement_incremental_invalidation
test_arrangement_lru_eviction, test_arrangement_pow2_overflow, test_arrangement_probe, test_arrangement_source_lifetime_tsan
test_average_rejected, test_base_skip_null_template, test_bitwise_eval, test_bitwise_integration
test_cache_reclaimer, test_cli, test_cli_delta, test_cli_physical_columns
test_cli_watch, test_col_rel_compound_schema, test_col_rel_deep_copy, test_col_rel_inline_storage
test_columnar_inline, test_columnar_teardown, test_compaction, test_compound_logging
test_compound_side_relation, test_consolidation_cow
test_constant_filter_pushdown, test_crdt_perf_gate, test_cryptographic_hashes, test_csv_limits
test_csv_wide_columns, test_deep_copy_ledger_canary, test_delta_arrangement, test_delta_callback
test_delta_factoring_e2e, test_delta_propagation, test_delta_retraction, test_delta_timestamp
test_diff_arrangement, test_diff_bridge, test_diff_consolidate, test_diff_integration
test_diff_join, test_diff_multiworker, test_diff_recursive_arrangement, test_diff_vtable
test_doop_multiworker, test_doop_strings, test_e2e_asan_side_relation_nested, test_nonrecursive_multiworker_authorization
test_inline_concurrent_read, test_empty_delta_skip, test_eval_continuation_publication, test_expr_compile
test_extension_filter, test_filt_cache, test_filter_select_rows, test_fpga_backend
test_frontier_progress, test_frontier_skip_mobius, test_frontier_vtable, test_gc_epoch_boundary
test_gc_freeze_guard, test_generation_cache, test_handle_remap_apply, test_handle_remap_side_apply
test_hash_eval, test_hash_integration, test_host_insert_width, test_inline_compound_wiring
test_input_arity, test_intern_parallel_determinism, test_issue914_nonrec_after_recursive, test_join_arrangement
test_join_batch_resume, test_join_limit, test_join_overflow
test_k_fusion_correctness, test_k_fusion_e2e, test_diff_arrangement_inline_shadow, test_k_fusion_memory
test_lftj, test_lftj_integration, test_magic_sets, test_malloc_optimization
test_mat_cache_lifetime, test_max_workers, test_memory_admission_relation, test_mixed_insert_remove_mask
test_mobius_count_signed, test_mobius_delta_formula, test_mobius_join_weighted, test_monotone_detection
test_multi_stratum_rule_frontier, test_multi_worker, test_optimizer_equivalence, test_option2_doop
test_partition, test_phase3b_integration, test_phase3c_full_integration, test_phase3c_join_integration
test_phase3c_reduce_integration, test_phase3d_frontier_skip_integration, test_phase4_frontier_integration, test_plan_exchange
test_pointer_swap, test_post_remap_invalidate, test_queue_transport, test_radix_sort
test_rdf_named_graph, test_recursive_agg_conformance, test_recursive_agg_contract, test_relation_append
test_rotate_latency, test_rotation_strategy, test_rss_bounded, test_rule_level_frontier
test_safe_worker_scaling, test_selective_frontier_reset, test_session_compound_arena_lifecycle, test_side_compound_delta
test_simd_filter, test_simd_join, test_simd_neon, test_simd_row_cmp
test_stress_harness, test_string_eval, test_sub_ms_graph_perf_gate, test_symbol_aggregates
test_symbol_digests, test_symbol_ordering, test_td_exchange, test_td_multiworker
test_tdd_convergence, test_tdd_exchange, test_tdd_integration, test_tdd_multiworker
test_tdd_nonrecursive, test_tdd_recursive, test_tdd_single_key_owner, test_wide_relation
test_wirelog_easy_inline_facts, test_worker_arena_borrow, test_worker_borrow_w2_tsan, test_worker_session
test_workers_as_cap, test_workqueue


## 7. Converted targets (155)

- bench_incremental_frontier, incremental_insertion, test_2d_frontier_init, test_2d_frontier_skip, test_affected_rules, test_affected_strata
- test_affected_strata_rewrites, test_arithmetic_overflow, test_arr_hash_rows_batch, test_arrangement, test_arrangement_cache_reuse, test_arrangement_incremental_invalidation
- test_arrangement_lru_eviction, test_arrangement_pow2_overflow, test_arrangement_probe, test_arrangement_source_lifetime_tsan, test_average_rejected, test_base_skip_null_template
- test_bitwise_eval, test_bitwise_integration, test_cache_reclaimer, test_cli, test_cli_delta, test_cli_physical_columns
- test_cli_watch, test_col_rel_compound_schema, test_col_rel_deep_copy, test_col_rel_inline_storage, test_columnar_inline, test_columnar_teardown
- test_compaction, test_compound_logging, test_compound_side_relation, test_consolidation_cow
- test_constant_filter_pushdown, test_crdt_perf_gate, test_cryptographic_hashes, test_csv_limits, test_csv_wide_columns, test_deep_copy_ledger_canary
- test_delta_arrangement, test_delta_callback, test_delta_factoring_e2e, test_delta_propagation, test_delta_retraction, test_delta_timestamp
- test_diff_arrangement, test_diff_bridge, test_diff_consolidate, test_diff_integration, test_diff_join, test_diff_multiworker
- test_diff_recursive_arrangement, test_diff_vtable, test_doop_multiworker, test_doop_strings, test_e2e_asan_side_relation_nested, test_nonrecursive_multiworker_authorization
- test_inline_concurrent_read, test_empty_delta_skip, test_eval_continuation_publication, test_expr_compile, test_extension_filter, test_filt_cache
- test_filter_select_rows, test_fpga_backend, test_frontier_progress, test_frontier_skip_mobius, test_frontier_vtable, test_gc_epoch_boundary
- test_gc_freeze_guard, test_generation_cache, test_handle_remap_apply, test_handle_remap_side_apply, test_hash_eval, test_hash_integration
- test_host_insert_width, test_inline_compound_wiring, test_input_arity, test_intern_parallel_determinism, test_issue914_nonrec_after_recursive, test_join_arrangement
- test_join_batch_resume, test_join_limit, test_join_overflow, test_k_fusion_correctness, test_k_fusion_e2e
- test_diff_arrangement_inline_shadow, test_k_fusion_memory, test_lftj, test_lftj_integration, test_magic_sets, test_malloc_optimization
- test_mat_cache_lifetime, test_max_workers, test_memory_admission_relation, test_mixed_insert_remove_mask, test_mobius_count_signed, test_mobius_delta_formula
- test_mobius_join_weighted, test_monotone_detection, test_multi_stratum_rule_frontier, test_multi_worker, test_optimizer_equivalence, test_option2_doop
- test_partition, test_phase3b_integration, test_phase3c_full_integration, test_phase3c_join_integration, test_phase3c_reduce_integration, test_phase3d_frontier_skip_integration
- test_phase4_frontier_integration, test_plan_exchange, test_pointer_swap, test_post_remap_invalidate, test_queue_transport, test_radix_sort
- test_rdf_named_graph, test_recursive_agg_conformance, test_recursive_agg_contract, test_relation_append, test_rotate_latency, test_rotation_strategy
- test_rss_bounded, test_rule_level_frontier, test_safe_worker_scaling, test_selective_frontier_reset, test_session_compound_arena_lifecycle, test_side_compound_delta
- test_simd_filter, test_simd_join, test_simd_neon, test_simd_row_cmp, test_stress_harness, test_string_eval
- test_sub_ms_graph_perf_gate, test_symbol_aggregates, test_symbol_digests, test_symbol_ordering, test_td_exchange, test_td_multiworker
- test_tdd_convergence, test_tdd_exchange, test_tdd_integration, test_tdd_multiworker, test_tdd_nonrecursive, test_tdd_recursive
- test_tdd_single_key_owner, test_wide_relation, test_wirelog_easy_inline_facts, test_worker_arena_borrow, test_worker_borrow_w2_tsan, test_worker_session
- test_workers_as_cap, test_workqueue

