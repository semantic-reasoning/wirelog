# tests/meson.build
# Test configuration for wirelog

# ============================================================================
# Parser source files (compiled directly into test executables)
# ============================================================================

parser_src = files(
  '../wirelog/parser/lexer.c',
  '../wirelog/parser/ast.c',
  '../wirelog/parser/parser.c',
)

ir_src = files(
  '../wirelog/ir/ir.c',
  '../wirelog/ir/program.c',
  '../wirelog/ir/stratify.c',
  '../wirelog/ir/api.c',
  '../wirelog/intern.c',
  '../wirelog/session.c',
  '../wirelog/exec_plan_gen.c',
  '../wirelog/session_facts.c',
)

optimizer_src = files(
  '../wirelog/passes/fusion.c',
  '../wirelog/passes/jpp.c',
  '../wirelog/passes/sip.c',
)

intern_src = files(
  '../wirelog/intern.c',
)

csv_src = files(
  '../wirelog/io/csv_reader.c',
)

# ============================================================================
# Test Executables
# ============================================================================

test_lexer_exe = executable(
  'test_lexer',
  files('test_lexer.c'),
  parser_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test_parser_exe = executable(
  'test_parser',
  files('test_parser.c'),
  parser_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test_ir_exe = executable(
  'test_ir',
  files('test_ir.c'),
  parser_src,
  ir_src,
  csv_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test_program_exe = executable(
  'test_program',
  files('test_program.c'),
  parser_src,
  ir_src,
  csv_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test_stratify_exe = executable(
  'test_stratify',
  files('test_stratify.c'),
  parser_src,
  ir_src,
  csv_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

# ============================================================================
# Register Tests
# ============================================================================

test('lexer', test_lexer_exe)
test('parser', test_parser_exe)
test('ir', test_ir_exe)
test('program', test_program_exe)
test('stratify', test_stratify_exe)

test_fusion_exe = executable(
  'test_fusion',
  files('test_fusion.c'),
  parser_src,
  ir_src,
  optimizer_src,
  csv_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('fusion', test_fusion_exe)

test_jpp_exe = executable(
  'test_jpp',
  files('test_jpp.c'),
  parser_src,
  ir_src,
  optimizer_src,
  csv_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('jpp', test_jpp_exe)

test_sip_exe = executable(
  'test_sip',
  files('test_sip.c'),
  parser_src,
  ir_src,
  optimizer_src,
  csv_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('sip', test_sip_exe)

# ============================================================================
# Rust FFI Tests (deferred to Phase 2A columnar backend)
# ============================================================================
#
# The following tests require the Rust Differential Dataflow executor:
# - test_dd_ffi
# - test_dd_execute
# - test_cli
# - test_session
# - test_recursive_agg_baseline
# - test_recursive_agg
# - test_recursive_agg_cc_min
# - test_recursive_agg_sssp_max
#
# These are deferred to Phase 2A when the columnar backend is implemented.

# ============================================================================
# Columnar Backend Session Tests (no Rust required)
# ============================================================================


test_csv_exe = executable(
  'test_csv',
  files('test_csv.c'),
  csv_src,
  intern_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('csv', test_csv_exe)

test_intern_exe = executable(
  'test_intern',
  files('test_intern.c'),
  intern_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('intern', test_intern_exe)

# ============================================================================
# Plan Generator + Session Facts Tests (Phase 2C columnar backend)
# ============================================================================

backend_src = files(
  '../wirelog/backend/columnar_nanoarrow.c',
  '../wirelog/backend/delta_pool.c',
)

io_src = files(
  '../wirelog/io/csv_reader.c',
)

arena_src = files(
  '../wirelog/arena/arena.c',
)

workqueue_src = files(
  '../wirelog/workqueue.c',
)

# Select threading backend based on platform
if host_machine.system() == 'windows'
  thread_src = files('../wirelog/thread_msvc.c',)
else
  thread_src = files('../wirelog/thread_posix.c',)
endif

test_plan_gen_exe = executable(
  'test_plan_gen',
  files('test_plan_gen.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('plan_gen', test_plan_gen_exe)

# ============================================================================
# Delta Callback Tests (re-enabled for columnar backend)
# ============================================================================

test_delta_callback_exe = executable(
  'test_delta_callback',
  files('test_delta_callback.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('delta_callback', test_delta_callback_exe)

# ============================================================================
# Workqueue Tests (Phase B-lite)
# ============================================================================

test_workqueue_exe = executable(
  'test_workqueue',
  files('test_workqueue.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('workqueue', test_workqueue_exe)

# ============================================================================
# Option 2 CSE Plan Rewriting Tests
# ============================================================================

test_option2_cse_exe = executable(
  'test_option2_cse',
  files('test_option2_cse.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
  c_args: ['-DENABLE_K_FUSION=1'],
)

test('option2_cse', test_option2_cse_exe)

# ============================================================================
# DOOP Validation Tests (US-009/US-010)
# ============================================================================
#
# Infrastructure tests (file checks, parse, synthetic 8-way join) run without
# the 83MB DOOP dataset.  Data-dependent tests auto-skip when bench/data/doop
# is absent.  Full DOOP execution uses scripts/run_doop_validation.sh.

test_option2_doop_exe = executable(
  'test_option2_doop',
  files('test_option2_doop.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('option2_doop', test_option2_doop_exe,
  env: ['DOOP_DATA_DIR=' + meson.project_source_root() / 'bench/data/doop'],
  should_fail: true,
)

# ============================================================================
# Consolidation Function Tests (US-003 TDD RED Phase)
# ============================================================================

test_consolidate_incremental_delta_exe = executable(
  'test_consolidate_incremental_delta',
  files('test_consolidate_incremental_delta.c'),
  backend_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('consolidate_incremental_delta', test_consolidate_incremental_delta_exe)

# ============================================================================
# CSE Cache Hit Rate Tests (US-001 TDD)
# ============================================================================

test_cse_cache_hit_rate_exe = executable(
  'test_cse_cache_hit_rate',
  files('test_cse_cache_hit_rate.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('cse_cache_hit_rate', test_cse_cache_hit_rate_exe)

# ============================================================================
# K-way Merge CONSOLIDATE Tests (US-002 TDD RED Phase)
# ============================================================================

test_consolidate_kway_merge_exe = executable(
  'test_consolidate_kway_merge',
  files('test_consolidate_kway_merge.c'),
  backend_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('consolidate_kway_merge', test_consolidate_kway_merge_exe)

# ============================================================================
# K-Fusion Merge Function Tests
# ============================================================================

test_k_fusion_merge_exe = executable(
  'test_k_fusion_merge',
  files('test_k_fusion_merge.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('k_fusion_merge', test_k_fusion_merge_exe)

# ============================================================================
# K-Fusion Dispatch Integration Tests
# ============================================================================

test_k_fusion_dispatch_exe = executable(
  'test_k_fusion_dispatch',
  files('test_k_fusion_dispatch.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('k_fusion_dispatch', test_k_fusion_dispatch_exe)

# ============================================================================
# R9 Self-Join Delta Permutation Tests
# ============================================================================

test_r9_self_join_delta_exe = executable(
  'test_r9_self_join_delta',
  files('test_r9_self_join_delta.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('r9_self_join_delta', test_r9_self_join_delta_exe)

# ============================================================================
# Empty-Delta Skip Tests (US-006/US-007)
# ============================================================================

test_empty_delta_skip_exe = executable(
  'test_empty_delta_skip',
  files('test_empty_delta_skip.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('empty_delta_skip', test_empty_delta_skip_exe)

# ============================================================================
# Delta Factoring E2E Tests (Issue #85)
# ============================================================================

test_delta_factoring_e2e_exe = executable(
  'test_delta_factoring_e2e',
  files('test_delta_factoring_e2e.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('delta_factoring_e2e', test_delta_factoring_e2e_exe)

# ============================================================================
# K-Fusion End-to-End Integration Tests (Phase 3A)
# ============================================================================

test_k_fusion_e2e_exe = executable(
  'test_k_fusion_e2e',
  files('test_k_fusion_e2e.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('k_fusion_e2e', test_k_fusion_e2e_exe)

# ============================================================================
# Delta Timestamp Tests (Phase 3B - 3B-001)
# ============================================================================

test_delta_timestamp_exe = executable(
  'test_delta_timestamp',
  files('test_delta_timestamp.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('delta_timestamp', test_delta_timestamp_exe)

# ============================================================================
# Arrangement Layer Tests (Phase 3C - 3C-001)
# ============================================================================

test_arrangement_exe = executable(
  'test_arrangement',
  files('test_arrangement.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('arrangement', test_arrangement_exe)

# ============================================================================
# Delta Arrangement Cache Tests (Phase 3C - 3C-001-Ext)
# ============================================================================

test_delta_arrangement_exe = executable(
  'test_delta_arrangement',
  files('test_delta_arrangement.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('delta_arrangement', test_delta_arrangement_exe)

# ============================================================================
# Join Arrangement Integration Tests (Phase 3C - 3C-002)
# ============================================================================

test_join_arrangement_exe = executable(
  'test_join_arrangement',
  files('test_join_arrangement.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('join_arrangement', test_join_arrangement_exe)

# ============================================================================
# Arrangement Cache Reuse Tests (Phase 3C - cache reuse across rules)
# ============================================================================

test_arrangement_cache_reuse_exe = executable(
  'test_arrangement_cache_reuse',
  files('test_arrangement_cache_reuse.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('arrangement_cache_reuse', test_arrangement_cache_reuse_exe)

# ============================================================================
# Frontier Tracking Tests (Phase 3B)
# ============================================================================

test_frontier_exe = executable(
  'test_frontier',
  files('test_frontier.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('frontier', test_frontier_exe)

# ============================================================================
# Frontier Integration Tests (Phase 3B-005)
# ============================================================================

test_frontier_integration_exe = executable(
  'test_frontier_integration',
  files('test_frontier_integration.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('frontier_integration', test_frontier_integration_exe)

# ============================================================================
# Frontier Filtering Tests (Phase 3D - 3D-001)
# ============================================================================

test_frontier_filtering_exe = executable(
  'test_frontier_filtering',
  files('test_frontier_filtering.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('frontier_filtering', test_frontier_filtering_exe)

# ============================================================================
# Frontier Skip Tests (Phase 3D-Ext - 3D-Ext-001)
# ============================================================================

test_frontier_skip_exe = executable(
  'test_frontier_skip',
  files('test_frontier_skip.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('frontier_skip', test_frontier_skip_exe)

# ============================================================================
# Frontier Skip Integration Tests (Phase 3D-Ext-FU - 3D-Ext-FU-002)
# ============================================================================

test_frontier_skip_integration_exe = executable(
  'test_frontier_skip_integration',
  files('test_frontier_skip_integration.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('frontier_skip_integration', test_frontier_skip_integration_exe)

# ============================================================================
# Phase 4: Per-Stratum Frontier Array Tests (US-4-001)
# ============================================================================

test_phase4_frontier_array_exe = executable(
  'test_phase4_frontier_array',
  files('test_phase4_frontier_array.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('phase4_frontier_array', test_phase4_frontier_array_exe)

# ============================================================================
# Phase 4: Incremental Fact Insertion Tests (US-4-002)
# ============================================================================

test_incremental_insertion_exe = executable(
  'incremental_insertion',
  files('test_incremental_insertion.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('incremental_insertion', test_incremental_insertion_exe)

bench_incremental_frontier_exe = executable(
  'bench_incremental_frontier',
  files('bench_incremental_frontier.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)


# ============================================================================
# Phase 4: Affected Strata Detection Tests (US-4-affected_strata)
# ============================================================================

test_affected_strata_exe = executable(
  'test_affected_strata',
  files('test_affected_strata.c'),
  backend_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('affected_strata', test_affected_strata_exe)

# ============================================================================
# Phase 4: Affected Rule Detection Tests (US-4-003)
# ============================================================================

test_affected_rules_exe = executable(
  'test_affected_rules',
  files('test_affected_rules.c'),
  backend_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('affected_rules', test_affected_rules_exe)

# ============================================================================
# Phase 4: Per-Stratum Frontier Integration Tests (US-4-005)
# ============================================================================

test_phase4_frontier_integration_exe = executable(
  'test_phase4_frontier_integration',
  files('test_phase4_frontier_integration.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('phase4_frontier_integration', test_phase4_frontier_integration_exe)

# ============================================================================
# Rule-Level Frontier Integration Tests (Phase 4, US-4-006)
# ============================================================================

test_rule_level_frontier_exe = executable(
  'test_rule_level_frontier',
  files('test_rule_level_frontier.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('rule_level_frontier', test_rule_level_frontier_exe)

# ============================================================================
# Mobius COUNT Signed Multiplicity Tests (US-3B-002 TDD RED Phase)
# ============================================================================

test_mobius_count_signed_exe = executable(
  'test_mobius_count_signed',
  files('test_mobius_count_signed.c'),
  backend_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('mobius_count_signed', test_mobius_count_signed_exe)

# ============================================================================
# Mobius JOIN Weighted Multiplicity Tests (US-3B-004 TDD RED Phase)
# ============================================================================

test_mobius_join_weighted_exe = executable(
  'test_mobius_join_weighted',
  files('test_mobius_join_weighted.c'),
  backend_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('mobius_join_weighted', test_mobius_join_weighted_exe)

# ============================================================================
# Mobius Delta Formula Tests (US-3B-006 TDD RED Phase)
# ============================================================================

test_mobius_delta_formula_exe = executable(
  'test_mobius_delta_formula',
  files('test_mobius_delta_formula.c'),
  backend_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('mobius_delta_formula', test_mobius_delta_formula_exe)

# ============================================================================
# Phase 3B End-to-End Integration Tests (US-3B-008)
# ============================================================================

test_phase3b_integration_exe = executable(
  'test_phase3b_integration',
  files('test_phase3b_integration.c'),
  backend_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('phase3b_integration', test_phase3b_integration_exe)

# ============================================================================
# Phase 3C JOIN Integration Tests (US-3C-001 TDD RED)
# ============================================================================

test_phase3c_join_integration_exe = executable(
  'test_phase3c_join_integration',
  files('test_phase3c_join_integration.c'),
  backend_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('phase3c_join_integration', test_phase3c_join_integration_exe)

# ============================================================================
# Phase 3C REDUCE Integration Tests (US-3C-003 TDD)
# ============================================================================
#
# Tests 1 and 2 (COUNT with multiplicities) call col_op_reduce_weighted directly
# and pass.  Test 3 (SUM weighted) declares col_op_reduce_weighted_sum which is
# not yet implemented; the executable is expected to fail at link time until the
# function is added to columnar_nanoarrow.c.

test_phase3c_reduce_integration_exe = executable(
  'test_phase3c_reduce_integration',
  files('test_phase3c_reduce_integration.c'),
  backend_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('phase3c_reduce_integration', test_phase3c_reduce_integration_exe)

# ============================================================================
# Phase 3C Full Integration Tests (US-3C-005)
# ============================================================================

test_phase3c_full_integration_exe = executable(
  'test_phase3c_full_integration',
  files('test_phase3c_full_integration.c'),
  backend_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('phase3c_full_integration', test_phase3c_full_integration_exe)

# ============================================================================
# Phase 3D Frontier Skip Tests (US-3D-001 TDD)
# ============================================================================

test_frontier_skip_mobius_exe = executable(
  'test_frontier_skip_mobius',
  files('test_frontier_skip_mobius.c'),
  backend_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('frontier_skip_mobius', test_frontier_skip_mobius_exe)

# ============================================================================
# Phase 3D Frontier Skip Integration Tests (US-3D-003)
# ============================================================================

test_phase3d_frontier_skip_integration_exe = executable(
  'test_phase3d_frontier_skip_integration',
  files('test_phase3d_frontier_skip_integration.c'),
  backend_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('phase3d_frontier_skip_integration', test_phase3d_frontier_skip_integration_exe)

# ============================================================================
# Consolidation CoW Tests (Issue #94)
# ============================================================================

test_consolidation_cow_exe = executable(
  'test_consolidation_cow',
  files('test_consolidation_cow.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('consolidation_cow', test_consolidation_cow_exe)

# ============================================================================
# Arrangement Incremental Invalidation Tests (Issue #92)
# ============================================================================

test_arrangement_incr_invalidation_exe = executable(
  'test_arrangement_incremental_invalidation',
  files('test_arrangement_incremental_invalidation.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('arrangement_incr_invalidation', test_arrangement_incr_invalidation_exe)

# ============================================================================
# Delta Propagation Tests (Issue #83)
# ============================================================================

test_delta_propagation_exe = executable(
  'test_delta_propagation',
  files('test_delta_propagation.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('delta_propagation', test_delta_propagation_exe)

# Issue #105: Monotone stratum detection
test_monotone_detection_exe = executable(
  'test_monotone_detection',
  files('test_monotone_detection.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('monotone_detection', test_monotone_detection_exe)

# Issue #99: Multi-worker task dispatch
test_multi_worker_exe = executable(
  'test_multi_worker',
  files('test_multi_worker.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('multi_worker', test_multi_worker_exe)

# Issue #103: 2D Frontier Epoch Tracking Tests
test_2d_frontier_init_exe = executable(
  'test_2d_frontier_init',
  files('test_2d_frontier_init.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('2d_frontier_init', test_2d_frontier_init_exe)

# Issue #104: 2D Frontier Skip Condition Tests
test_2d_frontier_skip_exe = executable(
  'test_2d_frontier_skip',
  files('test_2d_frontier_skip.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('2d_frontier_skip', test_2d_frontier_skip_exe)

test_selective_frontier_reset_exe = executable(
  'test_selective_frontier_reset',
  files('test_selective_frontier_reset.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('selective_frontier_reset', test_selective_frontier_reset_exe)

# Issue #106: Multi-Stratum Rule Frontier Tracking Tests
test_multi_stratum_rule_frontier_exe = executable(
  'test_multi_stratum_rule_frontier',
  files('test_multi_stratum_rule_frontier.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('multi_stratum_rule_frontier', test_multi_stratum_rule_frontier_exe)

# ============================================================================
# CLI Driver Tests (Issue #137 - .output filename parameter support)
# ============================================================================

driver_src = files(
  '../wirelog/cli/driver.c',
)

test_cli_exe = executable(
  'test_cli',
  files('test_cli.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  driver_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('cli', test_cli_exe)

# ============================================================================
# Bitwise Operator Tests (Issue #72)
# ============================================================================

test_bitwise_types_exe = executable(
  'test_bitwise_types',
  files('test_bitwise_types.c'),
  parser_src,
  ir_src,
  csv_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep],
)

test('bitwise_types', test_bitwise_types_exe)

test_bitwise_parser_exe = executable(
  'test_bitwise_parser',
  files('test_bitwise_parser.c'),
  parser_src,
  ir_src,
  csv_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep],
)

test('bitwise_parser', test_bitwise_parser_exe)

test_bitwise_eval_exe = executable(
  'test_bitwise_eval',
  files('test_bitwise_eval.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('bitwise_eval', test_bitwise_eval_exe)

test_bitwise_integration_exe = executable(
  'test_bitwise_integration',
  files('test_bitwise_integration.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('bitwise_integration', test_bitwise_integration_exe)

# ============================================================================
# Hash Function Tests (Issue #144)
# ============================================================================

test_hash_types_exe = executable(
  'test_hash_types',
  files('test_hash_types.c'),
  parser_src,
  ir_src,
  csv_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep],
)

test('hash_types', test_hash_types_exe)

test_hash_parser_exe = executable(
  'test_hash_parser',
  files('test_hash_parser.c'),
  parser_src,
  ir_src,
  csv_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep],
)

test('hash_parser', test_hash_parser_exe)

test_hash_eval_exe = executable(
  'test_hash_eval',
  files('test_hash_eval.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('hash_eval', test_hash_eval_exe)

test_hash_integration_exe = executable(
  'test_hash_integration',
  files('test_hash_integration.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep],
)

test('hash_integration', test_hash_integration_exe)

# ============================================================================
# CRC-32 Tests (Issue #145)
# ============================================================================

crc32_src = files(
  '../wirelog/crc32.c',
)

# Architecture-specific flags to enable hardware CRC32 instructions.
# Applied to all CRC-32 test executables so crc32.c compiles with the
# correct feature macros on each platform.
# Note: GCC/Clang only; MSVC doesn't support these flags and has built-in
# support for CRC32 instructions without explicit flags.
crc32_arch_c_args = []
if cc.get_id() != 'msvc'
  if host_machine.cpu_family() == 'x86_64'
    crc32_arch_c_args = ['-msse4.2']
  elif host_machine.cpu_family() == 'aarch64'
    crc32_arch_c_args = ['-march=armv8-a+crc']
  endif
endif

# CRC-32 tests (Issue #145)
# Note: MSVC has internal compiler errors on these test files; skip on Windows
if cc.get_id() != 'msvc'
  test_crc32_ethernet_exe = executable(
    'test_crc32_ethernet',
    files('test_crc32_ethernet.c'),
    crc32_src,
    c_args: crc32_arch_c_args,
    include_directories: [wirelog_inc, wirelog_src_inc],
  )

  test('crc32_ethernet', test_crc32_ethernet_exe)

  test_crc32_castagnoli_exe = executable(
    'test_crc32_castagnoli',
    files('test_crc32_castagnoli.c'),
    crc32_src,
    c_args: crc32_arch_c_args,
    include_directories: [wirelog_inc, wirelog_src_inc],
  )

  test('crc32_castagnoli', test_crc32_castagnoli_exe)

  # CRC-32 hardware/software equivalence test (Issue #145, US-145-004)
  # Reuses crc32_arch_c_args defined above for the same architecture flags.
  test_crc32_hw_equivalence_exe = executable(
    'test_crc32_hw_equivalence',
    files('test_crc32_hw_equivalence.c'),
    crc32_src,
    c_args: crc32_arch_c_args,
    include_directories: [wirelog_inc, wirelog_src_inc],
  )

  test('crc32_hw_equivalence', test_crc32_hw_equivalence_exe)
endif

# ============================================================================
# Recursive Aggregation Plan Tests - DISABLED in Phase 2C
# ============================================================================
#
# test_recursive_agg_plan depends on deleted DD backend (dd_plan.h)
# This test is disabled until columnar backend provides equivalent plan generation
