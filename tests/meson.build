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
  '../wirelog/ffi/dd_plan.c',
  '../wirelog/intern.c',
  '../wirelog/session.c',
)

optimizer_src = files(
  '../wirelog/passes/fusion.c',
  '../wirelog/passes/jpp.c',
  '../wirelog/passes/sip.c',
)

intern_src = files(
  '../wirelog/intern.c',
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
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test_program_exe = executable(
  'test_program',
  files('test_program.c'),
  parser_src,
  ir_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test_stratify_exe = executable(
  'test_stratify',
  files('test_stratify.c'),
  parser_src,
  ir_src,
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

test_dd_plan_exe = executable(
  'test_dd_plan',
  files('test_dd_plan.c'),
  parser_src,
  ir_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('dd_plan', test_dd_plan_exe)

test_fusion_exe = executable(
  'test_fusion',
  files('test_fusion.c'),
  parser_src,
  ir_src,
  optimizer_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('fusion', test_fusion_exe)

test_jpp_exe = executable(
  'test_jpp',
  files('test_jpp.c'),
  parser_src,
  ir_src,
  optimizer_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('jpp', test_jpp_exe)

test_sip_exe = executable(
  'test_sip',
  files('test_sip.c'),
  parser_src,
  ir_src,
  optimizer_src,
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

columnar_backend_src = files(
  '../wirelog/arena/arena.c',
  '../wirelog/backend/columnar_nanoarrow.c',
  '../wirelog/ffi/dd_marshal.c',
)

test_session_columnar_exe = executable(
  'test_session_columnar',
  files('test_session_columnar.c'),
  parser_src,
  ir_src,
  columnar_backend_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep],
)

test('session_columnar', test_session_columnar_exe)

test_delta_callback_exe = executable(
  'test_delta_callback',
  files('test_delta_callback.c'),
  parser_src,
  ir_src,
  columnar_backend_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep],
)

test('delta_callback', test_delta_callback_exe)

csv_src = files(
  '../wirelog/io/csv_reader.c',
)

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
# Recursive Aggregation Plan Tests (no Rust execution)
# ============================================================================

# Plan-level: non-monotone rejection and monotone acceptance
test_recursive_agg_plan_exe = executable(
  'test_recursive_agg_plan',
  files('test_recursive_agg_plan.c'),
  parser_src,
  ir_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('recursive_agg_plan', test_recursive_agg_plan_exe)
