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

# Columnar backend tests disabled in Phase 2C
# test_session_columnar and test_delta_callback depend on deleted DD backend
# These will be re-enabled when columnar backend provides equivalent functionality

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
# Plan Generator + Session Facts Tests (Phase 2C columnar backend)
# ============================================================================

backend_src = files(
  '../wirelog/backend/columnar_nanoarrow.c',
)

io_src = files(
  '../wirelog/io/csv_reader.c',
)

arena_src = files(
  '../wirelog/arena/arena.c',
)

test_plan_gen_exe = executable(
  'test_plan_gen',
  files('test_plan_gen.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep],
)

test('plan_gen', test_plan_gen_exe)

# ============================================================================
# Recursive Aggregation Plan Tests - DISABLED in Phase 2C
# ============================================================================
#
# test_recursive_agg_plan depends on deleted DD backend (dd_plan.h)
# This test is disabled until columnar backend provides equivalent plan generation
