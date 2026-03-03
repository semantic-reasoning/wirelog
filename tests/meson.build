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

ffi_src = files(
  '../wirelog/ffi/dd_marshal.c',
)

# facts_loader.c calls wl_dd_load_edb (Rust FFI), so it is only
# compiled into targets that link rust_ffi_dep.
facts_loader_src = files(
  '../wirelog/ffi/facts_loader.c',
)

dd_backend_src = files(
  '../wirelog/backend_dd.c',
)

test_dd_ffi_exe = executable(
  'test_dd_ffi',
  files('test_dd_ffi.c'),
  parser_src,
  ir_src,
  ffi_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('dd_ffi', test_dd_ffi_exe)

csv_src = files(
  '../wirelog/io/csv_reader.c',
)

test_dd_execute_exe = executable(
  'test_dd_execute',
  files('test_dd_execute.c'),
  parser_src,
  ir_src,
  ffi_src,
  facts_loader_src,
  dd_backend_src,
  csv_src,
  dependencies: [rust_ffi_dep],
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('dd_execute', test_dd_execute_exe)

cli_src = files(
  '../wirelog/cli/driver.c',
)

test_cli_exe = executable(
  'test_cli',
  files('test_cli.c'),
  parser_src,
  ir_src,
  ffi_src,
  facts_loader_src,
  dd_backend_src,
  csv_src,
  optimizer_src,
  cli_src,
  dependencies: [rust_ffi_dep],
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('cli', test_cli_exe)

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

test_session_exe = executable(
  'test_session',
  files('test_session.c'),
  parser_src,
  ir_src,
  ffi_src,
  facts_loader_src,
  dd_backend_src,
  dependencies: [rust_ffi_dep],
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('session', test_session_exe)
