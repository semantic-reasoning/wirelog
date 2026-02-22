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

# ============================================================================
# Register Tests
# ============================================================================

test('lexer', test_lexer_exe)
test('parser', test_parser_exe)
