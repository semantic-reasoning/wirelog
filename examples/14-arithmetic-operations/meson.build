# Example 14: Arithmetic Operations

arithmetic_demo = executable(
  'arithmetic_demo',
  files('arithmetic_demo.c'),
  wirelog_sources,
  config_h_file,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep, zlib_dep, nanoarrow_dep, xxhash_dep, mbedtls_dep, math_dep],
  install: false,
)

py3 = find_program('python3', 'python', required: true)
test(
  'example_14_arithmetic_golden',
  py3,
  args: [
    files('../09-retraction-basics/golden_diff.py')[0],
    arithmetic_demo.full_path(),
    meson.current_source_dir() / 'expected.txt',
  ],
  depends: arithmetic_demo,
  suite: 'examples',
)
