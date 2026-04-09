# examples/11-time-evolution/meson.build
# Time Evolution demo using the wl_easy facade (#444)
#
# Include all wirelog sources directly (same pattern as example 08 and
# wirelog_cli) to ensure proper linking under LTO.

time_evolution_demo = executable(
  'time_evolution_demo',
  files('time_evolution_demo.c'),
  wirelog_sources,
  config_h_file,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep, zlib_dep, nanoarrow_dep, xxhash_dep, mbedtls_dep, math_dep],
  install: false,
)

# Golden-output regression test: run time_evolution_demo and diff stdout
# against expected.txt.  Python3 is used instead of bash for portability
# across the Linux / macOS / Windows-MSVC CI matrix (wirelog already has
# a python3 dependency for the uncrustify hook setup).
py3 = find_program('python3', 'python', required: true)
test(
  'example_11_time_evolution_golden',
  py3,
  args: [
    files('golden_diff.py')[0],
    time_evolution_demo.full_path(),
    meson.current_source_dir() / 'expected.txt',
  ],
  depends: time_evolution_demo,
  suite: 'examples',
)
