# examples/08-delta-queries/meson.build
# Delta Queries demo using the wl_easy facade (#441)
#
# Include all wirelog sources directly (same pattern as wirelog_cli) to
# ensure proper linking under LTO. Linking against wirelog_static_lib fails
# on ubuntu-clang + LTO because the LTO plugin does not pull in all needed
# symbols from a static archive.

delta_demo = executable(
  'delta_demo',
  files('delta_demo.c'),
  wirelog_sources,
  config_h_file,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep, zlib_dep, nanoarrow_dep, xxhash_dep, mbedtls_dep, math_dep],
  install: false,
)
