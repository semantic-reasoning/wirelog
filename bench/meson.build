bench_flowlog = executable(
  'bench_flowlog',
  files('bench_flowlog.c'),
  wirelog_sources,
  files('../wirelog/ffi/facts_loader.c'),
  files('../wirelog/cli/driver.c'),
  config_h_file,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: wirelog_run_deps,
  install: false,
)

datagen = executable(
  'datagen',
  files('datagen.c'),
  install: false,
)
