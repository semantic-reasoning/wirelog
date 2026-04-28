# bench_flowlog re-enabled with columnar backend (Phase 2C)

subdir_wirelog = '../wirelog'

bench_parser_src = files(
  subdir_wirelog / 'parser/lexer.c',
  subdir_wirelog / 'parser/ast.c',
  subdir_wirelog / 'parser/parser.c',
)

bench_ir_src = files(
  subdir_wirelog / 'ir/ir.c',
  subdir_wirelog / 'ir/program.c',
  subdir_wirelog / 'ir/stratify.c',
  subdir_wirelog / 'ir/api.c',
  subdir_wirelog / 'intern.c',
  subdir_wirelog / 'session.c',
  subdir_wirelog / 'exec_plan_gen.c',
  subdir_wirelog / 'session_facts.c',
)

bench_optimizer_src = files(
  subdir_wirelog / 'passes/fusion.c',
  subdir_wirelog / 'passes/jpp.c',
  subdir_wirelog / 'passes/sip.c',
)

bench_backend_src = files(
  subdir_wirelog / 'columnar/relation.c',
  subdir_wirelog / 'columnar/cache.c',
  subdir_wirelog / 'columnar/ops.c',
  subdir_wirelog / 'columnar/eval.c',
  subdir_wirelog / 'columnar/arrangement.c',
  subdir_wirelog / 'columnar/frontier.c',
  subdir_wirelog / 'columnar/frontier_epoch.c',
  subdir_wirelog / 'columnar/mobius.c',
  subdir_wirelog / 'columnar/session.c',
  subdir_wirelog / 'columnar/session_hash.c',
  subdir_wirelog / 'columnar/delta_pool.c',
  subdir_wirelog / 'columnar/lftj.c',
  subdir_wirelog / 'columnar/mem_ledger.c',
  subdir_wirelog / 'columnar/diff_trace.c',
  subdir_wirelog / 'columnar/diff_arrangement.c',
  subdir_wirelog / 'columnar/diff_vtable.c',
  subdir_wirelog / 'columnar/diff_bridge.c',
  subdir_wirelog / 'columnar/partition.c',
  subdir_wirelog / 'columnar/progress.c',
  subdir_wirelog / 'columnar/rotation_standard.c',
  subdir_wirelog / 'columnar/rotation_pinned.c',
  subdir_wirelog / 'util/lockfree_queue.c',
  subdir_wirelog / 'util/log.c',
  subdir_wirelog / 'util/log_emit.c',
  subdir_wirelog / 'string_ops.c',
)

bench_io_src = files(
  subdir_wirelog / 'io/csv_reader.c',
  subdir_wirelog / 'io/io_adapter.c',
  subdir_wirelog / 'io/io_ctx.c',
  subdir_wirelog / 'io/csv_adapter.c',
)

bench_arena_src = files(
  subdir_wirelog / 'arena/arena.c',
  subdir_wirelog / 'arena/compound_arena.c',
)

bench_workqueue_src = files(
  subdir_wirelog / 'workqueue.c',
)

# Reuse the same threading backend selected by wirelog/meson.build
# (C11 threads.h > POSIX pthreads > Windows MSVC).
bench_thread_src = wirelog_thread_src

# Argv parsing routes through bench/bench_argv.h, so bench_flowlog and
# bench_flowlog_seq build under MSVC too (issue #508 Phase 1).
bench_flowlog = executable(
  'bench_flowlog',
  files('bench_flowlog.c'),
  bench_parser_src,
  bench_ir_src,
  bench_optimizer_src,
  bench_backend_src,
  bench_io_src,
  bench_arena_src,
  bench_thread_src,
  bench_workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
  c_args: ['-DWL_RADIX_BENCH'],
  install: false,
)

bench_flowlog_seq = executable(
  'bench_flowlog_seq',
  files('bench_flowlog.c'),
  bench_parser_src,
  bench_ir_src,
  bench_optimizer_src,
  bench_backend_src,
  bench_io_src,
  bench_arena_src,
  bench_thread_src,
  bench_workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
  c_args: ['-DENABLE_K_FUSION=0', '-DWITH_K_FUSION=0'],
  install: false,
)

# Column-major migration baseline benchmark (Issue #327)
bench_col_layout = executable(
  'bench_col_layout',
  files('bench_col_layout.c'),
  bench_backend_src,
  files(subdir_wirelog / 'intern.c'),
  bench_arena_src,
  bench_thread_src,
  bench_workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
  install: false,
)

# Compound term microbenchmarks (Issue #536)
# Modes: parser | match | semijoin | multi-graph
# bench_compound_exe is referenced by tests/meson.build (bench_compound_smoke).
bench_compound_exe = executable(
  'bench_compound',
  files('bench_compound.c'),
  bench_parser_src,
  bench_backend_src,
  files(subdir_wirelog / 'intern.c'),
  bench_arena_src,
  bench_thread_src,
  bench_workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
  install: false,
)

datagen = executable(
  'datagen',
  files('datagen.c'),
  install: false,
)
