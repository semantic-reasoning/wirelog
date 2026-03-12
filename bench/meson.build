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
  subdir_wirelog / 'backend/columnar_nanoarrow.c',
  subdir_wirelog / 'backend/delta_pool.c',
)

bench_io_src = files(
  subdir_wirelog / 'io/csv_reader.c',
)

bench_arena_src = files(
  subdir_wirelog / 'arena/arena.c',
)

bench_workqueue_src = files(
  subdir_wirelog / 'workqueue.c',
)

# Select threading backend based on platform
if host_machine.system() == 'windows'
  bench_thread_src = files(subdir_wirelog / 'thread_msvc.c')
else
  bench_thread_src = files(subdir_wirelog / 'thread_posix.c')
endif

# Skip bench_flowlog on Windows (uses POSIX getopt)
if host_machine.system() != 'windows'
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
    dependencies: [nanoarrow_dep, threads_dep],
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
    dependencies: [nanoarrow_dep, threads_dep],
    c_args: ['-DENABLE_K_FUSION=0', '-DWITH_K_FUSION=0'],
    install: false,
  )
endif

# Skip datagen on Windows (uses POSIX getopt_long)
if host_machine.system() != 'windows'
  datagen = executable(
    'datagen',
    files('datagen.c'),
    install: false,
  )
endif
