# examples/08-delta-queries/meson.build
# Delta Queries demo using the C callback API

subdir_wirelog = '../../wirelog'

demo_parser_src = files(
  subdir_wirelog / 'parser/lexer.c',
  subdir_wirelog / 'parser/ast.c',
  subdir_wirelog / 'parser/parser.c',
)

demo_ir_src = files(
  subdir_wirelog / 'ir/ir.c',
  subdir_wirelog / 'ir/program.c',
  subdir_wirelog / 'ir/stratify.c',
  subdir_wirelog / 'ir/api.c',
  subdir_wirelog / 'intern.c',
  subdir_wirelog / 'session.c',
  subdir_wirelog / 'exec_plan_gen.c',
  subdir_wirelog / 'session_facts.c',
)

demo_optimizer_src = files(
  subdir_wirelog / 'passes/fusion.c',
  subdir_wirelog / 'passes/jpp.c',
  subdir_wirelog / 'passes/sip.c',
)

demo_backend_src = files(
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
)

demo_arena_src = files(
  subdir_wirelog / 'arena/arena.c',
)

demo_workqueue_src = files(
  subdir_wirelog / 'workqueue.c',
)

demo_csv_src = files(
  subdir_wirelog / 'io/csv_reader.c',
)

if host_machine.system() == 'windows'
  demo_thread_src = files(subdir_wirelog / 'thread_msvc.c')
else
  demo_thread_src = files(subdir_wirelog / 'thread_posix.c')
endif

# delta_demo: C API example for delta-callback pattern
# Build: meson compile -C build delta_demo
delta_demo = executable(
  'delta_demo',
  files('delta_demo.c'),
  demo_parser_src,
  demo_ir_src,
  demo_optimizer_src,
  demo_backend_src,
  demo_arena_src,
  demo_thread_src,
  demo_workqueue_src,
  demo_csv_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep],
  install: false,
)
