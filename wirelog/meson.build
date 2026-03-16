#wirelog / meson.build
#Source module collection

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Parser Module
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_parser_src = files('parser/lexer.c', 'parser/ast.c', 'parser/parser.c', )

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#IR Module
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_ir_src = files('ir/ir.c', 'ir/program.c', 'ir/stratify.c', 'ir/api.c', 'intern.c',
                        'session.c', 'exec_plan_gen.c', 'session_facts.c', )

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Optimizer Module
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_optimizer_src = files('passes/fusion.c', 'passes/jpp.c', 'passes/sip.c', )

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Pure C11 Backend(Columnar Nanoarrow Only)
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

#DD backend removed in Phase 2C
#All execution now uses columnar_nanoarrow backend

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Arena Allocator Module
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_arena_src = files('arena/arena.c', )

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Thread Backend Module(Issue #88 - Cross-Platform Threading)
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

# Select threading backend based on platform
if host_machine.system() == 'windows'
    wirelog_thread_src = files('thread_msvc.c', )
else
    wirelog_thread_src = files('thread_posix.c', )
endif

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Work Queue Module(Phase B - lite)
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_workqueue_src = files('workqueue.c', )

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Columnar Backend Module(Phase 2A)
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_backend_src = files('columnar/relation.c', 'columnar/cache.c',
                           'columnar/ops.c', 'columnar/eval.c',
                           'columnar/arrangement.c', 'columnar/frontier.c',
                           'columnar/mobius.c', 'columnar/session.c',
                           'columnar/delta_pool.c', )

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#I / O Module
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_io_src = files('io/csv_reader.c', )

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#CLI Driver Module
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_cli_src = files('cli/driver.c', 'cli/main.c', )
