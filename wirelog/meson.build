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

wirelog_optimizer_src = files('passes/fusion.c', 'passes/jpp.c', 'passes/sip.c', 'passes/subsumption.c', 'passes/magic_sets.c', )

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Pure C11 Backend(Columnar Nanoarrow Only)
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

#DD backend removed in Phase 2C
#All execution now uses columnar_nanoarrow backend

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Arena Allocator Module
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_arena_src = files('arena/arena.c', 'arena/compound_arena.c', )

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Thread Backend Module(Issue #88 - Cross-Platform Threading)
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

# Select threading backend: C11 threads.h > POSIX pthreads > Windows MSVC
# C11 threads.h detection uses cc.links() to verify actual support,
# not just header presence (Apple Clang ships no threads.h).
#
# -Dthreads=posix forces POSIX pthreads (required for TSan, which only
# intercepts pthread calls, not C11 thread functions).
c11_threads_test = '''
#include <threads.h>
static int dummy(void *arg) { (void)arg; return 0; }
int main(void) {
    thrd_t t;
    if (thrd_create(&t, dummy, 0) == thrd_success)
        thrd_join(t, 0);
    return 0;
}
'''

if threads_option == 'posix'
    have_c11_threads = false
    message('  Threading backend: POSIX pthreads (forced via -Dthreads=posix)')
else
    have_c11_threads = cc.links(c11_threads_test, name: 'C11 <threads.h> support')
endif

if have_c11_threads
    wirelog_thread_src = files('thread_c11.c', )
    add_project_arguments('-DWL_HAVE_C11_THREADS', language: 'c')
    message('  Threading backend: C11 <threads.h>')
elif host_machine.system() == 'windows'
    wirelog_thread_src = files('thread_msvc.c', )
    message('  Threading backend: Windows MSVC')
else
    wirelog_thread_src = files('thread_posix.c', )
    message('  Threading backend: POSIX pthreads')
endif

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Work Queue Module(Phase B - lite)
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_workqueue_src = files('workqueue.c', )

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Lock-Free MPSC Queue Module(Issue #401)
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_lockfree_queue_src = files('util/lockfree_queue.c', )

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Columnar Backend Module(Phase 2A)
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_backend_src = files('columnar/relation.c', 'columnar/cache.c',
                           'columnar/ops.c', 'columnar/eval.c',
                           'columnar/arrangement.c', 'columnar/frontier.c', 'columnar/frontier_epoch.c',
                           'columnar/mobius.c', 'columnar/session.c', 'columnar/session_hash.c',
                           'columnar/delta_pool.c', 'columnar/lftj.c',
                           'columnar/mem_ledger.c', 'columnar/diff_trace.c',
                           'columnar/diff_arrangement.c',
                           'columnar/diff_vtable.c',
                           'columnar/diff_bridge.c',
                           'columnar/partition.c',
                           'columnar/progress.c',
                           'columnar/rotation_standard.c',
                           'columnar/rotation_mvcc.c',
                           'columnar/compound_side.c',
                           'columnar/handle_remap.c',
                           'columnar/handle_remap_apply.c',
                           'util/lockfree_queue.c', )

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#I / O Module
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_io_src = files('io/csv_reader.c', 'io/io_adapter.c', 'io/io_ctx.c', 'io/csv_adapter.c')

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#String Operations Module(Issue #143)
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_string_ops_src = files('string_ops.c', )

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#wl_easy Module(Issue #441)
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_wl_easy_src = files('wl_easy.c', )

#==================================================================================
# Util / Logger Module (Issue #287 - GST_DEBUG-style structured logging)
#==================================================================================

subdir('util')

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#CLI Driver Module
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_cli_src = files('cli/driver.c', 'cli/main.c', )
