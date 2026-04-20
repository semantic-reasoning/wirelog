/*
 * tests/test_log_gating_ceiling.c - Case A: compile-time ceiling short-circuit.
 *
 * This TU is compiled with -DWL_LOG_COMPILE_MAX_LEVEL=1 (ERROR). If the macro
 * expansion honors the compile-time guard via C11 §6.5.13 short-circuit, the
 * __VA_ARGS__ in a TRACE-level call site must not be evaluated. The side
 * effect in the format argument therefore never runs — proving zero argument
 * evaluation at compile-disabled levels.
 *
 * The ceiling override happens via meson's per-source c_args. Passing the
 * define here would be shadowed by the project-wide -DWL_LOG_COMPILE_MAX_LEVEL
 * emitted by the root meson.build; meson's per-target c_args are appended
 * after project_arguments, so the narrower define wins.
 */

#include "wirelog/util/log.h"

int wl_log_case_a_ceiling(int iters);

int
wl_log_case_a_ceiling(int iters)
{
    /* volatile prevents LTO from proving n is dead and eliminating the
     * increment as a whole-function optimization, which would make this
     * test pass vacuously even if the compile-time guard did not fire. */
    volatile int n = 0;
    for (int i = 0; i < iters; ++i) {
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_TRACE, "%d", ++n);
    }
    return n;
}
