/*
 * tests/test_log_gating.c - WL_LOG macro gating tests (Issue #287).
 *
 * Covers:
 *   Case A - compile-time ceiling short-circuits argument evaluation.
 *            Delegated to test_log_gating_ceiling.c (built with its own
 *            -DWL_LOG_COMPILE_MAX_LEVEL=1 override).
 *   Case B - runtime threshold=0 short-circuits argument evaluation.
 *   Threshold-based level gating: threshold=4 emits DEBUG, drops TRACE.
 *   Emit capture: last line + count reflect invocations.
 *
 * Links the testhook variant of wl_log_emit (wirelog/util/log_testhook.c)
 * instead of the production log_emit.c — the production TU is deliberately
 * omitted from this executable to avoid duplicate-symbol errors.
 */

#include "wirelog/util/log.h"

#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

/* Test-hook observables from log_testhook.c. */
extern char wl_log_test_last[512];
extern int wl_log_test_count;
extern void wl_log_test_reset(void);

/* Case A entry point (compiled in test_log_gating_ceiling.c with a
 * narrower -DWL_LOG_COMPILE_MAX_LEVEL=1 override). */
int wl_log_case_a_ceiling(int iters);

/*
 * Case A: compile-time ceiling prevents argument evaluation.
 *
 * The call site in test_log_gating_ceiling.c is WL_LOG(..., TRACE, "%d", ++n).
 * Because that TU is built with -DWL_LOG_COMPILE_MAX_LEVEL=1, the compile-time
 * guard (TRACE=5) <= (1) folds to false. C11 §6.5.13 guarantees the && RHS
 * (which contains ++n) is not evaluated.
 */
static void
test_case_a_compile_ceiling(void)
{
    wl_log_test_reset();
    /* Raise runtime threshold high so runtime guard alone would NOT block. */
    for (unsigned i = 0; i < WL_LOG_SEC__COUNT; ++i)
        wl_log_thresholds[i] = WL_LOG_TRACE;

    int n_returned = wl_log_case_a_ceiling(1000);
    assert(n_returned == 0);      /* ++n never ran */
    assert(wl_log_test_count == 0); /* emit never called */
}

/*
 * Case B: runtime threshold=0 leaves argument expressions unevaluated because
 * they live inside the if-body of the macro expansion. C11 §6.5.13 guarantees
 * the && short-circuit, so even at -O0 the ++n statement is syntactically in
 * the not-taken branch. (__builtin_expect is only a branch-prediction hint,
 * not the mechanism that elides the args.)
 *
 * We test at WL_LOG_ERROR rather than TRACE so the compile-time ceiling
 * does NOT also gate the site — this isolates the runtime guard.
 */
static void
test_case_b_runtime_threshold_zero(void)
{
    wl_log_test_reset();
    memset(wl_log_thresholds, 0, sizeof(wl_log_thresholds));

    volatile int n = 0;
    for (int i = 0; i < 1000; ++i) {
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_ERROR, "%d", ++n);
    }
    assert(n == 0);
    assert(wl_log_test_count == 0);
}

/* Threshold=4 (DEBUG): DEBUG emits, TRACE drops. */
static void
test_level_gating(void)
{
    wl_log_test_reset();
    memset(wl_log_thresholds, 0, sizeof(wl_log_thresholds));
    wl_log_thresholds[WL_LOG_SEC_JOIN] = WL_LOG_DEBUG;

    WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG, "debug-visible");
    assert(wl_log_test_count == 1);
    assert(strstr(wl_log_test_last, "debug-visible") != NULL);

    WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_TRACE, "trace-hidden");
    assert(wl_log_test_count == 1);   /* unchanged */
    assert(strstr(wl_log_test_last, "trace-hidden") == NULL);

    WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_ERROR, "error-visible");
    assert(wl_log_test_count == 2);
    assert(strstr(wl_log_test_last, "error-visible") != NULL);
}

/* Boundary test: LVL == threshold emits; LVL == threshold+1 drops. */
static void
test_threshold_boundary(void)
{
    wl_log_test_reset();
    memset(wl_log_thresholds, 0, sizeof(wl_log_thresholds));
    wl_log_thresholds[WL_LOG_SEC_JOIN] = WL_LOG_INFO; /* 3 */

    WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_INFO, "at-threshold");
    assert(wl_log_test_count == 1);
    assert(strstr(wl_log_test_last, "at-threshold") != NULL);

    WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG, "above-threshold-drops");
    assert(wl_log_test_count == 1);
    assert(strstr(wl_log_test_last, "above-threshold-drops") == NULL);
}

/* Different sections have independent thresholds. */
static void
test_section_isolation(void)
{
    wl_log_test_reset();
    memset(wl_log_thresholds, 0, sizeof(wl_log_thresholds));
    wl_log_thresholds[WL_LOG_SEC_JOIN] = WL_LOG_TRACE;
    /* SEC_CONSOLIDATION stays at 0 */

    WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_TRACE, "join-visible");
    assert(wl_log_test_count == 1);

    WL_LOG(WL_LOG_SEC_CONSOLIDATION, WL_LOG_TRACE, "consolidation-hidden");
    assert(wl_log_test_count == 1);
}

/*
 * Side-effect argument test (belt and suspenders with Case B): the format
 * argument includes an external function that would abort the test if
 * invoked. With threshold=0 the function must not be called.
 */
static int wl_log_abort_arg_called_(void);
static int wl_log_abort_arg_called_flag_ = 0;

static int
wl_log_abort_arg_called_(void)
{
    wl_log_abort_arg_called_flag_ = 1;
    return 42;
}

static void
test_disabled_path_no_side_effects(void)
{
    wl_log_test_reset();
    wl_log_abort_arg_called_flag_ = 0;
    memset(wl_log_thresholds, 0, sizeof(wl_log_thresholds));

    /* Use WL_LOG_ERROR so the compile-time ceiling does not gate (isolates
     * the runtime guard). */
    for (int i = 0; i < 100; ++i) {
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_ERROR, "%d", wl_log_abort_arg_called_());
    }
    assert(wl_log_abort_arg_called_flag_ == 0);
    assert(wl_log_test_count == 0);
}

int
main(void)
{
    test_case_a_compile_ceiling();
    test_case_b_runtime_threshold_zero();
    test_level_gating();
    test_threshold_boundary();
    test_section_isolation();
    test_disabled_path_no_side_effects();
    puts("test_log_gating OK");
    return 0;
}
