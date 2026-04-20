/*
 * tests/test_log_legacy_shim.c - #277 presence-check compat (Issue #287).
 *
 * The legacy flags WL_DEBUG_JOIN / WL_CONSOLIDATION_LOG were presence
 * checks: any value (including "0") enabled verbose output. The new
 * WL_LOG surface preserves that contract via a shim in wl_log_init():
 *
 *   if (getenv("WL_DEBUG_JOIN") != NULL)
 *       wl_log_thresholds[SEC_JOIN] = TRACE;
 *
 * So WL_DEBUG_JOIN=0 must still leave JOIN at TRACE. Likewise when
 * WL_LOG is set, it overrides the shim (including explicit silence via
 * WL_LOG=JOIN:0).
 */

#define _POSIX_C_SOURCE 200809L

#include "wirelog/util/log.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

static void
test_presence_any_value_enables(void)
{
    unsetenv("WL_LOG");
    unsetenv("WL_LOG_FILE");
    setenv("WL_DEBUG_JOIN", "0", 1);
    unsetenv("WL_CONSOLIDATION_LOG");

    wl_log_init();
    assert(wl_log_thresholds[WL_LOG_SEC_JOIN] == (uint8_t)WL_LOG_TRACE);
    assert(wl_log_thresholds[WL_LOG_SEC_CONSOLIDATION] == 0);
    wl_log_shutdown();

    unsetenv("WL_DEBUG_JOIN");
}

static void
test_presence_both_flags(void)
{
    unsetenv("WL_LOG");
    unsetenv("WL_LOG_FILE");
    setenv("WL_DEBUG_JOIN", "anything", 1);
    setenv("WL_CONSOLIDATION_LOG", "", 1);

    wl_log_init();
    assert(wl_log_thresholds[WL_LOG_SEC_JOIN] == (uint8_t)WL_LOG_TRACE);
    assert(wl_log_thresholds[WL_LOG_SEC_CONSOLIDATION] ==
        (uint8_t)WL_LOG_TRACE);
    wl_log_shutdown();

    unsetenv("WL_DEBUG_JOIN");
    unsetenv("WL_CONSOLIDATION_LOG");
}

static void
test_wl_log_overrides_shim(void)
{
    unsetenv("WL_LOG_FILE");
    setenv("WL_DEBUG_JOIN", "1", 1);
    setenv("WL_LOG", "JOIN:0", 1); /* explicit silence */

    wl_log_init();
    assert(wl_log_thresholds[WL_LOG_SEC_JOIN] == 0);
    wl_log_shutdown();

    unsetenv("WL_DEBUG_JOIN");
    unsetenv("WL_LOG");
}

static void
test_absent_flags_all_zero(void)
{
    unsetenv("WL_LOG");
    unsetenv("WL_LOG_FILE");
    unsetenv("WL_DEBUG_JOIN");
    unsetenv("WL_CONSOLIDATION_LOG");

    wl_log_init();
    for (unsigned i = 0; i < (unsigned)WL_LOG_SEC__COUNT; ++i)
        assert(wl_log_thresholds[i] == 0);
    wl_log_shutdown();
}

int
main(void)
{
    test_presence_any_value_enables();
    test_presence_both_flags();
    test_wl_log_overrides_shim();
    test_absent_flags_all_zero();
    puts("test_log_legacy_shim OK");
    return 0;
}
