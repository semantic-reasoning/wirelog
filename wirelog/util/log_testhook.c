/*
 * wirelog/util/log_testhook.c - test-only emit capture.
 *
 * NEVER linked into libwirelog. Linked only into gating test binaries via
 * the wirelog_log_testing_dep meson dependency. Enforced by
 * scripts/ci/check-no-testhook-in-libwirelog.sh.
 *
 * Commit 2 fleshes out the capture emit body; commit 1 is an empty shell.
 */

#include "wirelog/util/log.h"

/* Public test-observable state, populated by the commit-2 emit body. */
char wl_log_test_last[512];
int wl_log_test_count;
