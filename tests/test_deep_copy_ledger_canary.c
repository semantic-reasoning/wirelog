/*
 * test_deep_copy_ledger_canary.c - Issue #598 deep_copy ledger-charge canary
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Regression canary for the col_rel_deep_copy ledger contract.  Per
 * relation.c:1101-1107, deep copies are TRANSIENT relations: dst->mem_ledger
 * is unconditionally NULL even when src is wired to a real ledger, and
 * deep_copy itself does NOT report any allocations against src's ledger.
 *
 * The existing test_col_rel_deep_copy.c::test_deep_copy_mem_ledger_null
 * (Issue #554) already pins:
 *   1. dst->mem_ledger == NULL after deep_copy.
 *   2. src ledger counters are unchanged by the deep_copy itself.
 *
 * This canary adds the destroy-side half of the contract that #598 calls
 * out specifically:
 *
 *   - destroy(dst) does NOT double-charge the source session's ledger.
 *     Because dst->mem_ledger is NULL, col_rel_free_contents must not
 *     accidentally consult src->mem_ledger when it walks dst's columns.
 *
 * Vacuous in production today (col_rel_deep_copy has NO production
 * callers in rotation paths -- verified by grep across wirelog/).  Only
 * tests/test_col_rel_deep_copy.c and tests/col_rel_deep_copy_fixture.c
 * invoke it.  Becomes load-bearing when #550-C wl_session_rotate ships:
 * the cross-arena rotation helper will deep-copy the live relations into
 * the new arena, and any ledger churn would corrupt the per-session
 * memory accounting.
 */

#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/mem_ledger.h"
#include "../wirelog/wirelog-types.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                              \
        do {                                        \
            tests_run++;                            \
            printf("  [%d] %s", tests_run, name);   \
        } while (0)

#define PASS()                                  \
        do {                                        \
            tests_passed++;                         \
            printf(" ... PASS\n");                  \
        } while (0)

#define FAIL(msg)                               \
        do {                                        \
            tests_failed++;                         \
            printf(" ... FAIL: %s\n", (msg));       \
            goto cleanup;                           \
        } while (0)

#define ASSERT(cond, msg)                       \
        do {                                        \
            if (!(cond)) {                          \
                FAIL(msg);                          \
            }                                       \
        } while (0)

/* Build a small populated relation: 3 columns, 4 rows, deterministic
 * content.  Mirrors test_col_rel_deep_copy.c's build_populated helper. */
static col_rel_t *
build_populated(const char *name, uint32_t nrows)
{
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, name) != 0)
        return NULL;
    const char *names[3] = { "alpha", "beta", "gamma" };
    if (col_rel_set_schema(r, 3u, names) != 0) {
        col_rel_destroy(r);
        return NULL;
    }
    for (uint32_t i = 0; i < nrows; i++) {
        int64_t row[3] = {
            (int64_t)i,
            (int64_t)(i * 10),
            (int64_t)(i * 100),
        };
        if (col_rel_append_row(r, row) != 0) {
            col_rel_destroy(r);
            return NULL;
        }
    }
    return r;
}

static void
test_deep_copy_no_ledger_charge(void)
{
    /* Acceptance contract (Issue #598):
     *   - Snapshot ledger.current_bytes before deep_copy.
     *   - Call col_rel_deep_copy(src, &dst).
     *   - Assert ledger.current_bytes == snapshot (deep_copy does not
     *     charge the source ledger -- relation.c:1101-1107).
     *   - Destroy dst.
     *   - Snapshot ledger.current_bytes again.
     *   - Assert ledger.current_bytes == snapshot (destroy did not
     *     accidentally double-charge through the NULL dst->mem_ledger). */
    TEST("col_rel_deep_copy + destroy do not charge src ledger");
    col_rel_t *src = build_populated("ledger_canary_src", 4u);
    col_rel_t *dst = NULL;
    ASSERT(src != NULL, "build_populated failed");

    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 0u /* unlimited */);

    /* Stamp a baseline allocation that mimics what the surrounding
     * pipeline would have charged for src's data buffer.  Mirrors
     * test_deep_copy_mem_ledger_null in test_col_rel_deep_copy.c. */
    const uint64_t baseline_bytes = 4096u;
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION, baseline_bytes);
    src->mem_ledger = &ledger;

    /* Snapshot pre-copy. */
    uint64_t before_copy = atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed);
    ASSERT(before_copy == baseline_bytes,
        "ledger baseline not at expected stamp");

    /* Call deep_copy. */
    int rc = col_rel_deep_copy(src, &dst, NULL);
    ASSERT(rc == 0 && dst != NULL, "col_rel_deep_copy failed");
    ASSERT(dst->mem_ledger == NULL,
        "dst->mem_ledger must be NULL per #554/#598 transient contract");

    /* Snapshot post-copy: must be unchanged. */
    uint64_t after_copy = atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed);
    ASSERT(after_copy == before_copy,
        "deep_copy charged src ledger (must be a no-op)");

    /* Destroy dst.  Because dst->mem_ledger is NULL,
     * col_rel_free_contents must not double-charge through src's
     * ledger -- the canary asserts the destroy path is ledger-clean. */
    col_rel_destroy(dst);
    dst = NULL;

    uint64_t after_destroy = atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed);
    if (after_destroy != before_copy) {
        printf(" ... FAIL: destroy(dst) drifted ledger from %" PRIu64
            " to %" PRIu64 "\n", before_copy, after_destroy);
        tests_failed++;
        goto cleanup;
    }

    /* Detach src's ledger before destroy so col_rel_free_contents does
     * not unwind the synthetic baseline against our local stack ledger
     * (mirrors test_deep_copy_mem_ledger_null's cleanup pattern). */
    src->mem_ledger = NULL;

    PASS();
cleanup:
    if (src)
        src->mem_ledger = NULL;
    col_rel_destroy(src);
    col_rel_destroy(dst);
}

int
main(void)
{
    printf("test_deep_copy_ledger_canary (Issue #598)\n");
    printf("=========================================\n");

    test_deep_copy_no_ledger_charge();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
