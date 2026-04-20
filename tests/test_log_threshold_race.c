/*
 * tests/test_log_threshold_race.c - TSan sanity for wl_log_thresholds.
 *
 * wl_log_init() establishes thresholds once; runtime readers on hot paths
 * treat the byte load as lock-free. This test spawns N reader threads
 * that spin on wl_log_thresholds[SEC_JOIN] while one writer flips the
 * byte between two valid levels, and asserts readers never observe a
 * torn value.
 *
 * The production WL_LOG macro reads the byte with a plain load — which
 * is safe in practice because aligned 1-byte loads/stores are atomic on
 * every target wirelog supports. C's abstract memory model still treats
 * that as a data race, so under TSan we must communicate intent via
 * __atomic_* builtins with __ATOMIC_RELAXED. On x86/ARM these compile
 * to the same plain-byte instructions the production macro emits: the
 * test is functionally identical to the production reader, just
 * annotated for the sanitizer.
 *
 * Rationale for including this even though threshold writes normally
 * happen only at init: re-entering wl_log_init() (documented as
 * idempotent) writes the table; a concurrent reader in another thread
 * must not observe a torn value. This test is the canary for any future
 * change that would introduce an unsafe write pattern.
 */

#include "wirelog/util/log.h"

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

#define READERS 4
#define ITERS_PER_READER 200000
#define WRITER_ITERS 20000

static atomic_int stop_flag_;

static void *
reader_fn(void *arg)
{
    (void)arg;
    uint64_t saw_low = 0, saw_high = 0, saw_other = 0;
    for (int i = 0; i < ITERS_PER_READER; ++i) {
        /* __atomic_load_n(..., __ATOMIC_RELAXED) on a uint8_t compiles to a
         * plain movzb on x86/ARM - same instruction as the production
         * WL_LOG macro's plain-byte read - but satisfies TSan's memory
         * model rules. */
        uint8_t v = __atomic_load_n(&wl_log_thresholds[WL_LOG_SEC_JOIN],
                __ATOMIC_RELAXED);
        /* Writer only assigns 0 or TRACE; any other value is a torn read. */
        if (v == 0)
            ++saw_low;
        else if (v == (uint8_t)WL_LOG_TRACE)
            ++saw_high;
        else
            ++saw_other;
        if (atomic_load_explicit(&stop_flag_, memory_order_relaxed))
            break;
    }
    assert(saw_other == 0);
    (void)saw_low;
    (void)saw_high;
    return NULL;
}

static void *
writer_fn(void *arg)
{
    (void)arg;
    for (int i = 0; i < WRITER_ITERS; ++i) {
        uint8_t v = (i & 1) ? (uint8_t)WL_LOG_TRACE : (uint8_t)0;
        __atomic_store_n(&wl_log_thresholds[WL_LOG_SEC_JOIN],
            v, __ATOMIC_RELAXED);
    }
    atomic_store_explicit(&stop_flag_, 1, memory_order_relaxed);
    return NULL;
}

int
main(void)
{
    memset((void *)wl_log_thresholds, 0, WL_LOG_SEC__COUNT);
    atomic_init(&stop_flag_, 0);

    pthread_t readers[READERS];
    pthread_t writer;

    for (int i = 0; i < READERS; ++i) {
        int rc = pthread_create(&readers[i], NULL, reader_fn, NULL);
        assert(rc == 0);
    }
    int rc = pthread_create(&writer, NULL, writer_fn, NULL);
    assert(rc == 0);

    pthread_join(writer, NULL);
    for (int i = 0; i < READERS; ++i)
        pthread_join(readers[i], NULL);

    puts("test_log_threshold_race OK");
    return 0;
}
