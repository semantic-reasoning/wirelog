/*
 * test_k_fusion_adaptive.c - deterministic low-K K-fusion policy tests.
 */

#include "columnar/kfusion_adaptive.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

static void
observe_serial(wl_kfusion_adaptive_ctx_t *ctx, const void *key,
    uint32_t workers, uint64_t elapsed)
{
    wl_kfusion_adaptive_observe(ctx, key, 2, workers, 16,
        WL_KFUSION_ADAPTIVE_DECISION_SERIAL, elapsed, true);
}

static void
observe_parallel(wl_kfusion_adaptive_ctx_t *ctx, const void *key,
    uint32_t workers, uint64_t elapsed)
{
    wl_kfusion_adaptive_observe(ctx, key, 2, workers, 16,
        WL_KFUSION_ADAPTIVE_DECISION_PARALLEL, elapsed, true);
}

int
main(void)
{
    wl_kfusion_adaptive_ctx_t *ctx = wl_kfusion_adaptive_create();
    assert(ctx != NULL);
    int key = 1;

    assert(wl_kfusion_adaptive_begin(ctx, &key, 2, 4, 16)
        == WL_KFUSION_ADAPTIVE_DECISION_SERIAL);
    observe_serial(ctx, &key, 4, 100);
    observe_serial(ctx, &key, 4, 100);
    observe_serial(ctx, &key, 4, 100);

    /* Three fast parallel observations are required before promotion. */
    assert(wl_kfusion_adaptive_begin(ctx, &key, 2, 4, 16)
        == WL_KFUSION_ADAPTIVE_DECISION_PARALLEL);
    observe_parallel(ctx, &key, 4, 80);
    observe_parallel(ctx, &key, 4, 80);
    observe_parallel(ctx, &key, 4, 80);
    assert(ctx->records[0].state == WL_KFUSION_ADAPTIVE_PARALLEL);

    /* A worker-width change gets a fresh record and starts conservatively. */
    assert(wl_kfusion_adaptive_begin(ctx, &key, 2, 8, 16)
        == WL_KFUSION_ADAPTIVE_DECISION_SERIAL);

    /* A failed sample and a marginally slower probe both return to serial. */
    wl_kfusion_adaptive_observe(ctx, &key, 2, 4, 16,
        WL_KFUSION_ADAPTIVE_DECISION_PARALLEL, 100, true);
    assert(ctx->records[0].state == WL_KFUSION_ADAPTIVE_SERIAL);
    wl_kfusion_adaptive_observe(ctx, &key, 2, 4, 16,
        WL_KFUSION_ADAPTIVE_DECISION_PARALLEL, 100, false);
    assert(ctx->records[0].state == WL_KFUSION_ADAPTIVE_SERIAL);

    assert(wl_kfusion_adaptive_begin(ctx, &key, 4, 4, 16)
        == WL_KFUSION_ADAPTIVE_DECISION_SERIAL);
    wl_kfusion_adaptive_destroy(ctx);
    return 0;
}
