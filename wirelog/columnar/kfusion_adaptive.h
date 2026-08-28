/*
 * columnar/kfusion_adaptive.h - session-local low-K K-fusion policy
 *
 * INTERNAL HEADER - not installed, not part of public API.
 */

#ifndef WL_COLUMNAR_KFUSION_ADAPTIVE_H
#define WL_COLUMNAR_KFUSION_ADAPTIVE_H

#include <stdbool.h>
#include <stdint.h>

#define WL_KFUSION_ADAPTIVE_MAX_OPS 32u
#define WL_KFUSION_ADAPTIVE_MIN_SAMPLES 3u
#define WL_KFUSION_ADAPTIVE_MARGIN_PERCENT 5u
#define WL_KFUSION_ADAPTIVE_COOLDOWN 3u

typedef enum {
    WL_KFUSION_ADAPTIVE_UNKNOWN = 0,
    WL_KFUSION_ADAPTIVE_SERIAL,
    WL_KFUSION_ADAPTIVE_PARALLEL,
} wl_kfusion_adaptive_state_t;

typedef struct {
    const void *key;
    uint32_t k;
    uint32_t workers;
    uint64_t size_class;
    uint64_t serial_ewma_ns;
    uint64_t parallel_ewma_ns;
    uint32_t serial_samples;
    uint32_t parallel_samples;
    uint32_t cooldown;
    wl_kfusion_adaptive_state_t state;
} wl_kfusion_adaptive_record_t;

typedef struct {
    wl_kfusion_adaptive_record_t records[WL_KFUSION_ADAPTIVE_MAX_OPS];
} wl_kfusion_adaptive_ctx_t;

typedef enum {
    WL_KFUSION_ADAPTIVE_DECISION_SERIAL = 0,
    WL_KFUSION_ADAPTIVE_DECISION_PARALLEL,
} wl_kfusion_adaptive_decision_t;

wl_kfusion_adaptive_ctx_t *wl_kfusion_adaptive_create(void);
void wl_kfusion_adaptive_destroy(wl_kfusion_adaptive_ctx_t *ctx);
wl_kfusion_adaptive_decision_t wl_kfusion_adaptive_begin(
    wl_kfusion_adaptive_ctx_t *ctx, const void *key, uint32_t k,
    uint32_t workers, uint64_t size_class);
void wl_kfusion_adaptive_observe(wl_kfusion_adaptive_ctx_t *ctx,
    const void *key, uint32_t k, uint32_t workers, uint64_t size_class,
    wl_kfusion_adaptive_decision_t decision, uint64_t elapsed_ns, bool success);

#endif /* WL_COLUMNAR_KFUSION_ADAPTIVE_H */
