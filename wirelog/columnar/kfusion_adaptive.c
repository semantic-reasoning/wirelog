/*
 * columnar/kfusion_adaptive.c - session-local low-K K-fusion policy
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "columnar/kfusion_adaptive.h"

#include <stdlib.h>
#include <string.h>

static wl_kfusion_adaptive_record_t *
find_record(wl_kfusion_adaptive_ctx_t *ctx, const void *key, uint32_t k,
    uint32_t workers, uint64_t size_class)
{
    wl_kfusion_adaptive_record_t *free_record = NULL;
    for (uint32_t i = 0; i < WL_KFUSION_ADAPTIVE_MAX_OPS; i++) {
        wl_kfusion_adaptive_record_t *record = &ctx->records[i];
        if (record->key == key) {
            if (record->k != k || record->workers != workers
                || record->size_class != size_class) {
                memset(record, 0, sizeof(*record));
                record->key = key;
                record->k = k;
                record->workers = workers;
                record->size_class = size_class;
            }
            return record;
        }
        if (!record->key && !free_record)
            free_record = record;
    }
    if (!free_record)
        free_record = &ctx->records[0];
    memset(free_record, 0, sizeof(*free_record));
    free_record->key = key;
    free_record->k = k;
    free_record->workers = workers;
    free_record->size_class = size_class;
    return free_record;
}

static void
update_ewma(uint64_t *ewma, uint64_t sample)
{
    if (*ewma == 0)
        *ewma = sample;
    else if (*ewma > UINT64_MAX / 3
        || sample > UINT64_MAX - (*ewma * 3))
        *ewma = UINT64_MAX;
    else
        *ewma = (*ewma * 3 + sample) / 4;
}

wl_kfusion_adaptive_ctx_t *
wl_kfusion_adaptive_create(void)
{
    return (wl_kfusion_adaptive_ctx_t *)calloc(1,
               sizeof(wl_kfusion_adaptive_ctx_t));
}

void
wl_kfusion_adaptive_destroy(wl_kfusion_adaptive_ctx_t *ctx)
{
    free(ctx);
}

wl_kfusion_adaptive_decision_t
wl_kfusion_adaptive_begin(wl_kfusion_adaptive_ctx_t *ctx, const void *key,
    uint32_t k, uint32_t workers, uint64_t size_class)
{
    if (!ctx || !key || k >= 4 || workers <= 1)
        return WL_KFUSION_ADAPTIVE_DECISION_SERIAL;
    wl_kfusion_adaptive_record_t *record
        = find_record(ctx, key, k, workers, size_class);
    if (record->cooldown > 0) {
        record->cooldown--;
        return record->state == WL_KFUSION_ADAPTIVE_PARALLEL
            ? WL_KFUSION_ADAPTIVE_DECISION_PARALLEL
            : WL_KFUSION_ADAPTIVE_DECISION_SERIAL;
    }
    if (record->state == WL_KFUSION_ADAPTIVE_PARALLEL)
        return WL_KFUSION_ADAPTIVE_DECISION_PARALLEL;
    if (record->serial_samples < WL_KFUSION_ADAPTIVE_MIN_SAMPLES)
        return WL_KFUSION_ADAPTIVE_DECISION_SERIAL;
    return WL_KFUSION_ADAPTIVE_DECISION_PARALLEL;
}

void
wl_kfusion_adaptive_observe(wl_kfusion_adaptive_ctx_t *ctx, const void *key,
    uint32_t k, uint32_t workers, uint64_t size_class,
    wl_kfusion_adaptive_decision_t decision, uint64_t elapsed_ns, bool success)
{
    if (!ctx || !key || k >= 4 || workers <= 1)
        return;
    wl_kfusion_adaptive_record_t *record
        = find_record(ctx, key, k, workers, size_class);
    if (!success || elapsed_ns == 0 || elapsed_ns > UINT64_MAX / 2) {
        record->state = WL_KFUSION_ADAPTIVE_SERIAL;
        record->cooldown = WL_KFUSION_ADAPTIVE_COOLDOWN;
        return;
    }
    if (decision == WL_KFUSION_ADAPTIVE_DECISION_PARALLEL) {
        update_ewma(&record->parallel_ewma_ns, elapsed_ns);
        record->parallel_samples++;
    } else {
        update_ewma(&record->serial_ewma_ns, elapsed_ns);
        record->serial_samples++;
    }
    if (record->serial_samples < WL_KFUSION_ADAPTIVE_MIN_SAMPLES
        || record->parallel_samples < WL_KFUSION_ADAPTIVE_MIN_SAMPLES) {
        record->state = WL_KFUSION_ADAPTIVE_SERIAL;
        return;
    }
    uint64_t parallel_limit = record->serial_ewma_ns / 100u
        * (100u - WL_KFUSION_ADAPTIVE_MARGIN_PERCENT);
    record->state = record->parallel_ewma_ns <= parallel_limit
        ? WL_KFUSION_ADAPTIVE_PARALLEL : WL_KFUSION_ADAPTIVE_SERIAL;
    record->cooldown = WL_KFUSION_ADAPTIVE_COOLDOWN;
}
