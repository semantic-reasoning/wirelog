/*
 * wirelog/util/log.h - wirelog Structured Logger (GST_DEBUG style)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - never transitively included by any installed/public header.
 * Enforced by scripts/check_log_header_not_public.sh.
 *
 * Zero-overhead when disabled:
 *   - Levels above WL_LOG_COMPILE_MAX_LEVEL (-Dwirelog_log_max_level=...) are
 *     stripped at compile time; argument expressions are NOT evaluated.
 *   - Runtime-disabled sites are a single byte load + predicted-not-taken branch.
 *
 * Safety:
 *   - WL_LOG is NOT async-signal-safe. Do not call from signal handlers.
 *   - No pthread_atfork is installed. After fork(), children that changed the
 *     sink must call wl_log_init() again.
 *   - Thresholds are written once at wl_log_init(); subsequent reads are
 *     lock-free byte loads. Emit uses FILE-internal locking via fwrite.
 */

#ifndef WIRELOG_UTIL_LOG_H
#define WIRELOG_UTIL_LOG_H

#include <stdalign.h>
#include <stdint.h>

#ifndef WL_LOG_COMPILE_MAX_LEVEL
# ifdef NDEBUG
#  define WL_LOG_COMPILE_MAX_LEVEL 1 /* ERROR */
# else
#  define WL_LOG_COMPILE_MAX_LEVEL 5 /* TRACE */
# endif
#endif

typedef enum {
    WL_LOG_NONE  = 0,
    WL_LOG_ERROR = 1,
    WL_LOG_WARN  = 2,
    WL_LOG_INFO  = 3,
    WL_LOG_DEBUG = 4,
    WL_LOG_TRACE = 5,
    WL_LOG_LEVEL_MAX = WL_LOG_TRACE
} wl_log_level_t;

/* Closed enum for v1. Adding a section requires a recompile by design. */
typedef enum {
    WL_LOG_SEC_GENERAL = 0,
    WL_LOG_SEC_JOIN,
    WL_LOG_SEC_CONSOLIDATION,
    WL_LOG_SEC_ARRANGEMENT,
    WL_LOG_SEC_EVAL,
    WL_LOG_SEC_SESSION,
    WL_LOG_SEC_IO,
    WL_LOG_SEC_PARSER,
    WL_LOG_SEC_PLUGIN,
    WL_LOG_SEC__COUNT
} wl_log_section_t;

/* Cacheline-aligned; bilateral 128B pads in log.c prevent false sharing
 * with neighbor globals on both 64B (x86) and 128B (Apple/ARM) cachelines. */
extern alignas(64) uint8_t wl_log_thresholds[WL_LOG_SEC__COUNT];

int  wl_log_init(void);
void wl_log_shutdown(void);

/* Pure parser - does not read the environment. NULL spec treated as empty.
 * Returns 0 on success (thresholds_out populated); -1 on malformed token
 * (thresholds_out zeroed). Unknown sections are silently skipped. */
int  wl_log_parse_spec(const char *spec,
    uint8_t thresholds_out[WL_LOG_SEC__COUNT]);

const char        *wl_log_section_name(wl_log_section_t sec);
wl_log_section_t   wl_log_section_from_name(const char *name);

void wl_log_emit(wl_log_section_t sec, wl_log_level_t lvl,
    const char *file, int line, const char *fmt, ...)
__attribute__((format(printf, 5, 6)));

/* Compile-erasure sentinel. Stripped from libwirelog when
 * -Dwirelog_log_max_level=error. Verified by scripts/ci/check-log-erasure.sh.
 * DO NOT MODIFY: the script depends on this sentinel's TRACE-level shape. */
void wl_log_erasure_sentinel(void);

/* End-to-end demo helper used by tests/test_log_integration.c. Emits a single
 * TRACE-level line on SEC_JOIN. Not called from any production code path. */
void wl_log_demo_join(void);

/*
 * EXACT macro expansion. The compile-time guard `(LVL) <= WL_LOG_COMPILE_MAX_LEVEL`
 * is a comparison of two integer constant expressions; when it folds to false,
 * the && short-circuits and __VA_ARGS__ is unevaluated (C11 6.5.13). In release
 * builds with the ceiling set to ERROR, calls at TRACE/DEBUG/INFO/WARN expand to
 * dead code and contribute zero .text bytes and zero argument evaluation.
 */
#define WL_LOG(SEC, LVL, ...)                                                 \
        do {                                                                      \
            if ((LVL) <= WL_LOG_COMPILE_MAX_LEVEL                                 \
                && __builtin_expect((LVL) <= wl_log_thresholds[SEC], 0)) {        \
                wl_log_emit((SEC), (LVL), __FILE__, __LINE__, __VA_ARGS__);       \
            }                                                                     \
        } while (0)

#endif /* WIRELOG_UTIL_LOG_H */
