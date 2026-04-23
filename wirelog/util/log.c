/*
 * wirelog/util/log.c - wirelog Structured Logger: state, parser, init
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "wirelog/util/log.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Bilateral cacheline isolation. 128B pads on both sides cover 64B (x86) and
 * 128B (Apple/ARM) cachelines regardless of which edge a neighbor global lands
 * on. Total BSS overhead: 256 bytes.
 */
WL_LOG_ATTR_USED WL_LOG_ALIGN64 static char wl_log_pad_lead_[128];
WL_LOG_ALIGN64 uint8_t wl_log_thresholds[WL_LOG_SEC__COUNT];
WL_LOG_ATTR_USED WL_LOG_ALIGN64 static char wl_log_pad_trail_[128];

/* Cached sink set once by wl_log_init(). Reader access is not synchronized;
 * init must run before any WL_LOG call. */
static FILE *wl_log_sink_ = NULL;
static int wl_log_sink_is_own_ = 0;   /* 1 => we opened it and must fclose on shutdown */

static const char *const wl_log_section_names_[WL_LOG_SEC__COUNT] = {
    [WL_LOG_SEC_GENERAL] = "GENERAL",
    [WL_LOG_SEC_JOIN] = "JOIN",
    [WL_LOG_SEC_CONSOLIDATION] = "CONSOLIDATION",
    [WL_LOG_SEC_ARRANGEMENT] = "ARRANGEMENT",
    [WL_LOG_SEC_EVAL] = "EVAL",
    [WL_LOG_SEC_SESSION] = "SESSION",
    [WL_LOG_SEC_IO] = "IO",
    [WL_LOG_SEC_PARSER] = "PARSER",
    [WL_LOG_SEC_PLUGIN] = "PLUGIN",
    [WL_LOG_SEC_COMPOUND] = "COMPOUND",
};

const char *
wl_log_section_name(wl_log_section_t sec)
{
    if ((unsigned)sec >= (unsigned)WL_LOG_SEC__COUNT)
        return "?";
    return wl_log_section_names_[sec];
}

static int
ieq_(const char *a, const char *b, size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        unsigned char ca = (unsigned char)a[i];
        unsigned char cb = (unsigned char)b[i];
        if (tolower(ca) != tolower(cb))
            return 0;
    }
    return 1;
}

wl_log_section_t
wl_log_section_from_name(const char *name)
{
    if (!name || !*name)
        return WL_LOG_SEC__COUNT;
    size_t n = strlen(name);
    for (unsigned i = 0; i < (unsigned)WL_LOG_SEC__COUNT; ++i) {
        const char *k = wl_log_section_names_[i];
        if (strlen(k) == n && ieq_(k, name, n))
            return (wl_log_section_t)i;
    }
    return WL_LOG_SEC__COUNT;
}

/* Trim leading/trailing ASCII whitespace. Returns length of trimmed span.
 * Writes begin index via *out_begin. */
static size_t
trim_(const char *s, size_t len, size_t *out_begin)
{
    size_t b = 0, e = len;
    while (b < e && isspace((unsigned char)s[b])) ++b;
    while (e > b && isspace((unsigned char)s[e - 1])) --e;
    *out_begin = b;
    return e - b;
}

int
wl_log_parse_spec(const char *spec, uint8_t out[WL_LOG_SEC__COUNT])
{
    memset(out, 0, WL_LOG_SEC__COUNT);
    if (!spec || !*spec)
        return 0;

    const char *p = spec;
    while (*p) {
        /* Extract comma-delimited token. */
        const char *comma = strchr(p, ',');
        size_t tok_len = comma ? (size_t)(comma - p) : strlen(p);

        size_t tb;
        size_t tl = trim_(p, tok_len, &tb);
        if (tl == 0) {
            /* Empty token (e.g. leading/trailing/double comma) -> malformed. */
            memset(out, 0, WL_LOG_SEC__COUNT);
            return -1;
        }

        const char *tok = p + tb;
        const char *colon = memchr(tok, ':', tl);
        if (!colon) {
            memset(out, 0, WL_LOG_SEC__COUNT);
            return -1;
        }

        /* name part */
        size_t nb;
        size_t nl = trim_(tok, (size_t)(colon - tok), &nb);
        if (nl == 0) {
            memset(out, 0, WL_LOG_SEC__COUNT);
            return -1;
        }
        const char *name = tok + nb;

        /* level part */
        size_t vb;
        size_t vl = trim_(colon + 1, tl - (size_t)(colon - tok) - 1, &vb);
        if (vl == 0) {
            memset(out, 0, WL_LOG_SEC__COUNT);
            return -1;
        }
        const char *vstr = colon + 1 + vb;
        if (vl != 1 || vstr[0] < '0' || vstr[0] > '9') {
            /* Only single-digit levels 0..5 are valid. */
            memset(out, 0, WL_LOG_SEC__COUNT);
            return -1;
        }
        int lvl = vstr[0] - '0';
        if (lvl > WL_LOG_LEVEL_MAX) {
            memset(out, 0, WL_LOG_SEC__COUNT);
            return -1;
        }

        /* Apply: wildcard sets all sections, otherwise single section (last wins).
         * Unknown section names are silently skipped per spec. */
        if (nl == 1 && name[0] == '*') {
            for (unsigned i = 0; i < (unsigned)WL_LOG_SEC__COUNT; ++i)
                out[i] = (uint8_t)lvl;
        } else {
            /* Copy name into a small buffer so we can NUL-terminate for lookup. */
            char nbuf[32];
            if (nl >= sizeof(nbuf)) {
                /* Name too long: treat as unknown (silent skip). */
            } else {
                memcpy(nbuf, name, nl);
                nbuf[nl] = '\0';
                wl_log_section_t sec = wl_log_section_from_name(nbuf);
                if (sec != WL_LOG_SEC__COUNT)
                    out[sec] = (uint8_t)lvl;
            }
        }

        if (!comma)
            break;
        p = comma + 1;
    }
    return 0;
}

static void
apply_legacy_shim_(void)
{
    /* Phase 1: legacy compatibility for #277's WL_DEBUG_JOIN / WL_CONSOLIDATION_LOG.
     * Presence check (any value, including "0") enables TRACE on the section.
     * WL_LOG parser output overrides this (runs after in wl_log_init). */
    if (getenv("WL_DEBUG_JOIN") != NULL)
        wl_log_thresholds[WL_LOG_SEC_JOIN] = (uint8_t)WL_LOG_TRACE;
    if (getenv("WL_CONSOLIDATION_LOG") != NULL)
        wl_log_thresholds[WL_LOG_SEC_CONSOLIDATION] = (uint8_t)WL_LOG_TRACE;
}

int
wl_log_init(void)
{
    /* Idempotent: reset state on re-init. */
    if (wl_log_sink_is_own_ && wl_log_sink_ && wl_log_sink_ != stderr) {
        fclose(wl_log_sink_);
    }
    wl_log_sink_ = NULL;
    wl_log_sink_is_own_ = 0;
    memset(wl_log_thresholds, 0, WL_LOG_SEC__COUNT);

    /* Sink selection. */
    const char *path = getenv("WL_LOG_FILE");
    if (path && *path) {
        FILE *f = fopen(path, "a");
        if (f) {
            wl_log_sink_ = f;
            wl_log_sink_is_own_ = 1;
        } else {
            fprintf(stderr,
                "wirelog: cannot open WL_LOG_FILE=%s: %s; falling back to stderr\n",
                path, strerror(errno));
        }
    }
    if (!wl_log_sink_)
        wl_log_sink_ = stderr;

    /* Legacy shim applied first; WL_LOG spec overrides. */
    apply_legacy_shim_();

    /* WL_LOG spec. On malformed input the diagnostic fires once per process
     * (static gate) and legacy-shim thresholds are retained — the documented
     * precedence is "legacy shim first, WL_LOG overrides"; a parse failure
     * means WL_LOG did not override anything, which is the least-surprising
     * behavior for a user who typo'd WL_LOG but set a legacy flag. */
    const char *spec = getenv("WL_LOG");
    if (spec && *spec) {
        uint8_t parsed[WL_LOG_SEC__COUNT];
        if (wl_log_parse_spec(spec, parsed) == 0) {
            memcpy(wl_log_thresholds, parsed, WL_LOG_SEC__COUNT);
        } else {
            static int warned_malformed_ = 0;
            if (!warned_malformed_) {
                warned_malformed_ = 1;
                fprintf(stderr, "wirelog: malformed WL_LOG spec: %s\n", spec);
            }
        }
    }
    return 0;
}

void
wl_log_shutdown(void)
{
    if (wl_log_sink_is_own_ && wl_log_sink_ && wl_log_sink_ != stderr) {
        fclose(wl_log_sink_);
    }
    wl_log_sink_ = NULL;
    wl_log_sink_is_own_ = 0;
    memset(wl_log_thresholds, 0, WL_LOG_SEC__COUNT);
}

/* Private accessor exposed to log_emit.c (prototype declared extern in that
 * TU). Keeps the sink FILE* out of the public header; callers outside the
 * logger module must not use it. */
FILE *
wl_log_sink_get_(void)
{
    return wl_log_sink_ ? wl_log_sink_ : stderr;
}

/*
 * Compile-erasure sentinel. MUST call WL_LOG at TRACE level so that
 * scripts/ci/check-log-erasure.sh can assert its format string is absent
 * from .rodata when -Dwirelog_log_max_level=error. Not called from any
 * hot path; intended to exist solely as a build-property probe.
 * DO NOT MODIFY without updating the script's sentinel list.
 */
void
wl_log_erasure_sentinel(void)
{
    WL_LOG(WL_LOG_SEC_GENERAL, WL_LOG_TRACE,
        "wl_log_erasure_sentinel_trace %d", 0);
}

/*
 * End-to-end demo helper. Drives the full env-parse -> threshold -> WL_LOG
 * -> wl_log_emit -> fwrite path. tests/test_log_integration.c asserts the
 * output matches "[TRACE][JOIN] <file>:<line>: hello 42".
 */
void
wl_log_demo_join(void)
{
    WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_TRACE, "hello %d", 42);
}
