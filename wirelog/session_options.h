/*
 * session_options.h - internal session creation options
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * INTERNAL HEADER - not installed and not part of the public API.
 */

#ifndef WL_SESSION_OPTIONS_H
#define WL_SESSION_OPTIONS_H

#include <stdint.h>

typedef struct wl_columnar_memory_governor_ref
    wl_columnar_memory_governor_ref_t;

#define WL_SESSION_OPTIONS_VERSION UINT32_C(2)

/*
 * Options are valid only for the duration of session creation.  The Windows
 * Job Object handle is borrowed by the backend, queried synchronously, and
 * never stored or closed by Wirelog.  A void pointer keeps this header
 * portable and prevents windows.h from reaching installed headers.
 *
 * Version 2 (#1473) adds @memory_governor: a caller-retained admission
 * governor the session uses instead of resolving one from the environment
 * and host limits.  The session retains its own reference and releases it
 * on destroy; the caller keeps ownership of the reference it passed.  The
 * program-owned intern table attached at creation (#1431) also retains the
 * reference until the program is freed, so the governor can outlive the
 * session.  When it is set, @windows_job_handle and WIRELOG_MEMORY_BUDGET
 * are ignored.
 */
typedef struct {
    uint32_t size;
    uint32_t version;
    void *windows_job_handle;
    wl_columnar_memory_governor_ref_t *memory_governor;
} wl_session_options_t;

void
wl_session_options_init(wl_session_options_t *options);

#endif /* WL_SESSION_OPTIONS_H */
