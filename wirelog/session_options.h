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

typedef struct wl_evaluation_control wl_evaluation_control_t;

#define WL_SESSION_OPTIONS_VERSION UINT32_C(5)

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
 *
 * Version 3 (#1819) adds a retained evaluation-control reference. The common
 * wrapper claims one logical owner; workers only borrow it. Supported
 * nonrecursive controlled STEP plans use bounded resumable preflight;
 * controlled SNAPSHOT and unsupported STEP routes may still return ENOTSUP.
 * Version 2 prefixes remain accepted, with no evaluation control.
 *
 * Version 4 (#1372) adds an optional numeric spill store. A NULL directory
 * with zero limits disables it; otherwise the directory must be nonempty,
 * quota positive, and max_row_bytes at least 32. The directory is copied.
 * This is internal opt-in plumbing, not automatic evaluator spilling.
 *
 * Version 5 enables eligible serial ordinary numeric JOINs over spilled
 * snapshots when both spill_join limits are positive. Both zero disables
 * routing. A configured store is required; evaluation control is incompatible.
 * batch_bytes bounds output lanes, index_bytes bounds the window index;
 * resident inputs, final output and reader/producer metadata are additional.
 * Unsupported shapes retain their existing paths. Version 2/3/4 prefixes
 * keep this route disabled regardless of trailing storage.
 */
typedef struct {
    uint32_t size;
    uint32_t version;
    void *windows_job_handle;
    wl_columnar_memory_governor_ref_t *memory_governor;
    wl_evaluation_control_t *evaluation_control;
    const char *spill_directory;
    uint64_t spill_quota_bytes;
    uint64_t spill_max_row_bytes;
    uint64_t spill_join_batch_bytes;
    uint64_t spill_join_index_bytes;
} wl_session_options_t;

void
wl_session_options_init(wl_session_options_t *options);

#endif /* WL_SESSION_OPTIONS_H */
