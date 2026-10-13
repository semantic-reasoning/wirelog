/*
 * columnar/memory_governor.c - managed session memory foundation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "columnar/memory_governor.h"

#include <errno.h>
#include <string.h>
#include <stdlib.h>

#ifndef _WIN32
#include <errno.h>
#include <stdio.h>
#include <sys/resource.h>
#include <unistd.h>
#else
#include <windows.h>
#endif

#include <limits.h>

#ifdef WL_TEST_RELATION_RESIZE_HOOK
static bool wl_columnar_memory_governor_test_refuse_release;

void
wl_columnar_memory_governor_test_refuse_next_release(void)
{
    wl_columnar_memory_governor_test_refuse_release = true;
}
#endif

#define WL_MEMORY_PATH_MAX 4096

#ifdef WL_COLUMNAR_MEMORY_TEST_HOOKS
#define WL_MEMORY_TEST_POINT(token, phase) wl_columnar_memory_test_hook(token, \
            phase)
#else
#define WL_MEMORY_TEST_POINT(token, phase) ((void)0)
#endif

static uint64_t
headroom_for(uint64_t budget)
{
    uint64_t headroom = budget / 20;
    if (headroom > WL_COLUMNAR_MEMORY_HEADROOM_CAP)
        headroom = WL_COLUMNAR_MEMORY_HEADROOM_CAP;
    if (headroom > budget / 2)
        headroom = budget / 2;
    return headroom;
}

static bool
finite_limit(uint64_t value)
{
    return value != 0 && value != UINT64_MAX;
}

static bool
finite_source_limit(wl_columnar_memory_source_t source, uint64_t value)
{
    if (!finite_limit(value))
        return false;
    return source != WL_COLUMNAR_MEMORY_SOURCE_CGROUP_V1
           || value < (UINT64_C(1) << 62);
}

struct wl_columnar_memory_governor_ref {
    wl_columnar_memory_governor_t governor;
    wl_atomic_u64 references;
};

typedef enum {
    WL_MEMORY_PARSE_INVALID,
    WL_MEMORY_PARSE_VALID,
    WL_MEMORY_PARSE_OVERFLOW,
} wl_memory_parse_result_t;

static void
set_resolution(wl_columnar_memory_resolution_t *out, uint64_t budget,
    wl_columnar_memory_mode_t mode, wl_columnar_memory_source_t source)
{
    out->budget_bytes = budget;
    out->headroom_bytes = mode == WL_COLUMNAR_MEMORY_MODE_ENFORCING
        ? headroom_for(budget) : 0;
    out->usable_bytes = mode == WL_COLUMNAR_MEMORY_MODE_ENFORCING
        ? budget - out->headroom_bytes : UINT64_MAX;
    out->mode = mode;
    out->source = source;
    out->status = WL_COLUMNAR_MEMORY_OK;
}

const char *
wl_columnar_memory_source_name(wl_columnar_memory_source_t source)
{
    switch (source) {
    case WL_COLUMNAR_MEMORY_SOURCE_ENV: return "environment";
    case WL_COLUMNAR_MEMORY_SOURCE_CGROUP_V2: return "cgroup-v2";
    case WL_COLUMNAR_MEMORY_SOURCE_CGROUP_V1: return "cgroup-v1";
    case WL_COLUMNAR_MEMORY_SOURCE_RLIMIT_AS: return "rlimit-as";
    case WL_COLUMNAR_MEMORY_SOURCE_WINDOWS_JOB: return "windows-job";
    case WL_COLUMNAR_MEMORY_SOURCE_NONE: return "none";
    }
    return "unknown";
}

static wl_memory_parse_result_t
parse_decimal_bytes(const char *text, uint64_t *out)
{
    uint64_t value = 0;
    const char *p;

    if (!text || !text[0] || !out)
        return WL_MEMORY_PARSE_INVALID;
    for (p = text; *p; p++) {
        uint64_t digit;
        if (*p < '0' || *p > '9')
            return WL_MEMORY_PARSE_INVALID;
        digit = (uint64_t)(*p - '0');
        if (value > (UINT64_MAX - digit) / 10)
            return WL_MEMORY_PARSE_OVERFLOW;
        value = value * 10 + digit;
    }
    *out = value;
    return WL_MEMORY_PARSE_VALID;
}

#ifndef _WIN32
static bool
read_limit_file(const char *path, bool cgroup_v1, uint64_t *out)
{
    FILE *file;
    char value[128];
    char *end;
    unsigned long long parsed;

    if (!path || !out)
        return false;
    file = fopen(path, "r");
    if (!file)
        return false;
    if (!fgets(value, sizeof(value), file)) {
        fclose(file);
        return false;
    }
    fclose(file);
    if (strncmp(value, "max", 3) == 0)
        return false;
    errno = 0;
    parsed = strtoull(value, &end, 10);
    if (errno == ERANGE || end == value)
        return false;
    while (*end == ' ' || *end == '\t' || *end == '\n')
        end++;
    if (*end != '\0')
        return false;
    *out = (uint64_t)parsed;
    if (cgroup_v1 && *out >= (UINT64_C(1) << 62))
        return false;
    return finite_limit(*out);
}

static bool
read_cgroup_path(const char *prefix, char *out, size_t out_size)
{
    FILE *file;
    char line[WL_MEMORY_PATH_MAX];
    size_t prefix_len = strlen(prefix);

    file = fopen("/proc/self/cgroup", "r");
    if (!file)
        return false;
    while (fgets(line, sizeof(line), file)) {
        char *separator = strstr(line, prefix);
        if (!separator)
            continue;
        separator += prefix_len;
        if (*separator == '\0' || *separator == '\n')
            continue;
        separator[strcspn(separator, "\n")] = '\0';
        if (separator[0] != '/')
            continue;
        if (strlen(separator) >= out_size) {
            fclose(file);
            return false;
        }
        memcpy(out, separator, strlen(separator) + 1);
        fclose(file);
        return true;
    }
    fclose(file);
    return false;
}

static void
parent_cgroup_path(char *path)
{
    char *slash;
    size_t len;

    if (!path || strcmp(path, "/") == 0)
        return;
    len = strlen(path);
    while (len > 1 && path[len - 1] == '/')
        path[--len] = '\0';
    slash = strrchr(path, '/');
    if (!slash || slash == path) {
        path[0] = '/';
        path[1] = '\0';
    } else
        *slash = '\0';
}

static bool
probe_cgroup_tree(const char *mount_root, const char *path,
    const char *max_name, const char *high_name, bool cgroup_v1,
    uint64_t *out)
{
    char current[WL_MEMORY_PATH_MAX];
    uint64_t best = UINT64_MAX;
    bool found = false;

    if (!mount_root || !path || !out || strlen(path) >= sizeof(current))
        return false;
    memcpy(current, path, strlen(path) + 1);
    for (;;) {
        char max_path[WL_MEMORY_PATH_MAX];
        char high_path[WL_MEMORY_PATH_MAX];
        uint64_t value;
        int written;

        written = snprintf(max_path, sizeof(max_path), "%s%s/%s",
                mount_root, current, max_name);
        if (written > 0 && (size_t)written < sizeof(max_path)
            && read_limit_file(max_path, cgroup_v1, &value)
            && value < best) {
            best = value;
            found = true;
        }
        if (high_name) {
            written = snprintf(high_path, sizeof(high_path), "%s%s/%s",
                    mount_root, current, high_name);
            if (written > 0 && (size_t)written < sizeof(high_path)
                && read_limit_file(high_path, cgroup_v1, &value)
                && value < best) {
                best = value;
                found = true;
            }
        }
        if (strcmp(current, "/") == 0)
            break;
        parent_cgroup_path(current);
    }
    if (found)
        *out = best;
    return found;
}
#endif

wl_columnar_memory_status_t
wl_columnar_memory_probe_sources(wl_columnar_memory_sources_t *sources)
{
    if (!sources)
        return WL_COLUMNAR_MEMORY_INVALID;
    memset(sources, 0, sizeof(*sources));
#ifndef _WIN32
    {
        char path[WL_MEMORY_PATH_MAX];
        uint64_t value;
        if (read_cgroup_path("0::", path, sizeof(path))
            && probe_cgroup_tree("/sys/fs/cgroup", path,
            "memory.max", "memory.high", false, &value)) {
            sources->cgroup_v2_available = true;
            sources->cgroup_v2_limit = value;
        }
        if (read_cgroup_path("memory:", path, sizeof(path))
            && probe_cgroup_tree("/sys/fs/cgroup/memory", path,
            "memory.limit_in_bytes", NULL, true, &value)) {
            sources->cgroup_v1_available = true;
            sources->cgroup_v1_limit = value;
        }
    }
    {
        struct rlimit limit;
        if (getrlimit(RLIMIT_AS, &limit) == 0
            && limit.rlim_cur != RLIM_INFINITY
            && limit.rlim_cur > 0
            && (uintmax_t)limit.rlim_cur <= UINT64_MAX) {
            sources->rlimit_as_available = true;
            sources->rlimit_as_limit = (uint64_t)limit.rlim_cur;
        }
    }
#else
    /*
     * A process can only query a Job Object through a job handle. The handle
     * is supplied by the host/job launcher; there is no portable way to
     * recover an unnamed current-job handle. The provider field remains
     * injectable for callers that own that handle and for deterministic tests.
     */
#endif
    return WL_COLUMNAR_MEMORY_OK;
}

wl_columnar_memory_status_t
wl_columnar_memory_probe_windows_job(void *job_handle,
    wl_columnar_memory_sources_t *sources)
{
    if (!sources)
        return WL_COLUMNAR_MEMORY_INVALID;
    sources->windows_job_available = false;
    sources->windows_job_limit = 0;
#ifdef _WIN32
    {
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION info;

        if (!job_handle)
            return WL_COLUMNAR_MEMORY_UNAVAILABLE;
        memset(&info, 0, sizeof(info));
        if (!QueryInformationJobObject((HANDLE)job_handle,
            JobObjectExtendedLimitInformation, &info, sizeof(info), NULL)
            || !(info.BasicLimitInformation.LimitFlags
            & JOB_OBJECT_LIMIT_PROCESS_MEMORY)
            || info.ProcessMemoryLimit == 0)
            return WL_COLUMNAR_MEMORY_UNAVAILABLE;
        sources->windows_job_available = true;
        sources->windows_job_limit = (uint64_t)info.ProcessMemoryLimit;
        return WL_COLUMNAR_MEMORY_OK;
    }
#else
    (void)job_handle;
    return WL_COLUMNAR_MEMORY_UNAVAILABLE;
#endif
}

void
wl_columnar_memory_reservation_init(
    wl_columnar_memory_reservation_t *reservation)
{
    if (!reservation)
        return;
    memset(reservation, 0, sizeof(*reservation));
    reservation->identity = reservation;
    atomic_store_explicit(&reservation->state,
        WL_COLUMNAR_MEMORY_RESERVATION_EMPTY, memory_order_relaxed);
}

wl_columnar_memory_status_t
wl_columnar_memory_resolve(const char *env_value,
    const wl_columnar_memory_sources_t *sources,
    wl_columnar_memory_resolution_t *out)
{
    wl_columnar_memory_sources_t detected;
    uint64_t value;
    uint64_t best = UINT64_MAX;
    wl_columnar_memory_source_t source = WL_COLUMNAR_MEMORY_SOURCE_NONE;

    if (!out)
        return WL_COLUMNAR_MEMORY_INVALID;
    memset(out, 0, sizeof(*out));
    out->status = WL_COLUMNAR_MEMORY_INVALID;

    if (env_value) {
        wl_memory_parse_result_t parsed = parse_decimal_bytes(env_value,
                &value);
        if (parsed == WL_MEMORY_PARSE_OVERFLOW) {
            out->status = WL_COLUMNAR_MEMORY_OVERFLOW;
            return WL_COLUMNAR_MEMORY_OVERFLOW;
        }
        if (parsed != WL_MEMORY_PARSE_VALID)
            return WL_COLUMNAR_MEMORY_INVALID;
        if (value < WL_COLUMNAR_MEMORY_MIN_BUDGET)
            return WL_COLUMNAR_MEMORY_INVALID;
        set_resolution(out, value, WL_COLUMNAR_MEMORY_MODE_ENFORCING,
            WL_COLUMNAR_MEMORY_SOURCE_ENV);
        return WL_COLUMNAR_MEMORY_OK;
    }

    if (!sources) {
        if (wl_columnar_memory_probe_sources(&detected)
            != WL_COLUMNAR_MEMORY_OK)
            return WL_COLUMNAR_MEMORY_UNAVAILABLE;
        sources = &detected;
    }
    if (sources->cgroup_v2_available
        && finite_source_limit(WL_COLUMNAR_MEMORY_SOURCE_CGROUP_V2,
        sources->cgroup_v2_limit)
        && sources->cgroup_v2_limit < best) {
        best = sources->cgroup_v2_limit;
        source = WL_COLUMNAR_MEMORY_SOURCE_CGROUP_V2;
    }
    if (sources->cgroup_v1_available
        && finite_source_limit(WL_COLUMNAR_MEMORY_SOURCE_CGROUP_V1,
        sources->cgroup_v1_limit)
        && sources->cgroup_v1_limit < best) {
        best = sources->cgroup_v1_limit;
        source = WL_COLUMNAR_MEMORY_SOURCE_CGROUP_V1;
    }
    if (sources->rlimit_as_available
        && finite_source_limit(WL_COLUMNAR_MEMORY_SOURCE_RLIMIT_AS,
        sources->rlimit_as_limit)
        && sources->rlimit_as_limit < best) {
        best = sources->rlimit_as_limit;
        source = WL_COLUMNAR_MEMORY_SOURCE_RLIMIT_AS;
    }
    if (sources->windows_job_available
        && finite_source_limit(WL_COLUMNAR_MEMORY_SOURCE_WINDOWS_JOB,
        sources->windows_job_limit)
        && sources->windows_job_limit < best) {
        best = sources->windows_job_limit;
        source = WL_COLUMNAR_MEMORY_SOURCE_WINDOWS_JOB;
    }
    if (source == WL_COLUMNAR_MEMORY_SOURCE_NONE) {
        set_resolution(out, 0, WL_COLUMNAR_MEMORY_MODE_ADVISORY,
            WL_COLUMNAR_MEMORY_SOURCE_NONE);
        return WL_COLUMNAR_MEMORY_OK;
    }
    if (best < WL_COLUMNAR_MEMORY_MIN_BUDGET) {
        out->status = WL_COLUMNAR_MEMORY_INVALID;
        return WL_COLUMNAR_MEMORY_INVALID;
    }
    set_resolution(out, best, WL_COLUMNAR_MEMORY_MODE_ENFORCING, source);
    return WL_COLUMNAR_MEMORY_OK;
}

wl_columnar_memory_status_t
wl_columnar_memory_governor_init(wl_columnar_memory_governor_t *governor,
    const wl_columnar_memory_resolution_t *resolution)
{
    if (!governor || !resolution
        || resolution->status != WL_COLUMNAR_MEMORY_OK)
        return WL_COLUMNAR_MEMORY_INVALID;
    memset(governor, 0, sizeof(*governor));
    governor->budget_bytes = resolution->budget_bytes;
    governor->headroom_bytes = resolution->headroom_bytes;
    governor->mode = resolution->mode;
    governor->source = resolution->source;
    atomic_store_explicit(&governor->usable_bytes, resolution->usable_bytes,
        memory_order_relaxed);
    atomic_store_explicit(&governor->reserved_bytes, 0, memory_order_relaxed);
    return WL_COLUMNAR_MEMORY_OK;
}

wl_columnar_memory_governor_ref_t *
wl_columnar_memory_governor_ref_create(
    const wl_columnar_memory_resolution_t *resolution)
{
    wl_columnar_memory_governor_ref_t *ref;

    if (!resolution || resolution->status != WL_COLUMNAR_MEMORY_OK)
        return NULL;
    ref = (wl_columnar_memory_governor_ref_t *)calloc(1, sizeof(*ref));
    if (!ref)
        return NULL;
    if (wl_columnar_memory_governor_init(&ref->governor, resolution)
        != WL_COLUMNAR_MEMORY_OK) {
        free(ref);
        return NULL;
    }
    atomic_store_explicit(&ref->references, 1, memory_order_relaxed);
    return ref;
}

void
wl_columnar_memory_governor_ref_retain(
    wl_columnar_memory_governor_ref_t *ref)
{
    if (ref)
        atomic_fetch_add_explicit(&ref->references, 1, memory_order_relaxed);
}

void
wl_columnar_memory_governor_ref_release(
    wl_columnar_memory_governor_ref_t *ref)
{
    if (ref
        && atomic_fetch_sub_explicit(&ref->references, 1,
        memory_order_release) == 1) {
        (void)atomic_load_explicit(&ref->references, memory_order_acquire);
        free(ref);
    }
}

wl_columnar_memory_governor_t *
wl_columnar_memory_governor_ref_get(
    wl_columnar_memory_governor_ref_t *ref)
{
    return ref ? &ref->governor : NULL;
}

bool
wl_columnar_memory_governor_ref_is_sole(
    const wl_columnar_memory_governor_ref_t *ref)
{
    /* Acquire pairs with the release decrement in ref_release: a reader
     * that observes the count at one sees every earlier release. */
    return ref
           && atomic_load_explicit(&ref->references, memory_order_acquire)
           == 1;
}

static bool
claim_reservation_state(wl_columnar_memory_reservation_t *reservation,
    uint64_t from, uint64_t to);

static bool
reservation_identity_valid(const wl_columnar_memory_reservation_t *reservation)
{
    return reservation && reservation->identity == reservation;
}

bool
wl_columnar_memory_reservation_move(
    wl_columnar_memory_reservation_t *destination,
    wl_columnar_memory_reservation_t *source)
{
    uint64_t destination_state;
    uint64_t state;

    if (!destination || !source || destination == source
        || !reservation_identity_valid(destination)
        || !reservation_identity_valid(source))
        return false;
    destination_state = atomic_load_explicit(&destination->state,
            memory_order_acquire);
    if (destination_state != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        && destination_state != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED)
        return false;
    state = atomic_load_explicit(&source->state, memory_order_acquire);
    if (state != WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        && state != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)
        return false;
    destination->governor = source->governor;
    destination->bytes = source->bytes;
    destination->transition_kind = source->transition_kind;
    atomic_store_explicit(&destination->owner_bits,
        atomic_load_explicit(&source->owner_bits, memory_order_relaxed),
        memory_order_relaxed);
    atomic_store_explicit(&destination->state, state, memory_order_release);
    destination->identity = destination;
    source->governor = NULL;
    source->bytes = 0;
    source->transition_kind = WL_COLUMNAR_MEMORY_TRANSITION_NONE;
    atomic_store_explicit(&source->owner_bits, 0, memory_order_relaxed);
    atomic_store_explicit(&source->state,
        WL_COLUMNAR_MEMORY_RESERVATION_EMPTY, memory_order_release);
    source->identity = source;
    return true;
}

bool
wl_columnar_memory_size_add(uint64_t left, uint64_t right, uint64_t *out)
{
    if (!out || left > UINT64_MAX - right)
        return false;
    *out = left + right;
    return true;
}

bool
wl_columnar_memory_size_mul(uint64_t left, uint64_t right, uint64_t *out)
{
    if (!out || (left != 0 && right > UINT64_MAX / left))
        return false;
    *out = left * right;
    return true;
}

/* Controlled forward work is separate from mandatory physical-credit cleanup.
 * Tokens are exclusively caller-owned; only governor totals may contend. */
static bool credit_reservation(wl_columnar_memory_governor_t *, uint64_t);

#if defined(WL_SESSION_TEST_HOOKS) || defined(WL_COLUMNAR_MEMORY_TEST_HOOKS)
void (*wl_columnar_memory_governor_test_work_checkpoint)(
    wl_evaluation_control_t *, uint64_t);
void (*wl_columnar_memory_governor_test_checkpoint)(wl_evaluation_control_t *,
    wl_columnar_memory_reservation_t *, unsigned);
void (*wl_columnar_memory_governor_test_before_total_cas)(
    wl_evaluation_control_t *,
    wl_columnar_memory_governor_t *, uint64_t, bool);
void (*wl_columnar_memory_governor_test_after_total_cas)(
    wl_evaluation_control_t *,
    wl_columnar_memory_governor_t *, bool, bool);
#define WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(c, r, p) do { \
            if (wl_columnar_memory_governor_test_checkpoint) \
            wl_columnar_memory_governor_test_checkpoint(c, r, p); \
} while (0)
#else
#define WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(c, r, p) ((void)0)
#endif

static int
wl_columnar_memory_governor_tail(wl_evaluation_control_t *control,
    uint64_t work)
{
#if defined(WL_SESSION_TEST_HOOKS) || defined(WL_COLUMNAR_MEMORY_TEST_HOOKS)
    if (wl_columnar_memory_governor_test_work_checkpoint)
        wl_columnar_memory_governor_test_work_checkpoint(control, work);
#else
    (void)work;
#endif
    return wl_evaluation_control_charge(control, 0);
}

static bool
wl_columnar_memory_governor_strong(wl_atomic_u64 *state, uint64_t *expected,
    uint64_t desired)
{
#ifdef _MSC_VER
    /* Interlocked-backed existing shim is strong despite its weak spelling. */
    return atomic_compare_exchange_weak_explicit(state, expected, desired,
               memory_order_acq_rel, memory_order_acquire);
#else
    return atomic_compare_exchange_strong_explicit(state, expected, desired,
               memory_order_acq_rel, memory_order_acquire);
#endif
}

/* One platform atomic observation and its return; MSVC must use Interlocked. */
static uint64_t
wl_columnar_memory_governor_growth_commit_read(wl_atomic_u64 *value)
{
#ifdef _MSC_VER
    return (uint64_t)atomic_fetch_add_explicit(value, UINT64_C(0),
               memory_order_acquire);
#else
    return atomic_load_explicit(value, memory_order_acquire);
#endif
}

/* Valid25: five initial predicates + dispatch6; current thread/equality/
 * negation/dispatch4; ten final predicates10; read2 twice4; final return1.
 * The portable maximum includes the cheaper Windows thread comparison.
 */
static bool
wl_columnar_memory_governor_growth_commit_valid(
    const wl_columnar_memory_governor_growth_commit_prepared_t *prepared,
    uint64_t state)
{
    if (!prepared || prepared->identity != prepared || !prepared->thread_valid
        || !prepared->token || !prepared->governor)
        return false;
#if defined(WL_HAVE_C11_THREADS)
    if (!thrd_equal(prepared->owner_thread, thrd_current()))
        return false;
#elif defined(_WIN32) || defined(_WIN64)
    if (prepared->owner_thread != GetCurrentThreadId())
        return false;
#else
    if (!pthread_equal(prepared->owner_thread, pthread_self()))
        return false;
#endif
    return prepared->token->identity == prepared->token
           && prepared->token->governor == prepared->governor
           && wl_columnar_memory_governor_growth_commit_read(
        &prepared->token->owner_bits) == prepared->owner_bits
           && wl_columnar_memory_governor_growth_commit_read(
        &prepared->token->state) == state
           && prepared->token->bytes == prepared->prior_bytes
           && prepared->token->replacement_bytes == prepared->additional_bytes
           && prepared->token->transition_kind
           == WL_COLUMNAR_MEMORY_TRANSITION_GROWTH
           && prepared->additional_bytes != 0
           && prepared->additional_bytes <= UINT64_MAX - prepared->prior_bytes
           && prepared->final_bytes
           == prepared->prior_bytes + prepared->additional_bytes;
}

enum {
    /* Finish44 = valid25 + four predicates/dispatch5 + getter4/local1
     * + two active predicates/dispatch3 + token stores4 + consume1 + return1.
     * Preparation184 = arguments28 + snapshots50 + claim37 + five checkpoint
     * ceilings13 each + READY stores/return4. These include explicit error
     * alternatives, not an assertion that all admitted operations execute.
     * Helper: getter4, read2, strong2; charge/poll scaffolding and test hooks
     * excluded. No engine-owned loop is excluded or hidden in a helper.
     */
    WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_FINISH_WORK = 44,
    WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_PREPARE_WORK = 184,
    WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_TOTAL_WORK = 228,
};

typedef char wl_columnar_memory_governor_growth_commit_quantum_bound[
        (WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_TOTAL_WORK
        <= WL_EVALUATION_CONTROL_QUANTUM) ? 1 : -1];

/* H9: getter4/local1, two id predicates/dispatch3, return1. Caller rc
 * assignment/predicate/dispatch/error alternative adds4, yielding ceiling13.
 * Callback first, original-id validation BEFORE poll; never debit a successor.
 */
static int
wl_columnar_memory_governor_growth_commit_checkpoint(
    wl_evaluation_control_t *control,
    wl_columnar_memory_reservation_t *token, uint64_t saved_id,
    unsigned phase, uint64_t interval)
{
#if defined(WL_SESSION_TEST_HOOKS) || defined(WL_COLUMNAR_MEMORY_TEST_HOOKS)
    WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token, phase);
    if (wl_columnar_memory_governor_test_work_checkpoint)
        wl_columnar_memory_governor_test_work_checkpoint(control, interval);
#else
    (void)token;
    (void)phase;
    (void)interval;
#endif
    uint64_t active = wl_evaluation_control_active_attempt_id(control);
    if (!active || active != saved_id)
        return EINVAL;
    return wl_evaluation_control_charge(control, 0);
}

int
wl_columnar_memory_governor_growth_commit_cancel(
    wl_columnar_memory_governor_growth_commit_prepared_t *prepared)
{
    if (!wl_columnar_memory_governor_growth_commit_valid(prepared,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING)
        || (prepared->phase != WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_CLAIMED
        && prepared->phase != WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_READY))
        return EINVAL;
    (void)atomic_exchange_explicit(&prepared->token->state,
        WL_COLUMNAR_MEMORY_RESERVATION_REPLACING, memory_order_release);
    prepared->phase = WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_CONSUMED;
    return 0;
}

int
wl_columnar_memory_governor_growth_commit_finish(
    wl_columnar_memory_governor_growth_commit_prepared_t *prepared)
{
    if (!wl_columnar_memory_governor_growth_commit_valid(prepared,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING)
        || prepared->phase != WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_READY
        || !prepared->control
        || prepared->prepaid_work
        != WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_FINISH_WORK)
        return EINVAL;
    uint64_t active =
        wl_evaluation_control_active_attempt_id(prepared->control);
    if (!active || active != prepared->active_attempt_id)
        return EINVAL;
    prepared->token->bytes = prepared->final_bytes;
    prepared->token->replacement_bytes = 0;
    prepared->token->transition_kind = WL_COLUMNAR_MEMORY_TRANSITION_NONE;
    (void)atomic_exchange_explicit(&prepared->token->state,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, memory_order_release);
    prepared->phase = WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_CONSUMED;
    return 0;
}

int
wl_columnar_memory_governor_growth_commit_prepare_controlled(
    wl_columnar_memory_reservation_t *token,
    wl_columnar_memory_governor_t *expected_governor,
    const void *expected_owner,
    wl_columnar_memory_governor_growth_commit_prepared_t *prepared, bool *ready,
    wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control,
            WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_TOTAL_WORK);
    if (rc)
        return rc;
    if (!token || !expected_governor || !expected_owner || !prepared || !ready
        || prepared->identity || prepared->token || prepared->governor
        || prepared->control || prepared->thread_valid || prepared->owner_bits
        || prepared->active_attempt_id || prepared->prior_bytes
        || prepared->additional_bytes || prepared->final_bytes
        || prepared->prepaid_work
        || prepared->phase != WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_INERT)
        return EINVAL;
    *ready = false;
    uint64_t saved_id = wl_evaluation_control_active_attempt_id(control);
    if (!saved_id)
        return EINVAL;
    rc = wl_columnar_memory_governor_growth_commit_checkpoint(control, token,
            saved_id,
            WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_ARGUMENTS, 41);
    if (rc)
        return rc;
    prepared->identity = prepared;
    prepared->token = token;
    prepared->governor = expected_governor;
    prepared->owner_bits = (uint64_t)(uintptr_t)expected_owner;
    prepared->control = control;
    prepared->active_attempt_id = saved_id;
#if defined(WL_HAVE_C11_THREADS)
    prepared->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    prepared->owner_thread = GetCurrentThreadId();
#else
    prepared->owner_thread = pthread_self();
#endif
    prepared->thread_valid = true;
    prepared->prior_bytes = token->bytes;
    prepared->additional_bytes = token->replacement_bytes;
    prepared->final_bytes = token->bytes + token->replacement_bytes;
    prepared->prepaid_work = 0;
    prepared->phase = WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_INERT;
    bool valid = wl_columnar_memory_governor_growth_commit_valid(prepared,
            WL_COLUMNAR_MEMORY_RESERVATION_REPLACING);
    rc = wl_columnar_memory_governor_growth_commit_checkpoint(control, token,
            saved_id,
            WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_SNAPSHOT, 63);
    if (!valid) {
        prepared->phase = WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_CONSUMED;
        return rc > 0 ? rc : 0;
    }
    if (rc)
        goto unclaimed;
    rc = wl_columnar_memory_governor_growth_commit_checkpoint(control, token,
            saved_id,
            WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_PRECLAIM, 13);
    if (rc)
        goto unclaimed;
    if (!wl_columnar_memory_governor_growth_commit_valid(prepared,
        WL_COLUMNAR_MEMORY_RESERVATION_REPLACING)) {
        rc = wl_columnar_memory_governor_growth_commit_checkpoint(control,
                token,
                saved_id,
                WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_REFUSED, 50);
        prepared->phase = WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_CONSUMED;
        return rc > 0 ? rc : 0;
    }
    uint64_t expected = WL_COLUMNAR_MEMORY_RESERVATION_REPLACING;
    if (!wl_columnar_memory_governor_strong(&token->state, &expected,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING)) {
        rc = wl_columnar_memory_governor_growth_commit_checkpoint(control,
                token,
                saved_id,
                WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_REFUSED, 50);
        prepared->phase = WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_CONSUMED;
        return rc > 0 ? rc : 0;
    }
    prepared->phase = WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_CLAIMED;
    rc = wl_columnar_memory_governor_growth_commit_checkpoint(control, token,
            saved_id,
            WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_CLAIMED, 50);
    if (rc)
        goto claimed;
    rc = wl_columnar_memory_governor_growth_commit_checkpoint(control, token,
            saved_id,
            WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_READY_CUTOFF, 13);
    if (rc)
        goto claimed;
    prepared->prepaid_work =
        WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_FINISH_WORK;
    prepared->phase = WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_READY;
    *ready = true;
    return 0;
claimed:
    if (wl_columnar_memory_governor_growth_commit_cancel(prepared) != 0)
        abort();
    return rc;
unclaimed:
    prepared->phase = WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_CONSUMED;
    return rc;
}

typedef struct {
    wl_columnar_memory_governor_t *governor;
    uint64_t bytes;
    uint64_t replacement_bytes;
    uint64_t owner;
    uint8_t kind;
} wl_columnar_memory_governor_snapshot_t;

static void
wl_columnar_memory_governor_snapshot(wl_columnar_memory_reservation_t *token,
    wl_columnar_memory_governor_snapshot_t *snapshot)
{
    snapshot->governor = token->governor;
    snapshot->bytes = token->bytes;
    snapshot->replacement_bytes = token->replacement_bytes;
    snapshot->owner = atomic_load_explicit(&token->owner_bits,
            memory_order_relaxed);
    snapshot->kind = token->transition_kind;
}

/* Mandatory restoration under exclusive token claim, never work/credit refund. */
static int
wl_columnar_memory_governor_restore(wl_columnar_memory_reservation_t *token,
    const wl_columnar_memory_governor_snapshot_t *snapshot, uint64_t state,
    wl_columnar_memory_governor_t *credited_governor, uint64_t credited, int rc)
{
    if (credited && !credit_reservation(credited_governor, credited)) {
        /* Impossible with valid exclusive token authority: do not erase the
        * outstanding claim or invent restored credit before termination. */
        abort();
    }
    token->governor = snapshot->governor;
    token->bytes = snapshot->bytes;
    token->replacement_bytes = snapshot->replacement_bytes;
    token->transition_kind = snapshot->kind;
    atomic_store_explicit(&token->owner_bits, snapshot->owner,
        memory_order_relaxed);
    atomic_store_explicit(&token->state, state, memory_order_release);
    return rc;
}

/* One charged round includes limit arithmetic and at most one strong total CAS.
 * Typed DENIED/OVERFLOW outcomes retain provenance even if the tail latches stop. */
static int
wl_columnar_memory_governor_admit(wl_columnar_memory_governor_t *governor,
    uint64_t bytes, wl_columnar_memory_reservation_t *token,
    wl_columnar_memory_admission_status_t *status, bool *credited,
    wl_evaluation_control_t *control)
{
    uint64_t old = 0;
    bool first = true;
    *credited = false;
    for (;;) {
        int rc = wl_evaluation_control_charge(control, 1);
        if (rc)
            return rc;
        if (first) {
            old = atomic_load_explicit(&governor->reserved_bytes,
                    memory_order_relaxed);
            first = false;
        }
        uint64_t limit = atomic_load_explicit(&governor->usable_bytes,
                memory_order_relaxed);
        bool overflow = bytes > UINT64_MAX - old;
        if (!overflow && old + bytes > limit) {
            *status = WL_COLUMNAR_MEMORY_ADMISSION_DENIED;
            WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
                WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT);
            rc = wl_columnar_memory_governor_tail(control, 1);
            return rc > 0 ? rc : 0;
        }
        uint64_t desired = overflow ? old : old + bytes;
#if defined(WL_SESSION_TEST_HOOKS) || defined(WL_COLUMNAR_MEMORY_TEST_HOOKS)
        if (wl_columnar_memory_governor_test_before_total_cas)
            wl_columnar_memory_governor_test_before_total_cas(control, governor,
                old, overflow);
#endif
        bool admitted =
            wl_columnar_memory_governor_strong(&governor->reserved_bytes,
                &old, desired);
        if (admitted && !overflow)
            *credited = true;
#if defined(WL_SESSION_TEST_HOOKS) || defined(WL_COLUMNAR_MEMORY_TEST_HOOKS)
        if (wl_columnar_memory_governor_test_after_total_cas)
            wl_columnar_memory_governor_test_after_total_cas(control, governor,
                admitted, overflow);
#endif
        if (admitted && overflow)
            WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
                WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT);
        rc = wl_columnar_memory_governor_tail(control, 1);
        if (admitted && overflow) {
            *status = WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW;
            return rc > 0 ? rc : 0;
        }
        if (rc)
            return rc;
        if (admitted) {
            *status = governor->mode == WL_COLUMNAR_MEMORY_MODE_ADVISORY
                ? WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY :
                WL_COLUMNAR_MEMORY_ADMISSION_OK;
            return 0;
        }
    }
}

int
wl_columnar_memory_governor_reserve_checked_controlled(
    wl_columnar_memory_governor_t *governor, uint64_t bytes,
    wl_columnar_memory_reservation_t *token,
    wl_columnar_memory_admission_status_t *out,
    wl_evaluation_control_t *control)
{
    if (!out)
        return EINVAL;
    if (!control) {
        *out = wl_columnar_memory_reserve_checked(governor, bytes, token);
        return 0;
    }
    int rc = wl_evaluation_control_charge(control, 3);
    if (rc)
        return rc;
    if (!governor || !reservation_identity_valid(token) || !bytes) {
        *out = WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
        WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
            WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT);
        rc = wl_columnar_memory_governor_tail(control, 3);
        return rc > 0 ? rc : 0;
    }
    WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
        WL_COLUMNAR_MEMORY_GOVERNOR_VALIDATED);
    rc = wl_columnar_memory_governor_tail(control, 3);
    if (rc)
        return rc;
    uint64_t from = WL_COLUMNAR_MEMORY_RESERVATION_EMPTY;
    for (unsigned attempt = 0; attempt < 2; attempt++) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc)
            return rc;
        uint64_t expected = from;
        bool claimed = wl_columnar_memory_governor_strong(&token->state,
                &expected,
                WL_COLUMNAR_MEMORY_RESERVATION_CLAIMED);
#if defined(WL_SESSION_TEST_HOOKS) || defined(WL_COLUMNAR_MEMORY_TEST_HOOKS)
        if (claimed)
            WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
                WL_COLUMNAR_MEMORY_GOVERNOR_CLAIMED);
        else if (attempt == 1 ||
            expected != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED)
            WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
                WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT);
#endif
        rc = wl_columnar_memory_governor_tail(control, 1);
        if (claimed) {
            if (rc) {
                atomic_store_explicit(&token->state, from,
                    memory_order_release);
                return rc;
            }
            break;
        }
        if (attempt == 1 ||
            expected != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED) {
            *out = WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
            return rc > 0 ? rc : 0;
        }
        if (rc)
            return rc;
        from = WL_COLUMNAR_MEMORY_RESERVATION_RELEASED;
    }
    wl_columnar_memory_governor_snapshot_t prior;
    rc = wl_evaluation_control_charge(control, 5);
    if (rc) {
        atomic_store_explicit(&token->state, from, memory_order_release);
        return rc;
    }
    wl_columnar_memory_governor_snapshot(token, &prior);
    WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
        WL_COLUMNAR_MEMORY_GOVERNOR_SNAPSHOT);
    rc = wl_columnar_memory_governor_tail(control, 5);
    if (rc)
        return wl_columnar_memory_governor_restore(token, &prior, from, NULL, 0,
                   rc);
    rc = wl_evaluation_control_charge(control, 4);
    if (rc)
        return wl_columnar_memory_governor_restore(token, &prior, from, NULL, 0,
                   rc);
    token->governor = governor; token->bytes = bytes;
    token->transition_kind = WL_COLUMNAR_MEMORY_TRANSITION_NONE;
    atomic_store_explicit(&token->owner_bits, 0, memory_order_relaxed);
    WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
        WL_COLUMNAR_MEMORY_GOVERNOR_PREPARED);
    rc = wl_columnar_memory_governor_tail(control, 4);
    if (rc)
        return wl_columnar_memory_governor_restore(token, &prior, from, NULL, 0,
                   rc);
    bool credited;
    wl_columnar_memory_admission_status_t status;
    rc = wl_columnar_memory_governor_admit(governor, bytes, token, &status,
            &credited, control);
    if (rc)
        return wl_columnar_memory_governor_restore(token, &prior, from,
                   governor,
                   credited ? bytes : 0, rc);
    if (!credited) {
        atomic_store_explicit(&token->state,
            WL_COLUMNAR_MEMORY_RESERVATION_RELEASED,
            memory_order_release);
        *out = status;
        return 0;
    }
    WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
        WL_COLUMNAR_MEMORY_GOVERNOR_ADMITTED);
    rc = wl_evaluation_control_charge(control, 1);
    if (rc)
        return wl_columnar_memory_governor_restore(token, &prior, from,
                   governor, bytes, rc);
    atomic_store_explicit(&token->state,
        WL_COLUMNAR_MEMORY_RESERVATION_RESERVED,
        memory_order_release);
    WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
        WL_COLUMNAR_MEMORY_GOVERNOR_RESERVED);
    rc = wl_columnar_memory_governor_tail(control, 1);
    if (rc)
        return wl_columnar_memory_governor_restore(token, &prior, from,
                   governor, bytes, rc);
    *out = status;
    return 0;
}

int
wl_columnar_memory_governor_reserve_growth_controlled(
    wl_columnar_memory_governor_t *governor, uint64_t old_bytes,
    uint64_t new_bytes,
    wl_columnar_memory_reservation_t *token,
    wl_columnar_memory_admission_status_t *out,
    wl_evaluation_control_t *control)
{
    if (!out)
        return EINVAL;
    if (!control) {
        *out = wl_columnar_memory_reserve_growth(governor, old_bytes, new_bytes,
                token);
        return 0;
    }
    int rc = wl_evaluation_control_charge(control, 4);
    if (rc)
        return rc;
    if (!governor || !reservation_identity_valid(token) ||
        new_bytes < old_bytes) {
        *out = WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
        WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
            WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT);
        rc = wl_columnar_memory_governor_tail(control, 4);
        return rc > 0 ? rc : 0;
    }
    uint64_t state = atomic_load_explicit(&token->state, memory_order_acquire);
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        || state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || (new_bytes == old_bytes &&
        state != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        && state != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED)) {
        *out = WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
        WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
            WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT);
        rc = wl_columnar_memory_governor_tail(control, 4);
        return rc > 0 ? rc : 0;
    }
    rc = wl_columnar_memory_governor_tail(control, 4);
    return rc ? rc : wl_columnar_memory_governor_reserve_checked_controlled(
        governor, new_bytes, token, out, control);
}

int
wl_columnar_memory_governor_begin_growth_controlled(
    wl_columnar_memory_reservation_t *token, uint64_t additional,
    wl_columnar_memory_admission_status_t *out,
    wl_evaluation_control_t *control)
{
    if (!out)
        return EINVAL;
    if (!control) {
        *out = wl_columnar_memory_begin_growth(token, additional);
        return 0;
    }
    int rc = wl_evaluation_control_charge(control, 2);
    if (rc)
        return rc;
    if (!reservation_identity_valid(token) || !additional) {
        *out = WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
        WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
            WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT);
        rc = wl_columnar_memory_governor_tail(control, 2);
        return rc > 0 ? rc : 0;
    }
    rc = wl_columnar_memory_governor_tail(control, 2);
    if (rc)
        return rc;
    rc = wl_evaluation_control_charge(control, 1);
    if (rc)
        return rc;
    uint64_t expected = WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED;
    bool claimed = wl_columnar_memory_governor_strong(&token->state, &expected,
            WL_COLUMNAR_MEMORY_RESERVATION_REPLACEMENT_ADMITTING);
    if (!claimed) {
        *out = WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
        WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
            WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT);
        rc = wl_columnar_memory_governor_tail(control, 1);
        return rc > 0 ? rc : 0;
    }
    WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
        WL_COLUMNAR_MEMORY_GOVERNOR_CLAIMED);
    rc = wl_columnar_memory_governor_tail(control, 1);
    if (rc) {
        atomic_store_explicit(&token->state,
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED,
            memory_order_release);
        return rc;
    }
    wl_columnar_memory_governor_snapshot_t prior;
    rc = wl_evaluation_control_charge(control, 5);
    if (rc) {
        atomic_store_explicit(&token->state,
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED,
            memory_order_release);
        return rc;
    }
    wl_columnar_memory_governor_snapshot(token, &prior);
    WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
        WL_COLUMNAR_MEMORY_GOVERNOR_SNAPSHOT);
    rc = wl_columnar_memory_governor_tail(control, 5);
    if (rc)
        return wl_columnar_memory_governor_restore(token, &prior,
                   WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, NULL, 0, rc);
    rc = wl_evaluation_control_charge(control, 3);
    if (rc)
        return wl_columnar_memory_governor_restore(token, &prior,
                   WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, NULL, 0, rc);
    wl_columnar_memory_admission_status_t status =
        WL_COLUMNAR_MEMORY_ADMISSION_OK;
    if (!token->governor ||
        token->transition_kind != WL_COLUMNAR_MEMORY_TRANSITION_NONE)
        status = WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
    else if (additional > UINT64_MAX - token->bytes)
        status = WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW;
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK)
        WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
            WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT);
    rc = wl_columnar_memory_governor_tail(control, 3);
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK) {
        (void)wl_columnar_memory_governor_restore(token, &prior,
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, NULL, 0, 0);
        *out = status;
        return rc > 0 ? rc : 0;
    }
    if (rc)
        return wl_columnar_memory_governor_restore(token, &prior,
                   WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, NULL, 0, rc);
    bool credited;
    rc = wl_columnar_memory_governor_admit(token->governor, additional, token,
            &status,
            &credited, control);
    if (rc)
        return wl_columnar_memory_governor_restore(token, &prior,
                   WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, prior.governor,
                   credited ? additional : 0, rc);
    if (!credited) {
        (void)wl_columnar_memory_governor_restore(token, &prior,
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, NULL, 0, 0);
        *out = status;
        return 0;
    }
    WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
        WL_COLUMNAR_MEMORY_GOVERNOR_ADMITTED);
    rc = wl_evaluation_control_charge(control, 3);
    if (rc)
        return wl_columnar_memory_governor_restore(token, &prior,
                   WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, prior.governor,
                   additional, rc);
    token->replacement_bytes = additional;
    token->transition_kind = WL_COLUMNAR_MEMORY_TRANSITION_GROWTH;
    atomic_store_explicit(&token->state,
        WL_COLUMNAR_MEMORY_RESERVATION_REPLACING,
        memory_order_release);
    WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
        WL_COLUMNAR_MEMORY_GOVERNOR_RESERVED);
    rc = wl_columnar_memory_governor_tail(control, 3);
    if (rc)
        return wl_columnar_memory_governor_restore(token, &prior,
                   WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, prior.governor,
                   additional, rc);
    *out = status;
    return 0;
}

static int
wl_columnar_memory_governor_commit_private(
    wl_columnar_memory_reservation_t *token,
    const void *owner, bool growth, bool *out, wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 1);
    if (rc)
        return rc;
    if (!reservation_identity_valid(token)) {
        *out = false;
        WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
            WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT);
        rc = wl_columnar_memory_governor_tail(control, 1);
        return rc > 0 ? rc : 0;
    }
    rc = wl_columnar_memory_governor_tail(control, 1);
    if (rc)
        return rc;
    uint64_t from = growth ? WL_COLUMNAR_MEMORY_RESERVATION_REPLACING
        : WL_COLUMNAR_MEMORY_RESERVATION_RESERVED;
    rc = wl_evaluation_control_charge(control, 1);
    if (rc)
        return rc;
    uint64_t expected = from;
    bool claimed = wl_columnar_memory_governor_strong(&token->state, &expected,
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING);
    if (!claimed) {
        *out = false;
        WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
            WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT);
        rc = wl_columnar_memory_governor_tail(control, 1);
        return rc > 0 ? rc : 0;
    }
    WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
        WL_COLUMNAR_MEMORY_GOVERNOR_COMMITTING);
    rc = wl_columnar_memory_governor_tail(control, 1);
    if (rc)
        goto restore;
    if (growth) {
        rc = wl_evaluation_control_charge(control, 3);
        if (rc)
            goto restore;
        bool valid = token->transition_kind ==
            WL_COLUMNAR_MEMORY_TRANSITION_GROWTH
            && token->replacement_bytes &&
            token->replacement_bytes <= UINT64_MAX - token->bytes;
        if (!valid)
            WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
                WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT);
        rc = wl_columnar_memory_governor_tail(control, 3);
        if (!valid) {
            atomic_store_explicit(&token->state, from, memory_order_release);
            *out = false;
            return rc > 0 ? rc : 0;
        }
        if (rc)
            goto restore;
    }
    rc = wl_evaluation_control_charge(control, growth ? 4 : 2);
    if (rc)
        goto restore;
    WL_COLUMNAR_MEMORY_GOVERNOR_TEST_POINT(control, token,
        WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF);
    rc = wl_columnar_memory_governor_tail(control, growth ? 4 : 2);
    if (rc)
        goto restore;
    /* Non-failing publication; no negative poll after COMMITTED. */
    if (growth) {
        token->bytes += token->replacement_bytes;
        token->replacement_bytes = 0;
        token->transition_kind = WL_COLUMNAR_MEMORY_TRANSITION_NONE;
    } else
        atomic_store_explicit(&token->owner_bits, (uint64_t)(uintptr_t)owner,
            memory_order_release);
    atomic_store_explicit(&token->state,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED,
        memory_order_release);
    *out = true;
    return 0;
restore:
    atomic_store_explicit(&token->state, from, memory_order_release);
    return rc;
}

int
wl_columnar_memory_governor_commit_controlled(
    wl_columnar_memory_reservation_t *token,
    const void *owner, bool *out, wl_evaluation_control_t *control)
{
    if (!out)
        return EINVAL;
    if (!control) {
        *out = wl_columnar_memory_commit(token, owner);
        return 0;
    }
    return wl_columnar_memory_governor_commit_private(token, owner, false, out,
               control);
}

int
wl_columnar_memory_governor_commit_growth_controlled(
    wl_columnar_memory_reservation_t *token,
    bool *out, wl_evaluation_control_t *control)
{
    if (!out)
        return EINVAL;
    if (!control) {
        *out = wl_columnar_memory_commit_growth(token);
        return 0;
    }
    return wl_columnar_memory_governor_commit_private(token, NULL, true, out,
               control);
}

static wl_columnar_memory_admission_status_t
reserve_internal(wl_columnar_memory_governor_t *governor,
    uint64_t bytes, wl_columnar_memory_reservation_t *reservation)
{
    uint64_t old;
    uint64_t expected;

    if (!governor || !reservation_identity_valid(reservation) || bytes == 0)
        return WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
    expected = WL_COLUMNAR_MEMORY_RESERVATION_EMPTY;
    if (!claim_reservation_state(reservation, expected,
        WL_COLUMNAR_MEMORY_RESERVATION_CLAIMED)) {
        expected = WL_COLUMNAR_MEMORY_RESERVATION_RELEASED;
        if (!claim_reservation_state(reservation, expected,
            WL_COLUMNAR_MEMORY_RESERVATION_CLAIMED))
            return WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
    }
    reservation->governor = governor;
    reservation->bytes = bytes;
    reservation->transition_kind = WL_COLUMNAR_MEMORY_TRANSITION_NONE;
    atomic_store_explicit(&reservation->owner_bits, 0, memory_order_relaxed);
    old = atomic_load_explicit(&governor->reserved_bytes, memory_order_relaxed);
    for (;;) {
        uint64_t limit = atomic_load_explicit(&governor->usable_bytes,
                memory_order_relaxed);
        uint64_t next;
        if (bytes > UINT64_MAX - old) {
            uint64_t observed = old;
            /* Linearize the overflow verdict with a no-op CAS.  If another
             * thread changes the total after the load, retry against its new
             * value instead of reporting an obsolete arithmetic state. */
            if (atomic_compare_exchange_weak_explicit(
                    &governor->reserved_bytes, &observed, old,
                    memory_order_acquire, memory_order_relaxed)) {
                atomic_store_explicit(&reservation->state,
                    WL_COLUMNAR_MEMORY_RESERVATION_RELEASED,
                    memory_order_release);
                return WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW;
            }
            old = observed;
            continue;
        }
        next = old + bytes;
        if (next > limit) {
            atomic_store_explicit(&reservation->state,
                WL_COLUMNAR_MEMORY_RESERVATION_RELEASED, memory_order_release);
            return WL_COLUMNAR_MEMORY_ADMISSION_DENIED;
        }
        if (atomic_compare_exchange_weak_explicit(
                &governor->reserved_bytes, &old, next,
                memory_order_relaxed, memory_order_relaxed))
            break;
    }
    atomic_store_explicit(&reservation->state,
        WL_COLUMNAR_MEMORY_RESERVATION_RESERVED, memory_order_release);
    return governor->mode == WL_COLUMNAR_MEMORY_MODE_ADVISORY
        ? WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY
        : WL_COLUMNAR_MEMORY_ADMISSION_OK;
}

bool
wl_columnar_memory_reserve(wl_columnar_memory_governor_t *governor,
    uint64_t bytes, wl_columnar_memory_reservation_t *reservation)
{
    wl_columnar_memory_admission_status_t status
        = reserve_internal(governor, bytes, reservation);
    return status == WL_COLUMNAR_MEMORY_ADMISSION_OK
           || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY;
}

wl_columnar_memory_admission_status_t
wl_columnar_memory_reserve_checked(wl_columnar_memory_governor_t *governor,
    uint64_t bytes, wl_columnar_memory_reservation_t *reservation)
{
    if (!governor || !reservation_identity_valid(reservation) || bytes == 0)
        return WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
    return reserve_internal(governor, bytes, reservation);
}

wl_columnar_memory_admission_status_t
wl_columnar_memory_reserve_growth(wl_columnar_memory_governor_t *governor,
    uint64_t old_bytes, uint64_t new_bytes,
    wl_columnar_memory_reservation_t *reservation)
{
    uint64_t state;

    if (!governor || !reservation_identity_valid(reservation)
        || new_bytes < old_bytes)
        return WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
    state = atomic_load_explicit(&reservation->state, memory_order_acquire);
    if (new_bytes == old_bytes) {
        if (state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
            || state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED) {
            return WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
        }
        if (state != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
            && state != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED)
            return WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
        return wl_columnar_memory_reserve_checked(governor, new_bytes,
                   reservation);
    }
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        || state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)
        return WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
    return wl_columnar_memory_reserve_checked(governor, new_bytes,
               reservation);
}

static bool
transition_reservation(wl_columnar_memory_reservation_t *reservation,
    uint64_t from, uint64_t to)
{
    uint64_t observed = from;

    if (!reservation_identity_valid(reservation))
        return false;
    for (;;) {
        if (atomic_compare_exchange_weak_explicit(&reservation->state,
            &observed, to, memory_order_acq_rel, memory_order_acquire))
            return true;
        if (observed != from)
            return false;
        observed = from;
    }
}

static bool
claim_reservation_state(wl_columnar_memory_reservation_t *reservation,
    uint64_t from, uint64_t to)
{
    uint64_t observed = from;

    if (!reservation_identity_valid(reservation))
        return false;
    for (;;) {
        if (atomic_compare_exchange_weak_explicit(&reservation->state,
            &observed, to, memory_order_acq_rel, memory_order_acquire))
            return true;
        if (observed != from)
            return false;
        observed = from;
    }
}

bool
wl_columnar_memory_commit(wl_columnar_memory_reservation_t *reservation,
    const void *owner)
{
    if (!reservation_identity_valid(reservation)
        || !transition_reservation(reservation,
        WL_COLUMNAR_MEMORY_RESERVATION_RESERVED,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING))
        return false;
    atomic_store_explicit(&reservation->owner_bits,
        (uint64_t)(uintptr_t)owner, memory_order_release);
    atomic_store_explicit(&reservation->state,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, memory_order_release);
    return true;
}

bool
wl_columnar_memory_transfer(wl_columnar_memory_reservation_t *reservation,
    const void *owner)
{
    uint64_t state;
    if (!reservation_identity_valid(reservation))
        return false;
    state = atomic_load_explicit(&reservation->state, memory_order_acquire);
    if (state != WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        && state != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)
        return false;
    if (!claim_reservation_state(reservation, state,
        WL_COLUMNAR_MEMORY_RESERVATION_TRANSFERRING))
        return false;
    WL_MEMORY_TEST_POINT(reservation, WL_COLUMNAR_MEMORY_TEST_TRANSFER_CLAIMED);
    atomic_store_explicit(&reservation->owner_bits,
        (uint64_t)(uintptr_t)owner, memory_order_release);
    atomic_store_explicit(&reservation->state, state, memory_order_release);
    return true;
}

/* Called only while the token is exclusively claimed. */
static bool
credit_reservation(wl_columnar_memory_governor_t *governor, uint64_t bytes)
{
    uint64_t old;
    old = atomic_load_explicit(&governor->reserved_bytes, memory_order_relaxed);
    for (;;) {
        uint64_t next;
        if (bytes > old)
            return false;
        next = old - bytes;
        if (atomic_compare_exchange_weak_explicit(
                &governor->reserved_bytes, &old, next,
                memory_order_release, memory_order_relaxed))
            break;
    }
    return true;
}

static wl_columnar_memory_admission_status_t
reserve_replacement_overlap(wl_columnar_memory_governor_t *governor,
    uint64_t bytes)
{
    uint64_t old;

    if (!governor || bytes == 0)
        return WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
    old = atomic_load_explicit(&governor->reserved_bytes,
            memory_order_relaxed);
    for (;;) {
        uint64_t limit = atomic_load_explicit(&governor->usable_bytes,
                memory_order_relaxed);
        uint64_t next;
        if (bytes > UINT64_MAX - old) {
            uint64_t observed = old;
            /* Linearize the overflow verdict with a no-op CAS. */
            if (atomic_compare_exchange_weak_explicit(
                    &governor->reserved_bytes, &observed, old,
                    memory_order_acquire, memory_order_relaxed))
                return WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW;
            old = observed;
            continue;
        }
        next = old + bytes;
        if (next > limit)
            return WL_COLUMNAR_MEMORY_ADMISSION_DENIED;
        if (atomic_compare_exchange_weak_explicit(
                &governor->reserved_bytes, &old, next,
                memory_order_relaxed, memory_order_relaxed))
            return governor->mode == WL_COLUMNAR_MEMORY_MODE_ADVISORY
                ? WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY
                : WL_COLUMNAR_MEMORY_ADMISSION_OK;
    }
}

static wl_columnar_memory_admission_status_t
begin_overlap(wl_columnar_memory_reservation_t *reservation,
    uint64_t additional_bytes, wl_columnar_memory_transition_kind_t kind)
{
    wl_columnar_memory_admission_status_t status;

    if (!reservation_identity_valid(reservation) || additional_bytes == 0
        || !claim_reservation_state(reservation,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED,
        WL_COLUMNAR_MEMORY_RESERVATION_REPLACEMENT_ADMITTING))
        return WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
    if (!reservation->governor
        || reservation->transition_kind != WL_COLUMNAR_MEMORY_TRANSITION_NONE
        || (kind == WL_COLUMNAR_MEMORY_TRANSITION_REPLACEMENT
        && additional_bytes > reservation->bytes)) {
        atomic_store_explicit(&reservation->state,
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED,
            memory_order_release);
        return WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
    }
    if (kind == WL_COLUMNAR_MEMORY_TRANSITION_GROWTH
        && additional_bytes > UINT64_MAX - reservation->bytes) {
        atomic_store_explicit(&reservation->state,
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED,
            memory_order_release);
        return WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW;
    }
    status = reserve_replacement_overlap(reservation->governor,
            additional_bytes);
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
        && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
        atomic_store_explicit(&reservation->state,
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED,
            memory_order_release);
        return status;
    }
    reservation->replacement_bytes = additional_bytes;
    reservation->transition_kind = (uint8_t)kind;
    atomic_store_explicit(&reservation->state,
        WL_COLUMNAR_MEMORY_RESERVATION_REPLACING, memory_order_release);
    return status;
}

wl_columnar_memory_admission_status_t
wl_columnar_memory_begin_replacement(
    wl_columnar_memory_reservation_t *reservation, uint64_t new_bytes)
{
    return begin_overlap(reservation, new_bytes,
               WL_COLUMNAR_MEMORY_TRANSITION_REPLACEMENT);
}

wl_columnar_memory_admission_status_t
wl_columnar_memory_begin_growth(
    wl_columnar_memory_reservation_t *reservation, uint64_t additional_bytes)
{
    return begin_overlap(reservation, additional_bytes,
               WL_COLUMNAR_MEMORY_TRANSITION_GROWTH);
}

bool
wl_columnar_memory_commit_replacement(
    wl_columnar_memory_reservation_t *reservation)
{
    uint64_t old_bytes;
    uint64_t new_bytes;

    if (!reservation_identity_valid(reservation)
        || !claim_reservation_state(reservation,
        WL_COLUMNAR_MEMORY_RESERVATION_REPLACING,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING))
        return false;
    if (reservation->transition_kind
        != WL_COLUMNAR_MEMORY_TRANSITION_REPLACEMENT) {
        atomic_store_explicit(&reservation->state,
            WL_COLUMNAR_MEMORY_RESERVATION_REPLACING,
            memory_order_release);
        return false;
    }
    old_bytes = reservation->bytes;
    new_bytes = reservation->replacement_bytes;
    if (new_bytes == 0 || new_bytes > old_bytes
        || !credit_reservation(reservation->governor, old_bytes)) {
        atomic_store_explicit(&reservation->state,
            WL_COLUMNAR_MEMORY_RESERVATION_REPLACING,
            memory_order_release);
        return false;
    }
    reservation->bytes = new_bytes;
    reservation->replacement_bytes = 0;
    reservation->transition_kind = WL_COLUMNAR_MEMORY_TRANSITION_NONE;
    atomic_store_explicit(&reservation->state,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, memory_order_release);
    return true;
}

bool
wl_columnar_memory_commit_growth(
    wl_columnar_memory_reservation_t *reservation)
{
    if (!reservation_identity_valid(reservation)
        || !claim_reservation_state(reservation,
        WL_COLUMNAR_MEMORY_RESERVATION_REPLACING,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING))
        return false;
    if (reservation->transition_kind
        != WL_COLUMNAR_MEMORY_TRANSITION_GROWTH
        || reservation->replacement_bytes == 0
        || reservation->replacement_bytes > UINT64_MAX - reservation->bytes) {
        atomic_store_explicit(&reservation->state,
            WL_COLUMNAR_MEMORY_RESERVATION_REPLACING,
            memory_order_release);
        return false;
    }
    reservation->bytes += reservation->replacement_bytes;
    reservation->replacement_bytes = 0;
    reservation->transition_kind = WL_COLUMNAR_MEMORY_TRANSITION_NONE;
    atomic_store_explicit(&reservation->state,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, memory_order_release);
    return true;
}

static bool
rollback_overlap(wl_columnar_memory_reservation_t *reservation,
    wl_columnar_memory_transition_kind_t kind)
{
    uint64_t replacement_bytes;

    if (!reservation_identity_valid(reservation)
        || !claim_reservation_state(reservation,
        WL_COLUMNAR_MEMORY_RESERVATION_REPLACING,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING))
        return false;
    if (reservation->transition_kind != kind) {
        atomic_store_explicit(&reservation->state,
            WL_COLUMNAR_MEMORY_RESERVATION_REPLACING,
            memory_order_release);
        return false;
    }
    replacement_bytes = reservation->replacement_bytes;
    if (replacement_bytes == 0
        || !credit_reservation(reservation->governor, replacement_bytes)) {
        atomic_store_explicit(&reservation->state,
            WL_COLUMNAR_MEMORY_RESERVATION_REPLACING,
            memory_order_release);
        return false;
    }
    reservation->replacement_bytes = 0;
    reservation->transition_kind = WL_COLUMNAR_MEMORY_TRANSITION_NONE;
    atomic_store_explicit(&reservation->state,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, memory_order_release);
    return true;
}

bool
wl_columnar_memory_rollback_replacement(
    wl_columnar_memory_reservation_t *reservation)
{
    return rollback_overlap(reservation,
               WL_COLUMNAR_MEMORY_TRANSITION_REPLACEMENT);
}

bool
wl_columnar_memory_rollback_growth(
    wl_columnar_memory_reservation_t *reservation)
{
    return rollback_overlap(reservation,
               WL_COLUMNAR_MEMORY_TRANSITION_GROWTH);
}

bool
wl_columnar_memory_reservation_downsize(
    wl_columnar_memory_reservation_t *reservation, uint64_t bytes)
{
    bool ok = false;
    if (bytes == 0 || !claim_reservation_state(reservation,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED,
        WL_COLUMNAR_MEMORY_RESERVATION_DOWNSIZING))
        return false;
    WL_MEMORY_TEST_POINT(reservation, WL_COLUMNAR_MEMORY_TEST_DOWNSIZE_CLAIMED);
    if (reservation->governor && bytes <= reservation->bytes
        && credit_reservation(reservation->governor,
        reservation->bytes - bytes)) {
        reservation->bytes = bytes;
        ok = true;
        WL_MEMORY_TEST_POINT(reservation,
            WL_COLUMNAR_MEMORY_TEST_DOWNSIZE_CREDITED);
    }
    atomic_store_explicit(&reservation->state,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, memory_order_release);
    return ok;
}

static bool
finish_reservation(wl_columnar_memory_reservation_t *reservation,
    bool rollback)
{
    uint64_t state;
    uint64_t bytes;
    bool ok;
    if (!reservation_identity_valid(reservation))
        return false;
    state = atomic_load_explicit(&reservation->state, memory_order_acquire);
    if (state != WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        && !(!rollback
        && (state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || state == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING)))
        return false;
    WL_MEMORY_TEST_POINT(reservation,
        WL_COLUMNAR_MEMORY_TEST_RELEASE_BEFORE_CLAIM);
    if (!claim_reservation_state(reservation, state,
        WL_COLUMNAR_MEMORY_RESERVATION_RELEASING))
        return false;
    WL_MEMORY_TEST_POINT(reservation, WL_COLUMNAR_MEMORY_TEST_RELEASE_CLAIMED);
    /* Payload cannot change until accounting is complete and state published. */
    bytes = reservation->bytes;
    /* A replacing token owns both the committed old footprint and the
     * temporary overlap footprint.  Teardown must release both after all
     * physical storage has been freed, even if replacement rollback failed
     * and left the token in REPLACING. */
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING) {
        if (reservation->replacement_bytes > UINT64_MAX - bytes)
            ok = false;
        else {
            bytes += reservation->replacement_bytes;
            ok = reservation->governor && bytes != 0
                && credit_reservation(reservation->governor, bytes);
        }
    } else {
        ok = reservation->governor && bytes != 0
            && credit_reservation(reservation->governor, bytes);
    }
    WL_MEMORY_TEST_POINT(reservation, WL_COLUMNAR_MEMORY_TEST_RELEASE_CREDITED);
    if (ok) {
        if (state == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING)
            reservation->replacement_bytes = 0;
        reservation->transition_kind = WL_COLUMNAR_MEMORY_TRANSITION_NONE;
    }
    atomic_store_explicit(&reservation->state,
        ok ? WL_COLUMNAR_MEMORY_RESERVATION_RELEASED : state,
        memory_order_release);
    return ok;
}

bool
wl_columnar_memory_rollback(
    wl_columnar_memory_reservation_t *reservation)
{
    return finish_reservation(reservation, true);
}

bool
wl_columnar_memory_release(wl_columnar_memory_reservation_t *reservation)
{
#ifdef WL_TEST_RELATION_RESIZE_HOOK
    if (wl_columnar_memory_governor_test_refuse_release) {
        wl_columnar_memory_governor_test_refuse_release = false;
        return false;
    }
#endif
    return finish_reservation(reservation, false);
}

uint64_t
wl_columnar_memory_reserved(const wl_columnar_memory_governor_t *governor)
{
    if (!governor)
        return 0;
    return atomic_load_explicit(&governor->reserved_bytes,
               memory_order_acquire);
}
