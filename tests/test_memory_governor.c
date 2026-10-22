/*
 * test_memory_governor.c - deterministic memory resolver/governor tests
 *
 * Issue #1368: the governor is a foundation only. Allocation-site enforcement
 * and session ownership integration are separate implementation units.
 */

#include "../wirelog/columnar/memory_governor.h"
#include "wirelog/thread.h"

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

static int tests_run;
static int tests_failed;
static wl_columnar_memory_reservation_t growth_refund_peer;
static bool growth_refund_peer_created;

#define TEST(name) do { tests_run++; printf("  [%d] %s", tests_run, name); \
} while (0)
#define PASS() do { printf(" ... PASS\n"); } while (0)
#define FAIL(msg) do { printf(" ... FAIL: %s\n", msg); tests_failed++; \
} while (0)

static int
test_strong_atomic_compare_exchange(void)
{
    wl_atomic_u64 value = 7;
    uint64_t expected = 6;

    TEST("strong atomic compare exchange compatibility");
    if (atomic_compare_exchange_strong_explicit(&value, &expected, 9,
        memory_order_acq_rel, memory_order_acquire)
        || expected != 7 || value != 7) {
        FAIL("mismatch did not report the observed value");
        return 1;
    }
    expected = 7;
    if (!atomic_compare_exchange_strong_explicit(&value, &expected, 9,
        memory_order_acq_rel, memory_order_acquire)
        || value != 9) {
        FAIL("matching compare exchange failed");
        return 1;
    }
    PASS();
    return 0;
}

static void
growth_refund_add_peer_reservation(wl_columnar_memory_governor_t *governor)
{
    if (!growth_refund_peer_created) {
        wl_columnar_memory_reservation_init(&growth_refund_peer);
        if (wl_columnar_memory_reserve(governor, 10, &growth_refund_peer)
            && wl_columnar_memory_commit(&growth_refund_peer,
            &growth_refund_peer))
            growth_refund_peer_created = true;
    }
}

struct growth_refund_thread_arg {
    wl_columnar_memory_growth_refund_attempt_t *attempt;
    int result;
};

static void *
growth_refund_advance_on_thread(void *opaque)
{
    struct growth_refund_thread_arg *arg = opaque;
    arg->result = wl_columnar_memory_growth_refund_attempt_advance(
        arg->attempt);
    return NULL;
}

struct downsize_thread_arg {
    wl_columnar_memory_downsize_attempt_t *attempt;
    int result;
};

static void *
downsize_advance_on_thread(void *opaque)
{
    struct downsize_thread_arg *arg = opaque;
    arg->result = wl_columnar_memory_downsize_attempt_advance(arg->attempt);
    return NULL;
}

static int
test_explicit_values(void)
{
    wl_columnar_memory_resolution_t result;
    const char *invalid[] = {
        "", "0", "+268435456", "-1", " 268435456", "268435456 ",
        "abc", "268435455",
    };
    size_t i;

    TEST("strict explicit budget parsing");
    if (wl_columnar_memory_resolve("268435456", NULL, &result) != 0
        || result.mode != WL_COLUMNAR_MEMORY_MODE_ENFORCING
        || result.source != WL_COLUMNAR_MEMORY_SOURCE_ENV
        || result.budget_bytes != 268435456
        || result.headroom_bytes != 13421772
        || result.usable_bytes != 255013684) {
        FAIL("valid decimal budget or headroom arithmetic is wrong");
        return 1;
    }
    for (i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        if (wl_columnar_memory_resolve(invalid[i], NULL, &result) == 0) {
            FAIL("invalid explicit budget was accepted");
            return 1;
        }
    }
    if (wl_columnar_memory_resolve("18446744073709551616", NULL, &result)
        != WL_COLUMNAR_MEMORY_OVERFLOW
        || result.status != WL_COLUMNAR_MEMORY_OVERFLOW) {
        FAIL("decimal overflow was not reported distinctly");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_automatic_sources(void)
{
    wl_columnar_memory_sources_t sources;
    wl_columnar_memory_resolution_t result;

    TEST("automatic source reduction and advisory fallback");
    memset(&sources, 0, sizeof(sources));
    sources.cgroup_v2_available = true;
    sources.cgroup_v2_limit = UINT64_C(1024) * 1024 * 1024;
    sources.cgroup_v1_available = true;
    sources.cgroup_v1_limit = UINT64_C(2) * 1024 * 1024 * 1024;
    sources.rlimit_as_available = true;
    sources.rlimit_as_limit = UINT64_C(512) * 1024 * 1024;
    if (wl_columnar_memory_resolve(NULL, &sources, &result) != 0
        || result.source != WL_COLUMNAR_MEMORY_SOURCE_RLIMIT_AS
        || result.budget_bytes != sources.rlimit_as_limit
        || result.mode != WL_COLUMNAR_MEMORY_MODE_ENFORCING) {
        FAIL("effective automatic source was not selected");
        return 1;
    }
    sources.rlimit_as_limit = WL_COLUMNAR_MEMORY_MIN_BUDGET - 1;
    if (wl_columnar_memory_resolve(NULL, &sources, &result)
        != WL_COLUMNAR_MEMORY_INVALID) {
        FAIL("automatic budget below the minimum was accepted");
        return 1;
    }
    sources.rlimit_as_available = false;
    sources.rlimit_as_limit = 0;
    sources.cgroup_v2_available = false;
    sources.cgroup_v1_available = true;
    sources.cgroup_v1_limit = UINT64_C(1) << 62;
    if (wl_columnar_memory_resolve(NULL, &sources, &result)
        != WL_COLUMNAR_MEMORY_OK
        || result.mode != WL_COLUMNAR_MEMORY_MODE_ADVISORY) {
        FAIL("cgroup v1 unlimited sentinel was treated as a budget");
        return 1;
    }
    memset(&sources, 0, sizeof(sources));
    sources.windows_job_available = true;
    sources.windows_job_limit = UINT64_C(512) * 1024 * 1024;
    if (wl_columnar_memory_resolve(NULL, &sources, &result) != 0
        || result.mode != WL_COLUMNAR_MEMORY_MODE_ENFORCING
        || result.source != WL_COLUMNAR_MEMORY_SOURCE_WINDOWS_JOB) {
        FAIL("Windows Job source was not selected from an injected limit");
        return 1;
    }
    if (wl_columnar_memory_probe_windows_job(NULL, &sources)
        != WL_COLUMNAR_MEMORY_UNAVAILABLE
        || sources.windows_job_available || sources.windows_job_limit != 0) {
        FAIL("failed Windows provider probe retained stale limit");
        return 1;
    }
    if (wl_columnar_memory_resolve(NULL, &sources, &result) != 0
        || result.mode != WL_COLUMNAR_MEMORY_MODE_ADVISORY
        || result.usable_bytes != UINT64_MAX
        || result.source != WL_COLUMNAR_MEMORY_SOURCE_NONE) {
        FAIL("missing automatic source did not select advisory mode");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_headroom_boundaries(void)
{
    wl_columnar_memory_sources_t sources;
    wl_columnar_memory_resolution_t result;

    TEST("headroom cap and small-budget arithmetic");
    memset(&sources, 0, sizeof(sources));
    sources.cgroup_v2_available = true;
    sources.cgroup_v2_limit = UINT64_C(16) * 1024 * 1024 * 1024;
    if (wl_columnar_memory_resolve(NULL, &sources, &result) != 0
        || result.headroom_bytes != WL_COLUMNAR_MEMORY_HEADROOM_CAP
        || result.usable_bytes
        != result.budget_bytes - WL_COLUMNAR_MEMORY_HEADROOM_CAP) {
        FAIL("headroom cap was not applied");
        return 1;
    }
    sources.cgroup_v2_limit = WL_COLUMNAR_MEMORY_MIN_BUDGET;
    if (wl_columnar_memory_resolve(NULL, &sources, &result) != 0
        || result.headroom_bytes != WL_COLUMNAR_MEMORY_MIN_BUDGET / 20
        || result.usable_bytes != result.budget_bytes - result.headroom_bytes) {
        FAIL("small-budget headroom underflow or rounding");
        return 1;
    }
    PASS();
    return 0;
}

static void
make_resolution(wl_columnar_memory_resolution_t *resolution,
    uint64_t budget, uint64_t usable)
{
    memset(resolution, 0, sizeof(*resolution));
    resolution->budget_bytes = budget;
    resolution->headroom_bytes = budget - usable;
    resolution->usable_bytes = usable;
    resolution->mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution->source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution->status = WL_COLUMNAR_MEMORY_OK;
}

static int
test_reservation_lifecycle(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_t governor;
    wl_columnar_memory_reservation_t first;
    wl_columnar_memory_reservation_t second;
    int owner_a;
    int owner_b;

    TEST("reservation token lifecycle and denial");
    wl_columnar_memory_reservation_init(&first);
    wl_columnar_memory_reservation_init(&second);
    make_resolution(&resolution, 1000, 900);
    if (wl_columnar_memory_governor_init(&governor, &resolution) != 0
        || !wl_columnar_memory_reserve(&governor, 600, &first)
        || wl_columnar_memory_reserved(&governor) != 600
        || wl_columnar_memory_reserve(&governor, 301, &second)
        || wl_columnar_memory_reserved(&governor) != 600
        || !wl_columnar_memory_commit(&first, &owner_a)
        || !wl_columnar_memory_transfer(&first, &owner_b)
        || wl_columnar_memory_rollback(&first)
        || !wl_columnar_memory_release(&first)
        || wl_columnar_memory_release(&first)
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("commit/transfer/release lifecycle is not idempotent");
        return 1;
    }
    if (!wl_columnar_memory_reserve(&governor, 900, &second)
        || !wl_columnar_memory_rollback(&second)
        || wl_columnar_memory_rollback(&second)
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("rollback lifecycle is not enforced");
        return 1;
    }
    bool done = false;
    if (!wl_columnar_memory_reserve(&governor, 600, &first)
        || !wl_columnar_memory_commit(&first, &owner_a)
        || wl_columnar_memory_reservation_finish_step(&first, false, &done)
        || done
        || atomic_load_explicit(&first.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_RELEASING
        || wl_columnar_memory_reserved(&governor) != 600
        || wl_columnar_memory_reservation_finish_step(&first, false, &done)
        || !done || wl_columnar_memory_reserved(&governor) != 0
        || atomic_load_explicit(&first.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED) {
        FAIL("stepped committed release did not retain and retire its token");
        return 1;
    }
    done = false;
    if (!wl_columnar_memory_reserve(&governor, 600, &second)
        || wl_columnar_memory_reservation_finish_step(&second, true, &done)
        || done
        || wl_columnar_memory_reservation_finish_step(&second, true, &done)
        || !done || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("stepped reservation rollback did not restore accounting");
        return 1;
    }
    PASS();
    return 0;
}

struct reserve_thread_arg {
    wl_columnar_memory_governor_t *governor;
    wl_columnar_memory_reservation_t *reservation;
    bool admitted;
};

static void *
reserve_thread(void *arg)
{
    struct reserve_thread_arg *item = arg;
    item->admitted = wl_columnar_memory_reserve(item->governor, 200,
            item->reservation);
    return NULL;
}

static int
test_concurrent_admission(void)
{
    enum { THREADS = 8 };
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_t governor;
    wl_columnar_memory_reservation_t reservations[THREADS];
    struct reserve_thread_arg args[THREADS];
    wl_thread_t threads[THREADS];
    int admitted = 0;
    int i;

    TEST("concurrent reservations share one limit");
    make_resolution(&resolution, 1000, 900);
    if (wl_columnar_memory_governor_init(&governor, &resolution) != 0)
        return 1;
    for (i = 0; i < THREADS; i++) {
        wl_columnar_memory_reservation_init(&reservations[i]);
        args[i].governor = &governor;
        args[i].reservation = &reservations[i];
        args[i].admitted = false;
        if (wl_thread_create(&threads[i], reserve_thread, &args[i]) != 0) {
            FAIL("thread creation failed");
            return 1;
        }
    }
    for (i = 0; i < THREADS; i++)
        wl_thread_join(&threads[i]);
    for (i = 0; i < THREADS; i++)
        admitted += args[i].admitted ? 1 : 0;
    if (admitted > 4 || wl_columnar_memory_reserved(&governor) > 900) {
        FAIL("concurrent admission exceeded usable limit");
        return 1;
    }
    for (i = 0; i < THREADS; i++)
        if (args[i].admitted)
            wl_columnar_memory_release(&reservations[i]);
    if (wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("concurrent reservation cleanup leaked capacity");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_checked_admission(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_t governor;
    wl_columnar_memory_governor_t other_governor;
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_reservation_t second;
    wl_columnar_memory_reservation_t copied;
    wl_columnar_memory_reservation_t old_reservation;
    wl_columnar_memory_reservation_t growth_reservation;
    wl_columnar_memory_reservation_t moved_reservation;
    uint64_t value;

    TEST("typed admission, growth overlap, arithmetic, and token identity");
    make_resolution(&resolution, 1000, 900);
    wl_columnar_memory_reservation_init(&reservation);
    wl_columnar_memory_reservation_init(&second);
    if (wl_columnar_memory_governor_init(&governor, &resolution) != 0
        || wl_columnar_memory_size_add(UINT64_MAX, 1, &value)
        || wl_columnar_memory_size_mul(UINT64_MAX, 2, &value)
        || !wl_columnar_memory_size_add(40, 2, &value) || value != 42
        || !wl_columnar_memory_size_mul(6, 7, &value) || value != 42
        || wl_columnar_memory_reserve_checked(&governor, 901, &reservation)
        != WL_COLUMNAR_MEMORY_ADMISSION_DENIED
        || wl_columnar_memory_reserve_growth(&governor, 20, 10, &reservation)
        != WL_COLUMNAR_MEMORY_ADMISSION_INVALID
        || wl_columnar_memory_reserve_growth(&governor, 20, 20, &reservation)
        != WL_COLUMNAR_MEMORY_ADMISSION_OK
        || wl_columnar_memory_reserved(&governor) != 20
        || !wl_columnar_memory_release(&reservation)
        || !wl_columnar_memory_reserve(&governor, 400, &reservation)
        || wl_columnar_memory_reserve_growth(&governor, 400, 400,
        &reservation) != WL_COLUMNAR_MEMORY_ADMISSION_INVALID
        || wl_columnar_memory_reserved(&governor) != 400) {
        FAIL("checked admission or overflow arithmetic is wrong");
        return 1;
    }
    copied = reservation;
    if (wl_columnar_memory_release(&copied)
        || !wl_columnar_memory_release(&reservation)
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("copied reservation token was able to release capacity");
        return 1;
    }
    wl_columnar_memory_reservation_init(&old_reservation);
    wl_columnar_memory_reservation_init(&growth_reservation);
    wl_columnar_memory_reservation_init(&moved_reservation);
    if (!wl_columnar_memory_reserve(&governor, 100, &old_reservation)
        || wl_columnar_memory_reserve_growth(&governor, 100, 200,
        &growth_reservation) != WL_COLUMNAR_MEMORY_ADMISSION_OK
        || wl_columnar_memory_reserved(&governor) != 300
        || !wl_columnar_memory_release(&growth_reservation)
        || !wl_columnar_memory_release(&old_reservation)) {
        FAIL("growth did not reserve a distinct overlapping footprint");
        return 1;
    }
    if (!wl_columnar_memory_reserve(&governor, 100, &old_reservation)
        || !wl_columnar_memory_reserve(&governor, 50, &second)
        || wl_columnar_memory_reservation_move(&second, &old_reservation)
        || !wl_columnar_memory_release(&second)
        || !wl_columnar_memory_reservation_move(&moved_reservation,
        &old_reservation)
        || wl_columnar_memory_release(&old_reservation)
        || !wl_columnar_memory_release(&moved_reservation)
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("reservation move did not preserve token ownership");
        return 1;
    }
    make_resolution(&resolution, 1000, 900);
    if (wl_columnar_memory_governor_init(&other_governor, &resolution) != 0
        || !wl_columnar_memory_reserve(&governor, 400, &reservation)
        || wl_columnar_memory_reserve_growth(&other_governor, 400, 400,
        &reservation) != WL_COLUMNAR_MEMORY_ADMISSION_INVALID
        || !wl_columnar_memory_release(&reservation)) {
        FAIL("growth accepted a token owned by another governor");
        return 1;
    }
    memset(&resolution, 0, sizeof(resolution));
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ADVISORY;
    resolution.usable_bytes = UINT64_MAX;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    if (wl_columnar_memory_governor_init(&governor, &resolution) != 0) {
        FAIL("advisory governor initialization failed");
        return 1;
    }
    wl_columnar_memory_reservation_init(&reservation);
    if (wl_columnar_memory_reserve_growth(&governor, 0, 1, &reservation)
        != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY
        || !wl_columnar_memory_release(&reservation)) {
        FAIL("advisory admission status was not preserved");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_downsize(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_t governor;
    wl_columnar_memory_reservation_t token, other, copy, moved;
    int owner;
    TEST("committed reservation downsizing and invalid states");
    make_resolution(&resolution, 1000, 900);
    wl_columnar_memory_governor_init(&governor, &resolution);
    wl_columnar_memory_reservation_init(&token);
    wl_columnar_memory_reservation_init(&other);
    wl_columnar_memory_reservation_init(&moved);
    if (wl_columnar_memory_reservation_downsize(NULL, 1)
        || wl_columnar_memory_reservation_downsize(&token, 1)
        || !wl_columnar_memory_reserve(&governor, 600, &token)
        || !wl_columnar_memory_reserve(&governor, 100, &other)
        || wl_columnar_memory_reservation_downsize(&token, 300)
        || !wl_columnar_memory_commit(&token, &owner)) {
        FAIL("setup or noncommitted rejection");
        return 1;
    }
    memcpy(&copy, &token, sizeof(copy));
    if (wl_columnar_memory_reservation_downsize(&copy, 300)
        || wl_columnar_memory_reservation_downsize(&token, 0)
        || wl_columnar_memory_reservation_downsize(&token, 601)
        || !wl_columnar_memory_reservation_downsize(&token, 600)
        || wl_columnar_memory_reserved(&governor) != 700
        || !wl_columnar_memory_reservation_downsize(&token, 300)
        || !wl_columnar_memory_reservation_downsize(&token, 200)
        || token.bytes != 200
        || atomic_load_explicit(&token.owner_bits, memory_order_acquire)
        != (uint64_t)(uintptr_t)&owner
        || wl_columnar_memory_reserved(&governor) != 300) {
        FAIL("downsize changed identity/owner or credited wrong amount");
        return 1;
    }
    /* Exercise busy rejection without scheduling assumptions. */
    atomic_store_explicit(&token.state,
        WL_COLUMNAR_MEMORY_RESERVATION_DOWNSIZING, memory_order_release);
    if (wl_columnar_memory_reservation_downsize(&token, 100)
        || wl_columnar_memory_release(&token)
        || wl_columnar_memory_transfer(&token, &other)) {
        FAIL("busy token accepted an operation");
        return 1;
    }
    atomic_store_explicit(&token.state,
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED, memory_order_release);
    /* Inject corrupt aggregate accounting to verify failure is reversible. */
    atomic_store_explicit(&governor.reserved_bytes, 1, memory_order_relaxed);
    if (wl_columnar_memory_reservation_downsize(&token, 100)
        || wl_columnar_memory_release(&token) || token.bytes != 200
        || atomic_load_explicit(&token.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED) {
        FAIL("accounting failure changed token");
        return 1;
    }
    atomic_store_explicit(&governor.reserved_bytes, 300, memory_order_relaxed);
    if (!wl_columnar_memory_transfer(&token, &other)
        || atomic_load_explicit(&token.owner_bits, memory_order_acquire)
        != (uint64_t)(uintptr_t)&other
        || !wl_columnar_memory_reservation_move(&moved, &token)
        || wl_columnar_memory_reservation_downsize(&token, 100)
        || !wl_columnar_memory_reservation_downsize(&moved, 100)
        || atomic_load_explicit(&moved.owner_bits, memory_order_acquire)
        != (uint64_t)(uintptr_t)&other
        || !wl_columnar_memory_release(&moved)
        || wl_columnar_memory_reservation_downsize(&moved, 1)
        || wl_columnar_memory_reserved(&governor) != 100
        || !wl_columnar_memory_release(&other)
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("move or residual release accounting");
        return 1;
    }
    PASS();
    return 0;
}

static struct {
    wl_columnar_memory_reservation_t *token;
    unsigned phase;
    unsigned hits;
    bool armed;
    bool valid;
} schedule;

struct finish_step_thread_arg {
    wl_columnar_memory_reservation_t *token;
    bool done;
    int result;
};

static void *
finish_step_on_thread(void *opaque)
{
    struct finish_step_thread_arg *arg = opaque;
    arg->result = wl_columnar_memory_reservation_finish_step(arg->token,
            false, &arg->done);
    return NULL;
}

void
wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *token,
    unsigned phase)
{
    if (!schedule.armed || token != schedule.token || phase != schedule.phase)
        return;
    /* One-shot nesting models a competing operation while this call is paused. */
    schedule.armed = false;
    schedule.hits++;
    if (phase == WL_COLUMNAR_MEMORY_TEST_FINISH_PREPARING
        || phase == WL_COLUMNAR_MEMORY_TEST_FINISH_RELEASING) {
        wl_thread_t competitor;
        struct finish_step_thread_arg arg = { token, true, 0 };
        uint64_t expected_state = phase
            == WL_COLUMNAR_MEMORY_TEST_FINISH_PREPARING
            ? WL_COLUMNAR_MEMORY_RESERVATION_RELEASE_PREPARING
            : WL_COLUMNAR_MEMORY_RESERVATION_RELEASING;
        schedule.valid = atomic_load_explicit(&token->state,
                memory_order_acquire)
            == expected_state;
        if (wl_thread_create(&competitor, finish_step_on_thread, &arg) != 0
            || wl_thread_join(&competitor) != 0) {
            schedule.valid = false;
            return;
        }
        schedule.valid = schedule.valid && arg.result == EBUSY && !arg.done
            && atomic_load_explicit(&token->state, memory_order_acquire)
            == expected_state
            && wl_columnar_memory_reserved(token->governor) == 600;
        return;
    }
    if (phase == WL_COLUMNAR_MEMORY_TEST_RELEASE_BEFORE_CLAIM) {
        schedule.valid = wl_columnar_memory_reservation_downsize(token, 200)
            && wl_columnar_memory_reserved(token->governor) == 1200;
        return;
    }
    uint64_t state = WL_COLUMNAR_MEMORY_RESERVATION_RELEASING;
    uint64_t total = 1600;
    if (phase == WL_COLUMNAR_MEMORY_TEST_DOWNSIZE_CLAIMED
        || phase == WL_COLUMNAR_MEMORY_TEST_DOWNSIZE_CREDITED)
        state = WL_COLUMNAR_MEMORY_RESERVATION_DOWNSIZING;
    if (phase == WL_COLUMNAR_MEMORY_TEST_TRANSFER_CLAIMED)
        state = WL_COLUMNAR_MEMORY_RESERVATION_TRANSFERRING;
    if (phase == WL_COLUMNAR_MEMORY_TEST_RELEASE_CREDITED)
        total = 1000;
    if (phase == WL_COLUMNAR_MEMORY_TEST_DOWNSIZE_CREDITED)
        total = 1200;
    schedule.valid = atomic_load_explicit(&token->state, memory_order_acquire)
        == state && wl_columnar_memory_reserved(token->governor) == total;
    bool release = wl_columnar_memory_release(token);
    bool downsize = wl_columnar_memory_reservation_downsize(token, 100);
    bool transfer = wl_columnar_memory_transfer(token, &schedule);
    bool reserve = wl_columnar_memory_reserve(token->governor, 50, token);
    schedule.valid = schedule.valid && !release && !downsize && !transfer
        && !reserve && wl_columnar_memory_reserved(token->governor) == total
        && atomic_load_explicit(&token->owner_bits, memory_order_acquire)
        == (uint64_t)(uintptr_t)token->governor;
}

static int
test_finish_step_claim_before_receipt(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_t governor;
    wl_columnar_memory_reservation_t token;
    const int owner = 1;
    bool done = false;

    TEST("stepped finish claims before publishing receipt");
    make_resolution(&resolution, 5000, 4500);
    wl_columnar_memory_governor_init(&governor, &resolution);
    wl_columnar_memory_reservation_init(&token);
    if (!wl_columnar_memory_reserve(&governor, 600, &token)
        || !wl_columnar_memory_commit(&token, &owner)) {
        FAIL("stepped finish claim setup");
        return 1;
    }
    schedule.token = &token;
    schedule.phase = WL_COLUMNAR_MEMORY_TEST_FINISH_PREPARING;
    schedule.hits = 0;
    schedule.armed = true;
    schedule.valid = false;
    int rc = wl_columnar_memory_reservation_finish_step(&token, false, &done);
    schedule.armed = false;
    if (rc || done || !schedule.valid || schedule.hits != 1
        || token.release_bytes != 600
        || token.release_from_state != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || token.release_mode != 1
        || atomic_load_explicit(&token.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_RELEASING
        || wl_columnar_memory_reserved(&governor) != 600) {
        FAIL("competing finish observed an incomplete receipt");
        return 1;
    }
    schedule.token = &token;
    schedule.phase = WL_COLUMNAR_MEMORY_TEST_FINISH_RELEASING;
    schedule.hits = 0;
    schedule.armed = true;
    schedule.valid = false;
    rc = wl_columnar_memory_reservation_finish_step(&token, false, &done);
    schedule.armed = false;
    if (rc || !done || !schedule.valid || schedule.hits != 1
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("same-thread receipt resume did not credit exactly once");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_downsize_interleavings(void)
{
    TEST("controlled claim, credit, and publication interleavings");
    for (unsigned phase = WL_COLUMNAR_MEMORY_TEST_RELEASE_BEFORE_CLAIM;
        phase <= WL_COLUMNAR_MEMORY_TEST_DOWNSIZE_CREDITED; phase++) {
        wl_columnar_memory_resolution_t resolution;
        wl_columnar_memory_governor_t governor;
        wl_columnar_memory_reservation_t token, other;
        make_resolution(&resolution, 5000, 4500);
        wl_columnar_memory_governor_init(&governor, &resolution);
        wl_columnar_memory_reservation_init(&token);
        wl_columnar_memory_reservation_init(&other);
        if (!wl_columnar_memory_reserve(&governor, 600, &token)
            || !wl_columnar_memory_reserve(&governor, 1000, &other)
            || !wl_columnar_memory_commit(&token, &governor)) {
            FAIL("controlled schedule setup");
            return 1;
        }
        schedule.token = &token;
        schedule.phase = phase;
        schedule.hits = 0;
        schedule.armed = true;
        schedule.valid = false;
        bool is_downsize = phase == WL_COLUMNAR_MEMORY_TEST_DOWNSIZE_CLAIMED
            || phase == WL_COLUMNAR_MEMORY_TEST_DOWNSIZE_CREDITED;
        bool is_transfer = phase == WL_COLUMNAR_MEMORY_TEST_TRANSFER_CLAIMED;
        bool ok;
        if (is_downsize)
            ok = wl_columnar_memory_reservation_downsize(&token, 200);
        else if (is_transfer)
            ok = wl_columnar_memory_transfer(&token, &other);
        else
            ok = wl_columnar_memory_release(&token);
        schedule.armed = false;
        uint64_t expected = 1000 + (is_downsize ? 200 : is_transfer ? 600 : 0);
        bool valid = ok && schedule.valid && schedule.hits == 1
            && wl_columnar_memory_reserved(&governor) == expected
            && atomic_load_explicit(&token.owner_bits, memory_order_acquire)
            == (uint64_t)(uintptr_t)(is_transfer
                ? (void *)&other : (void *)&governor);
        if (!is_downsize && !is_transfer) {
            valid = valid && atomic_load_explicit(&token.state,
                    memory_order_acquire)
                == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED;
            valid = wl_columnar_memory_reserve(&governor, 50, &token) && valid;
        }
        valid = wl_columnar_memory_release(&token) && valid;
        valid = valid && wl_columnar_memory_reserved(&governor) == 1000;
        valid = wl_columnar_memory_release(&other) && valid;
        if (!valid || wl_columnar_memory_reserved(&governor) != 0) {
            FAIL("controlled schedule violated accounting or publication");
            return 1;
        }
    }
    PASS();
    return 0;
}

struct downsize_arg {
    wl_columnar_memory_reservation_t *token;
    wl_atomic_u64 *start;
    unsigned op;
    bool success;
};

static void *
downsize_thread(void *ptr)
{
    struct downsize_arg *arg = ptr;
    while (!atomic_load_explicit(arg->start, memory_order_acquire)) {
    }
    if (arg->op == 0)
        arg->success = wl_columnar_memory_reservation_downsize(arg->token, 200);
    else if (arg->op == 1)
        arg->success = wl_columnar_memory_release(arg->token);
    else
        arg->success = wl_columnar_memory_transfer(arg->token, arg->start);
    return NULL;
}

static int
test_downsize_races(void)
{
    TEST(
        "downsize/release/transfer contention conserves unrelated reservations");
    for (unsigned round = 0; round < 100; round++) {
        wl_columnar_memory_resolution_t resolution;
        wl_columnar_memory_governor_t governor;
        wl_columnar_memory_reservation_t token, other;
        wl_atomic_u64 start;
        wl_thread_t threads[3];
        struct downsize_arg args[3];
        unsigned created = 0;
        make_resolution(&resolution, 1000, 900);
        wl_columnar_memory_governor_init(&governor, &resolution);
        wl_columnar_memory_reservation_init(&token);
        wl_columnar_memory_reservation_init(&other);
        if (!wl_columnar_memory_reserve(&governor, 600, &token)
            || !wl_columnar_memory_reserve(&governor, 100, &other)
            || !wl_columnar_memory_commit(&token, &governor)) {
            FAIL("race setup");
            return 1;
        }
        atomic_store_explicit(&start, 0, memory_order_relaxed);
        for (unsigned i = 0; i < 3; i++) {
            args[i] = (struct downsize_arg){ &token, &start,
                                             i ==
                                             0 ? 0 : (round % 4 ==
                                             3 ? 1 : round % 3), false };
            if (wl_thread_create(&threads[i], downsize_thread, &args[i]) != 0)
                break;
            created++;
        }
        atomic_store_explicit(&start, 1, memory_order_release);
        for (unsigned i = 0; i < created; i++)
            wl_thread_join(&threads[i]);
        bool released = atomic_load_explicit(&token.state, memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED;
        bool downsize_succeeded = false;
        bool transfer_succeeded = false;
        unsigned releases = 0;
        for (unsigned i = 0; i < created; i++) {
            if (args[i].success && args[i].op == 0)
                downsize_succeeded = true;
            if (args[i].success && args[i].op == 1)
                releases++;
            if (args[i].success && args[i].op == 2)
                transfer_succeeded = true;
        }
        uint64_t expected = 100 + (released ? 0 : token.bytes);
        bool valid = wl_columnar_memory_reserved(&governor) == expected
            && releases == (released ? 1u : 0u)
            && token.bytes == (downsize_succeeded ? 200u : 600u)
            && atomic_load_explicit(&token.owner_bits, memory_order_acquire)
            == (uint64_t)(uintptr_t)(transfer_succeeded
                ? (void *)&start : (void *)&governor);
        if (!released)
            valid = wl_columnar_memory_release(&token) && valid;
        valid = valid && wl_columnar_memory_reserved(&governor) == 100;
        valid = wl_columnar_memory_release(&other) && valid;
        if (created != 3 || !valid
            || wl_columnar_memory_reserved(&governor) != 0) {
            FAIL("concurrent lifecycle leaked or double credited capacity");
            return 1;
        }
    }
    PASS();
    return 0;
}

static int
test_replacement_reservation(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_t governor;
    wl_columnar_memory_reservation_t token;
    wl_columnar_memory_reservation_t other;
    wl_columnar_memory_reservation_t moved;

    TEST("replacement reservation admits overlap and rolls back");
    make_resolution(&resolution, 1000, 900);
    wl_columnar_memory_governor_init(&governor, &resolution);
    wl_columnar_memory_reservation_init(&token);
    wl_columnar_memory_reservation_init(&other);
    wl_columnar_memory_reservation_init(&moved);
    {
        bool a = wl_columnar_memory_reserve(&governor, 400, &token);
        bool b = wl_columnar_memory_commit(&token, &governor);
        int c = wl_columnar_memory_begin_replacement(&token, 200);
        bool d = wl_columnar_memory_reserved(&governor) == 600;
        bool e = atomic_load_explicit(&token.state, memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING;
        bool f = wl_columnar_memory_reservation_move(&moved, &token);
        bool g = atomic_load_explicit(&token.state, memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING;
        bool h = wl_columnar_memory_reserve(&governor, 300, &other);
        bool i = wl_columnar_memory_rollback_replacement(&token);
        bool j = wl_columnar_memory_reserved(&governor) == 700;
        bool k = wl_columnar_memory_release(&token);
        bool l = wl_columnar_memory_release(&other);
        bool m = wl_columnar_memory_reserved(&governor) == 0;
        if (!(a && b && c == WL_COLUMNAR_MEMORY_ADMISSION_OK && d && e
            && !f && g && h && i && j && k && l && m)) {
            FAIL("replacement rollback did not preserve accounting");
            return 1;
        }
    }

    wl_columnar_memory_reservation_init(&token);
    if (!wl_columnar_memory_reserve(&governor, 400, &token)
        || !wl_columnar_memory_commit(&token, &governor)
        || wl_columnar_memory_begin_replacement(&token, 200)
        != WL_COLUMNAR_MEMORY_ADMISSION_OK
        || !wl_columnar_memory_commit_replacement(&token)
        || token.bytes != 200
        || wl_columnar_memory_reserved(&governor) != 200
        || !wl_columnar_memory_release(&token)
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("replacement commit did not publish the new footprint");
        return 1;
    }

    wl_columnar_memory_reservation_init(&token);
    if (!wl_columnar_memory_reserve(&governor, 800, &token)
        || !wl_columnar_memory_commit(&token, &governor)
        || wl_columnar_memory_begin_replacement(&token, 200)
        != WL_COLUMNAR_MEMORY_ADMISSION_DENIED
        || wl_columnar_memory_reserved(&governor) != 800
        || atomic_load_explicit(&token.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || wl_columnar_memory_begin_replacement(&token, 801)
        != WL_COLUMNAR_MEMORY_ADMISSION_INVALID
        || !wl_columnar_memory_release(&token)) {
        FAIL("replacement denial changed the committed reservation");
        return 1;
    }

    TEST("replacement teardown release reclaims stranded overlap");
    wl_columnar_memory_reservation_init(&token);
    if (!wl_columnar_memory_reserve(&governor, 400, &token)
        || !wl_columnar_memory_commit(&token, &governor)
        || wl_columnar_memory_begin_replacement(&token, 200)
        != WL_COLUMNAR_MEMORY_ADMISSION_OK
        || wl_columnar_memory_reserved(&governor) != 600
        || !wl_columnar_memory_release(&token)
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("replacement teardown did not reclaim overlap");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_growth_reservation(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_t governor;
    wl_columnar_memory_reservation_t token;

    TEST("committed growth exact fit, one-byte denial, and rollback");
    make_resolution(&resolution, 1000, 599);
    wl_columnar_memory_governor_init(&governor, &resolution);
    wl_columnar_memory_reservation_init(&token);
    if (!wl_columnar_memory_reserve(&governor, 400, &token)
        || !wl_columnar_memory_commit(&token, &governor)
        || wl_columnar_memory_begin_growth(&token, 200)
        != WL_COLUMNAR_MEMORY_ADMISSION_DENIED
        || token.bytes != 400 || token.replacement_bytes != 0
        || token.transition_kind != WL_COLUMNAR_MEMORY_TRANSITION_NONE
        || atomic_load_explicit(&token.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || wl_columnar_memory_reserved(&governor) != 400) {
        FAIL("one-byte growth denial changed committed credit");
        return 1;
    }
    atomic_store_explicit(&governor.usable_bytes, 600,
        memory_order_release);
    if (wl_columnar_memory_begin_growth(&token, 200)
        != WL_COLUMNAR_MEMORY_ADMISSION_OK
        || token.bytes != 400 || token.replacement_bytes != 200
        || token.transition_kind != WL_COLUMNAR_MEMORY_TRANSITION_GROWTH
        || wl_columnar_memory_reserved(&governor) != 600
        || wl_columnar_memory_commit_replacement(&token)
        || wl_columnar_memory_rollback_replacement(&token)
        || !wl_columnar_memory_commit_growth(&token)
        || token.bytes != 600 || token.replacement_bytes != 0
        || token.transition_kind != WL_COLUMNAR_MEMORY_TRANSITION_NONE
        || wl_columnar_memory_reserved(&governor) != 600
        || !wl_columnar_memory_reservation_downsize(&token, 450)
        || wl_columnar_memory_reserved(&governor) != 450
        || !wl_columnar_memory_release(&token)
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("growth commit/downsize or wrong-kind guard failed");
        return 1;
    }
    wl_columnar_memory_reservation_init(&token);
    if (!wl_columnar_memory_reserve(&governor, 400, &token)
        || !wl_columnar_memory_commit(&token, &governor)
        || wl_columnar_memory_begin_growth(&token, 200)
        != WL_COLUMNAR_MEMORY_ADMISSION_OK
        || !wl_columnar_memory_rollback_growth(&token)
        || token.bytes != 400 || token.replacement_bytes != 0
        || token.transition_kind != WL_COLUMNAR_MEMORY_TRANSITION_NONE
        || wl_columnar_memory_reserved(&governor) != 400
        || wl_columnar_memory_begin_replacement(&token, 200)
        != WL_COLUMNAR_MEMORY_ADMISSION_OK
        || wl_columnar_memory_commit_growth(&token)
        || wl_columnar_memory_rollback_growth(&token)
        || !wl_columnar_memory_rollback_replacement(&token)
        || !wl_columnar_memory_release(&token)
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("growth rollback or replacement kind isolation failed");
        return 1;
    }
    PASS();

    TEST("growth advisory and stranded-overlap teardown");
    make_resolution(&resolution, 1000, 600);
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ADVISORY;
    wl_columnar_memory_governor_init(&governor, &resolution);
    wl_columnar_memory_reservation_init(&token);
    if (!wl_columnar_memory_reserve(&governor, 400, &token)
        || !wl_columnar_memory_commit(&token, &governor)
        || wl_columnar_memory_begin_growth(&token, 200)
        != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY
        || !wl_columnar_memory_release(&token)
        || token.transition_kind != WL_COLUMNAR_MEMORY_TRANSITION_NONE
        || token.replacement_bytes != 0
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("advisory growth or overlap release lost credit");
        return 1;
    }
    PASS();

    TEST("growth token and governor arithmetic overflow");
    make_resolution(&resolution, UINT64_MAX, UINT64_MAX);
    wl_columnar_memory_governor_init(&governor, &resolution);
    wl_columnar_memory_reservation_init(&token);
    if (!wl_columnar_memory_reserve(&governor, UINT64_MAX - 5u, &token)
        || !wl_columnar_memory_commit(&token, &governor)
        || wl_columnar_memory_begin_growth(&token, 6u)
        != WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
        || token.bytes != UINT64_MAX - 5u
        || token.transition_kind != WL_COLUMNAR_MEMORY_TRANSITION_NONE
        || wl_columnar_memory_reserved(&governor) != UINT64_MAX - 5u
        || !wl_columnar_memory_release(&token)) {
        FAIL("token arithmetic overflow changed credit");
        return 1;
    }
    wl_columnar_memory_reservation_t other;
    wl_columnar_memory_reservation_init(&token);
    wl_columnar_memory_reservation_init(&other);
    if (!wl_columnar_memory_reserve(&governor, 10u, &token)
        || !wl_columnar_memory_commit(&token, &governor)
        || !wl_columnar_memory_reserve(&governor, UINT64_MAX - 15u,
        &other)
        || wl_columnar_memory_begin_growth(&token, 6u)
        != WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
        || wl_columnar_memory_reserved(&governor) != UINT64_MAX - 5u
        || !wl_columnar_memory_release(&token)
        || !wl_columnar_memory_rollback(&other)
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("global arithmetic overflow changed credit");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_growth_refund_attempt(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_t governor;
    wl_columnar_memory_reservation_t token;
    wl_columnar_memory_growth_refund_attempt_t attempt = { 0 }, copy;
    wl_columnar_memory_growth_refund_attempt_t wrong_owner_attempt = { 0 };
    struct growth_refund_thread_arg thread_arg = { &attempt, 0 };
    wl_thread_t thread;

    TEST(
        "bounded growth overlap refund resumes after aggregate CAS contention");
    make_resolution(&resolution, 1000, 900);
    wl_columnar_memory_governor_init(&governor, &resolution);
    wl_columnar_memory_reservation_init(&token);
    growth_refund_peer_created = false;
    wl_columnar_memory_reservation_init(&growth_refund_peer);
    if (!wl_columnar_memory_reserve(&governor, 100, &token)
        || !wl_columnar_memory_commit(&token, &token)
        || wl_columnar_memory_begin_growth(&token, 20)
        != WL_COLUMNAR_MEMORY_ADMISSION_OK
        || wl_columnar_memory_reserved(&governor) != 120
        || wl_columnar_memory_growth_refund_attempt_begin(&token,
        &governor, &wrong_owner_attempt) != EINVAL
        || wl_columnar_memory_reserved(&governor) != 120
        || wl_columnar_memory_growth_refund_attempt_begin(&token, &token,
        &attempt) != 0) {
        FAIL("growth refund setup or admission failed");
        return 1;
    }
    copy = attempt;
    if (wl_columnar_memory_growth_refund_attempt_advance(&copy) != EINVAL
        || wl_thread_create(&thread, growth_refund_advance_on_thread,
        &thread_arg) != 0
        || wl_thread_join(&thread) != 0 || thread_arg.result != EINVAL
        || wl_columnar_memory_growth_refund_attempt_advance(&attempt) != EAGAIN
        || attempt.phase != WL_COLUMNAR_MEMORY_GROWTH_REFUND_CLAIM
        || wl_columnar_memory_growth_refund_attempt_advance(&attempt) != EAGAIN
        || attempt.phase != WL_COLUMNAR_MEMORY_GROWTH_REFUND_CREDIT
        || atomic_load_explicit(&token.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_GROWTH_REFUNDING
        || wl_columnar_memory_rollback_growth(&token)
        || atomic_load_explicit(&token.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_GROWTH_REFUNDING) {
        FAIL("refund receipt validation/claim phases failed");
        return 1;
    }
    atomic_store_explicit(&governor.reserved_bytes, 10, memory_order_release);
    if (wl_columnar_memory_growth_refund_attempt_advance(&attempt) != EINVAL
        || atomic_load_explicit(&token.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_GROWTH_REFUNDING
        || token.replacement_bytes != 20) {
        FAIL("refund underflow did not retain the claimed token");
        return 1;
    }
    atomic_store_explicit(&governor.reserved_bytes, 120, memory_order_release);
    wl_columnar_memory_growth_refund_test_before_credit_cas
        = growth_refund_add_peer_reservation;
    int rc = wl_columnar_memory_growth_refund_attempt_advance(&attempt);
    wl_columnar_memory_growth_refund_test_before_credit_cas = NULL;
    if (rc != EAGAIN || !growth_refund_peer_created
        || wl_columnar_memory_reserved(&governor) != 130
        || token.bytes != 100 || token.replacement_bytes != 20
        || token.transition_kind != WL_COLUMNAR_MEMORY_TRANSITION_GROWTH
        || atomic_load_explicit(&token.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_GROWTH_REFUNDING) {
        FAIL("aggregate interference lost or duplicated overlap credit");
        return 1;
    }
    if (wl_columnar_memory_growth_refund_attempt_advance(&attempt) != 0
        || attempt.phase != WL_COLUMNAR_MEMORY_GROWTH_REFUND_DONE
        || token.bytes != 100 || token.replacement_bytes != 0
        || token.transition_kind != WL_COLUMNAR_MEMORY_TRANSITION_NONE
        || atomic_load_explicit(&token.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || wl_columnar_memory_reserved(&governor) != 110
        || wl_columnar_memory_growth_refund_attempt_advance(&attempt) != 0
        || wl_columnar_memory_reserved(&governor) != 110) {
        FAIL("refund did not restore old credit exactly once");
        return 1;
    }
    if (!wl_columnar_memory_release(&growth_refund_peer)
        || !wl_columnar_memory_release(&token)
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("refund test reservations did not retire");
        return 1;
    }
    PASS();
    return 0;
}

static void
downsize_add_peer_reservation(wl_columnar_memory_governor_t *governor)
{
    growth_refund_add_peer_reservation(governor);
}

static int
test_committed_downsize_attempt(void)
{
    wl_columnar_memory_resolution_t resolution;
    wl_columnar_memory_governor_t governor;
    wl_columnar_memory_reservation_t token, invalid_token;
    wl_columnar_memory_downsize_attempt_t attempt = { 0 }, copy, loser = { 0 };
    wl_columnar_memory_downsize_attempt_t invalid = { 0 };
    struct downsize_thread_arg thread_arg = { &attempt, 0 };
    wl_thread_t thread;

    TEST("bounded committed footprint reduction resumes after CAS contention");
    make_resolution(&resolution, 1000, 900);
    wl_columnar_memory_governor_init(&governor, &resolution);
    wl_columnar_memory_reservation_init(&token);
    growth_refund_peer_created = false;
    wl_columnar_memory_reservation_init(&growth_refund_peer);
    if (!wl_columnar_memory_reserve(&governor, 100, &token)
        || !wl_columnar_memory_commit(&token, &token)
        || wl_columnar_memory_downsize_attempt_begin(&token, &token, 0,
        &attempt) != EINVAL
        || wl_columnar_memory_reserved(&governor) != 100
        || wl_columnar_memory_downsize_attempt_begin(&token, &governor, 40,
        &attempt) != EINVAL
        || wl_columnar_memory_downsize_attempt_begin(&token, &token, 40,
        &attempt) != 0
        || wl_columnar_memory_downsize_attempt_begin(&token, &token, 40,
        &loser) != 0) {
        FAIL("downsize receipt setup or zero/owner validation failed");
        return 1;
    }
    copy = attempt;
    if (wl_columnar_memory_downsize_attempt_advance(&copy) != EINVAL
        || wl_thread_create(&thread, downsize_advance_on_thread,
        &thread_arg) != 0
        || wl_thread_join(&thread) != 0 || thread_arg.result != EINVAL
        || wl_columnar_memory_downsize_attempt_advance(&attempt) != EAGAIN
        || attempt.phase != WL_COLUMNAR_MEMORY_DOWNSIZE_CREDIT
        || wl_columnar_memory_downsize_attempt_advance(&loser) != EBUSY
        || loser.phase != WL_COLUMNAR_MEMORY_DOWNSIZE_FAILED
        || atomic_load_explicit(&token.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_DOWNSIZING
        || wl_columnar_memory_release(&token)
        || wl_columnar_memory_reservation_downsize(&token, 50)) {
        FAIL("downsize claim did not exclude ordinary token operations");
        return 1;
    }
    atomic_store_explicit(&governor.reserved_bytes, 30, memory_order_release);
    if (wl_columnar_memory_downsize_attempt_advance(&attempt) != EINVAL
        || token.bytes != 100
        || atomic_load_explicit(&token.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_DOWNSIZING) {
        FAIL("aggregate underflow did not retain old token authority");
        return 1;
    }
    atomic_store_explicit(&governor.reserved_bytes, 100, memory_order_release);
    wl_columnar_memory_downsize_test_before_credit_cas
        = downsize_add_peer_reservation;
    int rc = wl_columnar_memory_downsize_attempt_advance(&attempt);
    wl_columnar_memory_downsize_test_before_credit_cas = NULL;
    if (rc != EAGAIN || !growth_refund_peer_created
        || wl_columnar_memory_reserved(&governor) != 110
        || token.bytes != 100
        || atomic_load_explicit(&token.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_DOWNSIZING) {
        FAIL("aggregate CAS contention changed committed bytes");
        return 1;
    }
    if (wl_columnar_memory_downsize_attempt_advance(&attempt) != 0
        || attempt.phase != WL_COLUMNAR_MEMORY_DOWNSIZE_DONE
        || token.bytes != 40
        || atomic_load_explicit(&token.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || wl_columnar_memory_reserved(&governor) != 50
        || wl_columnar_memory_downsize_attempt_advance(&attempt) != 0
        || wl_columnar_memory_reserved(&governor) != 50
        || !wl_columnar_memory_release(&growth_refund_peer)
        || !wl_columnar_memory_release(&token)
        || wl_columnar_memory_reserved(&governor) != 0
        || !wl_columnar_memory_reserve(&governor, 7, &token)
        || !wl_columnar_memory_commit(&token, &token)
        || wl_columnar_memory_downsize_attempt_advance(&attempt) != 0
        || token.bytes != 7 || wl_columnar_memory_reserved(&governor) != 7
        || !wl_columnar_memory_release(&token)
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("downsize receipt did not reduce committed credit exactly once");
        return 1;
    }
    wl_columnar_memory_reservation_init(&invalid_token);
    if (!wl_columnar_memory_reserve(&governor, 20, &invalid_token)
        || !wl_columnar_memory_commit(&invalid_token, &invalid_token)
        || wl_columnar_memory_downsize_attempt_begin(&invalid_token,
        &invalid_token, 20, &invalid) != 0
        || wl_columnar_memory_downsize_attempt_advance(&invalid) != EINVAL
        || invalid.phase != WL_COLUMNAR_MEMORY_DOWNSIZE_FAILED
        || invalid_token.bytes != 20
        || atomic_load_explicit(&invalid_token.state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || !wl_columnar_memory_release(&invalid_token)
        || wl_columnar_memory_reserved(&governor) != 0) {
        FAIL("invalid target failed to restore the committed token");
        return 1;
    }
    PASS();
    return 0;
}

struct growth_race_arg {
    wl_columnar_memory_reservation_t *token;
    wl_atomic_u64 *start;
    unsigned op;
    bool success;
};

static void *
growth_race_thread(void *ptr)
{
    struct growth_race_arg *arg = ptr;
    while (!atomic_load_explicit(arg->start, memory_order_acquire)) {
    }
    if (arg->op == 0)
        arg->success = wl_columnar_memory_commit_growth(arg->token);
    else if (arg->op == 1)
        arg->success = wl_columnar_memory_rollback_growth(arg->token);
    else
        arg->success = wl_columnar_memory_release(arg->token);
    return NULL;
}

static int
test_growth_races(void)
{
    TEST("growth commit, rollback, and release conserve credit under race");
    for (unsigned round = 0; round < 100; round++) {
        wl_columnar_memory_resolution_t resolution;
        wl_columnar_memory_governor_t governor;
        wl_columnar_memory_reservation_t token;
        wl_atomic_u64 start;
        wl_thread_t threads[3];
        struct growth_race_arg args[3];
        unsigned created = 0;
        make_resolution(&resolution, 1000, 900);
        wl_columnar_memory_governor_init(&governor, &resolution);
        wl_columnar_memory_reservation_init(&token);
        if (!wl_columnar_memory_reserve(&governor, 400, &token)
            || !wl_columnar_memory_commit(&token, &governor)
            || wl_columnar_memory_begin_growth(&token, 200)
            != WL_COLUMNAR_MEMORY_ADMISSION_OK) {
            FAIL("growth race setup");
            return 1;
        }
        atomic_store_explicit(&start, 0, memory_order_relaxed);
        for (unsigned i = 0; i < 3; i++) {
            args[i] = (struct growth_race_arg){ &token, &start, i, false };
            if (wl_thread_create(&threads[i], growth_race_thread,
                &args[i]) != 0)
                break;
            created++;
        }
        atomic_store_explicit(&start, 1, memory_order_release);
        for (unsigned i = 0; i < created; i++)
            wl_thread_join(&threads[i]);
        uint64_t state = atomic_load_explicit(&token.state,
                memory_order_acquire);
        bool valid = created == 3
            && (unsigned)args[0].success + (unsigned)args[1].success <= 1u;
        if (state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED) {
            uint64_t expected = args[0].success ? 600u : 400u;
            valid = valid && token.bytes == expected
                && token.transition_kind == WL_COLUMNAR_MEMORY_TRANSITION_NONE
                && wl_columnar_memory_reserved(&governor) == expected;
            valid = wl_columnar_memory_release(&token) && valid;
        } else {
            valid = valid && state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED
                && args[2].success
                && wl_columnar_memory_reserved(&governor) == 0;
        }
        if (!valid || wl_columnar_memory_reserved(&governor) != 0) {
            FAIL("growth terminal race leaked or double credited");
            return 1;
        }
    }
    PASS();
    return 0;
}

#define WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(x) do { if (!(x)) { fprintf(stderr, \
                                                                  "controlled governor:%d: %s\n", \
                                                                  __LINE__, #x); \
                                                              exit(1); \
                                                  } } while (0)
static int controlled_owner;
static uint64_t controlled_work, controlled_cancel_at;
static unsigned controlled_phase, controlled_hits;
static int controlled_fault;
static bool prepared_trace_enabled;
static uint64_t prepared_trace[5];
static unsigned prepared_trace_count;
static unsigned prepared_phase_count, prepared_phase_at_fault;
static unsigned prepared_work_at_fault;
static wl_columnar_memory_governor_growth_commit_prepared_t *prepared_witness;
static void
controlled_work_point(wl_evaluation_control_t *c, uint64_t work)
{
    (void)c;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(work <= WL_EVALUATION_CONTROL_QUANTUM);
    if (prepared_trace_enabled) {
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(prepared_trace_count < 5);
        prepared_trace[prepared_trace_count++] = work;
    }
    controlled_work += work;
    if (controlled_cancel_at && controlled_work >= controlled_cancel_at)
        wl_evaluation_control_request_cancel(c);
}
static void
controlled_point(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    if (prepared_trace_enabled)
        ++prepared_phase_count;
    if (prepared_witness &&
        phase == WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_CLAIMED) {
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(prepared_witness->phase
            == WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_CLAIMED);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(prepared_witness->token == token
            && prepared_witness->prior_bytes == 8
            && prepared_witness->additional_bytes == 16
            && prepared_witness->final_bytes == 24
            && prepared_witness->thread_valid);
    }
    if (phase != controlled_phase)
        return;
    controlled_hits++;
    if (prepared_trace_enabled) {
        prepared_phase_at_fault = prepared_phase_count;
        prepared_work_at_fault = prepared_trace_count;
    }
    if (controlled_fault == 1)
        wl_evaluation_control_request_cancel(c);
    else if (controlled_fault == 2)
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else if (controlled_fault == 3 || controlled_fault == 4) {
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_finish(c,
            &controlled_owner, 0, 0) == 0);
        wl_evaluation_control_report_t report;
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_report(c,
            &report) == 0 && report.consumed == 228);
        if (controlled_fault == 3) {
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_reset(c) ==
                0);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_begin(c,
                &controlled_owner) == 0);
        }
    }
}
static wl_evaluation_control_t *
controlled_begin(uint64_t limit)
{
    wl_evaluation_control_t *c = NULL;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_create(limit,
        &c) == 0);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_attach(c,
        &controlled_owner) == 0);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_begin(c,
        &controlled_owner) == 0);
    controlled_work = 0; controlled_hits = 0; controlled_cancel_at = 0;
    prepared_phase_count = 0; prepared_phase_at_fault = 0;
    prepared_work_at_fault = 0;
    return c;
}
static uint64_t
controlled_end(wl_evaluation_control_t *c, int rc)
{
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_finish(c,
        &controlled_owner, rc > 0 ? rc : 0, 0) == rc);
    wl_evaluation_control_report_t report;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_report(c,
        &report) == 0 && report.result == rc);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_detach(c,
        &controlled_owner) == 0);
    wl_evaluation_control_release(c);
    return report.consumed;
}
static void
controlled_token_equal(wl_columnar_memory_reservation_t *token,
    const wl_columnar_memory_reservation_t *prior)
{
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(token->governor == prior->governor &&
        token->bytes == prior->bytes
        && token->replacement_bytes == prior->replacement_bytes
        && token->transition_kind == prior->transition_kind
        && atomic_load_explicit(&token->state,
        memory_order_relaxed) == atomic_load_explicit(&prior->state,
        memory_order_relaxed)
        && atomic_load_explicit(&token->owner_bits,
        memory_order_relaxed) == atomic_load_explicit(&prior->owner_bits,
        memory_order_relaxed)
        && token->identity == token);
}
static void
controlled_governor(wl_columnar_memory_governor_t *g, uint64_t limit)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = limit, .usable_bytes = limit,
        .mode = WL_COLUMNAR_MEMORY_MODE_ADVISORY,
    };
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_governor_init(g,
        &resolution) == 0);
}
static void
test_controlled_reserve_lifecycle(void)
{
    TEST("controlled fresh/released stop lifecycle and exact work");
    wl_columnar_memory_governor_t g; controlled_governor(&g, 1024);
    for (int released = 0; released < 2; released++) {
        wl_columnar_memory_reservation_t token;
        wl_columnar_memory_reservation_init(&token);
        if (released) {
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                wl_columnar_memory_reserve_checked(&g, 17,
                &token) == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_commit(&token,
                &controlled_owner));
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_release(
                    &token));
        }
        wl_columnar_memory_reservation_t prior = token; /* snapshot only, never an operation token */
        const unsigned phases[] = {WL_COLUMNAR_MEMORY_GOVERNOR_VALIDATED,
                                   WL_COLUMNAR_MEMORY_GOVERNOR_CLAIMED,
                                   WL_COLUMNAR_MEMORY_GOVERNOR_SNAPSHOT,
                                   WL_COLUMNAR_MEMORY_GOVERNOR_PREPARED,
                                   WL_COLUMNAR_MEMORY_GOVERNOR_ADMITTED,
                                   WL_COLUMNAR_MEMORY_GOVERNOR_RESERVED};
        for (int fault = 1; fault <= 2; fault++)
            for (unsigned i = 0; i < sizeof(phases) / sizeof(phases[0]); i++) {
                controlled_phase = phases[i]; controlled_fault = fault;
                wl_evaluation_control_t *c = controlled_begin(0);
                wl_columnar_memory_admission_status_t status;
                int rc =
                    wl_columnar_memory_governor_reserve_checked_controlled(&g,
                        31,
                        &token, &status, c);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc ==
                    (fault == 1 ? WL_EVALUATION_CONTROL_CANCELLED : EOVERFLOW));
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_hits == 1 &&
                    wl_columnar_memory_reserved(&g) == 0);
                controlled_token_equal(&token, &prior);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c, rc) > 0);
            }
        controlled_phase = 0; controlled_fault = 0;
        uint64_t cost = released ? 16 : 15;
        wl_evaluation_control_t *c = controlled_begin(cost - 1);
        wl_columnar_memory_admission_status_t status;
        int rc = wl_columnar_memory_governor_reserve_checked_controlled(&g, 31,
                &token, &status, c);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
        controlled_token_equal(&token, &prior);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(&g) == 0);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c, rc) == cost - 1);
        c = controlled_begin(cost);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
            wl_columnar_memory_governor_reserve_checked_controlled(&g, 31,
            &token, &status, c) == 0
            && status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c,
            0) == cost && controlled_work == cost);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_rollback(&token));
    }
    PASS();
}

static wl_columnar_memory_reservation_t independent;
static uint64_t competition_base;
static unsigned competition_count;
static bool competition_stop;
static void
controlled_before_total(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *g, uint64_t expected, bool overflow)
{
    (void)c; WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(!overflow);
    uint64_t amount = expected == competition_base + 1 ? 2 : 1;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve_checked(g,
        amount, &independent)
        == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(g) !=
        expected);
}
static void
controlled_after_total(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *g, bool success, bool overflow)
{
    (void)g; WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(!success && !overflow);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_rollback(
            &independent));
    if (++competition_count == 300) {
        wl_columnar_memory_governor_test_before_total_cas = NULL;
        wl_columnar_memory_governor_test_after_total_cas = NULL;
        if (competition_stop) wl_evaluation_control_request_cancel(c);
    }
}
static void
test_controlled_contention(void)
{
    TEST("controlled 300 genuine shared-credit CAS failures");
    wl_columnar_memory_governor_t g; controlled_governor(&g, 1024);
    wl_columnar_memory_reservation_t held, token;
    wl_columnar_memory_reservation_init(&held);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve(&g, 32,
        &held)); competition_base = 32;
    for (int stop = 0; stop < 2; stop++) {
        wl_columnar_memory_reservation_init(&token);
        wl_columnar_memory_reservation_init(&independent);
        competition_count = 0; competition_stop = stop != 0;
        wl_columnar_memory_governor_test_before_total_cas =
            controlled_before_total;
        wl_columnar_memory_governor_test_after_total_cas =
            controlled_after_total;
        wl_evaluation_control_t *c = controlled_begin(0);
        wl_columnar_memory_admission_status_t status;
        int rc = wl_columnar_memory_governor_reserve_checked_controlled(&g, 16,
                &token, &status, c);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(competition_count == 300);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc ==
            (stop ? WL_EVALUATION_CONTROL_CANCELLED : 0));
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c,
            rc) == (stop ? 313u : 316u));
        if (!stop) WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                wl_columnar_memory_rollback(&token));
        else WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(atomic_load_explicit(
                    &token.state, memory_order_relaxed) ==
                WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(&g) ==
            32);
    }
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_rollback(&held));
    PASS();
}

static void
test_controlled_growth_and_commit(void)
{
    TEST("controlled full-footprint growth and physical-credit commit cutoff");
    wl_columnar_memory_governor_t g; controlled_governor(&g, 1024);
    wl_columnar_memory_reservation_t old, fresh;
    wl_columnar_memory_reservation_init(&old);
    wl_columnar_memory_reservation_init(&fresh);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve(&g, 32, &old));
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_commit(&old, NULL));
    wl_evaluation_control_t *c = controlled_begin(19);
    wl_columnar_memory_admission_status_t status;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_reserve_growth_controlled(&g, 32, 48,
        &fresh, &status, c) == 0);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(status ==
        WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY &&
        wl_columnar_memory_reserved(&g) == 80);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c, 0) == 19);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_rollback(&fresh));
    for (int growth = 0; growth < 2; growth++) {
        wl_columnar_memory_reservation_t *token = growth ? &old : &fresh;
        if (!growth) WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                wl_columnar_memory_reserve(&g, 16, token));
        const unsigned phases[] = {WL_COLUMNAR_MEMORY_GOVERNOR_COMMITTING,
                                   WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF};
        if (growth) {
            wl_columnar_memory_reservation_t prior = old;
            const unsigned admit_phases[] =
            {WL_COLUMNAR_MEMORY_GOVERNOR_CLAIMED,
             WL_COLUMNAR_MEMORY_GOVERNOR_SNAPSHOT,
             WL_COLUMNAR_MEMORY_GOVERNOR_ADMITTED,
             WL_COLUMNAR_MEMORY_GOVERNOR_RESERVED};
            for (unsigned i = 0; i < 4; i++) {
                controlled_phase = admit_phases[i]; controlled_fault = 1;
                c = controlled_begin(0);
                int rc =
                    wl_columnar_memory_governor_begin_growth_controlled(&old,
                        16, &status, c);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc ==
                    WL_EVALUATION_CONTROL_CANCELLED && controlled_hits == 1);
                controlled_token_equal(&old, &prior);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(
                        &g) == 32);
                controlled_end(c, rc);
            }
            controlled_phase = 0; controlled_fault = 0;
            c = controlled_begin(15);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                wl_columnar_memory_governor_begin_growth_controlled(&old, 16,
                &status, c) == 0);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c, 0) == 15);
        }
        void *physical = malloc(16);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(physical); memset(physical, 0x5a, 16);
        wl_columnar_memory_reservation_t prior = *token;
        uint64_t credit = wl_columnar_memory_reserved(&g);
        for (unsigned i = 0; i < 2; i++) {
            controlled_phase = phases[i]; controlled_fault = 1;
            c = controlled_begin(0); bool committed;
            int rc =
                growth ?
                wl_columnar_memory_governor_commit_growth_controlled(token,
                    &committed, c)
                : wl_columnar_memory_governor_commit_controlled(token, NULL,
                    &committed, c);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc ==
                WL_EVALUATION_CONTROL_CANCELLED && controlled_hits == 1);
            controlled_token_equal(token, &prior);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(((unsigned char *)physical)[0] ==
                0x5a && wl_columnar_memory_reserved(&g) == credit);
            controlled_end(c, rc);
        }
        controlled_phase = 0; controlled_fault = 0;
        c = controlled_begin(growth ? 9 : 4); bool committed;
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK((growth ?
            wl_columnar_memory_governor_commit_growth_controlled(token,
            &committed, c)
            : wl_columnar_memory_governor_commit_controlled(token, NULL,
            &committed, c)) == 0 && committed);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c,
            0) == (growth ? 9 : 4));
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(atomic_load_explicit(
                &token->owner_bits, memory_order_relaxed) == 0);                                                /* real ownerNULL compatibility */
        free(physical);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_release(token));
    }
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(&g) == 0);
    PASS();
}

static void
test_controlled_verdict_precedence(void)
{
    TEST(
        "controlled typed verdicts retain negative stop, positive accounting overrides");
    for (unsigned which = 0; which < 5; which++)
        for (int fault = 1; fault <= 2; fault++) {
            wl_columnar_memory_governor_t g;
            controlled_governor(&g, which == 1 ? UINT64_MAX : 64);
            wl_columnar_memory_reservation_t held, token;
            wl_columnar_memory_reservation_init(&held);
            wl_columnar_memory_reservation_init(&token);
            uint64_t held_bytes = which == 1 ? UINT64_MAX - 1 : 64;
            if (which < 2)
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve(&g,
                    held_bytes, &held));
            if (which == 2) {
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve(&g,
                    16, &token));
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_commit(
                        &token, NULL));
            }
            wl_columnar_memory_reservation_t prior = token;
            uint64_t credit = wl_columnar_memory_reserved(&g);
            controlled_phase = WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT;
            controlled_fault = fault;
            wl_evaluation_control_t *c = controlled_begin(0);
            wl_columnar_memory_admission_status_t status;
            bool committed = true;
            int rc;
            if (which < 3)
                rc = wl_columnar_memory_governor_reserve_checked_controlled(&g,
                        which == 1 ? 2 : 16, &token, &status, c);
            else if (which == 3)
                rc = wl_columnar_memory_governor_begin_growth_controlled(&token,
                        16, &status, c);
            else
                rc = wl_columnar_memory_governor_commit_controlled(&token, NULL,
                        &committed, c);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_hits == 1);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(&g) ==
                credit);
            if (fault == 2) {
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc == EOVERFLOW);
                controlled_token_equal(&token, &prior);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c, rc) > 0);
            } else {
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc == 0);
                if (which < 2) {
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(status ==
                        (which == 0 ? WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                        : WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW));
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(token.governor == &g &&
                        token.bytes == (which == 1 ? 2u : 16u)
                        && atomic_load_explicit(&token.state,
                        memory_order_relaxed) ==
                        WL_COLUMNAR_MEMORY_RESERVATION_RELEASED);
                } else {
                    if (which ==
                        4) WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(!committed);
                    else WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(status ==
                            WL_COLUMNAR_MEMORY_ADMISSION_INVALID);
                    controlled_token_equal(&token, &prior);
                }
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c,
                    WL_EVALUATION_CONTROL_CANCELLED) > 0);
            }
            if (which <
                2) WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                    wl_columnar_memory_rollback(&held));
            if (which ==
                2) WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_release(
                        &token));
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(&g) ==
                0);
        }
    controlled_phase = 0; controlled_fault = 0;
    PASS();
}

static unsigned overflow_attempts;
static void
controlled_overflow_retry(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *g, uint64_t expected, bool overflow)
{
    (void)c;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(overflow && expected == UINT64_MAX - 1);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_rollback(
            &independent));
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(g) == 0);
}
static void
controlled_overflow_after(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *g, bool success, bool overflow)
{
    (void)c; (void)g;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(!success && overflow);
    overflow_attempts++;
    wl_columnar_memory_governor_test_before_total_cas = NULL;
    wl_columnar_memory_governor_test_after_total_cas = NULL;
}
static void
test_controlled_overflow_retry_and_boundaries(void)
{
    TEST("controlled real overflow CAS retry and retained exact cutoffs");
    controlled_phase = 0; controlled_fault = 0;
    wl_columnar_memory_governor_t g; controlled_governor(&g, UINT64_MAX);
    wl_columnar_memory_reservation_t token;
    wl_columnar_memory_reservation_init(&token);
    wl_columnar_memory_reservation_init(&independent);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve(&g,
        UINT64_MAX - 1, &independent));
    wl_columnar_memory_governor_test_before_total_cas =
        controlled_overflow_retry;
    wl_columnar_memory_governor_test_after_total_cas =
        controlled_overflow_after;
    overflow_attempts = 0;
    wl_evaluation_control_t *c = controlled_begin(16);
    wl_columnar_memory_admission_status_t status;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_reserve_checked_controlled(
            &g, 2, &token, &status, c) == 0 && overflow_attempts == 1);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(status ==
        WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c, 0) == 16);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_rollback(&token));
    for (int growth = 0; growth < 2; growth++) {
        wl_columnar_memory_reservation_init(&token);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve(&g, 16,
            &token));
        if (growth) {
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_commit(&token,
                NULL));
            wl_columnar_memory_reservation_t prior = token;
            c = controlled_begin(14);
            int rc = wl_columnar_memory_governor_begin_growth_controlled(&token,
                    16, &status, c);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc ==
                WL_EVALUATION_CONTROL_WORK_LIMIT);
            controlled_token_equal(&token, &prior);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(&g) ==
                16);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c, rc) == 12);
            c = controlled_begin(15);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                wl_columnar_memory_governor_begin_growth_controlled(
                    &token, 16, &status, c) == 0);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c, 0) == 15);
        }
        wl_columnar_memory_reservation_t prior = token;
        void *physical = malloc(16);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(physical);
        memset(physical, 0x6b, 16);
        uint64_t credit = wl_columnar_memory_reserved(&g);
        uint64_t cost = growth ? 9 : 4;
        c = controlled_begin(cost - 1); bool committed;
        int rc =
            growth ?
            wl_columnar_memory_governor_commit_growth_controlled(&token,
                &committed, c)
            : wl_columnar_memory_governor_commit_controlled(&token, NULL,
                &committed, c);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
        controlled_token_equal(&token, &prior);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(&g) ==
            credit
            && ((unsigned char *)physical)[0] == 0x6b);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c,
            rc) == (growth ? 5u : 2u));
        /* Stopped commit backs a live unpublished image until its actual free. */
        free(physical);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(growth ?
            wl_columnar_memory_rollback_growth(&token)
            : wl_columnar_memory_rollback(&token));
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(&g) ==
            (growth ? 16u : 0u));
        if (growth) WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                wl_columnar_memory_release(&token));
    }
    PASS();
}

static void
test_controlled_all_region_boundaries(void)
{
    TEST(
        "every controlled region refusal and tail preserves original authority");
    const uint64_t regions[5][7] = {
        {3, 1, 5, 4, 1, 1, 0}, {3, 1, 1, 5, 4, 1, 1},
        {2, 1, 5, 3, 1, 3, 0}, {1, 1, 2, 0, 0, 0, 0},
        {1, 1, 3, 4, 0, 0, 0},
    };
    const uint64_t totals[] = {15, 16, 15, 4, 9};
    controlled_phase = 0; controlled_fault = 0;
    for (unsigned kind = 0; kind < 5; kind++) {
        for (unsigned cancel = 0; cancel < 2; cancel++) {
            for (uint64_t boundary = 0; boundary <= totals[kind]; boundary++) {
                uint64_t prefix = 0;
                bool endpoint = boundary == 0;
                for (unsigned i = 0; i < 7 && regions[kind][i]; i++) {
                    if (prefix + regions[kind][i] > boundary)
                        break;
                    prefix += regions[kind][i];
                    endpoint = prefix == boundary;
                }
                if ((!cancel && (boundary == 0 || boundary == totals[kind])) ||
                    (cancel && !endpoint))
                    continue;
                wl_columnar_memory_governor_t g;
                controlled_governor(&g, 128);
                wl_columnar_memory_reservation_t token, held;
                wl_columnar_memory_reservation_init(&token);
                wl_columnar_memory_reservation_init(&held);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve(&g,
                    7, &held));
                if (kind) {
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                        wl_columnar_memory_reserve(&g, 16, &token));
                    if (kind != 3)
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                            wl_columnar_memory_commit(&token,
                            &controlled_owner));
                    if (kind == 1)
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                            wl_columnar_memory_release(&token));
                    if (kind == 4)
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                            wl_columnar_memory_begin_growth(&token,
                            8) == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY);
                }
                wl_columnar_memory_reservation_t prior = token;
                uint64_t credit = wl_columnar_memory_reserved(&g);
                wl_evaluation_control_t *c =
                    controlled_begin(cancel ? 0 : boundary);
                if (cancel) {
                    if (boundary == 0)
                        wl_evaluation_control_request_cancel(c);
                    else
                        controlled_cancel_at = boundary;
                }
                wl_columnar_memory_admission_status_t status;
                bool committed = false;
                int rc;
                if (kind < 2)
                    rc =
                        wl_columnar_memory_governor_reserve_checked_controlled(
                        &g, 16, &token, &status, c);
                else if (kind == 2)
                    rc =
                        wl_columnar_memory_governor_begin_growth_controlled(
                        &token, 8, &status, c);
                else if (kind == 3)
                    rc = wl_columnar_memory_governor_commit_controlled(&token,
                            NULL, &committed, c);
                else
                    rc =
                        wl_columnar_memory_governor_commit_growth_controlled(
                        &token, &committed, c);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc ==
                    (cancel ? WL_EVALUATION_CONTROL_CANCELLED :
                    WL_EVALUATION_CONTROL_WORK_LIMIT));
                controlled_token_equal(&token, &prior);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(
                        &g) == credit);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c,
                    rc) == prefix && controlled_work == prefix);
                if (kind == 4)
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                        wl_columnar_memory_rollback_growth(&token));
                if (kind == 2 || kind == 4)
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                        wl_columnar_memory_release(&token));
                if (kind == 3)
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                        wl_columnar_memory_rollback(&token));
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(
                        &g) == 7);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_rollback(
                        &held));
            }
        }
    }
    PASS();
}

static void
test_controlled_raw_admission_twins(void)
{
    TEST(
        "controlled/raw reserve twins retain typed state and advisory semantics");
    const uint64_t requests[] = {0, 0, 8, 65, UINT64_MAX};
    const uint64_t old_bytes[] = {8, 0, 8, 8, 8};
    for (unsigned delegate = 0; delegate < 2; delegate++)
        for (unsigned enforcing = 0; enforcing < 2; enforcing++)
            for (unsigned released = 0; released < 2; released++)
                for (unsigned growth = 0; growth < 2; growth++)
                    for (unsigned request = 0; request < 5; request++) {
                        wl_columnar_memory_governor_t g[2];
                        wl_columnar_memory_reservation_t token[2], held[2];
                        for (unsigned i = 0; i < 2; i++) {
                            controlled_governor(&g[i], 64);
                            if (enforcing)
                                g[i].mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
                            wl_columnar_memory_reservation_init(&token[i]);
                            wl_columnar_memory_reservation_init(&held[i]);
                            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                wl_columnar_memory_reserve(&g[i], 16,
                                &held[i]));
                            if (released) {
                                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                    wl_columnar_memory_reserve(&g[i], 7,
                                    &token[i]));
                                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                    wl_columnar_memory_rollback(&token[i]));
                            }
                        }
                        controlled_phase = 0; controlled_fault = 0;
                        wl_evaluation_control_t *c =
                            delegate ? NULL : controlled_begin(0);
                        wl_columnar_memory_admission_status_t raw = growth
                        ? wl_columnar_memory_reserve_growth(&g[0],
                                old_bytes[request], requests[request],
                                &token[0])
                        : wl_columnar_memory_reserve_checked(&g[0],
                                requests[request], &token[0]);
                        wl_columnar_memory_admission_status_t actual;
                        int rc = growth
                        ? wl_columnar_memory_governor_reserve_growth_controlled(
                            &g[1], old_bytes[request], requests[request],
                            &token[1], &actual, c)
                        : wl_columnar_memory_governor_reserve_checked_controlled
                                (&g[1], requests[request], &token[1], &actual,
                                c);
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc == 0 &&
                            actual == raw);
                        if (c) controlled_end(c, 0);
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(token[0].bytes ==
                            token[1].bytes &&
                            token[0].replacement_bytes ==
                            token[1].replacement_bytes &&
                            token[0].transition_kind ==
                            token[1].transition_kind);
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK((token[0].governor ==
                            NULL) == (token[1].governor == NULL));
                        for (unsigned i = 0; i < 2; i++)
                            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                token[i].identity == &token[i]);
                        uint64_t state = atomic_load_explicit(&token[0].state,
                                memory_order_relaxed);
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(state ==
                            atomic_load_explicit(&token[1].state,
                            memory_order_relaxed));
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(atomic_load_explicit(
                                &token[0].owner_bits, memory_order_relaxed) ==
                            atomic_load_explicit(&token[1].owner_bits,
                            memory_order_relaxed));
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                            wl_columnar_memory_reserved(&g[0]) ==
                            wl_columnar_memory_reserved(&g[1]));
                        for (unsigned i = 0; i < 2; i++) {
                            if (state ==
                                WL_COLUMNAR_MEMORY_RESERVATION_RESERVED)
                                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                    wl_columnar_memory_rollback(&token[i]));
                            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                wl_columnar_memory_rollback(&held[i]));
                            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                wl_columnar_memory_reserved(&g[i]) == 0);
                        }
                    }
    PASS();
}

static void
test_controlled_copied_authority(void)
{
    TEST("copied token and missing output never acquire authority");
    wl_columnar_memory_governor_t g;
    controlled_governor(&g, 64);
    wl_columnar_memory_reservation_t token;
    wl_columnar_memory_reservation_init(&token);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve(&g, 16,
        &token));
    wl_columnar_memory_reservation_t copy = token;
    controlled_phase = 0; controlled_fault = 0;
    for (unsigned kind = 0; kind < 5; kind++) {
        wl_evaluation_control_t *c = controlled_begin(0);
        wl_columnar_memory_admission_status_t status;
        bool committed = true;
        int rc;
        if (kind == 0)
            rc = wl_columnar_memory_governor_reserve_checked_controlled(&g, 8,
                    &copy, &status, c);
        else if (kind == 1)
            rc = wl_columnar_memory_governor_reserve_growth_controlled(&g, 0, 8,
                    &copy, &status, c);
        else if (kind == 2)
            rc = wl_columnar_memory_governor_begin_growth_controlled(&copy, 8,
                    &status, c);
        else if (kind == 3)
            rc = wl_columnar_memory_governor_commit_controlled(&copy, NULL,
                    &committed, c);
        else
            rc = wl_columnar_memory_governor_commit_growth_controlled(&copy,
                    &committed, c);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc == 0);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(kind < 3 ? status ==
            WL_COLUMNAR_MEMORY_ADMISSION_INVALID : !committed);
        const uint64_t costs[] = {3, 4, 2, 1, 1};
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c, 0) == costs[kind]);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(copy.identity == &token &&
            copy.bytes == 16 &&
            atomic_load_explicit(&copy.state,
            memory_order_relaxed) == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(&g) ==
            16);
    }
    wl_evaluation_control_t *c = controlled_begin(0);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_reserve_checked_controlled(&g, 8, &copy,
        NULL, c) == EINVAL);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(c, EINVAL) == 0);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_rollback(&token));
    PASS();
}

typedef struct {
    wl_columnar_memory_governor_t governor;
    wl_columnar_memory_reservation_t token;
    wl_columnar_memory_reservation_t peer;
    void *old_buffer;
    void *incoming_buffer;
} prepared_growth_fixture_t;

static void
prepared_growth_init(prepared_growth_fixture_t *fixture)
{
    controlled_governor(&fixture->governor, 1024);
    wl_columnar_memory_reservation_init(&fixture->token);
    wl_columnar_memory_reservation_init(&fixture->peer);
    fixture->old_buffer = malloc(8);
    fixture->incoming_buffer = NULL;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture->old_buffer);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve(
            &fixture->governor, 8, &fixture->token));
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_commit(
            &fixture->token, fixture->old_buffer));
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve(
            &fixture->governor, 17, &fixture->peer));
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_begin_growth(
            &fixture->token, 16) == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY);
    fixture->incoming_buffer = malloc(16);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture->incoming_buffer);
    memset(fixture->old_buffer, 0x17, 8);
    memset(fixture->incoming_buffer, 0x29, 16);
}

static void
prepared_growth_check(prepared_growth_fixture_t *fixture, uint64_t state)
{
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(
            &fixture->governor) == 41);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture->token.bytes == 8);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture->token.identity == &fixture->token
        && fixture->token.governor == &fixture->governor
        && atomic_load_explicit(&fixture->token.owner_bits,
        memory_order_relaxed) == (uintptr_t)fixture->old_buffer);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture->token.replacement_bytes == 16);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture->token.transition_kind
        == WL_COLUMNAR_MEMORY_TRANSITION_GROWTH);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(atomic_load_explicit(
            &fixture->token.state, memory_order_acquire) == state);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture->peer.bytes == 17);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(((unsigned char *)fixture->old_buffer)[7]
        == 0x17);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        ((unsigned char *)fixture->incoming_buffer)[15] == 0x29);
}

static void
prepared_growth_dispose(prepared_growth_fixture_t *fixture, bool committed)
{
    free(fixture->incoming_buffer);
    fixture->incoming_buffer = NULL;
    if (!committed) {
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_rollback_growth(
                &fixture->token));
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(
                &fixture->governor) == 25);
    }
    free(fixture->old_buffer);
    fixture->old_buffer = NULL;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_release(
            &fixture->token));
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(
            &fixture->governor) == 17);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_release(
            &fixture->peer));
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(
            &fixture->governor) == 0);
}

static uint64_t
prepared_growth_success(uint64_t limit)
{
    prepared_growth_fixture_t fixture;
    prepared_growth_init(&fixture);
    wl_columnar_memory_governor_growth_commit_prepared_t prepared = { 0 };
    wl_evaluation_control_t *control = controlled_begin(limit);
    bool ready = false;
    controlled_phase = 0;
    controlled_fault = 0;
    prepared_trace_enabled = true;
    prepared_trace_count = 0;
    prepared_witness = &prepared;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_growth_commit_prepare_controlled(
            &fixture.token,
            &fixture.governor, fixture.old_buffer, &prepared, &ready,
            control) == 0
        && ready);
    prepared_trace_enabled = false;
    prepared_witness = NULL;
    uint64_t intervals[] = { 41, 63, 13, 50, 13 };
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(prepared_trace_count == 5);
    for (unsigned i = 0; i < 5; i++)
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(prepared_trace[i] == intervals[i]);
    prepared_growth_check(&fixture, WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING);
    /* Independent source inventory: valid25, outer guards5, getter/local5,
     * id guards3, token stores4, consume1, return1; not a production macro.
     */
    unsigned finish_slots[] = { 25, 5, 5, 3, 4, 1, 1 };
    unsigned finish_work = 0;
    for (unsigned i = 0; i < sizeof(finish_slots) / sizeof(finish_slots[0]);
        i++)
        finish_work += finish_slots[i];
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(finish_work == 44
        && prepared.prepaid_work == finish_work);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_charge(control,
        0) == 0);
    /* Cancellation after genuine final cutoff remains for the next attempt. */
    wl_evaluation_control_request_cancel(control);
    uint64_t work = controlled_work;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_growth_commit_finish(&prepared) == 0);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_work == work);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture.token.bytes == 24
        && fixture.token.replacement_bytes == 0
        && fixture.token.transition_kind == WL_COLUMNAR_MEMORY_TRANSITION_NONE);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(atomic_load_explicit(&fixture.token.state,
        memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_growth_commit_finish(&prepared) == EINVAL);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_growth_commit_cancel(&prepared) == EINVAL);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(work == 180);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(control, 0) == 228);
    prepared_growth_dispose(&fixture, true);
    return work;
}

static void
test_prepared_growth_prefixes(void)
{
    TEST(
        "prepared growth preserves real storage and peer credit at every stop");
    uint64_t observed = prepared_growth_success(0);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(prepared_growth_success(228) == observed);
    /* Admitted228 differs from conservative checkpoint ceilings180. */
    unsigned preparation_slots[] = { 28, 50, 37, 65, 4 };
    uint64_t total = 44;
    for (unsigned i = 0;
        i < sizeof(preparation_slots) / sizeof(preparation_slots[0]); i++)
        total += preparation_slots[i];
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(total == 228 && observed == 180);
    for (unsigned mode = 0; mode < 2; mode++) {
        for (uint64_t at = 1; at <= (mode == 0 ? total : observed); at++) {
            prepared_growth_fixture_t fixture;
            prepared_growth_init(&fixture);
            wl_columnar_memory_governor_growth_commit_prepared_t prepared =
            { 0 };
            wl_evaluation_control_t *control = controlled_begin(
                mode == 0 ? at - 1 : 0);
            if (mode == 0 && at == 1) {
                /* Limit zero means unlimited; exercise initial refusal with1. */
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                    wl_evaluation_control_finish(control, &controlled_owner,
                    0, 0) == 0);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                    wl_evaluation_control_configure(control, 1) == 0);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                    wl_evaluation_control_begin(control,
                    &controlled_owner) == 0);
            }
            controlled_cancel_at = mode == 1 ? at : 0;
            bool ready = false;
            prepared_witness = &prepared;
            int rc =
                wl_columnar_memory_governor_growth_commit_prepare_controlled(
                &fixture.token, &fixture.governor, fixture.old_buffer,
                &prepared, &ready, control);
            prepared_witness = NULL;
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc == (mode == 0
                ? WL_EVALUATION_CONTROL_WORK_LIMIT
                : WL_EVALUATION_CONTROL_CANCELLED));
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(!ready);
            prepared_growth_check(&fixture,
                WL_COLUMNAR_MEMORY_RESERVATION_REPLACING);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(control, rc)
                == (mode == 0 ? 0 : total));
            prepared_growth_dispose(&fixture, false);
        }
    }
    unsigned phases[] = {
        WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_ARGUMENTS,
        WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_SNAPSHOT,
        WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_PRECLAIM,
        WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_CLAIMED,
        WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_READY_CUTOFF,
    };
    for (unsigned p = 0; p < sizeof(phases) / sizeof(phases[0]); p++) {
        for (int fault = 1; fault <= 4; fault++) {
            prepared_growth_fixture_t fixture;
            prepared_growth_init(&fixture);
            wl_columnar_memory_governor_growth_commit_prepared_t prepared =
            { 0 };
            wl_evaluation_control_t *control = controlled_begin(0);
            controlled_phase = phases[p];
            controlled_fault = fault;
            bool ready = false;
            prepared_witness = &prepared;
            prepared_trace_enabled = true;
            prepared_trace_count = 0;
            int rc =
                wl_columnar_memory_governor_growth_commit_prepare_controlled(
                &fixture.token, &fixture.governor, fixture.old_buffer,
                &prepared, &ready, control);
            prepared_witness = NULL;
            prepared_trace_enabled = false;
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(prepared_phase_at_fault == p + 1
                && prepared_work_at_fault == p);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(prepared_phase_count == p + 1
                && prepared_trace_count == p + (fault >= 3 ? 0u : 1u));
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(prepared.phase == (p == 0
                ? WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_INERT
                : WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_CONSUMED));
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc == (fault == 1
                ? WL_EVALUATION_CONTROL_CANCELLED : fault ==
                2 ? EOVERFLOW : EINVAL));
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(!ready && controlled_hits == 1);
            prepared_growth_check(&fixture,
                WL_COLUMNAR_MEMORY_RESERVATION_REPLACING);
            if (fault == 4) {
                wl_evaluation_control_report_t report;
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                    wl_evaluation_control_active_attempt_id(control) == 0);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_report(
                        control, &report) == 0 && report.consumed == total
                    && report.result == 0);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_detach(
                        control, &controlled_owner) == 0);
                wl_evaluation_control_release(control);
            } else WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(control, rc)
                    == (fault == 3 ? 0 : total));
            prepared_growth_dispose(&fixture, false);
        }
    }
    controlled_phase = 0;
    controlled_fault = 0;
    PASS();
}

typedef struct {
    wl_columnar_memory_governor_growth_commit_prepared_t *prepared;
    int finish_status;
    int cancel_status;
} prepared_growth_thread_t;

static void *
prepared_growth_wrong_thread(void *opaque)
{
    prepared_growth_thread_t *context = opaque;
    context->finish_status =
        wl_columnar_memory_governor_growth_commit_finish(context->prepared);
    context->cancel_status =
        wl_columnar_memory_governor_growth_commit_cancel(context->prepared);
    return NULL;
}

static void
test_prepared_growth_authority(void)
{
    TEST("prepared growth rejects copies, wrong threads and stale attempts");
    prepared_growth_fixture_t fixture;
    prepared_growth_init(&fixture);
    wl_columnar_memory_governor_growth_commit_prepared_t prepared = { 0 };
    wl_evaluation_control_t *control = controlled_begin(0);
    bool ready = false;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_growth_commit_prepare_controlled(
            &fixture.token,
            &fixture.governor, fixture.old_buffer, &prepared, &ready,
            control) == 0
        && ready);
    wl_columnar_memory_governor_growth_commit_prepared_t copy = prepared;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_growth_commit_finish(&copy) == EINVAL);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_growth_commit_cancel(&copy) == EINVAL);
    prepared_growth_thread_t context = { .prepared = &prepared };
    wl_thread_t worker;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_thread_create(&worker,
        prepared_growth_wrong_thread, &context) == 0);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_thread_join(&worker) == 0);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(context.finish_status == EINVAL
        && context.cancel_status == EINVAL);
    prepared_growth_check(&fixture, WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_finish(control,
        &controlled_owner, 0, 0) == 0);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_growth_commit_finish(&prepared) == EINVAL);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_reset(control) ==
        0);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_begin(control,
        &controlled_owner) == 0);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_growth_commit_finish(&prepared) == EINVAL);
    prepared_growth_check(&fixture, WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_growth_commit_cancel(&prepared) == 0);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_growth_commit_cancel(&prepared) == EINVAL);
    prepared_growth_check(&fixture, WL_COLUMNAR_MEMORY_RESERVATION_REPLACING);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(control, 0) == 0);
    prepared_growth_dispose(&fixture, false);
    PASS();
}

static void
test_prepared_growth_refusals(void)
{
    TEST(
        "prepared growth distinguishes typed shape refusal and execution stops");
    for (unsigned shape = 0; shape < 3; shape++) {
        for (int fault = 0; fault <= 3; fault++) {
            prepared_growth_fixture_t fixture;
            prepared_growth_init(&fixture);
            wl_columnar_memory_governor_t other;
            controlled_governor(&other, 1024);
            wl_columnar_memory_reservation_t copy = fixture.token;
            wl_columnar_memory_governor_growth_commit_prepared_t prepared =
            { 0 };
            wl_evaluation_control_t *control = controlled_begin(0);
            controlled_phase = WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_SNAPSHOT;
            controlled_fault = fault;
            bool ready = true;
            prepared_witness = &prepared;
            int rc =
                wl_columnar_memory_governor_growth_commit_prepare_controlled(
                shape == 2 ? &copy : &fixture.token,
                shape == 0 ? &other : &fixture.governor,
                shape == 1 ? &other : fixture.old_buffer,
                &prepared, &ready, control);
            prepared_witness = NULL;
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc == (fault <= 1 ? 0
                : fault == 2 ? EOVERFLOW : EINVAL));
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(!ready);
            prepared_growth_check(&fixture,
                WL_COLUMNAR_MEMORY_RESERVATION_REPLACING);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                wl_columnar_memory_governor_growth_commit_cancel(&prepared) ==
                EINVAL);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                wl_columnar_memory_governor_growth_commit_finish(&prepared) ==
                EINVAL);
            /* Typed refusal beats latched negative stop in execution rc, but
             * the control report independently retains that disposition. */
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(control,
                fault == 1 ? WL_EVALUATION_CONTROL_CANCELLED : rc)
                == (fault == 3 ? 0 : 228));
            prepared_growth_dispose(&fixture, false);
        }
    }
    controlled_phase = 0;
    controlled_fault = 0;
    prepared_growth_fixture_t fixture;
    prepared_growth_init(&fixture);
    wl_columnar_memory_governor_growth_commit_prepared_t prepared = { 0 };
    wl_evaluation_control_t *control = controlled_begin(0);
    bool ready = false;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_growth_commit_prepare_controlled(
            &fixture.token,
            &fixture.governor, fixture.old_buffer, &prepared, &ready,
            control) == 0
        && ready);
    /* A live original is never overwritten, despite conservative admission. */
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_growth_commit_prepare_controlled(
            &fixture.token,
            &fixture.governor, fixture.old_buffer, &prepared, &ready,
            control) == EINVAL
        && ready &&
        prepared.phase == WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_READY);
    prepared_growth_check(&fixture, WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        wl_columnar_memory_governor_growth_commit_cancel(&prepared) == 0);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(control, 0) == 456);
    prepared_growth_dispose(&fixture, false);
    PASS();
}

/* Attempt-bound admission fixtures. Physical storage follows successful
 * admission; a failing API has not allocated incoming storage. Independent
 * peer credit advances at a hook and must survive original-only restoration.
 */
typedef struct {
    wl_columnar_memory_governor_t governor;
    wl_columnar_memory_reservation_t token, peer, extra;
    void *old_buffer, *peer_buffer, *extra_buffer;
    wl_evaluation_control_t *control;
    uint64_t attempt_id, old_consumed;
    unsigned kind, seams, fault_at, fault_kind, hits;
    unsigned contention_remaining, failed_cas;
    unsigned phase[32], seam_kind[32];
    uint64_t work[32];
    bool running, skip_peer_update;
} attempt_admission_fixture_t;

static attempt_admission_fixture_t *attempt_fixture;

static void
attempt_seam(unsigned kind, unsigned phase, uint64_t work)
{
    attempt_admission_fixture_t *fixture = attempt_fixture;
    if (!fixture || !fixture->running)
        return;
    unsigned index = fixture->seams++;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(index < 32);
    fixture->phase[index] = phase;
    fixture->seam_kind[index] = kind;
    fixture->work[index] = work;
    if (fixture->seams != fixture->fault_at || !fixture->fault_kind)
        return;
    fixture->hits++;
    if (!fixture->skip_peer_update) {
        /* A genuine independent peer update; no original token mutation. */
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve(
                &fixture->governor, 3, &fixture->extra));
        fixture->extra_buffer = malloc(3);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture->extra_buffer);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_commit(
                &fixture->extra, fixture->extra_buffer));
        memset(fixture->extra_buffer, 0x33, 3);
    }
    if (fixture->fault_kind == 1)
        wl_evaluation_control_request_cancel(fixture->control);
    else if (fixture->fault_kind == 2)
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_charge(
                fixture->control, UINT64_MAX) == EOVERFLOW);
    else {
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_finish(
                fixture->control, &controlled_owner, 0, 0) == 0);
        wl_evaluation_control_report_t report;
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_report(
                fixture->control, &report) == 0);
        fixture->old_consumed = report.consumed;
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_reset(
                fixture->control) == 0);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_begin(
                fixture->control, &controlled_owner) == 0);
    }
}

static void
attempt_phase(wl_evaluation_control_t *control,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    if (!attempt_fixture || !attempt_fixture->running)
        return;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(control == attempt_fixture->control
        && (token == &attempt_fixture->token
        || phase == WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT));
    if (phase == WL_COLUMNAR_MEMORY_GOVERNOR_CLAIMED
        || phase == WL_COLUMNAR_MEMORY_GOVERNOR_COMMITTING) {
        uint64_t expected = attempt_fixture->kind == 0
            ? WL_COLUMNAR_MEMORY_RESERVATION_CLAIMED
            : attempt_fixture->kind == 1
            ? WL_COLUMNAR_MEMORY_RESERVATION_REPLACEMENT_ADMITTING
            : WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING;
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(atomic_load_explicit(&token->state,
            memory_order_acquire) == expected);
    }
    attempt_seam(0, phase, 0);
}

static void
attempt_work(wl_evaluation_control_t *control, uint64_t work)
{
    if (!attempt_fixture || !attempt_fixture->running)
        return;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(control == attempt_fixture->control);
    attempt_seam(1, 0, work);
}

static void
attempt_before_total(wl_evaluation_control_t *control,
    wl_columnar_memory_governor_t *governor, uint64_t old, bool overflow)
{
    (void)old;
    (void)overflow;
    if (!attempt_fixture || !attempt_fixture->running)
        return;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(control == attempt_fixture->control
        && governor == &attempt_fixture->governor);
    attempt_seam(2, 0, 0);
    if (attempt_fixture->contention_remaining) {
        attempt_admission_fixture_t *fixture = attempt_fixture;
        fixture->contention_remaining--;
        if (fixture->extra_buffer) {
            free(fixture->extra_buffer);
            fixture->extra_buffer = NULL;
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_release(
                    &fixture->extra));
        } else {
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve(
                    &fixture->governor, 3, &fixture->extra));
            fixture->extra_buffer = malloc(3);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture->extra_buffer);
            memset(fixture->extra_buffer, 0x33, 3);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_commit(
                    &fixture->extra, fixture->extra_buffer));
        }
    }
}

static void
attempt_after_total(wl_evaluation_control_t *control,
    wl_columnar_memory_governor_t *governor, bool admitted, bool overflow)
{
    if (!attempt_fixture || !attempt_fixture->running)
        return;
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(control == attempt_fixture->control
        && governor == &attempt_fixture->governor);
    if (!admitted)
        attempt_fixture->failed_cas++;
    if (admitted && !overflow) {
        uint64_t before = (attempt_fixture->kind == 0 ? 17 : 25)
            + (attempt_fixture->extra_buffer ? 3 : 0);
        uint64_t added = attempt_fixture->kind == 0 ? 8 : 16;
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(governor)
            == before + added);
    }
    attempt_seam(3, 0, 0);
}

static void
attempt_init(attempt_admission_fixture_t *fixture, unsigned kind,
    uint64_t limit)
{
    memset(fixture, 0, sizeof(*fixture));
    fixture->kind = kind;
    controlled_governor(&fixture->governor, 1024);
    wl_columnar_memory_reservation_init(&fixture->token);
    wl_columnar_memory_reservation_init(&fixture->peer);
    wl_columnar_memory_reservation_init(&fixture->extra);
    if (kind) {
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve(
                &fixture->governor, 8, &fixture->token));
        fixture->old_buffer = malloc(8);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture->old_buffer);
        memset(fixture->old_buffer, 0x22, 8);
        if (kind == 1)
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_commit(
                    &fixture->token, fixture->old_buffer));
    }
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserve(
            &fixture->governor, 17, &fixture->peer));
    fixture->peer_buffer = malloc(17);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture->peer_buffer);
    memset(fixture->peer_buffer, 0x17, 17);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_commit(
            &fixture->peer, fixture->peer_buffer));
    fixture->control = controlled_begin(limit);
    fixture->attempt_id = wl_evaluation_control_active_attempt_id(
        fixture->control);
    fixture->running = true;
    attempt_fixture = fixture;
}

static int
attempt_call(attempt_admission_fixture_t *fixture,
    wl_columnar_memory_admission_status_t *status, bool *committed)
{
    if (fixture->kind == 0)
        return wl_columnar_memory_governor_reserve_checked_attempt_controlled(
            &fixture->governor, 8, &fixture->token, status, fixture->control,
            fixture->attempt_id);
    if (fixture->kind == 1)
        return wl_columnar_memory_governor_begin_growth_attempt_controlled(
            &fixture->token, 16, status, fixture->control, fixture->attempt_id);
    return wl_columnar_memory_governor_commit_attempt_controlled(
        &fixture->token, fixture->old_buffer, committed, fixture->control,
        fixture->attempt_id);
}

static void
attempt_dispose(attempt_admission_fixture_t *fixture, bool success)
{
    fixture->running = false;
    attempt_fixture = NULL;
    if (success && fixture->kind == 0) {
        void *incoming = malloc(8);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(incoming);
        free(incoming);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_rollback(
                &fixture->token));
    } else if (success && fixture->kind == 1) {
        void *incoming = malloc(16);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(incoming);
        free(incoming);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_rollback_growth(
                &fixture->token));
    }
    if (fixture->old_buffer) {
        free(fixture->old_buffer);
        if (fixture->kind == 2 && !success)
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_rollback(
                    &fixture->token));
        else
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_release(
                    &fixture->token));
    }
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(
            &fixture->governor) == (fixture->extra_buffer ? 20 : 17));
    if (fixture->extra_buffer) {
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
            ((unsigned char *)fixture->extra_buffer)[2] == 0x33);
        free(fixture->extra_buffer);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_release(
                &fixture->extra));
    }
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
        ((unsigned char *)fixture->peer_buffer)[16] == 0x17);
    free(fixture->peer_buffer);
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_release(
            &fixture->peer));
    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(
            &fixture->governor) == 0);
}

static void
test_attempt_admission_typed_verdicts(void)
{
    TEST(
        "attempt-bound typed invalid/denied/overflow beat negative but not positive errors");
    wl_columnar_memory_governor_test_checkpoint = attempt_phase;
    wl_columnar_memory_governor_test_work_checkpoint = attempt_work;
    wl_columnar_memory_governor_test_before_total_cas = attempt_before_total;
    wl_columnar_memory_governor_test_after_total_cas = attempt_after_total;
    for (unsigned kind = 0; kind < 3; kind++)
        for (unsigned scenario = 0; scenario < (kind == 2 ? 1u : 3u);
            scenario++)
            for (unsigned tail = 0; tail < 2; tail++)
                for (unsigned fault = 1; fault <= 3; fault++) {
                    attempt_admission_fixture_t fixture;
                    attempt_init(&fixture, kind, 0);
                    fixture.skip_peer_update = true;
                    fixture.fault_kind = fault;
                    unsigned phase_at = scenario == 0 ||
                        (kind == 1 && scenario == 2)
                        ? 1 : scenario == 1 ? (kind == 0 ? 7 : 6) : 9;
                    fixture.fault_at = phase_at + tail;
                    if (scenario == 1)
                        atomic_store_explicit(&fixture.governor.usable_bytes,
                            kind == 0 ? 17 : 25, memory_order_relaxed);
                    wl_columnar_memory_reservation_t prior = fixture.token;
                    wl_columnar_memory_admission_status_t status =
                        WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY;
                    bool committed = true;
                    uint64_t bytes = scenario == 0 ? 0 : scenario == 1
                        ? (kind == 0 ? 8 : 16) : UINT64_MAX;
                    int rc = kind == 0
                        ?
                        wl_columnar_memory_governor_reserve_checked_attempt_controlled
                        (
                        &fixture.governor, bytes, &fixture.token, &status,
                        fixture.control, fixture.attempt_id)
                        : kind == 1
                        ?
                        wl_columnar_memory_governor_begin_growth_attempt_controlled
                        (
                        &fixture.token, bytes, &status, fixture.control,
                        fixture.attempt_id)
                        : wl_columnar_memory_governor_commit_attempt_controlled(
                        NULL, NULL, &committed, fixture.control,
                        fixture.attempt_id);
                    int expected_rc = fault == 1 ? 0 : fault ==
                        2 ? EOVERFLOW : EINVAL;
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc == expected_rc &&
                        fixture.hits == 1);
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture.seams ==
                        (fault == 1 ? phase_at + 1 : phase_at + tail));
                    uint64_t prefix = scenario == 0 ||
                        (kind == 1 && scenario == 2) ? 74 : 212;
                    if (fault == 1) {
                        if (kind == 2)
                            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(!committed);
                        else
                            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(status ==
                                (scenario == 0
                                ? WL_COLUMNAR_MEMORY_ADMISSION_INVALID :
                                scenario == 1
                                ? WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                                : WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW));
                    } else
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(status ==
                            WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY
                            && committed);
                    if (fault != 1 || kind != 0 || scenario == 0)
                        controlled_token_equal(&fixture.token, &prior);
                    else
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture.token.bytes ==
                            bytes
                            && atomic_load_explicit(&fixture.token.state,
                            memory_order_acquire)
                            == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED);
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                        wl_columnar_memory_reserved(
                            &fixture.governor) == (kind ? 25 : 17));
                    if (fault == 3) {
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                            fixture.old_consumed == prefix);
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(
                                fixture.control, 0) == 0);
                    } else
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(
                                fixture.control,
                                fault ==
                                1 ? WL_EVALUATION_CONTROL_CANCELLED : rc) ==
                            prefix);
                    attempt_dispose(&fixture, false);
                }
    wl_columnar_memory_governor_test_checkpoint = NULL;
    wl_columnar_memory_governor_test_work_checkpoint = NULL;
    wl_columnar_memory_governor_test_before_total_cas = NULL;
    wl_columnar_memory_governor_test_after_total_cas = NULL;
    PASS();
}

static void
test_attempt_admission_boundaries(void)
{
    TEST(
        "attempt-bound literal exact/minus-one admissions and genuine CAS retries");
    wl_columnar_memory_governor_test_checkpoint = attempt_phase;
    wl_columnar_memory_governor_test_work_checkpoint = attempt_work;
    wl_columnar_memory_governor_test_before_total_cas = attempt_before_total;
    wl_columnar_memory_governor_test_after_total_cas = attempt_after_total;
    for (unsigned kind = 0; kind < 3; kind++) {
        const uint64_t limits[2][8] = {
            { 73, 74, 127, 128, 211, 212, 240, 241 },
            { 73, 74, 127, 128, 156, 157 },
        };
        const uint64_t consumed[2][8] = {
            { 0, 74, 74, 128, 128, 212, 212, 241 },
            { 0, 74, 74, 128, 128, 157 },
        };
        unsigned row = kind == 2;
        for (unsigned i = 0; i < (row ? 6u : 8u); i++) {
            attempt_admission_fixture_t fixture;
            attempt_init(&fixture, kind, limits[row][i]);
            wl_columnar_memory_reservation_t prior = fixture.token;
            wl_columnar_memory_admission_status_t status =
                WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
            bool committed = false;
            bool success = i == (row ? 5u : 7u);
            int rc = attempt_call(&fixture, &status, &committed);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc == (success ? 0
                : WL_EVALUATION_CONTROL_WORK_LIMIT));
            if (!success)
                controlled_token_equal(&fixture.token, &prior);
            else
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(kind == 2 ? committed
                    : status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(fixture.control,
                rc)
                == consumed[row][i]);
            attempt_dispose(&fixture, success);
        }
    }
    /* Independent peer physical allocate/free changes the aggregate between
     * the actual read and strong CAS. No fabricated CAS result is injected. */
    for (unsigned kind = 0; kind < 2; kind++)
        for (unsigned failed = 1; failed <= 4; failed++)
            for (unsigned boundary = 0; boundary < 3; boundary++) {
                const uint64_t totals[] = { 325, 409, 493, 577 };
                const uint64_t round_denied[] = { 212, 296, 380, 464 };
                const uint64_t publication_denied[] = { 296, 380, 464, 548 };
                uint64_t limit = boundary == 0 ? totals[failed - 1]
                    : boundary == 1 ? totals[failed - 1] - 1
                    : publication_denied[failed - 1] - 1;
                attempt_admission_fixture_t fixture;
                attempt_init(&fixture, kind, limit);
                fixture.contention_remaining = failed;
                wl_columnar_memory_reservation_t prior = fixture.token;
                wl_columnar_memory_admission_status_t status =
                    WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
                bool committed = false;
                int rc = attempt_call(&fixture, &status, &committed);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc == (boundary == 0 ? 0
                    : WL_EVALUATION_CONTROL_WORK_LIMIT));
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                    fixture.contention_remaining == 0
                    && fixture.failed_cas == failed);
                if (boundary)
                    controlled_token_equal(&fixture.token, &prior);
                uint64_t expected = boundary == 0 ? totals[failed - 1]
                    : boundary == 1 ? publication_denied[failed - 1]
                    : round_denied[failed - 1];
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(
                        fixture.control, rc)
                    == expected);
                attempt_dispose(&fixture, boundary == 0);
            }
    wl_columnar_memory_governor_test_checkpoint = NULL;
    wl_columnar_memory_governor_test_work_checkpoint = NULL;
    wl_columnar_memory_governor_test_before_total_cas = NULL;
    wl_columnar_memory_governor_test_after_total_cas = NULL;
    PASS();
}

static void
test_attempt_admission_raw_twins(void)
{
    TEST(
        "attempt-bound raw parity for original lifecycles, modes and null owners");
    for (unsigned kind = 0; kind < 3; kind++)
        for (unsigned enforcing = 0; enforcing < 2; enforcing++)
            for (unsigned released = 0; released < 2; released++)
                for (unsigned request = 0; request < 4; request++) {
                    wl_columnar_memory_governor_t governor[2];
                    wl_columnar_memory_reservation_t token[2];
                    const uint64_t bytes[] = { 0, 8, 64, 65 };
                    for (unsigned i = 0; i < 2; i++) {
                        controlled_governor(&governor[i], 64);
                        if (enforcing)
                            governor[i].mode =
                                WL_COLUMNAR_MEMORY_MODE_ENFORCING;
                        wl_columnar_memory_reservation_init(&token[i]);
                        if (released) {
                            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                wl_columnar_memory_reserve(
                                    &governor[i], 7, &token[i]));
                            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                wl_columnar_memory_rollback(
                                    &token[i]));
                        }
                        if (kind) {
                            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                wl_columnar_memory_reserve(
                                    &governor[i], 8, &token[i]));
                            if (kind == 1)
                                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                    wl_columnar_memory_commit(
                                        &token[i], NULL));
                        }
                    }
                    wl_evaluation_control_t *control = controlled_begin(0);
                    uint64_t id =
                        wl_evaluation_control_active_attempt_id(control);
                    wl_columnar_memory_admission_status_t raw =
                        WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
                    wl_columnar_memory_admission_status_t actual = raw;
                    bool raw_committed = false, actual_committed = false;
                    int rc;
                    if (kind == 0) {
                        raw = wl_columnar_memory_reserve_checked(&governor[0],
                                bytes[request], &token[0]);
                        rc =
                            wl_columnar_memory_governor_reserve_checked_attempt_controlled
                            (
                            &governor[1], bytes[request], &token[1], &actual,
                            control, id);
                    } else if (kind == 1) {
                        raw = wl_columnar_memory_begin_growth(&token[0],
                                bytes[request]);
                        rc =
                            wl_columnar_memory_governor_begin_growth_attempt_controlled
                            (
                            &token[1], bytes[request], &actual, control, id);
                    } else {
                        const void *owner = request &
                            1 ? &controlled_owner : NULL;
                        raw_committed = wl_columnar_memory_commit(&token[0],
                                owner);
                        rc =
                            wl_columnar_memory_governor_commit_attempt_controlled
                            (
                            &token[1], owner, &actual_committed, control, id);
                    }
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc == 0 && (kind == 2
                        ? raw_committed == actual_committed : raw == actual));
                    uint64_t state = atomic_load_explicit(&token[0].state,
                            memory_order_acquire);
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(state ==
                        atomic_load_explicit(
                            &token[1].state, memory_order_acquire)
                        && token[0].bytes == token[1].bytes
                        && token[0].replacement_bytes ==
                        token[1].replacement_bytes
                        && token[0].transition_kind == token[1].transition_kind
                        && atomic_load_explicit(&token[0].owner_bits,
                        memory_order_relaxed)
                        == atomic_load_explicit(&token[1].owner_bits,
                        memory_order_relaxed)
                        && wl_columnar_memory_reserved(&governor[0])
                        == wl_columnar_memory_reserved(&governor[1]));
                    (void)controlled_end(control, 0);
                    for (unsigned i = 0; i < 2; i++) {
                        if (state == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING) {
                            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                wl_columnar_memory_rollback_growth(&token[i]));
                            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                wl_columnar_memory_release(&token[i]));
                        } else if (state ==
                            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)
                            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                wl_columnar_memory_release(&token[i]));
                        else if (state ==
                            WL_COLUMNAR_MEMORY_RESERVATION_RESERVED)
                            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                                wl_columnar_memory_rollback(&token[i]));
                        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                            wl_columnar_memory_reserved(&governor[i]) == 0);
                    }
                }
    PASS();
}

static void
test_attempt_admission_bootstrap(void)
{
    TEST(
        "attempt-bound bootstrap rejects missing and old authority without debit");
    for (unsigned kind = 0; kind < 3; kind++) {
        for (unsigned invalid = 0; invalid < 5; invalid++) {
            attempt_admission_fixture_t fixture;
            attempt_init(&fixture, kind, 0);
            wl_columnar_memory_reservation_t prior = fixture.token;
            wl_evaluation_control_t *control = invalid ==
                0 ? NULL : fixture.control;
            uint64_t id = invalid == 1 ? 0 : invalid == 2
                ? fixture.attempt_id + 1 : fixture.attempt_id;
            wl_columnar_memory_admission_status_t status =
                WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW;
            bool committed = true;
            if (invalid == 4) {
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_finish(
                        fixture.control, &controlled_owner, 0, 0) == 0);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_reset(
                        fixture.control) == 0);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_evaluation_control_begin(
                        fixture.control, &controlled_owner) == 0);
            }
            int rc = kind == 0
                ? wl_columnar_memory_governor_reserve_checked_attempt_controlled
                (
                &fixture.governor, 8, &fixture.token,
                invalid == 3 ? NULL : &status, control, id)
                : kind == 1
                ? wl_columnar_memory_governor_begin_growth_attempt_controlled(
                &fixture.token, 16, invalid == 3 ? NULL : &status, control, id)
                : wl_columnar_memory_governor_commit_attempt_controlled(
                &fixture.token, fixture.old_buffer,
                invalid == 3 ? NULL : &committed, control, id);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc == EINVAL &&
                fixture.seams == 0);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(status ==
                WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                && committed);
            controlled_token_equal(&fixture.token, &prior);
            WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(fixture.control,
                0) == 0);
            attempt_dispose(&fixture, false);
        }
    }
    PASS();
}

static void
test_attempt_admission_seams(void)
{
    TEST(
        "attempt-bound governor rejects every stale seam and restores own credit");
    wl_columnar_memory_governor_test_checkpoint = attempt_phase;
    wl_columnar_memory_governor_test_work_checkpoint = attempt_work;
    wl_columnar_memory_governor_test_before_total_cas = attempt_before_total;
    wl_columnar_memory_governor_test_after_total_cas = attempt_after_total;
    for (unsigned kind = 0; kind < 3; kind++) {
        attempt_admission_fixture_t success;
        attempt_init(&success, kind, 0);
        wl_columnar_memory_admission_status_t status =
            WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
        bool committed = false;
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(attempt_call(&success,
            &status, &committed) == 0);
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(kind == 2 ? committed
            : status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY);
        unsigned count = success.seams;
        const uint64_t prefixes[3][12] = {
            { 74, 74, 74, 128, 128, 128, 212, 212, 212, 212, 241, 241 },
            { 74, 74, 74, 128, 128, 212, 212, 212, 212, 241, 241 },
            { 74, 74, 74, 128, 128, 157, 157 },
        };
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(count ==
            (kind == 0 ? 12 : kind == 1 ? 11 : 7));
        WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(success.control, 0)
            == (kind == 2 ? 157 : 241));
        attempt_dispose(&success, true);
        for (unsigned at = 1; at <= count; at++) {
            for (unsigned fault = 1; fault <= 3; fault++) {
                attempt_admission_fixture_t fixture;
                attempt_init(&fixture, kind, 0);
                wl_columnar_memory_reservation_t prior = fixture.token;
                fixture.fault_at = at;
                fixture.fault_kind = fault;
                status = WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
                committed = false;
                int rc = attempt_call(&fixture, &status, &committed);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(rc == (fault == 1
                    ? WL_EVALUATION_CONTROL_CANCELLED
                    : fault == 2 ? EOVERFLOW : EINVAL));
                unsigned seam_kind = success.seam_kind[at - 1];
                unsigned suffix = fault == 3 ? 0 : seam_kind == 2 ? 2
                    : seam_kind == 3 ? 1 : 0;
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture.hits == 1
                    && fixture.seams == at + suffix);
                if (suffix == 2)
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture.seam_kind[at] ==
                        3);
                if (suffix)
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                        fixture.seam_kind[at + suffix - 1] == 1);
                controlled_token_equal(&fixture.token, &prior);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(wl_columnar_memory_reserved(
                        &fixture.governor) == (kind ? 28 : 20));
                if (fixture.old_buffer)
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(
                        ((unsigned char *)fixture.old_buffer)[7] == 0x22);
                WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture.peer.bytes == 17
                    && fixture.extra.bytes == 3);
                if (fault == 3) {
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(fixture.old_consumed ==
                        prefixes[kind][at - 1]);
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(
                            fixture.control, 0) == 0);
                } else
                    WL_COLUMNAR_MEMORY_GOVERNOR_CHECK(controlled_end(
                            fixture.control, rc) == prefixes[kind][at - 1]);
                attempt_dispose(&fixture, false);
            }
        }
    }
    wl_columnar_memory_governor_test_checkpoint = NULL;
    wl_columnar_memory_governor_test_work_checkpoint = NULL;
    wl_columnar_memory_governor_test_before_total_cas = NULL;
    wl_columnar_memory_governor_test_after_total_cas = NULL;
    PASS();
}

int
main(void)
{
    printf("Memory Governor Unit Tests (Issue #1368)\n");
    printf("=========================================\n");
    test_strong_atomic_compare_exchange();
    test_explicit_values();
    test_automatic_sources();
    test_headroom_boundaries();
    test_reservation_lifecycle();
    test_finish_step_claim_before_receipt();
    test_concurrent_admission();
    test_checked_admission();
    test_downsize();
    test_downsize_interleavings();
    test_downsize_races();
    test_replacement_reservation();
    test_growth_reservation();
    test_growth_refund_attempt();
    test_committed_downsize_attempt();
    test_growth_races();
    wl_columnar_memory_governor_test_work_checkpoint = controlled_work_point;
    wl_columnar_memory_governor_test_checkpoint = controlled_point;
    test_controlled_reserve_lifecycle();
    test_controlled_contention();
    test_controlled_growth_and_commit();
    test_controlled_verdict_precedence();
    test_controlled_overflow_retry_and_boundaries();
    test_controlled_all_region_boundaries();
    test_controlled_raw_admission_twins();
    test_controlled_copied_authority();
    test_prepared_growth_prefixes();
    test_prepared_growth_authority();
    test_prepared_growth_refusals();
    test_attempt_admission_raw_twins();
    test_attempt_admission_bootstrap();
    test_attempt_admission_seams();
    test_attempt_admission_boundaries();
    test_attempt_admission_typed_verdicts();
    wl_columnar_memory_governor_test_work_checkpoint = NULL;
    wl_columnar_memory_governor_test_checkpoint = NULL;
    wl_columnar_memory_governor_test_before_total_cas = NULL;
    wl_columnar_memory_governor_test_after_total_cas = NULL;
    printf("\nPassed %d/%d; Failed %d\n",
        tests_run - tests_failed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
