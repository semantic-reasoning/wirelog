/*
 * test_memory_governor.c - deterministic memory resolver/governor tests
 *
 * Issue #1368: the governor is a foundation only. Allocation-site enforcement
 * and session ownership integration are separate implementation units.
 */

#include "../wirelog/columnar/memory_governor.h"
#include "wirelog/thread.h"

#include <stdio.h>
#include <string.h>

static int tests_run;
static int tests_failed;

#define TEST(name) do { tests_run++; printf("  [%d] %s", tests_run, name); \
} while (0)
#define PASS() do { printf(" ... PASS\n"); } while (0)
#define FAIL(msg) do { printf(" ... FAIL: %s\n", msg); tests_failed++; \
} while (0)

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
    thread_t threads[THREADS];
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
        if (thread_create(&threads[i], reserve_thread, &args[i]) != 0) {
            FAIL("thread creation failed");
            return 1;
        }
    }
    for (i = 0; i < THREADS; i++)
        thread_join(&threads[i]);
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

void
wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *token,
    unsigned phase)
{
    if (!schedule.armed || token != schedule.token || phase != schedule.phase)
        return;
    /* One-shot nesting models a competing operation while this call is paused. */
    schedule.armed = false;
    schedule.hits++;
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
        thread_t threads[3];
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
            if (thread_create(&threads[i], downsize_thread, &args[i]) != 0)
                break;
            created++;
        }
        atomic_store_explicit(&start, 1, memory_order_release);
        for (unsigned i = 0; i < created; i++)
            thread_join(&threads[i]);
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

int
main(void)
{
    printf("Memory Governor Unit Tests (Issue #1368)\n");
    printf("=========================================\n");
    test_explicit_values();
    test_automatic_sources();
    test_headroom_boundaries();
    test_reservation_lifecycle();
    test_concurrent_admission();
    test_checked_admission();
    test_downsize();
    test_downsize_interleavings();
    test_downsize_races();
    printf("\nPassed %d/%d; Failed %d\n",
        tests_run - tests_failed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
