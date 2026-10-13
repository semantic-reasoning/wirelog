/* Private output-publication protocol tests; no operator activation. */
#include "wirelog/columnar/mem_ledger_publication.h"
#include "wirelog/columnar/memory_governor.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(expression) do { if (!(expression)) { \
                                   fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                       __LINE__, #expression); \
                                   abort(); } } while (0)

typedef wl_columnar_mem_ledger_publication_prepared_t publication_t;
typedef wl_columnar_mem_ledger_publication_request_t request_t;

static int owner;
static unsigned fault_phase;
static unsigned fault_kind;
static unsigned fault_hits;
static unsigned cas_count;
static bool terminal;
static uint64_t prior_consumed;

static wl_evaluation_control_t *
begin(uint64_t limit)
{
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(limit, &control) == 0);
    CHECK(wl_evaluation_control_attach(control, &owner) == 0);
    CHECK(wl_evaluation_control_begin(control, &owner) == 0);
    return control;
}

static uint64_t
end(wl_evaluation_control_t *control, int status)
{
    CHECK(wl_evaluation_control_finish(control, &owner,
        status > 0 ? status : 0, 0) == status);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(control, &report) == 0);
    CHECK(report.result == status);
    CHECK(wl_evaluation_control_detach(control, &owner) == 0);
    wl_evaluation_control_release(control);
    return report.consumed;
}

/* The governor's raw instrumentation must also remain outside cutoff. */
void
wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *token,
    unsigned phase)
{
    (void)token;
    (void)phase;
    CHECK(!terminal);
}

static void
governor_checkpoint(wl_evaluation_control_t *control,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    (void)control;
    (void)token;
    (void)phase;
    CHECK(!terminal);
}

typedef struct {
    wl_mem_ledger_t *ledger;
    wl_mutex_t mutex;
    wl_cond_t condition;
    wl_thread_t thread;
    bool held;
    bool release;
    bool exited;
    bool update;
} peer_t;

static peer_t *contender;
static unsigned release_at;

typedef char wl_columnar_mem_ledger_publication_fixture_quantum_bound[
        (6 + 3 + 44 + 3 + 4 + 117 + 3 + 23 + 3 + 1 + 1
        <= WL_EVALUATION_CONTROL_QUANTUM) ? 1 : -1];

static void *
hold_gate(void *opaque)
{
    peer_t *peer = opaque;
    /* A real writer contributes before holding its real ledger gate. */
    if (peer->update)
        wl_mem_ledger_alloc(peer->ledger, WL_MEM_SUBSYS_ARENA, 3);
    uint64_t expected = 0;
    while (!atomic_compare_exchange_weak_explicit(&peer->ledger->writer_gate,
        &expected, UINT64_C(1), memory_order_acquire, memory_order_relaxed))
        expected = 0;
    CHECK(wl_mutex_lock(&peer->mutex) == 0);
    peer->held = true;
    CHECK(wl_cond_signal(&peer->condition) == 0);
    while (!peer->release)
        CHECK(wl_cond_wait(&peer->condition, &peer->mutex) == 0);
#ifdef _MSC_VER
    (void)atomic_exchange_explicit(&peer->ledger->writer_gate, 0,
        memory_order_release);
#else
    atomic_store_explicit(&peer->ledger->writer_gate, 0, memory_order_release);
#endif
    peer->exited = true;
    CHECK(wl_cond_signal(&peer->condition) == 0);
    CHECK(wl_mutex_unlock(&peer->mutex) == 0);
    return NULL;
}

static void
peer_begin(peer_t *peer, wl_mem_ledger_t *ledger, bool update)
{
    memset(peer, 0, sizeof(*peer));
    peer->ledger = ledger;
    peer->update = update;
    CHECK(wl_mutex_init(&peer->mutex) == 0);
    CHECK(wl_cond_init(&peer->condition) == 0);
    CHECK(wl_thread_create(&peer->thread, hold_gate, peer) == 0);
    CHECK(wl_mutex_lock(&peer->mutex) == 0);
    while (!peer->held)
        CHECK(wl_cond_wait(&peer->condition, &peer->mutex) == 0);
    CHECK(wl_mutex_unlock(&peer->mutex) == 0);
}

static void
peer_release(peer_t *peer)
{
    CHECK(wl_mutex_lock(&peer->mutex) == 0);
    peer->release = true;
    CHECK(wl_cond_signal(&peer->condition) == 0);
    while (!peer->exited)
        CHECK(wl_cond_wait(&peer->condition, &peer->mutex) == 0);
    CHECK(wl_mutex_unlock(&peer->mutex) == 0);
}

static void
peer_end(peer_t *peer)
{
    peer_release(peer);
    CHECK(wl_thread_join(&peer->thread) == 0);
    wl_cond_destroy(&peer->condition);
    wl_mutex_destroy(&peer->mutex);
}

static void
checkpoint(wl_evaluation_control_t *control, publication_t *prepared,
    unsigned phase)
{
    (void)prepared;
    CHECK(!terminal);
    if (phase == WL_COLUMNAR_MEM_LEDGER_PUBLICATION_PRECAS) {
        cas_count++;
        if (contender && cas_count == release_at) {
            peer_release(contender);
            terminal = true;
        }
    }
    if (phase != fault_phase || !fault_kind)
        return;
    fault_hits++;
    if (fault_kind == 1)
        wl_evaluation_control_request_cancel(control);
    else if (fault_kind == 2)
        CHECK(wl_evaluation_control_charge(control, UINT64_MAX) == EOVERFLOW);
    else {
        CHECK(wl_evaluation_control_finish(control, &owner, 0, 0) == 0);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(control, &report) == 0);
        prior_consumed = report.consumed;
        CHECK(wl_evaluation_control_reset(control) == 0);
        CHECK(wl_evaluation_control_begin(control, &owner) == 0);
    }
}

static void
equal_ledgers(wl_mem_ledger_t *actual, wl_mem_ledger_t *reference)
{
    wl_mem_ledger_snapshot_t a, b;
    wl_mem_ledger_snapshot(actual, &a);
    wl_mem_ledger_snapshot(reference, &b);
    CHECK(a.total_budget == b.total_budget);
    CHECK(a.current_bytes == b.current_bytes && a.peak_bytes == b.peak_bytes);
    for (unsigned i = 0; i < WL_MEM_SUBSYS_COUNT; i++)
        CHECK(a.subsys_bytes[i] == b.subsys_bytes[i]
            && a.subsys_peak[i] == b.subsys_peak[i]);
}

static void
raw_requests(wl_mem_ledger_t *ledger, const request_t *requests, unsigned count)
{
    for (unsigned i = 0; i < count; i++) {
        if (requests[i].kind == WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ALLOC)
            wl_mem_ledger_alloc(ledger, requests[i].subsystem,
                requests[i].bytes);
        else
            wl_mem_ledger_free(ledger, requests[i].subsystem,
                requests[i].bytes);
    }
}

static void
test_ordered_arithmetic(void)
{
    const request_t cases[][2] = {
        {{0, 1, 0}, {0, 2, 0}},
        {{0, 1, 8}, {0, 1, 16}},
        {{0, 2, 8}, {0, 1, 16}},
        {{0, 1, UINT64_MAX}, {0, 2, 9}},
        {{0, 2, UINT64_MAX}, {2, 1, UINT64_MAX}},
        {{0, 2, 31}, {0, 2, 7}},
        {{4, 1, 12}, {2, 2, 40}},
    };
    for (unsigned count = 0; count <= 2; count++) {
        for (unsigned c = 0; c < sizeof(cases) / sizeof(cases[0]); c++) {
            wl_mem_ledger_t actual, reference;
            wl_mem_ledger_init(&actual, 0);
            wl_mem_ledger_init(&reference, 0);
            wl_mem_ledger_alloc(&actual, 0, 8);
            wl_mem_ledger_alloc(&actual, 2, 17);
            wl_mem_ledger_alloc(&reference, 0, 8);
            wl_mem_ledger_alloc(&reference, 2, 17);
            wl_evaluation_control_t *control = begin(303);
            publication_t prepared = {0};
            request_t copied[2];
            memcpy(copied, cases[c], sizeof(copied));
            CHECK(wl_columnar_mem_ledger_publication_prepare_controlled(
                    &actual, count ? copied : NULL, count, &prepared,
                    control) == 0);
            /* The requests are owned by the prepared record after prepare. */
            memset(copied, 0, sizeof(copied));
            CHECK(wl_columnar_mem_ledger_publication_acquire_controlled(
                    &prepared) == 0);
            terminal = true;
            CHECK(wl_columnar_mem_ledger_publication_apply(&prepared) == 0);
            CHECK(wl_columnar_mem_ledger_publication_cancel(&prepared) ==
                EINVAL);
            CHECK(wl_columnar_mem_ledger_publication_release(&prepared) == 0);
            terminal = false;
            raw_requests(&reference, cases[c], count);
            equal_ledgers(&actual, &reference);
            CHECK(end(control, 0) == 303);
        }
    }
    puts("ordered current-counter arithmetic: PASS (21 cases)");
}

static void
test_admission_boundaries(void)
{
    /* Literal source inventories, independent of production constants. */
    unsigned prepare_slots[] = {42, 50, 17};
    unsigned apply_slots[] = {16, 8, 4, 15, 72, 2};
    unsigned round_slots[] = {12, 1, 3, 2, 1, 9};
    unsigned p = 0, a = 0, r = 0;
    for (unsigned i = 0; i < 3; i++) p += prepare_slots[i];
    for (unsigned i = 0; i < 6; i++) {
        a += apply_slots[i]; r += round_slots[i];
    }
    CHECK(p == 109 && a == 117 && r == 28);
    const uint64_t limits[] = {1, 108, 109, 274, 275, 302, 303};
    const uint64_t consumed[] = {0, 0, 109, 109, 275, 275, 303};
    for (unsigned i = 0; i < sizeof(limits) / sizeof(limits[0]); i++) {
        wl_mem_ledger_t ledger, reference;
        wl_mem_ledger_init(&ledger, 0);
        wl_mem_ledger_init(&reference, 0);
        publication_t prepared = {0};
        wl_evaluation_control_t *control = begin(limits[i]);
        int rc = wl_columnar_mem_ledger_publication_prepare_controlled(
            &ledger, NULL, 0, &prepared, control);
        if (rc == 0) {
            rc =
                wl_columnar_mem_ledger_publication_acquire_controlled(
                &prepared);
            if (rc == 0) {
                CHECK(wl_columnar_mem_ledger_publication_apply(&prepared) == 0);
                CHECK(wl_columnar_mem_ledger_publication_release(&prepared) ==
                    0);
            } else {
                CHECK(wl_columnar_mem_ledger_publication_acquire_controlled(
                        &prepared) == EINVAL);
                CHECK(wl_columnar_mem_ledger_publication_cancel(&prepared) ==
                    0);
            }
        }
        CHECK(rc == (limits[i] == 303 ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
        equal_ledgers(&ledger, &reference);
        CHECK(end(control, rc) == consumed[i]);
    }
    puts("independent source costs and admission boundaries: PASS");
}

static void
test_faults(void)
{
    const uint64_t prefixes[] = {109, 109, 275, 303};
    for (unsigned phase = 1; phase <= 4; phase++) {
        for (unsigned kind = 1; kind <= 3; kind++) {
            wl_mem_ledger_t ledger, reference;
            wl_mem_ledger_init(&ledger, 0);
            wl_mem_ledger_init(&reference, 0);
            publication_t prepared = {0};
            wl_evaluation_control_t *control = begin(0);
            fault_phase = phase;
            fault_kind = kind;
            fault_hits = 0;
            int rc = wl_columnar_mem_ledger_publication_prepare_controlled(
                &ledger, NULL, 0, &prepared, control);
            if (rc == 0) {
                rc = wl_columnar_mem_ledger_publication_acquire_controlled(
                    &prepared);
                CHECK(wl_columnar_mem_ledger_publication_acquire_controlled(
                        &prepared) == EINVAL);
                CHECK(wl_columnar_mem_ledger_publication_cancel(&prepared) ==
                    0);
            }
            CHECK(rc == (kind == 1 ? WL_EVALUATION_CONTROL_CANCELLED
                : kind == 2 ? EOVERFLOW : EINVAL));
            CHECK(fault_hits == 1);
            equal_ledgers(&ledger, &reference);
            if (kind == 3) {
                CHECK(prior_consumed == prefixes[phase - 1]);
                CHECK(end(control, 0) == 0);
            } else
                CHECK(end(control, rc) == prefixes[phase - 1]);
            fault_phase = fault_kind = 0;
        }
    }
    puts("all callbacks: cancellation, overflow, stale successor: PASS");
}

static void *
wrong_thread(void *opaque)
{
    publication_t *prepared = opaque;
    CHECK(wl_columnar_mem_ledger_publication_acquire_controlled(prepared) ==
        EINVAL);
    CHECK(wl_columnar_mem_ledger_publication_apply(prepared) == EINVAL);
    CHECK(wl_columnar_mem_ledger_publication_release(prepared) == EINVAL);
    CHECK(wl_columnar_mem_ledger_publication_cancel(prepared) == EINVAL);
    return NULL;
}

static void
reject_foreign(publication_t *prepared)
{
    publication_t copy = *prepared;
    CHECK(wl_columnar_mem_ledger_publication_acquire_controlled(&copy) ==
        EINVAL);
    CHECK(wl_columnar_mem_ledger_publication_apply(&copy) == EINVAL);
    CHECK(wl_columnar_mem_ledger_publication_release(&copy) == EINVAL);
    CHECK(wl_columnar_mem_ledger_publication_cancel(&copy) == EINVAL);
    wl_thread_t thread;
    CHECK(wl_thread_create(&thread, wrong_thread, prepared) == 0);
    CHECK(wl_thread_join(&thread) == 0);
}

static void
test_authority(void)
{
    for (unsigned phase = 0; phase < 3; phase++) {
        wl_mem_ledger_t ledger;
        wl_mem_ledger_init(&ledger, 0);
        wl_evaluation_control_t *control = begin(0);
        publication_t prepared = {0};
        CHECK(wl_columnar_mem_ledger_publication_prepare_controlled(&ledger,
            NULL, 0, &prepared, control) == 0);
        reject_foreign(&prepared);
        CHECK(wl_columnar_mem_ledger_publication_prepare_controlled(&ledger,
            NULL, 0, &prepared, control) == EINVAL);
        CHECK(prepared.phase == WL_COLUMNAR_MEM_LEDGER_PUBLICATION_READY);
        if (phase) {
            CHECK(wl_columnar_mem_ledger_publication_acquire_controlled(
                    &prepared) == 0);
            reject_foreign(&prepared);
            if (phase == 2) {
                CHECK(wl_columnar_mem_ledger_publication_apply(&prepared) == 0);
                reject_foreign(&prepared);
            }
        }
        CHECK(wl_evaluation_control_finish(control, &owner, 0, 0) == 0);
        CHECK(wl_evaluation_control_reset(control) == 0);
        CHECK(wl_evaluation_control_begin(control, &owner) == 0);
        CHECK(wl_columnar_mem_ledger_publication_apply(&prepared) == EINVAL);
        if (phase == 2) {
            CHECK(wl_columnar_mem_ledger_publication_cancel(&prepared) ==
                EINVAL);
            CHECK(wl_columnar_mem_ledger_publication_release(&prepared) == 0);
        } else
            CHECK(wl_columnar_mem_ledger_publication_cancel(&prepared) == 0);
        /* Consumed originals must never release another writer's gate. */
        peer_t peer;
        peer_begin(&peer, &ledger, false);
        CHECK(wl_columnar_mem_ledger_publication_release(&prepared) == EINVAL);
        CHECK(wl_columnar_mem_ledger_publication_cancel(&prepared) == EINVAL);
        CHECK(atomic_load_explicit(&ledger.writer_gate,
            memory_order_acquire) == 1);
        peer_end(&peer);
        CHECK(end(control, 0) == 0);
    }
    puts("original/copy/thread/stale/consumed authority: PASS");
}

static void
test_invalid_and_late_cancel(void)
{
    for (unsigned invalid = 0; invalid < 7; invalid++) {
        wl_mem_ledger_t ledger, reference;
        wl_mem_ledger_init(&ledger, 0);
        wl_mem_ledger_init(&reference, 0);
        publication_t prepared = {0};
        request_t requests[] = {{0, 1, 8}, {0, 2, 3}};
        if (invalid == 3) requests[1].subsystem = -1;
        if (invalid == 4) requests[1].subsystem = WL_MEM_SUBSYS_COUNT;
        if (invalid == 5) requests[1].kind = 0;
        if (invalid == 6) prepared.requests[1].bytes = 1;
        wl_evaluation_control_t *control = begin(0);
        CHECK(wl_columnar_mem_ledger_publication_prepare_controlled(
                invalid == 0 ? NULL : &ledger,
                invalid == 1 ? NULL : requests, invalid == 2 ? 3 : 2,
                &prepared, control) == EINVAL);
        CHECK(prepared.phase == WL_COLUMNAR_MEM_LEDGER_PUBLICATION_INERT);
        equal_ledgers(&ledger, &reference);
        CHECK(end(control, EINVAL) == 109);
    }
    wl_mem_ledger_t ledger, reference;
    wl_mem_ledger_init(&ledger, 0);
    wl_mem_ledger_init(&reference, 0);
    request_t requests[] = {{0, 1, 8}, {0, 1, 16}};
    publication_t prepared = {0};
    wl_evaluation_control_t *control = begin(303);
    CHECK(wl_columnar_mem_ledger_publication_prepare_controlled(&ledger,
        requests, 2, &prepared, control) == 0);
    CHECK(wl_columnar_mem_ledger_publication_acquire_controlled(&prepared) ==
        0);
    prepared.requests[1].kind = 0;
    CHECK(wl_columnar_mem_ledger_publication_apply(&prepared) == EINVAL);
    equal_ledgers(&ledger, &reference);
    CHECK(prepared.phase == WL_COLUMNAR_MEM_LEDGER_PUBLICATION_HELD);
    prepared.requests[1].kind = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ALLOC;
    /* A late request after the last cutoff cannot interrupt finite commit. */
    wl_evaluation_control_request_cancel(control);
    terminal = true;
    CHECK(wl_columnar_mem_ledger_publication_apply(&prepared) == 0);
    CHECK(wl_columnar_mem_ledger_publication_release(&prepared) == 0);
    terminal = false;
    raw_requests(&reference, requests, 2);
    equal_ledgers(&ledger, &reference);
    CHECK(wl_evaluation_control_finish(control, &owner, 0, 0) == 0);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(control,
        &report) == 0 && report.consumed == 303);
    CHECK(wl_evaluation_control_begin(control, &owner) == 0);
    CHECK(wl_evaluation_control_charge(control,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(end(control, WL_EVALUATION_CONTROL_CANCELLED) == 0);
    puts(
        "invalid inputs, pre-write validation and late sticky cancellation: PASS");
}

typedef struct {
    wl_mem_ledger_t ledger;
    wl_columnar_memory_governor_t governor;
    wl_columnar_memory_reservation_t token, peer, extra;
    void *old_buffer, *incoming_buffer, *peer_buffer, *extra_buffer;
    void *canonical;
    size_t canonical_bytes;
    unsigned generation;
    bool published;
} fixture_t;

static void
fixture_init(fixture_t *fixture, bool extra)
{
    memset(fixture, 0, sizeof(*fixture));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = 1024, .usable_bytes = 1024,
        .mode = WL_COLUMNAR_MEMORY_MODE_ADVISORY,
    };
    CHECK(wl_columnar_memory_governor_init(&fixture->governor,
        &resolution) == 0);
    wl_columnar_memory_reservation_init(&fixture->token);
    wl_columnar_memory_reservation_init(&fixture->peer);
    wl_columnar_memory_reservation_init(&fixture->extra);
    fixture->old_buffer = malloc(8);
    fixture->incoming_buffer = malloc(16);
    fixture->peer_buffer = malloc(17);
    CHECK(fixture->old_buffer && fixture->incoming_buffer &&
        fixture->peer_buffer);
    memset(fixture->old_buffer, 0x17, 8);
    memset(fixture->incoming_buffer, 0x29, 16);
    CHECK(wl_columnar_memory_reserve(&fixture->governor, 8, &fixture->token));
    CHECK(wl_columnar_memory_commit(&fixture->token, fixture->old_buffer));
    CHECK(wl_columnar_memory_reserve(&fixture->governor, 17, &fixture->peer));
    CHECK(wl_columnar_memory_commit(&fixture->peer, fixture->peer_buffer));
    CHECK(wl_columnar_memory_begin_growth(&fixture->token, 16)
        == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY);
    if (extra) {
        fixture->extra_buffer = malloc(3);
        CHECK(fixture->extra_buffer);
        CHECK(wl_columnar_memory_reserve(&fixture->governor, 3,
            &fixture->extra));
        CHECK(wl_columnar_memory_commit(&fixture->extra,
            fixture->extra_buffer));
    }
    wl_mem_ledger_init(&fixture->ledger, 0);
    wl_mem_ledger_alloc(&fixture->ledger, 0, 8);
    wl_mem_ledger_alloc(&fixture->ledger, 2, 17);
    fixture->canonical = fixture->old_buffer;
    fixture->canonical_bytes = 8;
}

/* Exact whole post-CAS fixture: success exit6 + caller3 + governor44+3
 * +four stores4 +apply117+3 +release23+3 +return1 =207 (208 incl CAS).
 * No checks, callbacks, polls or dynamic work in the successful held region.
 * Real B1 relation/lease work must receive its own source inventory.
 */
static int
publish(fixture_t *fixture, publication_t *ledger,
    wl_columnar_memory_governor_growth_commit_prepared_t *growth)
{
    int rc = wl_columnar_mem_ledger_publication_acquire_controlled(ledger);
    if (rc)
        return rc;
    rc = wl_columnar_memory_governor_growth_commit_finish(growth);
    if (rc)
        abort();
    fixture->canonical = fixture->incoming_buffer;
    fixture->canonical_bytes = 16;
    fixture->generation = 1;
    fixture->published = true;
    rc = wl_columnar_mem_ledger_publication_apply(ledger);
    if (rc)
        abort();
    rc = wl_columnar_mem_ledger_publication_release(ledger);
    if (rc)
        abort();
    return 0;
}

static void
fixture_dispose(fixture_t *fixture, bool success)
{
    if (success) {
        free(fixture->old_buffer);
        CHECK(wl_columnar_memory_reservation_downsize(&fixture->token, 16));
        free(fixture->incoming_buffer);
        wl_mem_ledger_free(&fixture->ledger, 0, 16);
    } else {
        free(fixture->incoming_buffer);
        CHECK(wl_columnar_memory_rollback_growth(&fixture->token));
        free(fixture->old_buffer);
        wl_mem_ledger_free(&fixture->ledger, 0, 8);
    }
    CHECK(wl_columnar_memory_release(&fixture->token));
    CHECK(wl_columnar_memory_reserved(&fixture->governor)
        == (fixture->extra_buffer ? 20 : 17));
    if (fixture->extra_buffer) {
        free(fixture->extra_buffer);
        CHECK(wl_columnar_memory_release(&fixture->extra));
        wl_mem_ledger_free(&fixture->ledger, 1, 3);
    }
    CHECK(wl_columnar_memory_reserved(&fixture->governor) == 17);
    free(fixture->peer_buffer);
    CHECK(wl_columnar_memory_release(&fixture->peer));
    wl_mem_ledger_free(&fixture->ledger, 2, 17);
    CHECK(wl_columnar_memory_reserved(&fixture->governor) == 0);
    wl_mem_ledger_snapshot_t snapshot;
    wl_mem_ledger_snapshot(&fixture->ledger, &snapshot);
    CHECK(snapshot.current_bytes == 0);
}

static void
test_composition(void)
{
    const request_t requests[] = {{0, 2, 8}, {0, 1, 16}};
    for (unsigned attempts = 1; attempts <= 4; attempts++) {
        for (unsigned denied = 0; denied < 2; denied++) {
            fixture_t fixture;
            fixture_init(&fixture, true);
            wl_evaluation_control_t *control = begin(503 + 28 * attempts -
                    denied);
            publication_t ledger = {0};
            wl_columnar_memory_governor_growth_commit_prepared_t growth = {0};
            bool ready = false;
            CHECK(wl_columnar_memory_governor_growth_commit_prepare_controlled(
                    &fixture.token, &fixture.governor, fixture.old_buffer,
                    &growth, &ready, control) == 0 && ready);
            CHECK(wl_columnar_mem_ledger_publication_prepare_controlled(
                    &fixture.ledger, requests, 2, &ledger, control) == 0);
            peer_t peer;
            peer_begin(&peer, &fixture.ledger, true);
            contender = &peer;
            release_at = attempts;
            cas_count = 0;
            int rc = publish(&fixture, &ledger, &growth);
            terminal = false;
            contender = NULL;
            peer_end(&peer);
            CHECK(cas_count == attempts - denied);
            if (denied) {
                CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT);
                CHECK(fixture.canonical == fixture.old_buffer
                    && fixture.canonical_bytes == 8 && !fixture.published);
                CHECK(wl_columnar_mem_ledger_publication_cancel(&ledger) == 0);
                CHECK(wl_columnar_memory_governor_growth_commit_cancel(
                        &growth) == 0);
                CHECK(fixture.token.bytes == 8 &&
                    fixture.token.replacement_bytes == 16);
            } else {
                CHECK(rc == 0 && fixture.canonical == fixture.incoming_buffer
                    && fixture.canonical_bytes == 16 &&
                    fixture.generation == 1);
                CHECK(fixture.token.bytes == 24 &&
                    fixture.token.replacement_bytes == 0);
            }
            CHECK(wl_columnar_memory_reserved(&fixture.governor) == 44);
            CHECK(fixture.peer.bytes == 17 && fixture.extra.bytes == 3);
            CHECK(((unsigned char *)fixture.old_buffer)[7] == 0x17);
            CHECK(((unsigned char *)fixture.incoming_buffer)[15] == 0x29);
            wl_mem_ledger_t reference;
            wl_mem_ledger_init(&reference, 0);
            wl_mem_ledger_alloc(&reference, 0, 8);
            wl_mem_ledger_alloc(&reference, 2, 17);
            wl_mem_ledger_alloc(&reference, 1, 3);
            if (!denied) raw_requests(&reference, requests, 2);
            equal_ledgers(&fixture.ledger, &reference);
            CHECK(end(control, rc) == 503 + 28 * (attempts - denied));
            fixture_dispose(&fixture, !denied);
        }
    }
    puts(
        "real governor/storage/peer gate composition, exact/minus-one retries: PASS");
}

int
main(void)
{
    wl_columnar_mem_ledger_publication_test_checkpoint = checkpoint;
    wl_columnar_memory_governor_test_checkpoint = governor_checkpoint;
    test_ordered_arithmetic();
    test_admission_boundaries();
    test_faults();
    test_authority();
    test_invalid_and_late_cancel();
    test_composition();
    wl_columnar_memory_governor_test_checkpoint = NULL;
    wl_columnar_mem_ledger_publication_test_checkpoint = NULL;
    return 0;
}
