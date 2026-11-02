/* Finite ordered transfer sibling; no cache or session admission. */
#include "wirelog/columnar/mem_ledger_publication.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #x); abort(); \
                      } } while (0)
typedef wl_columnar_mem_ledger_publication_transfer_prepared_t transfer_t;
typedef wl_columnar_mem_ledger_publication_transfer_request_t request_t;
#define PREPARE wl_columnar_mem_ledger_publication_transfer_prepare_controlled
#define ACQUIRE wl_columnar_mem_ledger_publication_transfer_acquire_pair
#define APPLY wl_columnar_mem_ledger_publication_transfer_apply_prepared_nofail
#define RELEASE \
        wl_columnar_mem_ledger_publication_transfer_release_prepared_nofail
#define CANCEL wl_columnar_mem_ledger_publication_transfer_cancel
#define ACK wl_columnar_mem_ledger_publication_transfer_ack
#define QUERY wl_columnar_mem_ledger_publication_transfer_query
#define INERT WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_INERT
#define READY WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_READY
#define CANCEL_ONLY WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_CANCEL_ONLY
#define HELD WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_HELD
#define APPLIED WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_APPLIED
#define CONSUMED WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_CONSUMED
#define ALLOC WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ALLOC
#define FREE WL_COLUMNAR_MEM_LEDGER_PUBLICATION_FREE

static wl_evaluation_control_t *
control_new(uint64_t limit, const void *owner)
{
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c));
    CHECK(!wl_evaluation_control_attach(c, owner));
    CHECK(!wl_evaluation_control_begin(c, owner));
    return c;
}

static uint64_t
control_end(wl_evaluation_control_t *c, const void *owner, int result)
{
    CHECK(wl_evaluation_control_finish(c, owner, result > 0 ? result : 0,
        0) == result);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(c, &report));
    CHECK(!wl_evaluation_control_detach(c, owner));
    wl_evaluation_control_release(c);
    return report.consumed;
}

static uint64_t
gate(const wl_mem_ledger_t *l)
{
    return atomic_load_explicit(&l->writer_gate, memory_order_acquire);
}

static void
seed(wl_mem_ledger_t *l)
{
    wl_mem_ledger_init(l, 0);
    wl_mem_ledger_alloc(l, WL_MEM_SUBSYS_RELATION, 23);
    wl_mem_ledger_alloc(l, WL_MEM_SUBSYS_CACHE, 17);
    wl_mem_ledger_alloc(l, WL_MEM_SUBSYS_TIMESTAMP, 29);
}

static void
same(const wl_mem_ledger_t *a, const wl_mem_ledger_t *b)
{
    wl_mem_ledger_snapshot_t x, y;
    wl_mem_ledger_snapshot(a, &x); wl_mem_ledger_snapshot(b, &y);
    CHECK(x.current_bytes == y.current_bytes && x.peak_bytes == y.peak_bytes);
    for (unsigned i = 0; i < WL_MEM_SUBSYS_COUNT; ++i)
        CHECK(x.subsys_bytes[i] == y.subsys_bytes[i] &&
            x.subsys_peak[i] == y.subsys_peak[i]);
}

static void
raw(const request_t *r, unsigned count)
{
    for (unsigned i = 0; i < count; ++i) {
        if (r[i].event.kind == ALLOC)
            wl_mem_ledger_alloc(r[i].ledger, r[i].event.subsystem,
                r[i].event.bytes);
        else wl_mem_ledger_free(r[i].ledger, r[i].event.subsystem,
                r[i].event.bytes);
    }
}

static void
test_domains(void)
{
    for (unsigned mode = 0; mode < 5; ++mode) {
        wl_mem_ledger_t a[2], b[2];
        for (unsigned i = 0; i < 2; ++i) {
            seed(&a[i]); seed(&b[i]);
        }
        request_t requests[] = {
            {&a[0], {WL_MEM_SUBSYS_RELATION, FREE, 23}},
            {&a[mode == 1 ? 1 : 0], {WL_MEM_SUBSYS_TIMESTAMP, FREE, 29}},
            {&a[mode == 1 ? 1 : 0], {WL_MEM_SUBSYS_CACHE, ALLOC, 52}}
        };
        unsigned count = mode == 2 ? 0 : 3;
        if (mode == 3) for (unsigned i = 0; i < 3;
                ++i) requests[i].event.bytes = 0;
        if (mode == 4) { /* Order-sensitive clamping and peak, not net transfer. */
            requests[0].event.bytes = 100;
            requests[1].event =
                (wl_columnar_mem_ledger_publication_request_t){
                WL_MEM_SUBSYS_CACHE, ALLOC, 5
            };
            requests[2].event =
                (wl_columnar_mem_ledger_publication_request_t){
                WL_MEM_SUBSYS_CACHE, FREE, 100
            };
        }
        request_t reference[3];
        for (unsigned i = 0; i < 3; ++i) {
            reference[i] = requests[i];
            reference[i].ledger = &b[requests[i].ledger == &a[0] ? 0 : 1];
        }
        raw(reference, count);
        transfer_t p = {0}; int owner = 0;
        wl_evaluation_control_t *c = control_new(0, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        CHECK(!PREPARE(&p, count ? requests : NULL, count, c, id));
        CHECK(p.phase == READY &&
            p.unique_count == (count ? (mode == 1 ? 2 : 1) : 0));
        /* Only copied values survive preparation, not input backing. */
        memset(requests, 0xa5, sizeof(requests));
        CHECK(!ACQUIRE(&p) && p.phase == HELD);
        wl_columnar_mem_ledger_publication_transfer_status_t status;
        CHECK(!QUERY(&p,
            &status) && status.phase == HELD && status.count == count);
        if (count) CHECK(QUERY(&p,
                (wl_columnar_mem_ledger_publication_transfer_status_t *)&a[0])
                == EINVAL);
        CHECK(gate(&a[0]) == (count ? 1u : 0u));
        CHECK(gate(&a[1]) == (mode == 1 ? 1u : 0u));
        APPLY(&p); CHECK(p.phase == APPLIED);
        CHECK(CANCEL(&p) == EBUSY);
        RELEASE(&p); CHECK(!gate(&a[0]) && !gate(&a[1]) && p.phase == CONSUMED);
        same(&a[0], &b[0]); same(&a[1], &b[1]);
        wl_mem_ledger_snapshot_t result;
        wl_mem_ledger_snapshot(&a[0], &result);
        if (mode == 0) CHECK(result.current_bytes == 69 &&
                result.peak_bytes == 69
                && result.subsys_bytes[WL_MEM_SUBSYS_CACHE] == 69
                && !result.subsys_bytes[WL_MEM_SUBSYS_RELATION]
                && !result.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP]);
        if (mode == 2 ||
            mode == 3) CHECK(result.current_bytes == 69 &&
                result.peak_bytes == 69);
        if (mode == 4) CHECK(!result.current_bytes && result.peak_bytes == 69
                && !result.subsys_bytes[WL_MEM_SUBSYS_RELATION]
                && !result.subsys_bytes[WL_MEM_SUBSYS_CACHE]
                && result.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP] == 29);
        CHECK(control_end(c, &owner, 0) == 864+192*count);
        CHECK(!ACK(&p) && !p.identity && !p.control_ref);
    }
    /* Literal saturated counters retain raw FREE/ALLOC ordering and peaks. */
    wl_mem_ledger_t a, b; seed(&a); seed(&b);
    request_t r[] = {{&a, {WL_MEM_SUBSYS_CACHE, ALLOC, UINT64_MAX}},
                     {&a, {WL_MEM_SUBSYS_CACHE, FREE, 9}},
                     {&a, {WL_MEM_SUBSYS_CACHE, ALLOC, 3}}};
    request_t ref[3]; for (unsigned i = 0; i < 3; ++i) {
        ref[i] = r[i]; ref[i].ledger = &b;
    }
    raw(ref, 3);
    transfer_t p = {0}; int owner = 0;
    wl_evaluation_control_t *c = control_new(0, &owner);
    CHECK(!PREPARE(&p, r, 3, c, wl_evaluation_control_active_attempt_id(c)));
    CHECK(!ACQUIRE(&p)); APPLY(&p); RELEASE(&p); CHECK(!ACK(&p));
    same(&a, &b);
    wl_mem_ledger_snapshot_t s; wl_mem_ledger_snapshot(&a, &s);
    CHECK(s.current_bytes == UINT64_MAX-6 && s.peak_bytes == UINT64_MAX
        && s.subsys_bytes[WL_MEM_SUBSYS_CACHE] == UINT64_MAX-6
        && s.subsys_bytes[WL_MEM_SUBSYS_RELATION] == 23
        && s.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP] == 29);
    CHECK(control_end(c, &owner, 0) == 1440);
}

static unsigned hook_calls, stop_at, stop_phase;
static bool raw_hook, reentry_enabled;
static void
checkpoint(transfer_t *p, wl_evaluation_control_t *c, unsigned phase)
{
    ++hook_calls;
    if (raw_hook && phase == 3) {
        for (unsigned i = 0; i < p->unique_count; ++i) {
            CHECK(!gate(p->ledgers[i]));
            wl_mem_ledger_alloc(p->ledgers[i], WL_MEM_SUBSYS_ARENA, 7);
            wl_mem_ledger_free(p->ledgers[i], WL_MEM_SUBSYS_ARENA, 7);
        }
    }
    if (reentry_enabled) {
        transfer_t before = *p;
        wl_columnar_mem_ledger_publication_transfer_status_t status;
        CHECK(PREPARE(p, NULL, 0, c,
            wl_evaluation_control_active_attempt_id(c)) == EBUSY);
        CHECK(ACQUIRE(p) == EBUSY && CANCEL(p) == EBUSY && ACK(p) == EBUSY &&
            QUERY(p, &status) == EBUSY);
        CHECK(!memcmp(&before, p, sizeof(before)));
    }
    if ((stop_at && hook_calls == stop_at) ||
        (stop_phase && phase == stop_phase))
        wl_evaluation_control_request_cancel(c);
}

static void
test_boundaries(void)
{
    wl_mem_ledger_t ledger; seed(&ledger);
    request_t r[3]; for (unsigned i = 0; i < 3; ++i)
        r[i] = (request_t){&ledger, {WL_MEM_SUBSYS_CACHE, ALLOC, i+1}};
    for (unsigned count = 0; count < 4; ++count) {
        uint64_t prefix = 0;
        for (unsigned action = 0; action < 5+count; ++action) {
            uint64_t fee = action == 1 ? 96 : 192;
            uint64_t limit = prefix+fee-1;
            transfer_t p = {0}; int owner = 0;
            wl_evaluation_control_t *c = control_new(limit, &owner);
            int rc = PREPARE(&p, r, count, c,
                    wl_evaluation_control_active_attempt_id(c));
            CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && !gate(&ledger));
            if (action < 3) CHECK(!p.identity && !p.control_ref &&
                    p.phase == INERT);
            else {
                CHECK(p.identity == &p && p.control_ref &&
                    p.phase == CANCEL_ONLY);
                CHECK(ACQUIRE(&p) == EBUSY);
                CHECK(!CANCEL(&p));
            }
            CHECK(control_end(c, &owner, rc) == prefix);
            if (p.identity) CHECK(!ACK(&p)); /* Sole ref, no old-original access. */
            prefix += fee;
        }
        CHECK(prefix == 864+192*count);
        transfer_t p = {0}; int owner = 0;
        wl_evaluation_control_t *c = control_new(prefix, &owner);
        CHECK(!PREPARE(&p, r, count, c,
            wl_evaluation_control_active_attempt_id(c)));
        CHECK(!ACQUIRE(&p)); APPLY(&p); RELEASE(&p);
        CHECK(control_end(c, &owner, 0) == prefix && !ACK(&p));
    }
    /* Late invalid event and third unique ledger retain an actual cancel owner. */
    wl_mem_ledger_t ledgers[3];
    for (unsigned i = 0; i < 3; ++i) seed(&ledgers[i]);
    for (unsigned invalid = 0; invalid < 3; ++invalid) {
        request_t bad[] = {{&ledgers[0], {WL_MEM_SUBSYS_CACHE, ALLOC, 10}},
                           {&ledgers[1], {WL_MEM_SUBSYS_CACHE, ALLOC, 20}},
                           {&ledgers[2], {WL_MEM_SUBSYS_CACHE, ALLOC, 30}}};
        if (invalid == 0) bad[2].event.kind = 99;
        if (invalid == 1) bad[2].ledger = NULL;
        transfer_t p = {0}; int owner = 0;
        wl_evaluation_control_t *c = control_new(0, &owner);
        CHECK(PREPARE(&p, bad, 3, c,
            wl_evaluation_control_active_attempt_id(c)) == EINVAL);
        CHECK(p.phase == CANCEL_ONLY && p.count == 2 && !p.held_mask);
        CHECK(!CANCEL(&p)); CHECK(control_end(c, &owner, EINVAL) == 1056);
        CHECK(!ACK(&p));
        for (unsigned i = 0; i < 3; ++i) {
            wl_mem_ledger_snapshot_t s; wl_mem_ledger_snapshot(&ledgers[i], &s);
            CHECK(s.current_bytes == 69 && !gate(&ledgers[i]));
        }
    }
}

static void
test_stops_and_hooks(void)
{
    for (unsigned phase = 1; phase <= 3; ++phase) {
        wl_mem_ledger_t ledger; seed(&ledger);
        request_t r = {&ledger, {WL_MEM_SUBSYS_CACHE, ALLOC, 10}};
        transfer_t p = {0}; int owner = 0;
        wl_evaluation_control_t *c = control_new(0, &owner);
        stop_phase = phase; hook_calls = 0;
        wl_columnar_mem_ledger_publication_transfer_test_checkpoint =
            checkpoint;
        int rc = PREPARE(&p, &r, 1, c,
                wl_evaluation_control_active_attempt_id(c));
        if (phase < 3) CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED);
        else {
            CHECK(!rc); CHECK(ACQUIRE(&p) == WL_EVALUATION_CONTROL_CANCELLED);
        }
        stop_phase = 0;
        if (p.identity) CHECK(!CANCEL(&p));
        control_end(c, &owner, WL_EVALUATION_CONTROL_CANCELLED);
        if (p.identity) CHECK(!ACK(&p));
        CHECK(!gate(&ledger));
        wl_columnar_mem_ledger_publication_transfer_test_checkpoint = NULL;
    }
    wl_mem_ledger_t l[2]; seed(&l[0]); seed(&l[1]);
    request_t r[] = {{&l[1], {WL_MEM_SUBSYS_CACHE, ALLOC, 5}},
                     {&l[0], {WL_MEM_SUBSYS_CACHE, FREE, 3}}};
    transfer_t p = {0}; int owner = 0;
    wl_evaluation_control_t *c = control_new(0, &owner);
    reentry_enabled = raw_hook = true; hook_calls = 0;
    wl_columnar_mem_ledger_publication_transfer_test_checkpoint = checkpoint;
    CHECK(!PREPARE(&p, r, 2, c, wl_evaluation_control_active_attempt_id(c)));
    CHECK(!ACQUIRE(&p)); unsigned calls = hook_calls;
    APPLY(&p); RELEASE(&p); CHECK(!ACK(&p));
    CHECK(calls == hook_calls && calls == 3);
    reentry_enabled = raw_hook = false;
    wl_columnar_mem_ledger_publication_transfer_test_checkpoint = NULL;
    CHECK(control_end(c, &owner, 0) == 1248);
}

typedef struct {
    wl_mem_ledger_t *ledger;
    wl_mutex_t mutex;
    wl_cond_t condition;
    unsigned stage;
} peer_t;
static void *
peer_hold(void *arg)
{
    peer_t *peer = arg; int owner = 0;
    wl_evaluation_control_t *c = control_new(0, &owner);
    wl_columnar_mem_ledger_publication_prepared_t p = {0};
    wl_columnar_mem_ledger_publication_request_t request = {WL_MEM_SUBSYS_CACHE,
                                                            ALLOC, 0};
    CHECK(!wl_columnar_mem_ledger_publication_prepare_controlled(peer->ledger,
        &request, 1, &p, c));
    CHECK(!wl_columnar_mem_ledger_publication_acquire_controlled(&p));
    CHECK(!wl_mutex_lock(&peer->mutex)); peer->stage = 1;
    CHECK(!wl_cond_signal(&peer->condition));
    while (peer->stage == 1) CHECK(!wl_cond_wait(&peer->condition,
            &peer->mutex));
    CHECK(!wl_mutex_unlock(&peer->mutex));
    CHECK(!wl_columnar_mem_ledger_publication_cancel(&p));
    control_end(c, &owner, 0); return NULL;
}

static void
test_pair_contention(void)
{
    wl_mem_ledger_t l[2]; seed(&l[0]); seed(&l[1]);
    peer_t peer = {.ledger = &l[1]}; wl_thread_t thread;
    CHECK(!wl_mutex_init(&peer.mutex) && !wl_cond_init(&peer.condition));
    CHECK(!wl_thread_create(&thread, peer_hold, &peer));
    CHECK(!wl_mutex_lock(&peer.mutex));
    while (!peer.stage) CHECK(!wl_cond_wait(&peer.condition, &peer.mutex));
    CHECK(!wl_mutex_unlock(&peer.mutex));
    CHECK(gate(&l[1]) == 1 && !gate(&l[0]));
    request_t requests[] = {{&l[1], {WL_MEM_SUBSYS_CACHE, ALLOC, 10}},
                            {&l[0], {WL_MEM_SUBSYS_RELATION, FREE, 10}}};
    transfer_t p = {0}; int owner = 0;
    wl_evaluation_control_t *c = control_new(0, &owner);
    CHECK(!PREPARE(&p, requests, 2, c,
        wl_evaluation_control_active_attempt_id(c)));
    CHECK(p.ledgers[0] == &l[0] && p.ledgers[1] == &l[1]);
    CHECK(ACQUIRE(&p) == EAGAIN && p.phase == CANCEL_ONLY && !p.held_mask);
    CHECK(!gate(&l[0]) && gate(&l[1]) == 1 && p.prepaid_release == 84);
    CHECK(ACQUIRE(&p) == EBUSY);
    CHECK(!CANCEL(&p) && !ACK(&p) && gate(&l[1]) == 1);
    CHECK(!wl_mutex_lock(&peer.mutex)); peer.stage = 2;
    CHECK(!wl_cond_signal(&peer.condition));
    CHECK(!wl_mutex_unlock(&peer.mutex) && !wl_thread_join(&thread));
    CHECK(!gate(&l[0]) && !gate(&l[1]));
    wl_mem_ledger_snapshot_t s;
    wl_mem_ledger_snapshot(&l[0], &s); CHECK(s.current_bytes == 69);
    wl_mem_ledger_snapshot(&l[1], &s); CHECK(s.current_bytes == 69);
    /* Same original cumulative allowance; fresh prepared owner, no replay reset. */
    CHECK(!PREPARE(&p, requests, 2, c,
        wl_evaluation_control_active_attempt_id(c)));
    CHECK(!ACQUIRE(&p)); APPLY(&p); RELEASE(&p); CHECK(!ACK(&p));
    CHECK(control_end(c, &owner, 0) == 2496);
    wl_cond_destroy(&peer.condition); wl_mutex_destroy(&peer.mutex);
}

static void
test_overlap_and_reuse(void)
{
    /* Rejected target overlap is known BEFORE busy, hooks or ownref writes. */
    union { transfer_t transfer; wl_mem_ledger_t ledger; } storage;
    memset(&storage, 0, sizeof(storage));
    wl_mem_ledger_init(&storage.ledger, 0);
    unsigned char before[sizeof(storage)];
    memcpy(before, &storage, sizeof(storage));
    request_t request = {&storage.ledger, {WL_MEM_SUBSYS_CACHE, ALLOC, 1}};
    int owner = 0;
    wl_evaluation_control_t *c = control_new(0, &owner);
    hook_calls = 0;
    wl_columnar_mem_ledger_publication_transfer_test_checkpoint = checkpoint;
    int rc = PREPARE(&storage.transfer, &request, 1, c,
            wl_evaluation_control_active_attempt_id(c));
    CHECK(rc == EINVAL && !memcmp(before, &storage,
        sizeof(storage)) && !hook_calls);
    transfer_t p = {0};
    union { request_t requests[1]; wl_mem_ledger_t ledger; } input_overlap;
    memset(&input_overlap, 0, sizeof(input_overlap));
    wl_mem_ledger_init(&input_overlap.ledger, 0);
    input_overlap.requests[0] = (request_t){&input_overlap.ledger,
                                            {WL_MEM_SUBSYS_CACHE, ALLOC, 1}};
    unsigned char input_before[sizeof(input_overlap)];
    memcpy(input_before, &input_overlap, sizeof(input_overlap));
    CHECK(PREPARE(&p, input_overlap.requests, 1, c,
        wl_evaluation_control_active_attempt_id(c)) == EINVAL
        && !memcmp(input_before, &input_overlap,
        sizeof(input_overlap)) && !hook_calls && !p.identity);
    CHECK(PREPARE(&p, (const request_t *)&p, 1, c,
        wl_evaluation_control_active_attempt_id(c)) == EINVAL && !hook_calls);
    CHECK(PREPARE(&p, (const request_t *)(UINTPTR_MAX-1), 3, c,
        wl_evaluation_control_active_attempt_id(c)) == EINVAL && !hook_calls);
    CHECK(!p.identity && control_end(c, &owner, 0) == 0);
    wl_columnar_mem_ledger_publication_transfer_test_checkpoint = NULL;
    wl_mem_ledger_t ledger; seed(&ledger);
    request = (request_t){&ledger, {WL_MEM_SUBSYS_CACHE, ALLOC, 1}};
    c = control_new(0, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    CHECK(!PREPARE(&p, &request, 1, c, id));
    transfer_t copied = p;
    CHECK(ACQUIRE(&copied) == EINVAL && CANCEL(&copied) == EINVAL &&
        ACK(&copied) == EINVAL);
    CHECK(PREPARE(&p, &request, 1, c, id) == EBUSY);
    /* Finished/reset original: acquire refuses without claiming, but real
     * prepaid cancel/ACK consumes final ownref without consulting old ledger. */
    CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
    CHECK(!wl_evaluation_control_reset(c));
    CHECK(ACQUIRE(&p) == EINVAL && !gate(&ledger));
    CHECK(!wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
    CHECK(!CANCEL(&p) && !ACK(&p));
    c = control_new(0, &owner); id = wl_evaluation_control_active_attempt_id(c);
    CHECK(!PREPARE(&p, &request, 1, c, id) && !ACQUIRE(&p));
    /* Abandoning an already HELD capability after end uses prepaid cancel,
     * never a fallible or successor-attempt apply. */
    CHECK(control_end(c, &owner, 0) == 1056);
    CHECK(!CANCEL(&p) && !gate(&ledger) && !ACK(&p));
}

typedef struct { transfer_t *p; } foreign_t;
static void *
foreign_call(void *arg)
{
    foreign_t *f = arg; wl_columnar_mem_ledger_publication_transfer_status_t s;
    CHECK(ACQUIRE(f->p) == EINVAL && CANCEL(f->p) == EINVAL &&
        ACK(f->p) == EINVAL && QUERY(f->p, &s) == EINVAL);
    return NULL;
}
static void
test_foreign(void)
{
    wl_mem_ledger_t l; seed(&l);
    request_t request = {&l, {WL_MEM_SUBSYS_CACHE, ALLOC, 1}};
    transfer_t p = {0}; int owner = 0;
    wl_evaluation_control_t *c = control_new(0, &owner);
    CHECK(!PREPARE(&p, &request, 1, c,
        wl_evaluation_control_active_attempt_id(c)));
    foreign_t f = {&p}; wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, foreign_call,
        &f) && !wl_thread_join(&thread));
    CHECK(!ACQUIRE(&p)); APPLY(&p); RELEASE(&p); CHECK(!ACK(&p));
    CHECK(control_end(c, &owner, 0) == 1056);
}

static unsigned single_try_hooks;
static const void *single_try_owner;
static bool single_try_turnover;

static void
single_try_checkpoint(wl_evaluation_control_t *c,
    wl_columnar_mem_ledger_publication_prepared_t *p, unsigned phase)
{
    if (phase != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_PRECAS) return;
    CHECK(p->phase == WL_COLUMNAR_MEM_LEDGER_PUBLICATION_READY);
    ++single_try_hooks;
    if (single_try_turnover) {
        CHECK(!wl_evaluation_control_finish(c, single_try_owner, 0, 0));
        CHECK(!wl_evaluation_control_reset(c));
        CHECK(!wl_evaluation_control_begin(c, single_try_owner));
    }
}

static void
test_single_ledger_try(void)
{
    wl_mem_ledger_t ledger, expected;
    seed(&ledger); seed(&expected);
    wl_columnar_mem_ledger_publication_request_t requests[2] = {
        {WL_MEM_SUBSYS_RELATION, FREE, 23},
        {WL_MEM_SUBSYS_TIMESTAMP, FREE, 29}
    };
    int holder_owner = 0, owner = 0;
    wl_evaluation_control_t *holder = control_new(0, &holder_owner);
    wl_columnar_mem_ledger_publication_prepared_t held = {0};
    CHECK(!wl_columnar_mem_ledger_publication_prepare_controlled(
            &ledger, NULL, 0, &held, holder));
    CHECK(!wl_columnar_mem_ledger_publication_try_acquire_controlled(&held)
        && gate(&ledger) == 1);
    wl_columnar_mem_ledger_publication_test_checkpoint = single_try_checkpoint;
    single_try_hooks = 0; single_try_turnover = false;
    wl_evaluation_control_t *c = control_new(0, &owner);
    wl_columnar_mem_ledger_publication_prepared_t candidate = {0};
    CHECK(!wl_columnar_mem_ledger_publication_prepare_controlled(
            &ledger, requests, 2, &candidate, c));
    /* A genuine other publication owns the gate. Unlimited allowance still
     * makes exactly one attempt and returns; the candidate is cancel-only. */
    CHECK(wl_columnar_mem_ledger_publication_try_acquire_controlled(&candidate)
        == EAGAIN && single_try_hooks == 1 && gate(&ledger) == 1);
    CHECK(wl_columnar_mem_ledger_publication_try_acquire_controlled(&candidate)
        == EINVAL && single_try_hooks == 1);
    CHECK(!wl_columnar_mem_ledger_publication_cancel(&candidate));
    CHECK(control_end(c, &owner, 0) == 109 + 166 + 28);
    CHECK(!wl_columnar_mem_ledger_publication_cancel(&held) && !gate(&ledger));
    CHECK(control_end(holder, &holder_owner, 0) == 109 + 166 + 28);
    same(&ledger, &expected);
    for (unsigned exact = 0; exact < 2; ++exact) {
        candidate = (wl_columnar_mem_ledger_publication_prepared_t){0};
        c = control_new(109 + 166 + 28 - !exact, &owner);
        single_try_hooks = 0;
        CHECK(!wl_columnar_mem_ledger_publication_prepare_controlled(
                &ledger, requests, 2, &candidate, c));
        int rc =
            wl_columnar_mem_ledger_publication_try_acquire_controlled(
            &candidate);
        CHECK(rc == (exact ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT)
            && single_try_hooks == exact && gate(&ledger) == exact);
        if (exact) {
            CHECK(!wl_columnar_mem_ledger_publication_apply(&candidate));
            CHECK(!wl_columnar_mem_ledger_publication_release(&candidate));
            wl_mem_ledger_free(&expected, WL_MEM_SUBSYS_RELATION, 23);
            wl_mem_ledger_free(&expected, WL_MEM_SUBSYS_TIMESTAMP, 29);
        } else CHECK(!wl_columnar_mem_ledger_publication_cancel(&candidate));
        CHECK(control_end(c, &owner,
            exact ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT)
            == 109 + 166 + (exact ? 28 : 0));
        same(&ledger, &expected);
    }
    candidate = (wl_columnar_mem_ledger_publication_prepared_t){0};
    c = control_new(0, &owner);
    CHECK(!wl_columnar_mem_ledger_publication_prepare_controlled(
            &ledger, requests, 2, &candidate, c));
    uint64_t old_id = wl_evaluation_control_active_attempt_id(c);
    single_try_owner = &owner; single_try_turnover = true; single_try_hooks = 0;
    CHECK(wl_columnar_mem_ledger_publication_try_acquire_controlled(&candidate)
        == EINVAL && single_try_hooks == 1 && !gate(&ledger));
    uint64_t successor = wl_evaluation_control_active_attempt_id(c);
    CHECK(successor != old_id);
    CHECK(!wl_columnar_mem_ledger_publication_cancel(&candidate));
    same(&ledger, &expected);
    single_try_owner = NULL; single_try_turnover = false;
    wl_columnar_mem_ledger_publication_test_checkpoint = NULL;
    CHECK(!control_end(c, &owner, 0));
}

int
main(int argc, char **argv)
{
    CHECK(argc == 2);
    if (!strcmp(argv[1], "domains")) test_domains();
    else if (!strcmp(argv[1], "failures")) {
        test_boundaries(); test_stops_and_hooks(); test_pair_contention();
        test_single_ledger_try();
    }else if (!strcmp(argv[1], "authority")) {
        test_overlap_and_reuse(); test_foreign();
    }else abort();
    return 0;
}
