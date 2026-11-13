/* Description and actual one-shot publication use distinct finite grants. */
#include "wirelog/columnar/mem_ledger_publication.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(e) do { if (!(e)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #e); abort(); \
                      } } while (0)

static int owner, other;
static unsigned fault, fault_stage, hook_calls;
static uint64_t successor;
static wl_evaluation_control_report_t frozen;

static uint64_t
begin(wl_evaluation_control_t *c, uint64_t limit)
{
    CHECK(!wl_evaluation_control_active_attempt_id(c));
    CHECK(!wl_evaluation_control_reset(c)
        && !wl_evaluation_control_configure(c, limit)
        && !wl_evaluation_control_begin(c, &owner));
    return wl_evaluation_control_active_attempt_id(c);
}

static wl_evaluation_control_t *
create(uint64_t limit)
{
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c)
        && !wl_evaluation_control_attach(c, &owner));
    begin(c, limit);
    return c;
}

static wl_evaluation_control_report_t
finish(wl_evaluation_control_t *c, int result)
{
    CHECK(wl_evaluation_control_finish(c, &owner, result > 0 ? result : 0, 0)
        == result);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(c, &report));
    return report;
}

static uint64_t
gate(wl_mem_ledger_t *l)
{
    return atomic_load_explicit(&l->writer_gate, memory_order_acquire);
}

static void
same(const wl_mem_ledger_t *a, const wl_mem_ledger_t *b)
{
    wl_mem_ledger_snapshot_t x, y;
    wl_mem_ledger_snapshot(a, &x); wl_mem_ledger_snapshot(b, &y);
    CHECK(x.current_bytes == y.current_bytes && x.peak_bytes == y.peak_bytes);
    for (unsigned i = 0; i < WL_MEM_SUBSYS_COUNT; ++i)
        CHECK(x.subsys_bytes[i] == y.subsys_bytes[i]
            && x.subsys_peak[i] == y.subsys_peak[i]);
}

/* This test parent owns a separate actual metadata turn. No production ACK
 * tariff or Source/carry bound is inferred from this component harness. */
static void
ack(wl_columnar_mem_ledger_description_t *d, wl_evaluation_control_t *c)
{
    CHECK(d->phase == WL_COLUMNAR_MEM_LEDGER_DESCRIPTION_CONSUMED
        && d->parent == &owner && d->control == c && d->attempted_id);
    uint64_t completed = d->attempted_id;
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_active_attempt_id(c)
        && !wl_evaluation_control_report(c, &report)
        && report.attempt_id == completed);
    uint64_t id = begin(c, 128);
    CHECK(!wl_evaluation_control_charge_attempt(c, id, 128));
    wl_evaluation_control_completion_generation_t view =
        wl_evaluation_control_completion_generation_observe(c,
            d->control_identity);
    CHECK(!view.protocol && view.completed_id == completed &&
        view.active_id == id);
    memset(d, 0, sizeof(*d));
    CHECK(finish(c, 0).consumed == 128);
}

static void
checkpoint(wl_columnar_mem_ledger_description_t *d,
    wl_evaluation_control_t *c, unsigned stage)
{
    CHECK(d->phase == WL_COLUMNAR_MEM_LEDGER_DESCRIPTION_CONSUMED
        && d->attempted_id && !d->applied && !gate(d->ledger));
    ++hook_calls;
    wl_columnar_mem_ledger_description_t unchanged = *d;
    CHECK(wl_columnar_mem_ledger_publication_try_commit_described_controlled(
            d, &owner, c, d->attempted_id) == EINVAL
        && !memcmp(d, &unchanged, sizeof(*d)));
    if (stage != fault_stage) return;
    if (fault == 1) wl_evaluation_control_request_cancel(c);
    else if (fault == 2) {
        CHECK(wl_evaluation_control_charge_attempt(c, d->attempted_id,
            UINT64_C(1000000)) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    } else if (fault == 3) {
        frozen = finish(c, 7);
        CHECK(!wl_evaluation_control_begin(c, &owner));
        successor = wl_evaluation_control_active_attempt_id(c);
        CHECK(successor != d->attempted_id);
    }
}

typedef struct {
    wl_columnar_mem_ledger_description_t *d;
    wl_evaluation_control_t *control;
    uint64_t id;
} foreign_t;
static void *
foreign(void *payload)
{
    foreign_t *p = payload;
    CHECK(wl_columnar_mem_ledger_publication_try_commit_described_controlled(
            p->d, &owner, p->control, p->id) == EINVAL);
    return NULL;
}

static void
publish(wl_columnar_mem_ledger_description_t *d, wl_evaluation_control_t *c,
    wl_mem_ledger_t *ledger,
    const wl_columnar_mem_ledger_publication_request_t *requests,
    unsigned count)
{
    uint64_t id = begin(c, 109);
    CHECK(!wl_columnar_mem_ledger_publication_describe_controlled(d, &owner,
        ledger, requests, count, c, id) && !gate(ledger));
    CHECK(finish(c, 0).consumed == 109);
    id = begin(c, 194);
    CHECK(!wl_columnar_mem_ledger_publication_try_commit_described_controlled(
            d, &owner, c, id) && d->applied && !gate(ledger));
    CHECK(finish(c, 0).consumed == 194);
    ack(d, c);
}

static void
test(unsigned scenario)
{
    wl_mem_ledger_t ledger, reference;
    wl_mem_ledger_init(&ledger, 0); wl_mem_ledger_init(&reference, 0);
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION, 100);
    wl_mem_ledger_alloc(&reference, WL_MEM_SUBSYS_RELATION, 100);
    wl_columnar_mem_ledger_publication_request_t requests[] = {
        {WL_MEM_SUBSYS_RELATION, WL_COLUMNAR_MEM_LEDGER_PUBLICATION_FREE, 37},
        {WL_MEM_SUBSYS_CACHE, WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ALLOC, 19}
    };
    wl_columnar_mem_ledger_description_t d = {0};
    wl_evaluation_control_t *c = create(108);
    wl_evaluation_control_t *ack_funding = create(128);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    CHECK(wl_columnar_mem_ledger_publication_describe_controlled(&d, &owner,
        &ledger, requests, 2, c, id) == WL_EVALUATION_CONTROL_WORK_LIMIT
        && !d.identity && !gate(&ledger));
    CHECK(!finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT).consumed);
    id = begin(c, 109);
    CHECK(!wl_columnar_mem_ledger_publication_describe_controlled(&d, &owner,
        &ledger, requests, 2, c, id) && !gate(&ledger)
        && d.phase == WL_COLUMNAR_MEM_LEDGER_DESCRIPTION_DESCRIBED);
    CHECK(finish(c, 0).consumed == 109);
    same(&ledger, &reference);
    uint64_t limit = scenario == 1 ? 165 : scenario == 2 ? 193 : 256;
    id = begin(c, limit);
    wl_columnar_mem_ledger_description_t saved = d, copied = d;
    CHECK(wl_columnar_mem_ledger_publication_try_commit_described_controlled(
            &copied, &owner, c, id) == EINVAL);
    CHECK(wl_columnar_mem_ledger_publication_try_commit_described_controlled(
            &d, &other, c, id) == EINVAL);
    CHECK(wl_columnar_mem_ledger_publication_try_commit_described_controlled(
            &d, &owner, c, id+1) == EINVAL && !memcmp(&d, &saved, sizeof(d)));
    foreign_t wrong = {&d, c, id}; wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, foreign,
        &wrong) && !wl_thread_join(&thread)
        && !memcmp(&d, &saved, sizeof(d)));
    uint64_t used = UINT64_MAX;
    CHECK(!wl_evaluation_control_test_active_consumed(c, id, &used) && !used);
    requests[0].bytes = 999; /* Genuine copied request remains37. */
    fault = scenario >= 4 && scenario <= 9 ? (scenario-4)/2+1 : 0;
    fault_stage = scenario % 2 ? 2 : 1;
    hook_calls = 0; successor = 0;
    wl_evaluation_control_t *peer = NULL;
    wl_columnar_mem_ledger_publication_prepared_t held = {0};
    if (scenario == 3) {
        peer = create(512);
        CHECK(!wl_columnar_mem_ledger_publication_prepare_controlled(&ledger,
            NULL, 0, &held, peer)
            && !wl_columnar_mem_ledger_publication_try_acquire_controlled(&held)
            && gate(&ledger) == 1);
    }
    if (scenario !=
        3) wl_columnar_mem_ledger_description_test_checkpoint = checkpoint;
    int rc = wl_columnar_mem_ledger_publication_try_commit_described_controlled(
        &d, &owner, c, id);
    wl_columnar_mem_ledger_description_test_checkpoint = NULL;
    int expected = scenario == 1 || scenario == 2 || fault == 2
        ? WL_EVALUATION_CONTROL_WORK_LIMIT : scenario == 3 ? EAGAIN
        : fault == 1 ? WL_EVALUATION_CONTROL_CANCELLED : fault ==
        3 ? EINVAL : 0;
    CHECK(hook_calls == (scenario == 1 || scenario == 3 ? 0u
        : scenario == 2 || (fault && fault_stage == 1) ? 1u : 2u));
    CHECK(rc == expected &&
        d.phase == WL_COLUMNAR_MEM_LEDGER_DESCRIPTION_CONSUMED
        && d.attempted_id == id && d.applied == !expected);
    saved = d;
    CHECK(wl_columnar_mem_ledger_publication_describe_controlled(&d, &owner,
        &ledger, requests, 2, c, id) == EINVAL && !memcmp(&saved, &d,
        sizeof(d)));
    CHECK(wl_columnar_mem_ledger_publication_try_commit_described_controlled(
            &d, &owner, c, id) == EINVAL && !memcmp(&saved, &d, sizeof(d)));
    if (peer) {
        CHECK(!wl_columnar_mem_ledger_publication_cancel(&held) &&
            !gate(&ledger));
        finish(peer, 0);
        CHECK(!wl_evaluation_control_detach(peer, &owner));
        wl_evaluation_control_release(peer);
    }
    CHECK(!gate(&ledger));
    if (!expected) {
        wl_mem_ledger_free(&reference, WL_MEM_SUBSYS_RELATION, 37);
        wl_mem_ledger_alloc(&reference, WL_MEM_SUBSYS_CACHE, 19);
    }
    same(&ledger, &reference);
    wl_evaluation_control_report_t report;
    if (successor) {
        CHECK(!wl_evaluation_control_report(c, &report)
            && !memcmp(&report, &frozen, sizeof(report))
            && !wl_evaluation_control_test_active_consumed(c, successor,
            &used) && !used);
        /* Preserve the old receipt before ending the real successor. */
        CHECK(frozen.attempt_id == d.attempted_id && !d.applied);
        /* A distinct preborn metadata owner acknowledges the actual old
         * receipt while the live successor remains entirely uncharged. */
        uint64_t ack_id = wl_evaluation_control_active_attempt_id(ack_funding);
        CHECK(!wl_evaluation_control_charge_attempt(ack_funding, ack_id, 128));
        wl_evaluation_control_completion_generation_t view =
            wl_evaluation_control_completion_generation_observe(c,
                d.control_identity);
        CHECK(!view.protocol && view.completed_id == d.attempted_id
            && view.active_id == successor
            && !wl_evaluation_control_report(c, &report)
            && report.attempt_id == d.attempted_id);
        memset(&d, 0, sizeof(d));
        CHECK(!wl_evaluation_control_test_active_consumed(c, successor,
            &used) && !used);
        finish(c, 0);
    } else {
        int final = scenario == 10 ? WL_EVALUATION_CONTROL_CANCELLED : rc;
        if (scenario == 10) {
            wl_evaluation_control_request_cancel(c);
            CHECK(wl_evaluation_control_charge_attempt(c, id, 0)
                == WL_EVALUATION_CONTROL_CANCELLED && d.applied);
        }
        report = finish(c, final);
        CHECK(report.attempt_id == id
            && report.consumed == (scenario == 1 ? 0 : scenario == 2
            || (fault && fault_stage == 1) ? 166 : 194));
        ack(&d, c);
    }
    if (expected) {
        requests[0].bytes = 37;
        publish(&d, c, &ledger, requests, 2);
        wl_mem_ledger_free(&reference, WL_MEM_SUBSYS_RELATION, 37);
        wl_mem_ledger_alloc(&reference, WL_MEM_SUBSYS_CACHE, 19);
        same(&ledger, &reference);
    }
    const wl_columnar_mem_ledger_publication_request_t future[] = {
        {WL_MEM_SUBSYS_CACHE, WL_COLUMNAR_MEM_LEDGER_PUBLICATION_FREE, 19},
        {WL_MEM_SUBSYS_RELATION, WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ALLOC, 37}
    };
    publish(&d, c, &ledger, future, 2);
    wl_mem_ledger_free(&reference, WL_MEM_SUBSYS_CACHE, 19);
    wl_mem_ledger_alloc(&reference, WL_MEM_SUBSYS_RELATION, 37);
    same(&ledger, &reference);
    publish(&d, c, &ledger, NULL, 0);
    same(&ledger, &reference);
    const wl_columnar_mem_ledger_publication_request_t zero = {
        WL_MEM_SUBSYS_RELATION, WL_COLUMNAR_MEM_LEDGER_PUBLICATION_FREE, 0
    };
    publish(&d, c, &ledger, &zero, 1);
    same(&ledger, &reference);
    finish(ack_funding, 0);
    CHECK(!wl_evaluation_control_detach(ack_funding, &owner));
    wl_evaluation_control_release(ack_funding);
    CHECK(!wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
}

int
main(void)
{
    for (unsigned scenario = 0; scenario < 11; ++scenario) test(scenario);
    puts(
        "ledger description: finite independent grants and one-shot publication passed");
    return 0;
}
