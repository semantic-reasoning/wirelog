/* Timestamp retirement owns physical storage and charged ledger bytes
 * independently. Deferred ledger accounting must not fabricate a refund. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/thread.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__,#x); abort(); } } while (0)
static int owner;
static void restart(wl_evaluation_control_t *c, uint64_t limit)
{
    if (wl_evaluation_control_active_attempt_id(c))
        (void)wl_evaluation_control_finish(c, &owner, 0, 0);
    CHECK(!wl_evaluation_control_reset(c));
    CHECK(!wl_evaluation_control_configure(c, limit));
    CHECK(!wl_evaluation_control_begin(c, &owner));
}
static uint64_t reserved(wl_columnar_memory_governor_ref_t *g)
{
    return g?
           wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)):
           0;
}
static void stamps_equal(const col_rel_t *r, const col_delta_timestamp_t *saved)
{
    for (unsigned i = 0; i < 3; ++i) {
        const col_delta_timestamp_t *a = &r->timestamps[i], *b = &saved[i];
        CHECK(a->iteration == b->iteration && a->stratum == b->stratum
            && a->worker == b->worker && a->_reserved == 0 &&
            a->multiplicity == b->multiplicity);
    }
}
static uint64_t ledger_bytes(wl_mem_ledger_t *l)
{
    wl_mem_ledger_snapshot_t s; wl_mem_ledger_snapshot(l, &s);
    return s.current_bytes;
}

static void
staged_ledger(wl_columnar_relation_scalar_retirement_t *s,
    wl_evaluation_control_t *c, wl_mem_ledger_t *ledger,
    wl_columnar_memory_governor_ref_t *g)
{
    wl_columnar_mem_ledger_retirement_journal_t journal = {0};
    uint64_t credit = reserved(g), before = ledger_bytes(ledger);
    unsigned attempts = 0, acks = 0;
    bool ack_refused = false, empty_refused = false;
    bool billed = s->ledger && (s->ledger_bytes || s->ledger_timestamp_bytes);
    for (unsigned turn = 0; s->phase == 12; ++turn) {
        /* Three commit outcomes, each DESC/COMMIT/refusedACK/ACK, plus the
         * no-ledger exact/one-short metadata alternative. */
        CHECK(turn < 16);
        wl_columnar_mem_ledger_description_t *d = &journal.description;
        bool commit = d->phase == WL_COLUMNAR_MEM_LEDGER_DESCRIPTION_DESCRIBED;
        bool ack = d->phase == WL_COLUMNAR_MEM_LEDGER_DESCRIPTION_CONSUMED;
        uint64_t limit = !billed ? (empty_refused ? 128 : 127)
            : commit ? (attempts == 0 ? 165 : attempts == 1 ? 193 : 194)
            : ack ? (ack_refused ? 128 : 127) : 109;
        restart(c, limit);
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        wl_evaluation_control_report_t captured = journal.report;
        int rc = wl_columnar_relation_scalar_retirement_ledger_step_described(
            s, &journal, &owner, c, id);
        CHECK(rc == EAGAIN || rc == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(reserved(g) == credit && s->storage.relation);
        if (commit) {
            CHECK(d->phase == WL_COLUMNAR_MEM_LEDGER_DESCRIPTION_CONSUMED
                && d->attempted_id == id && d->applied == (attempts == 2));
            if (attempts < 2) CHECK(ledger_bytes(ledger) == before);
            else {
                CHECK(ledger_bytes(ledger) == 37);
                wl_evaluation_control_request_cancel(c);
                CHECK(wl_evaluation_control_charge_attempt(c, id, 0)
                    == WL_EVALUATION_CONTROL_CANCELLED);
            }
            ++attempts;
        } else if (ack) {
            CHECK(!memcmp(&captured, &journal.report, sizeof(captured)));
            if (!ack_refused) {
                CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT
                    && journal.report_valid && s->phase == 12);
                ack_refused = true;
            } else {
                CHECK(!journal.report_valid && !d->identity);
                ack_refused = false; ++acks;
            }
        } else if (!billed) empty_refused = true;
        int financial = wl_evaluation_control_finish(c, &owner, 0, 0);
        CHECK(!financial || financial == WL_EVALUATION_CONTROL_WORK_LIMIT
            || financial == WL_EVALUATION_CONTROL_CANCELLED);
        if (commit) {
            CHECK(!wl_columnar_mem_ledger_retirement_capture_owned(&journal,
                &owner, c, id));
            CHECK(journal.report.consumed == (attempts == 1 ? 0
                : attempts == 2 ? 166 : 194));
        }
    }
    CHECK(s->phase == 10 && s->storage.relation
        && !s->storage.relation->mem_ledger
        && !s->storage.relation->ledger_ts_bytes
        && !s->ledger && !s->ledger_bytes && !s->ledger_timestamp_bytes
        && !journal.description.identity && !journal.report_valid
        && (!billed || (attempts == 3 && acks == 3))
        && ledger_bytes(ledger) == 37 && reserved(g) == credit);
}

static void retirement(uint32_t width, bool governed, unsigned ledger_mode,
    bool described)
{
    wl_columnar_memory_resolution_t resolution =
    {.budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
     .usable_bytes =
         WL_COLUMNAR_MEMORY_MIN_BUDGET,
     .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
    wl_columnar_memory_governor_ref_t *g =
        governed?wl_columnar_memory_governor_ref_create(&resolution):NULL;
    CHECK(!governed || g);
    col_rel_t *r = NULL;
    if (g)CHECK(!wl_columnar_relation_new_auto_governed("physical retire",
            width,
            4, false, g, &r));
    else r = col_rel_new_auto("physical retire", width);
    CHECK(r);
    int64_t values[257] = {0};
    for (unsigned row = 0; row < 3;
        ++row) CHECK(!col_rel_append_row(r, values));
    wl_mem_ledger_t ledger; wl_mem_ledger_init(&ledger, 0);
    void *other_owner = malloc(37); CHECK(other_owner);
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_TIMESTAMP, 37);
    /* For ungoverned mode2 the existing enable API creates the legal unbilled
     * timestamp image on an attached ledger. Governed mode2 attaches the new
     * ledger after physical creation, before its first reconciliation. */
    if (ledger_mode && !(governed && ledger_mode == 2)) {
        r->mem_ledger = &ledger;
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION,
            col_rel_owned_ledger_bytes(r));
    }
    CHECK(!col_rel_enable_timestamps(r));
    if (governed && ledger_mode == 2) {
        r->mem_ledger = &ledger;
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION,
            col_rel_owned_ledger_bytes(r));
    }
    uint64_t physical_ts = (uint64_t)r->timestamp_capacity*
        sizeof(*r->timestamps);
    if (ledger_mode == 1 && !r->ledger_ts_bytes) {
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_TIMESTAMP, physical_ts);
        r->ledger_ts_bytes = physical_ts;
    }
    CHECK(ledger_mode != 2 || !r->ledger_ts_bytes);
    col_delta_timestamp_t saved[3]; const int64_t weights[] = {0, 3, -4};
    for (unsigned i = 0; i < 3;
        ++i) r->timestamps[i] = saved[i] = (col_delta_timestamp_t){
            .iteration = 91-i, .stratum = i+2, .worker = i%2, ._reserved = 0,
            .multiplicity = weights[i]
        };
    col_delta_timestamp_t *allocation = r->timestamps;
    uint32_t capacity = r->timestamp_capacity;
    col_relation_snapshot_t before = wl_columnar_relation_snapshot(r);
    uint64_t retained = reserved(g), charged = ledger_bytes(&ledger),
        ts_charge = r->ledger_ts_bytes;
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(0, &c));
    CHECK(!wl_evaluation_control_attach(c, &owner)); restart(c, 1);
    wl_columnar_relation_scalar_retirement_t receipt = {0};
    wl_columnar_source_access_reader_t pin = {0};
    CHECK(!col_rel_source_reader_acquire(r, &pin));
    CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt, r));
    CHECK(wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
        wl_evaluation_control_active_attempt_id(c)) == EAGAIN);
    CHECK(!wl_columnar_relation_scalar_retirement_cancel(&receipt));
    CHECK(!col_rel_source_reader_release(&pin));
    restart(c, 0);
    CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt, r));
    CHECK(wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
        wl_evaluation_control_active_attempt_id(c)) == EAGAIN);
    CHECK(wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
        wl_evaluation_control_active_attempt_id(c)) == EAGAIN);
    CHECK(receipt.phase == 2); /* actual timestamp shape/charge captured */
    wl_evaluation_control_request_cancel(c);
    CHECK(wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
        wl_evaluation_control_active_attempt_id(c)) ==
        WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!wl_columnar_relation_scalar_retirement_cancel(&receipt));
    CHECK(r->timestamps == allocation && r->timestamp_capacity == capacity
        && r->ledger_ts_bytes == ts_charge && reserved(g) == retained &&
        ledger_bytes(&ledger) == charged);
    CHECK(wl_columnar_relation_snapshot_equal(before,
        wl_columnar_relation_snapshot(r))); stamps_equal(r, saved);
    restart(c, 0);
    CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt, r));
    int rc; unsigned turns = 0;
    do{
        rc = wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
                wl_evaluation_control_active_attempt_id(c));
        CHECK(++turns < 100000);
    }while (rc == EAGAIN);
    CHECK(!rc && receipt.ready);
    CHECK(!wl_columnar_relation_scalar_retirement_cancel(&receipt));
    stamps_equal(r, saved);
    CHECK(reserved(g) == retained && ledger_bytes(&ledger) == charged);
    CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt, r));
    do{
        rc = wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
                wl_evaluation_control_active_attempt_id(c));
    }while (rc == EAGAIN);
    CHECK(!rc && !wl_columnar_relation_scalar_retirement_commit(&receipt));
    CHECK(wl_columnar_relation_scalar_retirement_cancel(&receipt) == EINVAL);
    bool freed_ts = false, stopped_after_free = false; turns = 0;
    do{
        if (described && receipt.phase == 12)
            staged_ledger(&receipt, c, &ledger, g);
        bool timestamp_stage = receipt.phase == 14;
        restart(c, (receipt.storage.relation && receipt.phase != 12)
            || turns%2 ? 1 : 4096);
        rc = wl_columnar_relation_scalar_retirement_step(&receipt, c,
                wl_evaluation_control_active_attempt_id(c));
        CHECK(rc == 0 || rc == EAGAIN ||
            rc == WL_EVALUATION_CONTROL_WORK_LIMIT);
        if (timestamp_stage) {
            CHECK(receipt.storage.relation && !r->timestamps &&
                !r->timestamp_capacity && receipt.phase == 12);
            CHECK(reserved(g) == retained && ledger_bytes(&ledger) == charged);
            freed_ts = true; restart(c, 0);
            wl_evaluation_control_request_cancel(c);
            CHECK(wl_columnar_relation_scalar_retirement_step(&receipt, c,
                wl_evaluation_control_active_attempt_id(c)) ==
                WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(receipt.phase == 12 &&
                receipt.ledger_timestamp_bytes == ts_charge
                && r->ledger_ts_bytes == ts_charge &&
                ledger_bytes(&ledger) == charged && reserved(g) == retained);
            stopped_after_free = true;
        }
        CHECK(++turns < 10000);
    }while (rc);
    CHECK(freed_ts && stopped_after_free && !reserved(g) &&
        ledger_bytes(&ledger) == 37);
    wl_mem_ledger_snapshot_t final; wl_mem_ledger_snapshot(&ledger, &final);
    CHECK(!final.subsys_bytes[WL_MEM_SUBSYS_RELATION]
        && final.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP] == 37);
    free(other_owner); wl_mem_ledger_free(&ledger, WL_MEM_SUBSYS_TIMESTAMP, 37);
    CHECK(!ledger_bytes(&ledger));
    CHECK(!wl_columnar_relation_scalar_retirement_step(&receipt, c,
        wl_evaluation_control_active_attempt_id(c)));
    (void)wl_evaluation_control_finish(c, &owner, 0, 0);
    CHECK(!wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
    if (g)wl_columnar_memory_governor_ref_release(g);
}

/* An accepted raw set projection may free stamps before ledger reconcile. */
static void deferred_projection(void)
{
    for (unsigned governed = 0; governed < 2; ++governed) {
        wl_columnar_memory_resolution_t resolution =
        {.budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
         .usable_bytes =
             WL_COLUMNAR_MEMORY_MIN_BUDGET,
         .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
        wl_columnar_memory_governor_ref_t *g =
            governed?wl_columnar_memory_governor_ref_create(&resolution):NULL;
        CHECK(!governed || g); col_rel_t *r = NULL;
        if (g)CHECK(!wl_columnar_relation_new_auto_governed(
                    "deferred projection", 1, 4, true, g, &r));
        else {
            r = col_rel_new_auto("deferred projection", 1);
            CHECK(r && !col_rel_enable_timestamps(r));
        }
        int64_t rows[] = {1, 2};
        CHECK(!col_rel_append_rows_atomic(r, rows, 2, 1, NULL));
        wl_mem_ledger_t ledger; wl_mem_ledger_init(&ledger, 0);
        r->mem_ledger = &ledger;
        col_rel_ledger_reconcile(r, 0); CHECK(r->ledger_ts_bytes);
        void *other = malloc(37); CHECK(other);
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_TIMESTAMP, 37);
        int fast = 0;
        CHECK(!col_op_consolidate_incremental_delta(r, 0, NULL, &fast));
        CHECK(!r->timestamps && !r->timestamp_capacity);
        if (!governed)CHECK(r->ledger_ts_bytes > 0);
        uint64_t outstanding = r->ledger_ts_bytes;
        wl_evaluation_control_t *c = NULL;
        CHECK(!wl_evaluation_control_create(0, &c));
        CHECK(!wl_evaluation_control_attach(c, &owner)); restart(c, 0);
        wl_columnar_relation_scalar_retirement_t receipt = {0};
        CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt, r));
        int rc; do {
            rc = wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
                    wl_evaluation_control_active_attempt_id(c));
        }while (rc == EAGAIN);
        CHECK(!rc && receipt.ledger_timestamp_bytes == outstanding);
        CHECK(!wl_columnar_relation_scalar_retirement_commit(&receipt));
        unsigned turns = 0;
        do{
            rc = wl_columnar_relation_scalar_retirement_step(&receipt, c,
                    wl_evaluation_control_active_attempt_id(c));
            CHECK(++turns < 10000);
        }while (rc == EAGAIN);
        CHECK(!rc && !reserved(g) && ledger_bytes(&ledger) == 37);
        wl_mem_ledger_snapshot_t final; wl_mem_ledger_snapshot(&ledger, &final);
        CHECK(!final.subsys_bytes[WL_MEM_SUBSYS_RELATION] &&
            final.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP] == 37);
        free(other); wl_mem_ledger_free(&ledger, WL_MEM_SUBSYS_TIMESTAMP, 37);
        CHECK(!ledger_bytes(&ledger));
        (void)wl_evaluation_control_finish(c, &owner, 0, 0);
        CHECK(!wl_evaluation_control_detach(c, &owner));
        wl_evaluation_control_release(c);
        if (g) wl_columnar_memory_governor_ref_release(g);
    }
}
static void malformed(void)
{
    for (unsigned shape = 0; shape < 8; ++shape) {
        wl_columnar_memory_resolution_t resolution =
        {.budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
         .usable_bytes =
             WL_COLUMNAR_MEMORY_MIN_BUDGET,
         .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
        wl_columnar_memory_governor_ref_t *g =
            wl_columnar_memory_governor_ref_create(&resolution); CHECK(g);
        col_rel_t *r = NULL;
        CHECK(!wl_columnar_relation_new_auto_governed("bad TS", 1, 4, true, g,
            &r));
        r->nrows = 3; col_delta_timestamp_t *allocation = r->timestamps;
        uint32_t capacity = r->timestamp_capacity;
        uint64_t bytes = r->retained_reserved_bytes;
        uint64_t reserved_before = reserved(g);
        if (shape == 0)r->timestamp_capacity = 0;
        if (shape == 1)r->timestamp_capacity = 2;
        if (shape == 2)r->timestamps = NULL;
        if (shape == 3)r->timestamps = (col_delta_timestamp_t *)r->columns[0];
        if (shape ==
            4) r->timestamps =
                (col_delta_timestamp_t *)r->schema.children[0]->format;
        if (shape == 5)r->timestamps = (col_delta_timestamp_t *)r;
        if (shape == 6) {
            --r->retained_reserved_bytes; --r->retained_reservation.bytes;
        }
        if (shape ==
            7) r->ledger_ts_bytes = (uint64_t)capacity*sizeof(*allocation)+1;
        wl_evaluation_control_t *c = NULL;
        CHECK(!wl_evaluation_control_create(0, &c));
        CHECK(!wl_evaluation_control_attach(c, &owner)); restart(c, 0);
        wl_columnar_relation_scalar_retirement_t receipt = {0};
        CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt, r));
        int rc; do {
            rc = wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
                    wl_evaluation_control_active_attempt_id(c));
        }while (rc == EAGAIN);
        CHECK(rc == EINVAL && !receipt.ready && !receipt.committed &&
            reserved(g) == reserved_before);
        CHECK(!wl_columnar_relation_scalar_retirement_cancel(&receipt));
        r->timestamps = allocation; r->timestamp_capacity = capacity;
        r->retained_reserved_bytes = bytes;
        r->retained_reservation.bytes = bytes; r->ledger_ts_bytes = 0;
        CHECK(!col_rel_destroy_checked(r) && !reserved(g));
        (void)wl_evaluation_control_finish(c, &owner, 0, 0);
        CHECK(!wl_evaluation_control_detach(c, &owner));
        wl_evaluation_control_release(c);
        wl_columnar_memory_governor_ref_release(g);
    }
}
/* Actual caller owns each relation/receipt. This progress object confers no
 * ownership/READY by itself; the parent freezes its target before BOOT. */
typedef wl_columnar_relation_scalar_retirement_initializer_t initializer_t;
typedef wl_columnar_relation_scalar_retirement_t scalar_t;
static const void *init_parent;
static scalar_t *init_scalar;
static col_rel_t *init_relation;
static unsigned init_hooks[5];
static bool init_cancel_after;
static unsigned init_after_stops;

static void
init_after_step(initializer_t *i, wl_evaluation_control_t *c)
{
    CHECK(i->identity == i && i->busy);
    if (!init_cancel_after) return;
    init_cancel_after = false;
    ++init_after_stops;
    wl_evaluation_control_request_cancel(c);
}

static wl_evaluation_control_t *init_control(uint64_t limit)
{
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c));
    CHECK(!wl_evaluation_control_attach(c, &owner)); restart(c, limit);
    return c;
}
static uint64_t init_control_close(wl_evaluation_control_t *c, int rc)
{
    CHECK(wl_evaluation_control_finish(c, &owner, rc > 0 ? rc : 0, 0) == rc);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(c, &report));
    CHECK(!wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
    return report.consumed;
}
static int init_turn(initializer_t *i, const void *parent, scalar_t *s,
    col_rel_t *r, unsigned api, uint64_t limit, uint64_t *consumed)
{
    wl_evaluation_control_t *c = init_control(limit);
    uint64_t id = wl_evaluation_control_active_attempt_id(c); bool done = false;
    int rc = api == 1 ? wl_columnar_relation_scalar_retirement_init_begin(i,
            parent, s, r, c, id)
        : api == 2 ? wl_columnar_relation_scalar_retirement_init_step(i, parent,
            s, r, c, id, &done)
        : api == 3 ? wl_columnar_relation_scalar_retirement_init_reset(i,
            parent, c, id)
        : wl_columnar_relation_scalar_retirement_release_claims_controlled(s, c,
            id);
    uint64_t work = init_control_close(c, rc);
    if (consumed) *consumed = work;
    return rc;
}
static void init_reentry(initializer_t *i, wl_evaluation_control_t *c,
    unsigned api)
{
    CHECK(api >= 1 && api <= 4 && i->busy); ++init_hooks[api];
    initializer_t before = *i; bool done = true;
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    CHECK(wl_columnar_relation_scalar_retirement_init_begin(i, init_parent,
        init_scalar, init_relation, c, id) == EBUSY);
    CHECK(wl_columnar_relation_scalar_retirement_init_step(i, init_parent,
        init_scalar, init_relation, c, id, &done) == EBUSY && done);
    CHECK(wl_columnar_relation_scalar_retirement_init_reset(i, init_parent, c,
        id) == EBUSY);
    CHECK(i->identity == before.identity && i->parent == before.parent
        && i->retirement == before.retirement && i->relation == before.relation
        && i->phase == before.phase && i->busy);
}
static void init_fields_equal(const scalar_t *a, const scalar_t *b)
{
    CHECK(a->storage.identity == (uintptr_t)&a->storage
        && b->storage.identity == (uintptr_t)&b->storage);
    CHECK(a->storage.relation == b->storage.relation
        && a->storage.original_relation_address ==
        b->storage.original_relation_address
        && a->storage.phase == b->storage.phase &&
        !a->storage.gate_round.identity
        && !a->storage.governor_ref && !a->storage.governor);
    CHECK(!a->ledger && !a->ledger_bytes && !a->ledger_timestamp_bytes &&
        !a->timestamp_bytes
        && !a->owned_columns && !a->column && !a->prior_column && !a->phase &&
        !a->ready && !a->committed);
    for (unsigned n = 0; n < 4; ++n) {
        CHECK(a->storage.tokens[n].identity == &a->storage.tokens[n]
            && b->storage.tokens[n].identity == &b->storage.tokens[n]);
        CHECK(!a->storage.tokens[n].bytes && !a->storage.tokens[n].governor
            && !a->storage.token_bytes[n] && !a->storage.token_owner_bits[n]
            && !a->storage.receipt_started[n] && !a->storage.token_empty[n]);
        CHECK(atomic_load_explicit(&a->storage.tokens[n].state,
            memory_order_relaxed)
            == atomic_load_explicit(&b->storage.tokens[n].state,
            memory_order_relaxed));
        CHECK(!atomic_load_explicit(&a->storage.tokens[n].owner_bits,
            memory_order_relaxed));
    }
}

typedef struct { initializer_t *i; scalar_t *s; col_rel_t *r;
                 const void *parent; wl_evaluation_control_t *c;
} init_foreign_t;
static void *init_foreign(void *arg)
{
    init_foreign_t *f = arg; bool done = true;
    uint64_t id = wl_evaluation_control_active_attempt_id(f->c);
    CHECK(wl_columnar_relation_scalar_retirement_init_step(f->i, f->parent,
        f->s, f->r, f->c, id, &done) == EINVAL && done);
    CHECK(wl_columnar_relation_scalar_retirement_init_reset(f->i, f->parent,
        f->c, id) == EINVAL);
    return NULL;
}

static void bounded_initializer(void)
{
    for (unsigned governed = 0; governed < 2; ++governed) {
        wl_columnar_memory_resolution_t resolution =
        {.budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
         .usable_bytes =
             WL_COLUMNAR_MEMORY_MIN_BUDGET,
         .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
        wl_columnar_memory_governor_ref_t *g =
            governed ? wl_columnar_memory_governor_ref_create(&resolution) :
            NULL;
        CHECK(!governed || g);
        col_rel_t *r = NULL;
        if (g) CHECK(!wl_columnar_relation_new_auto_governed("bounded init", 3,
                4, false, g, &r));
        else r = col_rel_new_auto("bounded init", 3);
        CHECK(r); int64_t row[] = {7, 8, 9}; CHECK(!col_rel_append_row(r, row));
        wl_mem_ledger_t ledger; wl_mem_ledger_init(&ledger, 0);
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_TEMPORARY, 37);
        r->mem_ledger = &ledger;
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION,
            col_rel_owned_ledger_bytes(r));
        uint64_t baseline_reserved = reserved(g),
            baseline_ledger = ledger_bytes(&ledger);
        col_relation_snapshot_t snapshot = wl_columnar_relation_snapshot(r);
        initializer_t i = {0}; scalar_t s, reference = {0}; int parent = 0;
        memset(&s, 0xa5, sizeof(s)); /* Inert historic bytes; never inspected before ZERO. */
        init_parent = &parent; init_scalar = &s; init_relation = r;
        memset(init_hooks, 0, sizeof(init_hooks));
        wl_columnar_relation_scalar_retirement_init_test_checkpoint =
            init_reentry;
        init_after_stops = 0;
        wl_columnar_relation_scalar_retirement_init_test_after_step =
            init_after_step;
        uint64_t consumed;
        CHECK(init_turn(&i, &parent, &s, r, 1, 127,
            &consumed) == WL_EVALUATION_CONTROL_WORK_LIMIT
            && !consumed && !i.identity);
        CHECK(!init_turn(&i, &parent, &s, r, 1, 128,
            &consumed) && consumed == 128
            && i.phase == WL_COLUMNAR_RELATION_SCALAR_INIT_ZERO);
        initializer_t copied = i;
        CHECK(init_turn(&copied, &parent, &s, r, 2, 384,
            &consumed) == EINVAL && !consumed);
        wl_evaluation_control_t *foreign_fund = init_control(384);
        init_foreign_t foreign = {&i, &s, r, &parent, foreign_fund};
        wl_thread_t thread;
        CHECK(!wl_thread_create(&thread, init_foreign,
            &foreign) && !wl_thread_join(&thread));
        CHECK(!init_control_close(foreign_fund, 0));
        col_rel_t *other = col_rel_new_auto("wrong target", 1); CHECK(other);
        CHECK(init_turn(&i, &parent, &s, other, 2, 384,
            &consumed) == EINVAL && !consumed);
        col_rel_destroy(other);
        CHECK(init_turn(&i, &parent, &s, r, 2, 255,
            &consumed) == WL_EVALUATION_CONTROL_WORK_LIMIT
            && !consumed && i.phase == WL_COLUMNAR_RELATION_SCALAR_INIT_ZERO);
        for (size_t offset = 0; offset < sizeof(s);) {
            initializer_t unchanged = i;
            scalar_t untouched = s;
            CHECK(init_turn(&i, &parent, &s, r, 2, 255, &consumed)
                == WL_EVALUATION_CONTROL_WORK_LIMIT && !consumed
                && !memcmp(&i, &unchanged, sizeof(i))
                && !memcmp(&s, &untouched, sizeof(s)));
            size_t count = sizeof(s)-offset;
            if (count > 64) count = 64;
            bool stop = !offset || offset == 7*64 || offset+count == sizeof(s);
            init_cancel_after = stop;
            CHECK(init_turn(&i, &parent, &s, r, 2, 256, &consumed)
                == (stop ? WL_EVALUATION_CONTROL_CANCELLED : 0)
                && !init_cancel_after
                && consumed == 256 && i.zero_offset == offset+count);
            offset += count;
            const unsigned char *bytes = (const unsigned char *)&s;
            for (size_t byte = 0; byte < sizeof(s); ++byte)
                CHECK(bytes[byte] == (byte < offset ? 0 : 0xa5));
        }
        CHECK(i.phase == WL_COLUMNAR_RELATION_SCALAR_INIT_TOKENS
            && !s.storage.identity);
        for (unsigned token = 0; token < 4; ++token) {
            CHECK(init_turn(&i, &parent, &s, r, 2, 191, &consumed)
                == WL_EVALUATION_CONTROL_WORK_LIMIT && !consumed
                && i.token_index == token && !s.storage.identity);
            init_cancel_after = true;
            CHECK(init_turn(&i, &parent, &s, r, 2, 192, &consumed)
                == WL_EVALUATION_CONTROL_CANCELLED && !init_cancel_after
                && consumed == 192 && i.token_index == token+1
                && !s.storage.identity);
        }
        CHECK(i.phase == WL_COLUMNAR_RELATION_SCALAR_INIT_BIND);
        CHECK(init_turn(&i, &parent, &s, r, 2, 191,
            &consumed) == WL_EVALUATION_CONTROL_WORK_LIMIT
            && !consumed && !s.storage.identity);
        init_cancel_after = true;
        CHECK(init_turn(&i, &parent, &s, r, 2, 192,
            &consumed) == WL_EVALUATION_CONTROL_CANCELLED
            && !init_cancel_after && consumed == 192
            && i.phase == WL_COLUMNAR_RELATION_SCALAR_INIT_DONE
            && init_after_stops == 8);
        bool parent_begun = true; /* Actual handoff survives reset refusal. */
        CHECK(init_turn(&i, &parent, &s, r, 3, 127,
            &consumed) == WL_EVALUATION_CONTROL_WORK_LIMIT
            && !consumed && parent_begun &&
            s.storage.identity == (uintptr_t)&s.storage);
        CHECK(!init_turn(&i, &parent, &s, r, 3, 128,
            &consumed) && consumed == 128 && !i.identity);
        CHECK(!wl_columnar_relation_scalar_retirement_begin(&reference, r));
        init_fields_equal(&s, &reference);
        CHECK(!wl_columnar_relation_scalar_retirement_cancel(&reference)); /* Raw comparison fixture only. */
        CHECK(wl_columnar_relation_snapshot_equal(snapshot,
            wl_columnar_relation_snapshot(r))
            && reserved(g) == baseline_reserved &&
            ledger_bytes(&ledger) == baseline_ledger);
        for (unsigned api = 1; api <= 4; ++api) CHECK(init_hooks[api]);
        wl_columnar_relation_scalar_retirement_init_test_checkpoint = NULL;
        wl_columnar_relation_scalar_retirement_init_test_after_step = NULL;
        wl_evaluation_control_t *c = init_control(0);
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        int rc; do {
            rc = wl_columnar_relation_scalar_retirement_prepare(&s, c, id);
        } while (rc == EAGAIN);
        CHECK(!rc && s.ready && !s.committed);
        CHECK(init_turn(&i, &parent, &s, r, 4, 255,
            &consumed) == WL_EVALUATION_CONTROL_WORK_LIMIT
            && !consumed && s.ready && s.storage.gate_round.identity);
        CHECK(!init_turn(&i, &parent, &s, r, 4, 256,
            &consumed) && consumed == 256
            && !s.ready && !s.storage.gate_round.identity);
        CHECK(wl_columnar_relation_scalar_retirement_commit(&s) == EINVAL);
        CHECK(wl_columnar_relation_snapshot_equal(snapshot,
            wl_columnar_relation_snapshot(r))
            && reserved(g) == baseline_reserved &&
            ledger_bytes(&ledger) == baseline_ledger);
        CHECK(!wl_evaluation_control_charge_attempt(c, id, 256));
        memset(&s, 0, sizeof(s));
        /* A fresh actual initialization of the same exclusively owned target. */
        CHECK(!init_turn(&i, &parent, &s, r, 1, 128, NULL));
        for (unsigned turn = 0;
            i.phase != WL_COLUMNAR_RELATION_SCALAR_INIT_DONE; ++turn) {
            CHECK(turn < 1000 && !init_turn(&i, &parent, &s, r, 2,
                i.phase == WL_COLUMNAR_RELATION_SCALAR_INIT_ZERO ? 256 : 192,
                NULL));
        }
        CHECK(!init_turn(&i, &parent, &s, r, 3, 128, NULL));
        do {
            rc = wl_columnar_relation_scalar_retirement_prepare(&s, c, id);
        } while (rc == EAGAIN);
        CHECK(!rc && !wl_columnar_relation_scalar_retirement_commit(&s));
        do {
            rc = wl_columnar_relation_scalar_retirement_step(&s, c, id);
        } while (rc == EAGAIN);
        CHECK(!rc && !s.storage.relation && reserved(g) == 0 &&
            ledger_bytes(&ledger) == 37);
        init_control_close(c, 0); wl_columnar_memory_governor_ref_release(g);
    }
}

int main(void)
{
    bounded_initializer();
    const uint32_t widths[] = {0, 1, 3, 257};
    for (unsigned w = 0; w < 4; ++w)for (unsigned governed = 0; governed < 2;
            ++governed)
            for (unsigned ledger = 0; ledger < 3;
                ++ledger) retirement(widths[w], governed, ledger, false);
    for (unsigned governed = 0; governed < 2; ++governed)
        for (unsigned ledger = 0; ledger < 3; ++ledger)
            retirement(3, governed, ledger, true);
    deferred_projection(); malformed();
    puts(
        "physical retirement: reversible ownership, timestamp disposal and exact ledger credit passed");
    return 0;
}
