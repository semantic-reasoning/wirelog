#include "../wirelog/columnar/radix_workspace_attempt.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__,#x); abort(); } } while (0)
wl_columnar_consolidate_alloc_hook_t wl_columnar_consolidate_alloc_hook;
void wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *token,
    unsigned phase) {
    (void)token; (void)phase;
}
static unsigned original_owner, cleanup_owner;
static unsigned mode;
static bool advisory;
static wl_evaluation_control_t *original, *cleanup;
static uint64_t original_id;
static unsigned calls, fail_at;
static const char *sites[16];
static bool allocation(const char *site) {
    CHECK(calls < 16); sites[calls++] = site;
    return fail_at && calls == fail_at;
}
static wl_evaluation_control_t *control(unsigned *owner, uint64_t limit) {
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c));
    CHECK(!wl_evaluation_control_attach(c, owner));
    CHECK(!wl_evaluation_control_begin(c, owner)); return c;
}
static void close_control(wl_evaluation_control_t *c, unsigned *owner, int rc) {
    CHECK(wl_evaluation_control_finish(c, owner, rc > 0?rc:0, 0) == rc);
    CHECK(!wl_evaluation_control_detach(c, owner));
    wl_evaluation_control_release(c);
}
static wl_columnar_memory_governor_ref_t *governor(void) {
    wl_columnar_memory_resolution_t r =
    {.mode =
         advisory?WL_COLUMNAR_MEMORY_MODE_ADVISORY:
         WL_COLUMNAR_MEMORY_MODE_ENFORCING,
     .budget_bytes = UINT64_C(512)<<20,
            .usable_bytes = UINT64_C(512)<<20};
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&r); CHECK(g); return g;
}
typedef struct {
    wl_columnar_relation_mutation_set_t set;
    wl_columnar_relation_mutation_role_t roles[9];
    wl_columnar_relation_mutation_descriptor_t descriptors[9];
    wl_columnar_relation_mutation_owner_t owners[18];
    wl_columnar_relation_mutation_lease_t leases[9];
    wl_columnar_relation_mutation_initialization_t initializations[9];
} fixture_t;
static void acquire(fixture_t *f, col_rel_t *r, unsigned n) {
    for (unsigned i = 0; i < n;
        i++) f->roles[i] = (wl_columnar_relation_mutation_role_t){r,
                                                                  WL_COLUMNAR_RELATION_PAYLOAD_MUTATION};
    CHECK(!col_rel_mutation_set_acquire(&f->set, f->roles, n, f->descriptors, 9,
        f->owners, 18, f->leases, 9, f->initializations, 9));
}
static col_rel_t *relation(wl_columnar_memory_governor_ref_t *g, uint32_t rows,
    unsigned type, bool ts) {
    col_rel_t *r = NULL;
    unsigned width = type == 3?3:type == 4?0:type == 5?17:1;
    if (g)CHECK(!wl_columnar_relation_new_auto_governed("radix", width,
            rows?rows:1, ts, g, &r));
    else {
        r = col_rel_new_auto("radix", width); CHECK(r);
        if (rows > r->capacity)CHECK(!col_rel_reserve_capacity_admitted(r, rows,
                NULL));
        if (ts)CHECK(!col_rel_enable_timestamps(r));
    }
    wirelog_column_type_t types[17];
    for (unsigned c = 0; c < 17;
        c++) types[c] = c%3 == 0?WIRELOG_TYPE_INT64:c%3 ==
            1?WIRELOG_TYPE_STRING:WIRELOG_TYPE_FLOAT;
    if (type < 3)types[0] = type == 0?WIRELOG_TYPE_INT64:type ==
            1?WIRELOG_TYPE_STRING:WIRELOG_TYPE_FLOAT;
    if (r->ncols) CHECK(!col_rel_set_column_types(r, types, r->ncols));
    for (uint32_t row = 0; row < rows; row++)for (uint32_t c = 0; c < r->ncols;
            c++){
            if (types[c] ==
                WIRELOG_TYPE_FLOAT) r->columns[c][row] =
                    wl_columnar_float_to_bits((double)(rows-row)+0.5);
            else r->columns[c][row] = (int64_t)(rows-row);
        }
    r->nrows = rows;
    if (ts)for (uint32_t row = 0; row < rows;
            row++) r->timestamps[row] =
                (col_delta_timestamp_t){.iteration = row+9, .multiplicity = 1};
    return r;
}
static int prepare(wl_columnar_relation_radix_workspace_attempt_t *a) {
    bool ready = false; for (unsigned i = 0; i < 100000 && !ready; i++) {
        int rc = wl_columnar_relation_radix_workspace_attempt_prepare_step(a,
                &ready); if (rc) return rc;
    }
    CHECK(ready); return 0;
}
static void end(wl_columnar_relation_radix_workspace_attempt_t *a) {
    bool done = false;
    for (unsigned i = 0; i < 100000 && !done;
        i++) CHECK(!wl_columnar_relation_radix_workspace_attempt_end_step(a,
            cleanup, wl_evaluation_control_active_attempt_id(cleanup), &done));
    CHECK(done);
}
static wl_columnar_relation_radix_workspace_attempt_t *create(void) {
    original = control(&original_owner, mode == 6?0:UINT64_MAX);
    cleanup = control(&cleanup_owner, mode == 6?0:UINT64_MAX);
    original_id = wl_evaluation_control_active_attempt_id(original);
    void *a = malloc(wl_columnar_relation_radix_workspace_attempt_size());
    CHECK(a);
    CHECK(!wl_columnar_relation_radix_workspace_attempt_init(a, original,
        original_id)); return a;
}
static void release(wl_columnar_relation_radix_workspace_attempt_t *a, int rc) {
    end(a); free(a); close_control(original, &original_owner, rc);
    close_control(cleanup, &cleanup_owner, 0);
}
static void equal(const wl_columnar_radix_workspace_t *a,
    const wl_columnar_radix_workspace_t *b) {
#define PTR(p) CHECK((a->p != NULL) == (b->p != NULL))
    PTR(perm_a); PTR(perm_b); PTR(temp_column); PTR(bucket_values);
    PTR(count16); PTR(insertion_rows); PTR(timestamps);
#undef PTR
#define SAME(p) CHECK(a->p == b->p)
    SAME(timestamp_capacity); SAME(k8_capacity); SAME(k16_capacity);
    SAME(insertion_capacity); SAME(insertion_bytes);
    SAME(prepared_relation); SAME(prepared_identity);
    SAME(prepared_view_generation); SAME(prepared_storage_generation);
    SAME(prepared_type_fingerprint); SAME(prepared_start); SAME(prepared_count);
    SAME(prepared_nrows); SAME(prepared_ncols);
    SAME(prepared_capacity); SAME(prepared_timestamp_capacity);
    SAME(prepared_has_timestamps); SAME(prepared_has_types);
    SAME(admission.bytes); SAME(admission_active);
#undef SAME
}
static void raw_case_mode(uint32_t rows, unsigned type, bool ts,
    unsigned members,
    unsigned selected, unsigned fail, bool consume, bool unmanaged) {
    wl_columnar_memory_governor_ref_t *g = unmanaged?NULL:governor();
    col_rel_t *r = relation(g, rows, type, ts);
    fixture_t f = {0}; acquire(&f, r, members);
    wl_columnar_radix_workspace_t raw = {0};
    wl_columnar_consolidate_alloc_hook = allocation; calls = 0; fail_at = fail;
    int rawrc = wl_columnar_relation_radix_workspace_prepare_with_lease(r, 0,
            rows, &raw, &f.leases[selected]);
    unsigned rawcalls = calls; const char *rawsites[16]; for (unsigned i = 0;
        i < calls; i++) rawsites[i] = sites[i];
    wl_columnar_relation_radix_workspace_attempt_t *a = create(); calls = 0;
    fail_at = fail;
    CHECK(!wl_columnar_relation_radix_workspace_attempt_begin(a, r, 0, rows,
        &f.leases[selected]));
    int rc = prepare(a); CHECK(rc == rawrc && calls == rawcalls);
    if (type == 4 && rows > 1 && rows <= 32 && fail) {
        CHECK(rawrc == ENOMEM && rawcalls == fail);
        CHECK(!strcmp(rawsites[fail-1],
            ts &&
            fail ==
            1?"radix_workspace_timestamps":"radix_workspace_insertion_rows"));
    }
    for (unsigned i = 0; i < calls; i++)CHECK(!strcmp(sites[i], rawsites[i]));
    wl_columnar_consolidate_alloc_hook = NULL; fail_at = 0;
    if (!rc) {
        if (advisory) {
            uint64_t status[12];
            CHECK(!wl_columnar_relation_radix_workspace_attempt_test_view(a,
                status));
            CHECK(status[3] == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY);
        }
        const wl_columnar_radix_workspace_t *nonempty = &raw;
        CHECK(wl_columnar_relation_radix_workspace_attempt_borrow(a,
            &nonempty) == EINVAL && nonempty == &raw);
        const wl_columnar_radix_workspace_t *w = NULL;
        CHECK(!wl_columnar_relation_radix_workspace_attempt_borrow(a, &w));
        equal(w, &raw);
        if (type == 4 && rows > 1 && rows <= 32) {
            CHECK(w->insertion_rows && w->insertion_capacity == rows &&
                !w->insertion_bytes);
            CHECK(!strcmp(rawsites[rawcalls-1],
                "radix_workspace_insertion_rows"));
            CHECK(rawcalls == (ts?2u:1u));
            if (!ts) {
                CHECK(!w->admission_active && !w->admission.bytes);
            }
        }
        CHECK(wl_columnar_relation_radix_workspace_attempt_borrow(a,
            &w) == EINVAL);
        const wl_columnar_radix_workspace_t *again = NULL;
        CHECK(wl_columnar_relation_radix_workspace_attempt_borrow(a,
            &again) == EINVAL && !again);
        if (consume)CHECK(!wl_columnar_relation_radix_sort_with_lease(r, 0,
                rows,
                w, &f.leases[selected]));
    }
    /* End intentionally follows real consumer generation changes. */
    release(a, rc); wl_columnar_radix_workspace_destroy(&raw);
    CHECK(!col_rel_mutation_set_finish(&f.set, false));
    CHECK(!col_rel_destroy_checked(r));
    if (g) {
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)));
        wl_columnar_memory_governor_ref_release(g);
    }
}
static void raw_case(uint32_t rows, unsigned type, bool ts, unsigned members,
    unsigned selected, unsigned fail, bool consume) {
    raw_case_mode(rows, type, ts, members, selected, fail, consume, false);
}
static void nullary_insertions(void) {
    const uint32_t counts[] = {0, 1, 2, 32, 33};
    unsigned cases = 0;
    for (unsigned unmanaged = 0; unmanaged < 2; unmanaged++)for (unsigned ts =
            0;
            ts < 2; ts++) for (unsigned i = 0; i < 5; i++) {
                uint32_t n = counts[i];
                unsigned allocations = n <= 1?0:n <= 32?(ts?2:1):(ts?5:4);
                for (unsigned fail = 0; fail <= allocations; fail++) {
                    raw_case_mode(n, 4, ts != 0, 1, 0, fail, false,
                        unmanaged != 0); cases++;
                }
            }
    fprintf(stderr,
        "radix workspace nullary0/1/2/32/33 tracked/untracked governed/unmanaged %u actual raw allocator/ENOMEM/End cases: PASS\n",
        cases);
}
typedef struct { unsigned hook;
                 uint64_t phase, index, lane, clear, state, receipt; } cut_t;
static cut_t cuts[256], selected;
static size_t cut_count;
static bool census, injected, original_ended, cleanup_ended, successor;
static unsigned gap_hits;
static wl_evaluation_control_report_t frozen;
static unsigned char *before_image;
static uint64_t before_consumed;
static bool same_cut(const cut_t *a, const cut_t *b) {
    return a->hook == b->hook && a->phase == b->phase && a->index == b->index &&
           a->lane == b->lane && a->clear == b->clear && a->state == b->state &&
           a->receipt == b->receipt;
}
static bool same_actor(const void *a) {
    size_t n = wl_columnar_relation_radix_workspace_attempt_size();
    unsigned char *now = malloc(n); CHECK(now); memcpy(now, a, n);
    bool same = memcmp(before_image, now, n) == 0; free(now); return same;
}
static bool same_report(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b) {
    return a->attempt_id == b->attempt_id && a->limit == b->limit &&
           a->consumed == b->consumed &&
           a->stop_reason == b->stop_reason &&
           a->execution_status == b->execution_status &&
           a->cleanup_status == b->cleanup_status && a->result == b->result;
}
static void cut_hook(const wl_columnar_relation_radix_workspace_attempt_t *a,
    unsigned hook) {
    if (hook == 2101 || hook == 2102) return; /* Real zero, not256. */
    uint64_t v[12];
    CHECK(!wl_columnar_relation_radix_workspace_attempt_test_view(a, v));
    cut_t key = {.hook = hook, .phase = v[0], .index = v[8], .lane = v[9],
                 .clear = v[10], .state = v[4], .receipt = v[6]};
    if (census) {
        for (size_t i = 0; i < cut_count;
            i++) if (same_cut(&cuts[i], &key)) return;
        CHECK(cut_count < 256); cuts[cut_count++] = key; return;
    }
    if (injected || !same_cut(&key, &selected))return;
    injected = true;
    if (hook < 2100 && v[4] == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED &&
        !v[5]) gap_hits++;
    wl_evaluation_control_t *c = hook >= 2200?cleanup:original;
    unsigned *owner = hook >= 2200?&cleanup_owner:&original_owner;
    uint64_t id = wl_evaluation_control_active_attempt_id(c), used;
    CHECK(!wl_evaluation_control_test_active_consumed(c, id, &used));
    memcpy(before_image, a,
        wl_columnar_relation_radix_workspace_attempt_size());
    if (mode == 1)wl_evaluation_control_request_cancel(c);
    else if (mode == 4 || mode == 5) {
        CHECK(!wl_evaluation_control_finish(c, owner, 0, 0));
        CHECK(!wl_evaluation_control_report(c, &frozen));
        if (hook >= 2200)cleanup_ended = true; else original_ended = true;
        if (mode == 5) {
            CHECK(!wl_evaluation_control_reset(c));
            CHECK(!wl_evaluation_control_begin(c, owner)); successor = true;
        }
    } else {
        uint64_t left = mode == 2?255:mode == 3?256:0;
        CHECK(!wl_evaluation_control_charge_attempt(c, id,
            UINT64_MAX-used-left));
    }
}
static void resume_cleanup(void) {
    if (cleanup_ended) {
        wl_evaluation_control_report_t now;
        CHECK(!wl_evaluation_control_report(cleanup, &now));
        CHECK(same_report(&frozen, &now));
        if (successor) {
            uint64_t used;
            CHECK(!wl_evaluation_control_test_active_consumed(cleanup,
                wl_evaluation_control_active_attempt_id(cleanup),
                &used) && !used);
        }
    }
    if (!cleanup_ended || successor) {
        int rc = wl_evaluation_control_finish(cleanup, &cleanup_owner, 0, 0);
        CHECK(!rc || rc == WL_EVALUATION_CONTROL_CANCELLED ||
            rc == WL_EVALUATION_CONTROL_WORK_LIMIT || rc == EOVERFLOW);
    }
    CHECK(!wl_evaluation_control_reset(cleanup));
    CHECK(!wl_evaluation_control_begin(cleanup, &cleanup_owner));
}
static void fault_run(bool collect) {
    wl_columnar_memory_governor_ref_t *g = governor();
    col_rel_t *r = relation(g, 33, 0, true);
    fixture_t f = {0}; acquire(&f, r, 7);
    wl_columnar_relation_radix_workspace_attempt_t *a = create();
    before_image = malloc(wl_columnar_relation_radix_workspace_attempt_size());
    CHECK(before_image);
    census = collect;
    injected = original_ended = cleanup_ended = successor = false;
    wl_columnar_relation_radix_workspace_attempt_test_checkpoint = cut_hook;
    int rc = wl_columnar_relation_radix_workspace_attempt_begin(a, r, 0, 33,
            &f.leases[3]);
    if (!rc)rc = prepare(a);
    if (!collect && selected.hook < 2200) {
        CHECK(injected);
        if (mode != 3) {
            CHECK(rc ==
                (mode == 1?WL_EVALUATION_CONTROL_CANCELLED:mode ==
                2?WL_EVALUATION_CONTROL_WORK_LIMIT:mode == 6?EOVERFLOW:EINVAL));
            CHECK(same_actor(a));
        }
        wl_columnar_relation_radix_workspace_attempt_test_checkpoint = NULL;
    }
    CHECK(!wl_evaluation_control_test_active_consumed(original,
        wl_evaluation_control_active_attempt_id(original),
        &before_consumed) || original_ended);
    bool done = false;
    for (unsigned i = 0; i < 100000 && !done; i++) {
        int x = wl_columnar_relation_radix_workspace_attempt_end_step(a,
                cleanup, wl_evaluation_control_active_attempt_id(cleanup),
                &done);
        if (x) {
            CHECK(!collect && injected && selected.hook >= 2200);
            if (mode != 3)CHECK(same_actor(a));
            wl_columnar_relation_radix_workspace_attempt_test_checkpoint = NULL;
            resume_cleanup();
        }
    }
    CHECK(done);
    wl_columnar_relation_radix_workspace_attempt_test_checkpoint = NULL;
    if (!collect)CHECK(injected);
    if (original_ended) {
        wl_evaluation_control_report_t now;
        CHECK(!wl_evaluation_control_report(original, &now));
        CHECK(same_report(&frozen, &now));
        if (successor) {
            uint64_t used;
            CHECK(!wl_evaluation_control_test_active_consumed(original,
                wl_evaluation_control_active_attempt_id(original),
                &used) && !used);
        }
    } else {
        uint64_t used;
        CHECK(!wl_evaluation_control_test_active_consumed(original, original_id,
            &used) && used == before_consumed);
    }
    free(before_image); free(a);
    if (!original_ended || successor)close_control(original, &original_owner,
            original_ended?0:rc);
    else {
        CHECK(!wl_evaluation_control_detach(original, &original_owner));
        wl_evaluation_control_release(original);
    }
    close_control(cleanup, &cleanup_owner, 0);
    CHECK(!col_rel_mutation_set_finish(&f.set, false));
    CHECK(!col_rel_destroy_checked(r));
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
static void faults(void) {
    fault_run(true); size_t n = cut_count;
    for (size_t i = 0; i < n; i++)for (mode = 1; mode <= 6; mode++) {
            selected = cuts[i]; fault_run(false);
        }
    CHECK(gap_hits); mode = 0;
    fprintf(stderr,
        "radix workspace %zu actual cut/cursor identities x6; reserve-before-verdict END gap%u passed\n",
        n, gap_hits);
}
static void stale_and_detached(void) {
    for (unsigned at = 0; at < 3; at++) {
        wl_columnar_memory_governor_ref_t *g = governor();
        col_rel_t *r = relation(g, 33, 0, true);
        fixture_t f = {0}; acquire(&f, r, 7);
        wl_columnar_relation_radix_workspace_attempt_t *a = create();
        CHECK(!wl_columnar_relation_radix_workspace_attempt_begin(a, r, 0, 33,
            &f.leases[6]));
        if (at == 0)for (unsigned i = 0; i < 7; i++) {
                bool ready = false;
                CHECK(
                    !wl_columnar_relation_radix_workspace_attempt_prepare_step(
                        a, &ready));
            }
        else CHECK(!prepare(a));
        CHECK(!col_rel_mutation_set_finish(&f.set, false));
        if (at == 0) {
            bool ready = false;
            CHECK(wl_columnar_relation_radix_workspace_attempt_prepare_step(a,
                &ready) == EINVAL);
        }else if (at == 1) {
            const wl_columnar_radix_workspace_t *w = NULL;
            CHECK(wl_columnar_relation_radix_workspace_attempt_borrow(a,
                &w) == EINVAL && !w);
        }
        if (at == 2) {
            acquire(&f, r, 7);
            const wl_columnar_radix_workspace_t *w = NULL;
            CHECK(wl_columnar_relation_radix_workspace_attempt_borrow(a,
                &w) == EINVAL && !w);
            CHECK(!col_rel_mutation_set_finish(&f.set, false));
        }
        CHECK(!col_rel_destroy_checked(r)); /* End never reads this old source. */
        release(a, 0);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g))); wl_columnar_memory_governor_ref_release(g);
    }
}
static unsigned resource_mode, resource_hits;
static wl_columnar_memory_governor_t *resource_governor;
static void resource_hook(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase) {
    (void)token; if (c != original || resource_hits)return;
    if (resource_mode == 1 && phase == WL_COLUMNAR_MEMORY_GOVERNOR_CLAIMED) {
        uint64_t held = wl_columnar_memory_reserved(resource_governor);
        atomic_store_explicit(&resource_governor->usable_bytes, held,
            memory_order_release); resource_hits++;
    }
    if (resource_mode >= 2 &&
        phase ==
        (resource_mode ==
        5?WL_COLUMNAR_MEMORY_GOVERNOR_RESERVED:
        WL_COLUMNAR_MEMORY_GOVERNOR_ADMITTED)) {
        resource_hits++;
        if (resource_mode == 2)wl_evaluation_control_request_cancel(c);
        else {
            CHECK(!wl_evaluation_control_finish(c, &original_owner, 0, 0));
            CHECK(!wl_evaluation_control_report(c, &frozen));
            CHECK(!wl_evaluation_control_reset(c));
            CHECK(!wl_evaluation_control_begin(c, &original_owner));
            original_ended = true;
        }
    }
}
static void resources(void) {
    mode = 0;
    for (resource_mode = 1; resource_mode <= 5; resource_mode++) {
        wl_columnar_memory_governor_ref_t *g = governor();
        col_rel_t *r = relation(g, 33, 0, true);
        fixture_t f = {0}; acquire(&f, r, 1);
        wl_columnar_relation_radix_workspace_attempt_t *a = create();
        resource_governor = wl_columnar_memory_governor_ref_get(g);
        resource_hits = 0; original_ended = false;
        uint64_t floor = wl_columnar_memory_reserved(resource_governor);
        wl_columnar_memory_reservation_t peer;
        wl_columnar_memory_reservation_init(&peer);
        if (resource_mode == 4) {
            atomic_store_explicit(&resource_governor->usable_bytes, UINT64_MAX,
                memory_order_release);
            CHECK(wl_columnar_memory_reserve_checked(resource_governor,
                UINT64_MAX-floor, &peer) == WL_COLUMNAR_MEMORY_ADMISSION_OK);
            CHECK(wl_columnar_memory_reserved(resource_governor) == UINT64_MAX);
        } else wl_columnar_memory_governor_test_checkpoint = resource_hook;
        wl_columnar_consolidate_alloc_hook_t saved_allocation_hook =
            wl_columnar_consolidate_alloc_hook;
        calls = 0; fail_at = 0; wl_columnar_consolidate_alloc_hook = allocation;
        CHECK(!wl_columnar_relation_radix_workspace_attempt_begin(a, r, 0, 33,
            &f.leases[0]));
        int rc = prepare(a); wl_columnar_memory_governor_test_checkpoint = NULL;
        wl_columnar_consolidate_alloc_hook = saved_allocation_hook;
        CHECK(!calls); /* Actual allocator observer was operative above. */
        CHECK(rc ==
            (resource_mode == 1?ENOMEM:resource_mode ==
            2?WL_EVALUATION_CONTROL_CANCELLED:resource_mode ==
            3 || resource_mode == 5?EINVAL:EOVERFLOW));
        if (resource_mode != 4)CHECK(resource_hits == 1);
        uint64_t v[12];
        CHECK(!wl_columnar_relation_radix_workspace_attempt_test_view(a, v));
        if (resource_mode ==
            1) CHECK(v[3] == WL_COLUMNAR_MEMORY_ADMISSION_DENIED &&
                r->memory_budget_denial_pending && !calls);
        if (resource_mode ==
            4) CHECK(v[3] == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW);
        end(a); free(a);
        if (resource_mode == 3 || resource_mode == 5) {
            wl_evaluation_control_report_t now;
            CHECK(!wl_evaluation_control_report(original,
                &now) && same_report(&frozen, &now));
            uint64_t used;
            CHECK(!wl_evaluation_control_test_active_consumed(original,
                wl_evaluation_control_active_attempt_id(original),
                &used) && !used);
        }
        close_control(original, &original_owner,
            (resource_mode == 3 || resource_mode == 5)?0:rc);
        close_control(cleanup, &cleanup_owner, 0);
        if (resource_mode == 4)CHECK(wl_columnar_memory_rollback(&peer));
        CHECK(wl_columnar_memory_reserved(resource_governor) == floor);
        /* A genuinely new Original retries the same still-held source after
         * the true budget/credit obstruction has been removed. */
        atomic_store_explicit(&resource_governor->usable_bytes,
            UINT64_C(512)<<20, memory_order_release);
        a = create();
        CHECK(!wl_columnar_relation_radix_workspace_attempt_begin(a, r, 0, 33,
            &f.leases[0]));
        CHECK(!prepare(a));
        const wl_columnar_radix_workspace_t *retry_view = NULL;
        CHECK(!wl_columnar_relation_radix_workspace_attempt_borrow(a,
            &retry_view));
        release(a, 0);
        CHECK(wl_columnar_memory_reserved(resource_governor) == floor);
        CHECK(!col_rel_mutation_set_finish(&f.set, false));
        CHECK(!col_rel_destroy_checked(r));
        CHECK(!wl_columnar_memory_reserved(resource_governor));
        wl_columnar_memory_governor_ref_release(g);
    }
    puts(
        "radix workspace real DENIED/overflow peer, credited cancellation and END/successor: PASS");
}
static unsigned init_kind, init_hits;
static void retain_hook(wl_evaluation_control_t *c) {
    if (init_hits++)return;
    if (init_kind == 1)CHECK(!wl_evaluation_control_retain(c));
    else if (init_kind == 2)wl_evaluation_control_request_cancel(c);
    else {
        CHECK(!wl_evaluation_control_finish(c, &original_owner, 0, 0));
        CHECK(!wl_evaluation_control_report(c, &frozen));
        CHECK(!wl_evaluation_control_reset(c));
        CHECK(!wl_evaluation_control_begin(c, &original_owner));
    }
}
static void init_boundaries(void) {
    size_t size = wl_columnar_relation_radix_workspace_attempt_size(),
        actions = (size+7)/8+1;
    for (size_t i = 0; i < actions; i++) {
        wl_evaluation_control_t *c = control(&original_owner, i*256+255);
        unsigned char *a = malloc(size+8); CHECK(a); memset(a, 0xa5, size+8);
        CHECK(wl_columnar_relation_radix_workspace_attempt_init((void *)a, c,
            wl_evaluation_control_active_attempt_id(c)) ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
        uint64_t used;
        CHECK(!wl_evaluation_control_test_active_consumed(c,
            wl_evaluation_control_active_attempt_id(c),
            &used) && used == i*256);
        for (unsigned j = 0; j < 8; j++)CHECK(a[size+j] == 0xa5);
        close_control(c, &original_owner, WL_EVALUATION_CONTROL_WORK_LIMIT);
        free(a);
    }
    for (init_kind = 1; init_kind <= 3; init_kind++) {
        original = control(&original_owner, 0);
        original_id = wl_evaluation_control_active_attempt_id(original);
        void *a = malloc(size); CHECK(a); init_hits = 0;
        wl_evaluation_control_test_retain_claim = retain_hook;
        int rc = wl_columnar_relation_radix_workspace_attempt_init(a, original,
                original_id);
        wl_evaluation_control_test_retain_claim = NULL;
        CHECK(init_hits == 1 &&
            rc ==
            (init_kind == 1?EAGAIN:init_kind ==
            2?WL_EVALUATION_CONTROL_CANCELLED:EINVAL));
        if (init_kind == 1) {
            wl_evaluation_control_release(original);
            CHECK(!wl_columnar_relation_radix_workspace_attempt_init(a,
                original, original_id));
            cleanup = control(&cleanup_owner, 0); end(a);
            close_control(original, &original_owner, 0);
            close_control(cleanup, &cleanup_owner, 0);
        }else if (init_kind == 2) close_control(original, &original_owner,
                WL_EVALUATION_CONTROL_CANCELLED);
        else {
            wl_evaluation_control_report_t now;
            CHECK(!wl_evaluation_control_report(original,
                &now) && same_report(&frozen, &now));
            uint64_t used;
            CHECK(!wl_evaluation_control_test_active_consumed(original,
                wl_evaluation_control_active_attempt_id(original),
                &used) && !used);
            close_control(original, &original_owner, 0);
        }
        free(a);
    }
    fprintf(stderr,
        "radix workspace Init %zu literal256 one-short cuts, actual retainCAS contention/cancel/END rollback passed\n",
        actions);
}
static void shared_owner(void) {
    wl_columnar_memory_governor_ref_t *g = governor();
    col_rel_t *root = relation(g, 33, 0, true), *view = NULL;
    CHECK(!wl_columnar_relation_new_like_governed_checked(&view, "view", root,
        g));
    CHECK(!col_rel_install_shared_view(view, root));
    CHECK(col_rel_storage_alias_borrow_count(root) == 1);
    fixture_t f = {0}; acquire(&f, view, 2);
    CHECK(f.leases[1].owner_slot != f.leases[1].self_owner_slot);
    wl_columnar_relation_radix_workspace_attempt_t *a = create();
    CHECK(!wl_columnar_relation_radix_workspace_attempt_begin(a, view, 0, 33,
        &f.leases[1])); CHECK(!prepare(a));
    const wl_columnar_radix_workspace_t *w = NULL;
    CHECK(!wl_columnar_relation_radix_workspace_attempt_borrow(a, &w));
    CHECK(!wl_columnar_relation_radix_sort_with_lease(view, 0, 33, w,
        &f.leases[1]));
    CHECK(view->columns[0][0] == 1 && root->columns[0][0] == 33);
    CHECK(!col_rel_mutation_set_finish(&f.set, false));
    CHECK(!col_rel_destroy_checked(view));
    CHECK(!col_rel_destroy_checked(root));
    release(a, 0);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
static void source_packet_boundaries(void) {
    /* Source vector is independently100/124/164/78. Before RESERVE: Begin1
     * plus stages1..42 and extra MEMBER/LANE/TYPES transitions3, all256.
     * Init buys one256 per8 bytes plus its real retain phase. */
    const uint64_t fees[] = {100, 124, 164, 78};
    size_t init = (wl_columnar_relation_radix_workspace_attempt_size()+7)/8+1;
    const uint64_t own_prefix = (init+1+42+3)*256;
    uint64_t prior = 0; mode = 0;
    for (unsigned p = 0; p < 4; p++) {
        for (unsigned exact = 0; exact < 2; exact++) {
            wl_columnar_memory_governor_ref_t *g = governor();
            col_rel_t *r = relation(g, 33, 0, true);
            fixture_t f = {0}; acquire(&f, r, 1);
            original = control(&original_owner,
                    own_prefix+prior+fees[p]-(exact?0:1));
            original_id = wl_evaluation_control_active_attempt_id(original);
            cleanup = control(&cleanup_owner, 0);
            void *a =
                malloc(wl_columnar_relation_radix_workspace_attempt_size());
            CHECK(a);
            CHECK(!wl_columnar_relation_radix_workspace_attempt_init(a,
                original, original_id));
            CHECK(!wl_columnar_relation_radix_workspace_attempt_begin(a, r, 0,
                33, &f.leases[0]));
            CHECK(prepare(a) == WL_EVALUATION_CONTROL_WORK_LIMIT);
            uint64_t used;
            CHECK(!wl_evaluation_control_test_active_consumed(original,
                original_id, &used));
            CHECK(used == own_prefix+prior+(exact?fees[p]:0));
            uint64_t v[12];
            CHECK(!wl_columnar_relation_radix_workspace_attempt_test_view(a,
                v));
            if (p == 3 &&
                exact) CHECK(v[4] == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED &&
                    !v[5]);
            else CHECK(v[4] == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
            release(a, WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(!col_rel_mutation_set_finish(&f.set, false));
            CHECK(!col_rel_destroy_checked(r));
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)));
            wl_columnar_memory_governor_ref_release(g);
        }
        prior += fees[p];
    }
    puts(
        "radix workspace literal source100/124/164/78 exact/one-short and actual gap credit closure: PASS");
}
static void partial_and_unmanaged(bool unmanaged) {
    wl_columnar_memory_governor_ref_t *g = unmanaged?NULL:governor();
    col_rel_t *r = relation(g, 40, 0, true); fixture_t f = {0};
    acquire(&f, r, 1);
    wl_columnar_radix_workspace_t raw = {0};
    CHECK(!wl_columnar_relation_radix_workspace_prepare_with_lease(r, 4, 33,
        &raw, &f.leases[0]));
    wl_columnar_relation_radix_workspace_attempt_t *a = create();
    CHECK(!wl_columnar_relation_radix_workspace_attempt_begin(a, r, 4, 33,
        &f.leases[0])); CHECK(!prepare(a));
    const wl_columnar_radix_workspace_t *w = NULL;
    CHECK(!wl_columnar_relation_radix_workspace_attempt_borrow(a, &w));
    equal(w, &raw);
    CHECK(!wl_columnar_relation_radix_sort_with_lease(r, 4, 33, w,
        &f.leases[0]));
    CHECK(r->columns[0][3] == 37 && r->columns[0][4] == 4 &&
        r->columns[0][36] == 36 && r->columns[0][37] == 3);
    CHECK(r->timestamps[4].iteration == 45);
    release(a, 0); wl_columnar_radix_workspace_destroy(&raw);
    CHECK(!col_rel_mutation_set_finish(&f.set, false));
    CHECK(!col_rel_destroy_checked(r));
    if (g){
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g))); wl_columnar_memory_governor_ref_release(g);
    }
}
static void changed_before_borrow(void) {
    wl_columnar_memory_governor_ref_t *g = governor();
    col_rel_t *r = relation(g, 33, 0, true);
    fixture_t f = {0}; acquire(&f, r, 1);
    wl_columnar_relation_radix_workspace_attempt_t *a = create();
    CHECK(!wl_columnar_relation_radix_workspace_attempt_begin(a, r, 0, 33,
        &f.leases[0])); CHECK(!prepare(a));
    wl_columnar_radix_workspace_t other = {0};
    CHECK(!wl_columnar_relation_radix_workspace_prepare_with_lease(r, 0, 33,
        &other, &f.leases[0]));
    CHECK(!wl_columnar_relation_radix_sort_with_lease(r, 0, 33, &other,
        &f.leases[0]));
    const wl_columnar_radix_workspace_t *w = NULL;
    CHECK(wl_columnar_relation_radix_workspace_attempt_borrow(a,
        &w) == EINVAL && !w);
    wl_columnar_radix_workspace_destroy(&other); release(a, EINVAL);
    CHECK(!col_rel_mutation_set_finish(&f.set, false));
    CHECK(!col_rel_destroy_checked(r));
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
static unsigned zero_target, zero_kind, zero_hits;
static void zero_hook(const wl_columnar_relation_radix_workspace_attempt_t *a,
    unsigned hook) {
    if (hook != zero_target || zero_hits)return; zero_hits++;
    memcpy(before_image, a,
        wl_columnar_relation_radix_workspace_attempt_size());
    if (zero_kind == 1)wl_evaluation_control_request_cancel(original);
    else if (zero_kind == 4) {
        uint64_t used;
        CHECK(!wl_evaluation_control_test_active_consumed(original, original_id,
            &used));
        CHECK(!wl_evaluation_control_charge_attempt(original, original_id,
            UINT64_MAX-used));
        CHECK(wl_evaluation_control_charge_attempt(original, original_id,
            1) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    } else {
        CHECK(!wl_evaluation_control_finish(original, &original_owner, 0, 0));
        CHECK(!wl_evaluation_control_report(original, &frozen));
        if (zero_kind == 3) {
            CHECK(!wl_evaluation_control_reset(original));
            CHECK(!wl_evaluation_control_begin(original, &original_owner));
        }
    }
}
static void final_zeros(void) {
    mode = 0;
    for (zero_target = 2101; zero_target <= 2102; zero_target++)for (zero_kind =
            1;
            zero_kind <= 4; zero_kind++) {
            wl_columnar_memory_governor_ref_t *g = governor();
            col_rel_t *r = relation(g, 33, 0, true); fixture_t f = {0};
            acquire(&f, r, 1);
            wl_columnar_relation_radix_workspace_attempt_t *a = create();
            CHECK(!wl_columnar_relation_radix_workspace_attempt_begin(a, r, 0,
                33, &f.leases[0]));
            before_image =
                malloc(wl_columnar_relation_radix_workspace_attempt_size());
            CHECK(before_image); zero_hits = 0;
            wl_columnar_relation_radix_workspace_attempt_test_checkpoint =
                zero_hook;
            int rc = prepare(a);
            const wl_columnar_radix_workspace_t *out = NULL;
            if (zero_target == 2102) {
                CHECK(!rc);
                rc = wl_columnar_relation_radix_workspace_attempt_borrow(a,
                        &out);
            }
            CHECK(zero_hits == 1 && !out && same_actor(a));
            CHECK(rc ==
                (zero_kind == 1?WL_EVALUATION_CONTROL_CANCELLED:zero_kind ==
                4?WL_EVALUATION_CONTROL_WORK_LIMIT:EINVAL));
            wl_columnar_relation_radix_workspace_attempt_test_checkpoint = NULL;
            end(a); free(a); free(before_image);
            if (zero_kind == 2 || zero_kind == 3) {
                wl_evaluation_control_report_t now;
                CHECK(!wl_evaluation_control_report(original,
                    &now) && same_report(&frozen, &now));
                if (zero_kind == 3) {
                    uint64_t used;
                    CHECK(!wl_evaluation_control_test_active_consumed(original,
                        wl_evaluation_control_active_attempt_id(original),
                        &used) && !used);
                    close_control(original, &original_owner, 0);
                }else {
                    CHECK(!wl_evaluation_control_detach(original,
                        &original_owner));
                    wl_evaluation_control_release(original);
                }
            } else close_control(original, &original_owner, rc);
            close_control(cleanup, &cleanup_owner, 0);
            CHECK(!col_rel_mutation_set_finish(&f.set, false));
            CHECK(!col_rel_destroy_checked(r));
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)));
            wl_columnar_memory_governor_ref_release(g);
        }
    puts(
        "radix workspace actual final READY/borrow zero refusal/cancel/END/successor: PASS");
}
int main(void) {
    nullary_insertions();
    const uint32_t sizes[] = {0, 1, 2, 32, 33, 49999, 50000};
    for (unsigned t = 0; t < 4;
        t++) for (unsigned ts = 0; ts < 2; ts++) for (unsigned i = 0;
                i < 7;
                i++) raw_case(sizes[i], t, ts, 1, 0, 0,
                    sizes[i] <= 33 || t < 2);
    for (unsigned slot = 0; slot < 7;
        slot += 3) raw_case(33, 0, true, 7, slot, 0, false);
    for (unsigned i = 1; i <= 6; i++)raw_case(50000, 0, true, 1, 0, i, false);
    raw_case(32, 2, true, 1, 0, 1, false);
    raw_case(32, 2, true, 1, 0, 2, false);
    faults(); stale_and_detached(); calls = 0; resources(); init_boundaries();
    final_zeros(); shared_owner(); source_packet_boundaries();
    partial_and_unmanaged(false);
    partial_and_unmanaged(true); changed_before_borrow();
    raw_case(33, 4, true, 1, 0, 0, false); raw_case(33, 5, true, 1, 0, 0, true);
    advisory = true; raw_case(33, 1, true, 1, 0, 0, false); advisory = false;
    puts(
        "radix workspace raw thresholds, real member leases, allocator order and lawful consumer End: PASS");
    return 0;
}
