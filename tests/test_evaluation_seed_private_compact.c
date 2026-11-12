#include "../wirelog/columnar/session_seed.h"
#include "../wirelog/intern.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__,#x); abort(); } } while (0)
static unsigned original_owner, cleanup_owner;
static wl_evaluation_control_t *control(unsigned *owner) {
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(UINT64_MAX, &c));
    CHECK(!wl_evaluation_control_attach(c, owner));
    CHECK(!wl_evaluation_control_begin(c, owner)); return c;
}
static void finish(wl_evaluation_control_t *c, unsigned *owner, int rc) {
    CHECK(wl_evaluation_control_finish(c, owner, rc > 0?rc:0, 0) == rc);
    CHECK(!wl_evaluation_control_detach(c, owner));
    wl_evaluation_control_release(c);
}
static wl_columnar_memory_governor_ref_t *governor(void) {
    wl_columnar_memory_resolution_t r = {.budget_bytes = UINT64_C(128)<<20,
                                         .usable_bytes = UINT64_C(128)<<20,
                                         .mode =
                                             WL_COLUMNAR_MEMORY_MODE_ENFORCING};
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&r); CHECK(g); return g;
}
static col_rel_t *source(wl_columnar_memory_governor_ref_t *g, uint32_t width,
    uint32_t rows, uint32_t cap, bool ts, unsigned type, wl_intern_t *strings,
    bool duplicate) {
    col_rel_t *r = NULL;
    if (g)CHECK(!wl_columnar_relation_alloc_governed(&r, "out", g));
    else CHECK(!col_rel_alloc(&r, "out"));
    CHECK(!col_rel_set_schema(r, width, NULL));
    if (width) {
        wirelog_column_type_t *types = malloc(width*sizeof(*types));
        CHECK(types);
        for (uint32_t c = 0; c < width;
            c++) types[c] = (wirelog_column_type_t)type;
        CHECK(!col_rel_set_column_types(r, types, width)); free(types);
    }
    if (cap > r->capacity)CHECK(!col_rel_reserve_capacity_admitted(r, cap,
            NULL));
    if (ts)CHECK(!col_rel_enable_timestamps(r));
    int64_t *values = malloc((width?width:1)*sizeof(*values)); CHECK(values);
    for (uint32_t n = 0; n < rows; n++) {
        for (uint32_t c = 0; c < width; c++){
            uint32_t value = duplicate?1:n+1;
            if (type == WIRELOG_TYPE_FLOAT) {
                double f = (double)value+0.25;
                memcpy(&values[c], &f, sizeof(f));
            }else if (type == WIRELOG_TYPE_STRING) {
                char s[48]; snprintf(s, sizeof(s), "value-%u-%u", value, c);
                values[c] = wl_intern_put(strings, s); CHECK(values[c] >= 0);
            }else values[c] = value+(int64_t)c;
        }
        CHECK(!col_rel_append_row(r, values));
        if (ts)r->timestamps[n] = (col_delta_timestamp_t){.iteration = n+17,
                                                          .stratum = 3,
                                                          .worker = 2,
                                                          .multiplicity =
                                                              (int64_t)n+1};
    }
    free(values);
    if (!cap) {
        CHECK(!rows); CHECK(!col_rel_compact(r));
    }
    return r;
}
static int forward(wl_columnar_session_seed_t *a, wl_evaluation_control_t *m) {
    for (unsigned i = 0; i < 1000000; i++) {
        unsigned p = wl_columnar_session_seed_progress(a);
        if (p == WL_COLUMNAR_SESSION_SEED_RULE_READY ||
            p == WL_COLUMNAR_SESSION_SEED_COMPOSED) return 0;
        int rc = p ==
            WL_COLUMNAR_SESSION_SEED_NEEDS_CLEANUP?
            wl_columnar_session_seed_maintenance_step(a, m,
                wl_evaluation_control_active_attempt_id(m)):
            wl_columnar_session_seed_step(a);
        if (rc)return rc;
    }
    CHECK(false); return EIO;
}
static void final(wl_columnar_session_seed_t *a, wl_evaluation_control_t *m) {
    bool ready = false;
    for (unsigned i = 0; i < 1000000 && !ready;
        i++) CHECK(!wl_columnar_session_seed_finalize(a, m,
            wl_evaluation_control_active_attempt_id(m), &ready)); CHECK(ready);
}
static void end(wl_columnar_session_seed_t *a, wl_evaluation_control_t *m,
    int rc) {
    bool done = false; for (unsigned i = 0; i < 1000000 && !done; i++) {
        int result = wl_columnar_session_seed_end(a, rc, m,
                wl_evaluation_control_active_attempt_id(m), &done);
        if (result) {
            uint64_t v[16];
            CHECK(!wl_columnar_session_seed_test_compact_view(a, v));
            fprintf(stderr, "End error%d compact%llu\n", result,
                (unsigned long long)v[0]);
        }
        CHECK(!result);
    }
    CHECK(done);
}
static wl_columnar_session_seed_t *actor(wl_evaluation_control_t *c,
    wl_evaluation_control_t *m, wl_columnar_memory_governor_ref_t *g,
    col_rel_t *t, col_rel_t *rule, col_rel_t *shadow,
    wl_columnar_session_seed_recipe_t *recipe) {
    wl_columnar_session_seed_t *a = malloc(wl_columnar_session_seed_size());
    CHECK(a);
    CHECK(!wl_columnar_session_seed_init(a, c,
        wl_evaluation_control_active_attempt_id(c), g));
    *recipe =
        (wl_columnar_session_seed_recipe_t){.reset =
                                                WL_COLUMNAR_SESSION_SEED_KEEP_INITIAL,
                                            .name = "out",
                                            .target_snapshot =
                                                wl_columnar_relation_snapshot(
                                                t)};
    CHECK(!wl_columnar_session_seed_prepare(a, t, shadow, recipe));
    CHECK(!forward(a, m));
    CHECK(!wl_columnar_session_seed_attach_rule(a, rule));
    CHECK(!forward(a, m)); final(a, m); return a;
}
static int compact(wl_columnar_session_seed_t *a, wl_evaluation_control_t *m,
    bool *ready) {
    for (unsigned i = 0; i < 1000000 && !*ready; i++) {
        int rc = wl_columnar_session_seed_progress(a) ==
            WL_COLUMNAR_SESSION_SEED_NEEDS_CLEANUP?
            wl_columnar_session_seed_maintenance_step(a, m,
                wl_evaluation_control_active_attempt_id(m)):
            wl_columnar_session_seed_compact_final_step(a, ready);
        if (rc)return rc;
    }
    CHECK(*ready); return 0;
}
static void equal(col_rel_t *a, col_rel_t *b, uint64_t av, uint64_t as,
    uint64_t bv, uint64_t bs) {
    CHECK(a->ncols == b->ncols && a->nrows == b->nrows &&
        a->capacity == b->capacity &&
        a->timestamp_capacity == b->timestamp_capacity);
    CHECK(a->declared_ncols == b->declared_ncols &&
        a->has_graph_column == b->has_graph_column &&
        a->sorted_nrows == b->sorted_nrows && a->base_nrows == b->base_nrows);
    CHECK(a->run_count == b->run_count && !memcmp(a->run_ends, b->run_ends,
        sizeof(a->run_ends)));
    CHECK((a->row_scratch != NULL) == (b->row_scratch != NULL));
    CHECK(a->view_generation-av == b->view_generation-bv &&
        a->storage_generation-as == b->storage_generation-bs);
    CHECK(a->schema_ok == b->schema_ok &&
        (a->column_types != NULL) == (b->column_types != NULL));
    for (uint32_t c = 0; c < a->ncols; c++) {
        CHECK(a->column_types[c] == b->column_types[c]);
        CHECK(!strcmp(a->schema.children[c]->format,
            b->schema.children[c]->format));
        for (uint32_t r = 0; r < a->nrows;
            r++) CHECK(a->columns[c][r] == b->columns[c][r]);
    }
    if (a->timestamps) {
        CHECK(b->timestamps);
        for (uint32_t r = 0; r < a->nrows;
            r++) CHECK(!memcmp(&a->timestamps[r], &b->timestamps[r],
                sizeof(*a->timestamps)));
    }
}
static void baseline(uint32_t width, uint32_t rows, uint32_t cap, bool ts,
    unsigned type, bool scratch, bool normalized, bool duplicate) {
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_intern_t *strings = wl_intern_create(); CHECK(strings);
    col_rel_t *t = source(scratch?g:NULL, width, rows, cap, ts, type, strings,
            duplicate);
    col_rel_t *rule = source(g, width, 0, 64, false, type, strings, false);
    col_rel_t *shadow = normalized?t:NULL;
    wl_evaluation_control_t *c = control(&original_owner),
        *m = control(&cleanup_owner);
    wl_columnar_session_seed_recipe_t recipe1, recipe2;
    wl_columnar_session_seed_t *a = actor(c, m, g, t, rule, shadow, &recipe1);
    wl_columnar_session_seed_t *b = actor(c, m, g, t, rule, shadow, &recipe2);
    col_rel_t *raw = NULL; CHECK(!wl_columnar_session_seed_take(b, &raw));
    end(b, m, 0); free(b);
    uint64_t rv = raw->view_generation, rs = raw->storage_generation;
    /* AUTH publishes only a private compact marker; no buffer changes. */
    bool ready = false;
    CHECK(!wl_columnar_session_seed_compact_final_step(a, &ready));
    CHECK(!ready);
    uint64_t view[16];
    CHECK(!wl_columnar_session_seed_test_compact_view(a, view));
    col_rel_t *owned = (col_rel_t *)(uintptr_t)view[15];
    /* Real committed owner bits are privileged test observation, not Native
     * permission. Nullary/no-payload owners need no pointer observation. */
    uint64_t av = raw->view_generation, as = raw->storage_generation;
    if (owned) {
        av = owned->view_generation; as = owned->storage_generation;
    }
    CHECK(!col_rel_compact(raw)); CHECK(!compact(a, m, &ready)); CHECK(ready);
    bool again = false;
    CHECK(!wl_columnar_session_seed_compact_final_step(a, &again) && again);
    col_rel_t *out = NULL; CHECK(!wl_columnar_session_seed_take(a, &out));
    equal(out, raw, av, as, rv, rs); end(a, m, 0); free(a);
    CHECK(!col_rel_destroy_checked(out) && !col_rel_destroy_checked(raw));
    CHECK(!col_rel_destroy_checked(rule) && !col_rel_destroy_checked(t));
    finish(c, &original_owner, 0); finish(m, &cleanup_owner, 0);
    wl_intern_free(strings);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
/* Literal own-action256 and prepare-child228 boundary tests. This census
 * selects actual phases/cursors only; it is never a measured host tariff. */
typedef struct { unsigned phase; uint64_t fields[14]; } cut_t;
static cut_t cuts[4096];
static size_t cut_count, selected;
static unsigned mode;
static bool watching, collecting, injected, maintenance_fault;
static bool original_closed, maintenance_closed, successor;
static wl_evaluation_control_t *fault_original, *fault_maintenance;
static wl_columnar_session_seed_t *fault_actor;
static wl_evaluation_control_report_t frozen;
static bool governor_only, prepare_seen;
static unsigned governor_phase;
static cut_t last_outer, prepare_cut;
static uint64_t last_outer_consumed;
static void same_report(wl_evaluation_control_report_t a,
    wl_evaluation_control_report_t b) {
    CHECK(a.attempt_id == b.attempt_id && a.limit == b.limit &&
        a.consumed == b.consumed && a.stop_reason == b.stop_reason &&
        a.execution_status == b.execution_status &&
        a.cleanup_status == b.cleanup_status && a.result == b.result);
}
static void inject(bool maintenance) {
    injected = true; maintenance_fault = maintenance;
    wl_evaluation_control_t *c = maintenance?fault_maintenance:fault_original;
    unsigned *owner = maintenance?&cleanup_owner:&original_owner;
    uint64_t id = wl_evaluation_control_active_attempt_id(c), used;
    CHECK(!wl_evaluation_control_test_active_consumed(c, id, &used));
    if (mode == 1)wl_evaluation_control_request_cancel(c);
    else if (mode == 2 || mode == 3 || mode == 7 || mode == 8) {
        uint64_t left = mode == 2?255:mode == 3?256:mode == 7?483:484;
        CHECK(!wl_evaluation_control_charge_attempt(c, id,
            UINT64_MAX-used-left));
        if (!maintenance && !governor_only)
            last_outer_consumed = UINT64_MAX-left;
    } else if (mode == 4 || mode == 5) {
        CHECK(!wl_evaluation_control_finish(c, owner, 0, 0));
        CHECK(!wl_evaluation_control_report(c, &frozen));
        if (mode == 5) {
            CHECK(!wl_evaluation_control_reset(c));
            CHECK(!wl_evaluation_control_begin(c, owner)); successor = true;
        }
        if (maintenance)maintenance_closed = true; else original_closed = true;
    }
}
static void fault_checkpoint(const wl_columnar_session_seed_t *a,
    unsigned phase, unsigned stage) {
    (void)stage;
    if (!watching || !a || a != fault_actor || phase < 1800 ||
        phase >= 2000) return;
    cut_t key = {0}; key.phase = phase; uint64_t v[16];
    CHECK(!wl_columnar_session_seed_test_compact_view(a, v));
    for (unsigned i = 0; i < 14; i++)key.fields[i] = v[i];
    if (phase < 1900) {
        last_outer = key;
        CHECK(!wl_evaluation_control_test_active_consumed(fault_original,
            wl_evaluation_control_active_attempt_id(fault_original),
            &last_outer_consumed));
    }
    if (collecting) {
        for (size_t i = 0; i < cut_count;
            i++) if (!memcmp(&key, &cuts[i], sizeof(key))) return;
        CHECK(cut_count < 4096); cuts[cut_count++] = key; return;
    }
    if (governor_only || injected || !mode || memcmp(&key, &cuts[selected],
        sizeof(key))) return;
    inject(phase >= 1900);
}
static void governor_checkpoint(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase) {
    (void)token;
    if (!watching || c != fault_original)return;
    if (phase == WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_ARGUMENTS) {
        uint64_t used;
        CHECK(!wl_evaluation_control_test_active_consumed(c,
            wl_evaluation_control_active_attempt_id(c), &used));
        CHECK(used-last_outer_consumed == 484); /* literal256+228, not host H */
        prepare_seen = true; prepare_cut = last_outer;
    }
    if (governor_only && !injected && phase == governor_phase)inject(false);
}
static void close_any(wl_evaluation_control_t *c, unsigned *owner,
    int execution) {
    if (wl_evaluation_control_active_attempt_id(c)) {
        int rc = wl_evaluation_control_finish(c, owner,
                execution > 0?execution:0, 0);
        CHECK(rc == execution || rc == 0 ||
            rc == WL_EVALUATION_CONTROL_CANCELLED ||
            rc == WL_EVALUATION_CONTROL_WORK_LIMIT);
    }
    CHECK(!wl_evaluation_control_detach(c, owner));
    wl_evaluation_control_release(c);
}
static void cut_run(bool empty, bool scratch, bool census) {
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_intern_t *strings = wl_intern_create(); CHECK(strings);
    col_rel_t *t = source(scratch?g:NULL, 3, empty?0:2, 64, true,
            WIRELOG_TYPE_INT64, strings, false);
    col_rel_t *rule = source(g, 3, 0, 64, false, WIRELOG_TYPE_INT64, strings,
            false);
    col_relation_snapshot_t before = wl_columnar_relation_snapshot(t);
    wl_evaluation_control_t *c = control(&original_owner),
        *m = control(&cleanup_owner);
    wl_columnar_session_seed_recipe_t recipe;
    wl_columnar_session_seed_t *a = actor(c, m, g, t, rule, NULL, &recipe);
    fault_original = c; fault_maintenance = m; fault_actor = a;
    original_closed = maintenance_closed = successor = injected =
        maintenance_fault = false;
    collecting = census; watching = true;
    wl_columnar_session_seed_test_checkpoint = fault_checkpoint;
    wl_columnar_memory_governor_test_checkpoint = governor_checkpoint;
    bool ready = false; int rc = compact(a, m, &ready);
    watching = false; wl_columnar_session_seed_test_checkpoint = NULL;
    wl_columnar_memory_governor_test_checkpoint = NULL;
    if (!census) {
        CHECK(injected); CHECK(rc || mode == 3);
    }
    if (mode == 7 || mode == 8) {
        uint64_t v[16];
        CHECK(!wl_columnar_session_seed_test_compact_view(a, v));
        CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(v[9] ==
            (mode ==
            7?WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_INERT:
            WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_READY));
    }

    if (maintenance_fault) {
        if (!maintenance_closed) {
            int stopped = wl_evaluation_control_finish(m, &cleanup_owner, 0, 0);
            CHECK(stopped == 0 || stopped == WL_EVALUATION_CONTROL_CANCELLED ||
                stopped == WL_EVALUATION_CONTROL_WORK_LIMIT);
        }
        m = control(&cleanup_owner);
    }
    uint64_t original_used = 0;
    if (!original_closed)CHECK(!wl_evaluation_control_test_active_consumed(c,
            wl_evaluation_control_active_attempt_id(c), &original_used));
    end(a, m, rc); free(a);
    if (!original_closed) {
        uint64_t after;
        CHECK(!wl_evaluation_control_test_active_consumed(c,
            wl_evaluation_control_active_attempt_id(c), &after));
        CHECK(after == original_used);
    }
    if (original_closed || maintenance_closed) {
        wl_evaluation_control_t *ended = original_closed?c:fault_maintenance;
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(ended, &report));
        same_report(report, frozen);
        if (successor) {
            uint64_t used;
            CHECK(!wl_evaluation_control_test_active_consumed(ended,
                wl_evaluation_control_active_attempt_id(ended),
                &used) && !used);
        }
    }
    CHECK(wl_columnar_relation_snapshot_equal(before,
        wl_columnar_relation_snapshot(t)));
    CHECK(!col_rel_destroy_checked(rule) && !col_rel_destroy_checked(t));
    close_any(c, &original_owner, original_closed?0:rc);
    finish(m, &cleanup_owner, 0);
    if (maintenance_fault)close_any(fault_maintenance, &cleanup_owner, 0);
    wl_intern_free(strings);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
static void cuts_matrix(bool empty, bool scratch) {
    mode = 0; cut_count = 0; cut_run(empty, scratch, true);
    size_t count = cut_count;
    for (size_t i = 0; i < count;
        i++) for (unsigned kind = 1; kind <= 5; kind++) {
            selected = i; mode = kind; cut_run(empty, scratch, false);
        }
    mode = 0;
    fprintf(stderr, "private compact: %zu actual %s cuts x5 scratch%u passed\n",
        count, empty?"empty":"shrink", scratch);
}
static void prepare_boundaries(void) {
    mode = 0; cut_count = 0; prepare_seen = false; cut_run(false, true, true);
    CHECK(prepare_seen);
    cuts[0] = prepare_cut; selected = 0; mode = 7; cut_run(false, true, false);
    mode = 8; cut_run(false, true, false);
    const unsigned phases[] = {WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_ARGUMENTS,
                               WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_SNAPSHOT,
                               WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_PRECLAIM,
                               WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_CLAIMED,
                               WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_READY_CUTOFF};
    governor_only = true;
    for (unsigned p = 0; p < sizeof(phases)/sizeof(phases[0]);
        p++) for (unsigned kind = 1;
            kind <= 5; kind++) {
            governor_phase = phases[p]; mode = kind;
            cut_run(false, true, false);
        }
    governor_only = false; mode = 0;
    fprintf(stderr,
        "private compact literal483/484 and25 actual prepare checkpoint cuts passed\n");
}
static uint64_t peak;
static unsigned admissions;
static wl_columnar_memory_governor_t *observed_governor;
static void admitted(wl_columnar_memory_governor_t *g, uint64_t total) {
    if (g != observed_governor)return;
    admissions++; if (total > peak)peak = total;
}
/* kind0 allocator ordinal;1 realDENIED;2 commitEFAULT;3 OOM+rollbackEAGAIN;
* 4 commit+rollbackEAGAIN;5 EAGAIN then realCANCEL/End;6 counterOVERFLOW. */
static void resources(unsigned kind, unsigned ordinal, uint32_t width) {
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_columnar_memory_governor_t *bank =
        wl_columnar_memory_governor_ref_get(g);
    wl_intern_t *strings = wl_intern_create(); CHECK(strings);
    col_rel_t *t = source(g, width, 2, 64, true, WIRELOG_TYPE_INT64, strings,
            false),
        *rule = source(g, width, 0, 64, false, WIRELOG_TYPE_INT64, strings,
            false);
    wl_evaluation_control_t *c = control(&original_owner),
        *m = control(&cleanup_owner);
    wl_columnar_session_seed_recipe_t recipe1, recipe2;
    wl_columnar_session_seed_t *a = actor(c, m, g, t, rule, NULL, &recipe1),
        *b = actor(c, m, g, t, rule, NULL, &recipe2);
    col_rel_t *raw = NULL; CHECK(!wl_columnar_session_seed_take(b, &raw));
    end(b, m, 0); free(b);
    bool ready = false;
    CHECK(!wl_columnar_session_seed_compact_final_step(a, &ready) && !ready);
    uint64_t v[16]; CHECK(!wl_columnar_session_seed_test_compact_view(a, v));
    col_rel_t *owned = (col_rel_t *)(uintptr_t)v[15]; CHECK(owned);
    col_rel_t *forbidden = NULL;
    CHECK(wl_columnar_session_seed_take(a, &forbidden) == EINVAL && !forbidden);
    uint64_t av = owned->view_generation, as = owned->storage_generation,
        rv = raw->view_generation, rs = raw->storage_generation;
    int64_t **old_columns = owned->columns;
    col_delta_timestamp_t *old_ts = owned->timestamps;
    uint64_t before = wl_columnar_memory_reserved(bank),
        denials = wl_columnar_memory_denials(bank);
    uint64_t incoming = (uint64_t)width*(sizeof(int64_t *)+64*sizeof(int64_t))+
        64*sizeof(col_delta_timestamp_t);
    uint64_t usable = atomic_load_explicit(&bank->usable_bytes,
            memory_order_acquire);
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer); bool peer_live = false;
    if (kind == 0 || kind == 3 ||
        kind == 5) wl_columnar_relation_test_fail_next_prepare_resize();
    if (kind == 2 ||
        kind == 4) wl_columnar_relation_test_fail_next_compact_commit();
    if (kind == 3 || kind == 4 ||
        kind == 5) wl_columnar_relation_test_fail_next_compact_rollback();
    if (kind == 1)atomic_store_explicit(&bank->usable_bytes, before,
            memory_order_release);
    if (kind == 6) {
        atomic_store_explicit(&bank->usable_bytes, UINT64_MAX,
            memory_order_release);
        CHECK(wl_columnar_memory_reserve_checked(bank, UINT64_MAX-before,
            &peer) == WL_COLUMNAR_MEMORY_ADMISSION_OK); peer_live = true;
    }
    int raw_rc = col_rel_compact(raw);
    CHECK(raw_rc == (kind == 2?EFAULT:(kind == 3 || kind == 4 ||
        kind == 5)?EAGAIN:0));
    observed_governor = bank; admissions = 0;
    peak = wl_columnar_memory_reserved(bank);
    wl_columnar_memory_governor_test_admitted = admitted;
    if (kind == 0 || kind == 3 ||
        kind ==
        5) wl_columnar_relation_test_constructor_fail_allocation = ordinal;
    if (kind == 2 ||
        kind == 4) wl_columnar_relation_test_fail_next_compact_commit();
    if (kind == 3 || kind == 4 ||
        kind == 5) wl_columnar_relation_test_fail_next_compact_rollback();
    int rc = compact(a, m, &ready);
    CHECK(rc == raw_rc);
    CHECK(!wl_columnar_session_seed_test_compact_view(a, v));
    if (kind == 0 || kind == 3 ||
        kind ==
        5) CHECK(!wl_columnar_relation_test_constructor_fail_allocation);
    CHECK(owned->columns == old_columns && owned->timestamps == old_ts &&
        owned->storage_generation == as);
    if (kind == 1 || kind == 6) {
        CHECK(ready &&
            v[8] ==
            (kind ==
            1?WL_COLUMNAR_MEMORY_ADMISSION_DENIED:
            WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW));
        CHECK(v[14] == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED && !v[9] &&
            !v[10] && !v[11]);
        CHECK(!admissions && !owned->memory_budget_denial_pending);
        CHECK(wl_columnar_memory_denials(bank)-denials == (kind == 1?2:0));
        CHECK(!wl_evaluation_control_charge_attempt(c,
            wl_evaluation_control_active_attempt_id(c), 0));
    } else {
        CHECK(admissions == 1 &&
            peak == before+incoming*((kind == 3 || kind == 4 ||
            kind == 5)?2:1));
        if (rc == EAGAIN) {
            CHECK(!ready && v[14] == WL_COLUMNAR_MEMORY_RESERVATION_REPLACING &&
                !v[10]);
            CHECK(wl_columnar_memory_reserved(bank) == before+2*incoming);
            uint64_t live = wl_columnar_memory_reserved(bank);
            atomic_store_explicit(&bank->usable_bytes, live,
                memory_order_release);
            wl_columnar_memory_reservation_t competitor;
            wl_columnar_memory_reservation_init(&competitor);
            CHECK(wl_columnar_memory_reserve_checked(bank, 1,
                &competitor) == WL_COLUMNAR_MEMORY_ADMISSION_DENIED);
            if (kind == 5) {
                wl_evaluation_control_request_cancel(c);
                CHECK(wl_evaluation_control_charge_attempt(c,
                    wl_evaluation_control_active_attempt_id(c),
                    0) == WL_EVALUATION_CONTROL_CANCELLED);
                rc = WL_EVALUATION_CONTROL_CANCELLED;
            } else {
                unsigned count = admissions;
                CHECK(!compact(a, m, &ready) && ready && admissions == count);
                CHECK(!col_rel_compact(raw));
                CHECK(wl_columnar_memory_reserved(bank) == before);
                rc = 0;
            }
        } else {
            CHECK(wl_columnar_memory_reserved(bank) == before &&
                v[14] == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
            CHECK(v[10] == WL_COLUMNAR_MEMORY_GROWTH_REFUND_DONE);
        }
    }
    wl_columnar_memory_governor_test_admitted = NULL;
    if (peer_live) {
        bool done = false; for (unsigned i = 0; i < 1000 && !done; i++){
            int x = wl_columnar_memory_reservation_finish_step(&peer, true,
                    &done); CHECK(!x || x == EAGAIN);
        }
        CHECK(done);
    }
    atomic_store_explicit(&bank->usable_bytes, usable, memory_order_release);
    col_rel_t *out = NULL;
    if (!rc) {
        CHECK(ready); CHECK(!wl_columnar_session_seed_take(a, &out));
        equal(out, raw, av, as, rv, rs);
    }else if (rc == EFAULT) {
        CHECK(!ready); equal(owned, raw, av, as, rv, rs);
    }
    end(a, m, rc); free(a);
    if (kind ==
        5) CHECK(wl_columnar_memory_rollback_growth(
                &raw->retained_reservation));
    if (out)CHECK(!col_rel_destroy_checked(out));
    CHECK(!col_rel_destroy_checked(raw) && !col_rel_destroy_checked(rule) &&
        !col_rel_destroy_checked(t));
    finish(c, &original_owner, rc); finish(m, &cleanup_owner, 0);
    wl_intern_free(strings);
    CHECK(!wl_columnar_memory_reserved(bank));
    wl_columnar_memory_governor_ref_release(g);
}
static void busy_case(bool alias) {
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_intern_t *strings = wl_intern_create();
    col_rel_t *t = source(g, 1, 1, 64, false, WIRELOG_TYPE_INT64, strings,
            false),
        *rule = source(g, 1, 0, 64, false, WIRELOG_TYPE_INT64, strings, false);
    wl_evaluation_control_t *c = control(&original_owner),
        *m = control(&cleanup_owner);
    wl_columnar_session_seed_recipe_t recipe;
    wl_columnar_session_seed_t *a = actor(c, m, g, t, rule, NULL, &recipe);
    bool ready = false;
    CHECK(!wl_columnar_session_seed_compact_final_step(a, &ready));
    uint64_t v[16]; CHECK(!wl_columnar_session_seed_test_compact_view(a, v));
    col_rel_t *owned = (col_rel_t *)(uintptr_t)v[15]; CHECK(owned);
    uint64_t generation = owned->storage_generation;
    col_rel_t *view = NULL; wl_columnar_source_access_reader_t reader = {0};
    if (alias) {
        CHECK(!wl_columnar_relation_new_like_governed_checked(&view,
            "alias", owned, g));
        CHECK(!col_rel_install_shared_view(view, owned));
        CHECK(col_rel_storage_alias_borrow_count(owned) == 1);
    }else CHECK(!wl_columnar_source_access_reader_acquire(
                &owned->descriptor_access, &reader));
    CHECK(compact(a, m,
        &ready) == EBUSY && !ready && owned->storage_generation == generation);
    if (alias)CHECK(!col_rel_destroy_checked(view));
    else CHECK(!wl_columnar_source_access_reader_release(&reader));
    end(a, m, EBUSY); free(a);
    CHECK(!col_rel_destroy_checked(t) && !col_rel_destroy_checked(rule));
    finish(c, &original_owner, EBUSY); finish(m, &cleanup_owner, 0);
    wl_intern_free(strings);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
static void foreign_claim(bool replacement) {
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_intern_t *strings = wl_intern_create();
    col_rel_t *t = source(g, 1, 1, 64, false, WIRELOG_TYPE_INT64, strings,
            false),
        *rule = source(g, 1, 0, 64, false, WIRELOG_TYPE_INT64, strings, false);
    wl_evaluation_control_t *c = control(&original_owner),
        *m = control(&cleanup_owner);
    wl_columnar_session_seed_recipe_t recipe;
    wl_columnar_session_seed_t *a = actor(c, m, g, t, rule, NULL, &recipe);
    bool ready = false;
    CHECK(!wl_columnar_session_seed_compact_final_step(a, &ready));
    uint64_t v[16]; CHECK(!wl_columnar_session_seed_test_compact_view(a, v));
    col_rel_t *owned = (col_rel_t *)(uintptr_t)v[15]; CHECK(owned);
    int64_t **columns = owned->columns;
    uint64_t gen = owned->storage_generation;
    wl_columnar_memory_admission_status_t status =
        replacement?
        wl_columnar_memory_begin_replacement(&owned->retained_reservation,
            1):wl_columnar_memory_begin_growth(&owned->retained_reservation, 1);
    CHECK(status == WL_COLUMNAR_MEMORY_ADMISSION_OK);
    CHECK(compact(a, m,
        &ready) == EBUSY && !ready && owned->columns == columns &&
        owned->storage_generation == gen);
    CHECK(col_rel_compact(owned) == EBUSY && owned->columns == columns &&
        owned->storage_generation == gen);
    /* This financial claim belongs to the adversarial test, not the actor.
     * The actor must retain its true owner rather than refund another role. */
    bool done = false; int stopped = 0;
    for (unsigned i = 0; i < 1000 && !stopped && !done;
        i++) stopped = wl_columnar_session_seed_end(a, EBUSY, m,
                wl_evaluation_control_active_attempt_id(m), &done);
    CHECK(stopped == EBUSY && !done);
    CHECK(replacement?
        wl_columnar_memory_rollback_replacement(&owned->retained_reservation):
        wl_columnar_memory_rollback_growth(&owned->retained_reservation));
    end(a, m, EBUSY); free(a);
    CHECK(!col_rel_destroy_checked(t) && !col_rel_destroy_checked(rule));
    finish(c, &original_owner, EBUSY); finish(m, &cleanup_owner, 0);
    wl_intern_free(strings);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
int main(void) {
    CHECK(!col_rel_compact(NULL));
    bool invalid_ready = false;
    CHECK(wl_columnar_session_seed_compact_final_step(NULL,
        &invalid_ready) == EINVAL);
    for (unsigned type = 0; type < 3;
        type++) for (unsigned ts = 0; ts < 2; ts++) for (unsigned scratch = 0;
                scratch < 2; scratch++) {
                unsigned code = type == 0?WIRELOG_TYPE_INT64:type ==
                    1?WIRELOG_TYPE_FLOAT:WIRELOG_TYPE_STRING;
                baseline(1, 0, 64, ts, code, scratch, false, false);
                baseline(1, 1, 64, ts, code, scratch, false, false);
                baseline(1, 16, 64, ts, code, scratch, false, false);
                baseline(1, 16, 65, ts, code, scratch, false, false);
                baseline(1, 2, 512, ts, code, scratch, false, false);
            }
    baseline(17, 3, 512, true, WIRELOG_TYPE_INT64, true, false, false);
    baseline(1, 80, 512, true, WIRELOG_TYPE_INT64, true, true, false);
    baseline(1, 80, 512, true, WIRELOG_TYPE_INT64, true, true, true);
    baseline(0, 0, 64, true, WIRELOG_TYPE_INT64, true, false, false);
    baseline(1, 0, 0, false, WIRELOG_TYPE_INT64, false, false, false);
    baseline(1, 0, 0, false, WIRELOG_TYPE_INT64, true, false, false);
    for (unsigned n = 1; n <= 5; n++)resources(0, n, 3);
    for (unsigned n = 1; n <= 19; n++)resources(0, n, 17);
    for (unsigned kind = 1; kind <= 6; kind++)resources(kind, 1, 3);
    busy_case(false); busy_case(true); foreign_claim(false);
    foreign_claim(true);
    prepare_boundaries();
    cuts_matrix(false, true); cuts_matrix(true, true); cuts_matrix(true, false);
    fprintf(stderr,
        "private compact raw baselines passed; actor bytes%zu token bytes%zu\n",
        wl_columnar_session_seed_size(),
        sizeof(wl_columnar_memory_reservation_t));
    return 0;
}
