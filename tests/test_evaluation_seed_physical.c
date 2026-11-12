#include "../wirelog/columnar/session_seed.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/wirelog-extension.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) {fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                     __LINE__,#x); abort();} } while (0)
static const char *programs[] = {
    ".decl input(x:int64)\n.decl out(y:int64)\ninput(1). out(7). out(strlen(to_string(x))):-input(x).",
    ".decl input(x:string)\n.decl out(y:string)\ninput(\"a\"). out(\"z\"). out(cat(x,\"!\")):-input(x).",
    ".decl input(x:float)\n.decl out(y:float)\ninput(1.5). out(7.5). out(x):-input(x).",
    "input(1). out(7). out(strlen(to_string(x))):-input(x).",
    ".decl input(x:int64)\n.decl out(y:int64)\ninput(1). out(7). out(strlen(to_string(x))):-input(x),x>9.",
    ".decl input(x:int64)\n.decl out(y:int64)\ninput(1). out(7). out(x+1):-input(x).",
    ".decl input(x:int64)\n.decl out(y:int64)\ninput(1). out(7). out(@call(\"seed.effect\",x)):-input(x)."
};
static unsigned effect_calls, pre_rule_denials, post_rule_denials;
static int effect(const wirelog_extension_value_t *args, uint32_t n,
    wirelog_extension_value_t *out, void *user) {
    (void)user; CHECK(n == 1); effect_calls++;
    out->type = WIRELOG_EXTENSION_VALUE_INT64;
    out->size = 8; out->as.int64_value = args[0].as.int64_value+1; return 0;
}
static unsigned operation_owner, cleanup_owner;
static unsigned injection_mode, prior_steps, extra_input, malformed_format,
    untyped_target;
static wl_evaluation_control_t *begin(unsigned *owner) {
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(injection_mode == 5 ? 0 : UINT64_MAX,
        &c));
    CHECK(!wl_evaluation_control_attach(c, owner));
    CHECK(!wl_evaluation_control_begin(c, owner)); return c;
}
static void finish(wl_evaluation_control_t *c, unsigned *owner, int rc) {
    CHECK(wl_evaluation_control_finish(c, owner, rc > 0 ? rc : 0, 0) == rc);
    if (injection_mode == 6 && owner == &operation_owner && rc == ENOSPC) {
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(c, &report));
        CHECK(report.stop_reason == WL_EVALUATION_CONTROL_NONE &&
            report.execution_status == ENOSPC && !report.cleanup_status &&
            report.result == ENOSPC && report.consumed < report.limit);
    }
    CHECK(!wl_evaluation_control_detach(c, owner));
    wl_evaluation_control_release(c);
}
static void cb(const char *n, const int64_t *r, uint32_t w, int32_t d, void *u){
    (void)n; (void)r; (void)w; (void)d; (void)u;
}
static int forward(wl_columnar_session_seed_t *a,
    wl_evaluation_control_t *cleanup) {
    for (unsigned i = 0; i < 500000; i++) {
        wl_columnar_session_seed_progress_t p =
            wl_columnar_session_seed_progress(a);
        if (p == WL_COLUMNAR_SESSION_SEED_RULE_READY ||
            p == WL_COLUMNAR_SESSION_SEED_COMPOSED) return 0;
        int rc = p == WL_COLUMNAR_SESSION_SEED_NEEDS_CLEANUP?
            wl_columnar_session_seed_maintenance_step(a, cleanup,
                wl_evaluation_control_active_attempt_id(cleanup)):
            wl_columnar_session_seed_step(a);
        if (rc) {
            uint64_t v[16]; CHECK(!wl_columnar_session_seed_test_view(a, v));
            if (!injection_mode &&
                !wl_columnar_relation_test_constructor_fail_allocation) fprintf(
                    stderr,
                    "seed rc%d progress%llu phase%llu stage%llu base%llu resize%llu\n",
                    rc, (unsigned long long)v[0], (unsigned long long)v[1],
                    (unsigned long long)v[2], (unsigned long long)v[3],
                    (unsigned long long)v[4]); return rc;
        }
    }
    CHECK(false); return EIO;
}
static void end(wl_columnar_session_seed_t *a, wl_evaluation_control_t *cleanup,
    int rc) {
    bool done = false; for (unsigned i = 0; i < 500000 && !done; i++) {
        int x = wl_columnar_session_seed_end(a, rc, cleanup,
                wl_evaluation_control_active_attempt_id(cleanup), &done);
        if (x) {
            uint64_t v[16]; CHECK(!wl_columnar_session_seed_test_view(a, v));
            fprintf(stderr,
                "end rc%d cleanup%llu retire%llu owner%llu base%llu\n", x,
                (unsigned long long)v[13], (unsigned long long)v[14],
                (unsigned long long)v[15], (unsigned long long)v[3]);
        }
        CHECK(!x);
    }
    CHECK(done);
}
static void equal(const col_rel_t *a, const col_rel_t *b) {
    CHECK(a && b && a->ncols == b->ncols && a->nrows == b->nrows);
    CHECK(a->declared_ncols == b->declared_ncols &&
        a->has_graph_column == b->has_graph_column);
    CHECK((a->timestamps != NULL) == (b->timestamps != NULL));
    CHECK((a->column_types != NULL) == (b->column_types != NULL));
    CHECK((a->col_names != NULL) == (b->col_names != NULL));
    CHECK(a->schema_ok == b->schema_ok);
    if (a->schema_ok) {
        CHECK(a->schema.n_children == b->schema.n_children);
        CHECK(!strcmp(a->schema.format, b->schema.format));
        for (uint32_t c = 0; c < a->ncols; c++) {
            CHECK(!strcmp(a->schema.children[c]->format,
                b->schema.children[c]->format));
            CHECK((a->schema.children[c]->name != NULL) ==
                (b->schema.children[c]->name != NULL));
            if (a->schema.children[c]->name)CHECK(!strcmp(
                        a->schema.children[c]->name,
                        b->schema.children[c]->name));
        }
    }

    for (uint32_t c = 0; c < a->ncols; c++) {
        CHECK((a->column_types?a->column_types[c]:WIRELOG_TYPE_INT64) ==
            (b->column_types?b->column_types[c]:WIRELOG_TYPE_INT64));
        for (uint32_t r = 0; r < a->nrows;
            r++) CHECK(a->columns[c][r] == b->columns[c][r]);
    }
    if (a->timestamps) for (uint32_t r = 0; r < a->nrows;
            r++) {
            if (memcmp(&a->timestamps[r], &b->timestamps[r],
                sizeof(*a->timestamps))) fprintf(stderr,
                    "TS row%u raw%u actor%u mult%lld vs%lld extra%u\n", r,
                    a->timestamps[r].iteration, b->timestamps[r].iteration,
                    (long long)a->timestamps[r].multiplicity,
                    (long long)b->timestamps[r].multiplicity, extra_input);
            CHECK(!memcmp(&a->timestamps[r], &b->timestamps[r],
                sizeof(*a->timestamps)));
        }
}
static int raw_reference(wl_session_t *raw, col_rel_t *target,
    const col_rel_t *rule) {
    if (!untyped_target)return wl_session_step(raw);
    CHECK(rule); int rc = col_rel_append_all(target, rule, NULL);
    if (!rc && target->nrows > 1)rc = wl_columnar_eval_delta_consolidate(target,
                COL_SESSION(raw));
    if (!rc) {
        rc = col_rel_compact(target);
    }
    return rc;
}
/* Censused identities select actual executed components, never a host-wide
 * measured tariff. Every selected cut buys a literal 256 before its body. */
typedef struct { unsigned phase, stage; uint64_t state[10]; } cut_t;
static cut_t cuts[1024];
static size_t cut_count, selected_cut;
static bool census_enabled;
static unsigned allocation_failure;
static bool injected, original_ended, cleanup_ended;
static uint64_t original_before_cleanup;
static wl_evaluation_control_t *active_original, *active_cleanup;
static wl_evaluation_control_report_t frozen;
static uint64_t successor_consumed;
static wl_columnar_memory_governor_t *active_governor;
static uint64_t saved_usable;
static bool governor_lowered;
static uint64_t reserved_at_floor;
static unsigned denied_site, governor_denials, raised_retries;
static bool saw_restore_denial, saw_append_growth_denial;
static bool saw_detached_ts, saw_zero_grid;
static bool cleanup_phase(unsigned p) {
    return p == 1002 || p == 1102 || p == 1500 || p == 1501 ||
           p == 1503 || p == 1602;
}
static unsigned admission_site(const cut_t *c) {
    if (c->phase == 1001 &&
        c->state[1] == 5) return WL_COLUMNAR_SESSION_SEED_TEST_BASE_LENGTH;
    if (c->phase == 1001 && c->state[1] == 10 && c->state[8] >= 9 &&
        c->state[8] <= 11){
        if (c->state[8] == 11 && !c->state[9])return 0;
        return (unsigned)c->state[8]-9+
               WL_COLUMNAR_SESSION_SEED_TEST_BASE_DESCRIPTOR;
    }
    if (c->phase == 1101 &&
        c->state[2] == 5) return WL_COLUMNAR_SESSION_SEED_TEST_RESIZE;
    if (c->phase == 1600 &&
        c->state[3] == 4) return WL_COLUMNAR_SESSION_SEED_TEST_TYPE_LENGTH;
    if (c->phase == 1600 &&
        c->state[3] == 9) return WL_COLUMNAR_SESSION_SEED_TEST_TYPE_METADATA;
    if (c->phase == 1402 && c->state[0] == 4 &&
        c->stage == 2) return WL_COLUMNAR_SESSION_SEED_TEST_RESTORE;
    if (c->phase == 1402 && c->state[0] == 7 &&
        c->stage == 7) return WL_COLUMNAR_SESSION_SEED_TEST_APPEND_SCRATCH;
    if (c->phase == 1402 && c->state[0] == 8 &&
        c->stage == 4) return WL_COLUMNAR_SESSION_SEED_TEST_SORT_SCRATCH;
    return 0;
}
static void checkpoint(const wl_columnar_session_seed_t *a, unsigned phase,
    unsigned stage) {
    if (!a) return;
    uint64_t geometry[8];
    if (!wl_columnar_session_seed_test_base_geometry(a, geometry)) {
        uint64_t w = geometry[0], cap = geometry[1], ts = geometry[2];
        CHECK(geometry[3] == (w && cap));
        CHECK(geometry[5] == (ts != 0));
        uint64_t expected = (cap?w*sizeof(int64_t *)+w*cap*sizeof(int64_t):0)+
            (geometry[4]?w*sizeof(int64_t):0)+ts*sizeof(col_delta_timestamp_t);
        CHECK(geometry[6] == expected && geometry[7] == expected);
        if (!cap) saw_zero_grid = true;
        if (!cap && ts) saw_detached_ts = true;
    }
    uint64_t v[16]; CHECK(!wl_columnar_session_seed_test_view(a, v));
    cut_t key = {.phase = phase, .stage = stage, .state = {v[1], v[3], v[4],
                                                           wl_columnar_session_seed_test_metadata_phase
                                                               (a), v[13],
                                                           v[14], v[15], v[0],
                                                           wl_columnar_session_seed_test_base_group
                                                               (a), v[11]}};
    if (!injection_mode) {
        if (!census_enabled)return;
        for (size_t i = 0; i < cut_count;
            i++) if (!memcmp(&key, &cuts[i], sizeof(key))) return;
        CHECK(cut_count < 1024); cuts[cut_count++] = key; return;
    }
    if (injected || memcmp(&key, &cuts[selected_cut], sizeof(key)))return;
    injected = true;
    bool maintenance = cleanup_phase(phase);
    if (maintenance) cleanup_ended = true;
    wl_evaluation_control_t *c = maintenance?active_cleanup:active_original;
    unsigned *owner = maintenance?&cleanup_owner:&operation_owner;
    uint64_t id = wl_evaluation_control_active_attempt_id(c), before;
    CHECK(!wl_evaluation_control_test_active_consumed(c, id, &before));
    if (injection_mode == 6) {
        CHECK(!maintenance && !governor_lowered);
        denied_site = admission_site(&key); CHECK(denied_site);
        saved_usable = atomic_load_explicit(&active_governor->usable_bytes,
                memory_order_acquire);
        reserved_at_floor = wl_columnar_memory_reserved(active_governor);
        CHECK(reserved_at_floor < saved_usable);
        atomic_store_explicit(&active_governor->usable_bytes, reserved_at_floor,
            memory_order_release);
        governor_lowered = true; governor_denials++;
    } else if (injection_mode == 1)wl_evaluation_control_request_cancel(c);
    else if (injection_mode == 2) {
        CHECK(!wl_evaluation_control_finish(c, owner, 0, 0));
        if (!maintenance) CHECK(!wl_evaluation_control_report(c, &frozen));
        CHECK(!wl_evaluation_control_reset(c));
        CHECK(!wl_evaluation_control_begin(c, owner));
        if (maintenance) cleanup_ended = true; else original_ended = true;
    } else {
        /* A real adversarial prefix leaves 255, 256, or zero work.  It does
         * not stand in for the source-derived normal action sequence. */
        uint64_t left = injection_mode == 3?255:injection_mode == 4?256:0;
        CHECK(!wl_evaluation_control_charge_attempt(c, id,
            UINT64_MAX-before-left));
    }
}
static int run(unsigned k, unsigned loaded, unsigned observed, unsigned ts) {
    wirelog_error_t error;
    wirelog_program_t *p = wirelog_parse_string(programs[k],
            &error); CHECK(p);
    wl_plan_t *plan = NULL;
    wirelog_extension_registry_t *registry = NULL;
    wirelog_extension_snapshot_t *snapshot = NULL;
    if (k == 6) {
        registry = wirelog_extension_registry_create(); CHECK(registry);
        uint32_t type = WIRELOG_EXTENSION_VALUE_INT64;
        wirelog_extension_descriptor_t descriptor =
        {.abi_version = WIRELOG_EXTENSION_ABI_VERSION,
         .size = sizeof(descriptor), .name = "seed.effect", .arity = 1,
         .argument_types = &type, .result_type = type, .invoke = effect};
        CHECK(!wirelog_extension_register(registry, &descriptor));
        snapshot = wirelog_extension_snapshot_acquire(registry);
        CHECK(snapshot);
        CHECK(!wl_plan_from_program_with_snapshot(p, snapshot, &plan));
    } else CHECK(!wl_plan_from_program(p, &plan));
    wl_columnar_memory_resolution_t resolution =
    {.budget_bytes = UINT64_C(128)<<20,
            .usable_bytes = UINT64_C(128)<<20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(g);
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.memory_governor = g;
    wl_session_t *raw = NULL, *physical = NULL;
    CHECK(!wl_session_create_with_snapshot_options(wl_backend_columnar(),
        plan, 1, snapshot, &options, &raw));
    CHECK(!wl_session_create_with_snapshot_options(wl_backend_columnar(),
        plan, 1, snapshot, &options, &physical));
    if (loaded) CHECK(!wl_session_load_facts(raw,
            p) && !wl_session_load_facts(physical, p));
    if (observed) {
        wl_session_set_delta_cb(raw, cb, NULL);
        wl_session_set_delta_cb(physical, cb, NULL);
    }
    for (unsigned i = 0; i < extra_input; i++) {
        int64_t row = (int64_t)i+10;
        CHECK(!wl_session_insert(raw, "input", &row, 1, 1));
        CHECK(!wl_session_insert(physical, "input", &row, 1, 1));
    }
    for (unsigned i = 0; i < prior_steps; i++) {
        CHECK(!wl_session_step(raw)); CHECK(!wl_session_step(physical));
    }

    if (prior_steps) {
        col_rel_t *input = session_find_rel(COL_SESSION(physical), "input");
        CHECK(input && input->nrows);
        int64_t row = input->columns[0][0];
        CHECK(!wl_session_insert(raw, "input", &row, 1, 1));
        CHECK(!wl_session_insert(physical, "input", &row, 1, 1));
        if (prior_steps > 1) {
            CHECK(!wl_session_remove(raw, "input", &row, 1, 1));
            CHECK(!wl_session_remove(physical, "input", &row, 1, 1));
        }
    }
    col_rel_t *t = session_find_rel(COL_SESSION(physical),
            "out"),
        *rt = session_find_rel(COL_SESSION(raw), "out");
    CHECK(t && rt);
    col_rel_t *owned_target = NULL, *owned_peer = NULL;
    if (untyped_target) {
        CHECK(!wl_columnar_relation_alloc_governed(&owned_target, "out", g));
        CHECK(!col_rel_set_schema(owned_target, t->ncols,
            (const char *const *)t->col_names));
        for (uint32_t row = 0; row < t->nrows; row++) {
            int64_t values[1]; CHECK(t->ncols == 1);
            col_rel_row_copy_out(t, row, values);
            CHECK(!col_rel_append_row(owned_target, values));
        }
        CHECK(!owned_target->column_types);
        CHECK(!wl_columnar_relation_alloc_governed(&owned_peer, "out", g));
        CHECK(!col_rel_set_schema(owned_peer, rt->ncols,
            (const char *const *)rt->col_names));
        for (uint32_t row = 0; row < rt->nrows; row++) {
            int64_t values[1]; CHECK(rt->ncols == 1);
            col_rel_row_copy_out(rt, row, values);
            CHECK(!col_rel_append_row(owned_peer, values));
        }
        CHECK(!owned_peer->column_types); t = owned_target; rt = owned_peer;
    }

    if (ts && t->ncols){
        CHECK(!col_rel_enable_timestamps(t) &&
            !col_rel_enable_timestamps(rt)); for (uint32_t r =
            0;
            r < t->nrows;
            r++) t->timestamps[r] = rt->timestamps[r] =
                (col_delta_timestamp_t){.iteration = 42,
                                        .multiplicity = 1};
    }
    col_rel_t *original_image = NULL, *peer_image = NULL;
    const char *saved_format = NULL;
    if (malformed_format) {
        CHECK(t->schema_ok && t->schema.n_children);
        saved_format = t->schema.children[0]->format;
        t->schema.children[0]->format = malformed_format ==
            1?NULL:malformed_format == 2?"":"ll";
    }
    if (ts && t->timestamps)for (uint32_t r = t->nrows;
            r < t->timestamp_capacity; r++)
            t->timestamps[r] = rt->timestamps[r] =
                (col_delta_timestamp_t){.iteration = 777+r, .multiplicity = 3};
    if (injection_mode == 6) {
        CHECK(!col_rel_deep_copy(t, &original_image, NULL));
        CHECK(!col_rel_deep_copy(rt, &peer_image, NULL));
    }
    col_relation_snapshot_t before =
        wl_columnar_relation_snapshot(t);
    wl_evaluation_control_t *c = begin(&operation_owner),
        *cleanup = begin(&cleanup_owner);
    wl_columnar_session_seed_t *a =
        malloc(wl_columnar_session_seed_size()); CHECK(a);
    CHECK(!wl_columnar_session_seed_init(a, c,
        wl_evaluation_control_active_attempt_id(c), g));
    wl_columnar_session_seed_recipe_t recipe =
    {.reset =
         observed?WL_COLUMNAR_SESSION_SEED_DETACH_OBSERVED:
         (prior_steps ? WL_COLUMNAR_SESSION_SEED_RESET_PLAIN :
         WL_COLUMNAR_SESSION_SEED_KEEP_INITIAL), .name = "out",
     .target_snapshot = before};
    active_original = c; active_cleanup = cleanup;
    active_governor = wl_columnar_memory_governor_ref_get(g);
    wl_columnar_session_seed_test_checkpoint = checkpoint;
    wl_columnar_relation_test_constructor_fail_allocation = allocation_failure;
    effect_calls = 0;
    int rc = wl_columnar_session_seed_prepare(a, t,
            wl_columnar_session_seed_rows(COL_SESSION(physical),
            "out"), &recipe);

    if (!rc) rc = forward(a, cleanup);
    eval_stack_t stack = {0};
    if (!rc) {
        uint64_t v[16]; CHECK(!wl_columnar_session_seed_test_view(a, v));
        col_rel_t *seed = wl_columnar_session_seed_rows(COL_SESSION(physical),
                "out");
        CHECK(v[8] ==
            (recipe.reset ==
            WL_COLUMNAR_SESSION_SEED_KEEP_INITIAL?t->nrows:
            (seed?seed->nrows:0)));
        CHECK(!COL_SESSION(physical)->cleanup_active);
    }
    int rule_rc = rc ? rc :
        col_eval_relation_plan(&plan->strata[0].relations[0],
            &stack, COL_SESSION(physical));
    eval_entry_t result = {0}; if (!rule_rc){
        CHECK(stack.top == 1); result = eval_stack_pop(&stack);
        CHECK(result.rel);
    }
    CHECK(!eval_stack_drain(&stack));
    if (!rule_rc){
        rc = wl_columnar_session_seed_attach_rule(a,
                result.rel); if (!rc) rc = forward(a, cleanup);
    }else rc = rule_rc;
    if (injection_mode == 6) {
        CHECK(injected && governor_lowered && denied_site && rc == ENOSPC);
        CHECK(!wl_evaluation_control_charge_attempt(c,
            wl_evaluation_control_active_attempt_id(c), 0));
        uint64_t actual[8];
        CHECK(!wl_columnar_session_seed_test_admission(a, denied_site, actual));
        CHECK(actual[0] == WL_COLUMNAR_MEMORY_ADMISSION_DENIED && actual[1]);
        CHECK(actual[2] == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED ||
            actual[2] == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY ||
            actual[2] == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
        if (actual[2] ==
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED) CHECK(actual[3] &&
                actual[7]);
        else CHECK(!actual[3]);
        CHECK(actual[4] == WL_COLUMNAR_MEMORY_GOVERNOR_GROWTH_COMMIT_INERT &&
            actual[5] == WL_COLUMNAR_MEMORY_GROWTH_REFUND_INERT &&
            actual[6] == WL_COLUMNAR_MEMORY_DOWNSIZE_INERT);
        CHECK(wl_columnar_memory_reserved(active_governor) ==
            reserved_at_floor);
        equal(t, original_image); equal(rt, peer_image);
        if (denied_site == WL_COLUMNAR_SESSION_SEED_TEST_RESTORE) {
            CHECK(!result.rel);
            if (k == 6) {
                CHECK(extra_input == 1100 && !effect_calls);
                saw_restore_denial = true;
            }
        }
        if (denied_site == WL_COLUMNAR_SESSION_SEED_TEST_RESIZE &&
            cuts[selected_cut].state[0] == 7) {
            CHECK(result.rel);
            if (k == 6) {
                CHECK(extra_input == 1100 && effect_calls == 1101);
                saw_append_growth_denial = true;
            }
        }
    }
    if (governor_lowered) {
        atomic_store_explicit(&active_governor->usable_bytes, saved_usable,
            memory_order_release); governor_lowered = false;
        CHECK(rc == ENOSPC);
    }
    if (k == 6) {
        if (!result.rel) {
            CHECK(!effect_calls); if (rc) pre_rule_denials++;
        }else {
            CHECK(effect_calls == extra_input+1); if (rc) post_rule_denials++;
        }
    }
    int raw_rc = 0;
    if (untyped_target && !result.rel) CHECK(rc);
    if (injection_mode != 6 && (!untyped_target || result.rel))
        raw_rc = raw_reference(raw, rt, result.rel);
    if (!injection_mode && !allocation_failure &&
        !malformed_format) CHECK(rc == raw_rc);
    if (malformed_format)CHECK(rc == ENOTSUP);
    col_rel_t *out = NULL; if (!rc){
        bool ready = false; for (unsigned i = 0;
            i < 500000 && !ready;
            i++){
            rc = wl_columnar_session_seed_finalize(a,
                    cleanup, wl_evaluation_control_active_attempt_id(cleanup),
                    &ready); if (rc) break;
        }
        if (!rc){
            CHECK(ready); rc = wl_columnar_session_seed_take(a, &out);
        }
        if (!rc){
            CHECK(out);
            /* Raw STEP has a separate compaction boundary; compare like boundaries. */
            CHECK(!col_rel_compact(out));
            equal(rt, out);
        }
    }
    CHECK(wl_columnar_relation_snapshot_equal(before,
        wl_columnar_relation_snapshot(t)));
    wl_columnar_relation_test_constructor_fail_allocation = 0;
    wl_columnar_session_seed_test_checkpoint = NULL;
    if (injection_mode){
        CHECK(injected); CHECK(rc || injection_mode == 4);
        if (injection_mode == 5)CHECK(rc == EOVERFLOW);
    }
    if (allocation_failure && rc) CHECK(rc == ENOMEM);
    if (cleanup_ended){
        int stopped = wl_evaluation_control_finish(cleanup, &cleanup_owner, 0,
                0);
        CHECK(stopped == 0 || stopped == WL_EVALUATION_CONTROL_CANCELLED ||
            stopped == WL_EVALUATION_CONTROL_WORK_LIMIT ||
            stopped == EOVERFLOW);
        CHECK(!wl_evaluation_control_detach(cleanup, &cleanup_owner));
        wl_evaluation_control_release(cleanup); cleanup = begin(&cleanup_owner);
    }
    if (!original_ended) CHECK(!wl_evaluation_control_test_active_consumed(c,
            wl_evaluation_control_active_attempt_id(c),
            &original_before_cleanup));
    end(a, cleanup, rc);
    bool done_again = false;
    CHECK(!wl_columnar_session_seed_end(a, rc, cleanup,
        wl_evaluation_control_active_attempt_id(cleanup),
        &done_again) && done_again);
    if (!original_ended) {
        uint64_t after;
        CHECK(!wl_evaluation_control_test_active_consumed(c,
            wl_evaluation_control_active_attempt_id(c), &after));
        CHECK(after == original_before_cleanup);
    }
    if (injection_mode == 6) {
        wl_evaluation_control_t *retry = begin(&operation_owner);
        CHECK(!wl_columnar_session_seed_init(a, retry,
            wl_evaluation_control_active_attempt_id(retry), g));
        CHECK(!wl_columnar_session_seed_prepare(a, t,
            wl_columnar_session_seed_rows(COL_SESSION(physical), "out"),
            &recipe));
        CHECK(!forward(a, cleanup));
        if (!result.rel) {
            eval_stack_t retry_stack = {0};
            CHECK(!col_eval_relation_plan(&plan->strata[0].relations[0],
                &retry_stack, COL_SESSION(physical)));
            CHECK(retry_stack.top == 1); result = eval_stack_pop(&retry_stack);
            CHECK(result.rel); CHECK(!eval_stack_drain(&retry_stack));
        }
        CHECK(!wl_columnar_session_seed_attach_rule(a, result.rel));
        CHECK(!forward(a, cleanup));
        bool ready = false;
        for (unsigned i = 0; i < 500000 && !ready;
            i++) CHECK(!wl_columnar_session_seed_finalize(a, cleanup,
                wl_evaluation_control_active_attempt_id(cleanup), &ready));
        CHECK(ready);
        CHECK(!wl_columnar_session_seed_take(a, &out) && out);
        end(a, cleanup, 0); finish(retry, &operation_owner, 0);
        CHECK(!raw_reference(raw, rt, result.rel));
        CHECK(!col_rel_compact(out));
        equal(rt, out);
        equal(t, original_image); raised_retries++;
    }
    if (original_image)CHECK(!col_rel_destroy_checked(original_image));
    if (peer_image)CHECK(!col_rel_destroy_checked(peer_image));
    wl_columnar_source_access_writer_t writer = {0};
    CHECK(!wl_columnar_source_access_writer_acquire(&t->descriptor_access,
        &writer)); CHECK(!wl_columnar_source_access_writer_release(&writer));
    CHECK(!wl_columnar_source_access_writer_acquire(&t->source_access,
        &writer)); CHECK(!wl_columnar_source_access_writer_release(&writer));
    free(a);
    if (malformed_format)t->schema.children[0]->format = saved_format;
    if (original_ended){
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(c, &report));
        CHECK(report.attempt_id == frozen.attempt_id &&
            report.limit == frozen.limit &&
            report.consumed == frozen.consumed &&
            report.stop_reason == frozen.stop_reason &&
            report.execution_status == frozen.execution_status &&
            report.cleanup_status == frozen.cleanup_status &&
            report.result == frozen.result);
        CHECK(!wl_evaluation_control_test_active_consumed(c,
            wl_evaluation_control_active_attempt_id(c), &successor_consumed));
        CHECK(!successor_consumed);
    }

    if (out) CHECK(!col_rel_destroy_checked(out));
    if (result.owned) CHECK(!col_rel_destroy_checked(
                result.rel));
    finish(c, &operation_owner,
        (original_ended || (cleanup_ended && rc < 0)) ? 0 : rc);
    finish(cleanup, &cleanup_owner, 0);
    if (owned_target)CHECK(!col_rel_destroy_checked(owned_target));
    if (owned_peer) CHECK(!col_rel_destroy_checked(owned_peer));
    CHECK(!wl_session_try_destroy(&raw) &&
        !wl_session_try_destroy(&physical));
    wl_plan_free(plan); wirelog_program_free(p);
    if (snapshot)wirelog_extension_snapshot_release(snapshot);
    if (registry) {
        CHECK(!wirelog_extension_unregister(registry, "seed.effect"));
        CHECK(!wirelog_extension_registry_destroy(registry));
    }
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
    return rc;
}
static void matrix(void) {
    for (unsigned k = 0; k < 5; k++)for (unsigned loaded = 0; loaded < 2;
            loaded++)
            for (unsigned observed = 0; observed < 2;
                observed++) for (unsigned ts = 0;
                    ts < 2; ts++)
                    run(k, loaded, observed, ts);
}
static void histories(void) {
    prior_steps = 1;
    for (unsigned k = 0; k < 4; k++)for (unsigned observed = 0; observed < 2;
            observed++)
            for (unsigned ts = 0; ts < 2; ts++) run(k, 1, observed, ts);
    prior_steps = 2;
    for (unsigned observed = 0; observed < 2; observed++)run(1, 1, observed, 1);
    prior_steps = 0; extra_input = 70;
    for (unsigned observed = 0; observed < 2; observed++)for (unsigned ts = 0;
            ts < 2;
            ts++) run(5, 1, observed, ts);
    extra_input = 1100; run(5, 1, 1, 1); extra_input = 0;
    CHECK(saw_detached_ts && saw_zero_grid);
}
static void component_failures(unsigned fixture) {
    /* Select a real typed STRING source and observed timestamped history.
     * The baseline captures independent phase identities including metadata,
     * normalized output, B2 drain and physical retirement. */
    cut_count = 0; extra_input = fixture == 5?70:0; census_enabled = true;
    run(fixture, 1, 1, 1); census_enabled = false;
    size_t count = cut_count, replays = 0;
    for (size_t i = 0; i < count;
        i++) for (unsigned mode = 1; mode <= 5; mode++) {
            /* End itself is driven after the forward census hook is disabled. */
            if (fixture == 5 && cuts[i].phase != 1100 &&
                cuts[i].phase != 1101 && cuts[i].phase != 1102) continue;
            selected_cut = i; injection_mode = mode; injected = false;
            original_ended = cleanup_ended = false;
            replays++;
            run(fixture, 1, 1, 1);
        }
    for (size_t i = 0; i < count; i++) {
        const cut_t *c = &cuts[i];
        if (!admission_site(c)) continue;
        selected_cut = i; injection_mode = 6; injected = false;
        original_ended = cleanup_ended = false; run(fixture, 1, 1, 1);
    }
    injection_mode = 0;
    for (unsigned i = 1; i < 512; i++) {
        allocation_failure = i; int rc = run(fixture, 1, 1, 1);
        if (!rc) break;
        CHECK(i < 511);
    }
    allocation_failure = 0; extra_input = 0;
    fprintf(stderr,
        "physical seed fixture%u: %zu component identities, %zu fault replays and allocator prefixes passed\n",
        fixture, count, replays);
}
static void resource_order(void) {
    extra_input = 1100; pre_rule_denials = post_rule_denials = 0;
    for (unsigned i = 1; i < 128; i++) {
        allocation_failure = i;
        int rc = run(6, 1, 1, 0);
        if (!rc)break;
        CHECK(i < 127);
    }
    allocation_failure = 0; CHECK(pre_rule_denials && post_rule_denials);
    pre_rule_denials = post_rule_denials = 0; cut_count = 0;
    census_enabled = true; run(6, 1, 1, 0); census_enabled = false;
    size_t count = cut_count;
    for (size_t i = 0; i < count; i++) {
        if (!admission_site(&cuts[i]))continue;
        selected_cut = i; injection_mode = 6; injected = false;
        original_ended = cleanup_ended = false;
        run(6, 1, 1, 0);
    }
    injection_mode = 0; untyped_target = 1; extra_input = 0; cut_count = 0;
    census_enabled = true; run(1, 1, 1, 0); census_enabled = false;
    count = cut_count; unsigned metadata_denials = 0, metadata_replays = 0;
    for (size_t i = 0; i < count; i++) {
        unsigned site = admission_site(&cuts[i]);
        if (site == WL_COLUMNAR_SESSION_SEED_TEST_TYPE_LENGTH ||
            site == WL_COLUMNAR_SESSION_SEED_TEST_TYPE_METADATA) {
            selected_cut = i; injection_mode = 6; injected = false;
            original_ended = cleanup_ended = false;
            run(1, 1, 1, 0); metadata_denials++;
        }
        if (cuts[i].phase < 1600 || cuts[i].phase > 1602)continue;
        for (unsigned mode = 1; mode <= 5; mode++) {
            selected_cut = i; injection_mode = mode; injected = false;
            original_ended = cleanup_ended = false;
            run(1, 1, 1, 0); metadata_replays++;
        }
    }
    CHECK(metadata_denials == 2 && metadata_replays);
    injection_mode = 0;
    for (unsigned i = 1; i < 128; i++) {
        allocation_failure = i; int rc = run(1, 1, 1, 0);
        if (!rc) {
            break;
        }
        CHECK(i < 127);
    }
    allocation_failure = 0;

    injection_mode = 0; untyped_target = 0; extra_input = 0;
    fprintf(stderr,
        "physical seed: %u actual governor denials, %u same-source retries, %u metadata fault replays passed\n",
        governor_denials, raised_retries, metadata_replays);
    CHECK(pre_rule_denials && post_rule_denials && saw_restore_denial &&
        saw_append_growth_denial);
    CHECK(governor_denials && raised_retries == governor_denials);
    fprintf(stderr,
        "physical seed: actual effect callbacks0/1101 across allocator prefixes passed\n");
}
static size_t init_position, init_selected;
static void init_checkpoint(const wl_columnar_session_seed_t *a, unsigned phase,
    unsigned stage) {
    (void)stage; CHECK(!a && (phase == 9000 || phase == 9001));
    if (init_position++ != init_selected)return;
    injected = true;
    uint64_t id = wl_evaluation_control_active_attempt_id(active_original),
        before;
    CHECK(!wl_evaluation_control_test_active_consumed(active_original, id,
        &before));
    if (injection_mode ==
        1) wl_evaluation_control_request_cancel(active_original);
    else if (injection_mode == 2) {
        CHECK(!wl_evaluation_control_finish(active_original, &operation_owner,
            0, 0));
        CHECK(!wl_evaluation_control_report(active_original, &frozen));
        CHECK(!wl_evaluation_control_reset(active_original));
        CHECK(!wl_evaluation_control_begin(active_original, &operation_owner));
        original_ended = true;
    } else {
        uint64_t left = injection_mode == 3?255:injection_mode == 4?256:0;
        CHECK(!wl_evaluation_control_charge_attempt(active_original, id,
            UINT64_MAX-before-left));
    }
}
static void initialization_failures(void) {
    size_t size = wl_columnar_session_seed_size(), actions = (size+7)/8+1;
    for (init_selected = 0; init_selected < actions;
        init_selected++) for (unsigned mode = 1;
            mode <= 5; mode++) {
            injection_mode = mode; injected = original_ended = false;
            init_position = 0;
            wl_evaluation_control_t *c = begin(&operation_owner);
            active_original = c;
            unsigned char *storage = malloc(size+8); CHECK(storage);
            memset(storage, 0xa5, size+8);
            wl_columnar_session_seed_test_checkpoint = init_checkpoint;
            int rc = wl_columnar_session_seed_init((void *)storage, c,
                    wl_evaluation_control_active_attempt_id(c), NULL);
            wl_columnar_session_seed_test_checkpoint = NULL; CHECK(injected);
            CHECK(rc || (mode == 4 && init_selected+1 == actions));
            for (unsigned i = 0; i < 8; i++) CHECK(storage[size+i] == 0xa5);
            if (!rc) {
                wl_evaluation_control_t *cleanup = begin(&cleanup_owner);
                end((void *)storage, cleanup, 0);
                finish(cleanup, &cleanup_owner, 0);
            }
            if (original_ended) {
                uint64_t consumed;
                CHECK(!wl_evaluation_control_test_active_consumed(c,
                    wl_evaluation_control_active_attempt_id(c),
                    &consumed) && !consumed);
            }
            finish(c, &operation_owner, original_ended?0:rc); free(storage);
        }
    injection_mode = 0;
    fprintf(stderr, "physical seed: %zu Init cuts x5 passed\n", actions);
}
int main(int argc, char **argv) {
    if (argc == 2 && !strcmp(argv[1], "--resource-only")) {
        resource_order(); return 0;
    }
    CHECK(argc == 1);
    matrix(); histories(); component_failures(1); component_failures(5);
    resource_order();
    for (malformed_format = 1; malformed_format <= 3;
        malformed_format++) run(1, 1, 1, 1);
    malformed_format = 0; initialization_failures(); return 0;
}
