/* Actual raw recursive scheduler parity against the pre-refactor baseline. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/intern.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/wirelog-extension.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); abort(); } } while (0)
static FILE *capture;
#define printf(...) fprintf(capture, __VA_ARGS__)
#define puts(s) fprintf(capture, "%s\n", (s))
enum { LENGTH = 13 };
static const char *program_text =
    ".decl seed(x:int64)\n.decl edge(x:int64,y:int64)\n"
    ".decl a(x:int64)\n.decl b(x:int64)\n"
    "a(x) :- seed(x).\na(y) :- b(x), edge(x,y).\nb(x) :- a(x).\n";
static int step(wl_session_t *session) {
    int result = wl_session_step(session);
    CHECK(result != EAGAIN); return result;
}
static wl_session_t *create(wl_plan_t *plan, wl_evaluation_control_t *control) {
    CHECK(control == NULL);
    wl_session_t *session = NULL;
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &session));
    int64_t seed = 0, edges[LENGTH * 2];
    for (unsigned i = 0; i < LENGTH; i++) {
        edges[2*i] = i; edges[2*i+1] = i+1;
    }
    CHECK(!wl_session_insert(session, "seed", &seed, 1, 1));
    CHECK(!wl_session_insert(session, "edge", edges, LENGTH, 2));
    return session;
}
static void trace_delta(const char *name, const int64_t *v, uint32_t n, int d,
    void *p){
    (void)p;
    printf("event %s %d %u", name, d, n);
    for (uint32_t i = 0; i < n; i++) {
        printf(" %016" PRIx64, (uint64_t)v[i]);
    }
    puts("");
}
static void trace_model(wl_session_t *s, int rc){
    wl_col_session_t *c = (wl_col_session_t *)s;
    printf(
        "status %d capped%d final%d iterations%u current%u pending%d stable%d evaluated%d nrels%u\n",
        rc, c->eval_stratum_iteration_capped, c->eval_stratum_heads_final,
        c->total_iterations, c->current_iteration, c->pending_input_change,
        c->snapshot_stable_valid, c->has_evaluated, c->nrels);
    for (uint32_t j = 0; j < c->nrels; j++) {
        const col_rel_t *r = c->rels[j]; if (!r) {
            puts("rel null"); continue;
        }
        printf("rel %s %u %u sorted%u gen%" PRIu64 "/%" PRIu64 " ts%d types",
            r->name, r->nrows, r->ncols, r->sorted_nrows, r->view_generation,
            r->storage_generation, r->timestamps != NULL);
        for (uint32_t k = 0; k < r->ncols;
            k++) printf(" %d",
                r->column_types?r->column_types[k]:WIRELOG_TYPE_INT64);
        puts("");
        for (uint32_t i = 0; i < r->nrows; i++) {
            printf("row");
            for (uint32_t k = 0; k < r->ncols;
                k++) printf(" %016" PRIx64, (uint64_t)r->columns[k][i]);
            if (r->timestamps) printf(" t%u/%u/%u/%" PRId64,
                    r->timestamps[i].iteration, r->timestamps[i].stratum,
                    r->timestamps[i].worker, r->timestamps[i].multiplicity);
            puts("");
        }
    }
}
static void shape(const wl_plan_t *p){
    for (uint32_t i = 0; i < p->stratum_count; i++) {
        printf("stratum%u recursive%d heads%u\n", i, p->strata[i].is_recursive,
            p->strata[i].relation_count);
        for (uint32_t j = 0; j < p->strata[i].relation_count; j++) {
            const wl_plan_relation_t *r = &p->strata[i].relations[j];
            printf("head %s ops", r->name);
            for (uint32_t k = 0; k < r->op_count; k++) {
                printf(" %d", r->ops[k].op);
            }
            puts("");
        }
    }
}

static void scalar_controls(void) {
    const char *programs[] = {
        ".decl input(x:int64)\n.decl out(x:int64)\ninput(123).\n"
        "out(strlen(to_string(x))+1):-input(x).\nout(x+1):-out(x),x<5.\n",
        ".decl input(x:int64)\n.decl out(x:int64)\ninput(0).\n"
        "out(10/x):-input(x).\nout(x):-out(x).\n",
        ".decl input(x:string)\n.decl out(x:string)\ninput(\"a\").\n"
        "out(x):-input(x).\nout(x):-out(x).\n",
        ".decl input(x:string)\n.decl out(x:string)\n"
        "out(x):-input(x).\nout(x):-out(x).\n"
    };
    for (unsigned kind = 0; kind < 4; kind++) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(programs[kind],
                &error);
        wl_plan_t *plan = NULL;
        CHECK(program && !wl_plan_from_program(program, &plan));
        wl_session_t *session = NULL;
        CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &session));
        CHECK(!wl_session_load_facts(session, program));
        if (kind == 2) {
            int64_t id = wl_intern_get(plan->intern, "a");
            CHECK(id >= 0 && !wl_session_remove(session, "input", &id, 1, 1));
        }
        wl_session_set_delta_cb(session, trace_delta, NULL);
        printf("CASE scalar%u\n", kind); shape(plan);
        int result = step(session); trace_model(session, result);
        CHECK(!wl_session_try_destroy(&session));
        wl_plan_free(plan); wirelog_program_free(program);
    }
}
static unsigned effect_calls, effect_destroyed;
static bool effect_fail;
static wl_session_t *effect_session;
static int effect_callback(const wirelog_extension_value_t *arguments,
    uint32_t count, wirelog_extension_value_t *result, void *context) {
    (void)context;
    CHECK(count == 1 && arguments[0].type == WIRELOG_EXTENSION_VALUE_INT64);
    wl_col_session_t *session = (wl_col_session_t *)effect_session;
    CHECK(session->callback_configured_workers == 1 &&
        session->callback_active_workers == 1
        && !session->callback_parallel_execution &&
        session->callback_session_key == session);
    effect_calls++;
    if (effect_fail)return -1;
    result->type = WIRELOG_EXTENSION_VALUE_INT64;
    result->size = sizeof(int64_t);
    result->as.int64_value = arguments[0].as.int64_value;
    return 0;
}
static void effect_destroy(void *context) {
    (void)context; effect_destroyed++;
}
static void effect_controls(void) {
    const char *source = ".decl input(x:int64)\n.decl out(x:int64)\ninput(1).\n"
        "out(@call(\"test.cursor\",x)):-input(x).\nout(x+1):-out(x),x<4.\n";
    for (unsigned failure = 0; failure < 2; failure++) {
        wirelog_extension_registry_t *registry =
            wirelog_extension_registry_create();
        CHECK(registry);
        uint32_t type = WIRELOG_EXTENSION_VALUE_INT64;
        wirelog_extension_descriptor_t descriptor = {
            .abi_version = WIRELOG_EXTENSION_ABI_VERSION,
            .size = sizeof(descriptor),
            .name = "test.cursor", .arity = 1, .argument_types = &type,
            .result_type = WIRELOG_EXTENSION_VALUE_INT64,
            .invoke = effect_callback,
            .destroy = effect_destroy
        };
        CHECK(!wirelog_extension_register(registry, &descriptor));
        wirelog_extension_snapshot_t *snapshot =
            wirelog_extension_snapshot_acquire(registry);
        CHECK(snapshot);
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(source, &error);
        wl_plan_t *plan = NULL;
        CHECK(program && !wl_plan_from_program_with_snapshot(program, snapshot,
            &plan));
        wl_session_t *session = NULL;
        CHECK(!wl_session_create_with_snapshot(wl_backend_columnar(), plan, 1,
            snapshot, &session)
            && !wl_session_load_facts(session, program));
        CHECK(!wirelog_extension_unregister(registry, "test.cursor"));
        wirelog_extension_snapshot_release(snapshot);
        effect_calls = effect_destroyed = 0; effect_fail = failure;
        effect_session = session;
        wl_session_set_delta_cb(session, trace_delta, NULL);
        printf("CASE effect failure%u\n", failure); shape(plan);
        int result = step(session); trace_model(session, result);
        CHECK(result == (failure?WL_COLUMNAR_EXPR_CALLBACK_FAILURE:0));
        printf("calls %u destroyed%u\n", effect_calls, effect_destroyed);
        effect_fail = false; result = step(session); puts("retry");
        trace_model(session, result);
        CHECK(!result);
        printf("calls %u destroyed%u\n", effect_calls, effect_destroyed);
        CHECK(!wl_session_try_destroy(&session)); effect_session = NULL;
        wl_plan_free(plan); wirelog_program_free(program);
        printf("destroyed%u\n", effect_destroyed);
        CHECK(effect_destroyed == 1 &&
            !wirelog_extension_registry_destroy(registry));
    }
}
int main(int argc, char **argv){
    CHECK(argc == 2); capture = tmpfile(); CHECK(capture);
    /* Override0 selects the actual default iteration limit. */
    for (unsigned limit = 0; limit < 3; limit++) for (unsigned observed = 0;
            observed < 2; observed++){
            wirelog_error_t e;
            wirelog_program_t *p = wirelog_parse_string(program_text, &e);
            wl_plan_t *plan = NULL; CHECK(p && !wl_plan_from_program(p, &plan));
            printf("CASE multi cap%u observed%u\n", limit, observed);
            shape(plan); wl_session_t *s = create(plan, NULL);
            if (observed) wl_session_set_delta_cb(s, trace_delta, NULL);
            wl_columnar_eval_serial_test_iteration_limit = limit;
            int rc = step(s); trace_model(s, rc);
            wl_columnar_eval_serial_test_iteration_limit = 0; rc = step(s);
            puts("retry"); trace_model(s, rc);
            CHECK(!wl_session_try_destroy(&s)); wl_plan_free(plan);
            wirelog_program_free(p);
        }
    const char *types[] = {"int64", "string", "float"};
    const char *facts[] = {"input(1). input(2).", "input(\"a\"). input(\"b\").",
                           "input(-0.0). input(1.5)."};
    for (unsigned t = 0; t < 3; t++) for (unsigned observed = 0; observed < 2;
            observed++){
            char source[1024];
            snprintf(source, sizeof(source),
                ".decl input(x:%s)\n.decl out(x:%s)\n%s\nout(x):-input(x).\nout(x):-out(x).\n",
                types[t], types[t], facts[t]);
            wirelog_error_t e;
            wirelog_program_t *p = wirelog_parse_string(source, &e);
            wl_plan_t *plan = NULL; CHECK(p && !wl_plan_from_program(p, &plan));
            wl_session_t *s = NULL;
            CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &s));
            CHECK(!wl_session_load_facts(s, p));
            if (observed) wl_session_set_delta_cb(s, trace_delta, NULL);
            printf("CASE typed %s observed%u\n", types[t], observed);
            shape(plan); int rc = step(s); trace_model(s, rc); CHECK(!rc);
            CHECK(!wl_session_try_destroy(&s)); wl_plan_free(plan);
            wirelog_program_free(p);
        }

    const char *ktext =
        ".decl e(x:int64,y:int64)\n.decl tc(x:int64,y:int64)\ne(1,2).e(2,3).e(3,4).e(4,5).\ntc(x,y):-e(x,y).\ntc(x,z):-tc(x,y),tc(y,z).\n";
    for (unsigned cap = 0; cap < 3; cap++) for (unsigned observed = 0;
            observed < 2; observed++){
            wirelog_error_t e;
            wirelog_program_t *p = wirelog_parse_string(ktext, &e);
            CHECK(p && !wl_fusion_apply(p, NULL) && !wl_jpp_apply(p,
                NULL) && !wl_sip_apply(p, NULL)); wl_plan_t *plan = NULL;
            CHECK(!wl_plan_from_program(p, &plan));
            bool k = false;
            for (uint32_t a = 0; a < plan->stratum_count; a++) for (uint32_t b =
                    0;
                    b < plan->strata[a].relation_count; b++) for (uint32_t c =
                        0;
                        c < plan->strata[a].relations[b].op_count;
                        c++) k |= plan->strata[a].relations[b].ops[c].op ==
                            WL_PLAN_OP_K_FUSION; CHECK(k);
            printf("CASE K cap%u observed%u\n", cap, observed); shape(plan);
            wl_session_t *s = NULL;
            CHECK(!wl_session_create(wl_backend_columnar(), plan, 1,
                &s) && !wl_session_load_facts(s, p));
            if (observed) wl_session_set_delta_cb(s, trace_delta, NULL);
            wl_columnar_eval_serial_test_iteration_limit = cap;
            int rc = step(s); trace_model(s, rc);
            wl_columnar_eval_serial_test_iteration_limit = 0; rc = step(s);
            puts("retry"); trace_model(s, rc);
            CHECK(!wl_session_try_destroy(&s)); wl_plan_free(plan);
            wirelog_program_free(p);
        }
    scalar_controls();
    effect_controls();
    CHECK(!fflush(capture)); rewind(capture);
    /* Text mode folds a CRLF checkout (core.autocrlf on Windows) to LF. */
    FILE *expected = fopen(argv[1], "r"); CHECK(expected);
    size_t offset = 0;
    for (;;) {
        int actual = fgetc(capture), reference = fgetc(expected);
        if (actual != reference) {
            fprintf(stderr, "raw cursor baseline differs at byte%zu: %d/%d\n",
                offset, actual, reference); abort();
        }
        if (actual == EOF) break;
        offset++;
    }
    CHECK(!ferror(capture) && !ferror(expected));
    CHECK(!fclose(expected) && !fclose(capture));
    fprintf(stdout,
        "raw recursive cursor: 24 compiler/session baseline cases passed\n");
    return 0;
}
