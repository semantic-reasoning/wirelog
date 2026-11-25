/* Exact long-name delta identity in real raw and materialized consumers. */
#ifndef _WIN32
#define _POSIX_C_SOURCE 200809L
#endif
#include "wirelog/columnar/internal.h"
#include "wirelog/exec_plan_gen.h"
#include "wirelog/session.h"
#include "wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)
extern void (*wl_columnar_kfusion_test_clone)(wl_col_session_t *,
    wl_col_session_t *, bool);
static unsigned clones;
static void clone_seen(wl_col_session_t *parent, wl_col_session_t *child,
    bool before) {
    (void)parent; (void)child; if (before) ++clones;
}
static void name_fill(char *name, size_t length) {
    memset(name, 'a', length); name[length] = '\0';
}
static col_rel_t *relation(const char *name, uint32_t width,
    const int64_t *rows, uint32_t count) {
    col_rel_t *r = col_rel_new_auto(name, width); CHECK(r);
    for (uint32_t i = 0; i < count; ++i)
        CHECK(!col_rel_append_row(r, rows + (size_t)i * width));
    return r;
}

static void hash_lookup(void) {
    char name[301], full[304], sibling[304], decoy[256], retract[304];
    name_fill(name, 300); snprintf(full, sizeof(full), "$d$%s", name);
    memcpy(sibling, full, sizeof(full)); sibling[302] = 'b';
    memcpy(decoy, full, 255); decoy[255] = '\0';
    snprintf(retract, sizeof(retract), "$r$%s", name);
    const int64_t row = 37;
    col_rel_t *registry[] = {NULL, relation("$", 1, &row, 1),
                             relation(decoy, 1, &row, 1),
                             relation(sibling, 1, &row, 1),
                             relation(full, 1, &row, 1),
                             relation(retract, 1, &row, 1),
                             relation("$d$a\xc3\xa9", 1, &row, 1)};
    wl_col_session_t s = {0}; s.rels = registry;
    s.nrels = s.rel_cap = sizeof(registry) / sizeof(registry[0]);
    CHECK(!s.rel_hash_nbuckets);
    CHECK(wl_columnar_session_find_prefixed(&s, "$d$", name) == registry[4]);
    CHECK(s.rel_hash_nbuckets && session_find_rel(&s, full) == registry[4]);
    CHECK(wl_columnar_session_hash_lookup_prefixed(&s, "$r$",
        name) == registry[5]);
    CHECK(wl_columnar_session_hash_lookup_prefixed(&s, "$d$",
        "a\xc3\xa9") == registry[6]);
    CHECK(!wl_columnar_session_find_prefixed(&s, "$d$", "absent"));
    /* A direct registry builder fills a previously NULL slot after the
     * index was built. This is a built-hash miss with a real live registry
     * entry, not a modified bucket or a natural session-registration claim. */
    col_rel_t *saved = registry[4]; registry[4] = NULL;
    CHECK(!session_rel_build_hash(&s)); registry[4] = saved;
    CHECK(!session_rel_hash_lookup(&s, full));
    CHECK(wl_columnar_session_find_prefixed(&s, "$d$", name) == saved);
    CHECK(!session_rel_build_hash(&s));
    CHECK(wl_columnar_session_hash_lookup_prefixed(&s, "$d$",
        name) == registry[4]);
    session_rel_free_hash(&s);
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(64) << 20, .usable_bytes = UINT64_C(64) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref); s.memory_governor = ref;
    wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(ref);
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer);
    CHECK(wl_columnar_memory_reserve_checked(g, resolution.usable_bytes - 1,
        &peer)
        == WL_COLUMNAR_MEMORY_ADMISSION_OK);
    uint64_t denials = wl_columnar_memory_denials(g);
    CHECK(wl_columnar_session_find_prefixed(&s, "$d$", name) == registry[4]
        && !s.rel_hash_nbuckets && wl_columnar_memory_denials(g) == denials + 1
        && !s.memory_budget_denied);
    CHECK(wl_columnar_memory_rollback(&peer) &&
        !wl_columnar_memory_reserved(g));
    s.memory_governor = NULL; wl_columnar_memory_governor_ref_release(ref);
    for (unsigned i = 1; i < sizeof(registry)/sizeof(registry[0]); ++i)
        col_rel_destroy(registry[i]);
    puts(
        "exact prefixed lookup: lazy/built/hash-miss/holes/UTF8/siblings passed");
}

typedef struct { const char *name; unsigned seen[16][16], count; } image_t;
static void capture(const char *name, const int64_t *row, uint32_t width,
    void *data) {
    image_t *image = data; if (strcmp(name, image->name)) return;
    CHECK(width == 2 && row[0] >= 0 && row[0] < 16 && row[1] >= 0 &&
        row[1] < 16);
    CHECK(!image->seen[row[0]][row[1]]++); ++image->count;
}
static void natural(size_t length, unsigned k, bool controlled) {
    char name[301], text[8192]; name_fill(name, length);
    size_t used = 0;
#define ADD(...) do { int n = snprintf(text + used, sizeof(text) - used, \
                              __VA_ARGS__); \
                      CHECK(n > 0 && (size_t)n < sizeof(text) - used); \
                      used += (size_t)n; } while (0)
    ADD(".decl edge(x:int64,y:int64)\n.decl %s(x:int64,y:int64)\n", name);
    ADD("%s(x,y):-edge(x,y).\n%s(a,z):-", name, name);
    const char *three[] = {"a,b", "b,c", "c,z"};
    const char *four[] = {"a,b", "b,c", "c,d", "d,z"};
    for (unsigned i = 0; i < k; ++i)
        ADD("%s%s(%s)", i ? "," : "", name, k == 3 ? three[i] : four[i]);
    ADD(".\n");
#undef ADD
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    unsigned found = 0, hints = 0;
    for (uint32_t s = 0; s < plan->stratum_count; ++s)
        for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r) {
            const wl_plan_relation_t *rp = &plan->strata[s].relations[r];
            if (strcmp(rp->name, name)) continue;
            for (uint32_t o = 0; o < rp->op_count; ++o)
                if (rp->ops[o].op == WL_PLAN_OP_K_FUSION) {
                    const wl_plan_op_k_fusion_t *meta = rp->ops[o].opaque_data;
                    CHECK(meta && meta->k == k); ++found;
                    for (uint32_t b = 0; b < meta->k; ++b)
                        for (uint32_t i = 0; i < meta->k_op_counts[b]; ++i)
                            hints += meta->k_ops[b][i].materialized ? 1u : 0u;
                }
        }
    CHECK(found == 1 && hints);
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(256) << 20,
            .usable_bytes = UINT64_C(256) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref); wl_evaluation_control_t *control = NULL;
    if (controlled) CHECK(!wl_evaluation_control_create(UINT64_C(100000000),
            &control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.memory_governor = ref; options.evaluation_control = control;
    wl_session_t *session = NULL; unsigned workers = k == 4 ? 4 : 1;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, workers,
        &options, &session));
    unsigned edges = k == 3 ? 6 : 12; int64_t rows[24];
    for (unsigned i = 0; i < edges; ++i) {
        rows[2*i] = i; rows[2*i+1] = i+1;
    }
    CHECK(!wl_session_insert(session, "edge", rows, edges, 2));
    clones = 0; wl_columnar_kfusion_test_clone = clone_seen;
    for (unsigned phase = 0; phase < 5; ++phase) {
        if (phase == 1) {
            int64_t row[] = {edges, edges+1};
            CHECK(!wl_session_insert(session, "edge", row, 1, 2));
        }
        if (phase == 3) {
            const int64_t row[] = {2, 3};
            CHECK(!wl_session_remove(session, "edge", row, 1, 2));
        }
        image_t image = {.name = name}; int rc; unsigned calls = 0;
        do {
            CHECK(++calls < 1000000); memset(image.seen, 0, sizeof(image.seen));
            image.count = 0; rc = wl_session_snapshot(session, capture, &image);
            if (rc == EAGAIN) CHECK(!image.count);
        } while (rc == EAGAIN);
        CHECK(!rc);
        unsigned end = edges + (phase != 0), expected = 0;
        for (unsigned a = 0; a <= end; ++a)
            for (unsigned b = 0; b <= end; ++b) {
                bool wanted = a < b && (b-a) % (k-1) == 1
                    && !(phase >= 3 && a <= 2 && b >= 3);
                CHECK(image.seen[a][b] == (wanted ? 1u : 0u));
                expected += wanted;
            }
        CHECK(image.count == expected);
        if (control) CHECK(!wl_evaluation_control_reset(control));
    }
    wl_columnar_kfusion_test_clone = NULL;
    if (k == 4 && !controlled) CHECK(clones > 0);
    printf(
        "natural name%zu K%u controlled%u: initial/append/repeat/remove/repeat, clones=%u\n",
        length, k, controlled, clones);
    wl_session_destroy(session);
    if (control) wl_evaluation_control_release(control);
    wl_plan_free(plan); wirelog_program_free(program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            ref)));
    wl_columnar_memory_governor_ref_release(ref);
}

static void check_rows(const col_rel_t *r, bool delta) {
    CHECK(r && r->ncols == 4 && r->nrows == (delta ? 1u : 2u));
    unsigned seen = 0;
    for (uint32_t i = 0; i < r->nrows; ++i) {
        int64_t key = col_rel_get(r, i, 0);
        CHECK((key == 8 || (!delta && key == 9)) && col_rel_get(r, i,
            1) == key*10
            && col_rel_get(r, i, 2) == key && col_rel_get(r, i,
            3) == (delta ? 800 : key*10));
        unsigned bit = key == 8 ? 1u : 2u; CHECK(!(seen & bit)); seen |= bit;
    }
    CHECK(seen == (delta ? 1u : 3u));
}
static void cache_consumer(bool diff) {
    char name[301], delta_name[304], decoy_name[256]; name_fill(name, 300);
    snprintf(delta_name, sizeof(delta_name), "$d$%s", name);
    memcpy(decoy_name, delta_name, 255); decoy_name[255] = '\0';
    wirelog_error_t error; wirelog_program_t *program = wirelog_parse_string(
        ".decl seed(x:int64)\n.decl out(x:int64)\nout(x):-seed(x).\n", &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(256) << 20,
            .usable_bytes = UINT64_C(256) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref); wl_session_options_t options; wl_session_options_init(&options);
    options.memory_governor = ref; wl_session_t *base = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &base));
    wl_col_session_t *s = COL_SESSION(base);
    const int64_t left_values[] = {8, 80, 9, 90},
        full_values[] = {7, 70, 8, 80, 9, 90};
    const int64_t delta_values[] = {8, 800}, decoy_values[] = {77, 777};
    col_rel_t *left = relation("left", 2, left_values, 2);
    col_rel_t *full = relation(name, 2, full_values, 3);
    col_rel_t *delta = relation(delta_name, 2, delta_values, 1);
    col_rel_t *decoy = relation(decoy_name, 2, decoy_values, 1);
    CHECK(!session_add_rel(s, full) && !session_add_rel(s,
        delta) && !session_add_rel(s, decoy));
    const char *keys[] = {"col0"};
    wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = name,
                       .key_count = 1, .left_keys = keys, .right_keys = keys,
                       .materialized = true};
    uint64_t hits = s->mat_cache.hits; s->current_iteration = 1;
    const bool selected_delta[] = {false, false, true, true, false, true,
                                   false};
    for (unsigned pass = 0; pass < 7; ++pass) {
        bool choose = selected_delta[pass];
        bool left_delta = pass == 6;
        op.delta_mode = pass >= 5 ? WL_DELTA_AUTO
            : choose ? WL_DELTA_FORCE_DELTA : WL_DELTA_FORCE_FULL;
        wl_columnar_join_selection_t selection =
            wl_columnar_join_select_right(&op, s, full, left_delta);
        CHECK(selection.right == (choose ? delta : full) &&
            selection.used_right_delta == choose
            && !selection.force_empty);
        eval_stack_t stack; eval_stack_init(&stack);
        CHECK(!eval_stack_push_delta(&stack, left, false, left_delta));
        CHECK(!(diff ? col_op_join_diff(&op, &stack, s) : col_op_join(&op,
            &stack, s)));
        eval_entry_t result = eval_stack_pop(&stack);
        CHECK(result.owned && result.is_delta == (choose || left_delta) &&
            result.rel->memory_governor == ref);
        check_rows(result.rel, choose); col_rel_destroy(result.rel);
        CHECK(!eval_stack_drain(&stack));
        if (pass == 1 || pass >= 3) ++hits;
        CHECK(s->mat_cache.hits == hits && !s->mat_cache.active_pins);
    }
    wl_plan_op_t variable = {.op = WL_PLAN_OP_VARIABLE, .relation_name = name,
                             .delta_mode = WL_DELTA_FORCE_DELTA};
    wl_plan_relation_t body = {.name = "direct", .ops = &variable,
                               .op_count = 1};
    CHECK(!has_empty_forced_delta(&body, s, 1));
    eval_stack_t stack; eval_stack_init(&stack);
    CHECK(!col_op_variable(&variable, &stack, s));
    eval_entry_t selected = eval_stack_pop(&stack);
    CHECK(selected.rel == delta && !selected.owned && selected.is_delta);
    /* Leave only the genuine prefix decoy: a seeded miss must be empty. */
    CHECK(!session_remove_rel(s, delta_name));
    s->delta_seeded = true;
    CHECK(has_empty_forced_delta(&body, s, 1));
    CHECK(!col_op_variable(&variable, &stack, s));
    selected = eval_stack_pop(&stack);
    CHECK(selected.owned && selected.is_delta && !selected.rel->nrows);
    col_rel_destroy(selected.rel); CHECK(!eval_stack_drain(&stack));
    col_rel_t *empty = relation(delta_name, 2, NULL, 0);
    CHECK(!session_add_rel(s, empty) && has_empty_forced_delta(&body, s, 1));
    CHECK(!col_op_variable(&variable, &stack, s));
    selected = eval_stack_pop(&stack);
    CHECK(selected.owned && selected.is_delta && !selected.rel->nrows);
    col_rel_destroy(selected.rel);
    op.delta_mode = WL_DELTA_AUTO;
    wl_columnar_join_selection_t selection = wl_columnar_join_select_right(&op,
            s, full, false);
    CHECK(selection.right == full && !selection.used_right_delta &&
        !selection.force_empty);
    char retract_name[304];
    snprintf(retract_name, sizeof(retract_name), "$r$%s", name);
    const int64_t removed[] = {9, 900};
    col_rel_t *retracted = relation(retract_name, 2, removed, 1);
    CHECK(!session_add_rel(s, retracted));
    CHECK(!session_remove_rel(s, delta_name));
    s->retraction_seeded = true; s->current_iteration = 0;
    CHECK(!has_empty_forced_delta(&body, s, 0));
    CHECK(!col_op_variable(&variable, &stack, s));
    selected = eval_stack_pop(&stack);
    CHECK(selected.rel == retracted && !selected.owned && selected.is_delta);
    op.delta_mode = WL_DELTA_FORCE_DELTA;
    selection = wl_columnar_join_select_right(&op, s, full, false);
    CHECK(selection.right == retracted && selection.used_right_delta &&
        !selection.force_empty);
    s->retraction_right_pass = true; op.delta_mode = WL_DELTA_FORCE_FULL;
    selection = wl_columnar_join_select_right(&op, s, full, false);
    CHECK(selection.right == retracted && selection.used_right_delta);
    variable.delta_mode = WL_DELTA_FORCE_FULL;
    CHECK(!col_op_variable(&variable, &stack, s));
    selected = eval_stack_pop(&stack);
    CHECK(selected.rel == full && !selected.is_delta && !selected.owned);
    CHECK(!eval_stack_drain(&stack));
    col_rel_destroy(left); wl_session_destroy(base); wl_plan_free(plan);
    wirelog_program_free(program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            ref)));
    wl_columnar_memory_governor_ref_release(ref);
    printf(
        "direct %s materialized full/delta/decoy cache hits and credit passed\n",
        diff ? "DIFF" : "ordinary");
}
int main(int argc, char **argv) {
    CHECK(argc == 2);
    if (!strcmp(argv[1], "hash")) hash_lookup();
    else if (!strcmp(argv[1], "cache")) {
        cache_consumer(false); cache_consumer(true);
    }else if (!strcmp(argv[1], "natural")) {
        const size_t lengths[] = {4, 252, 253, 300};
        for (unsigned i = 0; i < 4; ++i) natural(lengths[i], 3, false);
        natural(300, 4, false); natural(300, 3, true);
    } else CHECK(false);
    return 0;
}
