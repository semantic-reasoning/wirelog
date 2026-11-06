/* Independent finite-domain random-program oracle (#2242).
 * Copyright (C) CleverPlant; licensed under LGPL-3.0. */
#define _POSIX_C_SOURCE 200809L
#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DOMAIN 8
#define RELS 7
#define RULES 9
#define VARS 6
static const char *names[] = {"e0", "e1", "e2", "e3", "l0", "a", "b"};
static const unsigned arity[] = {3, 2, 3, 2, 2, 2, 2};
/* Nonnegative terms name variables; negative terms encode constant -t-1. */
typedef struct {
    unsigned rel;
    int t[3];
} atom_t;
typedef struct {
    atom_t head, body[4], neg;
    unsigned count;
    bool negative;
    int cmp, left, right;
} rule_t;
typedef struct {
    rule_t rules[RULES];
    unsigned count;
    bool facts[2][4][512];
} program_t;
typedef struct {
    bool rows[RELS][512];
} model_t;
static uint32_t rng = 2242;
static uint32_t next_random(void) {
    rng ^= rng << 13;
    rng ^= rng >> 17;
    rng ^= rng << 5;
    return rng;
}
static unsigned pick(unsigned n) {
    return next_random() % n;
}
static atom_t atom(unsigned rel, int a, int b, int c) {
    atom_t out = {rel, {a, b, c}};
    return out;
}
static void add(program_t *p, unsigned head, int x, int y, atom_t a, atom_t b,
    atom_t c, unsigned n) {
    rule_t *r = &p->rules[p->count++];
    r->head = atom(head, x, y, 0);
    r->body[0] = a;
    r->body[1] = b;
    r->body[2] = c;
    r->count = n;
}
static int value(int term, const int *env) {
    return term < 0 ? -term - 1 : env[term];
}
static unsigned encode(const int *v, unsigned n) {
    unsigned k = 0;
    for (unsigned i = 0; i < n; i++)
        k = k * DOMAIN + (unsigned)v[i];
    return k;
}
static void decode(unsigned k, int *v, unsigned n) {
    while (n) {
        v[--n] = (int)(k % DOMAIN);
        k /= DOMAIN;
    }
}
static bool derive(const rule_t *r, unsigned pos, int *env, model_t *m) {
    bool changed = false;
    if (pos == r->count) {
        int row[3];
        if (r->negative) {
            for (unsigned c = 0; c < arity[r->neg.rel]; c++)
                row[c] = value(r->neg.t[c], env);
            if (m->rows[r->neg.rel][encode(row, arity[r->neg.rel])])
                return false;
        }
        if (r->cmp && !(r->cmp == 1 ? env[r->left] < env[r->right]
                                : env[r->left] != env[r->right]))
            return false;
        for (unsigned c = 0; c < 2; c++)
            row[c] = value(r->head.t[c], env);
        unsigned key = encode(row, 2);
        changed = !m->rows[r->head.rel][key];
        m->rows[r->head.rel][key] = true;
        return changed;
    }
    const atom_t *a = &r->body[pos];
    unsigned limit = arity[a->rel] == 3 ? 512 : 64;
    for (unsigned k = 0; k < limit; k++)
        if (m->rows[a->rel][k]) {
            int row[3], bound[VARS];
            bool match = true;
            memcpy(bound, env, sizeof(bound));
            decode(k, row, arity[a->rel]);
            for (unsigned c = 0; c < arity[a->rel]; c++) {
                int t = a->t[c];
                if (t >= 0 && bound[t] < 0)
                    bound[t] = row[c];
                else if (value(t, bound) != row[c]) {
                    match = false;
                    break;
                }
            }
            if (match)
                changed = derive(r, pos + 1, bound, m) || changed;
        }
    return changed;
}
static void reference(const program_t *p, unsigned round, model_t *m) {
    memset(m, 0, sizeof(*m));
    for (unsigned b = 0; b <= round; b++)
        for (unsigned r = 0; r < 4; r++)
            for (unsigned k = 0; k < 512; k++)
                m->rows[r][k] |= p->facts[b][r][k];
    for (unsigned k = 0; k < 64; k++)
        m->rows[4][k] = m->rows[1][k];
    bool changed;
    do {
        changed = false;
        for (unsigned r = 0; r < p->count; r++) {
            int env[VARS];
            for (unsigned v = 0; v < VARS; v++)
                env[v] = -1;
            changed = derive(&p->rules[r], 0, env, m) || changed;
        }
    } while (changed);
}
static void render_atom(FILE *f, const atom_t *a) {
    fprintf(f, "%s(", names[a->rel]);
    for (unsigned c = 0; c < arity[a->rel]; c++) {
        if (c)
            fputc(',', f);
        if (a->t[c] < 0)
            fprintf(f, "%d", -a->t[c] - 1);
        else
            fprintf(f, "x%d", a->t[c]);
    }
    fputc(')', f);
}
static char *render(const program_t *p) {
    FILE *f = tmpfile();
    if (!f)
        return NULL;
    for (unsigned r = 0; r < RELS; r++) {
        fprintf(f, ".decl %s(", names[r]);
        for (unsigned c = 0; c < arity[r]; c++)
            fprintf(f, "%sx%u: int64", c ? "," : "", c);
        fprintf(f, ")\n");
    }
    fprintf(f, "l0(x0,x1) :- e1(x0,x1).\n");
    for (unsigned i = 0; i < p->count; i++) {
        const rule_t *r = &p->rules[i];
        render_atom(f, &r->head);
        fprintf(f, " :- ");
        for (unsigned j = 0; j < r->count; j++) {
            if (j)
                fprintf(f, ", ");
            render_atom(f, &r->body[j]);
        }
        if (r->negative) {
            fprintf(f, ", !");
            render_atom(f, &r->neg);
        }
        if (r->cmp)
            fprintf(f, ", x%d %s x%d", r->left, r->cmp == 1 ? "<" : "!=",
                r->right);
        fprintf(f, ".\n");
    }
    long n = ftell(f);
    rewind(f);
    char *s = malloc((size_t)n + 1);
    if (s) {
        if (fread(s, 1, (size_t)n, f) != (size_t)n) {
            free(s);
            s = NULL;
        } else
            s[n] = 0;
    }
    fclose(f);
    return s;
}
/* Half the corpus uses each randomized bug-path skeleton. Additional rules
 * and facts vary independently. These schedules guarantee eligible shapes,
 * rather than hoping unconstrained random rules happen to reach them. */
static void generate(program_t *p, unsigned index) {
    memset(p, 0, sizeof(*p));
    atom_t unused = atom(0, 0, 0, 0);
    if (index % 2 == 0) {
        /* Same pair of recursive inputs joined on different key columns. */
        add(p, 5, 2, 0, atom(0, 1, 0, 3), atom(3, 2, 1, 0), unused, 2);
        add(p, 5, 2, 2, atom(5, 0, 1, 0), atom(3, 1, 2, 0), unused, 2);
        add(p, 5, 0, 3, atom(3, 0, 1, 0), atom(3, 1, 2, 0), atom(5, 2, 3, 0),
            3);
        add(p, 5, 2, 2, atom(5, 1, 0, 0), atom(5, 1, 2, 0), unused, 2);
        p->rules[3].cmp = 2;
        p->rules[3].left = 0;
        p->rules[3].right = 1;
        add(p, 5, 0, 2, atom(5, 0, 1, 0), atom(5, 2, 1, 0), unused, 2);
        p->rules[4].negative = true;
        p->rules[4].neg = atom(3, 1, 2, 0);
        p->facts[0][0][4 * 64 + 7 * 8 + 3] = true;
        p->facts[1][3][5 * 8 + 4] = true;
    } else {
        /* EDB constant/repeat filter must not hide a later recursive JOIN. */
        add(p, 5, 0, 0, atom(3, 0, 0, 0), unused, unused, 1);
        add(p, 6, 0, 1, atom(0, 0, 2, -3), atom(1, 2, 3, 0), atom(5, 3, 1, 0),
            3);
        add(p, 5, 0, 0, atom(6, 0, 1, 0), unused, unused, 1);
        add(p, 5, 0, 1, atom(1, 0, 1, 0), unused, unused, 1);
        p->facts[0][3][1 * 8 + 1] = true;
        p->facts[0][1][0 * 8 + 1] = true;
        p->facts[0][0][3 * 64 + 0 * 8 + 2] = true;
    }
    unsigned extra = 1 + pick(2);
    for (unsigned i = 0; i < extra; i++) {
        unsigned n = 1 + pick(3), bound = 0;
        rule_t *r = &p->rules[p->count++];
        r->count = n;
        for (unsigned j = 0; j < n; j++) {
            unsigned rel = pick(9);
            if (rel >= 7)
                rel -= 2;
            r->body[j].rel = rel;
            for (unsigned c = 0; c < arity[rel]; c++) {
                int t;
                if (rel < 4 && pick(100) < 15)
                    t = -(int)pick(4) - 1;
                else if (bound && (bound == VARS || pick(100) < 55))
                    t = (int)pick(bound);
                else
                    t = (int)bound++;
                r->body[j].t[c] = t;
            }
        }
        /* Ensure safe heads even if an all-constant EDB atom was chosen. */
        if (!bound) {
            r->body[0].t[0] = 0;
            bound = 1;
        }
        /* Sequence random draws explicitly for identical corpora on MSVC,
         * Clang and GCC regardless of argument evaluation order. */
        int head_y = (int)pick(bound);
        int head_x = (int)pick(bound);
        unsigned head_rel = 5 + pick(2);
        r->head = atom(head_rel, head_x, head_y, 0);
        if (pick(100) < 35) {
            r->negative = true;
            unsigned rel = pick(5);
            r->neg.rel = rel;
            for (unsigned c = 0; c < arity[rel]; c++)
                r->neg.t[c] = (int)pick(bound);
        }
        if (pick(100) < 30) {
            r->cmp = 1 + pick(2);
            r->left = (int)pick(bound);
            r->right = (int)pick(bound);
        }
    }
    for (unsigned b = 0; b < 2; b++)
        for (unsigned r = 0; r < 4; r++)
            for (unsigned i = 0; i < 5; i++) {
                int row[3];
                for (unsigned c = 0; c < arity[r]; c++)
                    row[c] = (int)pick(DOMAIN);
                p->facts[b][r][encode(row, arity[r])] = true;
            }
    unsigned permutation[DOMAIN];
    for (unsigned i = 0; i < DOMAIN; i++)
        permutation[i] = i;
    for (unsigned base = 0; base < DOMAIN; base += 4)
        for (unsigned i = 3; i > 0; i--) {
            unsigned j = pick(i + 1), tmp = permutation[base + i];
            permutation[base + i] = permutation[base + j];
            permutation[base + j] = tmp;
        }
    for (unsigned i = 0; i < p->count; i++) {
        rule_t *r = &p->rules[i];
        for (unsigned j = 0; j < r->count; j++)
            for (unsigned c = 0; c < arity[r->body[j].rel]; c++)
                if (r->body[j].t[c] < 0)
                    r->body[j].t[c] = -(int)permutation[-r->body[j].t[c] - 1] -
                        1;
        /* Permuting an ordered domain would change <; comparisons instead
         * remain literal constraints on the renamed generated values. */
    }
    bool renamed[2][4][512] = {0};
    for (unsigned b = 0; b < 2; b++)
        for (unsigned r = 0; r < 4; r++)
            for (unsigned k = 0; k < 512; k++)
                if (p->facts[b][r][k]) {
                    int row[3];
                    decode(k, row, arity[r]);
                    for (unsigned c = 0; c < arity[r]; c++)
                        row[c] = (int)permutation[row[c]];
                    renamed[b][r][encode(row, arity[r])] = true;
                }
    memcpy(p->facts, renamed, sizeof(renamed));
}
static unsigned constant_programs, repeat_programs, keyed_programs,
    filtered_programs;
static unsigned parallel_runs;
static bool coverage(const program_t *p, const wl_plan_t *plan) {
    bool constants = false, repeats = false, keyed = false, filtered = false;
    for (unsigned i = 0; i < p->count; i++)
        for (unsigned j = 0; j < p->rules[i].count; j++) {
            const atom_t *a = &p->rules[i].body[j];
            for (unsigned c = 0; c < arity[a->rel]; c++) {
                if (a->t[c] < 0)
                    constants = true;
                for (unsigned d = 0; d < c; d++)
                    if (a->t[c] >= 0 && a->t[c] == a->t[d])
                        repeats = true;
            }
        }
    const wl_plan_op_t *joins[128];
    const char *lefts[128];
    unsigned count = 0;
    for (unsigned si = 0; si < plan->stratum_count; si++)
        for (unsigned ri = 0; ri < plan->strata[si].relation_count; ri++) {
            const wl_plan_relation_t *r = &plan->strata[si].relations[ri];
            count = 0; /* Compare JOINs within this head relation only. */
            const wl_plan_op_t *segments[128] = {r->ops};
            unsigned sizes[128] = {r->op_count}, segments_count = 1;
            for (unsigned seg = 0; seg < segments_count; seg++) {
                for (unsigned i = 0; i < sizes[seg]; i++)
                    if (segments[seg][i].op == WL_PLAN_OP_K_FUSION) {
                        const wl_plan_op_k_fusion_t *f =
                            segments[seg][i].opaque_data;
                        for (unsigned k = 0; k < f->k && segments_count < 128;
                            k++) {
                            segments[segments_count] = f->k_ops[k];
                            sizes[segments_count++] = f->k_op_counts[k];
                        }
                    }
            }
            for (unsigned seg = 0; seg < segments_count; seg++) {
                const char *left = NULL;
                bool filter = false, joined = false;
                for (unsigned i = 0; i < sizes[seg]; i++) {
                    const wl_plan_op_t *o = &segments[seg][i];
                    if (o->op == WL_PLAN_OP_VARIABLE) {
                        left = o->relation_name;
                        filter = false;
                        joined = false;
                    } else if (o->op == WL_PLAN_OP_FILTER)
                        filter = true;
                    else if (o->op == WL_PLAN_OP_JOIN) {
                        if (left && left[0] == 'e' && filter &&
                            o->right_relation &&
                            (!strcmp(o->right_relation, "a") ||
                            !strcmp(o->right_relation, "b")))
                            filtered = true;
                        if (left && !joined && o->right_relation &&
                            o->key_count) {
                            for (unsigned j = 0; j < count; j++)
                                if (!strcmp(left, lefts[j]) &&
                                    !strcmp(o->right_relation,
                                    joins[j]->right_relation)) {
                                    bool same = o->key_count ==
                                        joins[j]->key_count;
                                    if (same)
                                        for (unsigned k = 0; k < o->key_count;
                                            k++)
                                            if (strcmp(o->left_keys[k],
                                                joins[j]->left_keys[k]) ||
                                                strcmp(o->right_keys[k],
                                                joins[j]->right_keys[k]))
                                                same = false;
                                    if (!same)
                                        keyed = true;
                                }
                            if (count < 128) {
                                joins[count] = o;
                                lefts[count++] = left;
                            }
                        }
                        joined = true;
                    }
                }
            }
        }
    constant_programs += constants;
    repeat_programs += repeats;
    keyed_programs += keyed;
    filtered_programs += filtered;
    return true;
}
static bool oracle_selftest(void) {
    program_t p = {0};
    model_t m;
    atom_t unused = atom(0, 0, 0, 0);
    add(&p, 5, 0, 1, atom(0, 0, 0, -2), atom(1, 0, 1, 0), unused, 2);
    p.rules[0].negative = true;
    p.rules[0].neg = atom(3, 0, 1, 0);
    p.rules[0].cmp = 1;
    p.rules[0].left = 0;
    p.rules[0].right = 1;
    add(&p, 6, 0, 2, atom(5, 0, 1, 0), atom(1, 1, 2, 0), unused, 2);
    add(&p, 5, 0, 1, atom(6, 0, 1, 0), unused, unused, 1);
    p.facts[0][0][1] = true;
    p.facts[0][1][1] = true;
    p.facts[0][1][10] = true;
    p.facts[0][3][2] = true;
    /* Reject a tuple whose repeated variable disagrees. */
    p.facts[0][0][1 * 64 + 0 * 8 + 1] = true;
    reference(&p, 0, &m);
    for (unsigned k = 0; k < 64; k++) {
        if (m.rows[5][k] != (k == 1 || k == 2) || m.rows[6][k] != (k == 2))
            return false;
    }
    p.facts[1][3][1] = true;
    reference(&p, 1, &m);
    for (unsigned k = 0; k < 64; k++)
        if (m.rows[5][k] || m.rows[6][k])
            return false;
    return true;
}
static bool capture_bad;
static void capture(const char *name, const int64_t *row, uint32_t cols,
    void *ctx) {
    model_t *m = ctx;
    unsigned r;
    for (r = 4; r < RELS; r++)
        if (!strcmp(name, names[r]))
            break;
    if (r == RELS || cols != 2 || row[0] < 0 || row[0] >= DOMAIN ||
        row[1] < 0 ||
        row[1] >= DOMAIN) {
        capture_bad = true;
        return;
    }
    unsigned k = (unsigned)(row[0] * DOMAIN + row[1]);
    if (m->rows[r][k])
        capture_bad = true;
    m->rows[r][k] = true;
}
static bool run(const program_t *p, const char *src, unsigned index,
    unsigned workers, unsigned passes) {
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    wl_plan_t *plan = NULL;
    wl_session_t *session = NULL;
    int rc = prog ? 0 : -1;
    if (prog) {
        if (passes == 2) {
            wl_fusion_apply(prog, NULL);
            wl_jpp_apply(prog, NULL);
        }
        if (passes)
            wl_sip_apply(prog, NULL);
        rc = wl_plan_from_program(prog, &plan);
        plan_fixture_hold(prog);
    }
    if (!rc && workers == 1 && passes == 0)
        coverage(p, plan);
    if (!rc)
        rc = wl_session_create(wl_backend_columnar(), plan, workers, &session);
    bool ok = true;
    for (unsigned b = 0; b < 2 && !rc; b++) {
        for (unsigned r = 0; r < 4 && !rc; r++)
            for (unsigned k = 0; k < 512 && !rc; k++)
                if (p->facts[b][r][k]) {
                    int values[3];
                    int64_t row[3];
                    decode(k, values, arity[r]);
                    for (unsigned c = 0; c < arity[r]; c++)
                        row[c] = values[c];
                    rc = wl_session_insert(session, names[r], row, 1, arity[r]);
                }
        if (!rc && index % 2 == 0) {
            rc = wl_session_step(session);
            if (workers > 1 && COL_SESSION(session)->tdd_max_active_workers > 1)
                parallel_runs++;
        }
        model_t expected, actual = {0};
        reference(p, b, &expected);
        capture_bad = false;
        if (!rc)
            rc = wl_session_snapshot(session, capture, &actual);
        if (!rc)
            for (unsigned r = 4; r < RELS; r++)
                for (unsigned k = 0; k < 64; k++)
                    if (expected.rows[r][k] != actual.rows[r][k]) {
                        fprintf(stderr,
                            "MODEL MISMATCH seed=2242 index=%u round=%u W=%u passes=%u "
                            "%s(%u,%u) expected=%d actual=%d\n%s",
                            index, b, workers, passes, names[r], k / 8, k % 8,
                            expected.rows[r][k], actual.rows[r][k], src);
                        ok = false;
                        goto done;
                    }
        if (capture_bad) {
            ok = false;
            break;
        }
    }
done:
    if (!ok || rc)
        for (unsigned b = 0; b < 2; b++)
            for (unsigned r = 0; r < 4; r++)
                for (unsigned k = 0; k < 512; k++)
                    if (p->facts[b][r][k]) {
                        int row[3];
                        decode(k, row, arity[r]);
                        fprintf(stderr, "batch%u %s", b, names[r]);
                        for (unsigned c = 0; c < arity[r]; c++)
                            fprintf(stderr, " %d", row[c]);
                        fprintf(stderr, "\n");
                    }
    if (rc)
        fprintf(stderr,
            "ENGINE ERROR seed=2242 index=%u W=%u passes=%u rc=%d\n%s",
            index, workers, passes, rc, src);
    if (session)
        wl_session_destroy(session);
    wl_plan_free(plan);
    return ok && !rc;
}
int main(void) {
    if (!oracle_selftest()) {
        fprintf(stderr, "oracle self-test failed\n");
        return 1;
    }
#ifdef _WIN32
    _putenv_s("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1");
#else
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
#endif
    for (unsigned i = 0; i < 150; i++) {
        program_t p;
        generate(&p, i);
        char *src = render(&p);
        if (!src)
            return 1;
        for (unsigned mode = 0; mode < 3; mode++)
            for (unsigned w = 1; w <= 2; w++)
                if (!run(&p, src, i, w, mode)) {
                    free(src);
                    return 1;
                }
        free(src);
    }
    printf("coverage constants=%u repeats=%u keyed-pairs=%u "
        "filtered-recursive=%u parallel-steps=%u\n",
        constant_programs, repeat_programs, keyed_programs, filtered_programs,
        parallel_runs);
    if (constant_programs < 75 || repeat_programs < 75 || keyed_programs < 75 ||
        filtered_programs < 75 || parallel_runs != 450)
        return 1;
    printf(
        "150 programs x 3 optimizer modes x 2 worker widths x 2 rounds passed\n");
    return 0;
}
