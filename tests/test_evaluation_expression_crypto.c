/* #2299 original PSA digest compatibility through genuine scalar/session hosts. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/session.h"
#include <xxhash.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef WL_MBEDTLS_ENABLED
#include <psa/crypto.h>
#endif
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__,#x); exit(1); } } while (0)
static const uint8_t tags[] = {WL_PLAN_EXPR_ARITH_MD5, WL_PLAN_EXPR_ARITH_MD5_S,
                               WL_PLAN_EXPR_ARITH_SHA1,
                               WL_PLAN_EXPR_ARITH_SHA1_S,
                               WL_PLAN_EXPR_ARITH_SHA256,
                               WL_PLAN_EXPR_ARITH_SHA256_S,
                               WL_PLAN_EXPR_ARITH_SHA512,
                               WL_PLAN_EXPR_ARITH_SHA512_S};
static int owner;
extern void (*wl_columnar_expr_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t, bool);
static wl_evaluation_control_t *begin(uint64_t limit) {
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c));
    CHECK(!wl_evaluation_control_attach(c, &owner));
    CHECK(!wl_evaluation_control_begin(c, &owner)); return c;
}
static void finish(wl_evaluation_control_t *c, int rc) {
    CHECK(wl_evaluation_control_finish(c, &owner, rc > 0?rc:0, 0) == rc);
    CHECK(!wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
}
static uint32_t bytecode(uint8_t *b, int64_t operand, uint8_t tag) {
    b[0] = WL_PLAN_EXPR_CONST_INT; memcpy(b+1, &operand, 8); b[9] = tag;
    return 10;
}
#ifdef WL_MBEDTLS_ENABLED
static const size_t lengths[] = {16, 20, 32, 64};
static const psa_algorithm_t algorithms[] = {PSA_ALG_MD5, PSA_ALG_SHA_1,
                                             PSA_ALG_SHA_256, PSA_ALG_SHA_512};
static const char *known_hex[] = {"900150983cd24fb0d6963f7d28e17f72",
                                  "a9993e364706816aba3e25717850c26c9cd0d89d",
                                  "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                                  "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"};
static uint8_t known[4][64];
extern psa_status_t (*wl_columnar_expr_crypto_test_init)(void);
extern psa_status_t (*wl_columnar_expr_crypto_test_compute)(psa_algorithm_t,
    const uint8_t *, size_t, uint8_t *, size_t, size_t *);
enum {REAL, INIT_FAIL, COMPUTE_FAIL, SHORT_LENGTH, LONG_LENGTH, KNOWN,
      COMPUTE_MEMORY};
static unsigned fault, init_calls, compute_calls, wanted_kind;
static const uint8_t *wanted_input;
static size_t wanted_length;
static psa_status_t shim_init(void) {
    ++init_calls;
    return fault == INIT_FAIL?PSA_ERROR_NOT_SUPPORTED:psa_crypto_init();
}
static psa_status_t shim_compute(psa_algorithm_t alg, const uint8_t *input,
    size_t n,
    uint8_t *out, size_t capacity, size_t *actual) {
    ++compute_calls;
    CHECK(alg == algorithms[wanted_kind] && input == wanted_input &&
        n == wanted_length);
    CHECK(capacity == lengths[wanted_kind] && !*actual);
    if (fault == COMPUTE_FAIL)return PSA_ERROR_NOT_SUPPORTED;
    if (fault == COMPUTE_MEMORY)return PSA_ERROR_INSUFFICIENT_MEMORY;
    if (fault == KNOWN) {
        memcpy(out, known[wanted_kind], capacity); *actual = capacity;
        return PSA_SUCCESS;
    }
    psa_status_t rc = psa_hash_compute(alg, input, n, out, capacity, actual);
    CHECK(rc == PSA_SUCCESS && *actual == capacity);
    if (fault == SHORT_LENGTH)--*actual;
    if (fault == LONG_LENGTH)++*actual;
    return rc;
}
static void shim(unsigned kind, unsigned which, const uint8_t *input,
    size_t length) {
    wanted_kind = kind; wanted_input = input; wanted_length = length;
    fault = which; init_calls = compute_calls = 0;
    wl_columnar_expr_crypto_test_init = shim_init;
    wl_columnar_expr_crypto_test_compute = shim_compute;
}
static void unshim(void) {
    wl_columnar_expr_crypto_test_init = NULL;
    wl_columnar_expr_crypto_test_compute = NULL;
}
static void prepare_known(void) {
    for (unsigned k = 0; k < 4; ++k)for (size_t i = 0; i < lengths[k]; ++i) {
            unsigned byte = 0;
            CHECK(sscanf(known_hex[k]+2*i, "%2x", &byte) == 1);
            known[k][i] = (uint8_t)byte;
        }
}
#endif
static void helper_oracles(void) {
    const uint8_t input[] = {'a', 'b', 'c'};
    for (unsigned i = 0; i < 8; ++i) {
#ifdef WL_MBEDTLS_ENABLED
        uint64_t tariff = 2016;
        int64_t expected = (int64_t)XXH3_64bits(known[i/2], lengths[i/2]);
#else
        uint64_t tariff = 448;
        int64_t expected = 0;
#endif
        for (unsigned below = 0; below < 2; ++below) {
            wl_evaluation_control_t *c = begin(tariff-below);
            int64_t value = 999; bool failed = false;
#ifdef WL_MBEDTLS_ENABLED
            shim(i/2, REAL, input, sizeof(input));
#endif
            int rc = wl_columnar_expr_crypto_digest_attempt_controlled(tags[i],
                    input,
                    sizeof(input), &value, &failed, c,
                    wl_evaluation_control_active_attempt_id(c));
            CHECK(rc == (below?WL_EVALUATION_CONTROL_WORK_LIMIT:0));
            CHECK(value == (below?999:expected));
#ifdef WL_MBEDTLS_ENABLED
            CHECK(!failed && init_calls == 1 && compute_calls == 1); unshim();
#else
            CHECK(failed == !below);
#endif
            finish(c, rc);
        }
#ifdef WL_MBEDTLS_ENABLED
        /* A compute-only provider remains authoritative in either direction:
         * refusal never falls back to multipart, success never needs setup. */
        const unsigned faults[] = {INIT_FAIL, COMPUTE_FAIL, SHORT_LENGTH,
                                   LONG_LENGTH, KNOWN, COMPUTE_MEMORY};
        for (unsigned j = 0; j < sizeof(faults)/sizeof(*faults); ++j)
            for (unsigned below = 0; below < 2; ++below) {
                unsigned f = faults[j];
                uint64_t cost = f == INIT_FAIL?672:f == KNOWN?2016:896;
                wl_evaluation_control_t *c = begin(cost-below);
                shim(i/2, f, input, sizeof(input)); int64_t value = 999;
                bool failed = false;
                int rc =
                    wl_columnar_expr_crypto_digest_attempt_controlled(tags[i],
                        input,
                        sizeof(input), &value, &failed, c,
                        wl_evaluation_control_active_attempt_id(c));
                CHECK(rc == (below?WL_EVALUATION_CONTROL_WORK_LIMIT:0));
                CHECK(value == (below?999:f == KNOWN?expected:0));
                CHECK(failed == (!below && f != KNOWN));
                CHECK(init_calls == 1 && compute_calls == (f == INIT_FAIL?0:1));
                unshim(); finish(c, rc);
            }
        const uint64_t budgets[] = {223, 447, 671, 895};
        for (unsigned j = 0; j < 4; ++j) {
            wl_evaluation_control_t *c = begin(budgets[j]);
            shim(i/2, REAL, input, 3);
            int64_t value = 999; bool failed = false;
            int rc = wl_columnar_expr_crypto_digest_attempt_controlled(tags[i],
                    input, 3,
                    &value, &failed, c,
                    wl_evaluation_control_active_attempt_id(c));
            CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && value == 999 &&
                !failed);
            CHECK(init_calls == (j >= 2?1:0) && compute_calls == (j == 3?1:0));
            unshim(); finish(c, rc);
        }
#endif
    }
    wl_evaluation_control_t *c = begin(0);
    int64_t value = 999; bool failed = false;
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    CHECK(wl_columnar_expr_crypto_digest_attempt_controlled(
            WL_PLAN_EXPR_AGG_AVG,
            input, 3, &value, &failed, c, id) == EINVAL);
    CHECK(wl_columnar_expr_crypto_digest_attempt_controlled(tags[0], NULL, 1,
        &value, &failed, c, id) == EINVAL);
    CHECK(wl_columnar_expr_crypto_digest_attempt_controlled(tags[0], input, 3,
        NULL, &failed, c, id) == EINVAL);
    CHECK(wl_columnar_expr_crypto_digest_attempt_controlled(tags[0], input, 3,
        &value, &failed, NULL, id) == EINVAL);
    uint64_t used = 999;
    CHECK(!wl_evaluation_control_test_active_consumed(c, id, &used) && !used);
    CHECK(value == 999 && !failed); finish(c, 0);
}
static unsigned target_phase, action;
static bool stopped;
static uint64_t successor;
static wl_evaluation_control_report_t frozen;
static void stop_hook(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)progress; (void)failed;
    if (!c || stopped || phase != target_phase)return;
    stopped = true;
    if (action == 0)wl_evaluation_control_request_cancel(c);
    else if (action == 1) {
        CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
        CHECK(!wl_evaluation_control_report(c, &frozen));
        CHECK(!wl_evaluation_control_reset(c));
        CHECK(!wl_evaluation_control_begin(c, &owner));
        successor = wl_evaluation_control_active_attempt_id(c);
    } else {
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
        wl_evaluation_control_request_cancel(c);
    }
}
static void helper_stops(void) {
    const uint8_t input[] = {'a', 'b', 'c'};
    const unsigned phases[] = {200, 201, 202, 203, 204, 205};
#ifdef WL_MBEDTLS_ENABLED
    const unsigned faults[] = {REAL, INIT_FAIL, COMPUTE_FAIL, COMPUTE_MEMORY};
#else
    const unsigned faults[] = {0};
#endif
    for (unsigned f = 0; f < sizeof(faults)/sizeof(*faults); ++f)
        for (unsigned p = 0; p < sizeof(phases)/sizeof(*phases); ++p)
            for (action = 0; action < 3; ++action) {
#ifdef WL_MBEDTLS_ENABLED
                if (faults[f] == INIT_FAIL && phases[p] != 202 &&
                    phases[p] != 205) continue;
                if (faults[f] >= COMPUTE_FAIL && phases[p] != 204 &&
                    phases[p] != 205) continue;
#else
                if (phases[p] != 200 && phases[p] != 205)continue;
#endif
                wl_evaluation_control_t *c = begin(0);
                uint64_t id = wl_evaluation_control_active_attempt_id(c);
#ifdef WL_MBEDTLS_ENABLED
                shim(2, faults[f], input, 3);
#endif
                target_phase = phases[p]; stopped = false; successor = 0;
                wl_columnar_expr_test_checkpoint = stop_hook;
                int64_t value = 999; bool failed = false;
                int rc =
                    wl_columnar_expr_crypto_digest_attempt_controlled(tags[4],
                        input, 3,
                        &value, &failed, c, id);
                wl_columnar_expr_test_checkpoint = NULL;
                CHECK(stopped && value == 999 && !failed);
                CHECK(rc ==
                    (action == 0?WL_EVALUATION_CONTROL_CANCELLED:action ==
                    1?EINVAL:EOVERFLOW));
#ifdef WL_MBEDTLS_ENABLED
                CHECK(init_calls == (phases[p] >= 202?1:0));
                CHECK(compute_calls == (phases[p] >= 204 &&
                    faults[f] != INIT_FAIL?1:0)); unshim();
#endif
                if (action == 1) {
                    uint64_t used = 999; wl_evaluation_control_report_t report;
                    CHECK(successor != id &&
                        !wl_evaluation_control_test_active_consumed(c,
                        successor, &used) && !used);
                    CHECK(!wl_evaluation_control_report(c,
                        &report) && report.attempt_id == frozen.attempt_id &&
                        report.consumed == frozen.consumed &&
                        report.result == frozen.result);
                    CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
                } else CHECK(wl_evaluation_control_finish(c, &owner,
                        rc > 0?rc:0, 0) == rc);
                CHECK(!wl_evaluation_control_reset(c));
                CHECK(!wl_evaluation_control_begin(c, &owner));
                CHECK(!wl_columnar_expr_crypto_digest_attempt_controlled(
                        tags[4], input, 3,
                        &value, &failed, c,
                        wl_evaluation_control_active_attempt_id(c)));
#ifdef WL_MBEDTLS_ENABLED
                CHECK(!failed && value == (int64_t)XXH3_64bits(known[2], 32));
#else
                CHECK(failed && !value);
#endif
                finish(c, 0);
            }
}
static unsigned scan_calls;
static void scan_hook(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)c; (void)progress; (void)failed; if (phase == 41)++scan_calls;
}
static void scalar_parity(void) {
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    char large[4097]; memset(large, 'a', 4096); large[4096] = 0;
    const char embedded[] = {'a', 0, 'b'};
    int64_t inputs[] = {0, 1, -1, INT64_MIN, INT64_MAX,
                        wl_intern_put(intern, "abc"), wl_intern_put(intern, ""),
                        wl_intern_put(intern, large),
                        wl_intern_put(intern, "\x80\xff"),
                        wl_intern_put(intern, embedded)};
    for (unsigned i = 0; i < 8; ++i)for (unsigned v = 0;
            v < sizeof(inputs)/sizeof(*inputs); ++v)
            for (unsigned null = 0; null < 2; ++null) {
                uint8_t bytes[16];
                uint32_t n = bytecode(bytes, inputs[v], tags[i]);
                wl_columnar_expr_context_t context =
                {.intern = null?NULL:intern};
                int64_t raw = 999, value = 999;
                wl_columnar_expr_status_t raw_status, status;
                int raw_rc = wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0,
                        &raw, &context, &raw_status);
                wl_evaluation_control_t *c = begin(0);
                bool supported = false, failed = false;
                wl_columnar_expr_attempt_scalar_t *program = NULL;
                CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes,
                    n, NULL, &program, &supported, c,
                    wl_evaluation_control_active_attempt_id(c)) && supported);
                scan_calls = 0; wl_columnar_expr_test_checkpoint = scan_hook;
                CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program,
                    NULL, 0, &context,
                    &value, &failed, &status, c,
                    wl_evaluation_control_active_attempt_id(c)));
                wl_columnar_expr_test_checkpoint = NULL;
                CHECK(value == raw && status == raw_status &&
                    failed == (raw_rc != 0));
#ifndef WL_MBEDTLS_ENABLED
                CHECK(!scan_calls);
#endif
                wirelog_column_type_t type = WIRELOG_TYPE_STRING;
                supported = false;
                CHECK(!wl_columnar_expr_scalar_result_type_attempt_controlled(
                        program, NULL, 0, &type, &supported, c,
                        wl_evaluation_control_active_attempt_id(c)) &&
                    supported &&
                    type == WIRELOG_TYPE_INT64);
                wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
            }
    for (unsigned i = 0; i < 8; ++i) {
        /* Zero underflow plus a capped stack and ignored float producer. */
        for (unsigned capped = 0; capped < 2; ++capped) {
            uint8_t bytes[COL_FILTER_STACK*9+16]; uint32_t n = 0;
            if (capped)for (unsigned k = 0; k < COL_FILTER_STACK; ++k) {
                    int64_t one = 1; bytes[n++] = WL_PLAN_EXPR_CONST_INT;
                    memcpy(bytes+n, &one, 8); n += 8;
                }
            bytes[n++] = tags[i];
            if (capped) {
                double x = 3.5; bytes[n++] = WL_PLAN_EXPR_CONST_FLOAT;
                memcpy(bytes+n, &x, 8); n += 8;
            }
            wl_evaluation_control_t *c = begin(0);
            bool supported = false, failed = false;
            wl_columnar_expr_attempt_scalar_t *program = NULL;
            CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes, n,
                NULL, &program, &supported, c,
                wl_evaluation_control_active_attempt_id(c)) && supported);
            wirelog_column_type_t type = WIRELOG_TYPE_STRING;
            CHECK(!wl_columnar_expr_scalar_result_type_attempt_controlled(
                    program, NULL, 0, &type, &supported, c,
                    wl_evaluation_control_active_attempt_id(c)) && supported &&
                type == WIRELOG_TYPE_INT64);
            wl_columnar_expr_context_t context = {.intern = intern};
            int64_t raw = 999, value = 999; wl_columnar_expr_status_t a, b;
            int raw_rc = wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0, &raw,
                    &context, &a);
            CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program,
                NULL, 0, &context, &value, &failed, &b, c,
                wl_evaluation_control_active_attempt_id(c)) && value == raw &&
                a == b && failed == (raw_rc != 0));
            wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
        }
    }
    wl_intern_free(intern);
}

typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_session_t *session;
    wl_evaluation_control_t *control;
    int64_t input[3][2], expected[3][8];
    unsigned events, positives, negatives;
    int balance[3];
    bool filter;
} fixture_t;
static unsigned seen[8];
static fixture_t *active;
#ifdef WL_MBEDTLS_ENABLED
static unsigned session_action;
static bool session_stopped;
static uint64_t session_successor;
static wl_evaluation_control_report_t session_report;
#endif
static int complete(wl_session_t *session) {
    for (unsigned i = 0; i < 10000; ++i) {
        int rc = wl_session_step(session); if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EAGAIN;
}
static fixture_t create(const char *source) {
    fixture_t f = {0}; wirelog_error_t error;
    f.program = wirelog_parse_string(source, &error);
    if (!f.program)fprintf(stderr, "parse failed: %s\n", source);
    CHECK(f.program && !wl_plan_from_program(f.program, &f.plan));
    CHECK(!wl_evaluation_control_create(0, &f.control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = f.control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.session));
    char long_text[4097]; memset(long_text, 'a', 4096); long_text[4096] = 0;
    const char *text[] = {"abc", "", long_text};
    for (unsigned r = 0; r < 3; ++r) {
        f.input[r][0] = (int64_t)r+1;
        f.input[r][1] = wl_intern_put(f.plan->intern, text[r]);
        CHECK(f.input[r][1] >= 0);
        for (unsigned i = 0; i < 8; ++i) {
            uint8_t bytes[16];
            uint32_t n = bytecode(bytes, f.input[r][i%2], tags[i]);
            wl_columnar_expr_context_t context = {.intern = f.plan->intern};
            wl_columnar_expr_status_t status;
            int rc = wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0,
                    &f.expected[r][i], &context, &status);
#ifdef WL_MBEDTLS_ENABLED
            CHECK(!rc && status == WL_COLUMNAR_EXPR_OK);
#else
            CHECK(rc == WL_COLUMNAR_EXPR_MALFORMED &&
                status == WL_COLUMNAR_EXPR_MALFORMED && !f.expected[r][i]);
#endif
        }
    }
    return f;
}
static void destroy(fixture_t *f) {
    wl_columnar_memory_governor_ref_t *g =
        COL_SESSION(f->session)->memory_governor;
    wl_columnar_memory_governor_ref_retain(g);
    wl_session_destroy(f->session); wl_plan_free(f->plan);
    wirelog_program_free(f->program);
    wl_evaluation_control_release(f->control);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
static void capture(const char *name, const int64_t *row, uint32_t width,
    int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(name, "out") && (diff == 1 || diff == -1));
    CHECK(row[0] >= 1 && row[0] <= 3);
    unsigned r = (unsigned)(row[0]-1);
    CHECK(row[1] == f->input[r][1]);
    if (f->filter)CHECK(width == 2 && !r);
    else {
        CHECK(width == 10);
        for (unsigned i = 0; i < 8; ++i)CHECK(row[i+2] == f->expected[r][i]);
    }
    f->balance[r] += diff; CHECK(f->balance[r] >= 0 && f->balance[r] <= 1);
    ++f->events; f->positives += diff == 1; f->negatives += diff == -1;
}
static void contents(fixture_t *f, unsigned expected) {
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->nrows == expected && out->ncols == (f->filter?2:10));
    CHECK(out->column_types[0] == WIRELOG_TYPE_INT64 &&
        out->column_types[1] == WIRELOG_TYPE_STRING);
    bool visited[3] = {false, false, false};
    for (uint32_t row = 0; row < out->nrows; ++row) {
        int64_t identity = out->columns[0][row];
        CHECK(identity >= 1 && identity <= 3);
        unsigned r = (unsigned)(identity-1); CHECK(!visited[r]);
        visited[r] = true;
        CHECK(out->columns[1][row] == f->input[r][1]);
        if (f->filter)CHECK(!r);
        else for (unsigned i = 0; i < 8;
                ++i) CHECK(out->column_types[i+2] == WIRELOG_TYPE_INT64 &&
                    out->columns[i+2][row] == f->expected[r][i]);
    }
}
static void session_hook(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)failed;
    if (phase == 206) {
        unsigned i; for (i = 0; i < 8; ++i)if (tags[i] == progress) {
                ++seen[i]; break;
            }
        CHECK(i < 8);
    }
#ifdef WL_MBEDTLS_ENABLED
    unsigned evaluations = 0;
    for (unsigned i = 0; i < 8; ++i) evaluations += seen[i];
    if (!c || session_stopped || !active || phase != 204 ||
        evaluations < 9) return;
    session_stopped = true;
    if (session_action == 0)wl_evaluation_control_request_cancel(c);
    else if (session_action == 1)CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else {
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        wl_evaluation_control_attempt_finish_result_t end =
            wl_evaluation_control_finish_attempt(c,
                active->session->operation_admission, id, 0, 0);
        CHECK(!end.protocol && end.report_valid && !end.report.result &&
            end.report.consumed);
        session_report = end.report; CHECK(!wl_evaluation_control_reset(c));
        wl_evaluation_control_attempt_begin_result_t next =
            wl_evaluation_control_begin_attempt(c,
                active->session->operation_admission);
        CHECK(!next.protocol && next.attempt_id != id);
        session_successor = next.attempt_id;
    }
#else
    (void)c;
#endif
}
static void inputs(fixture_t *f) {
    for (unsigned r = 0; r < 3;
        ++r) CHECK(!wl_session_insert(f->session, "input", f->input[r], 1, 2));
}
static const char *map_source =
    ".decl input(i:int64,s:string)\n"
    ".decl out(i:int64,s:string,a:int64,b:int64,c:int64,d:int64,e:int64,f:int64,g:int64,h:int64)\n"
    "out(i,s,md5(i),md5(s),sha1(i),sha1(s),sha256(i),sha256(s),sha512(i),sha512(s)) :- input(i,s).\n";
static void session_maps(void) {
#ifdef WL_MBEDTLS_ENABLED
    for (session_action = 0; session_action < 3; ++session_action) {
        fixture_t f = create(map_source); active = &f;
        wl_session_set_delta_cb(f.session, capture, &f);
        memset(seen, 0, sizeof(seen)); session_stopped = false;
        session_successor = 0;
        wl_columnar_expr_test_checkpoint = session_hook;
        CHECK(!complete(f.session)); contents(&f, 0);
        for (unsigned i = 0; i < 8; ++i)CHECK(!seen[i]);
        inputs(&f);
        if (session_action ==
            1) CHECK(!wl_evaluation_control_configure(f.control,
                UINT64_MAX-1));
        int rc = complete(f.session);
        wl_columnar_expr_test_checkpoint = NULL;
        CHECK(session_stopped &&
            rc == (session_action == 0?WL_EVALUATION_CONTROL_CANCELLED:
            session_action == 1?WL_EVALUATION_CONTROL_WORK_LIMIT:EINVAL));
        contents(&f, 0); CHECK(!f.events);
        if (COL_SESSION(f.session)->cleanup_active ||
            COL_SESSION(f.session)->cleanup_pending ||
            COL_SESSION(f.session)->cleanup_reserved_bytes)
            fprintf(stderr,
                "crypto action=%u rc=%d active=%u pending=%u reserved=%llu native=%u\n",
                session_action, rc,
                COL_SESSION(f.session)->cleanup_active != NULL,
                COL_SESSION(f.session)->cleanup_pending != NULL,
                (unsigned long long)COL_SESSION(
                    f.session)->cleanup_reserved_bytes,
                wl_columnar_session_test_native_error_phase(COL_SESSION(
                    f.session)));
        CHECK(!COL_SESSION(f.session)->cleanup_active &&
            !COL_SESSION(f.session)->cleanup_pending &&
            !COL_SESSION(f.session)->cleanup_reserved_bytes);
        if (session_action == 2) {
            uint64_t used = 999; wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                session_successor, &used) && !used);
            CHECK(!wl_evaluation_control_report(f.control,
                &report) && report.attempt_id == session_report.attempt_id &&
                report.consumed == session_report.consumed &&
                report.result == session_report.result);
            wl_evaluation_control_attempt_finish_result_t end =
                wl_evaluation_control_finish_attempt(f.control,
                    f.session->operation_admission, session_successor, 0, 0);
            CHECK(!end.protocol && end.report_valid && !end.report.consumed);
        }
        CHECK(!wl_evaluation_control_reset(f.control));
        memset(seen, 0, sizeof(seen)); active = NULL;
        wl_columnar_expr_test_checkpoint = session_hook;
        CHECK(!complete(f.session)); wl_columnar_expr_test_checkpoint = NULL;
        contents(&f, 3);
        CHECK(f.events == 3 && f.positives == 3 && !f.negatives);
        for (unsigned i = 0; i < 8; ++i)CHECK(seen[i] >= 3);
        CHECK(!wl_session_remove(f.session, "input", f.input[0], 1, 2));
        CHECK(!complete(f.session)); contents(&f, 2);
        CHECK(f.events == 4 && f.negatives == 1 && !f.balance[0]);
        CHECK(!complete(f.session) && f.events == 4); destroy(&f);
    }
#else
    (void)map_source;
    const char *fn[] = {"md5", "sha1", "sha256", "sha512"};
    for (unsigned i = 0; i < 8; ++i) {
        char source[512]; snprintf(source, sizeof(source),
            ".decl input(i:int64,s:string)\n.decl out(i:int64,n:int64,k:int64)\n"
            "out(i,%s(%s),i+1) :- input(i,s).\n", fn[i/2], i%2?"s":"i");
        fixture_t f = create(source);
        wl_session_set_delta_cb(f.session, capture, &f);
        CHECK(!complete(f.session)); inputs(&f); memset(seen, 0, sizeof(seen));
        wl_columnar_expr_test_checkpoint = session_hook;
        CHECK(complete(f.session) == ERANGE);
        wl_columnar_expr_test_checkpoint = NULL;
        col_rel_t *out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out && !out->nrows && seen[i] && !f.events);
        fprintf(stderr,
            "crypto off tag=%u active=%u pending=%u reserved=%llu native=%u\n",
            i, COL_SESSION(f.session)->cleanup_active != NULL,
            COL_SESSION(f.session)->cleanup_pending != NULL,
            (unsigned long long)COL_SESSION(f.session)->cleanup_reserved_bytes,
            wl_columnar_session_test_native_error_phase(COL_SESSION(
                f.session)));
        CHECK(!COL_SESSION(f.session)->cleanup_active &&
            !COL_SESSION(f.session)->cleanup_pending &&
            !COL_SESSION(f.session)->cleanup_reserved_bytes);
        for (unsigned r = 0; r < 3; ++r) {
            int rc = wl_session_remove(f.session, "input", f.input[r], 1, 2);
            if (rc)fprintf(stderr, "off remove tag=%u row=%u rc=%d\n", i, r,
                    rc);
            CHECK(!rc);
        }
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!complete(f.session));
        out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out && !out->nrows && !complete(f.session)); destroy(&f);
    }
#endif
}
static void session_filters(void) {
    const char *fn[] = {"md5", "sha1", "sha256", "sha512"};
    for (unsigned i = 0; i < 8; ++i) {
        fixture_t reference = create(map_source);
        int64_t expected = reference.expected[0][i];
#ifdef WL_MBEDTLS_ENABLED
        CHECK(expected != reference.expected[1][i] &&
            expected != reference.expected[2][i]);
#endif
        destroy(&reference);
        char source[512]; snprintf(source, sizeof(source),
            ".decl input(i:int64,s:string)\n.decl out(i:int64,s:string)\n"
            "out(i,s) :- input(i,s), %s(%s) = %" PRId64 ".\n", fn[i/2],
            i%2?"s":"i", expected);
        fixture_t f = create(source); f.filter = true;
        wl_session_set_delta_cb(f.session, capture, &f);
        CHECK(!complete(f.session)); contents(&f, 0); inputs(&f);
        memset(seen, 0, sizeof(seen)); active = NULL;
        wl_columnar_expr_test_checkpoint = session_hook;
        CHECK(!complete(f.session)); wl_columnar_expr_test_checkpoint = NULL;
        CHECK(seen[i] >= 3);
#ifdef WL_MBEDTLS_ENABLED
        contents(&f, 1);
        CHECK(f.events == 1 && f.positives == 1 && !f.negatives);
        CHECK(!wl_session_remove(f.session, "input", f.input[0], 1, 2));
        CHECK(!complete(f.session)); contents(&f, 0);
        CHECK(f.events == 2 && f.negatives == 1 && !f.balance[0]);
        CHECK(!complete(f.session) && f.events == 2);
#else
        contents(&f, 0); CHECK(!f.events && !complete(f.session));
#endif
        destroy(&f);
    }
}
#ifdef WL_MBEDTLS_ENABLED
extern unsigned wl_columnar_session_test_cleanup_owners(
    const wl_col_session_t *);
extern void (*wl_columnar_session_test_source_tail)(const wl_col_session_t *,
    const wl_session_source_finance_t *);
static unsigned mixed_owner_reads;
static void mixed_source_tail(const wl_col_session_t *sess,
    const wl_session_source_finance_t *source) {
    unsigned owners = wl_columnar_session_test_cleanup_owners(sess);
    CHECK(owners != 3);
    if (source->charged == 96 && owners == 2) ++mixed_owner_reads;
}
static void mixed_checkpoint(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    if (phase == 204 && active)
        CHECK(wl_columnar_session_test_cleanup_owners(
                COL_SESSION(active->session)) == 2);
    session_hook(c, phase, progress, failed);
}
static void mixed_capture(const char *name, const int64_t *row,
    uint32_t width, int32_t diff, void *arg) {
    if (!strcmp(name, "side")) {
        CHECK(width == 1 && row[0] == 1 && diff == 1); return;
    }
    capture(name, row, width, diff, arg);
}
static void mixed_session_maps(void) {
    for (session_action = 0; session_action < 3; ++session_action) {
        char source[1024];
        snprintf(source, sizeof(source), "%s\n.decl a(x:int64)\n"
            ".decl b(x:int64)\n.decl c(x:int64)\n.decl side(x:int64)\n"
            "side(x) :- a(x),b(x),c(x).\n", map_source);
        fixture_t f = create(source);
        wl_session_set_delta_cb(f.session, mixed_capture, &f);
        int64_t one = 1; const char *names[] = {"a", "b", "c"};
        for (unsigned i = 0; i < 3; ++i)
            CHECK(!wl_session_insert(f.session, names[i], &one, 1, 1));
        CHECK(!wl_session_insert(f.session, "input", f.input[0], 1, 2));
        CHECK(!complete(f.session) && f.session->source_c1_admission);
        contents(&f, 1); CHECK(f.events == 1);
        for (unsigned r = 1; r < 3; ++r)
            CHECK(!wl_session_insert(f.session, "input", f.input[r], 1, 2));
        active = &f; memset(seen, 0, sizeof(seen)); mixed_owner_reads = 0;
        session_stopped = false; session_successor = 0;
        if (session_action == 1)
            CHECK(!wl_evaluation_control_configure(f.control, UINT64_MAX-1));
        wl_columnar_session_test_source_tail = mixed_source_tail;
        wl_columnar_expr_test_checkpoint = mixed_checkpoint;
        int rc = complete(f.session);
        wl_columnar_expr_test_checkpoint = NULL;
        wl_columnar_session_test_source_tail = NULL;
        CHECK(session_stopped && mixed_owner_reads && rc ==
            (session_action == 0 ? WL_EVALUATION_CONTROL_CANCELLED :
            session_action == 1 ? WL_EVALUATION_CONTROL_WORK_LIMIT : EINVAL));
        contents(&f, 1); CHECK(f.events == 1);
        CHECK(!COL_SESSION(f.session)->cleanup_active &&
            !COL_SESSION(f.session)->cleanup_pending &&
            !COL_SESSION(f.session)->cleanup_reserved_bytes);
        if (session_action == 2) {
            wl_evaluation_control_report_t report; uint64_t used = UINT64_MAX;
            CHECK(!wl_evaluation_control_report(f.control, &report));
            CHECK(report.attempt_id == session_report.attempt_id &&
                report.limit == session_report.limit &&
                report.consumed == session_report.consumed &&
                report.stop_reason == session_report.stop_reason &&
                report.execution_status == session_report.execution_status &&
                report.cleanup_status == session_report.cleanup_status &&
                report.result == session_report.result);
            CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                session_successor, &used) && !used);
            wl_evaluation_control_attempt_finish_result_t end =
                wl_evaluation_control_finish_attempt(f.control,
                    f.session->operation_admission, session_successor, 0, 0);
            CHECK(!end.protocol && end.report_valid && !end.report.consumed);
        }
        CHECK(!wl_evaluation_control_reset(f.control)); active = NULL;
        CHECK(!complete(f.session)); contents(&f, 3);
        CHECK(f.events == 3 && f.positives == 3 && !f.negatives);
        CHECK(!wl_session_remove(f.session, "input", f.input[0], 1, 2));
        CHECK(!complete(f.session)); contents(&f, 2);
        CHECK(f.events == 4 && f.negatives == 1 && !f.balance[0]);
        col_rel_t *side = session_find_rel(COL_SESSION(f.session), "side");
        CHECK(side && side->nrows == 1 && side->ncols == 1 &&
            side->column_types[0] == WIRELOG_TYPE_INT64 &&
            side->columns[0][0] == 1);
        CHECK(!complete(f.session) && f.events == 4); destroy(&f);
    }
}
#endif

static const uint8_t hmac_tags[] = {WL_PLAN_EXPR_ARITH_HMAC_SHA256,
                                    WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS,
                                    WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI,
                                    WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS};
static uint32_t hmac_bytecode(uint8_t *bytes, int64_t message, int64_t key,
    uint8_t tag) {
    bytes[0] = WL_PLAN_EXPR_CONST_INT; memcpy(bytes+1, &message, 8);
    bytes[9] = WL_PLAN_EXPR_CONST_INT; memcpy(bytes+10, &key, 8);
    bytes[18] = tag; return 19;
}
#ifdef WL_MBEDTLS_ENABLED
extern psa_status_t (*wl_columnar_expr_crypto_hmac_test_init)(void);
extern psa_status_t (*wl_columnar_expr_crypto_hmac_test_import)(
    const psa_key_attributes_t *, const uint8_t *, size_t, psa_key_id_t *);
extern void (*wl_columnar_expr_crypto_hmac_test_reset)(psa_key_attributes_t *);
extern psa_status_t (*wl_columnar_expr_crypto_hmac_test_mac)(psa_key_id_t,
    psa_algorithm_t, const uint8_t *, size_t, uint8_t *, size_t, size_t *);
extern psa_status_t (*wl_columnar_expr_crypto_hmac_test_destroy)(psa_key_id_t);
enum {H_REAL, H_INIT_FAIL, H_IMPORT_FAIL, H_MAC_FAIL, H_SHORT, H_LONG,
      H_DESTROY_FAIL, H_IMPORT_MEMORY, H_MAC_MEMORY};
static unsigned h_fault, h_init_calls, h_import_calls, h_reset_calls,
    h_mac_calls, h_destroy_calls;
static const uint8_t *h_message, *h_key;
static size_t h_message_length, h_key_length;
static psa_key_id_t h_live_key;
static psa_status_t h_init(void) {
    ++h_init_calls;
    return h_fault == H_INIT_FAIL ? PSA_ERROR_NOT_SUPPORTED : psa_crypto_init();
}
static psa_status_t h_import(const psa_key_attributes_t *attributes,
    const uint8_t *key, size_t length, psa_key_id_t *id) {
    CHECK(!h_import_calls++ && !h_live_key && !*id);
    CHECK(psa_get_key_type(attributes) == PSA_KEY_TYPE_HMAC &&
        psa_get_key_usage_flags(attributes) == PSA_KEY_USAGE_SIGN_MESSAGE &&
        psa_get_key_algorithm(attributes) == PSA_ALG_HMAC(PSA_ALG_SHA_256) &&
        !psa_get_key_bits(attributes));
    if (!h_key_length) CHECK(length == 1 && key && !*key);
    else CHECK(key == h_key && length == h_key_length);
    if (h_fault == H_IMPORT_FAIL) return PSA_ERROR_NOT_SUPPORTED;
    if (h_fault == H_IMPORT_MEMORY) return PSA_ERROR_INSUFFICIENT_MEMORY;
    psa_status_t rc = psa_import_key(attributes, key, length, id);
    if (rc == PSA_SUCCESS) h_live_key = *id;
    return rc;
}
static void h_reset(psa_key_attributes_t *attributes) {
    CHECK(!h_reset_calls++); psa_reset_key_attributes(attributes);
}
static psa_status_t h_mac(psa_key_id_t key, psa_algorithm_t algorithm,
    const uint8_t *message, size_t length, uint8_t *digest, size_t capacity,
    size_t *actual) {
    CHECK(!h_mac_calls++ && h_live_key && key == h_live_key &&
        algorithm == PSA_ALG_HMAC(PSA_ALG_SHA_256) && message == h_message &&
        length == h_message_length && capacity == 32 && !*actual &&
        h_reset_calls == 1);
    if (h_fault == H_MAC_FAIL) return PSA_ERROR_NOT_SUPPORTED;
    if (h_fault == H_MAC_MEMORY) return PSA_ERROR_INSUFFICIENT_MEMORY;
    psa_status_t rc = psa_mac_compute(key, algorithm, message, length,
            digest, capacity, actual);
    CHECK(rc == PSA_SUCCESS && *actual == 32);
    if (h_fault == H_SHORT) --*actual;
    if (h_fault == H_LONG) ++*actual;
    return rc;
}
static psa_status_t h_destroy(psa_key_id_t key) {
    CHECK(!h_destroy_calls++ && h_live_key && key == h_live_key);
    CHECK(psa_destroy_key(key) == PSA_SUCCESS); h_live_key = 0;
    return h_fault == H_DESTROY_FAIL ? PSA_ERROR_GENERIC_ERROR : PSA_SUCCESS;
}
static void h_shim(unsigned fault, const uint8_t *message,
    size_t message_length,
    const uint8_t *key, size_t key_length) {
    CHECK(!h_live_key); h_fault = fault;
    h_init_calls = h_import_calls = h_reset_calls = h_mac_calls =
        h_destroy_calls = 0;
    h_message = message; h_message_length = message_length;
    h_key = key; h_key_length = key_length;
    wl_columnar_expr_crypto_hmac_test_init = h_init;
    wl_columnar_expr_crypto_hmac_test_import = h_import;
    wl_columnar_expr_crypto_hmac_test_reset = h_reset;
    wl_columnar_expr_crypto_hmac_test_mac = h_mac;
    wl_columnar_expr_crypto_hmac_test_destroy = h_destroy;
}
static void h_unshim(void) {
    CHECK(!h_live_key);
    wl_columnar_expr_crypto_hmac_test_init = NULL;
    wl_columnar_expr_crypto_hmac_test_import = NULL;
    wl_columnar_expr_crypto_hmac_test_reset = NULL;
    wl_columnar_expr_crypto_hmac_test_mac = NULL;
    wl_columnar_expr_crypto_hmac_test_destroy = NULL;
}
/* Original provider route is the reference, including empty-key substitution. */
static bool h_reference(const uint8_t *message, size_t message_length,
    const uint8_t *key, size_t key_length, int64_t *value) {
    psa_key_attributes_t attributes = PSA_KEY_ATTRIBUTES_INIT;
    if (psa_crypto_init() != PSA_SUCCESS) return false;
    psa_set_key_type(&attributes, PSA_KEY_TYPE_HMAC);
    psa_set_key_usage_flags(&attributes, PSA_KEY_USAGE_SIGN_MESSAGE);
    psa_set_key_algorithm(&attributes, PSA_ALG_HMAC(PSA_ALG_SHA_256));
    static const uint8_t empty[1] = {0};
    if (!key_length) {
        key = empty; key_length = 1;
    }
    psa_key_id_t id = 0;
    psa_status_t status = psa_import_key(&attributes, key, key_length, &id);
    psa_reset_key_attributes(&attributes);
    if (status != PSA_SUCCESS) return false;
    uint8_t digest[32]; size_t actual = 0;
    status = psa_mac_compute(id, PSA_ALG_HMAC(PSA_ALG_SHA_256), message,
            message_length, digest, sizeof(digest), &actual);
    bool valid = status == PSA_SUCCESS && actual == sizeof(digest);
    CHECK(psa_destroy_key(id) == PSA_SUCCESS);
    if (valid) *value = (int64_t)XXH3_64bits(digest, sizeof(digest));
    return valid;
}
#endif
static void hmac_oracles(void) {
    const uint8_t message[] = "Hi There"; uint8_t key[20];
    memset(key, 0x0b, sizeof(key));
#ifdef WL_MBEDTLS_ENABLED
    const char *hex =
        "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7";
    uint8_t expected_digest[32];
    for (unsigned i = 0; i < 32; ++i) {
        unsigned byte;
        CHECK(sscanf(hex+2*i, "%2x", &byte) == 1);
        expected_digest[i] = (uint8_t)byte;
    }
    int64_t expected = (int64_t)XXH3_64bits(expected_digest, 32), reference = 0;
    CHECK(h_reference(message, sizeof(message)-1, key, sizeof(key), &reference)
        && reference == expected);
    for (unsigned fault = H_REAL; fault <= H_MAC_MEMORY; ++fault) {
        uint64_t tariff = fault == H_REAL ? 2912 : fault == H_INIT_FAIL ? 672 :
            (fault == H_IMPORT_FAIL || fault == H_IMPORT_MEMORY) ? 1344 : 1792;
#else
    uint64_t tariff = 448; int64_t expected = 0;
    {
#endif
        for (unsigned below = 0; below < 2; ++below) {
            wl_evaluation_control_t *c = begin(tariff-below);
#ifdef WL_MBEDTLS_ENABLED
            h_shim(fault, message, sizeof(message)-1, key, sizeof(key));
#endif
            int64_t value = 999; bool failed = false;
            int rc = wl_columnar_expr_crypto_hmac_attempt_controlled(message,
                    sizeof(message)-1, key, sizeof(key), &value, &failed, c,
                    wl_evaluation_control_active_attempt_id(c));
            CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
#ifdef WL_MBEDTLS_ENABLED
            CHECK(value == (below ? 999 : fault == H_REAL ? expected : 0));
            CHECK(failed == (!below && fault != H_REAL));
            CHECK(h_init_calls == 1 &&
                h_import_calls == (fault != H_INIT_FAIL) &&
                h_reset_calls == (fault != H_INIT_FAIL) &&
                h_mac_calls == (fault != H_INIT_FAIL &&
                fault != H_IMPORT_FAIL && fault != H_IMPORT_MEMORY) &&
                h_destroy_calls == h_mac_calls); h_unshim();
#else
            CHECK(value == (below ? 999 : expected) && failed == !below);
#endif
            finish(c, rc);
        }
    }
#ifdef WL_MBEDTLS_ENABLED
    const size_t sizes[] = {0, 1, 20, 63, 64, 65, 4096}; uint8_t large[4096];
    memset(large, 'k', sizeof(large));
    for (unsigned i = 0; i < sizeof(sizes)/sizeof(*sizes); ++i) {
        int64_t reference_value = 0;
        bool valid = h_reference(message, sizeof(message)-1, large, sizes[i],
                &reference_value);
        wl_evaluation_control_t *c = begin(0); int64_t value = 999;
        bool failed = false;
        h_shim(H_REAL, message, sizeof(message)-1, large, sizes[i]);
        CHECK(!wl_columnar_expr_crypto_hmac_attempt_controlled(message,
            sizeof(message)-1, large, sizes[i], &value, &failed, c,
            wl_evaluation_control_active_attempt_id(c)));
        CHECK(failed == !valid && value == (valid ? reference_value : 0));
        h_unshim(); finish(c, 0);
    }
#endif
}

static void hmac_scalar_parity(void) {
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    char large[4097]; memset(large, 'a', 4096); large[4096] = 0;
    const int64_t inputs[] = {0, 1, -1, INT64_MIN, INT64_MAX,
                              wl_intern_put(intern, "abc"),
                              wl_intern_put(intern, ""),
                              wl_intern_put(intern, large),
                              wl_intern_put(intern, "\x80\xff"),
                              wl_intern_put(intern, "a\0ignored")};
    for (unsigned tag = 0; tag < 4; ++tag)
        for (unsigned v = 0; v < sizeof(inputs)/sizeof(*inputs); ++v)
            for (unsigned null = 0; null < 2; ++null) {
                uint8_t bytes[24]; uint32_t n = hmac_bytecode(bytes, inputs[v],
                        inputs[(v+3) % (sizeof(inputs)/sizeof(*inputs))],
                        hmac_tags[tag]);
                wl_columnar_expr_context_t context =
                {.intern = null ? NULL : intern};
                int64_t raw = 999, value = 999;
                wl_columnar_expr_status_t raw_status, status;
                int raw_rc = wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0,
                        &raw, &context, &raw_status);
                wl_evaluation_control_t *c = begin(0);
                bool supported = false, failed = false;
                wl_columnar_expr_attempt_scalar_t *program = NULL;
                CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes,
                    n, NULL, &program, &supported, c,
                    wl_evaluation_control_active_attempt_id(c)) && supported);
                wirelog_column_type_t type = WIRELOG_TYPE_BOOL;
                CHECK(!wl_columnar_expr_scalar_result_type_attempt_controlled(
                        program, NULL, 0, &type, &supported, c,
                        wl_evaluation_control_active_attempt_id(c)) &&
                    supported &&
                    type == WIRELOG_TYPE_INT64);
                scan_calls = 0; wl_columnar_expr_test_checkpoint = scan_hook;
                CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program,
                    NULL, 0, &context, &value, &failed, &status, c,
                    wl_evaluation_control_active_attempt_id(c)));
                wl_columnar_expr_test_checkpoint = NULL;
                CHECK(value == raw && failed == (raw_rc != 0) &&
                    status == raw_status);
#ifndef WL_MBEDTLS_ENABLED
                CHECK(!scan_calls);
#endif
                wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
            }
    wl_intern_free(intern);
}

static unsigned h_stop_phase, h_stop_action;
static bool h_stopped;
static uint64_t h_successor;
static wl_evaluation_control_report_t h_report;
static void h_stop(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)progress; (void)failed;
    if (phase != h_stop_phase || h_stopped) return;
    h_stopped = true;
    if (h_stop_action == 0) wl_evaluation_control_request_cancel(c);
    else if (h_stop_action == 1)
        CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else if (h_stop_action == 3) {
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
        wl_evaluation_control_request_cancel(c);
    } else {
        CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
        CHECK(!wl_evaluation_control_report(c, &h_report));
        CHECK(!wl_evaluation_control_reset(c) && !wl_evaluation_control_begin(c,
            &owner));
        h_successor = wl_evaluation_control_active_attempt_id(c);
        CHECK(h_successor != h_report.attempt_id);
    }
}
static void hmac_stops(void) {
    const uint8_t message[] = "abc", key[] = "key";
#ifdef WL_MBEDTLS_ENABLED
    const unsigned phases[] = {230, 231, 232, 233, 234, 235, 236, 237, 238, 239,
                               240, 241};
#else
    const unsigned phases[] = {230, 241};
#endif
#ifdef WL_MBEDTLS_ENABLED
    const unsigned faults[] = {H_REAL, H_MAC_FAIL, H_DESTROY_FAIL,
                               H_IMPORT_FAIL};
#else
    const unsigned faults[] = {0};
#endif
    for (unsigned scenario = 0; scenario < sizeof(faults)/sizeof(*faults);
        ++scenario)
        for (unsigned p = 0; p < sizeof(phases)/sizeof(*phases); ++p)
            for (h_stop_action = 0; h_stop_action < 4; ++h_stop_action) {
                if (scenario &&
                    phases[p] !=
                    (scenario == 1 ? 238 : scenario == 2 ? 240 : 236)) continue;
                h_stop_phase = phases[p]; h_stopped = false; h_successor = 0;
                wl_evaluation_control_t *c = begin(h_stop_action ==
                        1 ? UINT64_MAX-1 : 0);
#ifdef WL_MBEDTLS_ENABLED
                h_shim(faults[scenario], message, 3, key, 3);
#endif
                int64_t value = 999; bool failed = false;
                wl_columnar_expr_test_checkpoint = h_stop;
                int rc =
                    wl_columnar_expr_crypto_hmac_attempt_controlled(message,
                        3, key, 3, &value, &failed, c,
                        wl_evaluation_control_active_attempt_id(c));
                wl_columnar_expr_test_checkpoint = NULL;
                CHECK(h_stopped && value == 999 && !failed && rc ==
                    (h_stop_action == 0 ? WL_EVALUATION_CONTROL_CANCELLED :
                    h_stop_action == 1 ? WL_EVALUATION_CONTROL_WORK_LIMIT :
                    h_stop_action == 3 ? EOVERFLOW : EINVAL));
#ifdef WL_MBEDTLS_ENABLED
                CHECK(h_destroy_calls == (h_stop_phase >= 236 &&
                    scenario != 3));
                CHECK(h_reset_calls == (h_stop_phase >= 235));
                CHECK(h_import_calls == (h_stop_phase >= 236));
                CHECK(h_mac_calls == (h_stop_phase >= 238 && scenario != 3));
                h_unshim();
#endif
                if (h_successor) {
                    uint64_t used = UINT64_MAX;
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_test_active_consumed(c,
                        h_successor, &used) && !used);
                    CHECK(!wl_evaluation_control_report(c, &report) &&
                        report.attempt_id == h_report.attempt_id &&
                        report.limit == h_report.limit &&
                        report.consumed == h_report.consumed &&
                        report.stop_reason == h_report.stop_reason &&
                        report.result == h_report.result &&
                        report.execution_status == h_report.execution_status &&
                        report.cleanup_status == h_report.cleanup_status);
                    CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
                } else {
                    CHECK(wl_evaluation_control_finish(c, &owner,
                        rc > 0 ? rc : 0, 0) == rc);
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(c,
                        &report) && report.result == rc &&
                        report.execution_status == (rc > 0 ? rc : 0) &&
                        !report.cleanup_status);
                }
                CHECK(!wl_evaluation_control_reset(c) &&
                    !wl_evaluation_control_begin(c, &owner));
                CHECK(!wl_columnar_expr_crypto_hmac_attempt_controlled(message,
                    3,
                    key, 3, &value, &failed, c,
                    wl_evaluation_control_active_attempt_id(c)));
#ifdef WL_MBEDTLS_ENABLED
                CHECK(!failed);
#else
                CHECK(failed && !value);
#endif
                finish(c, 0);
            }
}

static void hmac_prefixes(void) {
#ifdef WL_MBEDTLS_ENABLED
    const uint8_t message[] = "abc", key[] = "key";
    for (unsigned prefix = 1; prefix <= 13; ++prefix) {
        uint64_t cap = UINT64_C(224)*prefix-1;
        wl_evaluation_control_t *c = begin(cap);
        h_shim(H_REAL, message, 3, key, 3);
        int64_t value = 999; bool failed = false;
        CHECK(wl_columnar_expr_crypto_hmac_attempt_controlled(message, 3, key,
            3,
            &value, &failed, c, wl_evaluation_control_active_attempt_id(c)) ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(value == 999 && !failed && h_init_calls == (cap >= 448) &&
            h_import_calls == (cap >= 1120) && h_reset_calls == (cap >= 896) &&
            h_mac_calls == (cap >= 1344) && h_destroy_calls == (cap >= 1120));
        h_unshim(); finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
    }
#endif
}

static int64_t h_expected[3][4];
static unsigned h_seen[4];
static const char *h_map_source =
    ".decl input(i:int64,s:string)\n.decl out(i:int64,s:string,a:int64,b:int64,c:int64,d:int64)\n"
    "out(i,s,hmac_sha256(i,i),hmac_sha256(s,s),hmac_sha256(s,i),hmac_sha256(i,s)) :- input(i,s).\n";
static fixture_t h_create(const char *source) {
    fixture_t f = create(source);
    /* Large native/symbol spans and provider limits are independent parity
     * cases; these accepted host controls use provider-portable short keys. */
    f.input[2][1] = wl_intern_put(f.plan->intern, "third");
    for (unsigned r = 0; r < 3; ++r)
        for (unsigned tag = 0; tag < 4; ++tag) {
            uint8_t bytes[24]; bool msg_symbol = tag == 1 || tag == 2;
            bool key_symbol = tag == 1 || tag == 3;
            uint32_t n = hmac_bytecode(bytes, f.input[r][msg_symbol],
                    f.input[r][key_symbol], hmac_tags[tag]);
            wl_columnar_expr_context_t context = {.intern = f.plan->intern};
            wl_columnar_expr_status_t status;
            int rc = wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0,
                    &h_expected[r][tag], &context, &status);
#ifdef WL_MBEDTLS_ENABLED
            CHECK(!rc && status == WL_COLUMNAR_EXPR_OK);
#else
            CHECK(rc && status == WL_COLUMNAR_EXPR_MALFORMED &&
                !h_expected[r][tag]);
#endif
        }
    return f;
}
static void h_capture(const char *name, const int64_t *row, uint32_t width,
    int32_t diff, void *arg) {
    fixture_t *f = arg;
    if (!strcmp(name, "side")) {
        CHECK(width == 1 && row[0] == 1 && diff == 1); return;
    }
    CHECK(!strcmp(name, "out") && (diff == 1 || diff == -1));
    CHECK(row[0] >= 1 && row[0] <= 3); unsigned r = (unsigned)(row[0]-1);
    CHECK(row[1] == f->input[r][1] && width == (f->filter ? 2 : 6));
    if (f->filter) CHECK(!r);
    else for (unsigned tag = 0; tag < 4;
            ++tag) CHECK(row[tag+2] == h_expected[r][tag]);
    f->balance[r] += diff; CHECK(f->balance[r] >= 0 && f->balance[r] <= 1);
    ++f->events; f->positives += diff == 1; f->negatives += diff == -1;
}
static void h_contents(fixture_t *f, unsigned count) {
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->nrows == count && out->ncols == (f->filter ? 2 : 6) &&
        out->column_types[0] == WIRELOG_TYPE_INT64 &&
        out->column_types[1] == WIRELOG_TYPE_STRING);
    bool visited[3] = {false, false, false};
    for (uint32_t r = 0; r < out->nrows; ++r) {
        int64_t i = out->columns[0][r];
        CHECK(i >= 1 && i <= 3 && !visited[i-1]);
        visited[i-1] = true; CHECK(out->columns[1][r] == f->input[i-1][1]);
        if (f->filter) CHECK(i == 1);
        else for (unsigned tag = 0; tag < 4; ++tag)
                CHECK(out->column_types[tag+2] == WIRELOG_TYPE_INT64 &&
                    out->columns[tag+2][r] == h_expected[i-1][tag]);
    }
}
static void h_host_hook(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)failed;
    if (phase == 242) {
        unsigned tag; for (tag = 0; tag < 4; ++tag)
            if (progress == hmac_tags[tag]) {
                ++h_seen[tag]; break;
            }
        CHECK(tag < 4);
    }
#ifdef WL_MBEDTLS_ENABLED
    unsigned evaluations = 0;
    for (unsigned tag = 0; tag < 4; ++tag) evaluations += h_seen[tag];
    if (!active || !c || session_stopped || phase != 238 ||
        evaluations < 5) return;
    CHECK(wl_columnar_session_test_cleanup_owners(COL_SESSION(
            active->session)) == 2);
    session_stopped = true;
    if (session_action == 0) wl_evaluation_control_request_cancel(c);
    else if (session_action == 1)
        CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else {
        wl_evaluation_control_attempt_finish_result_t r =
            wl_evaluation_control_finish_attempt(c,
                active->session->operation_admission,
                wl_evaluation_control_active_attempt_id(c), 0, 0);
        CHECK(!r.protocol && r.report_valid && r.report.consumed &&
            !r.report.result);
        session_report = r.report; CHECK(!wl_evaluation_control_reset(c));
        wl_evaluation_control_attempt_begin_result_t b =
            wl_evaluation_control_begin_attempt(c,
                active->session->operation_admission);
        CHECK(!b.protocol && b.attempt_id != r.report.attempt_id);
        session_successor = b.attempt_id;
    }
#else
    (void)c;
#endif
}

static void hmac_session_maps(void) {
#ifdef WL_MBEDTLS_ENABLED
    for (unsigned mixed = 0; mixed < 2; ++mixed)
        for (session_action = 0; session_action < 3; ++session_action) {
            char source[1024];
            snprintf(source, sizeof(source), "%s%s", h_map_source,
                mixed ? ".decl a(x:int64)\n.decl b(x:int64)\n.decl c(x:int64)\n"
                ".decl side(x:int64)\nside(x) :- a(x),b(x),c(x).\n" : "");
            fixture_t f = h_create(source);
            wl_session_set_delta_cb(f.session, h_capture, &f);
            if (mixed) {
                int64_t one = 1; const char *names[] = {"a", "b", "c"};
                for (unsigned i = 0; i < 3;
                    ++i) CHECK(!wl_session_insert(f.session, names[i], &one, 1,
                        1));
                CHECK(!wl_session_insert(f.session, "input", f.input[0], 1, 2));
            }
            CHECK(!complete(f.session)); h_contents(&f, mixed);
            if (mixed) CHECK(f.session->source_c1_admission);
            for (unsigned r = mixed; r < 3; ++r)
                CHECK(!wl_session_insert(f.session, "input", f.input[r], 1, 2));
            active = &f; memset(h_seen, 0, sizeof(h_seen));
            session_stopped = false; session_successor = 0;
            mixed_owner_reads = 0;
            if (session_action ==
                1) CHECK(!wl_evaluation_control_configure(f.control,
                    UINT64_MAX-1));
            wl_columnar_expr_test_checkpoint = h_host_hook;
            wl_columnar_session_test_source_tail = mixed_source_tail;
            int rc = complete(f.session);
            wl_columnar_expr_test_checkpoint = NULL;
            wl_columnar_session_test_source_tail = NULL;
            CHECK(session_stopped && mixed_owner_reads && rc ==
                (session_action == 0 ? WL_EVALUATION_CONTROL_CANCELLED :
                session_action ==
                1 ? WL_EVALUATION_CONTROL_WORK_LIMIT : EINVAL));
            h_contents(&f, mixed); CHECK(f.events == mixed);
            CHECK(!COL_SESSION(f.session)->cleanup_active &&
                !COL_SESSION(f.session)->cleanup_pending &&
                !COL_SESSION(f.session)->cleanup_reserved_bytes);
            if (session_action == 2) {
                wl_evaluation_control_report_t report;
                uint64_t used = UINT64_MAX;
                CHECK(!wl_evaluation_control_report(f.control, &report) &&
                    report.attempt_id == session_report.attempt_id &&
                    report.limit == session_report.limit &&
                    report.consumed == session_report.consumed &&
                    report.result == session_report.result &&
                    report.stop_reason == session_report.stop_reason &&
                    report.execution_status ==
                    session_report.execution_status &&
                    report.cleanup_status == session_report.cleanup_status);
                CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                    session_successor, &used) && !used);
                wl_evaluation_control_attempt_finish_result_t r =
                    wl_evaluation_control_finish_attempt(f.control,
                        f.session->operation_admission, session_successor, 0,
                        0);
                CHECK(!r.protocol && r.report_valid && !r.report.consumed);
            }
            CHECK(!wl_evaluation_control_reset(f.control)); active = NULL;
            memset(h_seen, 0, sizeof(h_seen));
            wl_columnar_expr_test_checkpoint = h_host_hook;
            CHECK(!complete(f.session));
            wl_columnar_expr_test_checkpoint = NULL;
            h_contents(&f, 3);
            CHECK(f.events == 3 && f.positives == 3 && !f.negatives);
            for (unsigned tag = 0; tag < 4; ++tag) CHECK(h_seen[tag] >= 3);
            CHECK(!wl_session_remove(f.session, "input", f.input[0], 1, 2));
            CHECK(!complete(f.session)); h_contents(&f, 2);
            CHECK(f.events == 4 && f.negatives == 1 && !f.balance[0]);
            if (mixed) {
                col_rel_t *side = session_find_rel(COL_SESSION(f.session),
                        "side");
                CHECK(side && side->nrows == 1 &&
                    side->column_types[0] == WIRELOG_TYPE_INT64 &&
                    side->columns[0][0] == 1);
            }
            CHECK(!complete(f.session) && f.events == 4); destroy(&f);
        }
#else
    for (unsigned tag = 0; tag < 4; ++tag) {
        const char *message[] = {"i", "s", "s", "i"},
            *key[] = {"i", "s", "i", "s"};
        char source[512]; snprintf(source, sizeof(source),
            ".decl input(i:int64,s:string)\n.decl out(i:int64,n:int64)\n"
            "out(i,hmac_sha256(%s,%s)) :- input(i,s).\n", message[tag],
            key[tag]);
        fixture_t f = h_create(source);
        wl_session_set_delta_cb(f.session, h_capture, &f);
        CHECK(!complete(f.session)); inputs(&f);
        memset(h_seen, 0, sizeof(h_seen));
        wl_columnar_expr_test_checkpoint = h_host_hook;
        CHECK(complete(f.session) == ERANGE);
        wl_columnar_expr_test_checkpoint = NULL;
        col_rel_t *out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out && !out->nrows && !f.events && h_seen[tag]);
        CHECK(!COL_SESSION(f.session)->cleanup_active &&
            !COL_SESSION(f.session)->cleanup_pending &&
            !COL_SESSION(f.session)->cleanup_reserved_bytes);
        for (unsigned r = 0; r < 3;
            ++r) CHECK(!wl_session_remove(f.session, "input", f.input[r], 1,
                2));
        CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session));
        CHECK(!complete(f.session) && !f.events); destroy(&f);
    }
#endif
}

static void hmac_session_filters(void) {
    const char *message[] = {"i", "s", "s", "i"}, *key[] = {"i", "s", "i", "s"};
    for (unsigned tag = 0; tag < 4; ++tag) {
        fixture_t reference = h_create(h_map_source);
        int64_t expected = h_expected[0][tag];
#ifdef WL_MBEDTLS_ENABLED
        CHECK(expected != h_expected[1][tag] && expected != h_expected[2][tag]);
#endif
        destroy(&reference);
        char source[512]; snprintf(source, sizeof(source),
            ".decl input(i:int64,s:string)\n.decl out(i:int64,s:string)\n"
            "out(i,s) :- input(i,s), hmac_sha256(%s,%s)=%" PRId64 ".\n",
            message[tag], key[tag], expected);
        fixture_t f = h_create(source); f.filter = true;
        wl_session_set_delta_cb(f.session, h_capture, &f);
        CHECK(!complete(f.session)); h_contents(&f, 0); inputs(&f);
        active = NULL; memset(h_seen, 0, sizeof(h_seen));
        wl_columnar_expr_test_checkpoint = h_host_hook;
        CHECK(!complete(f.session)); wl_columnar_expr_test_checkpoint = NULL;
        CHECK(h_seen[tag] >= 3);
#ifdef WL_MBEDTLS_ENABLED
        h_contents(&f, 1);
        CHECK(f.events == 1 && f.positives == 1 && !f.negatives);
        CHECK(!wl_session_remove(f.session, "input", f.input[0], 1, 2));
        CHECK(!complete(f.session)); h_contents(&f, 0);
        CHECK(f.events == 2 && f.negatives == 1 && !f.balance[0]);
#else
        h_contents(&f, 0); CHECK(!f.events);
#endif
        unsigned events = f.events;
        CHECK(!complete(f.session) && f.events == events);
        destroy(&f);
    }
}

#ifdef WL_MBEDTLS_ENABLED
static bool h_long_stopped;
static void h_long_hook(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    h_host_hook(c, phase, progress, failed);
    if (h_long_stopped || phase != 41 || progress < 512) return;
    CHECK(h_seen[2] >= 9); h_long_stopped = true;
    if (!session_action) wl_evaluation_control_request_cancel(c);
    else CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
}
static void hmac_long_preparation(void) {
    const char *source = ".decl input(i:int64,s:string)\n"
        ".decl out(i:int64,s:string,a:int64,b:int64,c:int64,d:int64)\n"
        "out(i,s,hmac_sha256(s,i),hmac_sha256(s,i),hmac_sha256(s,i),hmac_sha256(s,i)) :- input(i,s).\n";
    for (session_action = 0; session_action < 2; ++session_action) {
        fixture_t f = h_create(source); char large[4097];
        memset(large, 'a', 4096); large[4096] = 0;
        f.input[2][1] = wl_intern_put(f.plan->intern, large);
        for (unsigned r = 0; r < 3; ++r) {
            uint8_t bytes[24]; uint32_t n = hmac_bytecode(bytes,
                    f.input[r][1], f.input[r][0],
                    WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI);
            wl_columnar_expr_context_t context = {.intern = f.plan->intern};
            wl_columnar_expr_status_t status; int64_t value;
            CHECK(!wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0, &value,
                &context, &status));
            for (unsigned tag = 0; tag < 4; ++tag) h_expected[r][tag] = value;
        }
        wl_session_set_delta_cb(f.session, h_capture, &f);
        CHECK(!complete(f.session)); inputs(&f);
        if (session_action) CHECK(!wl_evaluation_control_configure(f.control,
                UINT64_MAX-1));
        active = NULL; memset(h_seen, 0, sizeof(h_seen));
        h_long_stopped = false;
        wl_columnar_expr_test_checkpoint = h_long_hook;
        CHECK(complete(f.session) ==
            (session_action ? WL_EVALUATION_CONTROL_WORK_LIMIT :
            WL_EVALUATION_CONTROL_CANCELLED));
        wl_columnar_expr_test_checkpoint = NULL;
        CHECK(h_long_stopped && !f.events); h_contents(&f, 0);
        CHECK(!COL_SESSION(f.session)->cleanup_active &&
            !COL_SESSION(f.session)->cleanup_pending &&
            !COL_SESSION(f.session)->cleanup_reserved_bytes);
        CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session));
        h_contents(&f, 3);
        CHECK(f.events == 3 && f.positives == 3 && !f.negatives);
        CHECK(!complete(f.session) && f.events == 3); destroy(&f);
    }
}
#endif

static int hmac_output_type_policy(bool controlled) {
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl input(i:int64)\n.decl out(n:string)\n"
        "out(hmac_sha256(i,i)) :- input(i).\n", &error);
    if (!program) {
        CHECK(error != WIRELOG_OK); return (int)error;
    }
    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(program, &plan);
    wl_session_t *session = NULL; wl_evaluation_control_t *control = NULL;
    if (!rc) {
        if (controlled) CHECK(!wl_evaluation_control_create(0, &control));
        wl_session_options_t options; wl_session_options_init(&options);
        options.evaluation_control = control;
        rc = wl_session_create_with_options(wl_backend_columnar(), plan, 1,
                &options, &session);
        if (!rc) {
            int64_t value = 12;
            CHECK(!wl_session_insert(session, "input", &value, 1, 1));
            rc = complete(session);
        }
    }
    if (session) wl_session_destroy(session);
    if (plan) wl_plan_free(plan);
    if (control) wl_evaluation_control_release(control);
    wirelog_program_free(program); return rc;
}
static void hmac_compiler_policy(void) {
    const char *bad[] = {
        ".decl input(i:int64)\n.decl out(n:int64)\nout(hmac_sha256(i)) :- input(i).\n",
        ".decl input(i:int64)\n.decl out(n:int64)\nout(hmac_sha256(i,i,i)) :- input(i).\n"
    };
    for (unsigned i = 0; i < 2; ++i) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(bad[i], &error);
        CHECK(!program && error != WIRELOG_OK);
    }
    int native = hmac_output_type_policy(false);
    CHECK(native && native == hmac_output_type_policy(true));
}

int main(void) {
#ifdef WL_MBEDTLS_ENABLED
    prepare_known();
#endif
    helper_oracles(); helper_stops(); scalar_parity(); session_maps();
    hmac_compiler_policy(); hmac_oracles(); hmac_scalar_parity(); hmac_stops();
    hmac_prefixes();
    hmac_session_maps(); hmac_session_filters();
    session_filters();
#ifdef WL_MBEDTLS_ENABLED
    mixed_session_maps(); hmac_long_preparation();
#endif
    puts("controlled crypto digests: all checks passed"); return 0;
}
