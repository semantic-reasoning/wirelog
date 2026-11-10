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

int main(void) {
#ifdef WL_MBEDTLS_ENABLED
    prepare_known();
#endif
    helper_oracles(); helper_stops(); scalar_parity(); session_maps();
    session_filters();
#ifdef WL_MBEDTLS_ENABLED
    mixed_session_maps();
#endif
    puts("controlled crypto digests: all checks passed"); return 0;
}
