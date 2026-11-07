/* Focused regression for the private financial class and public birth lifetime.
 * Test prices exercise the finite kernel; they are not production source fees. */
#include "wirelog/evaluation_control.h"
#include "wirelog/thread.h"
#include "wirelog/session.h"
#include "wirelog/session_options.h"
#include "wirelog/exec_plan_gen.h"
#include "wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#define CHECK(x) do { if (!(x)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #x); exit(1); \
                      } } while (0)

static const wl_evaluation_control_source_phase_policy_t test_policy = {
    WL_EVALUATION_CONTROL_QUANTUM, WL_EVALUATION_CONTROL_QUANTUM,
    WL_EVALUATION_CONTROL_QUANTUM
};

static int
count_action(void *payload)
{
    ++*(unsigned *)payload;
    return 0;
}

/* Intentional invalid callers: neither probe is a positive SOURCE issuer. */
static wl_evaluation_control_source_context_t *negative_reentry_context;

static int
reentry_action(void *payload)
{
    wl_evaluation_control_source_phase_result_t nested =
        wl_evaluation_control_source_bank_run(negative_reentry_context,
            &test_policy, count_action, payload);
    CHECK(nested.financial_result != 0 && !nested.action_called);
    return 0;
}

typedef struct {
    wl_evaluation_control_source_context_t *context;
    unsigned *calls;
    wl_evaluation_control_source_phase_result_t result;
    int end_result;
} foreign_probe_t;

static void *
foreign_context(void *payload)
{
    foreign_probe_t *p = payload;
    p->result = wl_evaluation_control_source_bank_run(p->context,
            &test_policy, count_action, p->calls);
    p->end_result = wl_evaluation_control_source_context_end(p->context);
    return NULL;
}

static int
real_failed_constructor(void *payload)
{
    wl_evaluation_control_t **out = payload;
    return wl_evaluation_control_create(1, out);
}

static void
same_report(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b)
{
    CHECK(a->attempt_id == b->attempt_id && a->limit == b->limit
        && a->consumed == b->consumed && a->stop_reason == b->stop_reason
        && a->execution_status == b->execution_status
        && a->cleanup_status == b->cleanup_status && a->result == b->result);
}

typedef struct {
    wl_evaluation_control_t *control;
    uint64_t id;
} lexical_birth_t;

static int
lexical_birth(void *payload)
{
    lexical_birth_t *birth = payload;
    int rc = wl_evaluation_control_create(0, &birth->control);
    if (rc) return rc;
    rc = wl_evaluation_control_attach(birth->control, birth);
    if (rc) {
        wl_evaluation_control_release(birth->control);
        birth->control = NULL;
        return rc;
    }
    rc = wl_evaluation_control_begin(birth->control, birth);
    if (rc) {
        CHECK(!wl_evaluation_control_detach(birth->control, birth));
        wl_evaluation_control_release(birth->control);
        birth->control = NULL;
        return rc;
    }
    birth->id = wl_evaluation_control_active_attempt_id(birth->control);
    return birth->id ? 0 : EINVAL;
}

/* Deliberately invalid reentrant SOURCE caller, never a production action. */
static wl_evaluation_control_source_bank_t *negative_lexical_bank;
static int
lexical_reentry(void *payload)
{
    wl_evaluation_control_source_phase_result_t nested =
        wl_evaluation_control_source_bank_run_lexical(negative_lexical_bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            count_action, payload);
    CHECK(nested.financial_result == EBUSY && !nested.action_called);
    return 0;
}

static void
lexical_lifetime(wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_t *original)
{
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(original, &before));
    unsigned calls = 0;
    wl_evaluation_control_source_context_t borrowed = {0};
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &borrowed));
    wl_evaluation_control_source_phase_result_t r =
        wl_evaluation_control_source_bank_run_lexical(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            count_action, &calls);
    CHECK(r.financial_result == EBUSY && !r.action_called && !calls);
    CHECK(!wl_evaluation_control_source_context_end(&borrowed));
    const wl_evaluation_control_source_phase_policy_t invalid[] = {
        {0, 1, 1}, {1, 0, 1}, {1, 1, 0},
        {WL_EVALUATION_CONTROL_QUANTUM + 1, 1, 1},
        {1, WL_EVALUATION_CONTROL_QUANTUM + 1, 1},
        {1, 1, WL_EVALUATION_CONTROL_QUANTUM + 1}
    };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); ++i) {
        r = wl_evaluation_control_source_bank_run_lexical(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &invalid[i],
                count_action, &calls);
        CHECK(r.financial_result == EINVAL && !r.action_called && !calls);
    }
    negative_lexical_bank = bank;
    r = wl_evaluation_control_source_bank_run_lexical(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            lexical_reentry, &calls);
    CHECK(!r.financial_result && r.action_called && !r.action_result && !calls);
    negative_lexical_bank = NULL;
    for (unsigned lane = 0; lane < 2; ++lane) {
        lexical_birth_t birth = {0};
        r = wl_evaluation_control_source_bank_run_lexical(bank,
                (wl_evaluation_control_source_lane_t)lane, &test_policy,
                lexical_birth, &birth);
        CHECK(!r.financial_result && r.action_called && !r.action_result
            && birth.control && birth.id);
        CHECK(!wl_evaluation_control_charge_attempt(birth.control, birth.id,
            1));
        CHECK(!wl_evaluation_control_finish(birth.control, &birth, 0, 0));
        CHECK(!wl_evaluation_control_detach(birth.control, &birth));
        wl_evaluation_control_release(birth.control);
        /* Each lexical call released its real lane association, so an ordinary
         * context can immediately acquire it and complete its own lifetime. */
        CHECK(!wl_evaluation_control_source_context_begin(bank,
            (wl_evaluation_control_source_lane_t)lane, &borrowed));
        CHECK(!wl_evaluation_control_source_context_end(&borrowed));
    }
    for (unsigned i = 0; i < 4096; ++i) {
        r = wl_evaluation_control_source_bank_run_lexical(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
                count_action, &calls);
        CHECK(!r.financial_result && r.action_called && !r.action_result);
    }
    CHECK(calls == 4096);
    CHECK(!wl_evaluation_control_report(original, &after));
    same_report(&before, &after);
}

static void
public_birth_and_partial_failure(void)
{
    static const char source[] =
        ".decl input(x:int64)\n"
        ".decl output(x:int64)\n"
        "output(x) :- input(x).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(9, &control));
    int owner;
    CHECK(!wl_evaluation_control_attach(control, &owner));
    CHECK(!wl_evaluation_control_begin(control, &owner));
    CHECK(!wl_evaluation_control_charge_attempt(control, 1, 3));
    CHECK(!wl_evaluation_control_finish(control, &owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(control, &owner));
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(control, &before));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session) && session);
    CHECK(!wl_session_try_destroy(&session) && !session);
    CHECK(!wl_evaluation_control_report(control, &after));
    same_report(&before, &after);

    /* The first real private lane is created, the second genuinely exhausts
     * the global constructor identity. No phase/token/owner is fabricated. */
    CHECK(!wl_evaluation_control_test_seed_next_identity(UINT64_MAX - 1));
    CHECK(wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session) == EOVERFLOW && !session);
    CHECK(!wl_evaluation_control_report(control, &after));
    same_report(&before, &after);
    options.spill_quota_bytes = 1;
    CHECK(wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session) == EINVAL && !session);
    CHECK(!wl_evaluation_control_report(control, &after));
    same_report(&before, &after);
    /* Both successful retirement and partial birth rollback released only
     * their genuine attachment. The original remains reusable and monotone. */
    CHECK(!wl_evaluation_control_attach(control, &owner));
    CHECK(!wl_evaluation_control_begin(control, &owner));
    CHECK(wl_evaluation_control_active_attempt_id(control) == 2);
    CHECK(!wl_evaluation_control_charge_attempt(control, 2, 2));
    CHECK(!wl_evaluation_control_finish(control, &owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(control, &owner));
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

int
main(void)
{
    int owner;
    wl_evaluation_control_t *original = NULL;
    CHECK(!wl_evaluation_control_create(9, &original));
    CHECK(!wl_evaluation_control_attach(original, &owner));
    CHECK(!wl_evaluation_control_begin(original, &owner));
    CHECK(!wl_evaluation_control_charge_attempt(original, 1, 3));
    CHECK(!wl_evaluation_control_finish(original, &owner, 0, 0));
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(original, &before));

    wl_evaluation_control_source_bank_t *bank = NULL;
    CHECK(!wl_evaluation_control_source_bank_create(&bank) && bank);
    lexical_lifetime(bank, original);
    wl_evaluation_control_source_context_t context = {0};
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &context));
    unsigned calls = 0;
    wl_evaluation_control_source_context_t copied = context;
    wl_evaluation_control_source_phase_result_t r =
        wl_evaluation_control_source_bank_run(&copied, &test_policy,
            count_action, &calls);
    CHECK(r.financial_result != 0 && !r.action_called && calls == 0);
    static const wl_evaluation_control_source_phase_policy_t zero_policy = {
        0, WL_EVALUATION_CONTROL_QUANTUM, WL_EVALUATION_CONTROL_QUANTUM
    };
    static const wl_evaluation_control_source_phase_policy_t oversized_policy =
    {
        WL_EVALUATION_CONTROL_QUANTUM,
        WL_EVALUATION_CONTROL_QUANTUM + 1, WL_EVALUATION_CONTROL_QUANTUM
    };
    r = wl_evaluation_control_source_bank_run(&context, &zero_policy,
            count_action, &calls);
    CHECK(r.financial_result != 0 && !r.action_called && calls == 0);
    r = wl_evaluation_control_source_bank_run(&context, &oversized_policy,
            count_action, &calls);
    CHECK(r.financial_result != 0 && !r.action_called && calls == 0);
    wl_evaluation_control_source_bank_t *saved = bank;
    CHECK(wl_evaluation_control_source_bank_create(&bank) != 0 &&
        bank == saved);
    wl_evaluation_control_source_bank_t *copied_handle = bank;
    CHECK(wl_evaluation_control_source_bank_destroy(&copied_handle) == EINVAL
        && copied_handle == bank);
    CHECK(wl_evaluation_control_source_bank_destroy(&context.bank) == EINVAL
        && context.bank == bank);
    CHECK(wl_evaluation_control_source_bank_destroy(
            (wl_evaluation_control_source_bank_t **)(void *)bank) == EINVAL);
    CHECK(wl_evaluation_control_source_bank_destroy(&bank) == EBUSY
        && bank == saved);

    negative_reentry_context = &context;
    r = wl_evaluation_control_source_bank_run(&context, &test_policy,
            reentry_action, &calls);
    CHECK(!r.financial_result && r.action_called && !r.action_result
        && calls == 0);
    negative_reentry_context = NULL;
    foreign_probe_t foreign = {&context, &calls, {0}, 0};
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, foreign_context, &foreign));
    CHECK(!wl_thread_join(&thread));
    CHECK(foreign.result.financial_result != 0 && !foreign.result.action_called
        && foreign.end_result != 0 && calls == 0);

    for (unsigned i = 0; i < 4096; ++i) {
        r = wl_evaluation_control_source_bank_run(&context, &test_policy,
                count_action, &calls);
        CHECK(!r.financial_result && r.action_called && !r.action_result);
    }
    CHECK(calls == 4096);
    CHECK(!wl_evaluation_control_report(original, &after));
    same_report(&before, &after);
    CHECK(!wl_evaluation_control_begin(original, &owner));
    CHECK(wl_evaluation_control_active_attempt_id(original) == 2);
    uint64_t successor_consumed = UINT64_MAX;
    CHECK(!wl_evaluation_control_test_active_consumed(original, 2,
        &successor_consumed) && successor_consumed == 0);

    wl_evaluation_control_source_context_t closer = {0};
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_CLOSE, &closer));
    r = wl_evaluation_control_source_bank_run(&closer, &test_policy,
            count_action, &calls);
    CHECK(!r.financial_result && r.action_called && !r.action_result
        && calls == 4097);
    CHECK(!wl_evaluation_control_source_context_end(&closer));
    CHECK(!wl_evaluation_control_source_context_end(&context));
    r = wl_evaluation_control_source_bank_run(&context, &test_policy,
            count_action, &calls);
    CHECK(r.financial_result != 0 && !r.action_called && calls == 4097);
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &context));

    /* Real global constructor exhaustion, not fabricated native ownership.
     * The preborn bank must still actually call the failed constructor and
     * close its own finite paid phase. Nothing charges the original. */
    public_birth_and_partial_failure();
    wl_evaluation_control_t *failed = NULL;
    CHECK(!wl_evaluation_control_report(original, &before));
    r = wl_evaluation_control_source_bank_run(&context, &test_policy,
            real_failed_constructor, &failed);
    CHECK(!r.financial_result && r.action_called
        && r.action_result == EOVERFLOW && !failed);
    CHECK(!wl_evaluation_control_report(original, &after));
    same_report(&before, &after);
    CHECK(!wl_evaluation_control_source_context_end(&context));
    r = wl_evaluation_control_source_bank_run_lexical(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            real_failed_constructor, &failed);
    CHECK(!r.financial_result && r.action_called
        && r.action_result == EOVERFLOW && !failed);
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &context));
    CHECK(!wl_evaluation_control_source_context_end(&context));
    CHECK(!wl_evaluation_control_source_bank_destroy(&bank) && !bank);
    CHECK(wl_evaluation_control_active_attempt_id(original) == 2);
    successor_consumed = UINT64_MAX;
    CHECK(!wl_evaluation_control_test_active_consumed(original, 2,
        &successor_consumed) && successor_consumed == 0);
    CHECK(!wl_evaluation_control_finish(original, &owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(original, &owner));
    wl_evaluation_control_release(original);
    puts("evaluation_source_bank: OK");
    return 0;
}
