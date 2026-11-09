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
#include <string.h>

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
constructor_peer_claim(void)
{
    wl_evaluation_control_test_finance_birth_claim = NULL;
    wl_evaluation_control_t *peer = NULL;
    CHECK(!wl_evaluation_control_create(1, &peer) && peer);
    wl_evaluation_control_release(peer);
}

static int
bounded_constructor(void *payload)
{
    lexical_birth_t *birth = payload;
    wl_evaluation_control_lexical_finance_birth_result_t result =
        wl_evaluation_control_lexical_finance_birth_try(128, birth);
    if (result.protocol) return result.protocol;
    birth->control = result.control; birth->id = result.id;
    return 0;
}

/* Negative nested issuer: no BANK loan is part of the normal callback. */
static int
constructor_reentry(void *payload)
{
    wl_evaluation_control_source_phase_result_t result =
        wl_evaluation_control_source_bank_run_lexical_constructor(
        negative_lexical_bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
        count_action, payload);
    CHECK(result.financial_result == EBUSY && !result.action_called);
    return 0;
}

static void
constructor_lifetime(wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_t *original)
{
    wl_evaluation_control_report_t saved, after;
    CHECK(!wl_evaluation_control_report(original, &saved));
    unsigned calls = 0;
    wl_evaluation_control_source_context_t context = {0};
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &context));
    wl_evaluation_control_source_phase_result_t r =
        wl_evaluation_control_source_bank_run_lexical_constructor(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            count_action, &calls);
    CHECK(r.financial_result == EBUSY && !r.action_called && !calls);
    CHECK(!wl_evaluation_control_source_context_end(&context));
    const wl_evaluation_control_source_phase_policy_t invalid[] = {
        {0, 1, 1}, {1, 0, 1}, {1, 1, 0},
        {WL_EVALUATION_CONTROL_QUANTUM + 1, 1, 1},
        {1, WL_EVALUATION_CONTROL_QUANTUM + 1, 1},
        {1, 1, WL_EVALUATION_CONTROL_QUANTUM + 1}
    };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); ++i) {
        r = wl_evaluation_control_source_bank_run_lexical_constructor(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &invalid[i],
                count_action, &calls);
        CHECK(r.financial_result == EINVAL && !r.action_called && !calls);
    }
    negative_lexical_bank = bank;
    r = wl_evaluation_control_source_bank_run_lexical_constructor(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            constructor_reentry, &calls);
    CHECK(!r.financial_result && r.action_called && !r.action_result && !calls);
    negative_lexical_bank = NULL;
    for (unsigned lane = 0; lane < 2; ++lane) {
        for (unsigned fault = 0; fault < 4; ++fault) {
            lexical_birth_t birth = {0};
            if (fault == 1)
                wl_evaluation_control_test_finance_birth_claim =
                    constructor_peer_claim;
            else if (fault) wl_evaluation_control_test_finance_birth_fail =
                    fault - 1;
            r = wl_evaluation_control_source_bank_run_lexical_constructor(bank,
                    (wl_evaluation_control_source_lane_t)lane, &test_policy,
                    bounded_constructor, &birth);
            CHECK(!r.financial_result && r.action_called
                && r.action_result == (fault == 1 ? EAGAIN : fault ? ENOMEM : 0)
                && !wl_evaluation_control_test_finance_birth_claim
                && !wl_evaluation_control_test_finance_birth_fail);
            if (fault) CHECK(!birth.control && !birth.id);
            else {
                CHECK(birth.control && birth.id
                    && !wl_evaluation_control_charge_attempt(birth.control,
                    birth.id, 5));
                CHECK(!wl_evaluation_control_finish(birth.control, &birth, 0,
                    0));
                CHECK(!wl_evaluation_control_detach(birth.control, &birth));
                wl_evaluation_control_release(birth.control);
            }
            /* The constructor's actual completed financial history and
             * verified lane must permit the ordinary subsequent footer. */
            calls = 0;
            r = wl_evaluation_control_source_bank_run_lexical(bank,
                    (wl_evaluation_control_source_lane_t)lane, &test_policy,
                    count_action, &calls);
            CHECK(!r.financial_result && r.action_called && !r.action_result &&
                calls == 1);
            CHECK(!wl_evaluation_control_report(original, &after));
            same_report(&saved, &after);
        }
    }
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

typedef struct {
    wl_evaluation_control_source_bank_t *bank;
    unsigned first, between, second;
    int first_error, second_error;
} pair_fixture_t;

static pair_fixture_t *observed_pair;

static void
pair_checkpoint(void)
{
    CHECK(observed_pair && observed_pair->first == 1 && !observed_pair->second);
    ++observed_pair->between;
}

static int
pair_second(void *payload)
{
    pair_fixture_t *p = payload;
    CHECK(p->first == 1 && p->between == 1 && !p->second);
    ++p->second;
    return p->second_error;
}

static int
pair_first(void *payload)
{
    pair_fixture_t *p = payload;
    CHECK(!p->first && !p->between && !p->second);
    ++p->first;
    const wl_evaluation_control_source_pair_policy_t small = {1, 1, 1, 1};
    wl_evaluation_control_source_pair_result_t nested =
        wl_evaluation_control_source_bank_run_lexical_pair(p->bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &small,
            pair_first, p, pair_second, p);
    CHECK(nested.financial_result == EBUSY && !nested.first_called &&
        !nested.second_called);
    return p->first_error;
}

static void
lexical_pair(void)
{
    wl_evaluation_control_source_bank_t *bank = NULL;
    CHECK(!wl_evaluation_control_source_bank_create(&bank));
    for (unsigned mode = 0; mode < 3; ++mode) {
        pair_fixture_t p = {bank, 0, 0, 0, mode == 1 ? EIO : 0,
                            mode == 2 ? ENOMEM : 0};
        observed_pair = &p;
        wl_evaluation_control_test_source_pair_checkpoint = pair_checkpoint;
        /* Actual four-payment finite phase, exactly the old 3Q capacity.
         * Test prices are not a source-distance or production tariff proof. */
        const wl_evaluation_control_source_pair_policy_t policy = {256, 256,
                                                                   255, 1};
        wl_evaluation_control_source_pair_result_t r =
            wl_evaluation_control_source_bank_run_lexical_pair(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy,
                pair_first, &p, pair_second, &p);
        CHECK(!r.financial_result && r.first_called &&
            r.first_result == p.first_error
            && r.second_called == (mode != 1) &&
            r.second_result == (mode == 2 ? ENOMEM : 0)
            && p.first == 1 && p.between == 1 && p.second == (mode != 1));
        wl_evaluation_control_test_source_pair_checkpoint = NULL;
        observed_pair = NULL;
    }
    unsigned calls = 0;
    const wl_evaluation_control_source_pair_policy_t invalid[] = {
        {256, 256, 256, 256}, {0, 1, 1, 1}, {1, 0, 1, 1},
        {1, 1, 0, 1}, {1, 1, 1, 0}, {UINT64_MAX, 1, 1, 1},
    };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); ++i) {
        wl_evaluation_control_source_pair_result_t r =
            wl_evaluation_control_source_bank_run_lexical_pair(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &invalid[i],
                count_action, &calls, count_action, &calls);
        CHECK(r.financial_result == EINVAL && !r.first_called &&
            !r.second_called && !calls);
    }
    const wl_evaluation_control_source_pair_policy_t small = {1, 1, 1, 1};
    wl_evaluation_control_source_context_t context = {0};
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &context));
    wl_evaluation_control_source_pair_result_t busy =
        wl_evaluation_control_source_bank_run_lexical_pair(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &small,
            count_action, &calls, count_action, &calls);
    CHECK(busy.financial_result == EBUSY && !busy.first_called &&
        !busy.second_called && !calls);
    CHECK(!wl_evaluation_control_source_context_end(&context));
    CHECK(!wl_evaluation_control_source_bank_destroy(&bank) && !bank);
}

typedef struct {
    unsigned *journal;
    unsigned calls;
    int error;
} final_metadata_t;

static const void *expected_final_metadata_payload;
static bool early_nested_enabled;

static void
early_nested_check(void)
{
    if (!early_nested_enabled) return;
    unsigned session = 0, payload = 0, journal = 0;
    wl_evaluation_control_source_bank_t *bank = negative_lexical_bank;
    CHECK(bank);
    const wl_evaluation_control_source_phase_policy_t policy = {64, 64, 64};
    const wl_evaluation_control_source_pair_policy_t pair_policy = {64, 64, 64,
                                                                    64};
    const wl_evaluation_control_source_final_geometry_t geometry = {
        {&session, sizeof(session)}, {&payload, sizeof(payload)},
        {&journal, sizeof(journal)}
    };
    const wl_evaluation_control_source_final_pair_geometry_t pair_geometry =
    {0};
    wl_evaluation_control_source_early_result_t outcomes[5] = {
        wl_evaluation_control_source_bank_run_lexical_early(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, count_action,
            &payload),
        wl_evaluation_control_source_bank_run_lexical_constructor_early(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, count_action,
            &payload),
        wl_evaluation_control_source_bank_run_lexical_pair_early(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &pair_policy,
            count_action, &payload, count_action, &journal),
        wl_evaluation_control_source_bank_run_lexical_final_early(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, &geometry,
            count_action),
        wl_evaluation_control_source_bank_run_lexical_pair_final_early(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &pair_policy,
            &pair_geometry,
            count_action, count_action)
    };
    for (unsigned i = 0; i < 5; ++i)
        CHECK(outcomes[i].protocol == EBUSY &&
            outcomes[i].financial_result == EBUSY
            && !outcomes[i].phase_started && !outcomes[i].phase_finished
            && !outcomes[i].first_called && !outcomes[i].second_called);
    wl_evaluation_control_report_t report = {.attempt_id = 47, .result = EIO};
    unsigned char saved[sizeof(report)]; memcpy(saved, &report, sizeof(report));
    CHECK(wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &report) == EBUSY
        && !memcmp(saved, &report,
        sizeof(report)) && !payload && !journal && !session);
}

static void
early_expected_report(wl_evaluation_control_source_bank_t *bank, uint64_t work,
    int stop)
{
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &report));
    CHECK(report.attempt_id == 1 &&
        report.limit == 3 * WL_EVALUATION_CONTROL_QUANTUM
        && report.consumed == work && report.stop_reason == stop
        && !report.execution_status && !report.cleanup_status &&
        report.result == stop);
}

static int
final_metadata(void *payload)
{
    CHECK(!expected_final_metadata_payload ||
        payload == expected_final_metadata_payload);
    final_metadata_t *p = payload;
    early_nested_check();
    ++p->calls;
    ++*p->journal;
    return p->error;
}

static void
final_metadata_geometry(void)
{
    wl_evaluation_control_source_bank_t *bank = NULL;
    CHECK(!wl_evaluation_control_source_bank_create(&bank));
    for (unsigned mode = 0; mode < 8; ++mode) {
        unsigned session = 0, journal = 0;
        final_metadata_t payload = {&journal, 0, mode == 1 ? EIO : 0};
        const wl_evaluation_control_source_phase_policy_t policy = {64, 64, 64};
        wl_evaluation_control_source_final_geometry_t geometry = {
            {&session, sizeof(session)}, {&payload, sizeof(payload)},
            {&journal, sizeof(journal)}
        };
        if (mode == 2) geometry.payload.base = &journal;
        if (mode == 3) geometry.payload.bytes = 0;
        if (mode == 4) geometry.payload.bytes = SIZE_MAX;
        if (mode == 5) geometry.payload.base = (const unsigned char *)bank + 1;
        if (mode == 6) geometry.journal = geometry.session;
        if (mode ==
            7) geometry.payload = (wl_evaluation_control_source_extent_t){
                &geometry, sizeof(geometry)
            };
        expected_final_metadata_payload = &payload;
        wl_evaluation_control_source_final_result_t result =
            wl_evaluation_control_source_bank_run_lexical_final(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, &geometry,
                final_metadata);
        CHECK(result.phase_started && result.phase_finished &&
            !result.financial_result);
        if (mode <= 1)
            CHECK(result.action_called && result.action_result == payload.error
                && payload.calls == 1 && journal == 1);
        else CHECK(!result.action_called && result.action_result == EINVAL
                && !payload.calls && !journal && !session);
    }
    for (unsigned fault = 1; fault <= 2; ++fault) {
        unsigned session = 0, journal = 0;
        final_metadata_t payload = {&journal, 0, 0};
        const wl_evaluation_control_source_phase_policy_t policy = {64, 64, 64};
        const wl_evaluation_control_source_final_geometry_t geometry = {
            {&session, sizeof(session)}, {&payload, sizeof(payload)},
            {&journal, sizeof(journal)}
        };
        expected_final_metadata_payload = &payload;
        wl_evaluation_control_test_source_final_geometry_fault = fault;
        wl_evaluation_control_source_final_result_t result =
            wl_evaluation_control_source_bank_run_lexical_final(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, &geometry,
                final_metadata);
        CHECK(result.phase_started && result.phase_finished &&
            !result.financial_result
            && !result.action_called && result.action_result == EINVAL &&
            !payload.calls
            && !journal && !session &&
            !wl_evaluation_control_test_source_final_geometry_fault);
    }
    struct {
        final_metadata_t payload;
        wl_evaluation_control_source_phase_policy_t policy;
    } embedded = {{0}, {64, 64, 64}};
    unsigned policy_session = 0, policy_journal = 0;
    embedded.payload.journal = &policy_journal;
    const wl_evaluation_control_source_final_geometry_t policy_geometry = {
        {&policy_session, sizeof(policy_session)},
        {&embedded, sizeof(embedded)},
        {&policy_journal, sizeof(policy_journal)}
    };
    expected_final_metadata_payload = &embedded.payload;
    wl_evaluation_control_source_final_result_t policy_result =
        wl_evaluation_control_source_bank_run_lexical_final(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &embedded.policy,
            &policy_geometry, final_metadata);
    CHECK(policy_result.phase_started && policy_result.phase_finished
        && !policy_result.financial_result && !policy_result.action_called
        && policy_result.action_result == EINVAL && !embedded.payload.calls &&
        !policy_journal);
    expected_final_metadata_payload = NULL;
    /* Adjoining actual objects are disjoint; no strict '<=' overlap test. */
    struct { unsigned session, journal; } adjacent = {0};
    final_metadata_t payload = {&adjacent.journal, 0, 0};
    const wl_evaluation_control_source_phase_policy_t policy = {1, 1, 1};
    const wl_evaluation_control_source_final_geometry_t geometry = {
        {&adjacent.session, sizeof(adjacent.session)},
        {&payload, sizeof(payload)},
        {&adjacent.journal, sizeof(adjacent.journal)}
    };
    wl_evaluation_control_source_final_result_t result =
        wl_evaluation_control_source_bank_run_lexical_final(bank,
            WL_EVALUATION_CONTROL_SOURCE_CLOSE, &policy, &geometry,
            final_metadata);
    CHECK(result.phase_started && result.phase_finished && result.action_called
        && !result.financial_result && !result.action_result &&
        payload.calls == 1
        && adjacent.journal == 1 && !adjacent.session);
    for (unsigned after = 0; after < 2; ++after) {
        unsigned session = 0, journal = 0;
        final_metadata_t p = {&journal, 0, 0};
        const wl_evaluation_control_source_final_geometry_t g = {
            {&session, sizeof(session)}, {&p, sizeof(p)},
            {&journal, sizeof(journal)}
        };
        wl_evaluation_control_test_source_final_cancel_at = after
            ? WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_ACTION
            : WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_BEGIN;
        result = wl_evaluation_control_source_bank_run_lexical_final(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, &g,
                final_metadata);
        CHECK(result.phase_started && result.phase_finished
            && result.financial_result == WL_EVALUATION_CONTROL_CANCELLED
            && result.action_called == (after != 0) && !result.action_result
            && p.calls == after && journal == after && !session
            && !wl_evaluation_control_test_source_final_cancel_at);
        CHECK(!wl_evaluation_control_test_source_final_reset_cancel(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION));
    }
    CHECK(!wl_evaluation_control_source_bank_destroy(&bank) && !bank);
}

typedef struct {
    wl_evaluation_control_lexical_finance_birth_result_t born;
    wl_evaluation_control_lexical_finance_expectation_t expected;
    unsigned calls;
    int error;
} early_pair_scope_t;

static int
early_pair_close(void *payload)
{
    early_pair_scope_t *scope = payload;
    early_nested_check();
    const wl_evaluation_control_lexical_finance_expectation_t expected =
        scope->expected;
    wl_evaluation_control_lexical_finance_result_t closed =
        wl_evaluation_control_lexical_finance_close_checked(scope->born.control,
            &expected);
    CHECK(!closed.protocol && closed.closed && closed.report_valid &&
        !closed.result);
    scope->born.control = NULL; ++scope->calls;
    return scope->error;
}

static void
early_phase_cases(void)
{
    wl_evaluation_control_source_bank_t *bank = NULL;
    CHECK(!wl_evaluation_control_source_bank_create(&bank));
    const wl_evaluation_control_source_phase_policy_t policy = {64, 64, 64};
    const wl_evaluation_control_source_pair_policy_t pair_policy = {64, 64, 64,
                                                                    64};
    for (unsigned mode = 0; mode < 5; ++mode)
        for (unsigned fault = 0; fault < 7; ++fault) {
            if ((fault == 3 && mode < 3) ||
                ((fault == 4 || fault == 5) && mode != 4)) continue;
            unsigned session = 0, journal = 0, second_calls = 0, record = 0;
            final_metadata_t payload = {&journal, 0, fault == 2 ? EIO : 0};
            expected_final_metadata_payload = &payload;
            wl_evaluation_control_source_final_geometry_t geometry = {
                {&session, sizeof(session)}, {&payload, sizeof(payload)},
                {&journal, sizeof(journal)}
            };
            early_pair_scope_t scope = {.error = fault == 5 ? EIO : 0};
            if (mode == 4) {
                scope.born =
                    wl_evaluation_control_lexical_finance_birth_try(672,
                        &record);
                CHECK(!scope.born.protocol && scope.born.control);
                scope.expected =
                    (wl_evaluation_control_lexical_finance_expectation_t){
                    .owner = &record, .scope = &scope,
                    .owner_bytes = sizeof(record), .scope_bytes = sizeof(scope),
                    .identity = scope.born.identity, .id = scope.born.id,
                    .limit = 672, .active = true
                };
            }
            wl_evaluation_control_source_final_pair_geometry_t pair_geometry = {
                {&session, sizeof(session)}, {&record, sizeof(record)},
                {&scope, sizeof(scope)}, {&payload, sizeof(payload)},
                {&journal, sizeof(journal)},
                scope.born.control, scope.expected
            };
            if (fault == 3) {
                geometry.payload = geometry.journal;
                pair_geometry.payload = pair_geometry.journal;
            }
            if (fault == 4) wl_evaluation_control_test_source_final_cancel_at =
                    WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_FIRST;
            if (fault ==
                1) wl_evaluation_control_test_source_early_cancel_once = true;
            negative_lexical_bank = bank; early_nested_enabled = true;
            wl_evaluation_control_report_t before_ref, after_ref;
            if (fault == 6) {
                unsigned prime = 0;
                CHECK(wl_evaluation_control_source_bank_run_lexical_early(bank,
                    WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy,
                    count_action, &prime).first_called
                    && prime == 1);
                CHECK(!wl_evaluation_control_test_source_early_report(bank,
                    WL_EVALUATION_CONTROL_SOURCE_OPERATION, &before_ref));
                wl_evaluation_control_test_source_early_rearm_ref_once = true;
            }
            wl_evaluation_control_source_early_result_t result;
            for (unsigned retry = 0; retry < (fault == 6 ? 2u : 1u); ++retry) {
                if (!mode) result =
                        wl_evaluation_control_source_bank_run_lexical_early(
                        bank,
                        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy,
                        final_metadata, &payload);
                else if (mode ==
                    1) result =
                        wl_evaluation_control_source_bank_run_lexical_constructor_early
                            (bank,
                            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy,
                            final_metadata, &payload);
                else if (mode ==
                    2) result =
                        wl_evaluation_control_source_bank_run_lexical_pair_early
                            (bank,
                            WL_EVALUATION_CONTROL_SOURCE_OPERATION,
                            &pair_policy, final_metadata, &payload,
                            count_action, &second_calls);
                else if (mode ==
                    3) result =
                        wl_evaluation_control_source_bank_run_lexical_final_early
                            (bank,
                            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy,
                            &geometry, final_metadata);
                else result =
                        wl_evaluation_control_source_bank_run_lexical_pair_final_early
                            (bank,
                            WL_EVALUATION_CONTROL_SOURCE_OPERATION,
                            &pair_policy, &pair_geometry,
                            early_pair_close, final_metadata);
                if (fault == 6 && !retry) {
                    CHECK(result.protocol == EBUSY &&
                        result.financial_result == EBUSY
                        && !result.begin_protocol && !result.checkpoint_result
                        && !result.phase_started && !result.phase_finished
                        && !result.first_called && !result.second_called
                        && !payload.calls && !journal && !scope.calls &&
                        !second_calls
                        && !
                        wl_evaluation_control_test_source_early_rearm_ref_once);
                    CHECK(!wl_evaluation_control_test_source_early_report(bank,
                        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after_ref));
                    same_report(&before_ref, &after_ref);
                }
            }
            early_nested_enabled = false; negative_lexical_bank = NULL;
            CHECK(!result.protocol && !result.begin_protocol
                && result.phase_started && result.phase_finished);
            early_expected_report(bank,
                fault == 1 ? 0 : fault == 5 ? 192 : (fault == 3 ||
                fault == 4) ? 128
                : mode == 4 || (mode == 2 && fault != 2) ? 256 : 192,
                fault == 1 || fault == 4 ? WL_EVALUATION_CONTROL_CANCELLED : 0);
            if (fault == 1) {
                CHECK(result.checkpoint_result ==
                    WL_EVALUATION_CONTROL_CANCELLED
                    && result.financial_result ==
                    WL_EVALUATION_CONTROL_CANCELLED
                    && !result.first_called && !result.second_called &&
                    !payload.calls
                    && !journal && !second_calls && !scope.calls);
                CHECK(!wl_evaluation_control_test_source_final_reset_cancel(
                        bank,
                        WL_EVALUATION_CONTROL_SOURCE_OPERATION));
            } else if (fault == 3) {
                CHECK(!result.checkpoint_result && !result.financial_result
                    && !result.first_called && !result.second_called
                    && result.first_result == EINVAL && !payload.calls &&
                    !journal && !scope.calls);
            } else if (fault == 4) {
                CHECK(!result.checkpoint_result &&
                    result.financial_result == WL_EVALUATION_CONTROL_CANCELLED
                    && result.first_called && !result.first_result &&
                    !result.second_called
                    && scope.calls == 1 && !scope.born.control &&
                    !payload.calls && !journal);
                CHECK(!wl_evaluation_control_test_source_final_reset_cancel(
                        bank,
                        WL_EVALUATION_CONTROL_SOURCE_OPERATION));
            } else if (fault == 5) {
                CHECK(!result.checkpoint_result && !result.financial_result &&
                    result.first_called
                    && result.first_result == EIO && !result.second_called
                    && scope.calls == 1 && !scope.born.control &&
                    !payload.calls && !journal);
            } else {
                CHECK(!result.checkpoint_result && !result.financial_result &&
                    result.first_called);
                if (mode == 4) CHECK(scope.calls == 1 && !scope.born.control
                        && result.second_called &&
                        result.second_result == payload.error
                        && payload.calls == 1 && journal == 1);
                else {
                    CHECK(result.first_result == payload.error &&
                        payload.calls == 1 && journal == 1);
                    if (mode == 2) CHECK(result.second_called == (fault != 2)
                            && second_calls == (fault != 2));
                    else CHECK(!result.second_called);
                }
            }
            if (scope.born.control) {
                const wl_evaluation_control_lexical_finance_expectation_t
                    expected = scope.expected;
                wl_evaluation_control_lexical_finance_result_t closed =
                    wl_evaluation_control_lexical_finance_close_checked(
                    scope.born.control, &expected);
                CHECK(!closed.protocol && closed.closed && closed.report_valid);
            }
            CHECK(!wl_evaluation_control_test_source_early_cancel_once);
        }
    expected_final_metadata_payload = NULL;
    CHECK(!wl_evaluation_control_source_bank_destroy(&bank) && !bank);
}

static wl_evaluation_control_source_early_result_t
early_invalid_call(unsigned mode, wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t lane,
    const wl_evaluation_control_source_phase_policy_t *policy,
    const wl_evaluation_control_source_pair_policy_t *pair_policy,
    void *payload, bool missing_geometry)
{
    wl_evaluation_control_source_final_geometry_t geometry = {0};
    wl_evaluation_control_source_final_pair_geometry_t pair_geometry = {0};
    if (!mode) return wl_evaluation_control_source_bank_run_lexical_early(bank,
                   lane,
                   policy, count_action, payload);
    if (mode ==
        1) return
            wl_evaluation_control_source_bank_run_lexical_constructor_early(
            bank, lane,
            policy, count_action, payload);
    if (mode ==
        2) return wl_evaluation_control_source_bank_run_lexical_pair_early(bank,
                   lane,
                   pair_policy, count_action, payload, count_action, payload);
    if (mode ==
        3) return wl_evaluation_control_source_bank_run_lexical_final_early(
            bank, lane,
            policy, missing_geometry ? NULL : &geometry, count_action);
    return wl_evaluation_control_source_bank_run_lexical_pair_final_early(bank,
               lane,
               pair_policy, missing_geometry ? NULL : &pair_geometry,
               count_action, count_action);
}

static void
early_invalid_cases(void)
{
    wl_evaluation_control_source_bank_t *bank = NULL;
    CHECK(!wl_evaluation_control_source_bank_create(&bank));
    const wl_evaluation_control_source_phase_policy_t policy = {64, 64, 64};
    const wl_evaluation_control_source_pair_policy_t pair_policy = {64, 64, 64,
                                                                    64};
    unsigned calls = 0;
    CHECK(wl_evaluation_control_source_bank_run_lexical_early(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, count_action,
        &calls).first_called && calls == 1);
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &before));
    for (unsigned mode = 0; mode < 5; ++mode)
        for (unsigned fault = 0; fault < 6; ++fault) {
            wl_evaluation_control_source_phase_policy_t invalid = policy;
            wl_evaluation_control_source_pair_policy_t invalid_pair =
                pair_policy;
            if (fault == 2) {
                invalid.bootstrap_work = 0; invalid_pair.bootstrap_work = 0;
            }
            if (fault == 3) {
                invalid.current_work = 257; invalid_pair.first_work = 257;
            }
            if (fault == 4) {
                invalid.close_work = 0; invalid_pair.close_work = 0;
            }
            if (fault == 5 && (mode == 2 || mode == 4))
                invalid_pair = (wl_evaluation_control_source_pair_policy_t){256,
                                                                            256,
                                                                            256,
                                                                            256};
            wl_evaluation_control_source_early_result_t result =
                early_invalid_call(mode,
                    fault == 0 ? NULL : bank,
                    fault ==
                    1 ? (wl_evaluation_control_source_lane_t)2 :
                    WL_EVALUATION_CONTROL_SOURCE_OPERATION,
                    &invalid, &invalid_pair,
                    fault == 5 && mode < 2 ? (void *)bank : &calls,
                    fault == 5 && mode == 3);
            CHECK(result.protocol == EINVAL && result.financial_result == EINVAL
                && !result.begin_protocol && !result.checkpoint_result
                && !result.phase_started && !result.phase_finished
                && !result.first_called && !result.second_called && calls == 1);
            CHECK(!wl_evaluation_control_test_source_early_report(bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after));
            same_report(&before, &after);
        }
    for (unsigned mode = 2; mode <= 4; mode += 2) {
        wl_evaluation_control_source_early_result_t invalid =
            early_invalid_call(mode, bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION, &policy, &pair_policy,
                mode == 2 ? (void *)bank : &calls, mode == 4);
        CHECK(invalid.protocol == EINVAL && invalid.financial_result == EINVAL
            && !invalid.phase_started && !invalid.phase_finished &&
            !invalid.first_called
            && !invalid.second_called && calls == 1);
        CHECK(!wl_evaluation_control_test_source_early_report(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after));
        same_report(&before, &after);
    }
    unsigned char saved[sizeof(after)]; memcpy(saved, &after, sizeof(after));
    for (unsigned fault = 1; fault <= 3; ++fault) {
        wl_evaluation_control_test_source_early_report_fault = fault;
        CHECK(wl_evaluation_control_test_source_early_report(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after) == EINVAL
            && !wl_evaluation_control_test_source_early_report_fault
            && !memcmp(saved, &after, sizeof(after)));
        CHECK(!wl_evaluation_control_test_source_early_report(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after));
        same_report(&before, &after);
    }
    CHECK(wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION,
        (wl_evaluation_control_report_t *)(uintptr_t)(UINTPTR_MAX - 1)) ==
        EINVAL);
    CHECK(wl_evaluation_control_test_source_early_report(NULL,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after) == EINVAL
        && !memcmp(saved, &after, sizeof(after)));
    CHECK(wl_evaluation_control_test_source_early_report(bank,
        (wl_evaluation_control_source_lane_t)2, &after) == EINVAL
        && !memcmp(saved, &after, sizeof(after)));
    CHECK(wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, NULL) == EINVAL);
    CHECK(wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION,
        (wl_evaluation_control_report_t *)((unsigned char *)bank + 1)) ==
        EINVAL);
    wl_evaluation_control_source_context_t context = {0};
    CHECK(!wl_evaluation_control_source_context_begin(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &context));
    CHECK(wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after) == EBUSY
        && !memcmp(saved, &after, sizeof(after)));
    CHECK(!wl_evaluation_control_source_context_end(&context));
    CHECK(!wl_evaluation_control_test_source_early_report(bank,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, &after));
    same_report(&before, &after);
    CHECK(!wl_evaluation_control_source_bank_destroy(&bank));
}

static void
fresh_source_birth_cases(void)
{
    int owner = 0, scope = 0;
    for (unsigned mode = 0; mode < 2; ++mode) {
        wl_evaluation_control_lexical_finance_birth_result_t born = mode
            ? wl_evaluation_control_lexical_finance_fresh_birth_try(129, &owner)
            : wl_evaluation_control_lexical_finance_birth_try(129, &owner);
        CHECK(!born.protocol && born.control && born.id == 1 && born.identity
            && wl_evaluation_control_identity(born.control) == born.identity
            && wl_evaluation_control_active_attempt_id(born.control) == 1);
        wl_evaluation_control_lexical_finance_expectation_t expected = {
            .owner = &owner, .scope = &scope,
            .owner_bytes = sizeof(owner), .scope_bytes = sizeof(scope),
            .identity = born.identity, .limit = 129, .id = born.id,
            .active = true
        };
        CHECK(!wl_evaluation_control_lexical_finance_prove_checked(born.control,
            &expected));
        wl_evaluation_control_report_t report, saved;
        CHECK(wl_evaluation_control_report(born.control, &report) == ENOENT);
        uint64_t used = UINT64_MAX;
        CHECK(!wl_evaluation_control_test_active_consumed(born.control, 1,
            &used) && !used);
        CHECK(!wl_evaluation_control_retain(born.control));
        CHECK(wl_evaluation_control_lexical_finance_prove_checked(born.control,
            &expected) == EBUSY);
        wl_evaluation_control_release(born.control);
        CHECK(!wl_evaluation_control_charge_attempt(born.control, 1, 7));
        CHECK(!wl_evaluation_control_finish(born.control, &owner, 0, 0));
        CHECK(!wl_evaluation_control_report(born.control,
            &saved) && saved.attempt_id == 1
            && saved.limit == 129 && saved.consumed == 7 && !saved.stop_reason
            && !saved.execution_status && !saved.cleanup_status &&
            !saved.result);
        CHECK(!wl_evaluation_control_begin(born.control, &owner)
            && wl_evaluation_control_active_attempt_id(born.control) == 2);
        CHECK(!wl_evaluation_control_report(born.control, &report));
        same_report(&saved, &report);
        expected.charged = 7;
        wl_evaluation_control_lexical_finance_result_t stale =
            wl_evaluation_control_lexical_finance_close_checked(born.control,
                &expected);
        CHECK(stale.protocol == EINVAL && !stale.closed
            && !wl_evaluation_control_test_active_consumed(born.control, 2,
            &used) && !used
            && !wl_evaluation_control_report(born.control, &report));
        same_report(&saved, &report);
        expected.id = 2; expected.charged = 0;
        wl_evaluation_control_lexical_finance_result_t closed =
            wl_evaluation_control_lexical_finance_close_checked(born.control,
                &expected);
        CHECK(!closed.protocol && closed.closed && closed.report_valid
            && closed.report.attempt_id == 2 && !closed.report.consumed &&
            !closed.result);
    }
    wl_evaluation_control_test_finance_birth_claim = constructor_peer_claim;
    wl_evaluation_control_lexical_finance_birth_result_t invalid =
        wl_evaluation_control_lexical_finance_fresh_birth_try(0, &owner);
    CHECK(invalid.protocol == EINVAL && !invalid.control && !invalid.id &&
        !invalid.identity
        && wl_evaluation_control_test_finance_birth_claim);
    invalid = wl_evaluation_control_lexical_finance_fresh_birth_try(1, NULL);
    CHECK(invalid.protocol == EINVAL && !invalid.control && !invalid.id &&
        !invalid.identity
        && wl_evaluation_control_test_finance_birth_claim);
    invalid = wl_evaluation_control_lexical_finance_fresh_birth_try(129,
            &owner);
    CHECK(invalid.protocol == EAGAIN && !invalid.control && !invalid.id &&
        !invalid.identity
        && !wl_evaluation_control_test_finance_birth_claim);
    for (unsigned fault = 1; fault <= 2; ++fault) {
        wl_evaluation_control_lexical_finance_birth_result_t before =
            wl_evaluation_control_lexical_finance_fresh_birth_try(129, &owner);
        CHECK(!before.protocol && before.control);
        CHECK(!wl_evaluation_control_finish(before.control, &owner, 0, 0)
            && !wl_evaluation_control_detach(before.control, &owner));
        wl_evaluation_control_release(before.control);
        wl_evaluation_control_test_finance_birth_fail = fault;
        invalid = wl_evaluation_control_lexical_finance_fresh_birth_try(129,
                &owner);
        CHECK(invalid.protocol == ENOMEM && !invalid.control && !invalid.id &&
            !invalid.identity
            && !wl_evaluation_control_test_finance_birth_fail);
        wl_evaluation_control_lexical_finance_birth_result_t after =
            wl_evaluation_control_lexical_finance_fresh_birth_try(129, &owner);
        CHECK(!after.protocol && after.control &&
            after.identity == before.identity + 2);
        wl_evaluation_control_request_cancel(after.control);
        CHECK(wl_evaluation_control_charge_attempt(after.control, after.id, 0)
            == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(wl_evaluation_control_finish(after.control, &owner, 0,
            0) == WL_EVALUATION_CONTROL_CANCELLED);
        wl_evaluation_control_report_t cancelled;
        CHECK(!wl_evaluation_control_report(after.control, &cancelled)
            && cancelled.attempt_id == 1 && !cancelled.consumed
            && cancelled.result == WL_EVALUATION_CONTROL_CANCELLED
            && !cancelled.execution_status && !cancelled.cleanup_status);
        CHECK(!wl_evaluation_control_detach(after.control, &owner));
        wl_evaluation_control_release(after.control);
    }
}

static void
fresh_source_birth_exhaustion(void)
{
    int owner = 0;
    CHECK(!wl_evaluation_control_test_seed_next_identity(UINT64_MAX - 1));
    wl_evaluation_control_lexical_finance_birth_result_t last =
        wl_evaluation_control_lexical_finance_fresh_birth_try(129, &owner);
    CHECK(!last.protocol && last.control && last.identity == UINT64_MAX - 1 &&
        last.id == 1);
    wl_evaluation_control_lexical_finance_birth_result_t exhausted =
        wl_evaluation_control_lexical_finance_fresh_birth_try(129, &owner);
    CHECK(exhausted.protocol == EOVERFLOW && !exhausted.control &&
        !exhausted.id && !exhausted.identity);
    CHECK(!wl_evaluation_control_finish(last.control, &owner, 0, 0)
        && !wl_evaluation_control_detach(last.control, &owner));
    wl_evaluation_control_release(last.control);
}

int
main(int argc, char **argv)
{
    if (argc == 2 && !strcmp(argv[1], "--fresh-exhaustion")) {
        fresh_source_birth_exhaustion();
        return 0;
    }
    fresh_source_birth_cases();
    early_phase_cases();
    early_invalid_cases();
    final_metadata_geometry();
    lexical_pair();
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
    constructor_lifetime(bank, original);
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
    r = wl_evaluation_control_source_bank_run_lexical_constructor(bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, &test_policy,
            real_failed_constructor, &failed);
    CHECK(!r.financial_result && r.action_called
        && r.action_result == EOVERFLOW && !failed);
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
