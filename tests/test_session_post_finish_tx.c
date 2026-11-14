/* Actual Columnar POST_FINISH_TX events are descriptive observations, never
 * Source/Native permits. No phase, generation or backend is fabricated here. */
#include "wirelog/columnar/internal.h"
#include "wirelog/exec_plan_gen.h"
#include "wirelog/session.h"
#include "wirelog/session_options.h"
#include "wirelog/thread.h"
#include "wirelog/wirelog.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if !defined(_WIN32)
#include <signal.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#define CHECK(e) do { if (!(e)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #e); abort(); \
                      } } while (0)

/* Descriptive values of the private read-only view, not mutable phases. */
enum { OWNER_INERT = 0, OWNER_HANDOFF = 4 };
typedef enum {
    DEATH_NONE, DEATH_COPY, DEATH_GETTER, DEATH_KIND, DEATH_RESULT,
    DEATH_SESSION, DEATH_FOREIGN, DEATH_REVOKED, DEATH_RETURNED
} death_case_t;

typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_session_t *session, *other;
    wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
    wl_evaluation_control_report_t original;
    wl_backend_controlled_completion_t saved;
    wl_backend_completed_source_kind_t saved_kind;
    unsigned callbacks, frozen_callbacks, active_events, revoked_events;
    unsigned commits, ack_transitions, peer_endpoints;
    bool c1, park, original_saved, ack_seen, cancel_armed, cancel_seen;
    bool peer_running, predicate_foreign_false;
    death_case_t death;
    int ready_fd;
} fixture_t;

static fixture_t *current;

static void
same_report(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b)
{
    CHECK(a->attempt_id == b->attempt_id && a->limit == b->limit
        && a->consumed == b->consumed && a->stop_reason == b->stop_reason
        && a->execution_status == b->execution_status
        && a->cleanup_status == b->cleanup_status && a->result == b->result);
}

static wl_columnar_session_test_completed_view_t
owner_view(const wl_session_t *session)
{
    wl_columnar_session_test_completed_view_t v;
    CHECK(!wl_columnar_session_test_completed_view(
            (const wl_col_session_t *)session, &v));
    return v;
}

static void
same_view(const wl_columnar_session_test_completed_view_t *a,
    const wl_columnar_session_test_completed_view_t *b)
{
    CHECK(a->phase == b->phase && a->finance_kind == b->finance_kind
        && a->ticket == b->ticket
        && a->operation_identity == b->operation_identity
        && a->original_attempt_id == b->original_attempt_id
        && a->finance_consumed == b->finance_consumed
        && a->local_reset_completed == b->local_reset_completed
        && a->actor_current == b->actor_current
        && a->invocation_accepted == b->invocation_accepted
        && a->backend_active == b->backend_active
        && a->endpoint_busy == b->endpoint_busy
        && a->source_ready == b->source_ready
        && a->ack_paid == b->ack_paid && a->ack_prepared == b->ack_prepared
        && a->transaction_present == b->transaction_present
        && a->main_pending == b->main_pending
        && a->parent_pending == b->parent_pending);
    same_report(&a->report, &b->report);
}

static void
check_original(fixture_t *f)
{
    wl_evaluation_control_report_t r;
    CHECK(!wl_evaluation_control_active_attempt_id(f->control));
    CHECK(!wl_evaluation_control_report(f->control, &r));
    same_report(&f->original, &r);
    CHECK(f->callbacks == f->frozen_callbacks);
}

static void
capture(const char *name, const int64_t *row, uint32_t width, int32_t sign,
    void *payload)
{
    fixture_t *f = payload;
    CHECK(width == 2 && sign == 1);
    CHECK((!strcmp(name, "output") && row[0] == 1 && row[1] == 2)
        || (!strcmp(name, "other") && row[0] == 3 && row[1] == 4));
    ++f->callbacks;
}

static void
death_ready(fixture_t *f)
{
#if !defined(_WIN32)
    CHECK(f->ready_fd >= 0);
    const char byte = 'R';
    CHECK(write(f->ready_fd, &byte, 1) == 1);
#else
    (void)f;
    CHECK(false);
#endif
}

typedef struct {
    fixture_t *fixture;
    const wl_backend_controlled_completion_t *receipt;
    wl_backend_completed_source_kind_t kind;
    int result;
    bool death;
} endpoint_peer_t;

static void *
foreign_endpoint(void *argument)
{
    endpoint_peer_t *p = argument;
    CHECK(!wl_session_source_completed_endpoint_owned(p->fixture->session,
        p->receipt, p->kind, p->result));
    p->fixture->predicate_foreign_false = true;
    if (p->death) {
        death_ready(p->fixture);
        p->fixture->session->backend->session_controlled_completed_source_end(
            p->fixture->session, p->receipt, p->kind, p->result);
        _Exit(91);
    }
    return NULL;
}

static void
call_bad_endpoint(fixture_t *f, const wl_session_t *session,
    const wl_backend_controlled_completion_t *receipt,
    wl_backend_completed_source_kind_t kind, int result)
{
    death_ready(f);
    session->backend->session_controlled_completed_source_end(
        (wl_session_t *)session, receipt, kind, result);
    _Exit(92);
}

static void
completed_event(wl_session_test_completed_event_t event,
    const wl_session_t *session,
    const wl_backend_controlled_completion_t *receipt,
    wl_backend_completed_source_kind_t kind, int result)
{
    fixture_t *f = current;
    CHECK(f && session == f->session && receipt && receipt->selected
        && receipt->domain == WL_BACKEND_CONTROLLED_COMPLETION_POST_FINISH_TX
        && receipt->kind == WL_BACKEND_CONTROLLED_COMPLETION_STEP
        && receipt->ticket && receipt->operation_identity
        && receipt->completed_attempt_id);
    CHECK(kind == (event == WL_SESSION_TEST_COMPLETED_BEFORE_COMMIT ?
        WL_BACKEND_COMPLETED_SOURCE_UNKNOWN : f->c1 ?
        WL_BACKEND_COMPLETED_SOURCE_OBSERVATION :
        WL_BACKEND_COMPLETED_SOURCE_NOT_REQUIRED));
    wl_columnar_session_test_completed_view_t v = owner_view(session);
    CHECK(v.ticket == receipt->ticket
        && v.operation_identity == receipt->operation_identity
        && v.original_attempt_id == receipt->completed_attempt_id
        && !v.backend_active && !v.endpoint_busy);
    if (!f->original_saved) {
        CHECK(!wl_evaluation_control_report(f->control, &f->original));
        CHECK(f->original.attempt_id == receipt->completed_attempt_id
            && !f->original.result && !f->original.execution_status
            && !f->original.cleanup_status && f->callbacks == 2);
        f->original_saved = true;
        f->frozen_callbacks = f->callbacks;
    }
    same_report(&f->original, &v.report);
    check_original(f);
    if (v.ack_prepared && !f->ack_seen) {
        f->ack_seen = true;
        ++f->ack_transitions;
    }
    if (v.phase == OWNER_HANDOFF) {
        CHECK(v.ack_prepared && v.ack_paid && v.finance_consumed
            && v.local_reset_completed && !v.transaction_present
            && !v.main_pending && !v.parent_pending);
        /* The old finisher's real NO_FINANCE/FINANCE producer is distinct
         * from any Source metadata action executed by a Native packet. */
        CHECK(v.finance_kind == (f->c1 ? 2u : 1u));
    }
    if (event == WL_SESSION_TEST_COMPLETED_BEFORE_SOURCE_END
        || event == WL_SESSION_TEST_COMPLETED_ENDPOINT_ACTIVE) {
        /* Reentry must refuse before it can clear accepted provenance. */
        CHECK(session->backend->session_step((wl_session_t *)session) == EBUSY);
        wl_columnar_session_test_completed_view_t after = owner_view(session);
        same_view(&v, &after);
        check_original(f);
    }
    if (event == WL_SESSION_TEST_COMPLETED_BEFORE_SOURCE_END) {
        if (f->park && v.phase == OWNER_HANDOFF && !f->cancel_armed) {
            CHECK(f->c1 && v.actor_current && v.invocation_accepted);
            CHECK(!wl_evaluation_control_test_source_observation_cancel_at);
            wl_evaluation_control_test_source_observation_cancel_at = 5;
            f->cancel_armed = true;
        }
        return;
    }
    if (event == WL_SESSION_TEST_COMPLETED_ENDPOINT_ACTIVE) {
        ++f->active_events;
        CHECK(wl_session_source_completed_endpoint_owned(session, receipt,
            kind, result));
        wl_backend_controlled_completion_t copy = *receipt, getter = {0};
        CHECK(!wl_session_source_completed_endpoint_owned(session, &copy,
            kind, result));
        wl_backend_completed_source_kind_t wrong = f->c1 ?
            WL_BACKEND_COMPLETED_SOURCE_NOT_REQUIRED :
            WL_BACKEND_COMPLETED_SOURCE_OBSERVATION;
        CHECK(!wl_session_source_completed_endpoint_owned(session, receipt,
            wrong, result));
        CHECK(!wl_session_source_completed_endpoint_owned(session, receipt,
            kind, result + 1));
        CHECK(!wl_session_source_completed_endpoint_owned(f->other, receipt,
            kind, result));
        int observed = session->backend->session_controlled_step_observe(
            session, WL_BACKEND_CONTROLLED_COMPLETION_STEP, true, &getter);
        CHECK((observed ==
            WL_BACKEND_CONTROLLED_OBSERVATION_COMPLETED_OWNER_PENDING
            || observed == WL_BACKEND_CONTROLLED_OBSERVATION_COMPLETION_READY)
            && getter.selected && getter.domain == receipt->domain
            && getter.ticket == receipt->ticket
            && getter.operation_identity == receipt->operation_identity
            && getter.completed_attempt_id == receipt->completed_attempt_id);
        CHECK(!wl_session_source_completed_endpoint_owned(session, &getter,
            kind, result));
        f->saved = copy;
        f->saved_kind = kind;
        if (result == WL_EVALUATION_CONTROL_CANCELLED) {
            CHECK(f->cancel_armed && v.phase == OWNER_HANDOFF);
            f->cancel_seen = true;
        } else CHECK(!result);
        if (f->peer_running) {
            CHECK(!v.actor_current && !v.invocation_accepted
                && !receipt->invocation_accepted);
            ++f->peer_endpoints;
        }
        endpoint_peer_t peer = {f, receipt, kind, result,
                                f->death == DEATH_FOREIGN &&
                                v.phase == OWNER_HANDOFF};
        if (!f->predicate_foreign_false || peer.death) {
            wl_thread_t thread;
            CHECK(!wl_thread_create(&thread, foreign_endpoint, &peer));
            CHECK(!wl_thread_join(&thread));
        }
        CHECK(f->predicate_foreign_false);
        if (v.phase == OWNER_HANDOFF) {
            switch (f->death) {
            case DEATH_COPY:
                call_bad_endpoint(f, session, &copy, kind, result); break;
            case DEATH_GETTER:
                call_bad_endpoint(f, session, &getter, kind, result); break;
            case DEATH_KIND:
                call_bad_endpoint(f, session, receipt, wrong, result); break;
            case DEATH_RESULT:
                call_bad_endpoint(f, session, receipt, kind, result + 1); break;
            case DEATH_SESSION:
                call_bad_endpoint(f, f->other, receipt, kind, result); break;
            default: break;
            }
        }
        return;
    }
    CHECK(!wl_session_source_completed_endpoint_owned(session, receipt,
        kind, result));
    if (event == WL_SESSION_TEST_COMPLETED_ENDPOINT_REVOKED) {
        ++f->revoked_events;
        if (f->death == DEATH_REVOKED && v.phase == OWNER_HANDOFF)
            call_bad_endpoint(f, session, receipt, kind, result);
        return;
    }
    CHECK(event == WL_SESSION_TEST_COMPLETED_BEFORE_COMMIT
        && v.phase == OWNER_HANDOFF && v.source_ready
        && v.invocation_accepted && v.actor_current && !result);
    ++f->commits;
    CHECK(f->commits == 1);
}

static void
init(fixture_t *f, bool c1, bool park, death_case_t death, int ready_fd)
{
    memset(f, 0, sizeof(*f));
    f->c1 = c1; f->park = park; f->death = death; f->ready_fd = ready_fd;
    wirelog_error_t error;
    f->program = wirelog_parse_string(
        ".decl a(x:int64,y:int64)\n.decl b(x:int64,y:int64)\n"
        ".decl output(x:int64,y:int64)\n.decl other(x:int64,y:int64)\n"
        "output(x,y):-a(x,y).\nother(x,y):-b(x,y).\n", &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
    CHECK(!wl_evaluation_control_create(0, &f->control));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(128) << 20,
            .usable_bytes = UINT64_C(128) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    f->governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f->governor);
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = f->control;
    options.memory_governor = f->governor;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &f->session));
    CHECK(f->session->source_c1_admission == c1);
    /* A separate actual session, not a fabricated pointer/backend. */
    wl_session_options_init(&options);
    options.memory_governor = f->governor;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &f->other));
    wl_session_set_delta_cb(f->session, capture, f);
    const int64_t a[] = {1, 2}, b[] = {3, 4};
    CHECK(!wl_session_insert(f->session, "a", a, 1, 2));
    CHECK(!wl_session_insert(f->session, "b", b, 1, 2));
}

static void
check_pin(fixture_t *f, wl_session_test_completed_pin_t *pin)
{
    CHECK(!wl_session_test_completed_pin_view(f->session, pin));
    CHECK(pin->selected && !pin->active_operations
        && pin->ticket == f->saved.ticket
        && pin->operation_identity == f->saved.operation_identity
        && pin->completed_attempt_id == f->saved.completed_attempt_id
        && pin->result == f->saved.result);
}

static void *
foreign_step(void *argument)
{
    fixture_t *f = argument;
    CHECK(wl_session_step(f->session) == EBUSY);
    return NULL;
}

static void
parked_peer(fixture_t *f)
{
    CHECK(f->cancel_seen && !f->commits);
    wl_columnar_session_test_completed_view_t v = owner_view(f->session);
    CHECK(v.phase == OWNER_HANDOFF && v.ack_prepared && !v.source_ready);
    check_original(f);
    wl_session_test_completed_pin_t pin;
    check_pin(f, &pin);
    wl_evaluation_control_report_t stopped;
    CHECK(!wl_session_test_source_pending_reset_cancel(f->session, &stopped));
    CHECK(stopped.result == WL_EVALUATION_CONTROL_CANCELLED
        && stopped.stop_reason == WL_EVALUATION_CONTROL_CANCELLED
        && !stopped.execution_status && !stopped.cleanup_status);
    check_pin(f, &pin);
    check_original(f);
    /* Clearing the stopped genuine Source before peer entry is essential:
    * otherwise an admission refusal would miss the Native actor check. */
    f->peer_running = true;
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, foreign_step, f));
    CHECK(!wl_thread_join(&thread));
    f->peer_running = false;
    CHECK(f->peer_endpoints == 1 && !f->commits);
    check_pin(f, &pin);
    check_original(f);
    v = owner_view(f->session);
    CHECK(v.phase == OWNER_HANDOFF && !v.invocation_accepted && v.ack_prepared);
    /* The peer's real successful Source endpoint may set source_ready=true.
     * Rejected Native execution must still retain the pin and forbid commit. */
}

static void
run_case(bool c1, bool park, death_case_t death, int ready_fd)
{
    fixture_t f;
    init(&f, c1, park, death, ready_fd);
    current = &f;
    wl_session_test_completed_event = completed_event;
    bool handled = false;
    int rc = EAGAIN;
    for (unsigned n = 0; n != 200000 && rc == EAGAIN; ++n) {
        rc = wl_session_step(f.session);
        if (f.cancel_armed && !handled) {
            CHECK(rc == EAGAIN);
            parked_peer(&f);
            handled = true;
        }
    }
    CHECK(!rc && f.original_saved && f.active_events
        && f.active_events == f.revoked_events && f.commits == 1
        && f.ack_transitions == 1 && f.callbacks == 2);
    CHECK(park == handled);
    check_original(&f);
    wl_columnar_session_test_completed_view_t v = owner_view(f.session);
    CHECK(v.phase == OWNER_INERT && !v.ticket && !v.transaction_present
        && !v.main_pending && !v.parent_pending);
    wl_session_test_completed_pin_t pin;
    CHECK(!wl_session_test_completed_pin_view(f.session, &pin)
        && !pin.selected && !pin.active_operations);
    if (death == DEATH_RETURNED) {
        CHECK(!wl_session_source_completed_endpoint_owned(f.session,
            &f.saved, f.saved_kind, 0));
        call_bad_endpoint(&f, f.session, &f.saved, f.saved_kind, 0);
    }
    CHECK(!wl_session_step(f.session) && f.commits == 1);
    check_original(&f);
    wl_session_test_completed_event = NULL;
    current = NULL;
    wl_session_destroy(f.session);
    wl_session_destroy(f.other);
    wl_evaluation_control_release(f.control);
    /* The program owns the intern table and its committed governor credit. */
    wl_plan_free(f.plan);
    wirelog_program_free(f.program);
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(f.governor)));
    wl_columnar_memory_governor_ref_release(f.governor);
}

static void
environment(bool c1)
{
#if defined(_WIN32)
    CHECK(!_putenv_s("WIRELOG_JOIN_BATCH_BYTES", c1 ? "256" : ""));
    CHECK(!_putenv_s("WIRELOG_JOIN_BATCH_STRICT", ""));
#else
    CHECK(c1 ? !setenv("WIRELOG_JOIN_BATCH_BYTES", "256", 1)
        : !unsetenv("WIRELOG_JOIN_BATCH_BYTES"));
    CHECK(!unsetenv("WIRELOG_JOIN_BATCH_STRICT"));
#endif
}

#if !defined(_WIN32)
static void
death_case(death_case_t which, bool c1)
{
    int ready[2];
    CHECK(!pipe(ready));
    pid_t pid = fork();
    CHECK(pid >= 0);
    if (!pid) {
        close(ready[0]);
        struct rlimit core = {0, 0};
        (void)setrlimit(RLIMIT_CORE, &core);
        environment(c1);
        run_case(c1, false, which, ready[1]);
        _Exit(93);
    }
    close(ready[1]);
    char byte = 0;
    ssize_t got = read(ready[0], &byte, 1);
    close(ready[0]);
    int status;
    CHECK(waitpid(pid, &status, 0) == pid);
    CHECK(got == 1 && byte == 'R' && WIFSIGNALED(status)
        && WTERMSIG(status) == SIGABRT);
}
#endif

static char *
save_environment(const char *name)
{
    const char *value = getenv(name);
    if (!value) return NULL;
    char *copy = malloc(strlen(value) + 1);
    CHECK(copy);
    memcpy(copy, value, strlen(value) + 1);
    return copy;
}

static void
restore_environment(const char *name, char *value)
{
#if defined(_WIN32)
    CHECK(!_putenv_s(name, value ? value : ""));
#else
    CHECK(value ? !setenv(name, value, 1) : !unsetenv(name));
#endif
    free(value);
}

int
main(int argc, char **argv)
{
    if (argc > 2) return 2;
    const char *selected = argc == 2 ? argv[1] : NULL;
    if (selected && strcmp(selected, "--non-c1") && strcmp(selected, "--c1")
        && strcmp(selected, "--peer") && strcmp(selected, "--death")) return 2;
    char *batch = save_environment("WIRELOG_JOIN_BATCH_BYTES");
    char *strict = save_environment("WIRELOG_JOIN_BATCH_STRICT");
    if (!selected || !strcmp(selected, "--non-c1")) {
        environment(false);
        run_case(false, false, DEATH_NONE, -1);
    }
    if (!selected || !strcmp(selected, "--c1")) {
        environment(true);
        run_case(true, false, DEATH_NONE, -1);
    }
    if (!selected || !strcmp(selected, "--peer")) {
        environment(true);
        run_case(true, true, DEATH_NONE, -1);
    }
    if (!selected || !strcmp(selected, "--death")) {
#if !defined(_WIN32)
        for (unsigned c1 = 0; c1 != 2; ++c1)
            for (unsigned d = DEATH_COPY; d <= DEATH_RETURNED; ++d)
                death_case((death_case_t)d, c1 != 0);
#else
        puts(
            "POST_FINISH_TX: endpoint death cases require POSIX fork/wait; skipped");
#endif
    }
    restore_environment("WIRELOG_JOIN_BATCH_BYTES", batch);
    restore_environment("WIRELOG_JOIN_BATCH_STRICT", strict);
    /* A fixed10/768 observation ending with charge0 cannot naturally exhaust
     * WORK; literal ticketMAX is not a bounded setter-free test. Neither is
     * claimed as runtime coverage, and no ticket-capacity policy is installed. */
    puts("POST_FINISH_TX: selected natural cases passed");
    return 0;
}
