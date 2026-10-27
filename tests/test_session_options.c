/* Internal session options dispatch, compatibility and control lifetime. */
#include "wirelog/session.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(expr) do { if (!(expr)) { \
                             fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                                 #expr); exit(1); \
                         } } while (0)

static int legacy_calls;
static int options_calls;
static int evaluation_calls;
static bool fail_creation;
static wl_session_options_t observed;

static int
fake_create(const wl_plan_t *plan, uint32_t num_workers, wl_session_t **out)
{
    (void)plan;
    (void)num_workers;
    legacy_calls++;
    *out = calloc(1, sizeof(**out));
    return *out ? 0 : ENOMEM;
}

static int
fake_create_with_options(const wl_plan_t *plan, uint32_t num_workers,
    const wl_session_options_t *options, wl_session_t **out)
{
    (void)plan;
    (void)num_workers;
    options_calls++;
    observed = *options; /* ASan proves backend receives a complete v5. */
    if (fail_creation)
        return ENOMEM;
    *out = calloc(1, sizeof(**out));
    return *out ? 0 : ENOMEM;
}

static void
fake_destroy(wl_session_t *session)
{
    if (session->evaluation_control) {
        /* Still retained during backend teardown, even if caller released. */
        wl_evaluation_control_request_cancel(session->evaluation_control);
        CHECK(wl_evaluation_control_begin(session->evaluation_control,
            session->operation_admission) == 0);
        CHECK(wl_evaluation_control_charge(session->evaluation_control, 0)
            == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(wl_evaluation_control_finish(session->evaluation_control,
            session->operation_admission, 0,
            0) == WL_EVALUATION_CONTROL_CANCELLED);
    }
    free(session);
}

static int
fake_step(wl_session_t *session)
{
    (void)session;
    evaluation_calls++;
    return 0;
}

static int
fake_snapshot(wl_session_t *session, wirelog_on_tuple_fn cb, void *data)
{
    (void)cb;
    (void)data;
    return fake_step(session);
}

static const wl_compute_backend_t options_backend = {
    .name = "options",
    .session_create = fake_create,
    .session_destroy = fake_destroy,
    .session_create_with_options = fake_create_with_options,
    .session_step = fake_step,
    .session_snapshot = fake_snapshot,
};
static const wl_compute_backend_t legacy_backend = {
    .name = "legacy",
    .session_create = fake_create,
    .session_destroy = fake_destroy,
};

typedef struct {
    uint32_t size;
    uint32_t version;
    void *windows_job_handle;
    wl_columnar_memory_governor_ref_t *memory_governor;
} legacy_options_t;
typedef struct {
    uint32_t size, version;
    void *windows_job_handle;
    wl_columnar_memory_governor_ref_t *memory_governor;
    wl_evaluation_control_t *evaluation_control;
} v3_options_t;
typedef struct {
    uint32_t size, version;
    void *windows_job_handle;
    wl_columnar_memory_governor_ref_t *memory_governor;
    wl_evaluation_control_t *evaluation_control;
    const char *spill_directory;
    uint64_t spill_quota_bytes, spill_max_row_bytes;
} v4_options_t;

static void
compatibility(void)
{
    static int handle, governor_marker;
    wl_session_options_t options;
    wl_session_t *session = NULL;
    wl_session_options_init(&options);
    CHECK(options.version == 5 && options.size == sizeof(options));
    CHECK(options.evaluation_control == NULL);
    options.windows_job_handle = &handle;
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == 0);
    CHECK(observed.windows_job_handle == &handle && observed.version == 5);
    CHECK(wl_session_step(session) == 0);
    CHECK(wl_session_snapshot(session, NULL, NULL) == 0);
    CHECK(evaluation_calls == 2);
    wl_session_destroy(session);
    CHECK(wl_session_create(&options_backend, NULL, 1, &session) == 0);
    CHECK(legacy_calls == 1);
    wl_session_destroy(session);
    CHECK(wl_session_create_with_options(&legacy_backend, NULL, 1, &options,
        &session) == 0);
    CHECK(legacy_calls == 2);
    wl_session_destroy(session);

    /* Genuinely smaller allocation, not a new struct with size set smaller. */
    legacy_options_t *legacy = malloc(sizeof(*legacy));
    CHECK(legacy != NULL);
    *legacy = (legacy_options_t){
        .size = sizeof(*legacy), .version = 2, .windows_job_handle = &handle,
        .memory_governor =
            (wl_columnar_memory_governor_ref_t *)&governor_marker,
    };
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1,
        (const wl_session_options_t *)legacy, &session) == 0);
    CHECK(observed.version == 5 && observed.size == sizeof(observed));
    CHECK(observed.windows_job_handle == &handle);
    CHECK(observed.memory_governor == legacy->memory_governor);
    CHECK(observed.evaluation_control == NULL);
    free(legacy);
    wl_session_destroy(session);

    /* Unknown v2 tail must not be treated as a v3 control pointer. */
    unsigned char *oversized = malloc(sizeof(options) + 16);
    CHECK(oversized != NULL);
    memset(oversized, 0xff, sizeof(options) + 16);
    legacy_options_t prefix = { .size = sizeof(options) + 16, .version = 2 };
    memcpy(oversized, &prefix, sizeof(prefix));
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1,
        (const wl_session_options_t *)oversized, &session) == 0);
    CHECK(observed.evaluation_control == NULL);
    free(oversized);
    wl_session_destroy(session);

    /* Exact old v3 allocation and poisoned oversized tail are both bounded. */
    for (unsigned extra = 0; extra <= 64; extra += 64) {
        size_t length = sizeof(v3_options_t) + extra;
        unsigned char *storage = malloc(length);
        CHECK(storage);
        memset(storage, 0xff, length);
        v3_options_t v3 = { .size = (uint32_t)length, .version = 3,
                            .windows_job_handle = &handle };
        memcpy(storage, &v3, sizeof(v3));
        CHECK(wl_session_create_with_options(&options_backend, NULL, 1,
            (const wl_session_options_t *)storage, &session) == 0);
        CHECK(observed.windows_job_handle == &handle);
        CHECK(observed.spill_directory == NULL && !observed.spill_quota_bytes
            && !observed.spill_max_row_bytes);
        free(storage);
        wl_session_destroy(session);
    }

    /* Exact v4 storage and poisoned tails cannot enable v5 routing. */
    for (unsigned extra = 0; extra <= 64; extra += 64) {
        size_t length = sizeof(v4_options_t) + extra;
        unsigned char *storage = malloc(length);
        CHECK(storage);
        memset(storage, 0xff, length);
        v4_options_t v4 = { .size = (uint32_t)length, .version = 4,
                            .spill_directory = ".", .spill_quota_bytes = 4096,
                            .spill_max_row_bytes = 32 };
        memcpy(storage, &v4, sizeof(v4));
        CHECK(wl_session_create_with_options(&options_backend, NULL, 1,
            (const wl_session_options_t *)storage, &session) == 0);
        CHECK(observed.spill_quota_bytes == 4096 &&
            observed.spill_max_row_bytes == 32);
        CHECK(!observed.spill_join_batch_bytes &&
            !observed.spill_join_index_bytes);
        wl_session_destroy(session);
        free(storage);
    }

    /* First only size, then declared v2/v3/v4/v5 truncated buffers under ASan. */
    uint32_t *size_only = malloc(sizeof(*size_only));
    CHECK(size_only != NULL);
    *size_only = sizeof(*size_only);
    int calls_before = options_calls;
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1,
        (const wl_session_options_t *)size_only,
        &session) != 0 && session == NULL);
    free(size_only);
    for (unsigned version = 2; version <= 5; version++) {
        size_t length = (version == 2 ? sizeof(prefix)
            : version == 3 ? sizeof(v3_options_t)
            : version == 4 ? sizeof(v4_options_t) : sizeof(options)) - 1;
        unsigned char *short_options = calloc(1, length);
        CHECK(short_options != NULL);
        uint32_t size = (uint32_t)length;
        uint32_t wire_version = version;
        memcpy(short_options, &size, sizeof(size));
        memcpy(short_options + sizeof(size), &wire_version,
            sizeof(wire_version));
        CHECK(wl_session_create_with_options(&options_backend, NULL, 1,
            (const wl_session_options_t *)short_options,
            &session) != 0 && session == NULL);
        free(short_options);
    }
    options.version = 6;
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) != 0 && session == NULL);
    CHECK(options_calls == calls_before);
}

static void
spill_options(void)
{
    wl_session_options_t options;
    wl_session_t *session;
    wl_session_options_init(&options);
    CHECK(!options.spill_directory && !options.spill_quota_bytes
        && !options.spill_max_row_bytes);
    int before = options_calls;
    options.spill_quota_bytes = 1;
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == EINVAL && !session);
    options.spill_directory = "";
    options.spill_max_row_bytes = 32;
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == EINVAL && !session);
    options.spill_directory = ".";
    options.spill_quota_bytes = 0;
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == EINVAL && !session);
    options.spill_quota_bytes = 1024;
    options.spill_max_row_bytes = 31;
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == EINVAL && !session);
    CHECK(options_calls == before);
    options.spill_max_row_bytes = 32;
    before = legacy_calls;
    CHECK(wl_session_create_with_options(&legacy_backend, NULL, 1, &options,
        &session) == ENOTSUP && !session);
    CHECK(legacy_calls == before);
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == 0);
    CHECK(observed.spill_directory == options.spill_directory
        && observed.spill_quota_bytes == 1024 &&
        observed.spill_max_row_bytes == 32);
    wl_session_destroy(session);
}

static void
spill_join_options(void)
{
    wl_session_options_t options;
    wl_session_t *session = NULL;
    wl_session_options_init(&options);
    CHECK(!options.spill_join_batch_bytes && !options.spill_join_index_bytes);
    options.spill_join_batch_bytes = 64;
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == EINVAL && !session);
    options.spill_join_index_bytes = 128;
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == EINVAL && !session);
    options.spill_directory = ".";
    options.spill_quota_bytes = 4096;
    options.spill_max_row_bytes = 64;
    options.spill_join_batch_bytes = 0;
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == EINVAL && !session);
    options.spill_join_batch_bytes = 64;
    CHECK(wl_session_create_with_options(&legacy_backend, NULL, 1, &options,
        &session) == ENOTSUP && !session);
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == 0);
    CHECK(observed.spill_join_batch_bytes == 64 &&
        observed.spill_join_index_bytes == 128);
    wl_session_destroy(session);
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0);
    options.evaluation_control = control;
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == EINVAL && !session);
    /* Rejection did not attach or claim the control. */
    options.spill_join_batch_bytes = 0;
    options.spill_join_index_bytes = 0;
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == 0);
    wl_session_destroy(session);
    wl_evaluation_control_release(control);
}

static void
control_lifetime(void)
{
    wl_evaluation_control_t *control;
    wl_session_options_t options;
    wl_session_t *session = NULL, *second = NULL;
    int owner;
    CHECK(wl_evaluation_control_create(0, &control) == 0);
    wl_session_options_init(&options);
    options.evaluation_control = control;
    int calls_before = legacy_calls;
    CHECK(wl_session_create_with_options(&legacy_backend, NULL, 1, &options,
        &session) == ENOTSUP && session == NULL);
    CHECK(legacy_calls == calls_before);
    CHECK(wl_evaluation_control_attach(control, &owner) == 0);
    CHECK(wl_evaluation_control_detach(control, &owner) == 0);
    fail_creation = true;
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == ENOMEM && session == NULL);
    fail_creation = false;
    CHECK(wl_evaluation_control_attach(control, &owner) == 0);
    CHECK(wl_evaluation_control_detach(control, &owner) == 0);
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == 0);
    CHECK(session->evaluation_control == control &&
        session->owns_evaluation_control);
    calls_before = options_calls;
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &second) == EBUSY && second == NULL);
    CHECK(options_calls == calls_before);
    int eval_before = evaluation_calls;
    CHECK(wl_session_step(session) == ENOTSUP);
    CHECK(wl_session_snapshot(session, NULL, NULL) == ENOTSUP);
    CHECK(evaluation_calls == eval_before);
    wl_session_destroy(session);
    /* Destruction clears the claim, but caller's independent reference lives. */
    CHECK(wl_session_create_with_options(&options_backend, NULL, 1, &options,
        &session) == 0);
    wl_evaluation_control_release(control);
    memset(&options, 0, sizeof(options)); /* options storage is not borrowed */
    wl_session_destroy(session);
}

int
main(void)
{
    compatibility();
    spill_options();
    spill_join_options();
    control_lifetime();
    puts("session_options: OK");
    return 0;
}
