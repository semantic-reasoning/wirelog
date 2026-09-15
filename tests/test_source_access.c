/* Focused contract tests for the inactive source access gate (#1492). */

#include "../wirelog/columnar/source_access.h"
#include "wirelog/thread.h"

#include <stdio.h>
#include <string.h>

static int failures;

#define CHECK(condition, message) do { \
            if (!(condition)) { printf("FAIL: %s\n", message); failures++; \
                                return; } \
} while (0)

static void
test_lifecycle(void)
{
    wl_columnar_source_access_gate_t gate = { 0 };
    wl_columnar_source_access_reader_t reader = { 0 }, second = { 0 }, copy;
    wl_columnar_source_access_writer_t writer = { 0 };
    wl_columnar_source_access_gate_init(&gate);
    CHECK(wl_columnar_source_access_reader_acquire(&gate, &reader) == 0,
        "reader acquire");
    copy = reader;
    CHECK(wl_columnar_source_access_reader_release(&copy) == EINVAL,
        "copied reader release rejected");
    CHECK(atomic_load_explicit(&gate.state, memory_order_relaxed) == 1
        && reader.owner == &gate, "failed copy release changed state");
    CHECK(wl_columnar_source_access_reader_acquire(&gate, &second) == 0,
        "second reader acquire");
    CHECK(atomic_load_explicit(&gate.state, memory_order_relaxed) == 2,
        "multiple readers share the gate");
    CHECK(wl_columnar_source_access_writer_acquire(&gate, &writer) == EBUSY,
        "writer blocked by reader");
    CHECK(wl_columnar_source_access_reader_release(&second) == 0,
        "second reader release");
    CHECK(atomic_load_explicit(&gate.state, memory_order_relaxed) == 1,
        "one reader remains after second release");
    CHECK(wl_columnar_source_access_reader_release(&reader) == 0,
        "reader release");
    CHECK(wl_columnar_source_access_writer_acquire(&gate, &writer) == 0,
        "writer acquire");
    CHECK(wl_columnar_source_access_reader_acquire(&gate, &reader) == EBUSY,
        "reader blocked by writer");
    CHECK(wl_columnar_source_access_writer_acquire(&gate, &writer) == EINVAL,
        "writer token reuse rejected");
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0,
        "writer release");
}

struct cross_thread_arg {
    wl_columnar_source_access_reader_t *reader;
    int result;
};

static void *
cross_thread_release(void *opaque)
{
    struct cross_thread_arg *arg = opaque;
    arg->result = wl_columnar_source_access_reader_release(arg->reader);
    return NULL;
}

static void
test_invalid_and_cross_thread(void)
{
    wl_columnar_source_access_gate_t gate = { 0 };
    wl_columnar_source_access_reader_t reader = { 0 }, copy;
    struct cross_thread_arg arg = { &reader, 0 };
    wl_thread_t thread;
    wl_columnar_source_access_gate_init(&gate);
    CHECK(wl_columnar_source_access_reader_acquire(NULL, &reader) == EINVAL,
        "null gate rejected");
    CHECK(wl_columnar_source_access_reader_acquire(&gate, NULL) == EINVAL,
        "null token rejected");
    CHECK(wl_columnar_source_access_reader_acquire(&gate, &reader) == 0,
        "reader acquire");
    copy = reader;
    CHECK(wl_columnar_source_access_reader_release(&copy) == EINVAL,
        "copy rejected");
    CHECK(wl_thread_create(&thread, cross_thread_release, &arg) == 0,
        "cross-thread setup");
    CHECK(wl_thread_join(&thread) == 0 && arg.result == EINVAL,
        "cross-thread release rejected");
    CHECK(reader.owner == &gate
        && atomic_load_explicit(&gate.state, memory_order_relaxed) == 1,
        "cross-thread failure preserved state");
    CHECK(wl_columnar_source_access_reader_release(&reader) == 0,
        "owning thread release");
    CHECK(wl_columnar_source_access_reader_release(&reader) == EINVAL,
        "duplicate release rejected");

    memset(&reader, 0, sizeof(reader));
    arg.result = EINVAL;
    CHECK(wl_columnar_source_access_reader_acquire_transferable(&gate,
        &reader) == 0, "transferable reader acquire");
    copy = reader;
    CHECK(wl_columnar_source_access_reader_release(&copy) == EINVAL,
        "copied transferable reader release rejected");
    CHECK(wl_thread_create(&thread, cross_thread_release, &arg) == 0,
        "transferable cross-thread setup");
    CHECK(wl_thread_join(&thread) == 0 && arg.result == 0,
        "transferable cross-thread release");
    CHECK(atomic_load_explicit(&gate.state, memory_order_relaxed) == 0
        && reader.owner == NULL, "transferable release balanced gate");
}

static void
test_reader_overflow(void)
{
    wl_columnar_source_access_gate_t gate = { 0 };
    wl_columnar_source_access_reader_t reader = { 0 };
    wl_columnar_source_access_gate_init(&gate);
    atomic_store_explicit(&gate.state,
        WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u, memory_order_relaxed);
    CHECK(wl_columnar_source_access_reader_acquire(&gate, &reader)
        == EOVERFLOW, "reader overflow rejected");
    CHECK(atomic_load_explicit(&gate.state, memory_order_relaxed)
        == WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u && reader.owner == NULL,
        "overflow preserved state");
    atomic_store_explicit(&gate.state, WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        memory_order_relaxed);
    CHECK(wl_columnar_source_access_reader_acquire(&gate, &reader) == EBUSY,
        "writer sentinel remains exclusive");
}

struct stress_arg {
    wl_columnar_source_access_gate_t *gate;
    unsigned iterations;
    unsigned successes;
};

static void *
reader_stress(void *opaque)
{
    struct stress_arg *arg = opaque;
    for (unsigned i = 0; i < arg->iterations; i++) {
        wl_columnar_source_access_reader_t token = { 0 };
        int rc = wl_columnar_source_access_reader_acquire(arg->gate, &token);
        if (rc == 0) {
            arg->successes++;
            if (wl_columnar_source_access_reader_release(&token) != 0)
                return (void *)1;
        } else if (rc != EBUSY && rc != EOVERFLOW)
            return (void *)1;
    }
    return NULL;
}

static void
test_concurrent_readers(void)
{
    enum { THREADS = 4 };
    wl_columnar_source_access_gate_t gate = { 0 };
    struct stress_arg args[THREADS];
    wl_thread_t threads[THREADS];
    wl_columnar_source_access_gate_init(&gate);
    unsigned created = 0;
    for (unsigned i = 0; i < THREADS; i++) {
        args[i] = (struct stress_arg){ &gate, 1000, 0 };
        if (wl_thread_create(&threads[i], reader_stress, &args[i]) != 0)
            break;
        created++;
    }
    int joins_ok = 1;
    for (unsigned i = 0; i < created; i++)
        if (wl_thread_join(&threads[i]) != 0)
            joins_ok = 0;
    CHECK(created == THREADS && joins_ok, "reader stress lifecycle");
    unsigned total = 0;
    for (unsigned i = 0; i < THREADS; i++)
        total += args[i].successes;
    CHECK(total > 0 && atomic_load_explicit(&gate.state, memory_order_relaxed)
        == 0, "reader stress balanced");
}

enum publication_phase {
    PUBLICATION_START,
    PUBLICATION_WRITER_HELD,
    PUBLICATION_READER_BLOCKED,
    PUBLICATION_WRITER_RELEASED,
    PUBLICATION_DONE,
    PUBLICATION_FAILED,
};

struct publication_payload {
    uint64_t first;
    uint64_t second;
    uint64_t third;
};

struct publication_arg {
    wl_columnar_source_access_gate_t *gate;
    wl_mutex_t control_lock;
    wl_cond_t control_changed;
    wl_mutex_t release_lock;
    wl_cond_t release_changed;
    atomic_int phase;
    struct publication_payload payload;
    atomic_int reader_busy;
    atomic_int writer_success;
    atomic_int reader_success;
};

static void
publication_set_phase(struct publication_arg *arg, enum publication_phase phase)
{
    if (wl_mutex_lock(&arg->control_lock) != 0)
        return;
    atomic_store_explicit(&arg->phase, phase, memory_order_release);
    (void)wl_cond_broadcast(&arg->control_changed);
    (void)wl_mutex_unlock(&arg->control_lock);
}

static int
publication_wait_for(struct publication_arg *arg,
    enum publication_phase expected)
{
    if (wl_mutex_lock(&arg->control_lock) != 0)
        return -1;
    while (atomic_load_explicit(&arg->phase, memory_order_acquire)
        != (int)expected
        && atomic_load_explicit(&arg->phase, memory_order_acquire)
        != PUBLICATION_FAILED) {
        if (wl_cond_wait(&arg->control_changed, &arg->control_lock) != 0) {
            (void)wl_mutex_unlock(&arg->control_lock);
            return -1;
        }
    }
    int result = atomic_load_explicit(&arg->phase, memory_order_acquire)
        == (int)expected ? 0 : -1;
    (void)wl_mutex_unlock(&arg->control_lock);
    return result;
}

static void
publication_fail(struct publication_arg *arg)
{
    publication_set_phase(arg, PUBLICATION_FAILED);
    (void)wl_cond_broadcast(&arg->release_changed);
}

static void *
publication_writer(void *opaque)
{
    struct publication_arg *arg = opaque;
    wl_columnar_source_access_writer_t token = { 0 };
    if (wl_columnar_source_access_writer_acquire(arg->gate, &token) != 0) {
        publication_fail(arg);
        return NULL;
    }

    atomic_store_explicit(&arg->writer_success, 1, memory_order_relaxed);
    publication_set_phase(arg, PUBLICATION_WRITER_HELD);
    if (publication_wait_for(arg, PUBLICATION_READER_BLOCKED) != 0) {
        (void)wl_columnar_source_access_writer_release(&token);
        return NULL;
    }

    /* The reader holds release_lock while entering cond_wait.  Acquiring it
     * here proves that the wait has atomically released the lock, so the
     * post-release broadcast below cannot be lost.  Unlock before writing
     * the payload so this coordination mutex cannot publish the payload. */
    if (wl_mutex_lock(&arg->release_lock) != 0) {
        (void)wl_columnar_source_access_writer_release(&token);
        publication_fail(arg);
        return NULL;
    }
    (void)wl_mutex_unlock(&arg->release_lock);

    /* The reader has already observed EBUSY.  Write only after that
     * coordination so the gate's release/acquire edge is the publication
     * being tested below, not a test mutex or control flag. */
    arg->payload = (struct publication_payload){
        UINT64_C(0x1122334455667788),
        UINT64_C(0x99aabbccddeeff00),
        UINT64_C(0x13579bdf2468ace0),
    };
    if (wl_columnar_source_access_writer_release(&token) != 0) {
        publication_fail(arg);
        return NULL;
    }
    /* Do not take the coordination mutex after publishing the payload: the
     * source gate release/acquire pair must remain the only publication edge
     * visible to the reader.  This phase is only a scheduling hint, so it
     * must not itself publish the payload. */
    atomic_store_explicit(&arg->phase, PUBLICATION_WRITER_RELEASED,
        memory_order_relaxed);
    (void)wl_cond_broadcast(&arg->release_changed);
    return NULL;
}

static void *
publication_reader(void *opaque)
{
    struct publication_arg *arg = opaque;
    wl_columnar_source_access_reader_t token = { 0 };
    if (publication_wait_for(arg, PUBLICATION_WRITER_HELD) != 0) {
        publication_fail(arg);
        return NULL;
    }

    if (wl_columnar_source_access_reader_acquire(arg->gate, &token)
        != EBUSY) {
        if (token.owner != NULL)
            (void)wl_columnar_source_access_reader_release(&token);
        publication_fail(arg);
        return NULL;
    }
    atomic_store_explicit(&arg->reader_busy, 1, memory_order_relaxed);
    if (wl_mutex_lock(&arg->release_lock) != 0) {
        publication_fail(arg);
        return NULL;
    }
    publication_set_phase(arg, PUBLICATION_READER_BLOCKED);
    int wait_rc = 0;
    int acquired = 0;
    while (!acquired
        && atomic_load_explicit(&arg->phase, memory_order_relaxed)
        != PUBLICATION_FAILED) {
        if (wl_cond_wait(&arg->release_changed, &arg->release_lock) != 0) {
            wait_rc = -1;
            break;
        }
        int acquire_rc = wl_columnar_source_access_reader_acquire(arg->gate,
                &token);
        if (acquire_rc == 0)
            acquired = 1;
        else if (acquire_rc != EBUSY) {
            wait_rc = -1;
            break;
        }
    }
    (void)wl_mutex_unlock(&arg->release_lock);
    if (wait_rc != 0 || !acquired) {
        publication_fail(arg);
        return NULL;
    }
    if (arg->payload.first != UINT64_C(0x1122334455667788)
        || arg->payload.second != UINT64_C(0x99aabbccddeeff00)
        || arg->payload.third != UINT64_C(0x13579bdf2468ace0)) {
        (void)wl_columnar_source_access_reader_release(&token);
        publication_fail(arg);
        return NULL;
    }
    if (wl_columnar_source_access_reader_release(&token) != 0) {
        publication_fail(arg);
        return NULL;
    }
    atomic_store_explicit(&arg->reader_success, 1, memory_order_relaxed);
    atomic_store_explicit(&arg->phase, PUBLICATION_DONE,
        memory_order_release);
    return NULL;
}

static void
test_writer_publication(void)
{
    wl_columnar_source_access_gate_t gate = { 0 };
    struct publication_arg arg = { .gate = &gate };
    wl_thread_t writer, reader;
    wl_columnar_source_access_gate_init(&gate);
    if (wl_mutex_init(&arg.control_lock) != 0)
        CHECK(0, "publication mutex init");
    if (wl_cond_init(&arg.control_changed) != 0) {
        wl_mutex_destroy(&arg.control_lock);
        CHECK(0, "publication condition init");
    }
    if (wl_mutex_init(&arg.release_lock) != 0) {
        wl_cond_destroy(&arg.control_changed);
        wl_mutex_destroy(&arg.control_lock);
        CHECK(0, "publication release mutex init");
    }
    if (wl_cond_init(&arg.release_changed) != 0) {
        wl_mutex_destroy(&arg.release_lock);
        wl_cond_destroy(&arg.control_changed);
        wl_mutex_destroy(&arg.control_lock);
        CHECK(0, "publication release condition init");
    }
    atomic_init(&arg.phase, PUBLICATION_START);
    atomic_init(&arg.reader_busy, 0);
    atomic_init(&arg.writer_success, 0);
    atomic_init(&arg.reader_success, 0);
    int writer_created = wl_thread_create(&writer, publication_writer, &arg);
    int reader_created = writer_created == 0
        ? wl_thread_create(&reader, publication_reader, &arg) : -1;
    if (writer_created != 0 || reader_created != 0) {
        publication_fail(&arg);
        if (writer_created == 0)
            (void)wl_thread_join(&writer);
        if (reader_created == 0)
            (void)wl_thread_join(&reader);
        wl_cond_destroy(&arg.release_changed);
        wl_mutex_destroy(&arg.release_lock);
        wl_cond_destroy(&arg.control_changed);
        wl_mutex_destroy(&arg.control_lock);
        CHECK(0, "publication thread create");
    }
    int writer_joined = wl_thread_join(&writer) == 0;
    int reader_joined = wl_thread_join(&reader) == 0;
    int publication_ok = writer_joined && reader_joined
        && atomic_load_explicit(&arg.phase, memory_order_acquire)
        == PUBLICATION_DONE
        && atomic_load_explicit(&arg.reader_busy, memory_order_relaxed)
        && atomic_load_explicit(&arg.writer_success, memory_order_relaxed)
        && atomic_load_explicit(&arg.reader_success, memory_order_relaxed)
        && atomic_load_explicit(&gate.state, memory_order_relaxed) == 0;
    wl_cond_destroy(&arg.release_changed);
    wl_mutex_destroy(&arg.release_lock);
    wl_cond_destroy(&arg.control_changed);
    wl_mutex_destroy(&arg.control_lock);
    CHECK(publication_ok, "writer publication ordering and balance");
}

int
main(void)
{
    printf("Source access gate tests (#1492)\n");
    test_lifecycle();
    test_invalid_and_cross_thread();
    test_reader_overflow();
    test_concurrent_readers();
    test_writer_publication();
    printf("%s\n", failures == 0 ? "PASS" : "FAIL");
    return failures == 0 ? 0 : 1;
}
