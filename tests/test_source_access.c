/* Focused contract tests for the inactive source access gate (#1492). */

#include "../wirelog/columnar/source_access.h"
#include "wirelog/thread.h"

#include <stdio.h>

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
    thread_t thread;
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
    CHECK(thread_create(&thread, cross_thread_release, &arg) == 0,
        "cross-thread setup");
    CHECK(thread_join(&thread) == 0 && arg.result == EINVAL,
        "cross-thread release rejected");
    CHECK(reader.owner == &gate
        && atomic_load_explicit(&gate.state, memory_order_relaxed) == 1,
        "cross-thread failure preserved state");
    CHECK(wl_columnar_source_access_reader_release(&reader) == 0,
        "owning thread release");
    CHECK(wl_columnar_source_access_reader_release(&reader) == EINVAL,
        "duplicate release rejected");
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
    thread_t threads[THREADS];
    wl_columnar_source_access_gate_init(&gate);
    for (unsigned i = 0; i < THREADS; i++) {
        args[i] = (struct stress_arg){ &gate, 1000, 0 };
        CHECK(thread_create(&threads[i], reader_stress, &args[i]) == 0,
            "reader stress create");
    }
    for (unsigned i = 0; i < THREADS; i++)
        CHECK(thread_join(&threads[i]) == 0, "reader stress join");
    unsigned total = 0;
    for (unsigned i = 0; i < THREADS; i++)
        total += args[i].successes;
    CHECK(total > 0 && atomic_load_explicit(&gate.state, memory_order_relaxed)
        == 0, "reader stress balanced");
}

struct publication_arg {
    wl_columnar_source_access_gate_t *gate;
    atomic_int *payload;
    unsigned iterations;
    atomic_int failed;
    atomic_uint_fast64_t writer_active;
    atomic_uint_fast64_t writer_successes;
    atomic_uint_fast64_t reader_successes;
};

static void *
publication_writer(void *opaque)
{
    struct publication_arg *arg = opaque;
    wl_columnar_source_access_writer_t token = { 0 };
    for (unsigned i = 0; i < arg->iterations; i++) {
        int rc = wl_columnar_source_access_writer_acquire(arg->gate, &token);
        if (rc == 0) {
            atomic_store_explicit(&arg->writer_active, 1,
                memory_order_relaxed);
            atomic_store_explicit(arg->payload, (int)(i & 1u),
                memory_order_relaxed);
            atomic_fetch_add_explicit(&arg->writer_successes, 1,
                memory_order_relaxed);
            atomic_store_explicit(&arg->writer_active, 0,
                memory_order_relaxed);
            if (wl_columnar_source_access_writer_release(&token) != 0) {
                atomic_store_explicit(&arg->failed, 1, memory_order_relaxed);
                return NULL;
            }
        } else if (rc != EBUSY) {
            atomic_store_explicit(&arg->failed, 1, memory_order_relaxed);
            return NULL;
        }
    }
    return NULL;
}

static void *
publication_reader(void *opaque)
{
    struct publication_arg *arg = opaque;
    for (unsigned i = 0; i < arg->iterations; i++) {
        wl_columnar_source_access_reader_t token = { 0 };
        int rc = wl_columnar_source_access_reader_acquire(arg->gate, &token);
        if (rc == 0) {
            if (atomic_load_explicit(&arg->writer_active,
                memory_order_relaxed) != 0) {
                atomic_store_explicit(&arg->failed, 1, memory_order_relaxed);
                return NULL;
            }
            int value = atomic_load_explicit(arg->payload,
                    memory_order_relaxed);
            atomic_fetch_add_explicit(&arg->reader_successes, 1,
                memory_order_relaxed);
            if ((value != 0 && value != 1)
                || wl_columnar_source_access_reader_release(&token) != 0) {
                atomic_store_explicit(&arg->failed, 1, memory_order_relaxed);
                return NULL;
            }
        } else if (rc != EBUSY) {
            atomic_store_explicit(&arg->failed, 1, memory_order_relaxed);
            return NULL;
        }
    }
    return NULL;
}

static void
test_writer_publication(void)
{
    wl_columnar_source_access_gate_t gate = { 0 };
    atomic_int payload = 0;
    struct publication_arg arg = { &gate, &payload, 1 };
    thread_t writer, reader;
    wl_columnar_source_access_gate_init(&gate);
    atomic_store_explicit(&arg.failed, 0, memory_order_relaxed);
    atomic_store_explicit(&arg.writer_active, 0, memory_order_relaxed);
    atomic_store_explicit(&arg.writer_successes, 0, memory_order_relaxed);
    atomic_store_explicit(&arg.reader_successes, 0, memory_order_relaxed);
    CHECK(thread_create(&writer, publication_writer, &arg) == 0,
        "writer publication create");
    CHECK(thread_create(&reader, publication_reader, &arg) == 0,
        "reader publication create");
    CHECK(thread_join(&writer) == 0 && thread_join(&reader) == 0
        && atomic_load_explicit(&arg.failed, memory_order_relaxed) == 0
        && atomic_load_explicit(&arg.writer_successes, memory_order_relaxed) > 0
        && atomic_load_explicit(&arg.reader_successes, memory_order_relaxed) > 0
        && atomic_load_explicit(&gate.state, memory_order_relaxed) == 0,
        "writer publication ordering and balance");
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
