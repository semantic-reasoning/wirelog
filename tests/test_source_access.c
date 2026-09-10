/* Unit tests for the allocation-free source reader/writer gate (#1492). */

#include "../wirelog/columnar/source_access.h"
#include "../wirelog/thread.h"

#include <errno.h>
#include <stdio.h>

static int
test_gate_contract(void)
{
    wl_columnar_source_access_t gate;
    wl_columnar_source_access_token_t reader = { 0 };
    wl_columnar_source_access_token_t writer = { 0 };
    wl_columnar_source_access_token_t copy;

    wl_columnar_source_access_init(&gate);
    if (wl_columnar_source_access_read_acquire(&gate, &reader) != 0
        || wl_columnar_source_access_write_acquire(&gate, &writer) != EBUSY
        || wl_columnar_source_access_release(&writer) != EINVAL)
        return 1;
    copy = reader;
    if (wl_columnar_source_access_release(&copy) != EINVAL
        || wl_columnar_source_access_release(&reader) != 0
        || wl_columnar_source_access_release(&reader) != EINVAL)
        return 1;
    if (wl_columnar_source_access_write_acquire(&gate, &writer) != 0
        || wl_columnar_source_access_read_acquire(&gate, &reader) != EBUSY
        || wl_columnar_source_access_release(&writer) != 0)
        return 1;
    return 0;
}

static int
test_reader_count_and_overflow(void)
{
    wl_columnar_source_access_t gate;
    wl_columnar_source_access_token_t a = { 0 };
    wl_columnar_source_access_token_t b = { 0 };
    wl_columnar_source_access_token_t c = { 0 };

    wl_columnar_source_access_init(&gate);
    if (wl_columnar_source_access_read_acquire(&gate, &a) != 0
        || wl_columnar_source_access_read_acquire(&gate, &b) != 0
        || wl_columnar_source_access_release(&a) != 0
        || wl_columnar_source_access_release(&b) != 0)
        return 1;
    atomic_store_explicit(&gate.state, UINT64_MAX - UINT64_C(1),
        memory_order_relaxed);
    if (wl_columnar_source_access_read_acquire(&gate, &c) != EOVERFLOW)
        return 1;
    atomic_store_explicit(&gate.state, 0, memory_order_relaxed);
    return 0;
}

struct threaded_probe {
    wl_columnar_source_access_t *gate;
    int result;
};

static void *
probe_reader(void *arg)
{
    struct threaded_probe *probe = arg;
    wl_columnar_source_access_token_t token = { 0 };
    probe->result = wl_columnar_source_access_read_acquire(probe->gate,
            &token);
    if (probe->result == 0)
        probe->result = wl_columnar_source_access_release(&token);
    return NULL;
}

static int
test_thread_exclusion(void)
{
    wl_columnar_source_access_t gate;
    wl_columnar_source_access_token_t writer = { 0 };
    struct threaded_probe probe = { 0 };
    thread_t thread;

    wl_columnar_source_access_init(&gate);
    if (wl_columnar_source_access_write_acquire(&gate, &writer) != 0)
        return 1;
    probe.gate = &gate;
    if (thread_create(&thread, probe_reader, &probe) != 0
        || thread_join(&thread) != 0
        || probe.result != EBUSY
        || wl_columnar_source_access_release(&writer) != 0)
        return 1;
    if (thread_create(&thread, probe_reader, &probe) != 0
        || thread_join(&thread) != 0
        || probe.result != 0)
        return 1;
    return 0;
}

int
main(void)
{
    int failed = test_gate_contract() + test_reader_count_and_overflow()
        + test_thread_exclusion();
    printf("source_access: %s\n", failed ? "FAIL" : "OK");
    return failed;
}
