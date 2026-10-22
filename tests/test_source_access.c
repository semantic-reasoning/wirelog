/* Focused contract tests for the inactive source access gate (#1492). */

#include "../wirelog/columnar/source_access.h"
#include "wirelog/thread.h"

#include <stdio.h>
#include <string.h>

static int failures;

struct writer_move_thread_arg {
    wl_columnar_source_access_writer_t *dst;
    wl_columnar_source_access_writer_t *src;
    int result;
};

static void *
writer_move_on_thread(void *opaque)
{
    struct writer_move_thread_arg *arg = opaque;
    arg->result = wl_columnar_source_access_writer_move(arg->dst, arg->src);
    return NULL;
}

#define CHECK(condition, message) do { \
            if (!(condition)) { printf("FAIL: %s\n", message); failures++; \
                                return; } \
} while (0)

static int
paired_reader_acquire(wl_columnar_source_access_gate_t *source,
    wl_columnar_source_access_gate_t *descriptor,
    wl_columnar_source_access_reader_t *reader)
{
    int rc = wl_columnar_source_access_gate_reader_acquire(descriptor);
    if (rc != 0)
        return rc;
    rc = wl_columnar_source_access_reader_acquire_transferable(source, reader);
    if (rc != 0) {
        (void)wl_columnar_source_access_gate_reader_release(descriptor);
        return rc;
    }
    reader->secondary_owner = descriptor;
    return 0;
}

struct cohort_thread_arg {
    wl_columnar_source_access_cohort_t *cohort;
    int result;
};

static void *
cohort_restore_on_thread(void *opaque)
{
    struct cohort_thread_arg *arg = opaque;
    arg->result = wl_columnar_source_access_cohort_restore(arg->cohort);
    return NULL;
}

static void
test_paired_cohort(bool shared_descriptor)
{
    wl_columnar_source_access_gate_t source = { 0 }, first = { 0 };
    wl_columnar_source_access_gate_t second = { 0 };
    wl_columnar_source_access_gate_t wrong_source = { 0 };
    wl_columnar_source_access_gate_t *other
        = shared_descriptor ? &first : &second;
    wl_columnar_source_access_reader_t a = { 0 }, b = { 0 };
    wl_columnar_source_access_reader_t reader_copy;
    wl_columnar_source_access_reader_t *readers[] = { &a, &b };
    wl_columnar_source_access_cohort_entry_t entries[2] = { 0 };
    wl_columnar_source_access_cohort_t cohort = { 0 }, copy;
    wl_columnar_source_access_reader_t blocked = { 0 };
    wl_thread_t thread;
    struct cohort_thread_arg arg = { &cohort, EINVAL };

    wl_columnar_source_access_gate_init(&source);
    wl_columnar_source_access_gate_init(&first);
    wl_columnar_source_access_gate_init(&second);
    wl_columnar_source_access_gate_init(&wrong_source);
    CHECK(paired_reader_acquire(&source, &first, &a) == 0,
        "first paired reader");
    CHECK(paired_reader_acquire(&source, other, &b) == 0,
        "second paired reader");
    CHECK(wl_columnar_source_access_cohort_exchange(&source, readers, 2,
        entries, shared_descriptor ? 1 : 2, &cohort) == 0,
        "exact paired cohort exchange");
    CHECK(cohort.entry_count == (shared_descriptor ? 1u : 2u)
        && entries[0].readers == (shared_descriptor ? 2u : 1u),
        "descriptor multiplicity recorded");
    wl_columnar_source_access_cohort_t snapshot = cohort;
    CHECK(wl_columnar_source_access_cohort_validate(&cohort) == 0
        && memcmp(&cohort, &snapshot, sizeof(cohort)) == 0
        && atomic_load_explicit(&source.state, memory_order_acquire)
        == WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        "live cohort validation is read only");
    copy = cohort;
    CHECK(wl_columnar_source_access_cohort_validate(&copy) == EINVAL,
        "copied cohort validation rejected");
    CHECK(wl_columnar_source_access_cohort_restore(&copy) == EINVAL,
        "copied cohort rejected");
    CHECK(wl_columnar_source_access_reader_release(&a) == EINVAL
        && a.owner == &source && a.secondary_owner == &first,
        "suspended release leaves both references live");
    CHECK(atomic_load_explicit(&source.state, memory_order_acquire)
        == WL_COLUMNAR_SOURCE_ACCESS_WRITER
        && atomic_load_explicit(&first.state, memory_order_acquire)
        == WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        "suspended release changed neither gate");
    CHECK(wl_columnar_source_access_reader_acquire(&source, &blocked)
        == EBUSY
        && wl_columnar_source_access_gate_reader_acquire(&first) == EBUSY,
        "suspended gates deny new readers");
    reader_copy = a;
    readers[0] = &reader_copy;
    CHECK(wl_columnar_source_access_cohort_validate(&cohort) == EINVAL,
        "copied reader reference rejected");
    readers[0] = &a;
    CHECK(wl_columnar_source_access_cohort_validate(&cohort) == 0,
        "repaired reader reference validates");
    cohort.reader_count--;
    CHECK(wl_columnar_source_access_cohort_validate(&cohort) == EINVAL,
        "reader count mismatch rejected");
    cohort.reader_count++;
    CHECK(wl_columnar_source_access_cohort_validate(&cohort) == 0,
        "repaired reader count validates");
    if (!shared_descriptor) {
        entries[1].gate = entries[0].gate;
        CHECK(wl_columnar_source_access_cohort_validate(&cohort) == EINVAL,
            "duplicate descriptor entry rejected");
        entries[1].gate = &second;
        CHECK(wl_columnar_source_access_cohort_validate(&cohort) == 0,
            "repaired descriptor entry validates");
    } else {
        entries[1].gate = &second;
        cohort.entry_count = 2;
        CHECK(wl_columnar_source_access_cohort_validate(&cohort) == EBUSY,
            "unused zero-reader entry retains EBUSY result");
        cohort.entry_count = 1;
        entries[1].gate = NULL;
        CHECK(wl_columnar_source_access_cohort_validate(&cohort) == 0,
            "repaired entry count validates");
    }
    cohort.source = &wrong_source;
    CHECK(wl_columnar_source_access_cohort_validate(&cohort) == EINVAL,
        "wrong source gate rejected");
    cohort.source = &source;
    CHECK(wl_columnar_source_access_cohort_validate(&cohort) == 0,
        "repaired source gate validates");
    atomic_store_explicit(&source.state, 2, memory_order_release);
    CHECK(wl_columnar_source_access_cohort_validate(&cohort) == EBUSY,
        "source without WRITER rejected");
    atomic_store_explicit(&source.state, WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        memory_order_release);
    CHECK(wl_columnar_source_access_cohort_validate(&cohort) == 0,
        "repaired source WRITER validates");
    atomic_store_explicit(&first.state, entries[0].readers,
        memory_order_release);
    CHECK(wl_columnar_source_access_cohort_validate(&cohort) == EBUSY,
        "descriptor without WRITER rejected");
    atomic_store_explicit(&first.state, WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        memory_order_release);
    CHECK(wl_columnar_source_access_cohort_validate(&cohort) == 0,
        "repaired descriptor WRITER validates");
    entries[0].readers++;
    CHECK(wl_columnar_source_access_cohort_validate(&cohort) == EINVAL,
        "descriptor multiplicity mismatch rejected");
    CHECK(wl_columnar_source_access_cohort_restore(&cohort) == EINVAL,
        "malformed cohort snapshot rejected");
    entries[0].readers--;
    wl_columnar_source_access_gate_t *saved_descriptor
        = a.secondary_owner;
    a.secondary_owner = &source;
    CHECK(wl_columnar_source_access_cohort_validate(&cohort) == EINVAL,
        "source-as-descriptor reader validation rejected");
    CHECK(wl_columnar_source_access_cohort_restore(&cohort) == EINVAL
        && atomic_load_explicit(&source.state, memory_order_acquire)
        == WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        "source gate in reader descriptor rejected before mutation");
    a.secondary_owner = saved_descriptor;
    entries[0].gate = &source;
    CHECK(wl_columnar_source_access_cohort_validate(&cohort) == EINVAL,
        "source-as-descriptor entry validation rejected");
    CHECK(wl_columnar_source_access_cohort_restore(&cohort) == EINVAL
        && atomic_load_explicit(&source.state, memory_order_acquire)
        == WL_COLUMNAR_SOURCE_ACCESS_WRITER
        && atomic_load_explicit(&first.state, memory_order_acquire)
        == WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        "source gate in snapshot entry rejected before mutation");
    a.secondary_owner = &source;
    CHECK(wl_columnar_source_access_cohort_restore(&cohort) == EINVAL
        && atomic_load_explicit(&source.state, memory_order_acquire)
        == WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        "matching forged source descriptor does not publish source");
    a.secondary_owner = saved_descriptor;
    entries[0].gate = saved_descriptor;
    CHECK(wl_thread_create(&thread, cohort_restore_on_thread, &arg) == 0,
        "cross-thread cohort restore setup");
    CHECK(wl_thread_join(&thread) == 0 && arg.result == 0,
        "cross-thread paired cohort restore");
    CHECK(wl_columnar_source_access_cohort_restore(&cohort) == EINVAL,
        "reused cohort rejected");
    CHECK(atomic_load_explicit(&source.state, memory_order_acquire) == 2
        && atomic_load_explicit(&first.state, memory_order_acquire)
        == (shared_descriptor ? 2u : 1u),
        "paired gates restored to exact reader counts");
    CHECK(wl_columnar_source_access_reader_release(&b) == 0
        && wl_columnar_source_access_reader_release(&a) == 0,
        "restored readers release once");
    CHECK(atomic_load_explicit(&source.state, memory_order_acquire) == 0
        && atomic_load_explicit(&first.state, memory_order_acquire) == 0
        && atomic_load_explicit(&second.state, memory_order_acquire) == 0,
        "all paired gates drained");
}

static void
test_reader_acquire_steps(void)
{
    wl_columnar_source_access_gate_t gate = { 0 };
    wl_columnar_source_access_writer_t writer = { 0 };
    bool acquired = false;
    wl_columnar_source_access_gate_init(&gate);
    CHECK(wl_columnar_source_access_writer_acquire(&gate, &writer) == 0,
        "acquire-step contention writer");
    CHECK(wl_columnar_source_access_gate_reader_acquire_step(&gate,
        &acquired) == EAGAIN && !acquired
        && atomic_load_explicit(&gate.state, memory_order_acquire)
        == WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        "reader acquire step retains on writer contention");
    CHECK(wl_columnar_source_access_gate_reader_acquire_step(&gate,
        &acquired) == EAGAIN && !acquired,
        "reader acquire step retries without changing the gate");
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0,
        "release contention writer");
    CHECK(wl_columnar_source_access_gate_reader_acquire_step(&gate,
        &acquired) == 0 && acquired
        && atomic_load_explicit(&gate.state, memory_order_acquire) == 1,
        "reader acquire step admits one reader");
    CHECK(wl_columnar_source_access_gate_reader_release(&gate) == 0
        && atomic_load_explicit(&gate.state, memory_order_acquire) == 0,
        "reader acquire step balances the gate");
}

static void
test_reader_release_steps(void)
{
    wl_columnar_source_access_gate_t source = { 0 }, descriptor = { 0 };
    wl_columnar_source_access_reader_t reader = { 0 };
    bool done = false;
    wl_columnar_source_access_gate_init(&source);
    wl_columnar_source_access_gate_init(&descriptor);
    CHECK(paired_reader_acquire(&source, &descriptor, &reader) == 0,
        "stepped reader acquire");
    CHECK(wl_columnar_source_access_reader_release_step(&reader, &done) == 0
        && !done && reader.owner == &source && reader.secondary_owner
        == &descriptor && reader.release_stage == 1
        && atomic_load_explicit(&source.state, memory_order_acquire) == 0
        && atomic_load_explicit(&descriptor.state, memory_order_acquire) == 1,
        "first release step drops only source reference");
    CHECK(wl_columnar_source_access_reader_release_step(&reader, &done) == 0
        && done && !reader.owner && !reader.secondary_owner
        && atomic_load_explicit(&source.state, memory_order_acquire) == 0
        && atomic_load_explicit(&descriptor.state, memory_order_acquire) == 0,
        "second release step drops descriptor and clears token");
}

static void
test_paired_cohort_rejections(void)
{
    wl_columnar_source_access_gate_t source = { 0 }, descriptor = { 0 };
    wl_columnar_source_access_gate_t wrong = { 0 };
    wl_columnar_source_access_reader_t reader = { 0 }, external = { 0 };
    wl_columnar_source_access_reader_t copy, *readers[] = { &reader };
    wl_columnar_source_access_reader_t *duplicates[] = { &reader, &reader };
    wl_columnar_source_access_cohort_entry_t entries[2] = { 0 };
    wl_columnar_source_access_cohort_t cohort = { 0 };
    wl_columnar_source_access_gate_init(&source);
    wl_columnar_source_access_gate_init(&descriptor);
    wl_columnar_source_access_gate_init(&wrong);
    CHECK(paired_reader_acquire(&source, &descriptor, &reader) == 0,
        "paired rejection setup");
    copy = reader;
    readers[0] = &copy;
    CHECK(wl_columnar_source_access_cohort_exchange(&source, readers, 1,
        entries, 1, &cohort) == EINVAL, "copied reader rejected");
    readers[0] = &reader;
    CHECK(wl_columnar_source_access_cohort_exchange(&wrong, readers, 1,
        entries, 1, &cohort) == EINVAL, "wrong source gate rejected");
    CHECK(wl_columnar_source_access_cohort_exchange(&source, duplicates, 2,
        entries, 2, &cohort) == EINVAL, "duplicate reader rejected");
    reader.thread_valid = true;
    CHECK(wl_columnar_source_access_cohort_exchange(&source, readers, 1,
        entries, 1, &cohort) == EINVAL, "thread-valid lease rejected");
    reader.thread_valid = false;
    reader.secondary_owner = &source;
    CHECK(wl_columnar_source_access_cohort_exchange(&source, readers, 1,
        entries, 1, &cohort) == EINVAL, "source as descriptor rejected");
    reader.secondary_owner = &descriptor;
    atomic_store_explicit(&source.state, WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        memory_order_release);
    CHECK(wl_columnar_source_access_reader_release(&reader) == EINVAL
        && reader.owner == &source
        && atomic_load_explicit(&descriptor.state, memory_order_acquire) == 1,
        "release during source-first exchange changes neither gate");
    atomic_store_explicit(&source.state, 1, memory_order_release);
    CHECK(wl_columnar_source_access_reader_acquire(&source, &external) == 0,
        "external source reader setup");
    CHECK(wl_columnar_source_access_cohort_exchange(&source, readers, 1,
        entries, 1, &cohort) == EBUSY,
        "extra source reader denies exchange");
    CHECK(wl_columnar_source_access_reader_release(&external) == 0,
        "external source reader release");
    CHECK(wl_columnar_source_access_gate_reader_acquire(&descriptor) == 0,
        "external descriptor reader setup");
    CHECK(wl_columnar_source_access_cohort_exchange(&source, readers, 1,
        entries, 1, &cohort) == EBUSY,
        "extra descriptor reader denies exchange");
    CHECK(cohort.identity == 0
        && atomic_load_explicit(&source.state, memory_order_acquire) == 1
        && atomic_load_explicit(&descriptor.state, memory_order_acquire) == 2,
        "failed descriptor exchange restored source and token");
    CHECK(wl_columnar_source_access_gate_reader_release(&descriptor) == 0,
        "external descriptor reader release");
    CHECK(wl_columnar_source_access_reader_release(&reader) == 0,
        "original reader release after denials");
}

struct growth_gate_thread_arg {
    wl_columnar_source_access_growth_gate_attempt_t *attempt;
    int result;
};

static void *
growth_gate_advance_on_thread(void *opaque)
{
    struct growth_gate_thread_arg *arg = opaque;
    arg->result = wl_columnar_source_access_growth_gate_attempt_advance(
        arg->attempt);
    return NULL;
}

static void
test_growth_gate_attempt(void)
{
    wl_columnar_source_access_gate_t source = { 0 }, destination = { 0 };
    wl_columnar_source_access_growth_gate_attempt_t attempt = { 0 }, copy;
    wl_columnar_source_access_reader_t destination_reader = { 0 };
    struct growth_gate_thread_arg arg = { &attempt, 0 };
    wl_thread_t thread;
    wl_columnar_source_access_gate_init(&source);
    wl_columnar_source_access_gate_init(&destination);
    CHECK(wl_columnar_source_access_reader_acquire(&destination,
        &destination_reader) == 0, "growth destination contention setup");
    CHECK(wl_columnar_source_access_growth_gate_attempt_begin(&attempt,
        &source, &destination) == 0, "growth gate attempt begin");
    CHECK(wl_columnar_source_access_growth_gate_attempt_advance(&attempt)
        == EAGAIN && attempt.source_held && !attempt.destination_held
        && atomic_load_explicit(&source.state, memory_order_acquire) == 1,
        "one strong CAS acquires source and records partial progress");
    CHECK(wl_columnar_source_access_growth_gate_attempt_advance(&attempt)
        == EAGAIN && attempt.source_held && !attempt.destination_held
        && atomic_load_explicit(&source.state, memory_order_acquire) == 1
        && atomic_load_explicit(&destination.state, memory_order_acquire) == 1,
        "busy destination retains source without repeated acquisition");
    CHECK(wl_columnar_source_access_reader_release(&destination_reader) == 0,
        "growth destination contention release");
    CHECK(wl_columnar_source_access_growth_gate_attempt_advance(&attempt) == 0
        && attempt.source_held && attempt.destination_held
        && atomic_load_explicit(&destination.state, memory_order_acquire)
        == WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        "same attempt acquires destination after contention clears");
    copy = attempt;
    CHECK(wl_columnar_source_access_growth_gate_attempt_advance(&copy)
        == EINVAL, "copied growth gate receipt rejected");
    if (wl_thread_create(&thread, growth_gate_advance_on_thread, &arg) != 0)
        CHECK(0, "growth gate cross-thread setup");
    CHECK(wl_thread_join(&thread) == 0 && arg.result == EINVAL,
        "growth gate receipt remains on acquiring thread");

    CHECK(wl_columnar_source_access_growth_gate_attempt_release(&attempt) == 0
        && attempt.phase
        == WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_RELEASE_DESTINATION,
        "release begins with destination");
    atomic_store_explicit(&destination.state, 1, memory_order_release);
    CHECK(wl_columnar_source_access_growth_gate_attempt_advance(&attempt)
        == EAGAIN && attempt.destination_held && attempt.source_held,
        "refused destination release retains exact authority");
    atomic_store_explicit(&destination.state,
        WL_COLUMNAR_SOURCE_ACCESS_WRITER, memory_order_release);
    CHECK(wl_columnar_source_access_growth_gate_attempt_advance(&attempt)
        == EAGAIN && !attempt.destination_held && attempt.source_held
        && atomic_load_explicit(&destination.state, memory_order_acquire) == 0,
        "destination release recorded before source release");
    atomic_store_explicit(&source.state, WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        memory_order_release);
    CHECK(wl_columnar_source_access_growth_gate_attempt_advance(&attempt)
        == EAGAIN && attempt.source_held,
        "refused source release does not decrement twice");
    atomic_store_explicit(&source.state, 1, memory_order_release);
    CHECK(wl_columnar_source_access_growth_gate_attempt_advance(&attempt) == 0
        && !attempt.source_held
        && attempt.phase == WL_COLUMNAR_SOURCE_ACCESS_GROWTH_GATE_DONE
        && atomic_load_explicit(&source.state, memory_order_acquire) == 0,
        "source gate released exactly once");
    CHECK(wl_columnar_source_access_growth_gate_attempt_advance(&attempt) == 0
        && atomic_load_explicit(&source.state, memory_order_acquire) == 0,
        "completed release is idempotent");
}

static void
test_gate_set_attempt(void)
{
    wl_columnar_source_access_gate_t gates_storage[4] = { 0 };
    wl_columnar_source_access_gate_t *gates[4];
    wl_columnar_source_access_gate_mode_t modes[4];
    wl_columnar_source_access_gate_set_attempt_t attempt = { 0 }, copy;
    wl_columnar_source_access_reader_t blocker = { 0 };
    for (size_t i = 0; i < 4; i++) {
        wl_columnar_source_access_gate_init(&gates_storage[i]);
        gates[i] = &gates_storage[i];
        modes[i] = (i == 1 || i == 3)
            ? WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_WRITER
            : WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_READER;
    }
    /* Descriptor gates are ordered first, followed by separately ordered
     * storage gates, matching existing relation mutation sets. */
    for (size_t first = 0; first < 4; first += 2) {
        for (size_t i = first; i < first + 2; i++) {
            for (size_t j = i + 1; j < first + 2; j++) {
                if ((uintptr_t)gates[i] > (uintptr_t)gates[j]) {
                    wl_columnar_source_access_gate_t *gate = gates[i];
                    gates[i] = gates[j]; gates[j] = gate;
                }
            }
        }
    }
    modes[0] = WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_READER;
    modes[1] = WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_READER;
    modes[2] = WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_WRITER;
    modes[3] = WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_WRITER;
    CHECK(wl_columnar_source_access_reader_acquire(gates[2], &blocker) == 0,
        "gate set contention setup");
    CHECK(wl_columnar_source_access_gate_set_attempt_begin(&attempt, gates,
        modes, 4, 2) == 0, "gate set attempt begin");
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&attempt)
        == EAGAIN && attempt.cursor == 1,
        "gate set acquires at most one gate per advance");
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&attempt)
        == EAGAIN && attempt.cursor == 2,
        "gate set retains ordered partial authority");
    copy = attempt;
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&copy) == EINVAL,
        "copied gate set receipt rejected");
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&attempt)
        == EAGAIN && attempt.cursor == 2,
        "contended gate leaves cursor and earlier leases unchanged");
    CHECK(wl_columnar_source_access_gate_set_attempt_release(&attempt) == 0,
        "partial gate set begins release");
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&attempt) == EAGAIN
        && attempt.cursor == 1,
        "partial cleanup releases one gate per advance in reverse order");
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&attempt) == 0
        && attempt.cursor == 0
        && attempt.phase == WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_DONE,
        "partial cleanup releases the final gate");
    CHECK(wl_columnar_source_access_reader_release(&blocker) == 0,
        "gate set blocker release");
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&attempt) == 0,
        "completed gate set cleanup is idempotent");

    wl_columnar_source_access_gate_set_attempt_t full = { 0 };
    CHECK(wl_columnar_source_access_gate_set_attempt_begin(&full, gates,
        modes, 4, 2) == 0, "full gate set attempt begin");
    for (size_t i = 0; i < 3; i++)
        CHECK(wl_columnar_source_access_gate_set_attempt_advance(&full)
            == EAGAIN, "full gate set bounded intermediate acquisition");
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&full) == 0
        && full.phase == WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_HELD,
        "full gate set held after four bounded acquisitions");
    CHECK(wl_columnar_source_access_gate_set_attempt_release(&full) == 0,
        "full gate set release begins");
    for (size_t i = 0; i < 3; i++)
        CHECK(wl_columnar_source_access_gate_set_attempt_advance(&full)
            == EAGAIN, "full gate set bounded intermediate release");
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&full) == 0
        && full.phase == WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_DONE,
        "full gate set released in reverse acquisition order");

    wl_columnar_source_access_gate_t *unsorted[4] = {
        gates[1], gates[0], gates[2], gates[3]
    };
    wl_columnar_source_access_gate_t *duplicate[4] = {
        gates[0], gates[0], gates[2], gates[3]
    };
    wl_columnar_source_access_gate_mode_t reader_modes[4] = {
        WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_READER,
        WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_READER,
        WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_READER,
        WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_READER
    };
    wl_columnar_source_access_gate_set_attempt_t invalid = { 0 };
    CHECK(wl_columnar_source_access_gate_set_attempt_begin(&invalid,
        unsorted, reader_modes, 4, 2) == EINVAL,
        "gate set rejects noncanonical descriptor ordering");
    CHECK(wl_columnar_source_access_gate_set_attempt_begin(&invalid,
        duplicate, reader_modes, 4, 2) == EINVAL,
        "gate set rejects duplicate gate authority");
}

struct gate_set_thread_arg {
    wl_columnar_source_access_gate_set_attempt_t *attempt;
    int result;
};

static void *
gate_set_advance_on_thread(void *opaque)
{
    struct gate_set_thread_arg *arg = opaque;
    arg->result = wl_columnar_source_access_gate_set_attempt_advance(
        arg->attempt);
    return NULL;
}

static void
test_gate_set_rejections(void)
{
    wl_columnar_source_access_gate_t gate = { 0 }, storage = { 0 };
    wl_columnar_source_access_gate_t *gates[] = { &gate, &storage };
    wl_columnar_source_access_gate_mode_t writer[] = {
        WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_WRITER,
        WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_WRITER
    };
    wl_columnar_source_access_gate_mode_t reader[] = {
        WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_READER,
        WL_COLUMNAR_SOURCE_ACCESS_GATE_MODE_READER
    };
    wl_columnar_source_access_gate_set_attempt_t attempt = { 0 };
    struct gate_set_thread_arg arg = { &attempt, 0 };
    wl_thread_t thread;
    wl_columnar_source_access_gate_init(&gate);
    wl_columnar_source_access_gate_init(&storage);
    CHECK(wl_columnar_source_access_gate_set_attempt_begin(&attempt, gates,
        writer, 2, 1) == 0, "two-phase gate set begin");
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&attempt)
        == EAGAIN && attempt.cursor == 1,
        "descriptor gate is acquired before any storage gate");
    if (wl_thread_create(&thread, gate_set_advance_on_thread, &arg) != 0)
        CHECK(0, "gate set cross-thread setup");
    CHECK(wl_thread_join(&thread) == 0 && arg.result == EINVAL
        && attempt.cursor == 1
        && atomic_load_explicit(&gate.state, memory_order_acquire)
        == WL_COLUMNAR_SOURCE_ACCESS_WRITER
        && atomic_load_explicit(&storage.state, memory_order_acquire) == 0,
        "gate set rejects cross-thread advance without changing ownership");
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&attempt) == 0,
        "gate set acquires storage after descriptor on owner thread");
    CHECK(wl_columnar_source_access_gate_set_attempt_release(&attempt) == 0,
        "gate set release request accepted once");
    CHECK(wl_columnar_source_access_gate_set_attempt_release(&attempt)
        == EINVAL && attempt.cursor == 2,
        "repeated release request preserves exact authority");
    atomic_store_explicit(&storage.state, 1, memory_order_release);
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&attempt)
        == EAGAIN && attempt.cursor == 2,
        "release CAS refusal retains cursor for retry");
    atomic_store_explicit(&storage.state, WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        memory_order_release);
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&attempt)
        == EAGAIN && attempt.cursor == 1,
        "storage release retries before descriptor release");
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&attempt) == 0
        && attempt.phase == WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_DONE
        && atomic_load_explicit(&gate.state, memory_order_acquire) == 0,
        "release CAS retry relinquishes the writer exactly once");

    atomic_store_explicit(&storage.state, WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u,
        memory_order_release);
    wl_columnar_source_access_gate_set_attempt_t overflow = { 0 };
    CHECK(wl_columnar_source_access_gate_set_attempt_begin(&overflow, gates,
        reader, 2, 1) == 0, "overflow reader receipt begin");
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&overflow)
        == EAGAIN && overflow.cursor == 1,
        "reader overflow setup advances only descriptor gate");
    CHECK(wl_columnar_source_access_gate_set_attempt_advance(&overflow)
        == EOVERFLOW && overflow.cursor == 1,
        "reader-count overflow does not acquire or advance storage gate");
    atomic_store_explicit(&storage.state, 0, memory_order_release);
    CHECK(wl_columnar_source_access_gate_set_attempt_release(&overflow) == 0
        && wl_columnar_source_access_gate_set_attempt_advance(&overflow)
        == 0
        && overflow.phase == WL_COLUMNAR_SOURCE_ACCESS_GATE_SET_DONE,
        "overflow receipt can be safely retired without a lease");
}

static void
test_paired_cohort_partial_unwind(void)
{
    wl_columnar_source_access_gate_t source = { 0 }, first = { 0 };
    wl_columnar_source_access_gate_t second = { 0 };
    wl_columnar_source_access_reader_t a = { 0 }, b = { 0 };
    wl_columnar_source_access_reader_t *readers[] = { &a, &b };
    wl_columnar_source_access_cohort_entry_t entries[2] = { 0 };
    wl_columnar_source_access_cohort_t cohort = { 0 };
    wl_columnar_source_access_gate_init(&source);
    wl_columnar_source_access_gate_init(&first);
    wl_columnar_source_access_gate_init(&second);
    CHECK(paired_reader_acquire(&source, &first, &a) == 0
        && paired_reader_acquire(&source, &second, &b) == 0,
        "partial unwind setup");
    CHECK(wl_columnar_source_access_gate_reader_acquire(&second) == 0,
        "extra second descriptor reader");
    CHECK(wl_columnar_source_access_cohort_exchange(&source, readers, 2,
        entries, 2, &cohort) == EBUSY,
        "second descriptor denial unwinds first");
    CHECK(atomic_load_explicit(&source.state, memory_order_acquire) == 2
        && atomic_load_explicit(&first.state, memory_order_acquire) == 1
        && atomic_load_explicit(&second.state, memory_order_acquire) == 2
        && cohort.identity == 0 && a.owner == &source && b.owner == &source,
        "partial unwind restored exact counts and tokens");
    CHECK(wl_columnar_source_access_gate_reader_release(&second) == 0
        && wl_columnar_source_access_reader_release(&b) == 0
        && wl_columnar_source_access_reader_release(&a) == 0,
        "partial unwind teardown");
}

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

static void
test_writer_move(void)
{
    wl_columnar_source_access_gate_t gate = { 0 }, descriptor = { 0 };
    wl_columnar_source_access_writer_t src = { 0 }, dst = { 0 };
    wl_columnar_source_access_writer_t occupied = { 0 }, copy;
    struct writer_move_thread_arg arg = { &dst, &src, 0 };
    wl_thread_t thread;
    wl_columnar_source_access_gate_init(&gate);
    wl_columnar_source_access_gate_init(&descriptor);
    CHECK(wl_columnar_source_access_gate_reader_acquire(&descriptor) == 0,
        "writer move secondary setup");
    CHECK(wl_columnar_source_access_writer_acquire(&gate, &src) == 0,
        "writer move source setup");
    src.secondary_owner = &descriptor;
    CHECK(wl_columnar_source_access_writer_move(&src, &src) == EINVAL,
        "writer self move rejected");
    copy = src;
    CHECK(wl_columnar_source_access_writer_move(&dst, &copy) == EINVAL,
        "copied writer move rejected");
    occupied.owner = &gate;
    CHECK(wl_columnar_source_access_writer_move(&occupied, &src) == EINVAL,
        "occupied destination rejected");
    occupied.owner = NULL;
    CHECK(wl_thread_create(&thread, writer_move_on_thread, &arg) == 0
        && wl_thread_join(&thread) == 0 && arg.result == EINVAL,
        "cross-thread writer move rejected");
    atomic_store_explicit(&gate.state, 0, memory_order_release);
    CHECK(wl_columnar_source_access_writer_move(&dst, &src) == EBUSY,
        "writer move requires held gate");
    atomic_store_explicit(&gate.state, WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        memory_order_release);
    CHECK(wl_columnar_source_access_writer_move(&dst, &src) == 0,
        "writer move succeeds");
    CHECK(dst.identity == (uintptr_t)&dst && dst.owner == &gate
        && dst.secondary_owner == &descriptor && src.owner == NULL,
        "writer move preserves owner and secondary gate");
    CHECK(wl_columnar_source_access_writer_release(&src) == EINVAL,
        "moved source token cannot release");
    CHECK(wl_columnar_source_access_writer_release(&dst) == 0,
        "moved writer releases both gates once");
    CHECK(wl_columnar_source_access_writer_release(&dst) == EINVAL
        && atomic_load_explicit(&gate.state, memory_order_acquire) == 0
        && atomic_load_explicit(&descriptor.state, memory_order_acquire) == 0,
        "moved writer duplicate release rejected");
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
    test_writer_move();
    test_invalid_and_cross_thread();
    test_reader_overflow();
    test_concurrent_readers();
    test_writer_publication();
    test_paired_cohort(false);
    test_paired_cohort(true);
    test_reader_release_steps();
    test_reader_acquire_steps();
    test_paired_cohort_rejections();
    test_paired_cohort_partial_unwind();
    test_growth_gate_attempt();
    test_gate_set_attempt();
    test_gate_set_rejections();
    printf("%s\n", failures == 0 ? "PASS" : "FAIL");
    return failures == 0 ? 0 : 1;
}
