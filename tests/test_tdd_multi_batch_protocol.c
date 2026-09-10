/* Fake/internal TDD multi-batch protocol contract tests for issue #1454. */
#include "../wirelog/columnar/tdd_protocol.h"
#include "../wirelog/thread.h"

#include <assert.h>
#ifndef _MSC_VER
#include <stdatomic.h>
#else
#include <windows.h>
#endif
#include <stdio.h>
#include <stdlib.h>

#define BLOCKED_TIMEOUT_MS 1000u

#ifndef _MSC_VER
typedef atomic_size_t submitted_count_t;
#define submitted_load(ptr) atomic_load(ptr)
#define submitted_increment(ptr) atomic_fetch_add((ptr), 1)
#else
/* MSVC does not provide C11 atomics; match the established test fallback. */
typedef volatile LONG submitted_count_t;
#define submitted_load(ptr) (*(ptr))
#define submitted_increment(ptr) InterlockedIncrement((ptr))
#endif

typedef struct {
    wl_columnar_tdd_protocol_t *protocol;
    wl_columnar_tdd_payload_t *payloads;
    size_t start;
    size_t count;
    submitted_count_t submitted;
    submitted_count_t started;
    submitted_count_t finished;
} producer_arg_t;

static void *producer(void *opaque)
{
    producer_arg_t *arg = opaque;
    size_t i;
    for (i = arg->start; i < arg->start + arg->count; i++) {
        submitted_increment(&arg->started);
        wl_columnar_tdd_submit_result_t result =
            wl_columnar_tdd_protocol_submit(arg->protocol, &arg->payloads[i]);
        submitted_increment(&arg->finished);
        if (result != WL_COLUMNAR_TDD_SUBMITTED)
            break;
        submitted_increment(&arg->submitted);
    }
    return NULL;
}

static wl_columnar_tdd_payload_t *payloads(size_t count,
    wl_columnar_tdd_reservation_t *reservations)
{
    wl_columnar_tdd_payload_t *items = calloc(count, sizeof(*items));
    size_t i;
    assert(items != NULL);
    for (i = 0; i < count; i++) {
        items[i] = (wl_columnar_tdd_payload_t){
            i + 1, 7, 3, 11, (uint32_t)i, &reservations[i]
        };
        reservations[i].id = i + 1;
    }
    return items;
}

static void consume(wl_columnar_tdd_message_t *message)
{
    wl_columnar_tdd_message_release(message);
}

static void test_order_and_empty(void)
{
    wl_columnar_tdd_reservation_t reservations[3] = {{0}};
    wl_columnar_tdd_payload_t *items = payloads(3, reservations);
    wl_columnar_tdd_protocol_t *protocol = wl_columnar_tdd_protocol_create(4);
    wl_columnar_tdd_message_t message;
    assert(wl_columnar_tdd_protocol_pump(protocol,
        &message) == WL_COLUMNAR_TDD_EMPTY);
    assert(wl_columnar_tdd_protocol_submit(protocol,
        &items[0]) == WL_COLUMNAR_TDD_SUBMITTED);
    assert(wl_columnar_tdd_protocol_submit(protocol,
        &items[1]) == WL_COLUMNAR_TDD_SUBMITTED);
    assert(wl_columnar_tdd_protocol_submit(protocol,
        &items[2]) == WL_COLUMNAR_TDD_SUBMITTED);
    assert(wl_columnar_tdd_protocol_complete(protocol) ==
        WL_COLUMNAR_TDD_SUBMITTED);
    for (size_t i = 0; i < 3; i++) {
        assert(wl_columnar_tdd_protocol_pump(protocol,
            &message) == WL_COLUMNAR_TDD_DATA);
        assert(message.payload == &items[i]);
        consume(&message);
    }
    assert(wl_columnar_tdd_protocol_pump(protocol,
        &message) == WL_COLUMNAR_TDD_COMPLETE);
    assert(wl_columnar_tdd_protocol_pump(protocol,
        &message) == WL_COLUMNAR_TDD_EMPTY);
    for (size_t i = 0; i < 3; i++) {
        assert(items[i].release_count == 1);
        assert(reservations[i].transfer_count == 1);
        assert(reservations[i].release_count == 1);
    }
    wl_columnar_tdd_protocol_destroy(protocol);
    free(items);
}

static void test_null_pump_preserves_data(void)
{
    wl_columnar_tdd_reservation_t reservations[1] = {{0}};
    wl_columnar_tdd_payload_t *items = payloads(1, reservations);
    wl_columnar_tdd_protocol_t *protocol = wl_columnar_tdd_protocol_create(2);
    wl_columnar_tdd_message_t message;
    assert(wl_columnar_tdd_protocol_submit(protocol,
        &items[0]) == WL_COLUMNAR_TDD_SUBMITTED);
    assert(wl_columnar_tdd_protocol_pump(protocol,
        NULL) == WL_COLUMNAR_TDD_DATA);
    assert(items[0].release_count == 0);
    assert(reservations[0].release_count == 0);
    assert(wl_columnar_tdd_protocol_pump(protocol,
        &message) == WL_COLUMNAR_TDD_DATA);
    assert(message.payload == &items[0]);
    consume(&message);
    assert(items[0].release_count == 1);
    assert(reservations[0].release_count == 1);
    wl_columnar_tdd_protocol_destroy(protocol);
    free(items);
}

static void test_progress_with_blocked_producer(void)
{
    wl_columnar_tdd_reservation_t reservations[6] = {{0}};
    wl_columnar_tdd_payload_t *items = payloads(6, reservations);
    wl_columnar_tdd_protocol_t *protocol = wl_columnar_tdd_protocol_create(2);
    producer_arg_t arg = {protocol, items, 0, 6, 0, 0, 0};
    wl_thread_t thread;
    wl_columnar_tdd_message_t message;
    size_t expected;
    assert(wl_thread_create(&thread, producer, &arg) == 0);

    if (!wl_columnar_tdd_protocol_wait_full_with_blocked_submitter(protocol,
        BLOCKED_TIMEOUT_MS)) {
        wl_columnar_tdd_protocol_cancel(protocol);
        assert(wl_thread_join(&thread) == 0);
        wl_columnar_tdd_protocol_destroy(protocol);
        free(items);
        fprintf(stderr,
            "test_progress_with_blocked_producer: producer was not observed "
            "blocked in a full queue within %u ms\n",
            BLOCKED_TIMEOUT_MS);
        abort();
    }
    assert(submitted_load(&arg.submitted) == 2);
    assert(submitted_load(&arg.started) == 3);
    assert(submitted_load(&arg.finished) == 2);

    for (expected = 3; expected <= 5; expected++) {
        assert(wl_columnar_tdd_protocol_pump(protocol,
            &message) == WL_COLUMNAR_TDD_DATA);
        consume(&message);
        if (!wl_columnar_tdd_protocol_wait_full_with_blocked_submitter(
                protocol, BLOCKED_TIMEOUT_MS)) {
            wl_columnar_tdd_protocol_cancel(protocol);
            assert(wl_thread_join(&thread) == 0);
            wl_columnar_tdd_protocol_destroy(protocol);
            free(items);
            fprintf(stderr,
                "test_progress_with_blocked_producer: did not observe "
                "%zu submissions followed by a blocked full queue within "
                "%u ms\n",
                expected, BLOCKED_TIMEOUT_MS);
            abort();
        }
        assert(submitted_load(&arg.submitted) == expected);
        assert(submitted_load(&arg.started) == expected + 1);
        assert(submitted_load(&arg.finished) == expected);
    }

    assert(wl_columnar_tdd_protocol_pump(protocol,
        &message) == WL_COLUMNAR_TDD_DATA);
    consume(&message);
    assert(wl_thread_join(&thread) == 0);
    assert(submitted_load(&arg.submitted) == 6);
    assert(submitted_load(&arg.started) == 6);
    assert(submitted_load(&arg.finished) == 6);
    while (wl_columnar_tdd_protocol_pump(protocol,
        &message) == WL_COLUMNAR_TDD_DATA)
        consume(&message);
    assert(wl_columnar_tdd_protocol_complete(protocol) ==
        WL_COLUMNAR_TDD_SUBMITTED);
    while (wl_columnar_tdd_protocol_pump(protocol,
        &message) == WL_COLUMNAR_TDD_DATA)
        consume(&message);
    assert(message.kind == WL_COLUMNAR_TDD_COMPLETE);
    for (size_t i = 0; i < 6; i++)
        assert(items[i].release_count == 1);
    wl_columnar_tdd_protocol_destroy(protocol);
    free(items);
}

static void test_cancel_partial_and_alias(void)
{
    wl_columnar_tdd_reservation_t reservations[3] = {{0}};
    wl_columnar_tdd_payload_t *items = payloads(3, reservations);
    wl_columnar_tdd_protocol_t *protocol = wl_columnar_tdd_protocol_create(2);
    producer_arg_t arg = {protocol, items, 2, 1, 0, 0, 0};
    wl_thread_t thread;
    assert(wl_columnar_tdd_protocol_submit(protocol,
        &items[0]) == WL_COLUMNAR_TDD_SUBMITTED);
    assert(wl_columnar_tdd_protocol_submit(protocol,
        &items[1]) == WL_COLUMNAR_TDD_SUBMITTED);
    assert(wl_columnar_tdd_protocol_submit(protocol,
        &items[0]) == WL_COLUMNAR_TDD_DUPLICATE);
    items[2].reservation = items[0].reservation;
    assert(wl_columnar_tdd_protocol_submit(protocol,
        &items[2]) == WL_COLUMNAR_TDD_DUPLICATE);
    items[2].reservation = &reservations[2];
    assert(wl_thread_create(&thread, producer, &arg) == 0);
    if (!wl_columnar_tdd_protocol_wait_full_with_blocked_submitter(protocol,
        BLOCKED_TIMEOUT_MS)) {
        wl_columnar_tdd_protocol_cancel(protocol);
        assert(wl_thread_join(&thread) == 0);
        wl_columnar_tdd_protocol_destroy(protocol);
        free(items);
        fprintf(stderr,
            "test_cancel_partial_and_alias: producer was not observed "
            "blocked in a full queue within %u ms\n",
            BLOCKED_TIMEOUT_MS);
        abort();
    }
    assert(submitted_load(&arg.started) == 1);
    assert(submitted_load(&arg.finished) == 0);
    wl_columnar_tdd_protocol_cancel(protocol);
    assert(wl_thread_join(&thread) == 0);
    assert(submitted_load(&arg.finished) == 1);
    assert(submitted_load(&arg.submitted) == 0);
    assert(items[0].release_count == 1);
    assert(reservations[0].release_count == 1);
    assert(items[1].release_count == 1);
    assert(reservations[1].release_count == 1);
    assert(items[2].release_count == 0);
    assert(reservations[2].release_count == 0);
    assert(wl_columnar_tdd_protocol_pump(protocol,
        NULL) == WL_COLUMNAR_TDD_CANCELLED);
    wl_columnar_tdd_protocol_destroy(protocol);
    free(items);
}

int main(void)
{
    test_order_and_empty();
    test_null_pump_preserves_data();
    test_progress_with_blocked_producer();
    test_cancel_partial_and_alias();
    puts("TDD multi-batch protocol harness: PASS");
    return 0;
}
