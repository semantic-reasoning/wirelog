/*
 * tdd_protocol.c - bounded fake TDD publication protocol.
 *
 * INTERNAL TEST HARNESS ONLY.  It deliberately models a protocol boundary,
 * not the evaluator or any production queue.  Producers may block on the
 * bounded channel; the coordinator must pump concurrently to make progress.
 */
#include "tdd_protocol.h"

#include "../thread.h"

#if defined(WL_HAVE_C11_THREADS) || (!defined(_WIN32) && !defined(_WIN64))
#include <time.h>
#endif
#include <stdlib.h>
#include <stdint.h>

typedef struct {
    wl_columnar_tdd_pump_result_t kind;
    wl_columnar_tdd_payload_t *payload;
} wl_columnar_tdd_entry_t;

struct wl_columnar_tdd_protocol {
    wl_mutex_t mutex;
    wl_cond_t can_submit;
    wl_cond_t state_changed;
    size_t capacity;
    size_t count;
    size_t head;
    size_t tail;
    size_t blocked_submitters;
    wl_columnar_tdd_entry_t *entries;
    bool cancelled;
    bool complete_requested;
};

static uint64_t
wl_columnar_tdd_now_ms(void)
{
#if defined(WL_HAVE_C11_THREADS) || (!defined(_WIN32) && !defined(_WIN64))
    struct timespec now;
    if (timespec_get(&now, TIME_UTC) != TIME_UTC)
        return 0;
    return (uint64_t)now.tv_sec * 1000u +
           (uint64_t)now.tv_nsec / 1000000u;
#else
    return (uint64_t)GetTickCount64();
#endif
}

#if defined(WL_HAVE_C11_THREADS) || (!defined(_WIN32) && !defined(_WIN64))
static struct timespec
wl_columnar_tdd_deadline_from_now(unsigned timeout_ms)
{
    struct timespec now;
    struct timespec deadline;
    if (timespec_get(&now, TIME_UTC) != TIME_UTC)
        now = (struct timespec){0, 0};
    deadline = now;
    deadline.tv_sec += timeout_ms / 1000u;
    deadline.tv_nsec += (long)(timeout_ms % 1000u) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }
    return deadline;
}
#endif

static int
wl_columnar_tdd_cond_timedwait(wl_cond_t *cond, wl_mutex_t *mutex,
    unsigned timeout_ms)
{
#if defined(WL_HAVE_C11_THREADS)
    struct timespec deadline = wl_columnar_tdd_deadline_from_now(timeout_ms);
    return cnd_timedwait(&cond->c, &mutex->m, &deadline) == thrd_success
        ? 0 : -1;
#elif defined(_WIN32) || defined(_WIN64)
    if (SleepConditionVariableCS(&cond->cv, &mutex->cs, timeout_ms))
        return 0;
    return -1;
#else
    struct timespec deadline = wl_columnar_tdd_deadline_from_now(timeout_ms);
    int rc = pthread_cond_timedwait(&cond->c, &mutex->m, &deadline);
    return rc == 0 ? 0 : -1;
#endif
}

static bool
wl_columnar_tdd_full_with_blocked_submitter(
    const wl_columnar_tdd_protocol_t *protocol)
{
    return protocol->count == protocol->capacity &&
           protocol->blocked_submitters != 0;
}

static bool
wl_columnar_tdd_queue_contains(const wl_columnar_tdd_protocol_t *protocol,
    const wl_columnar_tdd_payload_t *payload)
{
    size_t i;
    size_t index;
    for (i = 0, index = protocol->head; i < protocol->count; i++) {
        const wl_columnar_tdd_entry_t *entry = &protocol->entries[index];
        if (entry->kind == WL_COLUMNAR_TDD_DATA &&
            (entry->payload == payload ||
            entry->payload->reservation == payload->reservation))
            return true;
        index = (index + 1) % protocol->capacity;
    }
    return false;
}

static void
wl_columnar_tdd_discard(wl_columnar_tdd_payload_t *payload)
{
    if (payload == NULL)
        return;
    wl_columnar_tdd_payload_release(payload);
    if (payload->reservation != NULL)
        payload->reservation->release_count++;
}

wl_columnar_tdd_protocol_t *
wl_columnar_tdd_protocol_create(size_t capacity)
{
    wl_columnar_tdd_protocol_t *protocol;
    if (capacity == 0)
        return NULL;
    protocol = calloc(1, sizeof(*protocol));
    if (protocol == NULL)
        return NULL;
    protocol->entries = calloc(capacity, sizeof(*protocol->entries));
    if (protocol->entries == NULL) {
        free(protocol);
        return NULL;
    }
    protocol->capacity = capacity;
    if (wl_mutex_init(&protocol->mutex) != 0) {
        free(protocol->entries);
        free(protocol);
        return NULL;
    }
    if (wl_cond_init(&protocol->can_submit) != 0) {
        wl_mutex_destroy(&protocol->mutex);
        free(protocol->entries);
        free(protocol);
        return NULL;
    }
    if (wl_cond_init(&protocol->state_changed) != 0) {
        wl_cond_destroy(&protocol->can_submit);
        wl_mutex_destroy(&protocol->mutex);
        free(protocol->entries);
        free(protocol);
        return NULL;
    }
    return protocol;
}

void
wl_columnar_tdd_protocol_cancel(wl_columnar_tdd_protocol_t *protocol)
{
    if (protocol == NULL)
        return;
    wl_mutex_lock(&protocol->mutex);
    if (!protocol->cancelled) {
        protocol->cancelled = true;
        while (protocol->count != 0) {
            wl_columnar_tdd_entry_t *entry = &protocol->entries[protocol->head];
            wl_columnar_tdd_discard(entry->payload);
            entry->payload = NULL;
            protocol->head = (protocol->head + 1) % protocol->capacity;
            protocol->count--;
        }
        wl_cond_broadcast(&protocol->can_submit);
        wl_cond_broadcast(&protocol->state_changed);
    }
    wl_mutex_unlock(&protocol->mutex);
}

void
wl_columnar_tdd_protocol_destroy(wl_columnar_tdd_protocol_t *protocol)
{
    if (protocol == NULL)
        return;
    wl_columnar_tdd_protocol_cancel(protocol);
    wl_cond_destroy(&protocol->state_changed);
    wl_cond_destroy(&protocol->can_submit);
    wl_mutex_destroy(&protocol->mutex);
    free(protocol->entries);
    free(protocol);
}

wl_columnar_tdd_submit_result_t
wl_columnar_tdd_protocol_submit(wl_columnar_tdd_protocol_t *protocol,
    wl_columnar_tdd_payload_t *payload)
{
    if (protocol == NULL || payload == NULL || payload->reservation == NULL)
        return WL_COLUMNAR_TDD_DUPLICATE;
    wl_mutex_lock(&protocol->mutex);
    if (wl_columnar_tdd_queue_contains(protocol, payload)) {
        wl_mutex_unlock(&protocol->mutex);
        return WL_COLUMNAR_TDD_DUPLICATE;
    }
    while (protocol->count == protocol->capacity && !protocol->cancelled &&
        !protocol->complete_requested) {
        protocol->blocked_submitters++;
        wl_cond_broadcast(&protocol->state_changed);
        wl_cond_wait(&protocol->can_submit, &protocol->mutex);
        protocol->blocked_submitters--;
        wl_cond_broadcast(&protocol->state_changed);
    }
    if (protocol->cancelled) {
        wl_mutex_unlock(&protocol->mutex);
        return WL_COLUMNAR_TDD_SUBMIT_CANCELLED;
    }
    if (protocol->complete_requested) {
        wl_mutex_unlock(&protocol->mutex);
        return WL_COLUMNAR_TDD_ALREADY_COMPLETE;
    }
    if (wl_columnar_tdd_queue_contains(protocol, payload)) {
        wl_mutex_unlock(&protocol->mutex);
        return WL_COLUMNAR_TDD_DUPLICATE;
    }
    payload->reservation->transfer_count++;
    protocol->entries[protocol->tail] = (wl_columnar_tdd_entry_t){
        WL_COLUMNAR_TDD_DATA, payload
    };
    protocol->tail = (protocol->tail + 1) % protocol->capacity;
    protocol->count++;
    wl_cond_broadcast(&protocol->can_submit);
    wl_cond_broadcast(&protocol->state_changed);
    wl_mutex_unlock(&protocol->mutex);
    return WL_COLUMNAR_TDD_SUBMITTED;
}

wl_columnar_tdd_submit_result_t
wl_columnar_tdd_protocol_complete(wl_columnar_tdd_protocol_t *protocol)
{
    wl_columnar_tdd_submit_result_t result;
    if (protocol == NULL)
        return WL_COLUMNAR_TDD_SUBMIT_CANCELLED;
    wl_mutex_lock(&protocol->mutex);
    if (protocol->cancelled) {
        wl_mutex_unlock(&protocol->mutex);
        return WL_COLUMNAR_TDD_SUBMIT_CANCELLED;
    }
    if (protocol->complete_requested) {
        wl_mutex_unlock(&protocol->mutex);
        return WL_COLUMNAR_TDD_ALREADY_COMPLETE;
    }
    protocol->complete_requested = true;
    while (protocol->count == protocol->capacity && !protocol->cancelled)
        wl_cond_wait(&protocol->can_submit, &protocol->mutex);
    if (!protocol->cancelled) {
        protocol->entries[protocol->tail] =
            (wl_columnar_tdd_entry_t){WL_COLUMNAR_TDD_COMPLETE, NULL};
        protocol->tail = (protocol->tail + 1) % protocol->capacity;
        protocol->count++;
    }
    wl_cond_broadcast(&protocol->can_submit);
    wl_cond_broadcast(&protocol->state_changed);
    result = protocol->cancelled ? WL_COLUMNAR_TDD_SUBMIT_CANCELLED
                                 : WL_COLUMNAR_TDD_SUBMITTED;
    wl_mutex_unlock(&protocol->mutex);
    return result;
}

wl_columnar_tdd_pump_result_t
wl_columnar_tdd_protocol_pump(wl_columnar_tdd_protocol_t *protocol,
    wl_columnar_tdd_message_t *message)
{
    wl_columnar_tdd_entry_t entry;
    if (message != NULL)
        *message = (wl_columnar_tdd_message_t){WL_COLUMNAR_TDD_EMPTY, NULL};
    if (protocol == NULL)
        return WL_COLUMNAR_TDD_CANCELLED;
    wl_mutex_lock(&protocol->mutex);
    if (protocol->count == 0) {
        wl_columnar_tdd_pump_result_t result =
            protocol->cancelled ? WL_COLUMNAR_TDD_CANCELLED :
            WL_COLUMNAR_TDD_EMPTY;
        wl_mutex_unlock(&protocol->mutex);
        return result;
    }
    entry = protocol->entries[protocol->head];
    if (message == NULL && entry.kind == WL_COLUMNAR_TDD_DATA) {
        wl_mutex_unlock(&protocol->mutex);
        return WL_COLUMNAR_TDD_DATA;
    }
    protocol->head = (protocol->head + 1) % protocol->capacity;
    protocol->count--;
    wl_cond_broadcast(&protocol->can_submit);
    wl_cond_broadcast(&protocol->state_changed);
    wl_mutex_unlock(&protocol->mutex);
    if (message != NULL)
        *message = (wl_columnar_tdd_message_t){entry.kind, entry.payload};
    return entry.kind;
}

bool
wl_columnar_tdd_protocol_wait_full_with_blocked_submitter(
    wl_columnar_tdd_protocol_t *protocol,
    unsigned timeout_ms)
{
    uint64_t deadline;
    bool observed = false;
    if (protocol == NULL)
        return false;
    deadline = wl_columnar_tdd_now_ms() + timeout_ms;
    wl_mutex_lock(&protocol->mutex);
    while (!wl_columnar_tdd_full_with_blocked_submitter(protocol) &&
        !protocol->cancelled && !protocol->complete_requested) {
        uint64_t now = wl_columnar_tdd_now_ms();
        unsigned remaining;
        if (timeout_ms == 0 || now >= deadline)
            break;
        remaining = (unsigned)(deadline - now);
        if (remaining == 0)
            remaining = 1;
        if (wl_columnar_tdd_cond_timedwait(&protocol->state_changed,
            &protocol->mutex, remaining) != 0)
            break;
    }
    observed = wl_columnar_tdd_full_with_blocked_submitter(protocol);
    wl_mutex_unlock(&protocol->mutex);
    return observed;
}

void
wl_columnar_tdd_payload_release(wl_columnar_tdd_payload_t *payload)
{
    if (payload != NULL)
        payload->release_count++;
}

void
wl_columnar_tdd_message_release(wl_columnar_tdd_message_t *message)
{
    if (message == NULL || message->kind != WL_COLUMNAR_TDD_DATA ||
        message->payload == NULL)
        return;
    wl_columnar_tdd_payload_release(message->payload);
    if (message->payload->reservation != NULL)
        message->payload->reservation->release_count++;
    message->payload = NULL;
}
