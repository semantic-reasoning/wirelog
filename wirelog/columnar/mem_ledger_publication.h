/* Internal controlled memory-ledger publication, not an installed API. */
#ifndef WL_COLUMNAR_MEM_LEDGER_PUBLICATION_H
#define WL_COLUMNAR_MEM_LEDGER_PUBLICATION_H

#include "columnar/mem_ledger.h"
#include "evaluation_control.h"
#include "thread.h"

typedef enum {
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ALLOC = 1,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_FREE = 2,
} wl_columnar_mem_ledger_publication_kind_t;

typedef struct {
    int subsystem;
    wl_columnar_mem_ledger_publication_kind_t kind;
    uint64_t bytes;
} wl_columnar_mem_ledger_publication_request_t;

typedef enum {
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_INERT = 0,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_READY,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_HELD,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_APPLIED,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_CONSUMED,
} wl_columnar_mem_ledger_publication_phase_t;

typedef struct {
    const void *identity;
    wl_mem_ledger_t *ledger;
    wl_evaluation_control_t *control;
    uint64_t attempt_id;
    uint64_t prepaid_apply;
    uint64_t prepaid_release;
    uint64_t prepaid_initial;
    unsigned count;
    bool thread_valid;
    bool acquisition_started;
    wl_columnar_mem_ledger_publication_phase_t phase;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    wl_columnar_mem_ledger_publication_request_t requests[2];
} wl_columnar_mem_ledger_publication_prepared_t;

/* Original zero-initialized address, same acquiring thread and live borrowed
 * ledger/control through consume. Host lifecycle remains externally serialized;
 * attempt observation is not a pin. Count0..2; NULL requests only with count0.
 * Inputs/record must not overlap ledger/control and inputs remain immutable
 * during preparation. Requests are copied, not borrowed after preparation.
 * Explicit record reinitialization is legal only with no remaining authority.
 * New protocol requires control; raw alloc/free remain separate entry points.
 */
int wl_columnar_mem_ledger_publication_prepare_controlled(wl_mem_ledger_t *,
    const wl_columnar_mem_ledger_publication_request_t *, unsigned,
    wl_columnar_mem_ledger_publication_prepared_t *, wl_evaluation_control_t *);
/* One invocation per prepared record. Every gate CAS attempt is admitted.
 * No poll or callback after successful CAS. Failed acquire is cancel-only.
 */
int wl_columnar_mem_ledger_publication_acquire_controlled(
    wl_columnar_mem_ledger_publication_prepared_t *);
/* Finite current-counter updates under original held gate; all validation
 * precedes writes. Count0 still transitions to APPLIED and requires release.
 */
int wl_columnar_mem_ledger_publication_apply(
    wl_columnar_mem_ledger_publication_prepared_t *);
/* Cleanup works after attempt end. Release accepts HELD/APPLIED; cancel only
 * READY/HELD. Applied counters/peaks are never rolled back by cancellation.
 */
int wl_columnar_mem_ledger_publication_release(
    wl_columnar_mem_ledger_publication_prepared_t *);
int wl_columnar_mem_ledger_publication_cancel(
    wl_columnar_mem_ledger_publication_prepared_t *);

enum {
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ARGUMENTS = 1,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_PREPARED,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_READY_CUTOFF,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_PRECAS,
};

#ifdef WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TEST_HOOKS
extern void (*wl_columnar_mem_ledger_publication_test_checkpoint)(
    wl_evaluation_control_t *, wl_columnar_mem_ledger_publication_prepared_t *,
    unsigned);
#endif

#endif /* WL_COLUMNAR_MEM_LEDGER_PUBLICATION_H */
