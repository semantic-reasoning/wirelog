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
/* Exactly one gate attempt. EAGAIN is cancel-only; success must be followed
 * immediately by callback-free apply/release on the acquiring thread. */
int wl_columnar_mem_ledger_publication_try_acquire_controlled(
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

/* A description owns no gate or prepaid mutation. The externally serialized
 * parent pins the actual ledger and control allocation through its matching
 * commit report and explicit funded ACK. Only then may it reinitialize this
 * original-address record. CONSUMED/applied distinguishes performed mutation
 * from financial refusal or one-shot CAS loss; neither permits re-description
 * before that ACK. Old prepared-publication contracts are unchanged. */
typedef enum {
    WL_COLUMNAR_MEM_LEDGER_DESCRIPTION_INERT,
    WL_COLUMNAR_MEM_LEDGER_DESCRIPTION_DESCRIBED,
    WL_COLUMNAR_MEM_LEDGER_DESCRIPTION_CONSUMED
} wl_columnar_mem_ledger_description_phase_t;

typedef struct wl_columnar_mem_ledger_description {
    struct wl_columnar_mem_ledger_description *identity;
    const void *parent;
    wl_mem_ledger_t *ledger;
    wl_evaluation_control_t *control;
    uint64_t control_identity, attempted_id;
    unsigned count;
    bool thread_valid, applied;
    wl_columnar_mem_ledger_description_phase_t phase;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    wl_columnar_mem_ledger_publication_request_t requests[2];
} wl_columnar_mem_ledger_description_t;

int wl_columnar_mem_ledger_publication_describe_controlled(
    wl_columnar_mem_ledger_description_t *, const void *, wl_mem_ledger_t *,
    const wl_columnar_mem_ledger_publication_request_t *, unsigned,
    wl_evaluation_control_t *, uint64_t);
/* Current genuine parent grant on the same allocation; no stored generation
 * is rebound. Protocol/foreign/stale rejection preserves DESCRIBED. After
 * owned attempt validation, every financial/CAS outcome is CONSUMED. One
 * successful CAS applies/releases without a subsequent poll or fallible step.
 * Inputs and record must be disjoint from each other and ledger/control;
 * the parent validates the opaque control's complete allocation extent. */
int wl_columnar_mem_ledger_publication_try_commit_described_controlled(
    wl_columnar_mem_ledger_description_t *, const void *,
    wl_evaluation_control_t *, uint64_t);

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
extern void (*wl_columnar_mem_ledger_description_test_checkpoint)(
    wl_columnar_mem_ledger_description_t *, wl_evaluation_control_t *,
    unsigned);
#endif

/* Additive finite transfer owner; existing max2 publication remains exact.
 * Zero initialize stable final-address storage. Caller owns actual ledgers
 * through terminal ACK. Control is genuinely retained after bootstrap payments.
 * Requests are immutable through preparation, copied on success and disjoint
 * from record, actual ledgers and control. Record/ledgers/control are likewise
 * disjoint. Complete opaque-control range validation is caller-owned; runtime
 * checks use only its known base address. Ledger identities alone coalesce,
 * never accounting events. Successful work864+192*count is cumulative, each
 * actual debit<=192; no entire-prepare256 claim.
 * HELD through apply/release is an immediate externally serialized callback-free
 * interval. No raw ledger accounting/control finish/reset may run in it.
 * Nofail leaves require this original sealed capability; misuse is outside
 * their contract. Checked cancel/ACK work after original completion/reset. */
typedef enum {
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_INERT,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_CANCEL_ONLY,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_READY,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_HELD,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_APPLIED,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_CONSUMED
} wl_columnar_mem_ledger_publication_transfer_phase_t;

typedef struct {
    wl_mem_ledger_t *ledger;
    wl_columnar_mem_ledger_publication_request_t event;
} wl_columnar_mem_ledger_publication_transfer_request_t;

typedef struct wl_columnar_mem_ledger_publication_transfer_prepared {
    struct wl_columnar_mem_ledger_publication_transfer_prepared *identity;
    wl_evaluation_control_t *control;
    uint64_t control_identity, attempt_id;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    bool thread_valid, busy, control_ref, acquisition_started, sealed;
    wl_columnar_mem_ledger_publication_transfer_phase_t phase;
    wl_columnar_mem_ledger_publication_transfer_request_t requests[3];
    wl_mem_ledger_t *ledgers[2];
    unsigned count, unique_count, held_mask;
    uint64_t prepaid_acquire, prepaid_apply, prepaid_release, prepaid_ack;
    int semantic_result;
} wl_columnar_mem_ledger_publication_transfer_prepared_t;

typedef struct {
    wl_columnar_mem_ledger_publication_transfer_phase_t phase;
    unsigned count, unique_count, held_mask;
    uint64_t control_identity, attempt_id;
    int semantic_result;
} wl_columnar_mem_ledger_publication_transfer_status_t;

int wl_columnar_mem_ledger_publication_transfer_prepare_controlled(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *,
    const wl_columnar_mem_ledger_publication_transfer_request_t *, unsigned,
    wl_evaluation_control_t *, uint64_t);
int wl_columnar_mem_ledger_publication_transfer_acquire_pair(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *);
void wl_columnar_mem_ledger_publication_transfer_apply_prepared_nofail(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *);
void wl_columnar_mem_ledger_publication_transfer_release_prepared_nofail(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *);
int wl_columnar_mem_ledger_publication_transfer_cancel(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *);
int wl_columnar_mem_ledger_publication_transfer_ack(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *);
int wl_columnar_mem_ledger_publication_transfer_query(
    const wl_columnar_mem_ledger_publication_transfer_prepared_t *,
    wl_columnar_mem_ledger_publication_transfer_status_t *);
#ifdef WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TEST_HOOKS
extern void (*wl_columnar_mem_ledger_publication_transfer_test_checkpoint)(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *,
    wl_evaluation_control_t *, unsigned);
#endif

#endif /* WL_COLUMNAR_MEM_LEDGER_PUBLICATION_H */
