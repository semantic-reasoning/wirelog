/* Private seed physical transport. Contexts never move while initialized. */
#ifndef WL_COLUMNAR_SESSION_SEED_H
#define WL_COLUMNAR_SESSION_SEED_H
#include "internal.h"
typedef struct wl_columnar_session_seed wl_columnar_session_seed_t;
typedef enum {
    WL_COLUMNAR_SESSION_SEED_KEEP_INITIAL,
    WL_COLUMNAR_SESSION_SEED_RESET_PLAIN,
    WL_COLUMNAR_SESSION_SEED_DETACH_OBSERVED
} wl_columnar_session_seed_reset_t;
typedef struct {
    wl_columnar_session_seed_reset_t reset;
    const char *name;
    col_relation_snapshot_t target_snapshot;
} wl_columnar_session_seed_recipe_t;
typedef enum {
    WL_COLUMNAR_SESSION_SEED_FORWARD,
    WL_COLUMNAR_SESSION_SEED_RULE_READY,
    WL_COLUMNAR_SESSION_SEED_NEEDS_CLEANUP,
    WL_COLUMNAR_SESSION_SEED_COMPOSED,
    WL_COLUMNAR_SESSION_SEED_RESULT_READY,
    WL_COLUMNAR_SESSION_SEED_TAKEN,
    WL_COLUMNAR_SESSION_SEED_ENDED,
    WL_COLUMNAR_SESSION_SEED_COMPACTING
} wl_columnar_session_seed_progress_t;
/* Serialize all calls; reentrancy is unsupported. Allocate disjoint stable
 * storage of exactly this size (malloc alignment).
 * Init meters its clearing; failed Init owns nothing. Source objects and the
 * immutable recipe/name remain alive until End completes. */
size_t wl_columnar_session_seed_size(void);
int wl_columnar_session_seed_init(wl_columnar_session_seed_t *,
    wl_evaluation_control_t *, uint64_t, wl_columnar_memory_governor_ref_t *);
int wl_columnar_session_seed_prepare(wl_columnar_session_seed_t *,
    const col_rel_t *, const col_rel_t *,
    const wl_columnar_session_seed_recipe_t *);
int wl_columnar_session_seed_step(wl_columnar_session_seed_t *);
int wl_columnar_session_seed_attach_rule(wl_columnar_session_seed_t *,
    const col_rel_t *);
wl_columnar_session_seed_progress_t wl_columnar_session_seed_progress(
    const wl_columnar_session_seed_t *);
int wl_columnar_session_seed_maintenance_step(wl_columnar_session_seed_t *,
    wl_evaluation_control_t *, uint64_t);
int wl_columnar_session_seed_finalize(wl_columnar_session_seed_t *,
    wl_evaluation_control_t *, uint64_t, bool *);
/* Optional after Finalize; existing Take remains precompaction if omitted.
 * NEEDS_CLEANUP requires actual distinct maintenance funding. EAGAIN from a
 * refused raw rollback keeps this same Original and real replacement claim
 * live for another compact call; it is not completed query failure. */
int wl_columnar_session_seed_compact_final_step(wl_columnar_session_seed_t *,
    bool *);
int wl_columnar_session_seed_take(wl_columnar_session_seed_t *, col_rel_t **);
int wl_columnar_session_seed_end(wl_columnar_session_seed_t *, int,
    wl_evaluation_control_t *, uint64_t, bool *);
#ifdef WL_SESSION_TEST_HOOKS
/* Read-only cuts; no native/source permission or output ownership escapes. */
enum {
    WL_COLUMNAR_SESSION_SEED_TEST_BASE_LENGTH = 1,
    WL_COLUMNAR_SESSION_SEED_TEST_BASE_DESCRIPTOR,
    WL_COLUMNAR_SESSION_SEED_TEST_BASE_METADATA,
    WL_COLUMNAR_SESSION_SEED_TEST_BASE_PAYLOAD,
    WL_COLUMNAR_SESSION_SEED_TEST_RESIZE,
    WL_COLUMNAR_SESSION_SEED_TEST_RESTORE,
    WL_COLUMNAR_SESSION_SEED_TEST_APPEND_SCRATCH,
    WL_COLUMNAR_SESSION_SEED_TEST_SORT_SCRATCH,
    WL_COLUMNAR_SESSION_SEED_TEST_TYPE_LENGTH,
    WL_COLUMNAR_SESSION_SEED_TEST_TYPE_METADATA
};
unsigned wl_columnar_session_seed_test_base_group(
    const wl_columnar_session_seed_t *);
int wl_columnar_session_seed_test_admission(const wl_columnar_session_seed_t *,
    unsigned, uint64_t [8]);
int wl_columnar_session_seed_test_compact_view(
    const wl_columnar_session_seed_t *, uint64_t [16]);
extern void (*wl_columnar_session_seed_test_checkpoint)(
    const wl_columnar_session_seed_t *, unsigned, unsigned);
/* Actual const credit images around purchase/primitive; observation only. */
extern void (*wl_columnar_session_seed_test_credit_checkpoint)(
    const wl_columnar_session_seed_t *, unsigned, unsigned, int, size_t,
    const char *,
    const wl_columnar_memory_reservation_t *,
    const wl_columnar_memory_retirement_receipt_t *, uint64_t, bool);
int wl_columnar_session_seed_test_view(const wl_columnar_session_seed_t *,
    uint64_t [16]);
unsigned wl_columnar_session_seed_test_metadata_phase(
    const wl_columnar_session_seed_t *);
int wl_columnar_session_seed_test_base_geometry(
    const wl_columnar_session_seed_t *, uint64_t [8]);
#endif
#endif
