#ifndef WL_COLUMNAR_RADIX_WORKSPACE_ATTEMPT_H
#define WL_COLUMNAR_RADIX_WORKSPACE_ATTEMPT_H
#include "internal.h"
typedef struct wl_columnar_relation_radix_workspace_attempt
    wl_columnar_relation_radix_workspace_attempt_t;
/* Exclusive stable malloc-aligned storage; serialize creator-thread calls.
 * Source, lease/set and their arrays stay alive through preparation/borrow.
 * Keep the genuine mutation set intact; finish/reuse is detected and refused.
 * The one borrowed view is synchronous and confers no sort/Native authority.
 * End follows consumer completion and needs no old source snapshot. Output
 * slots must be external writable storage disjoint from context/workspace. */
size_t wl_columnar_relation_radix_workspace_attempt_size(void);
int wl_columnar_relation_radix_workspace_attempt_init(
    wl_columnar_relation_radix_workspace_attempt_t *, wl_evaluation_control_t *,
    uint64_t);
int wl_columnar_relation_radix_workspace_attempt_begin(
    wl_columnar_relation_radix_workspace_attempt_t *, col_rel_t *, uint32_t,
    uint32_t, wl_columnar_relation_mutation_lease_t *);
int wl_columnar_relation_radix_workspace_attempt_prepare_step(
    wl_columnar_relation_radix_workspace_attempt_t *, bool *);
int wl_columnar_relation_radix_workspace_attempt_borrow(
    wl_columnar_relation_radix_workspace_attempt_t *,
    const wl_columnar_radix_workspace_t **);
int wl_columnar_relation_radix_workspace_attempt_end_step(
    wl_columnar_relation_radix_workspace_attempt_t *, wl_evaluation_control_t *,
    uint64_t, bool *);
#ifdef WL_SESSION_TEST_HOOKS
extern void (*wl_columnar_relation_radix_workspace_attempt_test_checkpoint)(
    const wl_columnar_relation_radix_workspace_attempt_t *, unsigned);
int wl_columnar_relation_radix_workspace_attempt_test_view(
    const wl_columnar_relation_radix_workspace_attempt_t *, uint64_t [12]);
#endif
#endif
