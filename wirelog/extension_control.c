/* Bounded expression registry preparation (#2299).
 * Each packet224 bounds guard51+body120+caller40=211<quantum256.
 * Allocator/mutex/raw mandatory lease release/destructor bodies are narrow
 * cooperation boundaries. No lookup/normalization/member loop is opaque.
 * TLS diagnostic publication is fixed171; complete locked lease interval196
 * includes error unlock, postunlock observation and next admission fronts.
 */
#include "wirelog-internal.h"
#include "extension_registry_private.h"
#include <errno.h>
#include <stdint.h>

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_extension_control_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t);
int wl_extension_control_test_fail_allocation = -1;
unsigned wl_extension_control_test_live_allocations;
#endif

static int wl_extension_control_poll(wl_evaluation_control_t *control,
    uint64_t id,
    unsigned phase, uint64_t progress) {
    if (!control || !id ||
        wl_evaluation_control_active_attempt_id(control) != id) return EINVAL;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_extension_control_test_checkpoint)
        wl_extension_control_test_checkpoint
            (control, phase, progress);
#else
    (void)phase; (void)progress;
#endif
    return wl_evaluation_control_charge_attempt(control, id, 0);
}
static int wl_extension_control_region(wl_evaluation_control_t *control,
    uint64_t id,
    unsigned phase, uint64_t progress) {
    if (!control || !id ||
        wl_evaluation_control_active_attempt_id(control) != id) return EINVAL;
    int rc = wl_evaluation_control_charge_attempt(control, id, 224);
    return rc?rc:wl_extension_control_poll(control, id, phase, progress);
}
static bool wl_extension_control_space(char byte) {
    return byte == ' ' || byte == '\t' || byte == '\n' || byte == '\r';
}
static char *wl_extension_control_allocate(size_t bytes) {
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_extension_control_test_fail_allocation == 0)return NULL;
    if (wl_extension_control_test_fail_allocation >
        0) --wl_extension_control_test_fail_allocation;
#endif
    char *copy = malloc(bytes);
#ifdef WL_SESSION_TEST_HOOKS
    if (copy)++wl_extension_control_test_live_allocations;
#endif
    return copy;
}
static void wl_extension_control_free(char *copy) {
#ifdef WL_SESSION_TEST_HOOKS
    if (copy)--wl_extension_control_test_live_allocations;
#endif
    free(copy);
}
int wl_extension_control_lookup_attempt(
    const wirelog_extension_snapshot_t *snapshot,
    const char *name, size_t name_bytes,
    const wirelog_extension_descriptor_t **descriptor,
    size_t *index, bool *found, wl_evaluation_control_t *control, uint64_t id) {
    if (name_bytes > 255 || !descriptor || !index || !found || !control ||
        !id) return EINVAL;
    int rc = wl_extension_control_region(control, id, 400, 0); if (rc)return rc;
    wl_extension_registry_lookup_outcome_t outcome =
        WL_EXTENSION_REGISTRY_LOOKUP_NULL;
    const wirelog_extension_descriptor_t *result = NULL;
    size_t result_index = SIZE_MAX;
    char *copy = NULL;
    if (!snapshot || !name)goto done;
    size_t length = 0;
    for (;;) {
        rc = wl_extension_control_region(control, id, 401, length);
        if (rc) goto fail;
        char byte = name[length];
        if (!byte)break;
        /* Invalid caller terminator contract: never read past encoded bound. */
        if (length == name_bytes) {
            rc = EINVAL; goto fail;
        }
        ++length;
    }
    size_t begin = 0, end = length;
    while (begin < length) {
        rc = wl_extension_control_region(control, id, 402, begin);
        if (rc) goto fail;
        if (!wl_extension_control_space(name[begin]))break;
        ++begin;
    }
    while (end > begin) {
        rc = wl_extension_control_region(control, id, 403, end-1);
        if (rc) goto fail;
        if (!wl_extension_control_space(name[end-1]))break;
        --end;
    }
    length = end-begin; outcome = WL_EXTENSION_REGISTRY_LOOKUP_INVALID;
    if (!length)goto done;
    /* This caller region prepays the one local free on every subsequent
     * return. Cleanup never invokes checkpoints or debits a successor. */
    rc = wl_extension_control_region(control, id, 404, length);
    if (rc) goto fail;
    copy = wl_extension_control_allocate(length+1); if (!copy)goto done;
    for (size_t j = 0; j < length; ++j) {
        rc = wl_extension_control_region(control, id, 405, j); if (rc)goto fail;
        char byte = name[begin+j];
        rc = wl_extension_control_region(control, id, 406, j); if (rc)goto fail;
        copy[j] = byte;
    }
    rc = wl_extension_control_region(control, id, 407, length);
    if (rc) goto fail;
    copy[length] = 0; outcome = WL_EXTENSION_REGISTRY_LOOKUP_MISSING;
    for (size_t entry = 0; entry < snapshot->count; ++entry) {
        rc = wl_extension_control_region(control, id, 408, entry);
        if (rc) goto fail;
        const char *candidate = snapshot->entries[entry]->name; size_t j = 0;
        for (;;) {
            rc = wl_extension_control_region(control, id, 409,
                    ((uint64_t)entry<<32)|j); if (rc) goto fail;
            unsigned char a = (unsigned char)candidate[j],
                b = (unsigned char)copy[j];
            if (a != b)break;
            if (!a) {
                result = &snapshot->entries[entry]->descriptor;
                result_index = entry;
                outcome = WL_EXTENSION_REGISTRY_LOOKUP_OK; goto done;
            }
            ++j;
        }
    }
done:
    wl_extension_control_free(copy);
    rc = wl_extension_control_region(control, id, 410, 0); if (rc)return rc;
    wl_extension_registry_lookup_error(outcome);
    *descriptor = result; *index = result_index; *found = result != NULL;
    return 0;
fail:
    wl_extension_control_free(copy); return rc;
}
int wl_extension_control_lease_index_attempt(
    const wirelog_extension_snapshot_t *snapshot,
    size_t index, const wirelog_extension_descriptor_t *expected, void **lease,
    bool *found, wl_evaluation_control_t *control, uint64_t id) {
    if (!lease || !found || !control || !id)return EINVAL;
    int rc = wl_extension_control_region(control, id, 420, 0); if (rc)return rc;
    rc = wl_extension_control_region(control, id, 421, 0); if (rc)return rc;
    rc = wl_extension_control_region(control, id, 422, 0); if (rc)return rc;
    wirelog_extension_registry_t *registry = snapshot?snapshot->registry:NULL;
    void *local = NULL; int error = 0;
    rc = wl_extension_control_region(control, id, 423, 0); if (rc)return rc;
    /* Whole validation/commit/unlock and refusal/next-guard fronts <=196:
     * 51+8+20+24+12+12+12+5+20+32. All four admissions precede lock;
     * no control operation or test hook executes while the mutex is held. */
    if (registry && expected) {
        wl_mutex_lock(&registry->mutex);
        if (index < snapshot->count) {
            wl_extension_entry_t *entry = snapshot->entries[index];
            if (&entry->descriptor == expected && entry->registry == registry &&
                entry->references) {
                if (entry->references == SIZE_MAX ||
                    registry->active_leases == SIZE_MAX) error = EOVERFLOW;
                else {
                    ++entry->references; ++registry->active_leases;
                    local = entry;
                }
            }
        }
        wl_mutex_unlock(&registry->mutex);
    }
    rc = wl_extension_control_poll(control, id, 424, 0); if (rc)goto fail;
    rc = wl_extension_control_region(control, id, 425, 0); if (rc)goto fail;
    if (error)return error;
    *lease = local; *found = local != NULL; return 0;
fail:
    /* Local authority is consumed before mandatory release. That release may
     * destroy user state and release the snapshot; never read it afterward. */
    if (local) {
        void *owned = local; local = NULL;
        wl_extension_callback_lease_release(owned);
    }
    return rc;
}
