/* Program-owned intern-table admission tests for issue #1431. */

#include "../wirelog/columnar/memory_governor.h"
#include "../wirelog/intern.h"

#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

static wl_columnar_memory_governor_ref_t *
test_governor(uint64_t usable_bytes)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = usable_bytes,
        .headroom_bytes = 0,
        .usable_bytes = usable_bytes,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK,
    };
    return wl_columnar_memory_governor_ref_create(&resolution);
}

static int
test_attach_and_release(void)
{
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_governor_ref_t *ref = test_governor(256);
    wl_columnar_memory_governor_t *governor;
    int rc;

    if (!intern || !ref)
        return 1;
    governor = wl_columnar_memory_governor_ref_get(ref);
    rc = wl_intern_attach_memory_governor(intern, ref);
    if (rc != 0 || wl_columnar_memory_reserved(governor) != 256) {
        wl_intern_free(intern);
        wl_columnar_memory_governor_ref_release(ref);
        return 1;
    }
    wl_intern_free(intern);
    if (wl_columnar_memory_reserved(governor) != 0) {
        wl_columnar_memory_governor_ref_release(ref);
        return 1;
    }
    wl_columnar_memory_governor_ref_release(ref);
    return 0;
}

static int
test_denied_growth_preserves_table(void)
{
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_governor_ref_t *ref = test_governor(256);
    wl_columnar_memory_governor_t *governor;
    int64_t id;

    if (!intern || !ref)
        return 1;
    governor = wl_columnar_memory_governor_ref_get(ref);
    if (wl_intern_attach_memory_governor(intern, ref) != 0)
        return 1;
    id = wl_intern_put(intern, "new-symbol");
    if (id != -1 || wl_intern_count(intern) != 0
        || wl_intern_get(intern, "new-symbol") != -1
        || wl_columnar_memory_reserved(governor) != 256) {
        wl_intern_free(intern);
        wl_columnar_memory_governor_ref_release(ref);
        return 1;
    }
    wl_intern_free(intern);
    wl_columnar_memory_governor_ref_release(ref);
    return 0;
}

static int
test_duplicate_is_uncharged(void)
{
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_governor_ref_t *ref = test_governor(4096);
    wl_columnar_memory_governor_t *governor;
    uint64_t before;
    int64_t first;
    int64_t duplicate;

    if (!intern || !ref)
        return 1;
    governor = wl_columnar_memory_governor_ref_get(ref);
    if (wl_intern_attach_memory_governor(intern, ref) != 0)
        return 1;
    first = wl_intern_put(intern, "same-symbol");
    before = wl_columnar_memory_reserved(governor);
    duplicate = wl_intern_put(intern, "same-symbol");
    if (first < 0 || duplicate != first
        || wl_columnar_memory_reserved(governor) != before) {
        wl_intern_free(intern);
        wl_columnar_memory_governor_ref_release(ref);
        return 1;
    }
    wl_intern_free(intern);
    wl_columnar_memory_governor_ref_release(ref);
    return 0;
}

static int
test_conflicting_governor_is_rejected(void)
{
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_governor_ref_t *first = test_governor(4096);
    wl_columnar_memory_governor_ref_t *second = test_governor(4096);
    int rc;

    if (!intern || !first || !second)
        return 1;
    if (wl_intern_attach_memory_governor(intern, first) != 0)
        return 1;
    rc = wl_intern_attach_memory_governor(intern, second);
    wl_intern_free(intern);
    wl_columnar_memory_governor_ref_release(first);
    wl_columnar_memory_governor_ref_release(second);
    return rc == EBUSY ? 0 : 1;
}

static int
test_same_governor_is_idempotent_and_detachable(void)
{
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_governor_ref_t *ref = test_governor(4096);
    wl_columnar_memory_governor_t *governor;
    int rc;

    if (!intern || !ref)
        return 1;
    governor = wl_columnar_memory_governor_ref_get(ref);
    if (wl_intern_attach_memory_governor(intern, ref) != 0)
        return 1;
    rc = wl_intern_attach_memory_governor(intern, ref);
    if (rc != EALREADY || wl_columnar_memory_reserved(governor) == 0)
        return 1;
    if (wl_intern_detach_memory_governor(intern, ref) != 0
        || wl_columnar_memory_reserved(governor) != 0) {
        wl_intern_free(intern);
        wl_columnar_memory_governor_ref_release(ref);
        return 1;
    }
    wl_intern_free(intern);
    wl_columnar_memory_governor_ref_release(ref);
    return 0;
}

/* Sizes derived from the layout rather than written as literals: the slot
 * array is opaque to this test, so its element size is read back from the
 * reservation an empty table publishes on attach (64 slots, no segment). */
#define INTERN_TEST_INITIAL_SLOTS 64u
#define INTERN_TEST_SEG0_ENTRIES 64u

static uint64_t
slot_bytes_for(uint32_t slots)
{
    static uint64_t per_slot;
    if (per_slot == 0) {
        wl_intern_t *probe = wl_intern_create();
        wl_columnar_memory_governor_ref_t *ref = test_governor(UINT64_MAX / 4);
        if (probe && ref
            && wl_intern_attach_memory_governor(probe, ref) == 0)
            per_slot = wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref))
                / INTERN_TEST_INITIAL_SLOTS;
        if (probe)
            wl_intern_free(probe);
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
    }
    return per_slot * slots;
}

static uint64_t
segment_bytes_for(uint32_t seg)
{
    return (uint64_t)(INTERN_TEST_SEG0_ENTRIES << seg) * sizeof(char *);
}

/* Put "sym-<i>" for i in [0, n) and return the bytes their copies retain. */
static uint64_t
fill_symbols(wl_intern_t *intern, uint32_t n, uint64_t *sum_out)
{
    uint64_t sum = 0;
    for (uint32_t i = 0; i < n; i++) {
        char buf[32];
        int len = snprintf(buf, sizeof(buf), "sym-%u", i);
        if (len < 0 || wl_intern_put(intern, buf) != (int64_t)i)
            return 1;
        sum += (uint64_t)len + 1u;
    }
    *sum_out = sum;
    return 0;
}

/* Every id below @n still names its "sym-<i>" and resolves back to it. */
static int
symbols_intact(const wl_intern_t *intern, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++) {
        char buf[32];
        const char *back = wl_intern_reverse(intern, (int64_t)i);
        snprintf(buf, sizeof(buf), "sym-%u", i);
        if (!back || strcmp(back, buf) != 0
            || wl_intern_get(intern, buf) != (int64_t)i)
            return 0;
    }
    return 1;
}

static int
test_unique_put_charges_exact_bytes(void)
{
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_governor_ref_t *ref = test_governor(1u << 20);
    wl_columnar_memory_governor_t *governor;
    const char *sym = "unique-symbol";
    uint64_t expect;
    int ok;

    if (!intern || !ref)
        return 1;
    governor = wl_columnar_memory_governor_ref_get(ref);
    if (wl_intern_attach_memory_governor(intern, ref) != 0)
        return 1;
    expect = slot_bytes_for(INTERN_TEST_INITIAL_SLOTS)
        + segment_bytes_for(0) + strlen(sym) + 1u;
    ok = wl_intern_put(intern, sym) == 0
        && wl_columnar_memory_reserved(governor) == expect;
    wl_intern_free(intern);
    ok = ok && wl_columnar_memory_reserved(governor) == 0;
    wl_columnar_memory_governor_ref_release(ref);
    return ok ? 0 : 1;
}

/* Grow a table of @prefill symbols by one more put under a budget of
 * exactly old + new (admits) and old + new - 1 (denies).  @old_slots and
 * @new_slots describe the slot array before and after the put, @new_seg is
 * the segment the put opens or UINT32_MAX. */
static int
exact_fit_case(uint32_t prefill, uint32_t old_slots, uint32_t new_slots,
    uint32_t new_seg, const char *name)
{
    for (int deny = 0; deny < 2; deny++) {
        wl_intern_t *intern = wl_intern_create();
        wl_columnar_memory_governor_ref_t *ref;
        wl_columnar_memory_governor_t *governor;
        uint64_t sum = 0;
        uint64_t old_bytes;
        uint64_t new_bytes;
        int64_t id;
        int ok;

        if (!intern || fill_symbols(intern, prefill, &sum) != 0)
            return 1;
        old_bytes = slot_bytes_for(old_slots) + sum;
        for (uint32_t seg = 0; (INTERN_TEST_SEG0_ENTRIES << seg) <= prefill
            || seg == 0; seg++) {
            if (prefill > 0 || seg == 0)
                old_bytes += segment_bytes_for(seg);
            if (prefill == 0)
                break;
            if ((INTERN_TEST_SEG0_ENTRIES <<
                (seg + 1)) - INTERN_TEST_SEG0_ENTRIES
                >= prefill)
                break;
        }
        if (prefill == 0)
            old_bytes -= segment_bytes_for(0);
        new_bytes = old_bytes - slot_bytes_for(old_slots)
            + slot_bytes_for(new_slots) + strlen(name) + 1u
            + (new_seg == UINT32_MAX ? 0 : segment_bytes_for(new_seg));
        ref = test_governor(old_bytes + new_bytes - (uint64_t)deny);
        if (!ref)
            return 1;
        governor = wl_columnar_memory_governor_ref_get(ref);
        if (wl_intern_attach_memory_governor(intern, ref) != 0
            || wl_columnar_memory_reserved(governor) != old_bytes) {
            fprintf(stderr, "exact fit %s: attach reserved %llu, want %llu\n",
                name, (unsigned long long)wl_columnar_memory_reserved(governor),
                (unsigned long long)old_bytes);
            return 1;
        }
        id = wl_intern_put(intern, name);
        if (deny)
            ok = id == -1 && symbols_intact(intern, prefill)
                && wl_intern_count(intern) == prefill
                && wl_intern_get(intern, name) == -1
                && wl_columnar_memory_reserved(governor) == old_bytes;
        else
            ok = id == (int64_t)prefill && symbols_intact(intern, prefill)
                && wl_intern_count(intern) == prefill + 1u
                && wl_intern_get(intern, name) == (int64_t)prefill
                && wl_columnar_memory_reserved(governor) == new_bytes;
        if (!ok)
            fprintf(stderr,
                "exact fit %s (%s): id %lld reserved %llu want %llu\n",
                name, deny ? "deny" : "admit", (long long)id,
                (unsigned long long)wl_columnar_memory_reserved(governor),
                (unsigned long long)(deny ? old_bytes : new_bytes));
        wl_intern_free(intern);
        ok = ok && wl_columnar_memory_reserved(governor) == 0;
        wl_columnar_memory_governor_ref_release(ref);
        if (!ok)
            return 1;
    }
    return 0;
}

static int
test_exact_fit_string(void)
{
    /* One symbol present: the put adds only its copy. */
    return exact_fit_case(1u, INTERN_TEST_INITIAL_SLOTS,
               INTERN_TEST_INITIAL_SLOTS, UINT32_MAX, "second-symbol");
}

static int
test_exact_fit_segment(void)
{
    /* Ids 0..63 fill segment 0 (the 49th put already doubled the slots);
     * id 64 opens segment 1. */
    return exact_fit_case(64u, 2u * INTERN_TEST_INITIAL_SLOTS,
               2u * INTERN_TEST_INITIAL_SLOTS, 1u, "opens-segment-one");
}

static int
test_exact_fit_hash_resize(void)
{
    /* 48 symbols sit at the 3/4 load factor of 64 slots; the 49th put
     * doubles the slot array and frees the old one. */
    return exact_fit_case(48u, INTERN_TEST_INITIAL_SLOTS,
               2u * INTERN_TEST_INITIAL_SLOTS, UINT32_MAX, "forces-resize");
}

static int
test_three_resizes_exact_footprint(void)
{
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_governor_ref_t *ref = test_governor(1u << 20);
    wl_columnar_memory_governor_ref_t *again = test_governor(1u << 20);
    wl_columnar_memory_governor_t *governor;
    uint64_t sum = 0;
    uint64_t expect;
    uint64_t running;
    int ok;

    if (!intern || !ref || !again)
        return 1;
    governor = wl_columnar_memory_governor_ref_get(ref);
    if (wl_intern_attach_memory_governor(intern, ref) != 0)
        return 1;
    /* 200 unique symbols: the slot array doubles at the 49th, 97th and
     * 193rd put (64 -> 128 -> 256 -> 512), segment 1 opens at id 64 and
     * segment 2 at id 192, so the 193rd put both resizes and opens a
     * segment in one reservation. */
    if (fill_symbols(intern, 200u, &sum) != 0)
        return 1;
    expect = slot_bytes_for(8u * INTERN_TEST_INITIAL_SLOTS)
        + segment_bytes_for(0) + segment_bytes_for(1) + segment_bytes_for(2)
        + sum;
    running = wl_columnar_memory_reserved(governor);
    ok = running == expect && symbols_intact(intern, 200u)
        && wl_intern_count(intern) == 200u;
    if (!ok)
        fprintf(stderr, "three resizes: reserved %llu, want %llu\n",
            (unsigned long long)running, (unsigned long long)expect);
    /* Detach and re-attach: the rescan must reproduce the running total. */
    ok = ok && wl_intern_detach_memory_governor(intern, ref) == 0
        && wl_columnar_memory_reserved(governor) == 0
        && wl_intern_attach_memory_governor(intern, again) == 0
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(again)) == running;
    wl_intern_free(intern);
    ok = ok && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(again)) == 0;
    wl_columnar_memory_governor_ref_release(ref);
    wl_columnar_memory_governor_ref_release(again);
    return ok ? 0 : 1;
}

static int
test_denied_growth_on_populated_table(void)
{
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_governor_ref_t *ref;
    wl_columnar_memory_governor_t *governor;
    uint64_t sum = 0;
    uint64_t old_bytes;
    int ok;

    if (!intern || fill_symbols(intern, 10u, &sum) != 0)
        return 1;
    old_bytes = slot_bytes_for(INTERN_TEST_INITIAL_SLOTS)
        + segment_bytes_for(0) + sum;
    /* A budget of exactly the current footprint admits the attach and
     * denies every growth. */
    ref = test_governor(old_bytes);
    if (!ref)
        return 1;
    governor = wl_columnar_memory_governor_ref_get(ref);
    ok = wl_intern_attach_memory_governor(intern, ref) == 0
        && wl_columnar_memory_reserved(governor) == old_bytes
        && wl_intern_put(intern, "sym-3") == 3 /* duplicate: uncharged */
        && wl_intern_put(intern, "one-too-many") == -1
        && wl_intern_get(intern, "one-too-many") == -1
        && symbols_intact(intern, 10u) && wl_intern_count(intern) == 10u
        && wl_columnar_memory_reserved(governor) == old_bytes;
    wl_intern_free(intern);
    ok = ok && wl_columnar_memory_reserved(governor) == 0;
    wl_columnar_memory_governor_ref_release(ref);
    return ok ? 0 : 1;
}

/* Attach admits the bytes the table already holds transactionally: a
 * budget one byte short of the footprint is refused with ENOMEM, no
 * governor is retained and the table is untouched. */
static int
test_attach_denied_when_existing_bytes_exceed_budget(void)
{
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_governor_ref_t *ref;
    wl_columnar_memory_governor_ref_t *fits;
    uint64_t sum = 0;
    uint64_t old_bytes;
    int ok;

    if (!intern || fill_symbols(intern, 10u, &sum) != 0)
        return 1;
    old_bytes = slot_bytes_for(INTERN_TEST_INITIAL_SLOTS)
        + segment_bytes_for(0) + sum;
    ref = test_governor(old_bytes - 1u);
    fits = test_governor(old_bytes);
    if (!ref || !fits)
        return 1;
    ok = wl_intern_attach_memory_governor(intern, ref) == ENOMEM
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref)) == 0
        && symbols_intact(intern, 10u) && wl_intern_count(intern) == 10u
        /* The refused governor left no ownership behind: a fitting one
         * attaches normally afterwards. */
        && wl_intern_attach_memory_governor(intern, fits) == 0
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(fits)) == old_bytes;
    wl_intern_free(intern);
    ok = ok && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(fits)) == 0;
    wl_columnar_memory_governor_ref_release(ref);
    wl_columnar_memory_governor_ref_release(fits);
    return ok ? 0 : 1;
}

/* Issue #1469: once the owning governor is held only by the table (the
 * session that owned it is gone), the next attach rebinds the table.  The
 * footprint moves to the new governor exactly and growth is bounded by the
 * new budget; while the owner is still held the attach stays EBUSY.  On
 * the previous code every later attach returned EBUSY. */
static int
test_rebind_after_owner_released(void)
{
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_governor_ref_t *owner = test_governor(UINT64_MAX / 4);
    wl_columnar_memory_governor_ref_t *next = NULL;
    uint64_t footprint;
    uint64_t sum;
    int ok;

    if (!intern || !owner)
        return 1;
    if (wl_intern_attach_memory_governor(intern, owner) != 0
        || fill_symbols(intern, 10, &sum) != 0)
        return 1;
    footprint = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(owner));
    next = test_governor(footprint);
    if (!next)
        return 1;
    /* Owner still held (by this test, as a live session or result would):
     * the attach must refuse and leave both governors untouched. */
    ok = wl_intern_attach_memory_governor(intern, next) == EBUSY
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(owner)) == footprint
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(next)) == 0
        && wl_intern_attach_memory_governor(intern, owner) == EALREADY;
    /* Release the test's reference: the table is now the sole holder.
     * The owner is never read again below, the rebind frees it. */
    wl_columnar_memory_governor_ref_release(owner);
    owner = NULL;
    ok = ok && wl_intern_attach_memory_governor(intern, next) == 0
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(next)) == footprint
        && wl_intern_attach_memory_governor(intern, next) == EALREADY
        && symbols_intact(intern, 10);
    /* Growth is now bounded by the new budget, which is exactly full. */
    ok = ok && wl_intern_put(intern, "one-too-many") == -1
        && wl_intern_get(intern, "one-too-many") == -1
        && symbols_intact(intern, 10)
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(next)) == footprint;
    /* Free releases from the governor that owns the table now, exactly
     * once (a second release of the old owner would be a use after free
     * under ASan). */
    wl_intern_free(intern);
    ok = ok && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(next)) == 0;
    wl_columnar_memory_governor_ref_release(next);
    return ok ? 0 : 1;
}

/* Issue #1469: a denied rebind leaves the orphaned owner's reservation and
 * the new governor unchanged, and a later attach with enough budget still
 * rebinds.  On the previous code the first attach returned EBUSY. */
static int
test_denied_rebind_leaves_both_unchanged(void)
{
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_governor_ref_t *owner = test_governor(UINT64_MAX / 4);
    wl_columnar_memory_governor_t *owner_governor;
    wl_columnar_memory_governor_ref_t *small = NULL;
    wl_columnar_memory_governor_ref_t *enough = NULL;
    uint64_t footprint;
    uint64_t sum;
    int ok;

    if (!intern || !owner)
        return 1;
    owner_governor = wl_columnar_memory_governor_ref_get(owner);
    if (wl_intern_attach_memory_governor(intern, owner) != 0
        || fill_symbols(intern, 10, &sum) != 0)
        return 1;
    footprint = wl_columnar_memory_reserved(owner_governor);
    small = test_governor(footprint - 1u);
    enough = test_governor(footprint);
    if (!small || !enough)
        return 1;
    wl_columnar_memory_governor_ref_release(owner);
    owner = NULL;
    /* Reading the orphaned owner is valid here only because the denied
     * rebind did not free it: the table still holds its reference. */
    ok = wl_intern_attach_memory_governor(intern, small) == ENOMEM
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(small)) == 0
        && wl_columnar_memory_reserved(owner_governor) == footprint
        && symbols_intact(intern, 10);
    owner_governor = NULL;
    ok = ok && wl_intern_attach_memory_governor(intern, enough) == 0
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(enough)) == footprint
        && symbols_intact(intern, 10);
    wl_intern_free(intern);
    ok = ok && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(enough)) == 0;
    wl_columnar_memory_governor_ref_release(small);
    wl_columnar_memory_governor_ref_release(enough);
    return ok ? 0 : 1;
}

int
main(void)
{
    int failures = 0;
    failures += test_attach_and_release();
    failures += test_denied_growth_preserves_table();
    failures += test_duplicate_is_uncharged();
    failures += test_conflicting_governor_is_rejected();
    failures += test_same_governor_is_idempotent_and_detachable();
    failures += test_unique_put_charges_exact_bytes();
    failures += test_exact_fit_string();
    failures += test_exact_fit_segment();
    failures += test_exact_fit_hash_resize();
    failures += test_three_resizes_exact_footprint();
    failures += test_denied_growth_on_populated_table();
    failures += test_attach_denied_when_existing_bytes_exceed_budget();
    failures += test_rebind_after_owner_released();
    failures += test_denied_rebind_leaves_both_unchanged();
    if (failures != 0) {
        fprintf(stderr, "memory admission intern tests failed: %d\n",
            failures);
        return 1;
    }
    puts("memory admission intern tests passed");
    return 0;
}
