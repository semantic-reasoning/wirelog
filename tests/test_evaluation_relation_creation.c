/* #1821: real governed fresh construction, before engine integration. */
#include "../wirelog/columnar/internal.h"
#include <stdio.h>

#define CHECK(x) do { if (!(x)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #x); exit(1); \
                      } } while (0)

static int owner;
static uint64_t seen, cancel_at, max_chunk;
static wl_columnar_memory_governor_ref_t *watched;
static uint64_t peak, retired_credit;
static unsigned retired, commit_stop, identity_races;
static wl_columnar_memory_reservation_t peer;
static bool peer_changed, positive_error;
static unsigned fault_phase, fault_hits;
static col_rel_t **external_out;

static uint64_t
reserved(void)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(watched));
}

static void
checkpoint(wl_evaluation_control_t *control, uint64_t work)
{
    if (external_out) CHECK(*external_out == NULL);
    CHECK(work <= WL_EVALUATION_CONTROL_QUANTUM);
    seen += work;
    if (work > max_chunk) max_chunk = work;
    if (watched && reserved() > peak) peak = reserved();
    if (cancel_at && seen >= cancel_at)
        wl_evaluation_control_request_cancel(control);
}

static void
physically_retired(void)
{
    retired++;
    retired_credit = reserved();
}

static void
committed(wl_evaluation_control_t *control, unsigned stage)
{
    if (commit_stop == stage + 1u)
        wl_evaluation_control_request_cancel(control);
}

static void
identity_race(wl_evaluation_control_t *control, uint64_t current)
{
    (void)control;
    if (identity_races) {
        col_rel_t *peer = NULL;
        CHECK(col_rel_alloc(&peer, "identity peer") == 0);
        CHECK(peer->relation_identity == current);
        CHECK(col_rel_destroy_checked(peer) == 0);
        identity_races--;
    }
}

static void
peer_race(wl_evaluation_control_t *control,
    wl_columnar_memory_governor_t *g, uint64_t expected, bool transition)
{
    (void)control; (void)expected;
    CHECK(!transition);
    if (!peer_changed) {
        CHECK(wl_columnar_memory_reserve(g, 17, &peer));
        CHECK(wl_columnar_memory_commit(&peer, &peer));
        peer_changed = true;
    }
}

static void
governor_fault(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    (void)token;
    if (phase != fault_phase) return;
    fault_hits++;
    if (positive_error)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    else
        wl_evaluation_control_request_cancel(c);
}

static wl_evaluation_control_t *
control(uint64_t limit)
{
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(limit, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, &owner) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    seen = cancel_at = max_chunk = peak = 0;
    retired = commit_stop = 0;
    return c;
}

static uint64_t
finish(wl_evaluation_control_t *c, int result)
{
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_finish(c, &owner,
        result > 0 ? result : 0, 0) == result);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    /* A charged allocation may fail before its successful-region tail. */
    CHECK(report.result == result && report.consumed >= seen);
    if (result <= 0) CHECK(report.consumed == seen);
    return report.consumed;
}

static void
retry(wl_evaluation_control_t *c, uint64_t limit)
{
    CHECK(wl_evaluation_control_reset(c) == 0);
    CHECK(wl_evaluation_control_configure(c, limit) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    seen = cancel_at = max_chunk = peak = 0;
    retired = commit_stop = 0;
}

static void
release(wl_evaluation_control_t *c)
{
    CHECK(wl_evaluation_control_detach(c, &owner) == 0);
    wl_evaluation_control_release(c);
}

static wl_columnar_memory_governor_ref_t *
governor(void)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
    };
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(g != NULL);
    return g;
}

static unsigned
digits(uint32_t column)
{
    unsigned n = 1;
    while (column >= 10) {
        column /= 10; n++;
    }
    return n;
}

/* Independent policy oracle: physical bytes, not measured consumption. */
static void
credits(const char *name, uint32_t width, uint32_t capacity, bool timestamps,
    uint64_t *descriptor, uint64_t *metadata, uint64_t *payload)
{
    *descriptor = sizeof(col_rel_t) + strlen(name) + 1;
    *metadata = 3;
    *payload = width ? width * (sizeof(int64_t *) + sizeof(int64_t)
        + (uint64_t)(capacity ? capacity : 1u) * sizeof(int64_t)) : 0;
    if (timestamps)
        *payload += (uint64_t)capacity * sizeof(col_delta_timestamp_t);
    for (uint32_t i = 0; i < width; i++)
        *metadata += sizeof(char *) + sizeof(struct ArrowSchema *)
            + sizeof(struct ArrowSchema) + 2 + 2 * (4u + digits(i));
}

static uint64_t
cost(const char *name, uint32_t width, uint32_t capacity, bool timestamps)
{
    unsigned k = width || (timestamps && capacity) ? 3 : 2;
    uint64_t total = 2 * (strlen(name) + 1) + sizeof(col_rel_t)
        + 5 * (sizeof(wl_columnar_memory_reservation_t) + 2)
        + 2 + 9 + 9 + 3 + 14 + k * (15 + 4 + 30);
    total += width ? 7 + 5 * (uint64_t)width : 3;
    if (timestamps && capacity)
        total += 1 + (uint64_t)capacity * sizeof(col_delta_timestamp_t);
    for (uint32_t i = 0; i < width; i++) {
        uint64_t d = digits(i), n = 4 + d;
        total += 3 * (3 * d + n) + 17 + 2 * n;
    }
    return total;
}

static void
equal(col_rel_t *r, col_rel_t *raw)
{
    CHECK(r && raw && r->nrows == 0 && r->ncols == raw->ncols);
    CHECK(strcmp(r->name, raw->name) == 0 && r->capacity == raw->capacity);
    CHECK(r->timestamp_capacity == raw->timestamp_capacity);
    CHECK((r->timestamps != NULL) == (raw->timestamps != NULL));
    CHECK((r->columns != NULL) == (raw->columns != NULL));
    CHECK((r->row_scratch != NULL) == (raw->row_scratch != NULL));
    CHECK(!r->column_types && !r->compound_arity_map && r->schema_ok);
    CHECK(r->view_generation == raw->view_generation
        && r->storage_generation == raw->storage_generation);
    CHECK(r->storage_owner == r && r->storage_owner_identity
        == r->relation_identity && r->storage_owner_generation
        == r->storage_generation);
    CHECK(strcmp(r->schema.format, raw->schema.format) == 0
        && r->schema.n_children == raw->schema.n_children);
    for (uint32_t i = 0; i < r->ncols; i++) {
        CHECK(r->columns[i] != NULL);
        CHECK(strcmp(r->col_names[i], raw->col_names[i]) == 0);
        CHECK(strcmp(r->schema.children[i]->name,
            raw->schema.children[i]->name) == 0);
        CHECK(strcmp(r->schema.children[i]->format, "l") == 0);
    }
    if (r->timestamps)
        for (uint32_t i = 0; i < r->timestamp_capacity; i++) {
            col_delta_timestamp_t zero = { 0 };
            CHECK(memcmp(&r->timestamps[i], &zero, sizeof(zero)) == 0);
        }
}

static void
test_success(const char *name, uint32_t width, uint32_t capacity,
    bool timestamps)
{
    watched = governor();
    uint64_t d, m, p, total = cost(name, width, capacity, timestamps);
    credits(name, width, capacity, timestamps, &d, &m, &p);
    col_rel_t *r = NULL, *raw = NULL;
    wl_evaluation_control_t *c = control(total - 1);
    external_out = &r;
    CHECK(wl_columnar_relation_new_auto_governed_controlled(name, width,
        capacity, timestamps, watched, &r,
        c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(r == NULL && finish(c,
        WL_EVALUATION_CONTROL_WORK_LIMIT) == total - 14);
    CHECK(reserved() == 0 && retired == 1 && retired_credit == d + m + p);
    for (unsigned repeat = 0; repeat < 2; repeat++) {
        retry(c, total);
        r = NULL; external_out = &r;
        CHECK(wl_columnar_relation_new_auto_governed_controlled(name, width,
            capacity, timestamps, watched, &r, c) == 0);
        CHECK(finish(c, 0) == total && peak == d + m + p && !retired);
        external_out = NULL;
        CHECK(r->descriptor_reserved_bytes == d
            && r->metadata_reserved_bytes == m &&
            r->retained_reserved_bytes == p);
        CHECK(r->descriptor_reservation.identity == &r->descriptor_reservation);
        CHECK(r->metadata_reservation.identity == &r->metadata_reservation);
        CHECK(r->retained_reservation.identity == &r->retained_reservation);
        CHECK(wl_columnar_relation_new_auto_governed(name, width, capacity,
            timestamps, watched, &raw) == 0);
        equal(r, raw);
        CHECK(col_rel_destroy_checked(raw) == 0);
        CHECK(col_rel_destroy_checked(r) == 0 && reserved() == 0);
    }
    release(c);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

/* Explicit protocol-prefix inventory, including charged allocation ordinals.
 * This computes the expected schedule from inputs without running the builder. */
static size_t
regions(const char *name, uint32_t width, uint32_t capacity, bool timestamps,
    uint64_t *out, uint64_t *allocations, size_t *allocation_count)
{
    uint64_t total = 0;
    size_t n = 0, a = 0;
#define R(v) do { uint64_t amount = (v); out[n++] = amount; total += amount; \
} while (0)
#define A(v) do { R(v); allocations[a++] = total; } while (0)
#define BYTES(v) do { uint64_t remaining = (v); while (remaining) { \
                          uint64_t q = remaining > WL_EVALUATION_CONTROL_QUANTUM \
                             ? WL_EVALUATION_CONTROL_QUANTUM : remaining; \
                          R(q); remaining -= q; } } while (0)
#define GEN(i) do { unsigned nd = digits(i); for (unsigned j = 0; j < nd; j++) { \
                        R(3); } R(4u + nd); } while (0)
#define RESERVE() do { R(3); R(1); R(5); R(4); R(1); R(1); } while (0)
    uint64_t name_bytes = strlen(name) + 1;
    for (uint64_t i = 0; i < name_bytes; i++) R(1);
    for (uint32_t i = 0; i < width; i++) {
        GEN(i); R(1);
    }
    RESERVE(); A(1); BYTES(sizeof(col_rel_t));
    for (unsigned i = 0; i < 5;
        i++) R(sizeof(wl_columnar_memory_reservation_t) + 2);
    A(1); BYTES(name_bytes); R(1); R(1); R(9); RESERVE();
    bool payload = width || (timestamps && capacity);
    if (payload) RESERVE();
    if (width) {
        A(1); BYTES(width);
        for (uint32_t i = 0; i < width; i++) A(1);
        A(1); A(1); BYTES(width);
        for (uint32_t i = 0; i < width; i++) {
            GEN(i); A(1); BYTES(4u + digits(i));
        }
    }
    if (timestamps && capacity) {
        A(1); BYTES((uint64_t)capacity * sizeof(col_delta_timestamp_t));
    }
    R(9); A(1); R(3);
    if (width) {
        A(1); BYTES(width);
        for (uint32_t i = 0; i < width; i++) {
            A(12); A(1); R(2); GEN(i); A(1); BYTES(4u + digits(i));
        }
    }
    unsigned k = payload ? 3 : 2;
    for (unsigned i = 0; i < k; i++) {
        R(1); R(1); R(2);
    }
    for (unsigned i = 0; i < k; i++) R(30);
    R(14);
    CHECK(total == cost(name, width, capacity, timestamps));
    *allocation_count = a;
#undef RESERVE
#undef GEN
#undef BYTES
#undef A
#undef R
    return n;
}

static void
test_prefixes(void)
{
    const char *name = "prefix";
    uint64_t work[4096], allocations[32], prefix = 0;
    size_t allocation_count;
    size_t n = regions(name, 3, 2, true, work, allocations, &allocation_count);
    CHECK(allocation_count == 23);
    watched = governor();
    wl_evaluation_control_t *c = control(0);
    CHECK(finish(c, 0) == 0);
    for (size_t i = 0; i < n; i++) {
        col_rel_t *r = (col_rel_t *)(uintptr_t)1;
        retry(c, prefix + work[i] - 1);
        if (prefix + work[i] > 1) {
            CHECK(wl_columnar_relation_new_auto_governed_controlled(name, 3,
                2, true, watched, &r, c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(r == NULL && finish(c,
                WL_EVALUATION_CONTROL_WORK_LIMIT) == prefix);
            CHECK(reserved() == 0);
        } else CHECK(finish(c, 0) == 0);
        prefix += work[i]; retry(c, 0); cancel_at = prefix;
        CHECK(wl_columnar_relation_new_auto_governed_controlled(name, 3,
            2, true, watched, &r, c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(r == NULL && finish(c,
            WL_EVALUATION_CONTROL_CANCELLED) == prefix);
        CHECK(reserved() == 0);
    }
    for (size_t i = 0; i < allocation_count; i++) {
        col_rel_t *r = NULL;
        retry(c, 0);
        wl_columnar_relation_test_constructor_fail_allocation = (uint32_t)i + 1;
        CHECK(wl_columnar_relation_new_auto_governed_controlled(name, 3, 2,
            true, watched, &r, c) == ENOMEM);
        CHECK(r == NULL && finish(c, ENOMEM) == allocations[i]);
        CHECK(!wl_columnar_relation_test_constructor_fail_allocation);
        CHECK(reserved() == 0 && retired == 1);
    }
    for (unsigned i = 1; i <= 3; i++) {
        col_rel_t *r = NULL;
        retry(c, 0); commit_stop = i;
        CHECK(wl_columnar_relation_new_auto_governed_controlled(name, 3, 2,
            true, watched, &r, c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(r == NULL && finish(c, WL_EVALUATION_CONTROL_CANCELLED) > 0);
        CHECK(reserved() == 0 && retired == 1 && retired_credit > 0);
    }
    release(c); wl_columnar_memory_governor_ref_release(watched);
    watched = NULL;
}

static void
test_governor(void)
{
    watched = governor();
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(watched);
    wl_columnar_memory_reservation_init(&peer);
    CHECK(wl_columnar_memory_reserve(g, 17, &peer));
    CHECK(wl_columnar_memory_commit(&peer, &peer));
    uint64_t d, m, p;
    credits("budget", 3, 4, true, &d, &m, &p);
    uint64_t limits[] = {17 + d - 1, 17 + d + m - 1,
                         17 + d + m + p - 1};
    uint64_t total = cost("budget", 3, 4, true);
    wl_evaluation_control_t *c = control(0);
    CHECK(finish(c, 0) == 0);
    for (unsigned i = 0; i < 3; i++) {
        retry(c, 0);
        atomic_store_explicit(&g->usable_bytes, limits[i],
            memory_order_relaxed);
        col_rel_t *r = (col_rel_t *)(uintptr_t)1;
        CHECK(wl_columnar_relation_new_auto_governed_controlled("budget", 3,
            4, true, watched, &r, c) == ENOSPC && r == NULL);
        CHECK(finish(c, ENOSPC) > 0 && reserved() == 17 && retired == 1);
        CHECK(retired_credit == 17 + (i ? d : 0) + (i == 2 ? m : 0));
    }
    col_rel_t *r = NULL;
    retry(c, total);
    atomic_store_explicit(&g->usable_bytes, 17 + d + m + p,
        memory_order_relaxed);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("budget", 3, 4,
        true, watched, &r, c) == 0);
    CHECK(finish(c, 0) == total && peak == 17 + d + m + p);
    CHECK(col_rel_destroy_checked(r) == 0 && reserved() == 17);
    for (unsigned i = 0; i < 3; i++) {
        retry(c, 0); commit_stop = i + 1;
        CHECK(wl_columnar_relation_new_auto_governed_controlled("budget", 3, 4,
            true, watched, &r, c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(r == NULL && finish(c, WL_EVALUATION_CONTROL_CANCELLED) > 0);
        CHECK(retired_credit == 17 + d + m + p && reserved() == 17);
    }
    atomic_store_explicit(&g->usable_bytes, WL_COLUMNAR_MEMORY_MIN_BUDGET,
        memory_order_relaxed);
    const unsigned phases[] = {WL_COLUMNAR_MEMORY_GOVERNOR_VALIDATED,
                               WL_COLUMNAR_MEMORY_GOVERNOR_CLAIMED,
                               WL_COLUMNAR_MEMORY_GOVERNOR_SNAPSHOT,
                               WL_COLUMNAR_MEMORY_GOVERNOR_PREPARED,
                               WL_COLUMNAR_MEMORY_GOVERNOR_ADMITTED,
                               WL_COLUMNAR_MEMORY_GOVERNOR_RESERVED,
                               WL_COLUMNAR_MEMORY_GOVERNOR_COMMITTING,
                               WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF};
    wl_columnar_memory_governor_test_checkpoint = governor_fault;
    for (unsigned pos = 0; pos < 2; pos++) {
        positive_error = pos != 0;
        for (size_t i = 0; i < sizeof(phases) / sizeof(phases[0]); i++) {
            retry(c, 0); fault_phase = phases[i]; fault_hits = 0;
            int expected =
                positive_error ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(wl_columnar_relation_new_auto_governed_controlled("budget",
                3, 4, true, watched, &r, c) == expected && r == NULL);
            CHECK(finish(c,
                expected) > 0 && fault_hits == 1 && reserved() == 17);
        }
    }
    wl_columnar_memory_governor_test_checkpoint = NULL;
    CHECK(wl_columnar_memory_release(&peer) && reserved() == 0);
    /* Real shared-total race: peer's own original token survives our cleanup. */
    wl_columnar_memory_reservation_init(&peer); peer_changed = false;
    wl_columnar_memory_governor_test_before_total_cas = peer_race;
    retry(c, total + 1); commit_stop = 3;
    CHECK(wl_columnar_relation_new_auto_governed_controlled("budget", 3, 4,
        true, watched, &r, c) == WL_EVALUATION_CONTROL_CANCELLED && r == NULL);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == total - 104 + 1);
    CHECK(peer_changed && reserved() == 17 && retired_credit == 17 + d + m + p);
    wl_columnar_memory_governor_test_before_total_cas = NULL;
    CHECK(wl_columnar_memory_release(&peer) && reserved() == 0);
    /* Advisory retains the raw numeric limit: finite denial, then unbounded
     * advisory admission. Real peer credit survives both paths. */
    g->mode = WL_COLUMNAR_MEMORY_MODE_ADVISORY;
    atomic_store_explicit(&g->usable_bytes, WL_COLUMNAR_MEMORY_MIN_BUDGET,
        memory_order_relaxed);
    wl_columnar_memory_reservation_init(&peer);
    CHECK(wl_columnar_memory_reserve(g, WL_COLUMNAR_MEMORY_MIN_BUDGET - 1,
        &peer));
    CHECK(wl_columnar_memory_commit(&peer, &peer));
    retry(c, total);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("budget", 3, 4,
        true, watched, &r, c) == ENOSPC && r == NULL);
    CHECK(finish(c,
        ENOSPC) > 0 && reserved() == WL_COLUMNAR_MEMORY_MIN_BUDGET - 1);
    atomic_store_explicit(&g->usable_bytes, UINT64_MAX, memory_order_relaxed);
    retry(c, total);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("budget", 3, 4,
        true, watched, &r, c) == 0);
    CHECK(finish(c, 0) == total && reserved()
        == WL_COLUMNAR_MEMORY_MIN_BUDGET - 1 + d + m + p);
    CHECK(col_rel_destroy_checked(r) == 0);
    CHECK(wl_columnar_memory_release(&peer) && reserved() == 0);
    wl_columnar_memory_reservation_init(&peer);
    CHECK(wl_columnar_memory_reserve(g, UINT64_MAX, &peer));
    CHECK(wl_columnar_memory_commit(&peer, &peer));
    retry(c, 0);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("budget", 3, 4,
        true, watched, &r, c) == EOVERFLOW && r == NULL);
    CHECK(finish(c, EOVERFLOW) > 0 && reserved() == UINT64_MAX);
    CHECK(wl_columnar_memory_release(&peer) && reserved() == 0);
    release(c); wl_columnar_memory_governor_ref_release(watched);
    watched = NULL;
}

static void
test_domain_and_identity(void)
{
    watched = governor();
    wl_evaluation_control_t *c = control(0);
    col_rel_t *r = (col_rel_t *)(uintptr_t)1;
    CHECK(wl_columnar_relation_new_auto_governed_controlled(NULL, 1, 1,
        false, watched, &r, c) == EINVAL && r == NULL);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("x", 1, 1,
        false, NULL, &r, c) == EINVAL && r == NULL);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("x", 1, 1,
        false, watched, NULL, c) == EINVAL);
    CHECK(wl_columnar_relation_new_auto_governed_controlled(NULL, 1, 1,
        false, watched, &r, NULL) == EINVAL && r == NULL);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("x", 1, 1,
        false, NULL, &r, NULL) == EINVAL && r == NULL);
    CHECK(finish(c, EINVAL) == 0);
    retry(c, 0); wl_evaluation_control_request_cancel(c);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("x", 1, 1,
        false, watched, &r, c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 0 && r == NULL);
    retry(c, 0); identity_races = 3;
    wl_columnar_relation_test_before_identity_cas = identity_race;
    CHECK(wl_columnar_relation_new_auto_governed_controlled("x", 1, 1,
        false, watched, &r, c) == 0);
    CHECK(finish(c, 0) == cost("x", 1, 1, false) + 3 && !identity_races);
    wl_columnar_relation_test_before_identity_cas = NULL;
    uint64_t previous = r->relation_identity;
    CHECK(col_rel_destroy_checked(r) == 0);
    retry(c, 0); commit_stop = 1;
    CHECK(wl_columnar_relation_new_auto_governed_controlled("x", 1, 1,
        false, watched, &r, c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(r == NULL && finish(c, WL_EVALUATION_CONTROL_CANCELLED) > 0);
    retry(c, 0);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("x", 1, 1,
        false, watched, &r, c) == 0);
    CHECK(finish(c, 0) == cost("x", 1, 1, false));
    CHECK(r->relation_identity > previous + 1);
    CHECK(col_rel_destroy_checked(r) == 0 && reserved() == 0);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("x", 0, 0,
        false, watched, &r, NULL) == 0);
    CHECK(col_rel_destroy_checked(r) == 0);
    release(c); wl_columnar_memory_governor_ref_release(watched);
    watched = NULL;
    /* All fixtures have been destroyed before selecting terminal test state. */
    watched = governor(); c = control(0);
    CHECK(col_rel_test_set_next_identity(UINT64_MAX) == 0);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("x", 0, 0,
        false, watched, &r, c) == EOVERFLOW && r == NULL);
    CHECK(finish(c, EOVERFLOW) > 0 && reserved() == 0);
    CHECK(col_rel_test_set_next_identity(1) == 0);
    retry(c, 0);
    CHECK(col_rel_test_set_next_identity(0) == 0);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("x", 0, 0,
        false, watched, &r, c) == EOVERFLOW && r == NULL);
    CHECK(finish(c, EOVERFLOW) > 0 && reserved() == 0);
    CHECK(col_rel_test_set_next_identity(1) == 0);
    release(c); wl_columnar_memory_governor_ref_release(watched);
    watched = NULL;
}

int
main(void)
{
    wl_columnar_relation_test_work_checkpoint = checkpoint;
    wl_columnar_memory_governor_test_work_checkpoint = checkpoint;
    wl_columnar_relation_test_constructor_retired = physically_retired;
    wl_columnar_relation_test_constructor_committed = committed;
    for (unsigned ts = 0; ts < 2; ts++) {
        test_success("", 0, 0, ts != 0);
        test_success("fresh", 0, 3, ts != 0);
        test_success("fresh", 1, 0, ts != 0);
        test_success("fresh", 3, 7, ts != 0);
        test_success("fresh", 300, 1, ts != 0);
    }
    char long_name[601]; memset(long_name, 'x', sizeof(long_name) - 1);
    long_name[sizeof(long_name) - 1] = '\0';
    test_success(long_name, 1, 300, true);
    test_prefixes(); test_governor(); test_domain_and_identity();
    puts("evaluation_relation_creation: OK");
    return 0;
}
