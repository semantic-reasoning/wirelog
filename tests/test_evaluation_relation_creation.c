/* #1821: real governed fresh construction, before engine integration. */
#include "../wirelog/columnar/internal.h"
#include <stdio.h>
#ifndef _WIN32
#include <signal.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

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
static col_rel_t *source_watch;
static uint64_t drained_at;
static bool drained_seen;
static col_rel_t *ledger_peer;
static bool ledger_peer_changed;
static wl_columnar_source_access_reader_t gate_peers[2];
static wl_columnar_source_access_gate_t *race_gates[2];
static unsigned gate_rounds[2];

static uint64_t
reserved(void)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(watched));
}

static void
checkpoint(wl_evaluation_control_t *control, uint64_t work)
{
    if (ledger_peer && !ledger_peer_changed) {
        bool denied = false;
        CHECK(col_rel_reserve_capacity_admitted(ledger_peer,
            COL_REL_INIT_CAP * 2, &denied) == 0 && !denied);
        ledger_peer_changed = true;
    }
    if (external_out) CHECK(*external_out == NULL);
    CHECK(work <= WL_EVALUATION_CONTROL_QUANTUM);
    seen += work;
    if (source_watch && drained_at && seen == drained_at) {
        col_rel_t *root = NULL;
        CHECK(col_rel_storage_owner_resolve(source_watch, &root) == 0);
        wl_columnar_source_access_writer_t a = { 0 }, b = { 0 };
        CHECK(wl_columnar_source_access_writer_acquire(
                &source_watch->descriptor_access, &a) == 0);
        CHECK(wl_columnar_source_access_writer_acquire(&root->source_access,
            &b) == 0);
        CHECK(wl_columnar_source_access_writer_release(&b) == 0);
        CHECK(wl_columnar_source_access_writer_release(&a) == 0);
        drained_seen = true;
    }
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

static void
reader_race(wl_evaluation_control_t *control,
    wl_columnar_source_access_gate_t *gate, uint64_t observed)
{
    (void)control; (void)observed;
    for (unsigned i = 0; i < 2; i++) {
        if (gate != race_gates[i] || !gate_rounds[i]) continue;
        if (gate_peers[i].owner)
            CHECK(wl_columnar_source_access_reader_release(&gate_peers[i]) ==
                0);
        else
            CHECK(wl_columnar_source_access_reader_acquire(gate,
                &gate_peers[i]) == 0);
        gate_rounds[i]--;
    }
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

/* Owned partial metadata fixtures are assembled before governor attachment.
 * NULL name slots and absent name table have matching empty Arrow names. */
static col_rel_t *
new_like_source(uint32_t width, bool typed, bool timestamps,
    unsigned names_mode)
{
    char long_name[301];
    memset(long_name, 'n', sizeof(long_name) - 1);
    long_name[sizeof(long_name) - 1] = '\0';
    const char **names = width ? calloc(width, sizeof(*names)) : NULL;
    CHECK(!width || names != NULL);
    for (uint32_t c = 0; c < width; c++)
        names[c] = c % 3 == 0 ? long_name : c % 3 == 1 ? "" : NULL;
    col_rel_t *r = NULL;
    CHECK(col_rel_alloc(&r, "source") == 0);
    CHECK(col_rel_set_schema(r, width, names) == 0);
    free(names);
    if (typed) {
        wirelog_column_type_t *types = width ? malloc(width *
                sizeof(*types)) : NULL;
        CHECK(!width || types != NULL);
        for (uint32_t c = 0; c < width; c++)
            types[c] = (wirelog_column_type_t)(c % 7);
        CHECK(col_rel_set_column_types(r, types, width) == 0);
        free(types);
    }
    for (uint32_t c = 0; c < width; c++) {
        if (names_mode == 2 || (names_mode == 1 && c % 3 == 2)) {
            free(r->col_names[c]); r->col_names[c] = NULL;
            CHECK(ArrowSchemaSetName(r->schema.children[c], "") == 0);
        }
    }
    if (names_mode == 2) {
        free(r->col_names); r->col_names = NULL;
    }
    if (timestamps) CHECK(col_rel_enable_timestamps(r) == 0);
    r->has_graph_column = width > 0;
    r->graph_col_idx = width ? width - 1 : 99;
    r->declared_ncols = width;
    r->nrows = 2;
    for (uint32_t c = 0; c < width; c++)
        for (uint32_t row = 0; row < 2; row++) r->columns[c][row] = c + row;
    if (r->timestamps) r->timestamps[0].multiplicity = -7;
    if (width) {
        bool denied = false;
        CHECK(col_rel_reserve_capacity_admitted(r, COL_REL_INIT_CAP + 17,
            &denied) == 0 && !denied);
        CHECK(r->capacity != COL_REL_INIT_CAP);
    }
    wl_columnar_memory_governor_ref_t *g = governor();
    CHECK(col_rel_attach_memory_governor(r, g) == 0);
    wl_columnar_memory_governor_ref_release(g);
    return r;
}

static uint64_t
new_like_name_length(const col_rel_t *src, uint32_t col)
{
    return src->col_names && src->col_names[col]
        ? strlen(src->col_names[col]) + 1 : 4u + digits(col);
}

static uint64_t
new_like_cost(const char *name, const col_rel_t *src, bool governed,
    bool preserve_timestamps)
{
    uint32_t w = src->ncols;
    bool ts = preserve_timestamps && src->timestamps;
    unsigned k = governed ? (w || ts ? 3 : 2) : 0;
    uint64_t total = 55 + 1 + 2 * (strlen(name) + 1) + sizeof(col_rel_t)
        + 5 * (sizeof(wl_columnar_memory_reservation_t) + 2)
        + 2 + 8 + governed + 9 + 3 + 18 + k * (15 + 4 + 30);
    total += w ? 7 + 5 * (uint64_t)w - !governed : 3;
    if (ts) total += 1 + (uint64_t)COL_REL_INIT_CAP *
            sizeof(col_delta_timestamp_t);
    if (src->column_types && w) total += 1 + 2 * (uint64_t)w;
    for (uint32_t c = 0; c < w; c++) {
        uint64_t length = new_like_name_length(src, c);
        uint64_t name_work = src->col_names && src->col_names[c]
            ? length : 3 * digits(c) + length;
        total += 3 + 3 * name_work + 2 * length + 17;
    }
    return total;
}

static void
new_like_credits(const char *name, const col_rel_t *src, bool ts,
    uint64_t *d, uint64_t *m, uint64_t *p)
{
    credits(name, src->ncols, COL_REL_INIT_CAP, ts, d, m, p);
    *m = 3;
    for (uint32_t c = 0; c < src->ncols; c++)
        *m += sizeof(char *) + sizeof(struct ArrowSchema *)
            + sizeof(struct ArrowSchema) + 2 + 2 * new_like_name_length(src, c);
    if (src->column_types) *m += src->ncols * sizeof(*src->column_types);
}

static void
new_like_equal(col_rel_t *r, col_rel_t *raw, const col_rel_t *src)
{
    CHECK(r && raw && !r->nrows && !raw->nrows && r->ncols == raw->ncols);
    CHECK(r->capacity == COL_REL_INIT_CAP && r->capacity == raw->capacity);
    CHECK(r->view_generation == raw->view_generation
        && r->storage_generation == raw->storage_generation);
    CHECK(r->storage_owner == r &&
        r->storage_owner_generation == r->storage_generation);
    CHECK(r->has_graph_column == raw->has_graph_column
        && r->graph_col_idx == raw->graph_col_idx
        && r->declared_ncols == raw->declared_ncols);
    CHECK((r->column_types != NULL) == (raw->column_types != NULL));
    CHECK((r->timestamps != NULL) == (raw->timestamps != NULL)
        && r->timestamp_capacity == raw->timestamp_capacity);
    for (uint32_t c = 0; c < src->ncols; c++) {
        CHECK(strcmp(r->col_names[c], raw->col_names[c]) == 0);
        CHECK(strcmp(r->schema.children[c]->name,
            raw->schema.children[c]->name) == 0);
        CHECK(strcmp(r->schema.children[c]->format,
            raw->schema.children[c]->format) == 0);
        if (src->column_types) CHECK(r->column_types[c] ==
                src->column_types[c]);
    }
    if (r->timestamps) {
        unsigned char zero[sizeof(col_delta_timestamp_t)] = { 0 };
        for (uint32_t i = 0; i < r->timestamp_capacity; i++)
            CHECK(memcmp(&r->timestamps[i], zero, sizeof(zero)) == 0);
    }
}

static void
new_like_released(col_rel_t *src)
{
    col_rel_t *root = NULL;
    CHECK(col_rel_storage_owner_resolve(src, &root) == 0);
    wl_columnar_source_access_writer_t a = { 0 }, b = { 0 };
    CHECK(wl_columnar_source_access_writer_acquire(&src->descriptor_access,
        &a) == 0);
    CHECK(wl_columnar_source_access_writer_acquire(&root->source_access,
        &b) == 0);
    CHECK(wl_columnar_source_access_writer_release(&b) == 0);
    CHECK(wl_columnar_source_access_writer_release(&a) == 0);
}

static void
new_like_success(col_rel_t *src, bool governed, bool preserve_timestamps)
{
    watched = governor();
    uint64_t baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(src->memory_governor));
    uint64_t total = new_like_cost("like", src, governed, preserve_timestamps);
    uint64_t d, m, p;
    new_like_credits("like", src, preserve_timestamps && src->timestamps, &d,
        &m, &p);
    col_rel_t *r = NULL, *raw = NULL;
    wl_evaluation_control_t *c = control(total - 1);
    source_watch = src; drained_at = total; drained_seen = false;
    external_out = &r;
    CHECK(wl_columnar_relation_new_like_governed_checked_mode_controlled("like",
        src,
        governed ? watched : NULL, preserve_timestamps, &r,
        c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(r == NULL && finish(c,
        WL_EVALUATION_CONTROL_WORK_LIMIT) == total - 1);
    CHECK(reserved() == 0); new_like_released(src);
    retry(c, total); cancel_at = total;
    CHECK(wl_columnar_relation_new_like_governed_checked_mode_controlled("like",
        src,
        governed ? watched : NULL, preserve_timestamps, &r,
        c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(r == NULL && finish(c,
        WL_EVALUATION_CONTROL_CANCELLED) == total && drained_seen);
    CHECK(reserved() == 0); new_like_released(src);
    for (unsigned repeat = 0; repeat < 2; repeat++) {
        retry(c, total); drained_seen = false;
        CHECK(wl_columnar_relation_new_like_governed_checked_mode_controlled(
                "like", src,
                governed ? watched : NULL, preserve_timestamps, &r, c) == 0);
        CHECK(finish(c, 0) == total && drained_seen
            && peak == (governed ? d + m + p : 0));
        external_out = NULL;
        CHECK(wl_columnar_relation_new_like_governed_checked_mode("like", src,
            governed ? watched : NULL, preserve_timestamps, &raw) == 0);
        new_like_equal(r, raw, src);
        CHECK(r->memory_governor == (governed ? watched : NULL));
        CHECK((r->row_scratch != NULL) == (governed && src->ncols));
        CHECK(r->descriptor_reserved_bytes == (governed ? d : 0)
            && r->metadata_reserved_bytes == (governed ? m : 0)
            && r->retained_reserved_bytes == (governed ? p : 0));
        CHECK(col_rel_destroy_checked(raw) == 0 &&
            col_rel_destroy_checked(r) == 0);
        CHECK(reserved() == 0 && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(src->memory_governor)) ==
            baseline);
        r = NULL; external_out = &r;
    }
    external_out = NULL; source_watch = NULL; drained_at = 0;
    release(c); wl_columnar_memory_governor_ref_release(watched);
    watched = NULL;
}

static size_t
new_like_regions(const char *name, const col_rel_t *src, bool governed,
    bool preserve_timestamps, uint64_t *out, uint64_t *allocations,
    size_t *allocation_count)
{
    uint64_t total = 0;
    size_t n = 0, a = 0;
    uint32_t width = src->ncols;
    bool ts = preserve_timestamps && src->timestamps;
#define R(v) do { uint64_t amount = (v); out[n++] = amount; total += amount; \
} while (0)
#define A(v) do { R(v); allocations[a++] = total; } while (0)
#define BYTES(v) do { uint64_t remaining = (v); while (remaining) { \
                          uint64_t q = remaining > WL_EVALUATION_CONTROL_QUANTUM \
                             ? WL_EVALUATION_CONTROL_QUANTUM : remaining; \
                          R(q); remaining -= q; } } while (0)
#define NAME(i) do { R(1); if (src->col_names && src->col_names[i]) { \
                         for (uint64_t j = 0; j < new_like_name_length(src, i); \
                             j++) R(1); \
                     } else { unsigned nd = digits(i); \
                              for (unsigned j = 0; j < nd; j++) { R(3); \
                              } R(4u + nd); } } while (0)
#define RESERVE() do { R(3); R(1); R(5); R(4); R(1); R(1); } while (0)
    R(1); R(1); R(12); R(1); R(40);
    uint64_t name_bytes = strlen(name) + 1;
    for (uint64_t i = 0; i < name_bytes; i++) R(1);
    for (uint32_t i = 0; i < width; i++) {
        NAME(i); R(1);
    }
    if (governed) RESERVE();
    A(1); BYTES(sizeof(col_rel_t));
    for (unsigned i = 0; i < 5;
        i++) R(sizeof(wl_columnar_memory_reservation_t) + 2);
    A(1); BYTES(name_bytes); R(1); R(1); R(8u + governed);
    if (governed) {
        RESERVE(); if (width || ts) RESERVE();
    }
    if (width) {
        A(1); BYTES(width);
        for (uint32_t i = 0; i < width; i++) A(1);
        if (governed) A(1);
        A(1); BYTES(width);
        for (uint32_t i = 0; i < width; i++) {
            NAME(i); A(1); BYTES(new_like_name_length(src, i));
        }
    }
    if (width && src->column_types) {
        A(1); for (uint32_t i = 0; i < width; i++) R(1);
    }
    if (ts) {
        A(1); BYTES((uint64_t)COL_REL_INIT_CAP * sizeof(col_delta_timestamp_t));
    }
    R(9); A(1); R(3);
    if (width) {
        A(1); BYTES(width);
        for (uint32_t i = 0; i < width; i++) {
            A(12); A(1); if (src->column_types) R(1); R(2);
            NAME(i); A(1); BYTES(new_like_name_length(src, i));
        }
    }
    unsigned k = governed ? (width || ts ? 3 : 2) : 0;
    for (unsigned i = 0; i < k; i++) {
        R(1); R(1); R(2);
    }
    for (unsigned i = 0; i < k; i++) R(30);
    R(18); R(1);
    CHECK(total == new_like_cost(name, src, governed, preserve_timestamps));
    *allocation_count = a;
#undef RESERVE
#undef NAME
#undef BYTES
#undef A
#undef R
    return n;
}

static void
new_like_prefixes(bool governed)
{
    col_rel_t *src = new_like_source(3, true, true, 1);
    watched = governor();
    uint64_t baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(src->memory_governor));
    uint64_t work[4096], allocations[32], prefix = 0;
    size_t allocation_count;
    size_t n = new_like_regions("prefix", src, governed, true,
            work, allocations, &allocation_count);
    CHECK(allocation_count == (governed ? 24u : 23u));
    wl_evaluation_control_t *c = control(0);
    CHECK(finish(c, 0) == 0);
    for (size_t i = 0; i < n; i++) {
        col_rel_t *r = (col_rel_t *)(uintptr_t)1;
        retry(c, prefix + work[i] - 1);
        if (prefix + work[i] > 1) {
            CHECK(
                wl_columnar_relation_new_like_governed_checked_mode_controlled(
                    "prefix",
                    src, governed ? watched : NULL, true, &r,
                    c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(r == NULL && finish(c,
                WL_EVALUATION_CONTROL_WORK_LIMIT) == prefix);
            CHECK(reserved() == 0); new_like_released(src);
        } else CHECK(finish(c, 0) == 0);
        prefix += work[i]; retry(c, 0); cancel_at = prefix;
        CHECK(wl_columnar_relation_new_like_governed_checked_mode_controlled(
                "prefix",
                src, governed ? watched : NULL, true, &r,
                c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(r == NULL && finish(c,
            WL_EVALUATION_CONTROL_CANCELLED) == prefix);
        CHECK(reserved() == 0); new_like_released(src);
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(src->memory_governor)) ==
            baseline);
    }
    for (size_t i = 0; i < allocation_count; i++) {
        col_rel_t *r = NULL;
        retry(c, 0);
        wl_columnar_relation_test_constructor_fail_allocation = (uint32_t)i + 1;
        CHECK(wl_columnar_relation_new_like_governed_checked_mode_controlled(
                "prefix",
                src, governed ? watched : NULL, true, &r, c) == ENOMEM);
        CHECK(r == NULL && finish(c, ENOMEM) == allocations[i]);
        CHECK(!wl_columnar_relation_test_constructor_fail_allocation &&
            reserved() == 0);
        new_like_released(src);
    }
    if (governed)
        for (unsigned stage = 1; stage <= 3; stage++) {
            col_rel_t *r = NULL;
            retry(c, 0); commit_stop = stage;
            CHECK(
                wl_columnar_relation_new_like_governed_checked_mode_controlled(
                    "prefix",
                    src, watched, true, &r,
                    c) == WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(r == NULL && finish(c, WL_EVALUATION_CONTROL_CANCELLED) > 0);
            CHECK(reserved() == 0 && retired_credit > 0);
            new_like_released(src);
        }
    release(c); CHECK(col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static void
new_like_busy_and_scope(void)
{
    col_rel_t *src = new_like_source(2, true, false, 0);
    watched = governor();
    wl_evaluation_control_t *c = control(0);
    wl_columnar_source_access_writer_t writer = { 0 };
    col_rel_t *r = (col_rel_t *)(uintptr_t)1;
    CHECK(wl_columnar_source_access_writer_acquire(&src->descriptor_access,
        &writer) == 0);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&r, "busy",
        src,
        watched, c) == EBUSY && r == NULL);
    CHECK(finish(c, EBUSY) == 1);
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
    retry(c, 0);
    CHECK(wl_columnar_source_access_writer_acquire(&src->source_access,
        &writer) == 0);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&r, "busy",
        src,
        watched, c) == EBUSY && r == NULL);
    CHECK(finish(c, EBUSY) == 15);
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
    new_like_released(src);
    retry(c, 0);
    col_rel_t *saved_owner = src->storage_owner;
    src->storage_owner = NULL;
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&r, "lazy",
        src,
        watched, c) == ENOTSUP && r == NULL);
    CHECK(finish(c, ENOTSUP) == 2); src->storage_owner = saved_owner;
    new_like_released(src);
    uint64_t generation = src->storage_owner_generation;
    src->storage_owner_generation--;
    retry(c, 0);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&r, "stale",
        src,
        watched, c) == EINVAL && r == NULL);
    CHECK(finish(c, EINVAL) == 55); src->storage_owner_generation = generation;
    new_like_released(src);
    uint32_t graph_idx = src->graph_col_idx;
    src->graph_col_idx = src->ncols;
    retry(c, 0);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&r, "graph",
        src,
        watched, c) == EINVAL && r == NULL);
    CHECK(finish(c, EINVAL) == 55); src->graph_col_idx = graph_idx;
    new_like_released(src);
    src->compound_arity_len = 1;
    retry(c, 0);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&r, "map",
        src,
        watched, c) == EINVAL && r == NULL);
    CHECK(finish(c, EINVAL) == 55); src->compound_arity_len = 0;
    src->compound_count = 1;
    retry(c, 0);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&r,
        "scalar", src,
        watched, c) == EINVAL && r == NULL);
    CHECK(finish(c, EINVAL) == 55); src->compound_count = 0;
    src->compound_kind = (wirelog_compound_kind_t)99;
    retry(c, 0);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&r, "kind",
        src,
        watched, c) == EINVAL && r == NULL);
    CHECK(finish(c, EINVAL) == 55);
    src->compound_kind = WIRELOG_COMPOUND_KIND_NONE;
    new_like_released(src);
    /* Real compound producer; refuse without reading its variable map. */
    src->nrows = 0;
    col_rel_logical_col_t logical = {WIRELOG_COMPOUND_KIND_INLINE, 2u, 1u};
    CHECK(col_rel_apply_compound_schema(src, &logical, 1) == 0);
    retry(c, 0);
    wl_columnar_relation_test_constructor_fail_allocation = 1;
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&r,
        "compound", src,
        watched, c) == ENOTSUP && r == NULL);
    CHECK(finish(c, ENOTSUP) == 55
        && wl_columnar_relation_test_constructor_fail_allocation == 1);
    wl_columnar_relation_test_constructor_fail_allocation = 0;
    new_like_released(src);
    retry(c, 0);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&r, NULL,
        src,
        watched, c) == EINVAL && r == NULL);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&r, "bad",
        NULL,
        watched, c) == EINVAL && r == NULL);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(NULL, "bad",
        src,
        watched, c) == EINVAL);
    CHECK(finish(c, EINVAL) == 0);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&r, "raw",
        src,
        watched, NULL) == 0 && r && r->compound_arity_len == 1);
    CHECK(col_rel_destroy_checked(r) == 0);
    release(c); CHECK(col_rel_destroy_checked(src) == 0 && reserved() == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static void
new_like_sources(void)
{
    /* Owned ungoverned metadata import with a type buffer at width0. The raw
     * setter touches view even though the empty clone has no type allocation. */
    col_rel_t *nullary = col_rel_new_auto("nullary import", 0);
    CHECK(nullary != NULL);
    nullary->column_types = malloc(sizeof(*nullary->column_types));
    CHECK(nullary->column_types != NULL);
    nullary->column_types[0] = WIRELOG_TYPE_INT64;
    new_like_success(nullary, true, false);
    new_like_success(nullary, false, false);
    CHECK(col_rel_destroy_checked(nullary) == 0);
    col_rel_t *src = new_like_source(7, true, true, 1);
    watched = governor();
    wl_evaluation_control_t *c = control(new_like_cost("default", src, true,
            true));
    col_rel_t *out = NULL, *raw = NULL;
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&out,
        "default", src, watched, c) == 0);
    CHECK(finish(c, 0) == new_like_cost("default", src, true, true));
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&raw,
        "default", src, watched, NULL) == 0);
    new_like_equal(out, raw, src);
    CHECK(col_rel_destroy_checked(out) == 0 &&
        col_rel_destroy_checked(raw) == 0);
    release(c); wl_columnar_memory_governor_ref_release(watched);
    watched = NULL;
    col_rel_t *alias = col_rel_new_like("alias", src);
    CHECK(alias && col_rel_install_shared_view(alias, src) == 0);
    CHECK(alias->storage_owner == src && alias->col_shared);
    new_like_success(alias, true, true); new_like_success(alias, false, false);
    CHECK(col_rel_destroy_checked(alias) == 0);
    delta_pool_t *pool = delta_pool_create(2, sizeof(col_rel_t), 65536);
    CHECK(pool != NULL);
    col_rel_t *pooled = col_rel_pool_new_like(pool, "pool", src);
    CHECK(pooled && pooled->pool_owned);
    new_like_success(pooled, true, true);
    new_like_success(pooled, false, false);
    CHECK(col_rel_destroy_checked(pooled) == 0); delta_pool_destroy(pool);
    wl_arena_t *arena = wl_arena_create(65536);
    pool = delta_pool_create(1, sizeof(col_rel_t), 32);
    CHECK(pool && arena);
    pooled = col_rel_pool_new_auto(pool, arena, "arena", 1);
    CHECK(pooled && pooled->pool_owned && pooled->arena_owned);
    new_like_success(pooled, true, true);
    new_like_success(pooled, false, false);
    CHECK(col_rel_destroy_checked(pooled) == 0); delta_pool_destroy(pool);
    wl_arena_free(arena);
    CHECK(col_rel_destroy_checked(src) == 0);
}

static void
new_like_reader_races(void)
{
    col_rel_t *src = new_like_source(1, true, true, 0);
    watched = governor();
    race_gates[0] = &src->descriptor_access;
    race_gates[1] = &src->source_access;
    gate_rounds[0] = gate_rounds[1] = 3;
    uint64_t total = new_like_cost("race", src, true, true) + 6;
    wl_evaluation_control_t *c = control(total);
    wl_columnar_relation_test_before_reader_cas = reader_race;
    col_rel_t *out = NULL;
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&out,
        "race", src, watched, c) == 0);
    CHECK(finish(c, 0) == total && !gate_rounds[0] && !gate_rounds[1]);
    wl_columnar_relation_test_before_reader_cas = NULL;
    for (unsigned i = 0; i < 2; i++) {
        CHECK(gate_peers[i].owner == race_gates[i]);
        CHECK(wl_columnar_source_access_reader_release(&gate_peers[i]) == 0);
    }
    new_like_released(src);
    CHECK(col_rel_destroy_checked(out) == 0 &&
        col_rel_destroy_checked(src) == 0);
    CHECK(reserved() == 0);
    release(c); wl_columnar_memory_governor_ref_release(watched);
    watched = NULL;
}

static void
new_like_ledger_and_governor(void)
{
    for (unsigned success = 0; success < 2; success++) {
        col_rel_t *src = new_like_source(3, true, true, 1);
        wl_mem_ledger_t ledger; wl_mem_ledger_init(&ledger, 0);
        col_rel_t *peer_rel = col_rel_new_auto("ledger peer", 1);
        CHECK(peer_rel != NULL);
        src->mem_ledger = peer_rel->mem_ledger = &ledger;
        col_rel_ledger_reconcile(src, 0); col_rel_ledger_reconcile(peer_rel, 0);
        uint64_t before = atomic_load_explicit(&ledger.current_bytes,
                memory_order_relaxed);
        watched = governor();
        wl_columnar_memory_reservation_init(&peer);
        CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(
                watched), 17, &peer));
        CHECK(wl_columnar_memory_commit(&peer, &peer));
        uint64_t total = new_like_cost("ledger", src, true, true);
        wl_evaluation_control_t *c = control(total);
        ledger_peer = peer_rel; ledger_peer_changed = false;
        if (!success) commit_stop = 3;
        col_rel_t *out = NULL;
        int expected = success ? 0 : WL_EVALUATION_CONTROL_CANCELLED;
        CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&out,
            "ledger", src, watched, c) == expected);
        CHECK(finish(c, expected) == total - (success ? 0 : 109));
        ledger_peer = NULL;
        CHECK(ledger_peer_changed && atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed) ==
            before + COL_REL_INIT_CAP * sizeof(int64_t));
        if (out) {
            CHECK(!out->mem_ledger && col_rel_destroy_checked(out) == 0);
        }
        CHECK(reserved() == 17 && wl_columnar_memory_release(&peer));
        CHECK(col_rel_destroy_checked(src) == 0 &&
            col_rel_destroy_checked(peer_rel) == 0);
        CHECK(atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed) == 0);
        release(c); wl_columnar_memory_governor_ref_release(watched);
        watched = NULL;
    }
}

static void
new_like_budget(void)
{
    col_rel_t *src = new_like_source(3, true, true, 1);
    uint64_t source_baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(src->memory_governor));
    watched = governor();
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(watched);
    wl_columnar_memory_reservation_init(&peer);
    CHECK(wl_columnar_memory_reserve(g, 17, &peer));
    CHECK(wl_columnar_memory_commit(&peer, &peer));
    uint64_t d, m, p;
    new_like_credits("budget", src, true, &d, &m, &p);
    uint64_t limits[] = {17 + d - 1, 17 + d + m - 1, 17 + d + m + p - 1};
    wl_evaluation_control_t *c = control(0);
    CHECK(finish(c, 0) == 0);
    for (unsigned i = 0; i < 3; i++) {
        retry(c, 0);
        atomic_store_explicit(&g->usable_bytes, limits[i],
            memory_order_relaxed);
        col_rel_t *out = (col_rel_t *)(uintptr_t)1;
        CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&out,
            "budget", src, watched, c) == ENOSPC && out == NULL);
        CHECK(finish(c, ENOSPC) > 0 && reserved() == 17);
        CHECK(retired_credit == 17 + (i ? d : 0) + (i == 2 ? m : 0));
        new_like_released(src);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                src->memory_governor)) == source_baseline &&
            !src->memory_budget_denial_pending);
    }
    uint64_t total = new_like_cost("budget", src, true, true);
    retry(c, total);
    atomic_store_explicit(&g->usable_bytes, 17 + d + m + p,
        memory_order_relaxed);
    col_rel_t *out = NULL;
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&out,
        "budget", src, watched, c) == 0);
    CHECK(finish(c, 0) == total && peak == 17 + d + m + p);
    CHECK(col_rel_destroy_checked(out) == 0 && reserved() == 17);
    CHECK(wl_columnar_memory_release(&peer) && reserved() == 0);
    release(c); CHECK(col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

#ifndef _WIN32
static void
corrupt_release(const col_rel_t *src)
{
    /* Child process only: force actual mandatory release validation to fail. */
    atomic_store_explicit(&((col_rel_t *)src)->source_access.state,
        WL_COLUMNAR_SOURCE_ACCESS_WRITER, memory_order_relaxed);
}

static void
new_like_release_fail_stop(void)
{
    col_rel_t *src = new_like_source(1, false, false, 0);
    pid_t child = fork();
    CHECK(child >= 0);
    if (child == 0) {
        struct rlimit core_limit = { 0, 0 };
        (void)setrlimit(RLIMIT_CORE, &core_limit); fclose(stderr);
        watched = governor();
        wl_evaluation_control_t *c = control(0);
        wl_columnar_relation_test_new_like_before_release = corrupt_release;
        col_rel_t *r = NULL;
        (void)wl_columnar_relation_new_like_governed_checked_controlled(&r,
            "death", src, watched, c);
        _exit(0);
    }
    int status = 0;
    CHECK(waitpid(child, &status, 0) == child && WIFSIGNALED(status)
        && WTERMSIG(status) == SIGABRT);
    new_like_released(src); CHECK(col_rel_destroy_checked(src) == 0);
}
#endif

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
    for (unsigned governed = 0; governed < 2; governed++)
        for (unsigned typed = 0; typed < 2; typed++)
            for (unsigned ts = 0; ts < 2; ts++) {
                const uint32_t widths[] = {0, 1, 7, 300};
                for (unsigned i = 0; i < 4; i++) {
                    col_rel_t *src = new_like_source(widths[i], typed != 0,
                            ts != 0, i % 3);
                    new_like_success(src, governed != 0, true);
                    if (ts) new_like_success(src, governed != 0, false);
                    CHECK(col_rel_destroy_checked(src) == 0);
                }
            }
    new_like_prefixes(false); new_like_prefixes(true);
    new_like_busy_and_scope(); new_like_sources();
    new_like_reader_races();
    new_like_ledger_and_governor();
    new_like_budget();
#ifndef _WIN32
    new_like_release_fail_stop();
#endif
    puts("evaluation_relation_creation: OK");
    return 0;
}
