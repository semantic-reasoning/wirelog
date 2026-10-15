/* #1821: real governed fresh construction, before engine integration. */
#include "../wirelog/columnar/internal.h"
#include <stdio.h>
#include <math.h>
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

static int
attempt_new_like(col_rel_t **out, col_rel_t *src, bool governed, bool ts,
    wl_evaluation_control_t *c)
{
    return
        wl_columnar_relation_new_like_governed_checked_mode_attempt_controlled(
        "attempt clone", src, governed ? watched : NULL, ts, out, c,
        wl_evaluation_control_active_attempt_id(c));
}

static void
attempt_new_like_success(col_rel_t *src, bool governed, bool ts)
{
    watched = governor();
    uint64_t baseline = src->memory_governor
        ? wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(src->memory_governor)) : 0;
    uint64_t d, m, p;
    new_like_credits("attempt clone", src, ts && src->timestamps, &d, &m, &p);
    wl_evaluation_control_t *c = control(0);
    col_rel_t *out = NULL, *raw = NULL;
    external_out = &out;
    CHECK(attempt_new_like(&out, src, governed, ts, c) == 0);
    external_out = NULL;
    CHECK(finish(c, 0) > 0 && max_chunk <= WL_EVALUATION_CONTROL_QUANTUM);
    CHECK(out->memory_governor == (governed ? watched : NULL));
    CHECK(reserved() == (governed ? d + m + p : 0));
    CHECK(wl_columnar_memory_governor_ref_is_sole(watched) == !governed);
    CHECK(wl_columnar_relation_new_like_governed_checked_mode("attempt clone",
        src, NULL, ts, &raw) == 0);
    new_like_equal(out, raw, src);
    new_like_released(src);
    if (src->memory_governor)
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(src->memory_governor)) ==
            baseline);
    CHECK(col_rel_destroy_checked(raw) == 0);
    CHECK(col_rel_destroy_checked(out) == 0 && reserved() == 0);
    CHECK(wl_columnar_memory_governor_ref_is_sole(watched));
    release(c); wl_columnar_memory_governor_ref_release(watched);
    watched = NULL;
}

static void
attempt_new_like_domains_and_prefixes(void)
{
    col_rel_t *src = new_like_source(1, false, false, 0);
    watched = governor();
    wl_evaluation_control_t *c = control(0);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    col_rel_t *out = src;
    /* out holds a borrowed test sentinel, never an owned result. */
    CHECK(
        wl_columnar_relation_new_like_governed_checked_mode_attempt_controlled(
            "x", src, watched, false, &out, c, id + 1u) == EINVAL && !out);
    CHECK(finish(c, EINVAL) == 0); release(c);
    const uint64_t prefixes[] = {128, 256, 352, 496, 624, 720, 864, 1024, 1216};
    for (unsigned i = 0; i < sizeof(prefixes) / sizeof(prefixes[0]); i++) {
        for (unsigned below = 0; below < 2; below++) {
            c = control(prefixes[i] - below);
            out = src;
            CHECK(attempt_new_like(&out, src, true, false, c)
                == WL_EVALUATION_CONTROL_WORK_LIMIT && !out);
            uint64_t expected = below ? (i ? prefixes[i - 1] : 0) : prefixes[i];
            CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == expected);
            CHECK(reserved() == 0 &&
                wl_columnar_memory_governor_ref_is_sole(watched));
            new_like_released(src); release(c);
        }
    }
    c = control(0); out = src;
    CHECK(
        wl_columnar_relation_new_like_governed_checked_mode_attempt_controlled(
            NULL, src, watched, false, &out, c,
            wl_evaluation_control_active_attempt_id(c)) == EINVAL && !out);
    CHECK(finish(c, EINVAL) == 128); release(c);
    /* NULL control ignores ID and retains exact raw delegation. */
    CHECK(
        wl_columnar_relation_new_like_governed_checked_mode_attempt_controlled(
            "raw", src, NULL, false, &out, NULL, UINT64_MAX) == 0);
    CHECK(col_rel_destroy_checked(out) == 0);
    CHECK(col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static void
attempt_new_like_allocations(void)
{
    col_rel_t *src = new_like_source(17, true, true, 1);
    watched = governor();
    bool completed = false;
    for (long ordinal = 1; ordinal < 128; ordinal++) {
        wl_evaluation_control_t *c = control(0);
        col_rel_t *out = NULL;
        wl_columnar_relation_test_constructor_fail_allocation = ordinal;
        int rc = attempt_new_like(&out, src, true, true, c);
        wl_columnar_relation_test_constructor_fail_allocation = 0;
        if (rc == 0) {
            CHECK(out && ordinal > 20);
            CHECK(finish(c, 0) > 0);
            CHECK(col_rel_destroy_checked(out) == 0);
            completed = true;
        } else {
            CHECK(rc == ENOMEM && !out && retired == 1);
            CHECK(finish(c, ENOMEM) > 0);
        }
        CHECK(reserved() == 0 &&
            wl_columnar_memory_governor_ref_is_sole(watched));
        new_like_released(src); release(c);
        if (completed) break;
    }
    CHECK(completed);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
    CHECK(col_rel_destroy_checked(src) == 0);
}

static void
attempt_new_like_fixtures(void)
{
    const uint32_t widths[] = {0, 1, 17};
    for (unsigned typed = 0; typed < 2; typed++)
        for (unsigned ts = 0; ts < 2; ts++)
            for (unsigned i = 0; i < 3; i++) {
                col_rel_t *src = new_like_source(widths[i], typed != 0,
                        ts != 0, i);
                attempt_new_like_success(src, false, false);
                attempt_new_like_success(src, true, ts != 0);
                CHECK(col_rel_destroy_checked(src) == 0);
            }
    col_rel_t *src = new_like_source(17, true, true, 1);
    col_rel_t *alias = col_rel_new_like("attempt alias", src);
    CHECK(alias && col_rel_install_shared_view(alias, src) == 0);
    attempt_new_like_success(alias, true, true);
    CHECK(col_rel_destroy_checked(alias) == 0);
    delta_pool_t *pool = delta_pool_create(2, sizeof(col_rel_t), 65536);
    CHECK(pool);
    col_rel_t *pooled = col_rel_pool_new_like(pool, "attempt pool", src);
    CHECK(pooled && pooled->pool_owned);
    attempt_new_like_success(pooled, true, true);
    CHECK(col_rel_destroy_checked(pooled) == 0); delta_pool_destroy(pool);
    wl_arena_t *arena = wl_arena_create(65536);
    pool = delta_pool_create(1, sizeof(col_rel_t), 32);
    CHECK(pool && arena);
    pooled = col_rel_pool_new_auto(pool, arena, "attempt arena", 1);
    CHECK(pooled && pooled->pool_owned && pooled->arena_owned);
    attempt_new_like_success(pooled, true, false);
    CHECK(col_rel_destroy_checked(pooled) == 0); delta_pool_destroy(pool);
    wl_arena_free(arena); CHECK(col_rel_destroy_checked(src) == 0);
    attempt_new_like_domains_and_prefixes(); attempt_new_like_allocations();
}

static void
attempt_constructor_pin(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    (void)c; (void)token; (void)phase;
    CHECK(watched && !wl_columnar_memory_governor_ref_is_sole(watched));
}

static void
attempt_new_like_credit_and_peers(void)
{
    col_rel_t *src = new_like_source(17, true, true, 1);
    watched = governor();
    uint64_t d, m, p;
    new_like_credits("attempt clone", src, true, &d, &m, &p);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(watched);
    const uint64_t limits[] = {d - 1, d + m - 1, d + m + p - 1};
    wl_columnar_memory_governor_test_checkpoint = attempt_constructor_pin;
    for (unsigned i = 0; i < 3; i++) {
        atomic_store_explicit(&g->usable_bytes, limits[i],
            memory_order_release);
        wl_evaluation_control_t *c = control(0);
        col_rel_t *out = NULL;
        CHECK(attempt_new_like(&out, src, true, true, c) == ENOSPC && !out);
        CHECK(finish(c, ENOSPC) > 0);
        CHECK(reserved() == 0 &&
            wl_columnar_memory_governor_ref_is_sole(watched));
        new_like_released(src); release(c);
    }
    atomic_store_explicit(&g->usable_bytes, WL_COLUMNAR_MEMORY_MIN_BUDGET,
        memory_order_release);
    wl_columnar_memory_reservation_init(&peer); peer_changed = false;
    wl_columnar_memory_governor_test_before_total_cas = peer_race;
    wl_evaluation_control_t *c = control(0);
    col_rel_t *out = NULL;
    CHECK(attempt_new_like(&out, src, true, true, c) == 0 && out);
    CHECK(finish(c, 0) > 0 && peer_changed && reserved() == d + m + p + 17);
    wl_columnar_memory_governor_test_before_total_cas = NULL;
    wl_columnar_memory_governor_test_checkpoint = NULL;
    CHECK(col_rel_destroy_checked(out) == 0 && reserved() == 17);
    CHECK(wl_columnar_memory_release(&peer) && reserved() == 0);
    release(c); CHECK(col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static unsigned attempt_busy_round, attempt_busy_stop;

static void
attempt_busy_checkpoint(wl_evaluation_control_t *c, uint64_t work)
{
    checkpoint(c, work);
    if (work == 144 && ++attempt_busy_round == attempt_busy_stop)
        wl_evaluation_control_request_cancel(c);
}

static wl_evaluation_control_report_t attempt_old_report;
static bool attempt_restarted;
static unsigned attempt_successor_callbacks;

static void
attempt_restart(wl_evaluation_control_t *c)
{
    CHECK(!attempt_restarted);
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
    CHECK(wl_evaluation_control_report(c, &attempt_old_report) == 0);
    CHECK(wl_evaluation_control_reset(c) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_active_attempt_id(c) !=
        attempt_old_report.attempt_id);
    attempt_restarted = true;
}

static void
attempt_restart_before_reader(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate, uint64_t observed)
{
    (void)gate; (void)observed;
    if (attempt_restarted) attempt_successor_callbacks++;
    else attempt_restart(c);
}

static void
attempt_restart_after_reader(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate, bool acquired)
{
    (void)gate;
    CHECK(acquired);
    if (attempt_restarted) attempt_successor_callbacks++;
    else attempt_restart(c);
}

static void
attempt_restart_after_commit(wl_evaluation_control_t *c, unsigned stage)
{
    (void)stage;
    if (attempt_restarted) attempt_successor_callbacks++;
    else attempt_restart(c);
}

static void
attempt_new_like_authority(void)
{
    col_rel_t *src = new_like_source(1, false, false, 0);
    watched = governor();
    for (unsigned gate = 0; gate < 2; gate++) {
        wl_columnar_source_access_writer_t writer = {0};
        CHECK(wl_columnar_source_access_writer_acquire(gate
            ? &src->source_access : &src->descriptor_access, &writer) == 0);
        wl_evaluation_control_t *c = control(0);
        attempt_busy_round = 0; attempt_busy_stop = gate ? 2 : 1;
        wl_columnar_relation_test_work_checkpoint = attempt_busy_checkpoint;
        col_rel_t *out = NULL;
        CHECK(attempt_new_like(&out, src, true, false, c) == EBUSY && !out);
        CHECK(finish(c, EBUSY) > 0 && attempt_busy_round == attempt_busy_stop);
        wl_columnar_relation_test_work_checkpoint = checkpoint;
        CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
        new_like_released(src); CHECK(reserved() == 0); release(c);
    }
    for (unsigned seam = 0; seam < 3; seam++) {
        wl_evaluation_control_t *c = control(0);
        attempt_restarted = false; attempt_successor_callbacks = 0;
        if (seam == 0)
            wl_columnar_relation_test_before_reader_cas =
                attempt_restart_before_reader;
        else if (seam == 1)
            wl_columnar_relation_test_after_reader_cas =
                attempt_restart_after_reader;
        else
            wl_columnar_relation_test_constructor_committed =
                attempt_restart_after_commit;
        col_rel_t *out = NULL;
        CHECK(attempt_new_like(&out, src, true, false, c) == EINVAL && !out);
        CHECK(attempt_restarted && attempt_old_report.result == 0
            && attempt_old_report.consumed > 0 && !attempt_successor_callbacks);
        wl_columnar_relation_test_before_reader_cas = NULL;
        wl_columnar_relation_test_after_reader_cas = NULL;
        wl_columnar_relation_test_constructor_committed = committed;
        CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
        wl_evaluation_control_report_t successor;
        CHECK(wl_evaluation_control_report(c, &successor) == 0
            && successor.consumed == 0 && successor.result == 0);
        CHECK(reserved() == 0 &&
            wl_columnar_memory_governor_ref_is_sole(watched));
        new_like_released(src); release(c);
    }
    CHECK(col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static void
attempt_phase_finish(wl_evaluation_control_t *c, int expected)
{
    CHECK(wl_evaluation_control_finish(c, &owner,
        expected > 0 ? expected : 0, 0) == expected);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(c, &report) == 0
        && report.result == expected && report.consumed >= seen);
}

static unsigned attempt_domain_round, attempt_domain_mode;

static void
attempt_domain_checkpoint(wl_evaluation_control_t *c, uint64_t work)
{
    if (attempt_restarted) {
        attempt_successor_callbacks++;
        return;
    }
    checkpoint(c, work);
    if (work != 128 || ++attempt_domain_round != 3)
        return;
    if (attempt_domain_mode == 0)
        wl_evaluation_control_request_cancel(c);
    else if (attempt_domain_mode == 1)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    else
        attempt_restart(c);
}

static void
attempt_new_like_domain_precedence(void)
{
    col_rel_t *src = new_like_source(2, false, false, 0);
    col_rel_t *saved_owner = src->storage_owner;
    uint32_t live_map = 2;
    const struct {
        bool lazy;
        wirelog_compound_kind_t kind;
        uint32_t count;
        bool map;
        uint32_t length, offset;
        int expected;
    } cases[] = {
        {true, (wirelog_compound_kind_t)99, 1, true, 0, 2, ENOTSUP},
        {false, WIRELOG_COMPOUND_KIND_INLINE, 1, false, 1, 2, EINVAL},
        {false, WIRELOG_COMPOUND_KIND_SIDE, 1, true, 0, 2, EINVAL},
        {false, (wirelog_compound_kind_t)99, 0, false, 0, 0, EINVAL},
        {false, WIRELOG_COMPOUND_KIND_INLINE, 1, true, 1, 2, ENOTSUP},
        {false, WIRELOG_COMPOUND_KIND_SIDE, 1, true, 1, 2, ENOTSUP},
        {false, WIRELOG_COMPOUND_KIND_NONE, 1, false, 0, 0, EINVAL},
        {false, WIRELOG_COMPOUND_KIND_NONE, 0, true, 1, 0, EINVAL},
        {false, WIRELOG_COMPOUND_KIND_NONE, 0, false, 0, 1, EINVAL},
    };
    watched = governor();
    for (unsigned i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        for (unsigned mode = 0; mode < 4; mode++) {
            src->storage_owner = cases[i].lazy ? NULL : saved_owner;
            src->compound_kind = cases[i].kind;
            src->compound_count = cases[i].count;
            src->compound_arity_map = cases[i].map ? &live_map : NULL;
            src->compound_arity_len = cases[i].length;
            src->inline_physical_offset = cases[i].offset;
            wl_evaluation_control_t *c = control(0);
            attempt_domain_round = 0;
            attempt_domain_mode = mode;
            attempt_restarted = false;
            attempt_successor_callbacks = 0;
            if (mode < 3)
                wl_columnar_relation_test_work_checkpoint =
                    attempt_domain_checkpoint;
            col_rel_t *out = NULL;
            int expected = mode == 1 ? EOVERFLOW
                : mode == 2 ? EINVAL : cases[i].expected;
            CHECK(attempt_new_like(&out, src, true, false, c) == expected
                && !out);
            wl_columnar_relation_test_work_checkpoint = checkpoint;
            if (mode == 2) {
                CHECK(attempt_restarted && !attempt_successor_callbacks
                    && attempt_old_report.consumed == 624
                    && attempt_old_report.result == 0);
                CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
                wl_evaluation_control_report_t successor;
                CHECK(wl_evaluation_control_report(c, &successor) == 0
                    && successor.result == 0 && successor.consumed == 0);
            } else {
                CHECK(finish(c, expected) == 624);
            }
            CHECK(reserved() == 0
                && wl_columnar_memory_governor_ref_is_sole(watched));
            /* The bounded map is borrowed test storage. Restore the genuine
             * scalar metadata before resolver checks or owned destruction. */
            src->storage_owner = saved_owner;
            src->compound_kind = WIRELOG_COMPOUND_KIND_NONE;
            src->compound_count = 0;
            src->compound_arity_map = NULL;
            src->compound_arity_len = 0;
            src->inline_physical_offset = 0;
            new_like_released(src);
            release(c);
        }
    }
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
    attempt_new_like_success(src, true, false);
    /* Refuse a genuine supported compound producer before construction. */
    src->nrows = 0;
    col_rel_logical_col_t logical = {WIRELOG_COMPOUND_KIND_INLINE, 2u, 1u};
    CHECK(col_rel_apply_compound_schema(src, &logical, 1) == 0);
    watched = governor();
    wl_evaluation_control_t *c = control(0);
    col_rel_t *out = NULL;
    CHECK(attempt_new_like(&out, src, true, false, c) == ENOTSUP && !out);
    CHECK(finish(c, ENOTSUP) == 624 && reserved() == 0
        && wl_columnar_memory_governor_ref_is_sole(watched));
    new_like_released(src); release(c);
    CHECK(col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static void
attempt_restart_governor(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    (void)token;
    if (attempt_restarted) {
        attempt_successor_callbacks++;
        return;
    }
    if (phase == fault_phase) {
        fault_hits++;
        attempt_restart(c);
    }
}

static void
attempt_new_like_governor_seams(void)
{
    const unsigned phases[] = {
        WL_COLUMNAR_MEMORY_GOVERNOR_VALIDATED,
        WL_COLUMNAR_MEMORY_GOVERNOR_CLAIMED,
        WL_COLUMNAR_MEMORY_GOVERNOR_SNAPSHOT,
        WL_COLUMNAR_MEMORY_GOVERNOR_PREPARED,
        WL_COLUMNAR_MEMORY_GOVERNOR_ADMITTED,
        WL_COLUMNAR_MEMORY_GOVERNOR_RESERVED,
        WL_COLUMNAR_MEMORY_GOVERNOR_COMMITTING,
        WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF
    };
    col_rel_t *src = new_like_source(17, true, true, 1);
    watched = governor();
    for (unsigned mode = 0; mode < 3; mode++) {
        for (unsigned i = 0; i < sizeof(phases) / sizeof(phases[0]); i++) {
            wl_evaluation_control_t *c = control(0);
            fault_phase = phases[i]; fault_hits = 0;
            attempt_restarted = false; attempt_successor_callbacks = 0;
            positive_error = mode == 1;
            wl_columnar_memory_governor_test_checkpoint = mode == 2
                ? attempt_restart_governor : governor_fault;
            col_rel_t *out = NULL;
            int expected = mode == 2 ? EINVAL : mode == 1 ? EOVERFLOW
                : WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(attempt_new_like(&out, src, true, true,
                c) == expected && !out);
            CHECK(fault_hits == 1 && reserved() == 0
                && wl_columnar_memory_governor_ref_is_sole(watched));
            wl_columnar_memory_governor_test_checkpoint = NULL;
            if (mode == 2) {
                CHECK(attempt_restarted && !attempt_successor_callbacks);
                CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
                wl_evaluation_control_report_t successor;
                CHECK(wl_evaluation_control_report(c, &successor) == 0
                    && successor.result == 0 && successor.consumed == 0);
            } else
                attempt_phase_finish(c, expected);
            new_like_released(src); release(c);
        }
    }
    positive_error = false; fault_phase = 0;
    CHECK(col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

/* Independent literal admission oracle for the complete empty-name/nullary,
* ungoverned/no-types/no-TS path. Source descriptor byte size is ABI dependent;
* its actual physical zero image takes separately counted16byte chunks.
* Includes identity observation/CAS, all three skipped commit iterations,
* irreversible installation and the final external store, not only entry. */
static void
attempt_new_like_complete_literal(void)
{
    uint64_t prefixes[2048];
    size_t count = 0;
    uint64_t total = 0;
#define ATTEMPT_LITERAL_ADD(work) do { \
            CHECK(count < sizeof(prefixes) / sizeof(prefixes[0])); \
            total += (work); prefixes[count++] = total; \
} while (0)
    const uint64_t entry_and_preflight[] = {
        128, 128, 96, 144, 128, 96, 144, 160, 192,
        96, /* empty relation name includes itsNUL */
        96, 96, 96, 96, 96, 96, /* checked D/table/names/lane/scratch/TS */
        96, /* type-presence/no-typebytes setup */
        96, 96, 96, /* owned-grid/table/TS payload checked arithmetic */
        128, 96, 160, 112 /* fits, credits, skipped D reserve, descriptor malloc */
    };
    for (size_t i = 0;
        i < sizeof(entry_and_preflight) / sizeof(entry_and_preflight[0]); i++)
        ATTEMPT_LITERAL_ADD(entry_and_preflight[i]);
    for (size_t pos = 0; pos < sizeof(col_rel_t); pos += 16)
        ATTEMPT_LITERAL_ADD(224);
    for (unsigned i = 0; i < 5; i++)
        ATTEMPT_LITERAL_ADD(96); /* five original embedded inert credit tokens */
    const uint64_t completion[] = {
        112, 224, 80, 112, 96, /* name malloc/copy, identity read/claim, owner init */
        160, 160, /* no-governor M/P reserve wrappers */
        96, 112, 224, /* Arrow root, root format malloc and3bytes */
        128, 128, 128, /* all three ungoverned skipped commit iterations */
        220, 64 /* complete local installation, source drain, external store */
    };
    for (size_t i = 0; i < sizeof(completion) / sizeof(completion[0]); i++)
        ATTEMPT_LITERAL_ADD(completion[i]);
#undef ATTEMPT_LITERAL_ADD
    col_rel_t *src = new_like_source(0, false, false, 2);
    watched = governor();
    for (size_t i = 0; i < count; i++) {
        for (unsigned below = 0; below < 2; below++) {
            wl_evaluation_control_t *c = control(prefixes[i] - below);
            col_rel_t *out = NULL;
            int rc =
                wl_columnar_relation_new_like_governed_checked_mode_attempt_controlled
                (
                "", src, NULL, false, &out, c,
                wl_evaluation_control_active_attempt_id(c));
            if (!below && i + 1 == count) {
                CHECK(rc == 0 && out && !out->ncols && !out->nrows);
                CHECK(finish(c, 0) == total);
                CHECK(col_rel_destroy_checked(out) == 0);
            } else {
                CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && !out);
                CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT)
                    == (below ? (i ? prefixes[i - 1] : 0) : prefixes[i]));
            }
            CHECK(reserved() == 0 &&
                wl_columnar_memory_governor_ref_is_sole(watched));
            new_like_released(src); release(c);
        }
    }
    CHECK(col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

/* Literal source schedule for auto constructors. Fees are independent numeric
 * literals, never production enums or a measured consumed-work oracle. */
typedef struct {
    uint64_t prefix[8192], allocations[2048], total;
    size_t count, allocation_count;
} auto_literal_t;

static void
auto_literal_add(auto_literal_t *s, uint64_t work, bool allocation)
{
    CHECK(s->count < sizeof(s->prefix) / sizeof(s->prefix[0]));
    s->total += work;
    s->prefix[s->count++] = s->total;
    if (allocation) {
        CHECK(s->allocation_count <
            sizeof(s->allocations) / sizeof(s->allocations[0]));
        s->allocations[s->allocation_count++] = s->total;
    }
}

static void
auto_literal_bytes(auto_literal_t *s, uint64_t bytes)
{
    for (uint64_t pos = 0; pos < bytes; pos += 16)
        auto_literal_add(s, 224, false);
}

static void
auto_literal_name(auto_literal_t *s, uint32_t column)
{
    auto_literal_add(s, 96, false);
    for (unsigned i = 0; i < digits(column); i++)
        auto_literal_add(s, 96, false);
    auto_literal_add(s, 192, false);
}

static void
auto_literal(const char *name, uint32_t width, bool governed,
    auto_literal_t *s)
{
    *s = (auto_literal_t){0};
#define AUTO_R(v) auto_literal_add(s, (v), false)
#define AUTO_A(v) auto_literal_add(s, (v), true)
#define AUTO_POINTERS() do { for (uint32_t pos = 0; pos < width; \
                                 pos += 16) { AUTO_R(192); } } while (0)
#define AUTO_RESERVE(nonzero) do { AUTO_R(160); \
                                   if (governed && (nonzero)) { AUTO_R(74); \
                                                                AUTO_R(54); \
                                                                AUTO_R(84); \
                                                                AUTO_R(29); \
                                   } } while (0)
    AUTO_R(128); AUTO_R(128); AUTO_R(128); AUTO_R(192);
    uint64_t name_bytes = strlen(name) + 1;
    for (uint64_t i = 0; i < name_bytes; i++) AUTO_R(96);
    for (unsigned i = 0; i < 6; i++) AUTO_R(96);
    AUTO_R(96); /* no type bytes */
    AUTO_R(96); AUTO_R(96); AUTO_R(96);
    if (governed) AUTO_R(96); /* scratch payload */
    AUTO_R(128);
    for (uint32_t i = 0; i < width; i++) {
        auto_literal_name(s, i);
        AUTO_R(96); AUTO_R(96); AUTO_R(96);
    }
    AUTO_R(96); AUTO_RESERVE(true); AUTO_A(112);
    auto_literal_bytes(s, sizeof(col_rel_t));
    for (unsigned i = 0; i < 5; i++) AUTO_R(96);
    AUTO_A(112); auto_literal_bytes(s, name_bytes);
    AUTO_R(80); AUTO_R(112); AUTO_R(96);
    AUTO_RESERVE(true); AUTO_RESERVE(width != 0);
    if (width) {
        AUTO_A(112); AUTO_POINTERS();
        for (uint32_t i = 0; i < width; i++) AUTO_A(112);
        if (governed) AUTO_A(112);
        AUTO_A(112); AUTO_POINTERS();
        for (uint32_t i = 0; i < width; i++) {
            auto_literal_name(s, i); AUTO_A(112);
            auto_literal_bytes(s, 4u + digits(i));
        }
    }
    AUTO_R(96); AUTO_A(112); auto_literal_bytes(s, 3);
    if (width) {
        AUTO_A(112); AUTO_POINTERS();
        for (uint32_t i = 0; i < width; i++) {
            AUTO_A(128); AUTO_A(112); auto_literal_bytes(s, 2);
            auto_literal_name(s, i); AUTO_A(112);
            auto_literal_bytes(s, 4u + digits(i));
        }
    }
    for (unsigned i = 0; i < 3; i++) {
        AUTO_R(128);
        if (governed && (i < 2 || width)) {
            AUTO_R(74); AUTO_R(54); AUTO_R(29);
        }
    }
    AUTO_R(220); AUTO_R(128);
#undef AUTO_RESERVE
#undef AUTO_POINTERS
#undef AUTO_A
#undef AUTO_R
}

static int
auto_attempt(const char *name, uint32_t width, uint32_t capacity,
    bool governed, col_rel_t **out, wl_evaluation_control_t *c, uint64_t id)
{
    return governed
        ? wl_columnar_relation_new_auto_governed_attempt_controlled(name,
               width, capacity, watched, out, c, id)
        : wl_columnar_relation_new_auto_attempt_controlled(name, width,
               out, c, id);
}

static void
auto_equal(col_rel_t *r, col_rel_t *raw, uint32_t width, uint32_t capacity,
    bool governed)
{
    CHECK(r && raw && !r->nrows && !raw->nrows && r->ncols == width);
    CHECK(r->capacity == capacity && raw->capacity == capacity);
    CHECK(r->view_generation == raw->view_generation &&
        r->view_generation == 2);
    CHECK(r->storage_generation == raw->storage_generation &&
        r->storage_generation == 1);
    CHECK(r->storage_owner == r && r->storage_owner_generation == 1);
    CHECK(!r->column_types && !raw->column_types && !r->timestamps &&
        !raw->timestamps);
    CHECK(!r->timestamp_capacity && !r->has_graph_column && !r->graph_col_idx &&
        !r->declared_ncols);
    CHECK(r->schema_ok && r->schema.n_children == width);
    CHECK(strcmp(r->name, raw->name) == 0);
    CHECK((r->row_scratch != NULL) == (governed && width != 0));
    for (uint32_t i = 0; i < width; i++) {
        CHECK(r->columns[i] && strcmp(r->col_names[i], raw->col_names[i]) == 0);
        CHECK(strcmp(r->schema.children[i]->name,
            raw->schema.children[i]->name) == 0);
        CHECK(strcmp(r->schema.children[i]->format, "l") == 0);
        CHECK(strcmp(raw->schema.children[i]->format, "l") == 0);
    }
}

static void
auto_prefixes(const char *name, uint32_t width, uint32_t capacity,
    bool governed, bool all_prefixes)
{
    auto_literal_t s; auto_literal(name, width, governed, &s);
    watched = governor();
    for (size_t i = all_prefixes ? 0 : s.count - 1; i < s.count; i++) {
        for (unsigned below = 0; below < 2; below++) {
            wl_evaluation_control_t *c = control(s.prefix[i] - below);
            col_rel_t *out = NULL;
            external_out = &out;
            uint64_t id = wl_evaluation_control_active_attempt_id(c);
            int rc = auto_attempt(name, width, capacity, governed, &out, c,
                    id);
            external_out = NULL;
            if (!below && i + 1 == s.count) {
                CHECK(rc == 0 && out && finish(c, 0) == s.total);
                col_rel_t *raw = NULL;
                if (governed)
                    CHECK(wl_columnar_relation_new_auto_governed(name, width,
                        capacity, false, watched, &raw) == 0);
                else raw = col_rel_new_auto(name, width);
                auto_equal(out, raw, width,
                    governed ? capacity : COL_REL_INIT_CAP, governed);
                CHECK(col_rel_destroy_checked(raw) == 0 &&
                    col_rel_destroy_checked(out) == 0);
            } else {
                CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && !out);
                attempt_phase_finish(c, rc);
                wl_evaluation_control_report_t report;
                CHECK(wl_evaluation_control_report(c, &report) == 0
                    && report.attempt_id == id &&
                    report.limit == s.prefix[i] - below
                    && report.result == rc &&
                    report.stop_reason == WL_EVALUATION_CONTROL_WORK_LIMIT
                    && !report.execution_status && !report.cleanup_status
                    && report.consumed ==
                    (below ? (i ? s.prefix[i - 1] : 0) : s.prefix[i]));
                /* At the independently identified commit128 -> child74
                 * boundary, the prepaid caller has no reached tail when the
                 * child refuses. This does not define expected consumption. */
                if (governed && !below && i + 1 < s.count
                    && s.prefix[i] - (i ? s.prefix[i - 1] : 0) == 128
                    && s.prefix[i + 1] - s.prefix[i] == 74)
                    CHECK(report.consumed - seen == 128);
            }
            CHECK(!reserved() &&
                wl_columnar_memory_governor_ref_is_sole(watched));
            release(c);
        }
    }
    for (size_t i = 0; i < s.allocation_count; i++) {
        wl_evaluation_control_t *c = control(0);
        col_rel_t *out = NULL;
        wl_columnar_relation_test_constructor_fail_allocation = (uint32_t)i + 1;
        CHECK(auto_attempt(name, width, capacity, governed, &out, c,
            wl_evaluation_control_active_attempt_id(c)) == ENOMEM && !out);
        CHECK(!wl_columnar_relation_test_constructor_fail_allocation);
        CHECK(finish(c, ENOMEM) == s.allocations[i] && !reserved());
        CHECK(wl_columnar_memory_governor_ref_is_sole(watched)); release(c);
    }
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

/* Callback occurrence inventory locates real seams only. Work expectations
 * remain the independent literal schedule; no recorded telemetry defines it. */
static unsigned auto_family, auto_target, auto_occurrences, auto_mode,
    auto_hits;

static unsigned auto_failed_rounds;

static void
auto_fault(wl_evaluation_control_t *c, unsigned family)
{
    if (attempt_restarted) {
        attempt_successor_callbacks++; return;
    }
    if (family != auto_family) return;
    auto_occurrences++;
    if (!auto_target || auto_occurrences != auto_target) return;
    auto_hits++;
    if (auto_mode == 0) wl_evaluation_control_request_cancel(c);
    else if (auto_mode == 1) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else attempt_restart(c);
}

static void
auto_work_fault(wl_evaluation_control_t *c, uint64_t work)
{
    if (!attempt_restarted) checkpoint(c, work);
    auto_fault(c, 0);
}

static void
auto_governor_fault(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    (void)token; (void)phase;
    CHECK(!wl_columnar_memory_governor_ref_is_sole(watched));
    auto_fault(c, 1);
}

static void
auto_commit_fault(wl_evaluation_control_t *c, unsigned stage)
{
    (void)stage; auto_fault(c, 2);
}

static void
auto_identity_fault(wl_evaluation_control_t *c, uint64_t current)
{
    (void)current; auto_fault(c, 3);
}

static void
auto_total_fault(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *g, uint64_t expected, bool transition)
{
    (void)g; (void)expected; CHECK(!transition); auto_fault(c, 4);
}

static void
auto_after_total_fault(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *g, bool success, bool overflow)
{
    (void)g; CHECK(!overflow);
    if (!attempt_restarted && !success) auto_failed_rounds++;
    auto_fault(c, 5);
}

static void
auto_callback_seams(bool governed)
{
    watched = governor();
    wl_columnar_relation_test_work_checkpoint = auto_work_fault;
    wl_columnar_memory_governor_test_work_checkpoint = auto_work_fault;
    wl_columnar_memory_governor_test_checkpoint = auto_governor_fault;
    wl_columnar_relation_test_constructor_committed = auto_commit_fault;
    wl_columnar_relation_test_before_identity_cas = auto_identity_fault;
    wl_columnar_memory_governor_test_before_total_cas = auto_total_fault;
    wl_columnar_memory_governor_test_after_total_cas = auto_after_total_fault;
    for (auto_family = 0; auto_family < 6; auto_family++) {
        if (!governed &&
            (auto_family == 1 || auto_family == 2 ||
            auto_family >= 4)) continue;
        auto_target = auto_occurrences = 0; attempt_restarted = false;
        wl_evaluation_control_t *c = control(0); col_rel_t *out = NULL;
        CHECK(auto_attempt("seams", 17, 0, governed, &out, c,
            wl_evaluation_control_active_attempt_id(c)) == 0 && out);
        unsigned count = auto_occurrences;
        CHECK(count && finish(c, 0) > 0); release(c);
        CHECK(col_rel_destroy_checked(out) == 0);
        for (auto_target = 1; auto_target <= count; auto_target++) {
            for (auto_mode = 0; auto_mode < 3; auto_mode++) {
                auto_occurrences = auto_hits = 0;
                attempt_restarted = false; attempt_successor_callbacks = 0;
                c = control(0); out = NULL; external_out = &out;
                int expected = auto_mode == 0 ? WL_EVALUATION_CONTROL_CANCELLED
                    : auto_mode == 1 ? EOVERFLOW : EINVAL;
                CHECK(auto_attempt("seams", 17, 0, governed, &out, c,
                    wl_evaluation_control_active_attempt_id(c)) == expected &&
                    !out);
                external_out = NULL;
                CHECK(auto_hits == 1 && !attempt_successor_callbacks);
                if (auto_mode == 2) {
                    CHECK(attempt_restarted && attempt_old_report.consumed > 0);
                    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
                    wl_evaluation_control_report_t next;
                    CHECK(wl_evaluation_control_report(c,
                        &next) == 0 && !next.consumed && !next.result);
                } else attempt_phase_finish(c, expected);
                CHECK(!reserved() &&
                    wl_columnar_memory_governor_ref_is_sole(watched));
                release(c);
            }
        }
    }
    attempt_restarted = false;
    wl_columnar_relation_test_work_checkpoint = checkpoint;
    wl_columnar_memory_governor_test_work_checkpoint = checkpoint;
    wl_columnar_memory_governor_test_checkpoint = NULL;
    wl_columnar_relation_test_constructor_committed = committed;
    wl_columnar_relation_test_before_identity_cas = NULL;
    wl_columnar_memory_governor_test_before_total_cas = NULL;
    wl_columnar_memory_governor_test_after_total_cas = NULL;
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static void
auto_failed_cas_seams(void)
{
    watched = governor();
    wl_columnar_relation_test_work_checkpoint = auto_work_fault;
    wl_columnar_memory_governor_test_work_checkpoint = auto_work_fault;
    auto_literal_t s; auto_literal("failed CAS", 17, true, &s);
    uint64_t failed_prefix = 0;
    for (size_t i = 0; i + 2 < s.count; i++) {
        uint64_t prior = i ? s.prefix[i - 1] : 0;
        if (s.prefix[i] - prior == 74) {
            CHECK(s.prefix[i + 1] - s.prefix[i] == 54
                && s.prefix[i + 2] - s.prefix[i + 1] == 84);
            failed_prefix = s.prefix[i + 2]; break;
        }
    }
    CHECK(failed_prefix);
    wl_columnar_memory_governor_test_before_total_cas = peer_race;
    wl_columnar_memory_governor_test_after_total_cas = auto_after_total_fault;
    auto_family = 5;
    for (unsigned run = 0; run < 4; run++) {
        wl_columnar_memory_reservation_init(&peer); peer_changed = false;
        auto_target = run ? 1 : 0; auto_mode = run ? run - 1 : 0;
        auto_occurrences = auto_hits = auto_failed_rounds = 0;
        attempt_restarted = false; attempt_successor_callbacks = 0;
        wl_evaluation_control_t *c = control(0); col_rel_t *out = NULL;
        external_out = &out;
        int expected = !run ? 0 : auto_mode ==
            0 ? WL_EVALUATION_CONTROL_CANCELLED
            : auto_mode == 1 ? EOVERFLOW : EINVAL;
        CHECK(auto_attempt("failed CAS", 17, 1, true, &out, c,
            wl_evaluation_control_active_attempt_id(c)) == expected);
        external_out = NULL;
        CHECK(peer_changed && auto_failed_rounds == 1 &&
            !attempt_successor_callbacks);
        if (!run) {
            CHECK(out && finish(c, 0) == s.total + 84);
            CHECK(col_rel_destroy_checked(out) == 0);
        } else {
            CHECK(!out && auto_hits == 1);
            if (auto_mode == 2) {
                CHECK(attempt_restarted &&
                    attempt_old_report.consumed == failed_prefix);
                CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
                wl_evaluation_control_report_t next;
                CHECK(wl_evaluation_control_report(c, &next) == 0
                    && !next.consumed && !next.result &&
                    !next.execution_status && !next.cleanup_status);
            } else {
                attempt_phase_finish(c, expected);
                wl_evaluation_control_report_t report;
                CHECK(wl_evaluation_control_report(c,
                    &report) == 0 && report.consumed == failed_prefix);
            }
        }
        CHECK(reserved() == 17 &&
            wl_columnar_memory_governor_ref_is_sole(watched));
        CHECK(wl_columnar_memory_release(&peer) && !reserved()); release(c);
    }
    attempt_restarted = false;
    wl_columnar_memory_governor_test_before_total_cas = NULL;
    wl_columnar_memory_governor_test_after_total_cas = NULL;
    wl_columnar_relation_test_work_checkpoint = checkpoint;
    wl_columnar_memory_governor_test_work_checkpoint = checkpoint;
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static void
auto_credits(const char *name, uint32_t width, uint32_t capacity,
    uint64_t *d, uint64_t *m, uint64_t *p)
{
    *d = sizeof(col_rel_t) + strlen(name) + 1;
    *m = 3;
    for (uint32_t i = 0; i < width; i++)
        *m += sizeof(char *) + sizeof(struct ArrowSchema *)
            + sizeof(struct ArrowSchema) + 2 + 2 * (4u + digits(i));
    *p = (uint64_t)width * ((capacity ? capacity : 1u) * sizeof(int64_t)
        + sizeof(int64_t *) + sizeof(int64_t));
}

static void
auto_governor_cases(void)
{
    watched = governor();
    uint64_t d, m, p; auto_credits("credit", 17, 0, &d, &m, &p);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(watched);
    const uint64_t limits[] = {d - 1, d + m - 1, d + m + p - 1};
    wl_columnar_memory_governor_test_checkpoint = attempt_constructor_pin;
    for (unsigned i = 0; i < 3; i++) {
        atomic_store_explicit(&g->usable_bytes, limits[i],
            memory_order_release);
        wl_evaluation_control_t *c = control(0); col_rel_t *out = NULL;
        CHECK(auto_attempt("credit", 17, 0, true, &out, c,
            wl_evaluation_control_active_attempt_id(c)) == ENOSPC && !out);
        CHECK(finish(c, ENOSPC) > 0 && !reserved());
        CHECK(wl_columnar_memory_governor_ref_is_sole(watched)); release(c);
    }
    atomic_store_explicit(&g->usable_bytes, WL_COLUMNAR_MEMORY_MIN_BUDGET,
        memory_order_release);
    wl_columnar_memory_reservation_init(&peer); peer_changed = false;
    wl_columnar_memory_governor_test_before_total_cas = peer_race;
    auto_literal_t s; auto_literal("credit", 17, true, &s);
    wl_evaluation_control_t *c = control(s.total + 84); col_rel_t *out = NULL;
    CHECK(auto_attempt("credit", 17, 0, true, &out, c,
        wl_evaluation_control_active_attempt_id(c)) == 0 && out);
    CHECK(peer_changed && finish(c,
        0) == s.total + 84 && reserved() == d + m + p + 17);
    CHECK(out->descriptor_reserved_bytes == d &&
        out->metadata_reserved_bytes == m
        && out->retained_reserved_bytes == p);
    wl_columnar_memory_governor_test_before_total_cas = NULL;
    wl_columnar_memory_governor_test_checkpoint = NULL;
    CHECK(col_rel_destroy_checked(out) == 0 && reserved() == 17);
    CHECK(wl_columnar_memory_release(&peer) && !reserved()); release(c);
    CHECK(wl_columnar_memory_governor_ref_is_sole(watched));
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static void
auto_domains_and_bits(void)
{
    watched = governor();
    col_rel_t *out = NULL;
    CHECK(!col_rel_new_auto(NULL, 1));
    CHECK(wl_columnar_relation_new_auto_attempt_controlled(NULL, 1, &out, NULL,
        0) == ENOMEM && !out);
    CHECK(wl_columnar_relation_new_auto_attempt_controlled("raw", 1, NULL, NULL,
        0) == EINVAL);
    CHECK(wl_columnar_relation_new_auto_governed_attempt_controlled("raw", 1, 0,
        watched, &out, NULL, 0) == 0 && out && !out->capacity);
    CHECK(col_rel_destroy_checked(out) == 0); out = NULL;
    CHECK(wl_columnar_relation_new_auto_attempt_controlled("raw", 1, &out, NULL,
        0) == 0 && out);
    CHECK(out->capacity == COL_REL_INIT_CAP &&
        col_rel_destroy_checked(out) == 0); out = NULL;
    for (unsigned bad = 0; bad < 3; bad++) {
        for (unsigned mode = 0; mode < 4; mode++) {
            wl_evaluation_control_t *c = control(0);
            auto_family = 0; auto_target = 1; auto_occurrences = auto_hits = 0;
            auto_mode = mode; attempt_restarted = false;
            attempt_successor_callbacks = 0;
            if (mode <
                3) wl_columnar_relation_test_work_checkpoint = auto_work_fault;
            int expected = mode == 1 ? EOVERFLOW : EINVAL;
            CHECK(wl_columnar_relation_new_auto_governed_attempt_controlled(
                    bad == 0 ? NULL : "domain", 1, 0, bad == 1 ? NULL : watched,
                    bad == 2 ? NULL : &out, c,
                    wl_evaluation_control_active_attempt_id(c)) == expected &&
                !out);
            wl_columnar_relation_test_work_checkpoint = checkpoint;
            if (mode == 2) {
                CHECK(attempt_restarted && !attempt_successor_callbacks &&
                    attempt_old_report.consumed == 128);
                CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
                wl_evaluation_control_report_t next;
                CHECK(wl_evaluation_control_report(c,
                    &next) == 0 && !next.consumed);
            } else CHECK(finish(c, expected) == 128);
            release(c);
        }
    }
    attempt_restarted = false;
    wl_evaluation_control_t *c = control(0);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    CHECK(auto_attempt("stale", 1, 1, false, &out, c,
        id + 1) == EINVAL && !out);
    CHECK(finish(c, EINVAL) == 0); release(c);
    const uint64_t bits[] = {UINT64_C(0x8000000000000000),
                             UINT64_C(0x3ff8000000000000)};
    c = control(0);
    CHECK(auto_attempt("map bits", 1, 1, false, &out, c,
        wl_evaluation_control_active_attempt_id(c)) == 0 && out);
    CHECK(finish(c, 0) > 0); release(c);
    for (unsigned i = 0; i < 2; i++) {
        int64_t lane; memcpy(&lane, &bits[i], sizeof(lane));
        CHECK(col_rel_append_row(out, &lane) == 0);
        CHECK(memcmp(&out->columns[0][i], &bits[i], sizeof(lane)) == 0);
    }
    CHECK(!out->column_types && strcmp(out->schema.children[0]->format,
        "l") == 0);
    CHECK(col_rel_destroy_checked(out) == 0 && !reserved());
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static void
auto_published_frame_retention(void)
{
    watched = governor();
    wl_col_session_t *sess = calloc(1, sizeof(*sess)); CHECK(sess);
    wl_mem_ledger_init(&sess->mem_ledger, 0); sess->memory_governor = watched;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(sess, &frame) == 0);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    col_rel_t *input = col_rel_new_auto("borrowed", 1); CHECK(input);
    CHECK(eval_stack_push(stack, input, false) == 0);
    wl_evaluation_control_t *c = control(0);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_controlled(frame, c,
        wl_evaluation_control_active_attempt_id(c)) == 0);
    col_rel_t **slot = NULL;
    CHECK(wl_columnar_eval_stack_unary_output(frame,
        &slot) == 0 && slot && !*slot);
    CHECK(auto_attempt("published", 1, 1, true, slot, c,
        wl_evaluation_control_active_attempt_id(c)) == 0 && *slot);
    col_rel_t *published = *slot;
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(wl_columnar_source_access_reader_acquire(&published->source_access,
        &reader) == 0);
    auto_literal_t literal; auto_literal("published", 1, true, &literal);
    attempt_phase_finish(c, 0);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(c, &report) == 0
        && !report.result && !report.execution_status && !report.cleanup_status
        && report.consumed == 544 + literal.total);
    release(c);
    uint64_t retained_bytes = reserved();
    CHECK(wl_columnar_eval_stack_unary_finish(frame, 0) == EBUSY);
    CHECK(reserved() == retained_bytes && stack->top == 1 &&
        stack->items[0].rel == input);
    CHECK(wl_columnar_source_access_reader_release(&reader) == 0);
    CHECK(wl_columnar_eval_stack_unary_finish(frame, 0) == 0);
    CHECK(wl_columnar_eval_stack_unary_reset(frame) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !reserved());
    CHECK(col_rel_destroy_checked(input) == 0);
    CHECK(wl_columnar_memory_governor_ref_is_sole(watched));
    free(sess); wl_columnar_memory_governor_ref_release(watched);
    watched = NULL;
}

#ifndef _WIN32
static wl_columnar_memory_reservation_t *auto_private_token;
static col_rel_t *auto_private_relation;
static uint64_t auto_private_cutoff;

static void
auto_private_observe(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    (void)c;
    if (phase == WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF &&
        !auto_private_token)
        auto_private_token = token;
}

static void
auto_private_committed(wl_evaluation_control_t *c, unsigned stage)
{
    (void)c; (void)stage;
    CHECK(auto_private_token);
    /* Read only the still-live original token, never unfinished descriptor
     * fields. Save the owner address before the builder's token scope ends. */
    auto_private_relation = (col_rel_t *)(uintptr_t)atomic_load_explicit(
        &auto_private_token->owner_bits, memory_order_acquire);
    CHECK(auto_private_relation);
}

static void
auto_private_violation(wl_evaluation_control_t *c, uint64_t work)
{
    checkpoint(c, work);
    if (seen != auto_private_cutoff) return;
    CHECK(auto_private_relation && external_out && !*external_out);
    /* Deliberately privileged invariant injection after completed install.
     * This is unsupported private-reader escape, not retained-frame behavior. */
    col_rel_t *private = auto_private_relation;
    CHECK(private && private->schema_ok);
    atomic_store_explicit(&private->source_access.state,
        WL_COLUMNAR_SOURCE_ACCESS_WRITER, memory_order_relaxed);
    wl_evaluation_control_request_cancel(c);
}

static void
auto_private_fail_stop(void)
{
    pid_t child = fork(); CHECK(child >= 0);
    if (child == 0) {
        struct rlimit no_core = {0, 0}; (void)setrlimit(RLIMIT_CORE, &no_core);
        fclose(stderr); watched = governor(); auto_private_token = NULL;
        auto_private_relation = NULL;
        auto_literal_t s; auto_literal("private", 1, true, &s);
        auto_private_cutoff = s.total;
        wl_columnar_memory_governor_test_checkpoint = auto_private_observe;
        wl_columnar_relation_test_constructor_committed =
            auto_private_committed;
        wl_columnar_relation_test_work_checkpoint = auto_private_violation;
        wl_evaluation_control_t *c = control(0); col_rel_t *out = NULL;
        external_out = &out;
        (void)auto_attempt("private", 1, 1, true, &out, c,
            wl_evaluation_control_active_attempt_id(c));
        _exit(0);
    }
    int status = 0;
    CHECK(waitpid(child, &status,
        0) == child && WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
}
#endif

/* Original protected source: independent literal nested admission schedule. */
static void
scalar_source_report(wl_evaluation_control_t *c, uint64_t id, int rc,
    uint64_t expected)
{
    CHECK(wl_evaluation_control_finish(c, &owner, rc > 0 ? rc : 0, 0) == rc);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.attempt_id == id && report.consumed == expected
        && report.result == rc && report.execution_status == (rc > 0 ? rc : 0)
        && report.cleanup_status == 0);
    if (rc < 0) CHECK(report.stop_reason == rc);
    else if (rc == 0) CHECK(report.stop_reason == WL_EVALUATION_CONTROL_NONE);
}

static int
scalar_source_open(wl_columnar_relation_scalar_source_t *s,
    col_rel_t *r, bool ts, wl_evaluation_control_t *c, bool *initialized)
{
    int rc = wl_columnar_relation_scalar_source_init_attempt_controlled(s,
            initialized, c, wl_evaluation_control_active_attempt_id(c));
    if (!rc) rc = wl_columnar_relation_scalar_source_begin_attempt_controlled(
            s, r, ts);
    return rc;
}

static void
scalar_source_prefixes(uint32_t width)
{
    static const uint64_t fixed[] = {256, 256, 96, 144, 224, 96, 144, 224, 192};
    uint64_t fees[64];
    size_t count = sizeof(fixed) / sizeof(fixed[0]);
    memcpy(fees, fixed, sizeof(fixed));
    for (uint32_t i = 0; i < width; i += 8) fees[count++] = 224;
    col_rel_t *r = new_like_source(width, false, true, 2);
    uint64_t baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(r->memory_governor));
    uint64_t prefix = 0;
    for (size_t i = 0; i < count; i++) {
        uint64_t previous = prefix; prefix += fees[i];
        for (unsigned below = 0; below < 2; below++) {
            wl_evaluation_control_t *c = control(prefix - below);
            uint64_t id = wl_evaluation_control_active_attempt_id(c);
            wl_columnar_relation_scalar_source_t s;
            bool initialized = true;
            int rc = scalar_source_open(&s, r, true, c, &initialized);
            int expected = !below && i + 1 == count
                ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT;
            CHECK(rc == expected);
            CHECK(initialized == (i != 0 || !below));
            if (initialized) {
                CHECK(wl_columnar_relation_scalar_source_end(&s, rc) == rc);
                CHECK(s.phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED);
                CHECK(!s.descriptor_reader.owner && !s.owner_reader.owner);
            }
            scalar_source_report(c, id, rc, below ? previous : prefix);
            new_like_released(r);
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(r->memory_governor))
                == baseline);
            release(c);
        }
    }
    CHECK(prefix == 1632 + 224 * ((width + 7u) / 8u));
    CHECK(col_rel_destroy_checked(r) == 0);
}

static void *
scalar_source_wrong_thread(void *arg)
{
    wl_columnar_relation_scalar_source_t *s = arg;
    uint32_t width = 99, rows = 99; bool ts = false;
    int64_t values[320] = {0};
    CHECK(wl_columnar_relation_scalar_source_shape_attempt_controlled(s,
        &width, &rows, &ts) == EINVAL && width == 99 && rows == 99);
    CHECK(wl_columnar_relation_scalar_source_gather_attempt_controlled(s,
        0, values, s->width) == EINVAL);
    CHECK(wl_columnar_relation_scalar_source_end(s, EIO) == EINVAL);
    return NULL;
}

static void
scalar_source_twins(uint32_t width, bool typed, bool ts, bool alias_mode)
{
    col_rel_t *root = new_like_source(width, typed, true, 1);
    col_rel_t *r = root;
    if (alias_mode) {
        r = col_rel_new_like("source alias", root);
        CHECK(r && col_rel_install_shared_view(r, root) == 0);
    }
    uint64_t view = r->view_generation, storage = r->storage_generation;
    uint64_t baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(root->memory_governor));
    watched = governor();
    wl_evaluation_control_t *c = control(0);
    wl_columnar_relation_scalar_source_t s;
    bool initialized = false;
    CHECK(scalar_source_open(&s, r, ts, c, &initialized) == 0 && initialized);
    CHECK(s.descriptor == r && s.owner == root);
    CHECK(s.descriptor_reader.identity == (uintptr_t)&s.descriptor_reader);
    CHECK(s.owner_reader.identity == (uintptr_t)&s.owner_reader);
    CHECK(s.descriptor_reader.owner == &r->descriptor_access);
    CHECK(s.owner_reader.owner == &root->source_access);
    wl_columnar_source_access_writer_t writer = {0};
    CHECK(wl_columnar_source_access_writer_acquire(&r->descriptor_access,
        &writer) == EBUSY);
    CHECK(wl_columnar_source_access_writer_acquire(&root->source_access,
        &writer) == EBUSY);
    uint32_t got_width = 99, rows = 99; bool got_ts = !ts;
    CHECK(wl_columnar_relation_scalar_source_shape_attempt_controlled(&s,
        &got_width, &rows, &got_ts) == 0);
    CHECK(got_width == width && rows == r->nrows && got_ts == ts);
    int64_t values[320];
    CHECK(width <= 320);
    for (uint32_t row = 0; row < rows; row++) {
        CHECK(wl_columnar_relation_scalar_source_gather_attempt_controlled(&s,
            row, values, width) == 0);
        for (uint32_t col = 0; col < width; col++)
            CHECK(values[col] == r->columns[col][row]);
    }
    wl_columnar_relation_scalar_source_t copy = s;
    CHECK(wl_columnar_relation_scalar_source_end(&copy, EIO) == EINVAL);
    CHECK(wl_columnar_relation_scalar_source_shape_attempt_controlled(&copy,
        &got_width, &rows, &got_ts) == EINVAL);
    wl_thread_t thread;
    CHECK(wl_thread_create(&thread, scalar_source_wrong_thread, &s) == 0);
    CHECK(wl_thread_join(&thread) == 0);
    CHECK(s.phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD);
    col_rel_t *out = NULL, *raw = NULL;
    CHECK(wl_columnar_relation_scalar_source_new_like_attempt_controlled(&s,
        "protected clone", watched, &out) == 0 && out);
    CHECK(wl_columnar_relation_new_like_governed_checked_mode(
            "protected clone", r, NULL, ts, &raw) == 0);
    new_like_equal(out, raw, r);
    CHECK(s.phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD);
    CHECK(wl_columnar_relation_scalar_source_end(&s, 0) == 0);
    CHECK(wl_columnar_relation_scalar_source_end(&s, EIO) == 0);
    CHECK(finish(c, 0) > 0);
    release(c);
    /* DRAINED owns no old control or source lifetime. */
    CHECK(wl_columnar_relation_scalar_source_end(&s, ENOMEM) == 0);
    new_like_released(r);
    CHECK(r->view_generation == view && r->storage_generation == storage);
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(root->memory_governor)) ==
        baseline);
    CHECK(col_rel_destroy_checked(out) == 0 &&
        col_rel_destroy_checked(raw) == 0);
    CHECK(reserved() == 0 && wl_columnar_memory_governor_ref_is_sole(watched));
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
    if (alias_mode) CHECK(col_rel_destroy_checked(r) == 0);
    CHECK(col_rel_destroy_checked(root) == 0);
}

static unsigned scalar_fault_mode;
static uint64_t scalar_fault_at;
static void
scalar_source_checkpoint(wl_evaluation_control_t *c, uint64_t work)
{
    if (attempt_restarted) {
        attempt_successor_callbacks++; return;
    }
    checkpoint(c, work);
    if (seen != scalar_fault_at) return;
    if (scalar_fault_mode == 0) wl_evaluation_control_request_cancel(c);
    else if (scalar_fault_mode == 1)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    else attempt_restart(c);
}

static void
scalar_source_seams(void)
{
    static const uint64_t prefixes[] = {256, 512, 608, 752, 976, 1072, 1216,
                                        1440,
                                        1632, 1856, 2080, 2304};
    col_rel_t *r = new_like_source(17, false, false, 2);
    wl_columnar_relation_test_work_checkpoint = scalar_source_checkpoint;
    for (scalar_fault_mode = 0; scalar_fault_mode < 3; scalar_fault_mode++)
        for (size_t i = 0; i < sizeof(prefixes)/sizeof(prefixes[0]); i++) {
            wl_evaluation_control_t *c = control(0);
            uint64_t id = wl_evaluation_control_active_attempt_id(c);
            attempt_restarted = false; attempt_successor_callbacks = 0;
            scalar_fault_at = prefixes[i];
            wl_columnar_relation_scalar_source_t s; bool initialized = false;
            int expected = scalar_fault_mode == 0
                ? WL_EVALUATION_CONTROL_CANCELLED
                : scalar_fault_mode == 1 ? EOVERFLOW : EINVAL;
            CHECK(scalar_source_open(&s, r, false, c,
                &initialized) == expected);
            CHECK(initialized && wl_columnar_relation_scalar_source_end(&s,
                expected) == expected);
            new_like_released(r);
            if (attempt_restarted) {
                CHECK(attempt_old_report.attempt_id == id
                    && attempt_old_report.consumed == prefixes[i]);
                CHECK(attempt_successor_callbacks == 0);
                scalar_source_report(c,
                    wl_evaluation_control_active_attempt_id(c), 0, 0);
            } else scalar_source_report(c, id, expected, prefixes[i]);
            release(c);
        }
    wl_columnar_relation_test_work_checkpoint = checkpoint;
    attempt_restarted = false;
    /* The first live END remains available after an interrupted init. */
    CHECK(col_rel_destroy_checked(r) == 0);
}

static void
scalar_source_gather_prefixes(void)
{
    col_rel_t *r = new_like_source(17, true, false, 2);
    static const uint64_t fees[] = {128, 224, 224, 224};
    for (unsigned shape = 0; shape < 2; shape++) {
        uint64_t prefix = 0;
        size_t count = shape ? 1 : sizeof(fees)/sizeof(fees[0]);
        for (size_t i = 0; i < count; i++) {
            uint64_t previous = prefix; prefix += fees[i];
            for (unsigned below = 0; below < 2; below++) {
                wl_evaluation_control_t *c = control(2304 + prefix - below);
                uint64_t id = wl_evaluation_control_active_attempt_id(c);
                wl_columnar_relation_scalar_source_t s; bool init = false;
                CHECK(scalar_source_open(&s, r, false, c, &init) == 0 && init);
                int64_t values[17];
                for (unsigned j = 0; j < 17; j++)values[j] = -999;
                uint32_t width = 99, rows = 99; bool ts = true;
                int rc = shape
                    ?
                    wl_columnar_relation_scalar_source_shape_attempt_controlled(
                    &s, &width, &rows, &ts)
                    :
                    wl_columnar_relation_scalar_source_gather_attempt_controlled
                    (
                    &s, 1, values, 17);
                int expected = !below && i+1 == count
                    ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT;
                CHECK(rc == expected);
                if (shape) {
                    CHECK(expected ? width == 99 && rows == 99 && ts
                        : width == 17 && rows == 2 && !ts);
                } else {
                    unsigned copied = (below ? i : i+1);
                    copied = copied ? (copied-1)*8 : 0;
                    if (copied > 17)copied = 17;
                    for (unsigned j = 0; j < 17; j++)
                        CHECK(values[j] ==
                            (j < copied ? r->columns[j][1] : -999));
                }
                CHECK(wl_columnar_relation_scalar_source_end(&s, rc) == rc);
                scalar_source_report(c, id, rc, 2304+(below?previous:prefix));
                release(c); new_like_released(r);
            }
        }
    }
    CHECK(col_rel_destroy_checked(r) == 0);
}

static void
scalar_source_like_literal(void)
{
    col_rel_t *r = new_like_source(0, false, false, 2);
    watched = governor();
    /* Empty recipe and no TS exactly match independent auto builder inputs.
     * Same three128 caller phases and external128, but no new source readers. */
    auto_literal_t literal; auto_literal("protected", 0, true, &literal);
    for (size_t i = 0; i < literal.count; i++)
        for (unsigned below = 0; below < 2; below++) {
            wl_evaluation_control_t *c = control(1632+literal.prefix[i]-below);
            uint64_t id = wl_evaluation_control_active_attempt_id(c);
            wl_columnar_relation_scalar_source_t s; bool init = false;
            CHECK(scalar_source_open(&s, r, false, c, &init) == 0 && init);
            col_rel_t *out = r;
            int rc =
                wl_columnar_relation_scalar_source_new_like_attempt_controlled(
                &s, "protected", watched, &out);
            int expected = !below && i+1 == literal.count
                ?0:WL_EVALUATION_CONTROL_WORK_LIMIT;
            CHECK(rc == expected && (expected ? out == NULL : out != NULL));
            CHECK(wl_columnar_relation_scalar_source_end(&s, rc) == rc);
            scalar_source_report(c, id, rc,
                1632+(below?(i?literal.prefix[i-1]:0):literal.prefix[i]));
            if (out)CHECK(col_rel_destroy_checked(out) == 0);
            CHECK(reserved() == 0 &&
                wl_columnar_memory_governor_ref_is_sole(watched));
            new_like_released(r); release(c);
        }
    for (size_t i = 0; i < literal.allocation_count; i++) {
        wl_evaluation_control_t *c = control(0);
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        wl_columnar_relation_scalar_source_t s; bool init = false;
        CHECK(scalar_source_open(&s, r, false, c, &init) == 0 && init);
        col_rel_t *out = NULL;
        wl_columnar_relation_test_constructor_fail_allocation = (uint32_t)i+1;
        int rc = wl_columnar_relation_scalar_source_new_like_attempt_controlled(
            &s, "protected", watched, &out);
        CHECK(rc == ENOMEM && !out
            && wl_columnar_relation_test_constructor_fail_allocation == 0);
        CHECK(wl_columnar_relation_scalar_source_end(&s, rc) == rc);
        scalar_source_report(c, id, rc, 1632+literal.allocations[i]);
        CHECK(reserved() == 0 &&
            wl_columnar_memory_governor_ref_is_sole(watched));
        new_like_released(r); release(c);
    }
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
    CHECK(col_rel_destroy_checked(r) == 0);
}

static wl_columnar_relation_scalar_source_t *scalar_reader_record;
static wl_columnar_source_access_gate_t *scalar_reader_gate;
static unsigned scalar_reader_mode, scalar_reader_hits;
static void
scalar_reader_after(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate, bool acquired)
{
    if (gate != scalar_reader_gate || !acquired)return;
    CHECK(!scalar_reader_hits++);
    CHECK(scalar_reader_record->owner_reader.owner == gate);
    CHECK(scalar_reader_record->descriptor_reader.owner != NULL);
    if (scalar_reader_mode == 0)wl_evaluation_control_request_cancel(c);
    else if (scalar_reader_mode == 1)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    else attempt_restart(c);
}

static void
scalar_source_reader_authority(void)
{
    col_rel_t *r = new_like_source(1, false, false, 2);
    for (scalar_reader_mode = 0; scalar_reader_mode < 3; scalar_reader_mode++) {
        wl_evaluation_control_t *c = control(0);
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        wl_columnar_relation_scalar_source_t s; bool init = false;
        scalar_reader_record = &s; scalar_reader_gate = &r->source_access;
        scalar_reader_hits = 0; attempt_restarted = false;
        attempt_successor_callbacks = 0;
        wl_columnar_relation_test_after_reader_cas = scalar_reader_after;
        int rc = scalar_source_open(&s, r, false, c, &init);
        wl_columnar_relation_test_after_reader_cas = NULL;
        int expected = scalar_reader_mode == 0?WL_EVALUATION_CONTROL_CANCELLED:
            scalar_reader_mode == 1?EOVERFLOW:EINVAL;
        CHECK(rc == expected && init && scalar_reader_hits == 1);
        CHECK(s.phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED
            && !s.owner_reader.owner && !s.descriptor_reader.owner);
        new_like_released(r);
        if (attempt_restarted) {
            CHECK(attempt_old_report.attempt_id == id
                && attempt_old_report.consumed == 1216);
            scalar_source_report(c, wl_evaluation_control_active_attempt_id(c),
                0, 0);
        } else scalar_source_report(c, id, rc, 1216);
        release(c);
    }
    attempt_restarted = false;
    /* Force an actual failed strong CAS on each original gate. */
    race_gates[0] = &r->descriptor_access; race_gates[1] = &r->source_access;
    gate_rounds[0] = gate_rounds[1] = 1;
    memset(gate_peers, 0, sizeof(gate_peers));
    wl_columnar_relation_test_before_reader_cas = reader_race;
    wl_evaluation_control_t *c = control(1856+288);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    wl_columnar_relation_scalar_source_t s; bool init = false;
    CHECK(scalar_source_open(&s, r, false, c, &init) == 0 && init);
    wl_columnar_relation_test_before_reader_cas = NULL;
    CHECK(gate_rounds[0] == 0 && gate_rounds[1] == 0);
    CHECK(gate_peers[0].owner && gate_peers[1].owner);
    CHECK(wl_columnar_relation_scalar_source_end(&s, 0) == 0);
    scalar_source_report(c, id, 0, 2144);
    for (unsigned i = 0; i < 2; i++) {
        CHECK(wl_columnar_source_access_reader_release(&gate_peers[i]) == 0);
        race_gates[i] = NULL;
    }
    release(c); new_like_released(r);
    CHECK(col_rel_destroy_checked(r) == 0);
}

static void
scalar_source_domains(void)
{
    col_rel_t *r = new_like_source(1, false, false, 2);
    /* Refused initialization publishes only its disjoint false flag. */
    wl_evaluation_control_t *c = control(0);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    wl_columnar_relation_scalar_source_t s; bool init = true;
    CHECK(wl_columnar_relation_scalar_source_init_attempt_controlled(&s,
        &init, c, id+1) == EINVAL && !init);
    scalar_source_report(c, id, EINVAL, 0); release(c);
    for (unsigned mode = 0; mode < 7; mode++) {
        c = control(0); id = wl_evaluation_control_active_attempt_id(c);
        CHECK(scalar_source_open(&s, r, false, c, &init) == 0 && init);
        uint32_t width = 99, rows = 99; bool ts = true; int64_t value = -999;
        col_rel_t *out = r;
        int rc;
        if (mode ==
            0) rc = wl_columnar_relation_scalar_source_shape_attempt_controlled(
                &s, NULL, &rows, &ts);
        else if (mode <
            4) rc =
                wl_columnar_relation_scalar_source_gather_attempt_controlled(
                &s, mode == 1?2:0, mode == 2?NULL:&value, mode == 3?0:1);
        else if (mode ==
            4) rc =
                wl_columnar_relation_scalar_source_new_like_attempt_controlled(
                &s, NULL, NULL, &out);
        else if (mode ==
            5) rc =
                wl_columnar_relation_scalar_source_new_like_attempt_controlled(
                &s, "x", NULL, NULL);
        else {
            CHECK(wl_columnar_relation_scalar_source_end(&s, EIO) == EIO);
            rc = wl_columnar_relation_scalar_source_shape_attempt_controlled(
                &s, &width, &rows, &ts);
        }
        CHECK(rc == EINVAL && width == 99 && rows == 99 && ts && value == -999);
        if (mode == 4)CHECK(out == NULL);
        CHECK(wl_columnar_relation_scalar_source_end(&s,
            rc) == (mode == 6?EIO:rc));
        scalar_source_report(c, id, rc, 1856+(mode == 6?0:128));
        release(c); new_like_released(r);
    }
    /* Typed classifier rejection retains domain priority over cancellation;
     * positive accounting/stale errors win at the admitted classifier tail. */
    for (unsigned mode = 0; mode < 3; mode++) {
        wirelog_compound_kind_t kind = r->compound_kind;
        r->compound_kind = (wirelog_compound_kind_t)99;
        c = control(0); id = wl_evaluation_control_active_attempt_id(c);
        scalar_fault_mode = mode; scalar_fault_at = 976;
        attempt_restarted = false; attempt_successor_callbacks = 0;
        wl_columnar_relation_test_work_checkpoint = scalar_source_checkpoint;
        int rc = scalar_source_open(&s, r, false, c, &init);
        wl_columnar_relation_test_work_checkpoint = checkpoint;
        CHECK(rc == (mode == 0?EINVAL:mode == 1?EOVERFLOW:EINVAL) && init);
        r->compound_kind = kind; new_like_released(r);
        if (attempt_restarted) {
            CHECK(attempt_old_report.attempt_id == id
                && attempt_old_report.consumed == 976);
            scalar_source_report(c, wl_evaluation_control_active_attempt_id(c),
                0, 0);
        } else scalar_source_report(c, id, rc, 976);
        release(c);
    }
    attempt_restarted = false;
    CHECK(col_rel_destroy_checked(r) == 0);
}

/* Typed JOIN constructor/append reuse: these tests use real credits and the
 * original checked row wrapper, not a separate unguarded append entrypoint. */
static void
join_typed_operator_fixtures(void)
{
    const uint32_t widths[] = {0, 1, 17, 129};
    for (unsigned gi = 0; gi < 2; ++gi) {
        watched = gi ? governor() : NULL;
        for (unsigned wi = 0; wi < 4; ++wi) {
            uint32_t width = widths[wi];
            wirelog_column_type_t *types = malloc((width ? width : 1) *
                    sizeof(*types));
            int64_t *row = calloc(width ? width : 1, sizeof(*row));
            CHECK(types && row);
            for (uint32_t c = 0; c < width; ++c) {
                types[c] = c % 7 + WIRELOG_TYPE_INT32;
                row[c] = types[c] == WIRELOG_TYPE_FLOAT
                    ? wl_columnar_float_to_bits(-0.0) : (int64_t)c;
            }
            wl_evaluation_control_t *ctl = control(0);
            uint64_t id = wl_evaluation_control_active_attempt_id(ctl);
            col_rel_t *out = NULL;
            CHECK(wl_columnar_relation_new_typed_operator_attempt_controlled(
                    "$join", width, width ? types : NULL, watched, &out, ctl,
                    id) == 0);
            CHECK(out && out->ncols == width && !out->nrows &&
                !out->timestamps);
            CHECK(out->capacity == COL_REL_INIT_CAP && !out->has_graph_column);
            for (uint32_t c = 0; c < width; ++c) {
                char name[32]; snprintf(name, sizeof(name), "col%u", c);
                CHECK(strcmp(out->col_names[c], name) == 0);
                CHECK(out->column_types[c] == types[c]);
            }
            wl_columnar_relation_append_batch_t batch = {0};
            CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch,
                out, ctl) == 0);
            for (unsigned r = 0; r < 65; ++r)
                CHECK(
                    wl_columnar_relation_append_batch_numeric_row_operator_controlled
                    (
                        &batch, row, ctl, id) == 0);
            CHECK(wl_columnar_relation_append_batch_end_controlled(&batch,
                0) == 0);
            CHECK(out->nrows == 65);
            for (uint32_t c = 0; c < width; ++c) {
                CHECK(out->column_types[c] == types[c]);
                int64_t expected = types[c] == WIRELOG_TYPE_FLOAT ? 0 : row[c];
                CHECK(out->columns[c][0] == expected &&
                    out->columns[c][64] == expected);
            }
            /* Existing wrapper must retain typed finite-value validation. */
            if (width > 4) {
                row[4] = wl_columnar_float_to_bits(INFINITY);
                wl_columnar_relation_append_batch_t invalid = {0};
                CHECK(wl_columnar_relation_append_batch_begin_controlled(
                        &invalid, out, ctl) == 0);
                CHECK(
                    wl_columnar_relation_append_batch_numeric_row_operator_controlled
                    (
                        &invalid, row, ctl, id) == EINVAL);
                CHECK(wl_columnar_relation_append_batch_end_controlled(&invalid,
                    EINVAL) == EINVAL);
                CHECK(out->nrows == 65);
            }
            CHECK(wl_evaluation_control_finish(ctl, &owner, 0, 0) == 0);
            release(ctl);
            CHECK(col_rel_destroy_checked(out) == 0);
            CHECK(!watched || reserved() == 0);
            free(types); free(row);
        }
        if (watched) wl_columnar_memory_governor_ref_release(watched);
        watched = NULL;
    }
    watched = governor();
    wirelog_column_type_t allocation_types[17];
    for (unsigned c = 0; c < 17; ++c) allocation_types[c] = WIRELOG_TYPE_FLOAT;
    bool allocation_completed = false;
    for (unsigned ordinal = 1; ordinal < 128; ++ordinal) {
        wl_evaluation_control_t *ctl = control(0);
        col_rel_t *out = NULL;
        wl_columnar_relation_test_constructor_fail_allocation = ordinal;
        int rc = wl_columnar_relation_new_typed_operator_attempt_controlled(
            "$join", 17, allocation_types, watched, &out, ctl,
            wl_evaluation_control_active_attempt_id(ctl));
        bool injected = !wl_columnar_relation_test_constructor_fail_allocation;
        wl_columnar_relation_test_constructor_fail_allocation = 0;
        if (rc) CHECK(rc == ENOMEM && injected && !out);
        else {
            CHECK(out && !injected && ordinal > 20);
            CHECK(col_rel_destroy_checked(out) == 0);
            allocation_completed = true;
        }
        CHECK(wl_evaluation_control_finish(ctl, &owner, rc, 0) == rc);
        release(ctl);
        CHECK(!reserved() && wl_columnar_memory_governor_ref_is_sole(watched));
        if (allocation_completed) break;
    }
    CHECK(allocation_completed);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
    wl_columnar_memory_resolution_t tiny = {
        .budget_bytes = 1, .usable_bytes = 1,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK,
    };
    watched = wl_columnar_memory_governor_ref_create(&tiny); CHECK(watched);
    wl_evaluation_control_t *denied_control = control(0);
    col_rel_t *denied_output = NULL;
    CHECK(wl_columnar_relation_new_typed_operator_attempt_controlled("$join",
        17,
        allocation_types, watched, &denied_output, denied_control,
        wl_evaluation_control_active_attempt_id(denied_control)) == ENOSPC);
    CHECK(!denied_output && !reserved());
    CHECK(wl_evaluation_control_finish(denied_control, &owner, ENOSPC,
        0) == ENOSPC);
    release(denied_control);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
    const uint64_t prefix_limits[] = {1, 127, 128, 255, 256, 383, 384};
    const uint64_t prefix_consumed[] = {0, 0, 128, 128, 256, 256, 384};
    const wirelog_column_type_t valid_type = WIRELOG_TYPE_FLOAT;
    for (unsigned i = 0; i < sizeof(prefix_limits) / sizeof(prefix_limits[0]);
        ++i) {
        wl_evaluation_control_t *ctl = control(prefix_limits[i]);
        col_rel_t *out = NULL;
        CHECK(wl_columnar_relation_new_typed_operator_attempt_controlled(
                "$join", 1, &valid_type, NULL, &out, ctl,
                wl_evaluation_control_active_attempt_id(ctl)) ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(!out);
        CHECK(wl_evaluation_control_finish(ctl, &owner, 0,
            0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(ctl, &report) == 0);
        CHECK(report.consumed == prefix_consumed[i]);
        release(ctl);
    }
    wl_evaluation_control_t *cancelled = control(0);
    col_rel_t *cancelled_output = NULL;
    cancel_at = 256; /* ENTRY128 then first TYPE128, before PREPARE/build. */
    CHECK(wl_columnar_relation_new_typed_operator_attempt_controlled("$join", 1,
        &valid_type, NULL, &cancelled_output, cancelled,
        wl_evaluation_control_active_attempt_id(cancelled)) ==
        WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!cancelled_output);
    CHECK(finish(cancelled, WL_EVALUATION_CONTROL_CANCELLED) == 256);
    release(cancelled); cancel_at = 0;
    wirelog_column_type_t invalid_type = (wirelog_column_type_t)99;
    const uint64_t limits[] = {1, 127, 128, 255, 256, 383, 384};
    for (unsigned i = 0; i < sizeof(limits) / sizeof(limits[0]); ++i) {
        wl_evaluation_control_t *ctl = control(limits[i]);
        col_rel_t *out = NULL;
        int rc = wl_columnar_relation_new_typed_operator_attempt_controlled(
            "limit", 1, &invalid_type, NULL, &out, ctl,
            wl_evaluation_control_active_attempt_id(ctl));
        CHECK((rc == WL_EVALUATION_CONTROL_WORK_LIMIT || rc == EINVAL) && !out);
        CHECK(wl_evaluation_control_finish(ctl, &owner, rc > 0 ? rc : 0,
            0) == rc);
        release(ctl);
    }
    wl_evaluation_control_t *ctl = control(0);
    col_rel_t *out = NULL;
    CHECK(wl_columnar_relation_new_typed_operator_attempt_controlled("bad", 1,
        &invalid_type, NULL, &out, ctl,
        wl_evaluation_control_active_attempt_id(ctl)) == EINVAL && !out);
    CHECK(wl_columnar_relation_new_typed_operator_attempt_controlled("bad", 1,
        NULL, NULL, &out, ctl,
        wl_evaluation_control_active_attempt_id(ctl)) == EINVAL && !out);
    CHECK(wl_evaluation_control_finish(ctl, &owner, EINVAL, 0) == EINVAL);
    release(ctl);
}

static void
sorted_stamp_drain(wl_columnar_relation_append_batch_t *batch, int rc)
{
    wl_columnar_relation_private_batch_drain_t drain;
    wl_columnar_relation_private_batch_drain_init(&drain);
    bool done = false;
    for (unsigned turn = 0; !done; ++turn) {
        CHECK(turn < 20);
        CHECK(!wl_columnar_relation_private_batch_drain_step(&drain, batch,
            rc, &done));
    }
}

static void *
sorted_stamp_wrong_thread(void *arg)
{
    wl_columnar_relation_append_batch_t *batch = arg;
    CHECK(wl_columnar_relation_append_batch_mark_sorted_operator_controlled(
            batch, batch->control, batch->attempt_id) == EINVAL);
    return NULL;
}

static void
sorted_stamp_faults(void)
{
    for (unsigned mode = 0; mode < 4; ++mode) {
        col_rel_t *r = col_rel_new_auto("$stamp", 1); CHECK(r);
        if (mode == 3) {
            wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
            CHECK(!col_rel_set_column_types(r, &type, 1));
        }
        wl_evaluation_control_t *c = control(0);
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        wl_columnar_relation_append_batch_t batch = {0};
        CHECK(!wl_columnar_relation_append_batch_begin_controlled(&batch, r,
            c));
        if (!mode) {
            wl_thread_t thread;
            CHECK(!wl_thread_create(&thread, sorted_stamp_wrong_thread,
                &batch));
            CHECK(!wl_thread_join(&thread));
        } else if (mode == 1) cancel_at = 1116;
        else if (mode == 2) {
            (void)finish(c, 0); retry(c, 0);
        } else {
            uint64_t nonce = batch.raw.single.set.acquisition_nonce;
            for (unsigned n = 0; n < 130; ++n) {
                int64_t value = wl_columnar_float_to_bits((double)n);
                CHECK(
                    !wl_columnar_relation_append_batch_row_controlled(
                        &batch, &value));
            }
            CHECK(batch.raw.single.set.acquisition_nonce != nonce);
        }
        int rc =
            wl_columnar_relation_append_batch_mark_sorted_operator_controlled(
            &batch, c, id);
        CHECK(rc == (mode == 1 ? WL_EVALUATION_CONTROL_CANCELLED :
            mode == 2 ? EINVAL : 0));
        if (rc) CHECK(!r->sorted_nrows && !r->run_count);
        sorted_stamp_drain(&batch, rc);
        CHECK(r->nrows == (mode == 3 ? 130u : 0u));
        if (mode == 3) CHECK(r->sorted_nrows == 130 && r->run_count == 1 &&
                r->run_ends[0] == 130 &&
                r->column_types[0] == WIRELOG_TYPE_FLOAT);
        if (mode == 3) {
            /* Legacy growing-row hooks are checkpoints, not an admitted-work
             * oracle. This case verifies renewed authority and cache lifetime. */
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
            CHECK(!wl_evaluation_control_report(c, &report));
            CHECK(report.attempt_id == id && !report.result &&
                report.stop_reason == WL_EVALUATION_CONTROL_NONE);
        } else (void)finish(c, mode == 2 ? 0 : rc);
        release(c); col_rel_destroy(r);
    }
}

/* A sorted cache stamp changes neither rows nor generations and survives the
 * genuine staged original-writer drain. These fees are independent literals,
 * provisional until the production source certificate is accepted. */
static void
sorted_stamp_cases(void)
{
    wl_columnar_relation_test_work_checkpoint = checkpoint;
    for (unsigned rows = 0; rows <= 2; ++rows) {
        const uint64_t limits[] = {0, 668 + 127, 668 + 128,
                                   668 + 128 + 191, 668 + 320, 668 + 447,
                                   668 + 448};
        for (unsigned j = 0; j < sizeof(limits) / sizeof(limits[0]); ++j) {
            col_rel_t *r = col_rel_new_auto("$stamp", 1);
            CHECK(r);
            for (unsigned n = 0; n < rows; ++n) {
                int64_t value = n;
                CHECK(!col_rel_append_row(r, &value));
            }
            wl_evaluation_control_t *c = control(limits[j]);
            uint64_t id = wl_evaluation_control_active_attempt_id(c);
            wl_columnar_relation_append_batch_t batch = {0};
            CHECK(!wl_columnar_relation_append_batch_begin_controlled(&batch,
                r, c));
            uint64_t storage = r->storage_generation, view = r->view_generation;
            wl_columnar_relation_append_batch_t copy = batch;
            CHECK(
                wl_columnar_relation_append_batch_mark_sorted_operator_controlled
                (
                    &copy, c, id) == EINVAL);
            CHECK(
                wl_columnar_relation_append_batch_mark_sorted_operator_controlled
                (
                    &batch, c, id + 1) == EINVAL);
            CHECK(
                wl_columnar_relation_append_batch_mark_sorted_operator_controlled
                (
                    &batch, NULL, id) == EINVAL);
            int rc =
                wl_columnar_relation_append_batch_mark_sorted_operator_controlled
                (
                &batch, c, id);
            CHECK(rc == ((!limits[j] || limits[j] >= 1116) ? 0 :
                WL_EVALUATION_CONTROL_WORK_LIMIT));
            CHECK(r->nrows == rows && r->storage_generation == storage &&
                r->view_generation == view);
            if (!rc) CHECK(r->sorted_nrows == rows &&
                    r->run_count == (rows ? 1u : 0u) && r->run_ends[0] == rows);
            else CHECK(!r->sorted_nrows && !r->run_count);
            wl_columnar_relation_private_batch_drain_t drain;
            wl_columnar_relation_private_batch_drain_init(&drain);
            bool done = false;
            for (unsigned turn = 0; !done; ++turn) {
                CHECK(turn < 20);
                CHECK(!wl_columnar_relation_private_batch_drain_step(&drain,
                    &batch, rc, &done));
            }
            if (!rc) CHECK(r->sorted_nrows == rows &&
                    r->run_count == (rows ? 1u : 0u));
            uint64_t expected = !limits[j] || limits[j] >= 1116 ? 1116 :
                limits[j] < 796 ? 668 : limits[j] < 988 ? 796 : 988;
            CHECK(finish(c, rc) == expected);
            release(c); col_rel_destroy(r);
        }
    }
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
    attempt_new_like_fixtures();
    attempt_new_like_credit_and_peers();
    attempt_new_like_authority();
    attempt_new_like_domain_precedence();
    attempt_new_like_governor_seams();
    attempt_new_like_complete_literal();
    for (unsigned governed = 0; governed < 2; governed++) {
        const uint32_t widths[] = {0, 1, 16, 17};
        for (unsigned i = 0; i < 4; i++)
            auto_prefixes(i ? "auto" : "", widths[i], i % 2, governed != 0,
                true);
        auto_prefixes(long_name, 1, 64, governed != 0, false);
        auto_prefixes("wide", 300, 7, governed != 0, false);
        auto_callback_seams(governed != 0);
    }
    auto_failed_cas_seams();
    auto_governor_cases(); auto_domains_and_bits();
    auto_published_frame_retention();
#ifndef _WIN32
    new_like_release_fail_stop();
    auto_private_fail_stop();
#endif
    const uint32_t scalar_widths[] = {0, 1, 8, 9, 17, 300};
    for (size_t i = 0; i < sizeof(scalar_widths)/sizeof(scalar_widths[0]); i++)
        scalar_source_prefixes(scalar_widths[i]);
    scalar_source_reader_authority();
    scalar_source_domains();
    scalar_source_gather_prefixes();
    scalar_source_seams();
    scalar_source_like_literal();
    for (unsigned typed = 0; typed < 2; typed++)for (unsigned ts = 0; ts < 2;
            ts++) {
            scalar_source_twins(0, typed != 0, ts != 0, false);
            scalar_source_twins(17, typed != 0, ts != 0, false);
            scalar_source_twins(17, typed != 0, ts != 0, true);
        }
    scalar_source_twins(300, true, true, false);
    join_typed_operator_fixtures();
    sorted_stamp_cases();
    sorted_stamp_faults();
    puts("evaluation_relation_creation: OK");
    return 0;
}
