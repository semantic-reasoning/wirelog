/* #2299 independent lookup tariffs and real indexed callback lease ownership. */
#include "../wirelog/wirelog-internal.h"
#include "../wirelog/extension_registry_private.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#define CHECK(x) do{if (!(x)){fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                                  #x); exit(1);}}while (0)
extern void (*wl_extension_control_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t);
extern int wl_extension_control_test_fail_allocation;
extern unsigned wl_extension_control_test_live_allocations;
static int owner;
static unsigned destroyed;
static int invoke(const wirelog_extension_value_t *args, uint32_t count,
    wirelog_extension_value_t *out, void *arg){
    (void)args; (void)count; (void)arg;
    out->type = WIRELOG_EXTENSION_VALUE_BOOL; out->size = 1;
    out->as.bool_value = 1; return 0;
}
static void destroy(void *arg){
    CHECK(arg == &owner); ++destroyed;
}
static wirelog_extension_registry_t *create(void){
    wirelog_extension_registry_t *registry =
        wirelog_extension_registry_create(); CHECK(registry);
    const char *names[] = {"a", "ab", "abc"};
    for (unsigned i = 0; i < 3; ++i) {
        wirelog_extension_descriptor_t d =
        {.abi_version = WIRELOG_EXTENSION_ABI_VERSION,
         .size = sizeof(d), .name = names[i],
         .invoke = invoke, .result_type = WIRELOG_EXTENSION_VALUE_BOOL,
         .user_data = &owner, .destroy = destroy};
        CHECK(!wirelog_extension_register(registry, &d));
    }
    return registry;
}
static void release(wirelog_extension_registry_t *registry,
    wirelog_extension_snapshot_t *snapshot){
    wirelog_extension_snapshot_release(snapshot);
    CHECK(!wirelog_extension_unregister(registry,
        "a") && !wirelog_extension_unregister(registry, "ab") &&
        !wirelog_extension_unregister(registry,
        "abc") && !wirelog_extension_registry_destroy(registry));
}
static wl_evaluation_control_t *begin(uint64_t limit){
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c) &&
        !wl_evaluation_control_attach(c,
        &owner) && !wl_evaluation_control_begin(c, &owner)); return c;
}
static uint64_t id(wl_evaluation_control_t *c){
    return wl_evaluation_control_active_attempt_id(c);
}
static void finish(wl_evaluation_control_t *c, int rc){
    CHECK(wl_evaluation_control_finish(c, &owner, rc > 0?rc:0, 0) == rc);
    CHECK(!wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
}
static void oracles(void){
    /* Literal hand schedules, not observed-consumption values. Entries a/ab/abc
     * in registration order. Packet counts=4+R+F+B+2L+E+P after allocation.
     * a:2,1,1,1,1,2; ab:3,1,1,2,2,5; abc:4,1,1,3,3,9;
     * abd:4,1,1,3,3,8. Empty3T; all four trims:2+5+4=11T.
     * Trimmed abc:8,3,3,3,3,9. VT is not trim:5,1,1,4,3,3. */
    struct {const char *name; size_t span; unsigned packets; int entry;
    } cases[] = {
        {"a", 1, 13, 0}, {"ab", 2, 20, 1}, {"abc", 3, 28, 2},
        {"abd", 3, 27, -1},
        {" \tabc\r\n", 7, 36, 2}, {"\vabc", 4, 25, -1}, {"A", 1, 16, -1},
        {"", 0, 3, -1}, {" \t\r\n", 4, 11, -1}, {"abc\0ignored", 11, 28, 2}
    };
    wirelog_extension_registry_t *registry = create();
    wirelog_extension_snapshot_t *snapshot =
        wirelog_extension_snapshot_acquire(registry); CHECK(snapshot);
    for (unsigned k = 0; k < sizeof(cases)/sizeof(*cases); ++k) {
        const wirelog_extension_descriptor_t *raw =
            wirelog_extension_snapshot_find(snapshot, cases[k].name);
        char expected_error[64];
        snprintf(expected_error, sizeof(expected_error), "%s",
            wirelog_extension_last_error());
        CHECK(raw ==
            (cases[k].entry <
            0?NULL:&snapshot->entries[cases[k].entry]->descriptor));
        for (unsigned below = 0; below < 2; ++below) {
            wl_evaluation_control_t *c = begin(UINT64_C(224)*cases[k].packets-
                    below);
            const wirelog_extension_descriptor_t *descriptor = (void *)&owner;
            size_t index = 99; bool found = true;
            wl_extension_error_set("preserved");
            int rc = wl_extension_control_lookup_attempt(snapshot,
                    cases[k].name, cases[k].span, &descriptor, &index, &found,
                    c, id(c));
            uint64_t used = 0;
            CHECK(!wl_evaluation_control_test_active_consumed(c, id(c), &used));
            CHECK(used == UINT64_C(224)*(cases[k].packets-below));
            if (below)CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                    descriptor == (void *)&owner && index == 99 && found &&
                    !strcmp(wirelog_extension_last_error(), "preserved"));
            else CHECK(!rc && descriptor == raw && found == (raw != NULL) &&
                    index == (raw?(size_t)cases[k].entry:SIZE_MAX) &&
                    !strcmp(wirelog_extension_last_error(), expected_error));
            CHECK(!wl_extension_control_test_live_allocations); finish(c, rc);
        }
        for (unsigned prefix = 1; prefix <= cases[k].packets; ++prefix) {
            wl_evaluation_control_t *c = begin(UINT64_C(224)*prefix-1);
            const wirelog_extension_descriptor_t *d = (void *)&owner;
            size_t index = 99; bool found = true;
            wl_extension_error_set("preserved");
            CHECK(wl_extension_control_lookup_attempt(snapshot, cases[k].name,
                cases[k].span, &d, &index, &found, c,
                id(c)) == WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(d == (void *)&owner && index == 99 && found &&
                !strcmp(wirelog_extension_last_error(), "preserved") &&
                !wl_extension_control_test_live_allocations);
            finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
        }
    }
    /* NULL semantic miss2T; API invalid bound before any read/charge. */
    for (unsigned which = 0; which < 2; ++which) {
        wl_evaluation_control_t *c = begin(448);
        const wirelog_extension_descriptor_t *d = (void *)&owner;
        size_t index = 99; bool found = true;
        CHECK(!wl_extension_control_lookup_attempt(which?snapshot:NULL,
            which?NULL:"abc", 3, &d, &index, &found, c, id(c)) &&
            !d && !found && index == SIZE_MAX &&
            !strcmp(wirelog_extension_last_error(),
            "snapshot or name is NULL")); finish(c, 0);
    }
    wl_evaluation_control_t *c = begin(0);
    const wirelog_extension_descriptor_t *d = (void *)&owner; size_t index = 99;
    bool found = true;
    wl_extension_error_set("preserved");
    CHECK(wl_extension_control_lookup_attempt(snapshot, NULL, 256, &d, &index,
        &found, c, id(c)) == EINVAL);
    uint64_t used = 99;
    CHECK(!wl_evaluation_control_test_active_consumed(c, id(c),
        &used) && !used && d == (void *)&owner && index == 99 && found &&
        !strcmp(wirelog_extension_last_error(), "preserved"));
    finish(c, EINVAL);
    /* Allocation failure:3+R4+F1+B1=9T, unchanged raw invalid-name outcome. */
    c = begin(9*224); wl_extension_control_test_fail_allocation = 0;
    CHECK(!wl_extension_control_lookup_attempt(snapshot, "abc", 3, &d, &index,
        &found, c, id(c)) && !d && !found && index == SIZE_MAX &&
        !strcmp(wirelog_extension_last_error(), "invalid extension name"));
    wl_extension_control_test_fail_allocation = -1; finish(c, 0);
    release(registry, snapshot);
    /* Max255 bytes, one authentic matching entry: R256,F1,B1,L255,E1,P256.
     * 4+256+1+1+510+1+256=1029 packets. */
    char long_name[256]; memset(long_name, 'z', 255); long_name[255] = 0;
    registry = wirelog_extension_registry_create(); CHECK(registry);
    wirelog_extension_descriptor_t descriptor =
    {.abi_version = WIRELOG_EXTENSION_ABI_VERSION, .size = sizeof(descriptor),
     .name = long_name,
     .invoke = invoke, .result_type = WIRELOG_EXTENSION_VALUE_BOOL,
     .user_data = &owner, .destroy = destroy};
    CHECK(!wirelog_extension_register(registry, &descriptor));
    snapshot = wirelog_extension_snapshot_acquire(registry); CHECK(snapshot);
    for (unsigned below = 0; below < 2; ++below) {
        c = begin(UINT64_C(1029)*224-below); d = (void *)&owner; index = 99;
        found = false;
        int rc = wl_extension_control_lookup_attempt(snapshot, long_name, 255,
                &d, &index, &found, c, id(c));
        if (below)CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                d == (void *)&owner && index == 99 && !found);
        else CHECK(!rc && d == &snapshot->entries[0]->descriptor && !index &&
                found); finish(c, rc);
    }
    wirelog_extension_snapshot_release(snapshot);
    CHECK(!wirelog_extension_unregister(registry,
        long_name) && !wirelog_extension_registry_destroy(registry));
}
static unsigned stop_phase, action; static uint64_t stop_progress, successor;
static bool stopped;
static wl_evaluation_control_report_t original;
static void stop(wl_evaluation_control_t *c, unsigned phase, uint64_t progress){
    if (stopped || phase != stop_phase || progress != stop_progress)return;
    stopped = true;
    if (action == 0)wl_evaluation_control_request_cancel(c);
    else if (action == 1)CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else if (action == 2) {
        CHECK(!wl_evaluation_control_finish(c, &owner, 0,
            0) && !wl_evaluation_control_report(c, &original) &&
            !wl_evaluation_control_reset(c) && !wl_evaluation_control_begin(c,
            &owner)); successor = id(c);
    }else {
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
        wl_evaluation_control_request_cancel(c);
    }
}
static int expected_rc(void){
    return action == 0?WL_EVALUATION_CONTROL_CANCELLED:action ==
           1?WL_EVALUATION_CONTROL_WORK_LIMIT:action == 2?EINVAL:EOVERFLOW;
}
static void retry(wl_evaluation_control_t *c, int rc){
    if (action == 2) {
        wl_evaluation_control_report_t report; uint64_t used = 99;
        CHECK(!wl_evaluation_control_report(c,
            &report) && report.attempt_id == original.attempt_id &&
            report.consumed == original.consumed &&
            report.result == original.result &&
            report.stop_reason == original.stop_reason);
        CHECK(!wl_evaluation_control_test_active_consumed(c, successor,
            &used) && !used);
        CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
    }else CHECK(wl_evaluation_control_finish(c, &owner, rc > 0?rc:0, 0) == rc);
    CHECK(!wl_evaluation_control_reset(c) && !wl_evaluation_control_configure(c,
        0) && !wl_evaluation_control_begin(c, &owner));
}
static void lookup_stops(void){
    struct {unsigned phase; uint64_t progress;} cuts[] = {
        {400, 0}, {401, 0}, {401, 1}, {401, 2}, {401, 3}, {402, 0}, {403, 2},
        {404, 3},
        {405, 0}, {405, 1}, {405, 2}, {406, 0}, {406, 1}, {406, 2}, {407, 3},
        {408, 0}, {408, 1}, {408, 2}, {409, 0}, {409, 1},
        {409, UINT64_C(1)<<32}, {409, (UINT64_C(1)<<32)|1},
        {409, (UINT64_C(1)<<32)|2},
        {409, UINT64_C(2)<<32}, {409, (UINT64_C(2)<<32)|1},
        {409, (UINT64_C(2)<<32)|2},
        {409, (UINT64_C(2)<<32)|3}, {410, 0}
    };
    wirelog_extension_registry_t *registry = create();
    wirelog_extension_snapshot_t *snapshot =
        wirelog_extension_snapshot_acquire(registry); CHECK(snapshot);
    for (unsigned cut = 0; cut < sizeof(cuts)/sizeof(*cuts); ++cut)for (action =
            0;
            action < 4; ++action) {
            wl_evaluation_control_t *c = begin(action == 1?UINT64_MAX-1:0);
            const wirelog_extension_descriptor_t *d = (void *)&owner;
            size_t index = 99; bool found = false;
            stop_phase = cuts[cut].phase; stop_progress = cuts[cut].progress;
            stopped = false; successor = 0;
            wl_extension_error_set("preserved");
            wl_extension_control_test_checkpoint = stop;
            int rc = wl_extension_control_lookup_attempt(snapshot, "abc", 3, &d,
                    &index, &found, c, id(c));
            wl_extension_control_test_checkpoint = NULL;
            CHECK(stopped && rc == expected_rc() && d == (void *)&owner &&
                index == 99 && !found &&
                !strcmp(wirelog_extension_last_error(),
                "preserved") && !wl_extension_control_test_live_allocations);
            retry(c, rc);
            CHECK(!wl_extension_control_lookup_attempt(snapshot, "abc", 3, &d,
                &index, &found, c, id(c)) &&
                d == &snapshot->entries[2]->descriptor && index == 2 && found);
            finish(c, 0);
        }
    release(registry, snapshot);
}
static void leases(void){
    unsigned destroyed_before = destroyed;
    wirelog_extension_registry_t *registry = create();
    wirelog_extension_snapshot_t *snapshot =
        wirelog_extension_snapshot_acquire(registry); CHECK(snapshot);
    const wirelog_extension_descriptor_t *d = &snapshot->entries[1]->descriptor;
    size_t refs = snapshot->entries[1]->references;
    for (unsigned prefix = 1; prefix <= 5; ++prefix) {
        wl_evaluation_control_t *c = begin(UINT64_C(224)*prefix-1);
        void *lease = &owner; bool found = true;
        CHECK(wl_extension_control_lease_index_attempt(snapshot, 1, d, &lease,
            &found, c, id(c)) == WL_EVALUATION_CONTROL_WORK_LIMIT &&
            lease == &owner && found &&
            snapshot->entries[1]->references == refs &&
            !registry->active_leases);
        finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
    }
    for (unsigned kind = 0; kind < 5; ++kind) {
        wl_evaluation_control_t *c = begin(1120); void *lease = &owner;
        bool found = true;
        const wirelog_extension_descriptor_t *expected = kind ==
            1?&snapshot->entries[2]->descriptor:d;
        if (kind == 3)snapshot->entries[1]->references = SIZE_MAX;
        if (kind == 4)registry->active_leases = SIZE_MAX;
        int rc = wl_extension_control_lease_index_attempt(snapshot,
                kind == 2?SIZE_MAX:1, expected, &lease, &found, c, id(c));
        if (kind >= 3) {
            CHECK(rc == EOVERFLOW && lease == &owner && found);
            snapshot->entries[1]->references = refs;
            registry->active_leases = 0;
        }else if (kind) CHECK(!rc && !lease && !found &&
                !registry->active_leases);
        else {
            CHECK(!rc && lease == snapshot->entries[1] && found &&
                registry->active_leases == 1 &&
                snapshot->entries[1]->references == refs+1);
            wl_extension_callback_lease_release(lease);
            CHECK(!registry->active_leases &&
                snapshot->entries[1]->references == refs);
        }
        finish(c, rc);
    }
    const unsigned phases[] = {420, 421, 422, 423, 424, 425};
    for (unsigned cut = 0; cut < 6;
        ++cut) for (action = 0; action < 4; ++action) {
            wl_evaluation_control_t *c = begin(action == 1?UINT64_MAX-1:0);
            void *lease = &owner; bool found = false;
            stop_phase = phases[cut]; stop_progress = 0; stopped = false;
            successor = 0; wl_extension_control_test_checkpoint = stop;
            int rc = wl_extension_control_lease_index_attempt(snapshot, 1, d,
                    &lease, &found, c, id(c));
            wl_extension_control_test_checkpoint = NULL;
            CHECK(stopped && rc == expected_rc() && lease == &owner && !found &&
                !registry->active_leases &&
                snapshot->entries[1]->references == refs);
            retry(c, rc);
            CHECK(!wl_extension_control_lease_index_attempt(snapshot, 1, d,
                &lease, &found, c,
                id(c)) && lease == snapshot->entries[1] && found);
            wl_extension_callback_lease_release(lease); finish(c, 0);
        }
    /* Saved-identity/accounting/cancel wins over staged saturation afterunlock. */
    for (action = 0; action < 4; ++action) {
        wl_evaluation_control_t *c = begin(action == 1?UINT64_MAX-1:0);
        void *lease = &owner; bool found = false;
        snapshot->entries[1]->references = SIZE_MAX; stop_phase = 424;
        stop_progress = 0; stopped = false;
        wl_extension_control_test_checkpoint = stop;
        int rc = wl_extension_control_lease_index_attempt(snapshot, 1, d,
                &lease, &found, c, id(c));
        wl_extension_control_test_checkpoint = NULL;
        CHECK(stopped && rc == expected_rc() && lease == &owner && !found &&
            !registry->active_leases &&
            snapshot->entries[1]->references == SIZE_MAX);
        snapshot->entries[1]->references = refs; retry(c, rc); finish(c, 0);
    }
    release(registry, snapshot);
    CHECK(destroyed == destroyed_before+3);
    /* Real lease survives legal unregister plus final snapshot release. */
    registry = wirelog_extension_registry_create(); CHECK(registry);
    destroyed = 0;
    wirelog_extension_descriptor_t descriptor =
    {.abi_version = WIRELOG_EXTENSION_ABI_VERSION, .size = sizeof(descriptor),
     .name = "owned", .invoke = invoke,
     .result_type =
         WIRELOG_EXTENSION_VALUE_BOOL, .user_data = &owner,
     .destroy = destroy};
    CHECK(!wirelog_extension_register(registry, &descriptor));
    snapshot = wirelog_extension_snapshot_acquire(registry); CHECK(snapshot);
    wl_evaluation_control_t *c = begin(1120); void *lease = NULL;
    bool found = false;
    CHECK(!wl_extension_control_lease_index_attempt(snapshot, 0,
        &snapshot->entries[0]->descriptor, &lease, &found, c,
        id(c)) && found && lease);
    CHECK(!wirelog_extension_unregister(registry, "owned"));
    wirelog_extension_snapshot_release(snapshot); CHECK(!destroyed);
    CHECK(wirelog_extension_registry_destroy(registry) == -1 &&
        !strcmp(wirelog_extension_last_error(), "registry is not empty"));
    wl_extension_callback_lease_release(lease);
    CHECK(destroyed == 1 && !wirelog_extension_registry_destroy(registry));
    finish(c, 0);
}

static void trimmed_stops(void){
    wirelog_extension_registry_t *registry = create();
    wirelog_extension_snapshot_t *snapshot =
        wirelog_extension_snapshot_acquire(registry); CHECK(snapshot);
    /* Independent actual offsets of seven-byte space/TAB/abc/CR/LF input. */
    struct {unsigned phase, first, last;
    } ranges[] = {{401, 0, 7}, {402, 0, 2}, {403, 4, 6}, {405, 0, 2},
                  {406, 0, 2}};
    for (unsigned range = 0; range < sizeof(ranges)/sizeof(*ranges); ++range)
        for (unsigned offset =
            ranges[range].first; offset <= ranges[range].last;
            ++offset)
            for (action = 0; action < 4; ++action) {
                wl_evaluation_control_t *c = begin(action == 1?UINT64_MAX-1:0);
                const wirelog_extension_descriptor_t *d = (void *)&owner;
                size_t index = 99; bool found = false;
                stop_phase = ranges[range].phase; stop_progress = offset;
                stopped = false; successor = 0;
                wl_extension_error_set("preserved");
                wl_extension_control_test_checkpoint = stop;
                int rc = wl_extension_control_lookup_attempt(snapshot,
                        " \tabc\r\n", 7, &d, &index, &found, c, id(c));
                wl_extension_control_test_checkpoint = NULL;
                CHECK(stopped && rc == expected_rc() && d == (void *)&owner &&
                    index == 99 && !found &&
                    !strcmp(wirelog_extension_last_error(), "preserved") &&
                    !wl_extension_control_test_live_allocations); retry(c, rc);
                CHECK(!wl_extension_control_lookup_attempt(snapshot,
                    " \tabc\r\n", 7, &d, &index, &found, c,
                    id(c)) && found && index == 2); finish(c, 0);
            }
    /* Descriptor from another authentic live snapshot cannot match this index. */
    wirelog_extension_registry_t *other = create();
    wirelog_extension_snapshot_t *foreign =
        wirelog_extension_snapshot_acquire(other); CHECK(foreign);
    wl_evaluation_control_t *c = begin(1120); void *lease = &owner;
    bool found = true;
    CHECK(!wl_extension_control_lease_index_attempt(snapshot, 0,
        &foreign->entries[0]->descriptor, &lease, &found, c,
        id(c)) && !lease && !found &&
        !registry->active_leases && !other->active_leases); finish(c, 0);
    release(other, foreign);
    c = begin(1120); lease = &owner; found = true;
    CHECK(!wl_extension_control_lease_index_attempt(NULL, 0, NULL, &lease,
        &found, c, id(c)) && !lease && !found); finish(c, 0);
    release(registry, snapshot);
}

int main(void){
    oracles(); lookup_stops(); trimmed_stops(); leases();
    puts("bounded extension lookup and leases: all checks passed"); return 0;
}
