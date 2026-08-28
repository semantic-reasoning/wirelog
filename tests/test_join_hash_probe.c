/*
 * test_join_hash_probe.c - measured join-hash probe-chain validation.
 *
 * This is deliberately a test-side model of the scalar kc <= 2 join hash
 * table.  It uses the same two-word FNV-1a value hashing as
 * wirelog/columnar/join.c, but does not change the production hash path.
 * The fixture pairs are the structured CRDT and CSPA key distributions used
 * by the benchmark suite (Issue #807).
 */

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define JOIN_HASH_REPETITIONS 5

struct key_set {
    int64_t *values;
    size_t count;
};

struct hash_slot {
    int64_t key;
    uint32_t count;
    unsigned char used;
};

struct probe_metrics {
    uint64_t insert_probes;
    uint64_t lookup_probes;
    uint32_t max_insert_probe;
    uint32_t max_lookup_probe;
    uint64_t collisions;
    uint64_t join_rows;
    uint32_t capacity;
    double elapsed_ms;
};

static double
wall_now_ms(void)
{
    struct timespec timestamp;
    if (timespec_get(&timestamp, TIME_UTC) == 0)
        return 0.0;
    return (double)timestamp.tv_sec * 1000.0
           + (double)timestamp.tv_nsec / 1000000.0;
}

static int
compare_double(const void *left, const void *right)
{
    double a = *(const double *)left;
    double b = *(const double *)right;
    return a < b ? -1 : a > b;
}

static uint32_t
hash_key(int64_t key)
{
    uint64_t value = (uint64_t)key;
    uint32_t hash = UINT32_C(2166136261);
    hash ^= (uint32_t)value;
    hash *= UINT32_C(16777619);
    hash ^= (uint32_t)(value >> 32);
    hash *= UINT32_C(16777619);
    return hash;
}

static int
read_keys(const char *path, struct key_set *out)
{
    FILE *file = fopen(path, "rb");
    if (!file)
        return errno;
    size_t capacity = 1024;
    int64_t *values = (int64_t *)malloc(capacity * sizeof(*values));
    if (!values) {
        fclose(file);
        return ENOMEM;
    }
    char line[4096];
    size_t count = 0;
    while (fgets(line, sizeof(line), file)) {
        char *end = NULL;
        errno = 0;
        int64_t value = (int64_t)strtoll(line, &end, 10);
        if (end == line || errno == ERANGE) {
            free(values);
            fclose(file);
            return EINVAL;
        }
        if (count == capacity) {
            size_t next = capacity * 2;
            int64_t *grown = (int64_t *)realloc(values,
                    next * sizeof(*grown));
            if (!grown) {
                free(values);
                fclose(file);
                return ENOMEM;
            }
            values = grown;
            capacity = next;
        }
        values[count++] = value;
    }
    if (ferror(file)) {
        free(values);
        fclose(file);
        return EIO;
    }
    fclose(file);
    out->values = values;
    out->count = count;
    return 0;
}

static int
compare_i64(const void *left, const void *right)
{
    int64_t a = *(const int64_t *)left;
    int64_t b = *(const int64_t *)right;
    return a < b ? -1 : a > b;
}

static uint64_t
sorted_join_rows(const struct key_set *build, const struct key_set *probe)
{
    int64_t *left = (int64_t *)malloc(build->count * sizeof(*left));
    int64_t *right = (int64_t *)malloc(probe->count * sizeof(*right));
    if ((!left && build->count) || (!right && probe->count)) {
        free(left);
        free(right);
        return UINT64_MAX;
    }
    memcpy(left, build->values, build->count * sizeof(*left));
    memcpy(right, probe->values, probe->count * sizeof(*right));
    qsort(left, build->count, sizeof(*left), compare_i64);
    qsort(right, probe->count, sizeof(*right), compare_i64);
    size_t i = 0;
    size_t j = 0;
    uint64_t rows = 0;
    while (i < build->count && j < probe->count) {
        if (left[i] < right[j]) {
            i++;
            continue;
        }
        if (left[i] > right[j]) {
            j++;
            continue;
        }
        int64_t key = left[i];
        size_t i_end = i;
        size_t j_end = j;
        while (i_end < build->count && left[i_end] == key)
            i_end++;
        while (j_end < probe->count && right[j_end] == key)
            j_end++;
        rows += (uint64_t)(i_end - i) * (uint64_t)(j_end - j);
        i = i_end;
        j = j_end;
    }
    free(left);
    free(right);
    return rows;
}

static int
measure_join(const struct key_set *build, const struct key_set *probe,
    struct probe_metrics *metrics)
{
    uint64_t desired = build->count > UINT32_MAX / 2
        ? UINT32_MAX : (uint64_t)build->count * 2;
    uint32_t capacity = 1;
    while ((uint64_t)capacity < (desired ? desired : 1)) {
        if (capacity > UINT32_MAX / 2)
            return EOVERFLOW;
        capacity <<= 1;
    }
    struct hash_slot *slots = (struct hash_slot *)calloc(capacity,
            sizeof(*slots));
    if (!slots)
        return ENOMEM;

    double started = wall_now_ms();
    for (size_t i = 0; i < build->count; i++) {
        uint32_t slot = hash_key(build->values[i]) & (capacity - 1);
        uint32_t probes = 1;
        while (slots[slot].used && slots[slot].key != build->values[i]) {
            metrics->collisions++;
            slot = (slot + 1) & (capacity - 1);
            probes++;
        }
        if (!slots[slot].used) {
            slots[slot].used = 1;
            slots[slot].key = build->values[i];
        }
        slots[slot].count++;
        metrics->insert_probes += probes;
        if (probes > metrics->max_insert_probe)
            metrics->max_insert_probe = probes;
    }
    for (size_t i = 0; i < probe->count; i++) {
        uint32_t slot = hash_key(probe->values[i]) & (capacity - 1);
        uint32_t probes = 1;
        while (slots[slot].used && slots[slot].key != probe->values[i]) {
            slot = (slot + 1) & (capacity - 1);
            probes++;
        }
        if (slots[slot].used)
            metrics->join_rows += slots[slot].count;
        metrics->lookup_probes += probes;
        if (probes > metrics->max_lookup_probe)
            metrics->max_lookup_probe = probes;
    }
    metrics->capacity = capacity;
    metrics->elapsed_ms = wall_now_ms() - started;
    free(slots);
    return 0;
}

static int
run_case(const char *name, const char *build_path, const char *probe_path)
{
    struct key_set build = { NULL, 0 };
    struct key_set probe = { NULL, 0 };
    int rc = read_keys(build_path, &build);
    if (rc == 0)
        rc = read_keys(probe_path, &probe);
    if (rc != 0) {
        fprintf(stderr, "%s: unable to read fixtures (%s)\n", name,
            strerror(rc));
        free(build.values);
        free(probe.values);
        return 1;
    }
    uint64_t expected = sorted_join_rows(&build, &probe);
    struct probe_metrics metrics = { 0 };
    double wall_samples[JOIN_HASH_REPETITIONS];
    for (size_t repeat = 0; repeat < JOIN_HASH_REPETITIONS; repeat++) {
        struct probe_metrics sample = { 0 };
        rc = measure_join(&build, &probe, &sample);
        if (rc != 0 || expected == UINT64_MAX
            || sample.join_rows != expected) {
            metrics = sample;
            break;
        }
        if (repeat == 0)
            metrics = sample;
        wall_samples[repeat] = sample.elapsed_ms;
    }
    if (rc != 0 || expected == UINT64_MAX
        || metrics.join_rows != expected) {
        fprintf(stderr,
            "%s: tuple preservation failed (hash=%" PRIu64
            ", sorted=%" PRIu64 ")\n",
            name, metrics.join_rows, expected);
        free(build.values);
        free(probe.values);
        return 1;
    }
    qsort(wall_samples, JOIN_HASH_REPETITIONS, sizeof(*wall_samples),
        compare_double);
    metrics.elapsed_ms = wall_samples[JOIN_HASH_REPETITIONS / 2];
    printf("join_hash_probe\t%s\tbuild_rows=%zu\tprobe_rows=%zu"
        "\ttable_size=%" PRIu32 "\tload=%.3f"
        "\tinsert_avg=%.3f\tlookup_avg=%.3f"
        "\tmax_insert=%" PRIu32 "\tmax_lookup=%" PRIu32
        "\tcollisions=%" PRIu64 "\tjoin_rows=%" PRIu64
        "\trepetitions=%d\tmedian_wall_ms=%.3f\n",
        name, build.count, probe.count, metrics.capacity,
        metrics.capacity ? (double)build.count / metrics.capacity : 0.0,
        build.count ? (double)metrics.insert_probes / build.count : 0.0,
        probe.count ? (double)metrics.lookup_probes / probe.count : 0.0,
        metrics.max_insert_probe, metrics.max_lookup_probe,
        metrics.collisions, metrics.join_rows, JOIN_HASH_REPETITIONS,
        metrics.elapsed_ms);
    free(build.values);
    free(probe.values);
    return 0;
}

int
main(void)
{
    const char *crdt_dir = getenv("WIRELOG_JOIN_HASH_CRDT_DIR");
    const char *cspa_dir = getenv("WIRELOG_JOIN_HASH_CSPA_DIR");
    if (!crdt_dir || !cspa_dir) {
        fprintf(stderr,
            "WIRELOG_JOIN_HASH_CRDT_DIR and WIRELOG_JOIN_HASH_CSPA_DIR "
            "are required\n");
        return 2;
    }
    char crdt_build[4096];
    char crdt_probe[4096];
    char cspa_build[4096];
    char cspa_probe[4096];
    if (snprintf(crdt_build, sizeof(crdt_build), "%s/Insert_input.csv",
        crdt_dir) >= (int)sizeof(crdt_build)
        || snprintf(crdt_probe, sizeof(crdt_probe), "%s/Remove_input.csv",
        crdt_dir) >= (int)sizeof(crdt_probe)
        || snprintf(cspa_build, sizeof(cspa_build), "%s/assign.csv",
        cspa_dir) >= (int)sizeof(cspa_build)
        || snprintf(cspa_probe, sizeof(cspa_probe), "%s/dereference.csv",
        cspa_dir) >= (int)sizeof(cspa_probe))
        return 2;
    return run_case("crdt", crdt_build, crdt_probe)
           || run_case("cspa", cspa_build, cspa_probe);
}
