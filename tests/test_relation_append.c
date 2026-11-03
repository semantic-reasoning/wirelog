/*
 * test_relation_append.c - generic relation append overflow regression
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "columnar/internal.h"

#include <errno.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>

static int tests_failed;

static void
check_bits(bool ok, const char *message)
{
    if (!ok) {
        printf("  FAIL: %s\n", message);
        tests_failed++;
    }
}

static void
check_float_pair(uint64_t left_raw, uint64_t right_raw)
{
    int64_t left_bits, right_bits;
    double left, right;
    /* Independent numeric oracle, not the bit-ordering implementation. */
    memcpy(&left_bits, &left_raw, sizeof(left_bits));
    memcpy(&right_bits, &right_raw, sizeof(right_bits));
    memcpy(&left, &left_raw, sizeof(left));
    memcpy(&right, &right_raw, sizeof(right));
    bool valid = isfinite(left) && isfinite(right);
    int expected = valid ? (left < right ? -1 : left > right ? 1 : 0)
                         : WL_COLUMNAR_CMP_INCOMPATIBLE;
    check_bits(wl_columnar_float_bits_valid(left_bits) == !!isfinite(left),
        "binary64 validity differs from numeric oracle");
    check_bits(wl_columnar_float_bits_zero(left_bits) == (left == 0.0),
        "binary64 zero predicate differs from numeric oracle");
    check_bits(wl_columnar_float_canonical_bits(left_bits)
        == (left == 0.0 ? UINT64_C(0) : left_raw),
        "canonicalization changed a nonzero encoding");
    check_bits(wl_columnar_float_compare_bits(left_bits,
        right_bits) == expected,
        "binary64 ordering differs from numeric oracle");
    if (valid)
        check_bits(wl_columnar_float_compare_bits(right_bits, left_bits)
            == -expected,
            "binary64 ordering is not antisymmetric");
}

static void
test_float_bit_semantics(void)
{
    static const uint64_t patterns[] = {
        UINT64_C(0), UINT64_C(0x8000000000000000),
        UINT64_C(1), UINT64_C(0x8000000000000001),
        UINT64_C(0x000fffffffffffff), UINT64_C(0x800fffffffffffff),
        UINT64_C(0x0010000000000000), UINT64_C(0x8010000000000000),
        UINT64_C(0x3ff0000000000000), UINT64_C(0xbff0000000000000),
        UINT64_C(0x7fefffffffffffff), UINT64_C(0xffefffffffffffff),
        UINT64_C(0x7ff0000000000000), UINT64_C(0xfff0000000000000),
        UINT64_C(0x7ff0000000000001), UINT64_C(0xfff0000000000001),
        UINT64_C(0x7ff8000000000000), UINT64_C(0xfff8000000000000),
        UINT64_C(0x7fffffffffffffff), UINT64_C(0xffffffffffffffff)
    };
    int before = tests_failed;
    for (size_t i = 0; i < sizeof(patterns) / sizeof(patterns[0]); i++) {
        for (size_t j = 0; j < sizeof(patterns) / sizeof(patterns[0]); j++)
            check_float_pair(patterns[i], patterns[j]);
    }
    uint64_t state = UINT64_C(0x123456789abcdef);
    for (unsigned i = 0; i < 8192; i++) {
        state = state * UINT64_C(6364136223846793005) + 1;
        uint64_t left = state;
        state = state * UINT64_C(6364136223846793005) + 1;
        check_float_pair(left, state);
    }
    printf("  binary64 edge patterns and seeded numeric oracle: %s\n",
        tests_failed == before ? "PASS" : "FAIL");
}

/* Side-compound intern IDs and legacy integer producers share a physical
 * lane with declared symbols. Join admission accepts these pairs, so equality
 * and hashing must agree without accepting float encodings as integer IDs. */
static void
test_integer_family_keys(void)
{
    wirelog_column_type_t types[] = {
        WIRELOG_TYPE_INT32, WIRELOG_TYPE_INT64, WIRELOG_TYPE_UINT32,
        WIRELOG_TYPE_UINT64, WIRELOG_TYPE_STRING, WIRELOG_TYPE_BOOL
    };
    wirelog_column_type_t floating = WIRELOG_TYPE_FLOAT;
    col_rel_t float_rel = { .column_types = &floating };
    const int64_t values[] = { 0, 1, 107, INT64_MAX, INT64_MIN };
    for (unsigned i = 0; i <= sizeof(types) / sizeof(types[0]); i++) {
        col_rel_t left = { .column_types = i < sizeof(types) / sizeof(types[0])
                ? &types[i] : NULL };
        for (unsigned j = 0; j <= sizeof(types) / sizeof(types[0]); j++) {
            col_rel_t right = { .column_types = j <
                                    sizeof(types) / sizeof(types[0])
                    ? &types[j] : NULL };
            for (unsigned k = 0; k < sizeof(values) / sizeof(values[0]); k++) {
                int64_t value = values[k];
                check_bits(wl_columnar_value_equal(&left, 0, value,
                    &right, 0, value), "equal integer-family keys differ");
                check_bits(!wl_columnar_value_equal(&left, 0, value,
                    &right, 0, value ^ 1),
                    "different integer-family keys match");
                check_bits(wl_columnar_hash_value(2166136261u, &left, 0, value)
                    == wl_columnar_hash_value(2166136261u, &right, 0, value),
                    "equal integer-family keys hash differently");
            }
        }
        check_bits(!wl_columnar_value_equal(&left, 0, 0, &float_rel, 0, 0)
            && !wl_columnar_value_equal(&float_rel, 0, 0, &left, 0, 0),
            "float zero matched an integer-family zero");
    }
    check_bits(wl_columnar_value_equal(&float_rel, 0, INT64_MIN,
        &float_rel, 0, 0), "float signed zeros differ");
    check_bits(wl_columnar_hash_value(2166136261u, &float_rel, 0, INT64_MIN)
        == wl_columnar_hash_value(2166136261u, &float_rel, 0, 0),
        "float signed zeros hash differently");
}

static void
test_float_write_boundaries(void)
{
    int before = tests_failed;
    wirelog_column_type_t types[] = { WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT };
    col_rel_t *dst = col_rel_new_auto("bits_dst", 2);
    col_rel_t *src = col_rel_new_auto("bits_src", 2);
    if (!dst || !src || col_rel_set_column_types(dst, types, 2) != 0
        || col_rel_set_column_types(src, types, 2) != 0) {
        check_bits(false, "allocate typed bit regression relations");
        col_rel_destroy(dst);
        col_rel_destroy(src);
        return;
    }
    int64_t one = wl_columnar_float_to_bits(1.0);
    int64_t initial[] = { 17, one };
    int64_t invalid[] = { 99, wl_columnar_float_to_bits(INFINITY) };
    int64_t zeros[] = { INT64_MIN, INT64_MIN };
    check_bits(col_rel_append_row(dst, initial) == 0, "append initial row");
    check_bits(col_rel_set(dst, 0, 1, invalid[1]) == EINVAL
        && dst->columns[1][0] == one,
        "invalid float cell write changed storage");
    check_bits(col_rel_row_copy_in(dst, 0, invalid) == EINVAL
        && dst->columns[0][0] == 17 && dst->columns[1][0] == one,
        "invalid second lane partially changed the row");
    check_bits(col_rel_append_row(dst, invalid) == EINVAL && dst->nrows == 1,
        "invalid row append changed row count");
    check_bits(col_rel_row_copy_in(dst, 0, zeros) == 0
        && dst->columns[0][0] == INT64_MIN && dst->columns[1][0] == 0,
        "row copy must normalize float zero but preserve integer INT64_MIN");
    check_bits(col_rel_set(dst, 0, 0, INT64_MIN) == 0
        && col_rel_set(dst, 0, 1, INT64_MIN) == 0
        && dst->columns[0][0] == INT64_MIN && dst->columns[1][0] == 0,
        "cell writes must preserve integer bits and normalize float zero");
    check_bits(wl_columnar_value_key(dst, 0, INT64_MIN)
        == UINT64_C(0x8000000000000000)
        && wl_columnar_value_key(dst, 1, INT64_MIN) == 0
        && wl_columnar_hash_value(2166136261u, dst, 1, INT64_MIN)
        == wl_columnar_hash_value(2166136261u, dst, 1, 0)
        && wl_columnar_value_equal(dst, 1, INT64_MIN, dst, 1, 0)
        && !wl_columnar_value_equal(dst, 0, 0, dst, 1, 0),
        "typed equality and hashing must preserve lane semantics");
    check_bits(col_rel_append_row(src, zeros) == 0
        && src->columns[0][0] == INT64_MIN && src->columns[1][0] == 0,
        "append row signed-zero normalization");
    /* Simulate an older typed producer that retained negative zero. */
    src->columns[1][0] = INT64_MIN;
    check_bits(col_rel_set_column_types(src, types, 2) == 0
        && src->columns[0][0] == INT64_MIN && src->columns[1][0] == 0,
        "metadata refresh signed-zero normalization");
    src->columns[1][0] = INT64_MIN;
    check_bits(col_rel_append_all(dst, src, NULL) == 0 && dst->nrows == 2
        && dst->columns[0][1] == INT64_MIN && dst->columns[1][1] == 0,
        "bulk append signed-zero normalization");
    src->columns[1][0] = invalid[1];
    check_bits(col_rel_append_all(dst, src, NULL) == EINVAL
        && dst->nrows == 2 && dst->columns[1][1] == 0,
        "invalid bulk append changed destination");
    wirelog_column_type_t *old_types = src->column_types;
    check_bits(col_rel_set_column_types(src, types, 2) == EINVAL
        && src->column_types == old_types && src->columns[1][0] == invalid[1],
        "invalid metadata refresh changed data or metadata");
    col_rel_destroy(dst);
    col_rel_destroy(src);
    printf("  typed cell/row/metadata/bulk bit boundaries: %s\n",
        tests_failed == before ? "PASS" : "FAIL");
}

static void
test_float_storage_validation(void)
{
    printf("  float lanes reject non-finite values and canonicalize -0: ");
    col_rel_t *r = col_rel_new_auto("float", 1);
    wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
    int64_t nan_bits = wl_columnar_float_to_bits(NAN);
    int64_t neg_zero_bits = wl_columnar_float_to_bits(-0.0);
    if (!r || col_rel_set_column_types(r, &type, 1) != 0
        || col_rel_append_row(r, &nan_bits) != EINVAL
        || r->nrows != 0
        || col_rel_append_row(r, &neg_zero_bits) != 0
        || r->nrows != 1
        || r->columns[0][0] != wl_columnar_float_to_bits(0.0)) {
        printf("FAIL\n");
        tests_failed++;
    } else {
        printf("PASS\n");
    }
    col_rel_destroy(r);

    printf(
        "  setting float metadata validates existing rows transactionally: ");
    r = col_rel_new_auto("metadata", 1);
    int64_t invalid = wl_columnar_float_to_bits(INFINITY);
    if (!r) {
        printf("FAIL\n");
        tests_failed++;
    } else {
        r->columns[0][0] = invalid;
        r->nrows = 1;
        int rc = col_rel_set_column_types(r, &type, 1);
        if (rc != EINVAL || r->column_types != NULL || r->nrows != 1
            || r->columns[0][0] != invalid) {
            printf("FAIL\n");
            tests_failed++;
        } else {
            printf("PASS\n");
        }
        col_rel_destroy(r);
    }

    printf("  untyped integer rows cannot be reinterpreted as float: ");
    r = col_rel_new_auto("reinterpret", 1);
    int64_t integer = 1;
    if (!r || col_rel_append_row(r, &integer) != 0
        || col_rel_set_column_types(r, &type, 1) != EINVAL
        || r->column_types != NULL || r->columns[0][0] != integer) {
        printf("FAIL\n");
        tests_failed++;
    } else {
        printf("PASS\n");
    }
    col_rel_destroy(r);
}

static void
test_row_count_boundaries(void)
{
    col_rel_t dst = { 0 };
    col_rel_t src = { 0 };

    printf("  exact UINT32_MAX row total: ");
    dst.capacity = UINT32_MAX;
    dst.nrows = UINT32_MAX - 1u;
    src.nrows = 1;
    if (col_rel_append_all(&dst, &src, NULL) != 0
        || dst.nrows != UINT32_MAX) {
        printf("FAIL\n");
        tests_failed++;
    } else {
        printf("PASS\n");
    }

    printf("  overflowing row total leaves destination unchanged: ");
    dst.nrows = UINT32_MAX;
    src.nrows = 1;
    int64_t **columns = (int64_t **)(uintptr_t)0x1;
    col_delta_timestamp_t *timestamps
        = (col_delta_timestamp_t *)(uintptr_t)0x2;
    bool *col_shared = (bool *)(uintptr_t)0x3;
    wl_mem_ledger_t *mem_ledger = (wl_mem_ledger_t *)(uintptr_t)0x4;
    dst.columns = columns;
    dst.timestamps = timestamps;
    dst.col_shared = col_shared;
    dst.mem_ledger = mem_ledger;
    dst.arena_owned = true;
    if (col_rel_append_all(&dst, &src, NULL) != EOVERFLOW
        || dst.nrows != UINT32_MAX || dst.capacity != UINT32_MAX
        || dst.columns != columns || dst.timestamps != timestamps
        || dst.col_shared != col_shared || dst.mem_ledger != mem_ledger
        || !dst.arena_owned) {
        printf("FAIL\n");
        tests_failed++;
    } else {
        printf("PASS\n");
    }
}

int
main(void)
{
    printf("=== col_rel_append_all overflow tests (Issue #1200) ===\n");
    test_integer_family_keys();
    test_float_bit_semantics();
    test_float_write_boundaries();
    test_float_storage_validation();
    test_row_count_boundaries();
    return tests_failed != 0;
}
