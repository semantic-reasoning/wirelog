/*
 * test_crc32_castagnoli.c - CRC-32C (Castagnoli) Unit Tests (Issue #145)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * TDD RED phase: Tests for CRC-32C (Castagnoli) with known test vectors
 * These tests FAIL until US-145-003 is implemented.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Forward declaration - to be implemented in wirelog/crc32.c */
extern uint32_t
castagnoli_crc32(const uint8_t *data, size_t len);

/* Test vectors from iSCSI standard */
struct test_vector {
    const char *name;
    const uint8_t *data;
    size_t len;
    uint32_t expected;
};

/* CRC-32C test vectors (Castagnoli, poly=0x1EDC6F41 reflected=0x82F63B78)
 * init=0xFFFFFFFF, xorout=0xFFFFFFFF, refin=true, refout=true
 * Verified against standard check value: crc32c("123456789") == 0xE3069283
 */
static const uint8_t test_data_abc[] = { 'A', 'B', 'C' };
static const uint8_t test_data_123[] = { '1', '2', '3' };
static const uint8_t test_data_empty[] = {};

/* Test cases */
static struct test_vector vectors[] = { { .name = "iSCSI ABC",
                                          .data = test_data_abc,
                                          .len = 3,
                                          .expected = 0x8839A97F },
                                        { .name = "iSCSI 123",
                                          .data = test_data_123,
                                          .len = 3,
                                          .expected = 0x107B2FB2 },
                                        { .name = "Empty",
                                          .data = test_data_empty,
                                          .len = 0,
                                          .expected = 0x00000000 } };

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                      \
    do {                                \
        tests_run++;                    \
        printf("  [TEST] %-40s", name); \
        fflush(stdout);                 \
    } while (0)

#define PASS()             \
    do {                   \
        tests_passed++;    \
        printf(" PASS\n"); \
    } while (0)

#define FAIL(msg)                   \
    do {                            \
        tests_failed++;             \
        printf(" FAIL: %s\n", msg); \
    } while (0)

int
main(void)
{
    printf("\n=== CRC-32C (Castagnoli) Tests ===\n\n");

    for (size_t i = 0; i < sizeof(vectors) / sizeof(vectors[0]); i++) {
        struct test_vector *v = &vectors[i];
        TEST(v->name);

        uint32_t result = castagnoli_crc32(v->data, v->len);

        if (result == v->expected) {
            PASS();
        } else {
            char msg[128];
            snprintf(msg, sizeof(msg), "Expected 0x%08X, got 0x%08X",
                     v->expected, result);
            FAIL(msg);
        }
    }

    printf("\n=== Summary ===\n");
    printf("Tests run: %d\n", tests_run);
    printf("Passed: %d\n", tests_passed);
    printf("Failed: %d\n\n", tests_failed);

    return tests_failed == 0 ? 0 : 1;
}
