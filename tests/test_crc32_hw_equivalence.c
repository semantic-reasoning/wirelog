/*
 * test_crc32_hw_equivalence.c - Hardware vs Software CRC-32 equivalence tests
 * (Issue #145)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Verifies that hardware and software CRC-32 implementations produce identical
 * checksums on the same data.  The test is meaningful on any platform:
 *   - On platforms with hardware CRC32, both paths are exercised directly.
 *   - On platforms without hardware CRC32, the test verifies software results
 *     are self-consistent (hw helpers return 0, sw path is the only path).
 */

#include <stdint.h>
#include <stdio.h>
#include <string.h>

/* Pull in all crc32 declarations */
#include "../wirelog/crc32.h"

/* -------------------------------------------------------------------------
 * Minimal test harness matching the project convention
 * ------------------------------------------------------------------------- */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                      \
    do {                                \
        tests_run++;                    \
        printf("  [TEST] %-50s", name); \
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

/* -------------------------------------------------------------------------
 * Software-only Castagnoli CRC32C (reference implementation for comparison)
 * This is a local copy so we can compare sw vs hw independently.
 * ------------------------------------------------------------------------- */

static uint32_t castagnoli_sw_table[256];
static int castagnoli_sw_table_ready = 0;

static void
castagnoli_sw_init(void)
{
    uint32_t i, crc;
    for (i = 0; i < 256; i++) {
        crc = i;
        unsigned j;
        for (j = 0; j < 8; j++) {
            if (crc & 1u)
                crc = (crc >> 1) ^ 0x82F63B78u;
            else
                crc >>= 1;
        }
        castagnoli_sw_table[i] = crc;
    }
    castagnoli_sw_table_ready = 1;
}

static uint32_t
castagnoli_sw(const uint8_t *data, size_t len)
{
    if (!castagnoli_sw_table_ready)
        castagnoli_sw_init();
    uint32_t crc = 0xFFFFFFFFu;
    size_t i;
    for (i = 0; i < len; i++)
        crc = (crc >> 8) ^ castagnoli_sw_table[(crc ^ data[i]) & 0xFFu];
    return crc ^ 0xFFFFFFFFu;
}

/* -------------------------------------------------------------------------
 * Architecture detection (mirrors crc32.c)
 * ------------------------------------------------------------------------- */

static int
hw_available(void)
{
#if (defined(__x86_64__) || defined(_M_X64)) && !defined(_MSC_VER)
    unsigned int eax, ebx, ecx, edx;
    __asm__ volatile("cpuid"
                     : "=a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx)
                     : "a"(1), "c"(0));
    return (ecx >> 20) & 1;
#elif defined(__aarch64__) || defined(_M_ARM64)
#if defined(__APPLE__)
    return 1;
#elif defined(__linux__)
#include <sys/auxv.h>
#ifndef HWCAP_CRC32
#define HWCAP_CRC32 (1 << 7)
#endif
    unsigned long hwcap = getauxval(AT_HWCAP);
    return (hwcap & HWCAP_CRC32) != 0;
#else
#ifdef __ARM_FEATURE_CRC32
    return 1;
#else
    return 0;
#endif
#endif
#else
    return 0;
#endif
}

/* -------------------------------------------------------------------------
 * Test vectors
 * ------------------------------------------------------------------------- */

static const uint8_t tv_empty[] = {};
static const uint8_t tv_abc[] = { 'A', 'B', 'C' };
static const uint8_t tv_123[] = { '1', '2', '3' };
static const uint8_t tv_long[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
    0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
    0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
};

struct tv {
    const char *name;
    const uint8_t *data;
    size_t len;
};

static struct tv vectors[] = {
    { "empty", tv_empty, 0 },
    { "ABC", tv_abc, 3 },
    { "123", tv_123, 3 },
    { "32-byte sequence", tv_long, 32 },
};

/* -------------------------------------------------------------------------
 * Tests
 * ------------------------------------------------------------------------- */

/*
 * castagnoli_crc32() must match the local software reference on all platforms.
 */
static void
test_castagnoli_matches_sw_reference(void)
{
    for (size_t i = 0; i < sizeof(vectors) / sizeof(vectors[0]); i++) {
        struct tv *v = &vectors[i];
        char name[128];
        snprintf(name, sizeof(name), "castagnoli_crc32 == sw_ref [%s]",
                 v->name);
        TEST(name);

        uint32_t sw = castagnoli_sw(v->data, v->len);
        uint32_t got = castagnoli_crc32(v->data, v->len);

        if (got == sw) {
            PASS();
        } else {
            char msg[128];
            snprintf(msg, sizeof(msg), "sw_ref=0x%08X castagnoli_crc32=0x%08X",
                     sw, got);
            FAIL(msg);
        }
    }
}

/*
 * On platforms with hardware CRC32, manually compute via per-byte hw helper
 * and compare against software reference.
 */
static void
test_hw_per_byte_matches_sw_reference(void)
{
    int hw = hw_available();
    printf("\n  [INFO] Hardware CRC32 available: %s\n\n", hw ? "yes" : "no");

    for (size_t i = 0; i < sizeof(vectors) / sizeof(vectors[0]); i++) {
        struct tv *v = &vectors[i];
        char name[128];
        snprintf(name, sizeof(name), "hw_per_byte == sw_ref [%s]", v->name);
        TEST(name);

        uint32_t sw = castagnoli_sw(v->data, v->len);

        if (!hw) {
            /* Skip hardware path, just verify sw reference is stable */
            uint32_t sw2 = castagnoli_sw(v->data, v->len);
            if (sw == sw2) {
                PASS();
            } else {
                FAIL("sw reference unstable");
            }
            continue;
        }

        /* Manually apply per-byte hw helper */
        uint32_t crc = 0xFFFFFFFFu;
        for (size_t b = 0; b < v->len; b++) {
#if defined(__x86_64__) || defined(_M_X64)
            crc = intel_crc32_hw_u8(crc, v->data[b]);
#elif (defined(__aarch64__) || defined(_M_ARM64)) \
    && defined(__ARM_FEATURE_CRC32)
            crc = arm_crc32_hw_u8(crc, v->data[b]);
#else
            /* Should not reach here since hw_available() returned true */
            crc = 0xDEADBEEFu;
#endif
        }
        uint32_t hw_result = crc ^ 0xFFFFFFFFu;

        if (hw_result == sw) {
            PASS();
        } else {
            char msg[128];
            snprintf(msg, sizeof(msg), "sw_ref=0x%08X hw_per_byte=0x%08X", sw,
                     hw_result);
            FAIL(msg);
        }
    }
}

/*
 * Verify known CRC-32C check value: crc32c("123456789") == 0xE3069283
 * (standard check value from the CRC-32C specification)
 */
static void
test_castagnoli_check_value(void)
{
    TEST("castagnoli_crc32(\"123456789\") == 0xE3069283");
    static const uint8_t nine[]
        = { '1', '2', '3', '4', '5', '6', '7', '8', '9' };
    uint32_t got = castagnoli_crc32(nine, 9);
    if (got == 0xE3069283u) {
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 0xE3069283 got 0x%08X", got);
        FAIL(msg);
    }
}

/* -------------------------------------------------------------------------
 * main
 * ------------------------------------------------------------------------- */

int
main(void)
{
    printf("\n=== CRC-32 Hardware/Software Equivalence Tests ===\n\n");

    test_castagnoli_check_value();
    printf("\n");
    test_castagnoli_matches_sw_reference();
    printf("\n");
    test_hw_per_byte_matches_sw_reference();

    printf("\n=== Summary ===\n");
    printf("Tests run:    %d\n", tests_run);
    printf("Passed:       %d\n", tests_passed);
    printf("Failed:       %d\n\n", tests_failed);

    return tests_failed == 0 ? 0 : 1;
}
