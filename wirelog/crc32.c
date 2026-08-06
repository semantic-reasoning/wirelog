/*
 * wirelog/crc32.c - CRC-32 variant implementations (Issue #145)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Implements CRC-32 lookup-table algorithms for two variants:
 *   - Ethernet (IEEE 802.3): poly=0x04C11DB7, reflected
 *   - Castagnoli (iSCSI/SCTP): poly=0x1EDC6F41, reflected
 *
 * Both use init=0xFFFFFFFF and final XOR=0xFFFFFFFF per their standards.
 * Empty input returns 0x00000000 (0xFFFFFFFF XOR 0xFFFFFFFF).
 *
 * Hardware acceleration is used when available:
 *   - Intel x86-64: SSE4.2 CRC32 instruction (CPUID leaf 1, ECX bit 20)
 *   - ARM64: CRC32 extension (HWCAP_CRC32 or __ARM_FEATURE_CRC32)
 * Falls back to software lookup table when hardware is not available.
 */

#include "crc32.h"

#include <stdio.h>

/* -------------------------------------------------------------------------
 * Hardware acceleration detection and implementation
 * ------------------------------------------------------------------------- */

/* Hardware intrinsics headers (GCC/Clang only) */
#if (defined(__x86_64__) || defined(_M_X64)) && !defined(_MSC_VER)
#define WL_HAVE_X86_CRC32_INTRINSICS 1
#include <nmmintrin.h> /* _mm_crc32_u8 */
#elif (defined(__aarch64__) || defined(_M_ARM64)) && !defined(_MSC_VER) && \
    defined(__ARM_FEATURE_CRC32)
#define WL_HAVE_ARM_CRC32_INTRINSICS 1
#include <arm_acle.h> /* __crc32cb */
#endif

/* MSVC spells it differently and rejects the GNU attribute outright. */
#if defined(_MSC_VER)
#define WL_CRC32_NOINLINE __declspec(noinline)
#elif defined(__GNUC__) || defined(__clang__)
#define WL_CRC32_NOINLINE __attribute__((noinline))
#else
#define WL_CRC32_NOINLINE
#endif

/* Runtime detection: returns 1 if hardware CRC32 is available */
static int
crc32_hw_available(void)
{
#if defined(WL_HAVE_X86_CRC32_INTRINSICS)
    /* CPUID leaf 1, ECX bit 20 = SSE4.2 (includes CRC32 instruction) */
    unsigned int eax, ebx, ecx, edx;
    __asm__ volatile ("cpuid"
    : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)
    : "a" (1), "c" (0));
    return ((ecx >> 20) & 1U) != 0U;
#elif defined(WL_HAVE_ARM_CRC32_INTRINSICS)
#if defined(__APPLE__)
    /* Apple Silicon always has CRC32 extension */
    return 1;
#elif defined(__linux__)
    /* Linux: check HWCAP_CRC32 via getauxval */
#include <sys/auxv.h>
#ifndef HWCAP_CRC32
#define HWCAP_CRC32 (1 << 7)
#endif
    unsigned long hwcap = getauxval(AT_HWCAP);
    return (hwcap & HWCAP_CRC32) != 0;
#else
    /* Conservative: the intrinsic path is only compiled when the feature exists. */
    return 1;
#endif
#else
    return 0;
#endif
}

/* Cached result: -1 = not checked, 0 = unavailable, 1 = available */
static int g_hw_crc32_available = -1;

static int
hw_crc32_check(void)
{
    if (g_hw_crc32_available < 0)
        g_hw_crc32_available = crc32_hw_available();
    return g_hw_crc32_available;
}

/*
 * intel_crc32_hw_u8 - process one byte using Intel SSE4.2 CRC32 instruction
 * Castagnoli polynomial (0x82F63B78 reflected), same polynomial used by
 * Intel hardware for _mm_crc32_u8.
 */
uint32_t
intel_crc32_hw_u8(uint32_t crc, uint8_t byte)
{
#if defined(WL_HAVE_X86_CRC32_INTRINSICS)
    return (uint32_t)_mm_crc32_u8((unsigned int)crc, byte);
#else
    (void)crc;
    (void)byte;
    return 0;
#endif
}

/*
 * arm_crc32_hw_u8 - process one byte using ARM CRC32 extension instruction
 * Castagnoli polynomial, __crc32cb instruction.
 */
uint32_t
arm_crc32_hw_u8(uint32_t crc, uint8_t byte)
{
#if defined(WL_HAVE_ARM_CRC32_INTRINSICS)
    return __crc32cb(crc, byte);
#else
    (void)crc;
    (void)byte;
    return 0;
#endif
}

/* -------------------------------------------------------------------------
 * Ethernet CRC-32 (IEEE 802.3 / ISO-HDLC)
 * Polynomial: 0x04C11DB7, reflected form: 0xEDB88320
 * ------------------------------------------------------------------------- */

static uint32_t ethernet_table[256];
static int ethernet_table_ready = 0;

static void
ethernet_crc32_init(void)
{
    uint32_t i, crc;
    for (i = 0; i < 256; i++) {
        crc = i;
        unsigned j;
        for (j = 0; j < 8; j++) {
            if (crc & 1u)
                crc = (crc >> 1) ^ 0xEDB88320u;
            else
                crc >>= 1;
        }
        ethernet_table[i] = crc;
    }
    ethernet_table_ready = 1;
}

uint32_t
ethernet_crc32(const uint8_t *data, size_t len)
{
    /*
     * Ethernet uses the IEEE 802.3 polynomial (0xEDB88320 reflected).
     * Intel/ARM hardware CRC32 instructions use the Castagnoli polynomial
     * (0x82F63B78 reflected), which is a different polynomial.
     * Therefore Ethernet CRC-32 always uses the software lookup table.
     */
    if (!ethernet_table_ready)
        ethernet_crc32_init();

    uint32_t crc = 0xFFFFFFFFu;
    size_t i;
    for (i = 0; i < len; i++)
        crc = (crc >> 8) ^ ethernet_table[(crc ^ data[i]) & 0xFFu];
    return crc ^ 0xFFFFFFFFu;
}

/* -------------------------------------------------------------------------
 * Castagnoli CRC-32C (iSCSI / SCTP)
 * Polynomial: 0x1EDC6F41, reflected form: 0x82F63B78
 * ------------------------------------------------------------------------- */

static uint32_t castagnoli_table[256];
static int castagnoli_table_ready = 0;

static void
castagnoli_crc32_init(void)
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
        castagnoli_table[i] = crc;
    }
    castagnoli_table_ready = 1;
}

/*
 * noinline is load-bearing, not a style choice.  Under clang 18 with LTO
 * and AddressSanitizer, inlining this function across the TU boundary into
 * col_eval_expr_run() produces a copy whose references to this file's
 * statics -- castagnoli_table, castagnoli_table_ready, and the cached
 * g_hw_crc32_available behind hw_crc32_check() -- resolve to unmapped
 * addresses, and the very first read of one segfaults.  ethernet_crc32(),
 * which is not inlined at that call site, reads its own statics correctly
 * on the identical arguments.  Bisected on CI: forcing the software path
 * did not help, a global-free rewrite did, and restoring the table with
 * only this attribute added also does.  See #970.
 */
WL_CRC32_NOINLINE uint32_t
castagnoli_crc32(const uint8_t *data, size_t len)
{
    uint32_t crc = 0xFFFFFFFFu;
    size_t i;

#if defined(WL_HAVE_X86_CRC32_INTRINSICS) || \
    defined(WL_HAVE_ARM_CRC32_INTRINSICS)
    if (hw_crc32_check()) {
        /* Hardware path: use CRC32 instruction (Castagnoli polynomial) */
#if defined(WL_HAVE_X86_CRC32_INTRINSICS)
        for (i = 0; i < len; i++)
            crc = intel_crc32_hw_u8(crc, data[i]);
#elif defined(WL_HAVE_ARM_CRC32_INTRINSICS)
        for (i = 0; i < len; i++)
            crc = arm_crc32_hw_u8(crc, data[i]);
#endif
        return crc ^ 0xFFFFFFFFu;
    }
#endif

    /* Software fallback path */
    if (!castagnoli_table_ready)
        castagnoli_crc32_init();

    for (i = 0; i < len; i++)
        crc = (crc >> 8) ^ castagnoli_table[(crc ^ data[i]) & 0xFFu];
    return crc ^ 0xFFFFFFFFu;
}
