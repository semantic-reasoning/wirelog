/*
 * test_crc32_windows_fallback.c - CRC-32 smoke test for MSVC/clang-cl
 */

#include <stdint.h>
#include <stdio.h>

#include "../wirelog/crc32.h"

int
main(void)
{
    static const uint8_t input[] = { '1', '2', '3', '4', '5',
                                     '6', '7', '8', '9' };
    uint32_t eth = ethernet_crc32(input, sizeof(input));
    uint32_t crc32c = castagnoli_crc32(input, sizeof(input));

    if (eth != 0xCBF43926u) {
        printf(
            "ethernet_crc32(\"123456789\"): expected 0xCBF43926, got 0x%08X\n",
            eth);
        return 1;
    }

    if (crc32c != 0xE3069283u) {
        printf(
            "castagnoli_crc32(\"123456789\"): expected 0xE3069283, got 0x%08X\n",
            crc32c);
        return 1;
    }

    return 0;
}
