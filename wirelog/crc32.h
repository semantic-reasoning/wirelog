/*
 * wirelog/crc32.h - CRC-32 variant selection (Issue #145)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This header provides compile-time selection of the default CRC-32 variant.
 * Two variants are supported:
 * - Ethernet (standard): poly=0x04C11DB7, used in Ethernet/HDLC
 * - Castagnoli (iSCSI): poly=0x1EDC6F41, used in iSCSI/SCTP
 *
 * Configuration is set via meson option 'crc32_variant'.
 */

#ifndef WIRELOG_CRC32_H
#define WIRELOG_CRC32_H

#include <stdint.h>
#include <stddef.h>

/*
 * Function declarations for both CRC-32 variants
 * (implementation in wirelog/crc32.c)
 */
extern uint32_t
ethernet_crc32(const uint8_t *data, size_t len);
extern uint32_t
castagnoli_crc32(const uint8_t *data, size_t len);

/*
 * Hardware-accelerated per-byte CRC-32C (Castagnoli) helpers.
 * These wrap the native CRC32 instruction on each architecture.
 * On unsupported platforms they return 0 and should not be called directly.
 * Use castagnoli_crc32() which dispatches automatically.
 */
extern uint32_t
intel_crc32_hw_u8(uint32_t crc, uint8_t byte);
extern uint32_t
arm_crc32_hw_u8(uint32_t crc, uint8_t byte);

/*
 * Default CRC-32 variant selection via compile-time define
 * Set by meson build system based on crc32_variant option
 */
#ifdef CRC32_DEFAULT_VARIANT_ETHERNET
#define crc32(data, len) ethernet_crc32(data, len)
#elif CRC32_DEFAULT_VARIANT_CASTAGNOLI
#define crc32(data, len) castagnoli_crc32(data, len)
#else
/* Default to Ethernet if no variant is specified */
#define crc32(data, len) ethernet_crc32(data, len)
#endif

#endif /* WIRELOG_CRC32_H */
