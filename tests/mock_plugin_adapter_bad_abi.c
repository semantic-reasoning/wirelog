/*
 * mock_plugin_adapter_bad_abi.c - ABI mismatch mock for test_plugin_loader
 *
 * Always returns NULL to simulate an ABI version mismatch.
 */

#include "wirelog/io/io_adapter.h"

#include <stddef.h>

WL_IO_PLUGIN_EXPORT
const wl_io_adapter_t *const *
wl_io_plugin_entry(uint32_t *n_out, uint32_t abi_ver)
{
    (void)abi_ver;
    *n_out = 0;
    return NULL;
}
