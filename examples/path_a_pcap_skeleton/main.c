/*
 * Path A pcap skeleton — standalone I/O adapter example
 *
 * Demonstrates how a user registers a custom I/O adapter with libwirelog
 * WITHOUT rebuilding the library.  The user compiles only this file and
 * links against the installed libwirelog.
 *
 * Build (after `meson install`):
 *   cc -o pcap_skeleton main.c $(pkg-config --cflags --libs wirelog)
 *
 * This is a compile-check skeleton; the read callback returns an empty
 * result set.  A real adapter would parse pcap data here.
 *
 * See also: Issue #446 (Option C design), Issue #462 (CI compile gate).
 */

#include <wirelog/wirelog.h>

#include <stdio.h>
#include <stdlib.h>

/* ---------- adapter callbacks ---------- */

static int
pcap_validate(wirelog_io_ctx_t *ctx, char *errbuf, size_t errbuf_len,
    void *user_data)
{
    (void)user_data;

    const char *filename = wirelog_io_ctx_param(ctx, "filename");
    if (!filename) {
        snprintf(errbuf, errbuf_len, "missing required param 'filename'");
        return -1;
    }
    return 0;
}

static int
pcap_read(wirelog_io_ctx_t *ctx, int64_t **out_data, uint32_t *out_nrows,
    void *user_data)
{
    (void)ctx;
    (void)user_data;

    /*
     * Skeleton: return an empty result set.
     * A real adapter would open the pcap file, parse packets,
     * and fill a row-major int64_t buffer here.
     */
    *out_data = NULL;
    *out_nrows = 0;
    return 0;
}

/* ---------- adapter definition ---------- */

static const wirelog_io_adapter_t pcap_adapter = {
    .abi_version = WIRELOG_IO_ABI_VERSION,
    .scheme = "pcap",
    .description = "libpcap file reader (skeleton)",
    .read = pcap_read,
    .validate = pcap_validate,
    .user_data = NULL,
};

/* ---------- main ---------- */

int
main(void)
{
    if (wirelog_io_register_adapter(&pcap_adapter) != 0) {
        fprintf(stderr, "register failed: %s\n", wirelog_io_last_error());
        return 1;
    }

    const wirelog_io_adapter_t *found = wirelog_io_find_adapter("pcap");
    if (!found) {
        fprintf(stderr, "adapter lookup failed\n");
        return 1;
    }

    printf("registered adapter: scheme=%s desc=\"%s\"\n",
        found->scheme, found->description);

    wirelog_io_unregister_adapter("pcap");
    return 0;
}
