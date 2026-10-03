/* Smoke contract for bench/bench_batch_append; timings are not asserted. */
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32)
#define popen _popen
#define pclose _pclose
#endif

#define OUTPUT_CAPACITY 8192

int
main(int argc, char **argv)
{
    if (argc != 2) {
        fprintf(stderr, "bench_batch_append_smoke: missing benchmark path\n");
        return 1;
    }
    char command[1024];
    int written = snprintf(command, sizeof(command),
            "\"%s\" --iterations 4 --samples 2 --warmups 1", argv[1]);
    if (written < 0 || (size_t)written >= sizeof(command)) {
        fprintf(stderr, "bench_batch_append_smoke: path too long\n");
        return 1;
    }
    FILE *pipe = popen(command, "r");
    if (!pipe) {
        fprintf(stderr, "bench_batch_append_smoke: popen failed\n");
        return 1;
    }
    char output[OUTPUT_CAPACITY];
    size_t length = fread(output, 1, sizeof(output) - 1, pipe);
    output[length] = '\0';
    int status = pclose(pipe);
    if (status != 0) {
        fprintf(stderr, "bench_batch_append_smoke: benchmark failed\n%s",
            output);
        return 1;
    }
    static const char *const expected[] = { "case=1x1", "case=1x256",
                                            "case=32x256" };
    for (size_t i = 0; i < sizeof(expected) / sizeof(expected[0]); i++) {
        if (!strstr(output, expected[i])) {
            fprintf(stderr,
                "bench_batch_append_smoke: missing %s output\n%s",
                expected[i], output);
            return 1;
        }
    }
    size_t sample_count = 0, summary_count = 0;
    for (const char *p = output; (p = strstr(p, "sample\tcase=")) != NULL;
        p += sizeof("sample\tcase=") - 1)
        sample_count++;
    for (const char *p = output; (p = strstr(p, "summary\tcase=")) != NULL;
        p += sizeof("summary\tcase=") - 1)
        summary_count++;
    if (sample_count != 6 || summary_count != 3) {
        fprintf(stderr,
            "bench_batch_append_smoke: expected six raw samples and three summaries\n%s",
            output);
        return 1;
    }
    if (!strstr(output, "append_ns_per_call=")
        || !strstr(output, "reset_ns_per_call=")
        || !strstr(output, "median_append_ns_per_call=")
        || !strstr(output, "cov_append_percent=")
        || !strstr(output, "status=OK")) {
        fprintf(stderr, "bench_batch_append_smoke: incomplete metrics\n%s",
            output);
        return 1;
    }
    return 0;
}
