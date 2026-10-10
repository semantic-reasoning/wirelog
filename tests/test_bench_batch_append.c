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
    for (const char *p = output; (p = strstr(p, "sample\tcontract=")) != NULL;
        p += sizeof("sample\tcontract=") - 1)
        sample_count++;
    for (const char *p = output; (p = strstr(p, "summary\tcontract=")) != NULL;
        p += sizeof("summary\tcontract=") - 1)
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
    static const char *const success_fields[] = {
        "contract=wirelog.batch-append-benchmark.v2",
        "denied=0",
        "row_count_check=OK",
        "capacity_check=OK",
        "value_check=OK",
        "distinct_input_probe=OK",
        "status=OK",
    };
    size_t sample_records = 0, case_records = 0;
    for (char *line = output; line && *line;) {
        char *next = strchr(line, '\n');
        if (next)
            *next++ = '\0';
        if (strncmp(line, "sample\t", 7) == 0
            || strncmp(line, "case\t", 5) == 0) {
            if (strncmp(line, "sample\t", 7) == 0)
                sample_records++;
            else
                case_records++;
            for (size_t i = 0;
                i < sizeof(success_fields) / sizeof(success_fields[0]); i++) {
                if (!strstr(line, success_fields[i])) {
                    fprintf(stderr,
                        "bench_batch_append_smoke: missing %s from success record\n%s\n",
                        success_fields[i], line);
                    return 1;
                }
            }
        } else if (strncmp(line, "summary\t", 8) == 0
            && !strstr(line, "contract=wirelog.batch-append-benchmark.v2")) {
            fprintf(stderr,
                "bench_batch_append_smoke: missing output schema from summary\n%s\n",
                line);
            return 1;
        }
        line = next;
    }
    if (sample_records != 6 || case_records != 3) {
        fprintf(stderr,
            "bench_batch_append_smoke: expected six sample and three case records\n%s",
            output);
        return 1;
    }
    if (!strstr(output,
        "bench_batch_append\tcontract=wirelog.batch-append-benchmark.v2")) {
        fprintf(stderr,
            "bench_batch_append_smoke: missing output schema header\n%s",
            output);
        return 1;
    }
    written = snprintf(command, sizeof(command), "\"%s\" --iterations 0",
            argv[1]);
    if (written < 0 || (size_t)written >= sizeof(command)) {
        fprintf(stderr,
            "bench_batch_append_smoke: failure probe path too long\n");
        return 1;
    }
    pipe = popen(command, "r");
    if (!pipe) {
        fprintf(stderr,
            "bench_batch_append_smoke: failure probe popen failed\n");
        return 1;
    }
    length = fread(output, 1, sizeof(output) - 1, pipe);
    output[length] = '\0';
    status = pclose(pipe);
    if (status == 0 || strstr(output, "status=OK")
        || strstr(output, "row_count_check=OK")
        || strstr(output, "distinct_input_probe=OK")) {
        fprintf(stderr,
            "bench_batch_append_smoke: failure emitted a success record\n%s",
            output);
        return 1;
    }

    /* 32x256-float (#2125) runs only when named; "all" is checked above. */
    written = snprintf(command, sizeof(command),
            "\"%s\" --case 32x256-float --iterations 4 --samples 2"
            " --warmups 1", argv[1]);
    if (written < 0 || (size_t)written >= sizeof(command)) {
        fprintf(stderr, "bench_batch_append_smoke: float path too long\n");
        return 1;
    }
    pipe = popen(command, "r");
    if (!pipe) {
        fprintf(stderr, "bench_batch_append_smoke: float popen failed\n");
        return 1;
    }
    length = fread(output, 1, sizeof(output) - 1, pipe);
    output[length] = '\0';
    status = pclose(pipe);
    if (status != 0
        || !strstr(output, "case\tcontract=wirelog.batch-append-benchmark.v2"
        "\tname=32x256-float\tcolumns=32\trows_per_call=256")
        || !strstr(output, "value_check=OK")
        || !strstr(output, "distinct_input_probe=OK")
        || strstr(output, "case=1x1\t")) {
        fprintf(stderr, "bench_batch_append_smoke: float case failed\n%s",
            output);
        return 1;
    }
    return 0;
}
