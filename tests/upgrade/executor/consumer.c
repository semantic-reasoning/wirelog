#include <wirelog/wirelog.h>

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv)
{
    if (argc != 2)
        return 2;
    FILE *file = fopen(argv[1], "rb");
    if (!file)
        return 3;
    fseek(file, 0, SEEK_END);
    long size = ftell(file);
    rewind(file);
    char *source = malloc((size_t)size + 1);
    if (!source || fread(source, 1, (size_t)size, file) != (size_t)size) {
        fclose(file);
        free(source);
        return 4;
    }
    fclose(file);
    source[size] = '\0';
    wirelog_error_t error = WIRELOG_ERR_UNKNOWN;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    wirelog_executor_t *executor = program
        ? wirelog_executor_create(program, &error) : NULL;
    wirelog_result_t *result = executor ? wirelog_evaluate(executor, &error)
                                        : NULL;
    uint64_t rows = result ? wirelog_result_relation_cardinality(result,
            "reach") : 0;
    if (!result || error != WIRELOG_OK || rows != 3) {
        wirelog_result_free(result);
        wirelog_executor_free(executor);
        wirelog_program_free(program);
        free(source);
        return 5;
    }
    printf("reach-count %" PRIu64 "\n", rows);
    wirelog_result_free(result);
    wirelog_executor_free(executor);
    wirelog_program_free(program);
    free(source);
    return 0;
}
