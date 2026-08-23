#include <wirelog/wirelog.h>

int main(int argc, char **argv)
{
    wirelog_error_t error = WIRELOG_ERR_UNKNOWN;
    wirelog_program_t *program = (wirelog_program_t *)(void *)argv;
    wirelog_executor_t *executor = wirelog_executor_create(program, &error);
    (void)argc;
    (void)wirelog_load_facts_from_csv(executor, argv[0], argv[0], &error);
    (void)wirelog_evaluate(executor, &error);
    (void)wirelog_result_get_relation(NULL, NULL);
    (void)wirelog_result_relation_cardinality(NULL, NULL);
    (void)wirelog_result_write_csv(NULL, NULL, NULL, &error);
    wirelog_result_free(NULL);
    wirelog_executor_free(executor);
    return 0;
}
