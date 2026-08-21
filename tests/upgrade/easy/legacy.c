#include <wirelog/wl_easy.h>

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct row { int64_t x, y; };
struct rows { struct row values[8]; uint32_t count; };

static void collect(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    struct rows *rows = user_data;
    if (relation && strcmp(relation, "reach") == 0 && ncols == 2
        && diff > 0 && rows->count < 8) {
        rows->values[rows->count].x = row[0];
        rows->values[rows->count++].y = row[1];
    }
}

static int compare_rows(const void *left, const void *right)
{
    const struct row *a = left;
    const struct row *b = right;
    if (a->x != b->x)
        return a->x < b->x ? -1 : 1;
    return (a->y > b->y) - (a->y < b->y);
}

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

    wl_easy_session_t *session = NULL;
    if (wl_easy_open(source, &session) != WIRELOG_OK || !session) {
        free(source);
        return 5;
    }
    struct rows rows = {0};
    if (wl_easy_set_delta_cb(session, collect, &rows) != WIRELOG_OK) {
        wl_easy_close(session);
        free(source);
        return 6;
    }
    int64_t first[] = {1, 2};
    int64_t second[] = {2, 3};
    if (wl_easy_insert(session, "edge", first, 2) != WIRELOG_OK
        || wl_easy_insert(session, "edge", second, 2) != WIRELOG_OK
        || wl_easy_step(session) != WIRELOG_OK) {
        wl_easy_close(session);
        free(source);
        return 6;
    }
    /* The callback count is intentionally reset per process; sort before emit. */
    qsort(rows.values, rows.count, sizeof(rows.values[0]), compare_rows);
    for (uint32_t i = 0; i < rows.count; i++)
        printf("reach %" PRId64 " %" PRId64 "\n",
            rows.values[i].x, rows.values[i].y);
    wl_easy_close(session);
    free(source);
    return 0;
}
