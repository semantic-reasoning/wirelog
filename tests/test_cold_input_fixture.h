/* Shared public/internal causal workload: rows exceed the public 256 MiB floor.
 * Only one 2 MiB host chunk is retained; all three controls use identical data. */
#ifndef WL_TEST_COLD_INPUT_FIXTURE_H
#define WL_TEST_COLD_INPUT_FIXTURE_H
#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#define WL_TEST_COLD_BUDGET UINT64_C(268435456)
#define WL_TEST_COLD_WIDTH 1024u
#define WL_TEST_COLD_ROWS 40960u
#define WL_TEST_COLD_CHUNK 256u
static void wl_test_cold_text(char **cursor, size_t *remaining,
    const char *format, ...)
{
    va_list args; va_start(args, format);
    int n = vsnprintf(*cursor, *remaining, format, args); va_end(args);
    assert(n >= 0 && (size_t)n < *remaining);
    *cursor += n; *remaining -= (size_t)n;
}
static char *wl_test_cold_query(int scan)
{
    size_t remaining = 128*1024; char *text = malloc(remaining); assert(text);
    char *cursor = text;
    wl_test_cold_text(&cursor, &remaining, ".decl b(");
    for (unsigned i = 0; i < WL_TEST_COLD_WIDTH; i++)
        wl_test_cold_text(&cursor, &remaining, "%sc%u:int64", i ? "," : "", i);
    wl_test_cold_text(&cursor, &remaining, ")\n");
    if (scan == 3) {
        wl_test_cold_text(&cursor, &remaining,
            ".decl a(g:int64,k:int64,v:int64)\n.decl result(k:int64,x:int64,y:int64)\n"
            "result(k,x,y) :- a(g,k,x),b(");
    } else if (scan == 2) {
        wl_test_cold_text(&cursor, &remaining,
            ".decl result(k:int64,n:int64)\nresult(k,sum(v)) :- b(");
    } else if (scan) {
        wl_test_cold_text(&cursor, &remaining, ".decl result(");
        for (unsigned i = 0; i < WL_TEST_COLD_WIDTH; i++)
            wl_test_cold_text(&cursor, &remaining, "%sc%u:int64", i ? "," : "",
                i);
        wl_test_cold_text(&cursor, &remaining, ")\nresult(");
        for (unsigned i = 0; i < WL_TEST_COLD_WIDTH; i++)
            wl_test_cold_text(&cursor, &remaining, "%sc%u", i ? "," : "", i);
        wl_test_cold_text(&cursor, &remaining, ") :- b(");
    } else wl_test_cold_text(&cursor, &remaining,
            ".decl a(k:int64,v:int64)\n.decl result(k:int64,x:int64,y:int64)\n"
            "result(k,x,y+0) :- a(k,x),b(");
    for (unsigned i = 0; i < WL_TEST_COLD_WIDTH; i++) {
        if (scan == 3 && i < 3) wl_test_cold_text(&cursor, &remaining,
                "%s", i == 0 ? "g" : i == 1 ? ",k" : ",y");
        else if (scan == 1) wl_test_cold_text(&cursor, &remaining, "%sc%u",
                i ? "," : "",
                i);
        else if (i < 2) wl_test_cold_text(&cursor, &remaining, "%s",
                i ? (scan == 2 ? ",v" : ",y") : "k");
        else wl_test_cold_text(&cursor, &remaining, ",u%u", i);
    }
    wl_test_cold_text(&cursor, &remaining, ").\n");
    return text;
}
static void wl_test_cold_rows(int64_t *rows, unsigned first)
{
    for (unsigned i = 0; i < WL_TEST_COLD_CHUNK; i++)
        for (unsigned c = 0; c < WL_TEST_COLD_WIDTH; c++)
            rows[(size_t)i*WL_TEST_COLD_WIDTH+c] = first+i+(c ? 100 : 0);
}
/* Constant first key deliberately leaves all selectivity to the second key. */
static void wl_test_cold_composite_rows(int64_t *rows, unsigned first)
{
    wl_test_cold_rows(rows, first);
    for (unsigned i = 0; i < WL_TEST_COLD_CHUNK; i++) {
        rows[(size_t)i*WL_TEST_COLD_WIDTH] = 7;
        rows[(size_t)i*WL_TEST_COLD_WIDTH+1] = first+i;
    }
}
/* The SUM workload uses four fixed groups with 320 MiB of physical input. */
static void wl_test_cold_group_rows(int64_t *rows, unsigned first)
{
    wl_test_cold_rows(rows, first);
    for (unsigned i = 0; i < WL_TEST_COLD_CHUNK; i++)
        rows[(size_t)i*WL_TEST_COLD_WIDTH] %= 4;
}
static void wl_test_cold_group_observe(const char *name, const int64_t *row,
    uint32_t cols, void *context)
{
    if (strcmp(name, "result") != 0) return;
    unsigned *seen = context;
    assert(cols == 2 && row[0] >= 0 && row[0] < 4);
    int64_t count = WL_TEST_COLD_ROWS/4;
    int64_t expected = count*(2*row[0]+WL_TEST_COLD_ROWS-4)/2+100*count;
    assert(row[1] == expected && !(*seen & (1u << row[0])));
    *seen |= 1u << row[0];
}

#endif
