/*
 * arithmetic_demo.c - Example 14: Arithmetic Operations
 *
 * Copyright (C) CleverPlant
 * SPDX-License-Identifier: LGPL-3.0-or-later
 */

#include "wirelog/wirelog.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *ARITHMETIC_SRC =
    ".decl sample(label: symbol, a: int64, b: int64, c: int64)\n"
    ".decl result(label: symbol, added: int64, difference: int64, "
    "product: int64, quotient: int64, remainder: int64, precedence: int64)\n"
    "result(Label, A + B, A - B, A * B, A / B, A % B, A + B * C) "
    ":- sample(Label, A, B, C), B != 0.\n";

static const char *AGGREGATE_SRC =
    ".decl sample(a: int64, value: float)\n"
    ".decl zero_input(value: float)\n"
    ".decl zero_observed(value: float)\n"
    ".decl minimum(value: int64)\n"
    ".decl maximum(value: int64)\n"
    ".decl average_value(value: float)\n"
    ".decl sample_count(value: int64)\n"
    "minimum(min(A)) :- sample(A, _).\n"
    "maximum(max(A)) :- sample(A, _).\n"
    "average_value(average(Value)) :- sample(_, Value).\n"
    "sample_count(count(A)) :- sample(A, _).\n"
    "zero_observed(Value) :- zero_input(Value).\n";

typedef struct {
    int64_t id;
    const char *name;
} symbol_t;

typedef struct {
    char lines[12][256];
    size_t count;
    bool invalid;
    const char *wanted;
    const symbol_t *symbols;
    size_t symbol_count;
} output_t;

static const char *
lookup_symbol(const output_t *out, int64_t id)
{
    for (size_t i = 0; i < out->symbol_count; i++) {
        if (out->symbols[i].id == id)
            return out->symbols[i].name;
    }
    return NULL;
}

static void
on_result(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    output_t *out = (output_t *)user_data;
    const char *label;

    if (strcmp(relation, "result") == 0 && ncols == 7) {
        if (out->count >= 8 || !(label = lookup_symbol(out, row[0]))) {
            fprintf(stderr, "unexpected arithmetic result\n");
            abort();
        }
        snprintf(out->lines[out->count++], sizeof(out->lines[0]),
            "result(\"%s\", %" PRId64 ", %" PRId64 ", %" PRId64
            ", %" PRId64 ", %" PRId64 ", %" PRId64 ")", label,
            row[1], row[2], row[3], row[4], row[5], row[6]);
        return;
    }
    return;
}

static void
on_typed_aggregate(const char *relation, const wirelog_typed_row_v1_t *row,
    int32_t diff, void *user_data)
{
    output_t *out = (output_t *)user_data;
    uint64_t lane;
    double value;

    (void)diff;
    if (!out->wanted || strcmp(relation, out->wanted) != 0
        || !row || row->logical_ncols != 1 || out->count >= 12)
        return;
    lane = row->lanes[row->lane_offsets[0]];
    if (strcmp(relation, "average_value") == 0
        || strcmp(relation, "zero_observed") == 0) {
        memcpy(&value, &lane, sizeof(value));
        if (strcmp(relation, "zero_observed") == 0) {
            if (lane != UINT64_C(0)) {
                out->invalid = true;
                return;
            }
            snprintf(out->lines[out->count++], sizeof(out->lines[0]),
                "zero_observed(%s0.0)",
                (lane >> 63) == 0 ? "+" : "-");
        } else {
            snprintf(out->lines[out->count++], sizeof(out->lines[0]),
                "average_value(%.6f)", value);
        }
    } else {
        int64_t integer_value;

        memcpy(&integer_value, &lane, sizeof(integer_value));
        snprintf(out->lines[out->count++], sizeof(out->lines[0]),
            "%s(%" PRId64 ")", relation, integer_value);
    }
}

static int
line_compare(const void *left, const void *right)
{
    return strcmp((const char *)left, (const char *)right);
}

static int
insert_sample(wirelog_easy_session_t *session, int64_t label, int64_t a,
    int64_t b, int64_t c)
{
    int64_t row[] = { label, a, b, c };
    return wirelog_easy_insert(session, "sample", row, 4) == WIRELOG_OK
        ? 0 : -1;
}

static int
collect_aggregate(const char *relation, output_t *output)
{
    wirelog_error_t error = WIRELOG_OK;
    wirelog_program_t *program = wirelog_parse_string(AGGREGATE_SRC, &error);
    wirelog_session_t *advanced = NULL;
    const int64_t values[] = { -17, 17, 8 };
    const double measurements[] = { 1.5, 2.5, 3.5 };

    if (!program || !wirelog_optimize(program, &error)
        || wirelog_session_create(program, WIRELOG_BACKEND_DEFAULT, 1,
        &advanced) != WIRELOG_OK) {
        fprintf(stderr, "%s aggregate setup failed: %s\n", relation,
            wirelog_error_string(error));
        wirelog_program_free(program);
        return -1;
    }
    const bool zero_demo = strcmp(relation, "zero_observed") == 0;
    for (size_t i = 0; i < (zero_demo ? 2u : 3u); i++) {
        uint64_t lanes[2] = { 0, 0 };
        double value = zero_demo ? (i == 0 ? -0.0 : +0.0) : measurements[i];
        memcpy(&lanes[0], &values[i], sizeof(values[i]));
        memcpy(&lanes[1], &value, sizeof(value));
        uint32_t sample_types[2] = {
            WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT
        };
        uint32_t sample_offsets[2] = { 0, 1 };
        uint32_t zero_type = WIRELOG_TYPE_FLOAT;
        uint32_t zero_offset = 0;
        wirelog_typed_row_v1_t row = zero_demo
            ? (wirelog_typed_row_v1_t) {
            sizeof(row), 1, 0, 1, 1, 1, &zero_type, &zero_offset,
            &zero_type, &lanes[1]
        }
            : (wirelog_typed_row_v1_t) {
            sizeof(row), 1, 0, 2, 2, 2, sample_types, sample_offsets,
            sample_types, lanes
        };
        wirelog_typed_error_v1_t typed_error = {
            sizeof(typed_error), WIRELOG_TYPED_ERROR_NONE,
            UINT32_MAX, UINT32_MAX, NULL, 0
        };
        const char *input = zero_demo ? "zero_input" : "sample";
        if (wirelog_session_insert_typed(advanced, input, &row, 1,
            &typed_error) != WIRELOG_OK) {
            fprintf(stderr, "%s aggregate input failed\n", relation);
            wirelog_session_destroy(advanced);
            wirelog_program_free(program);
            return -1;
        }
        if (zero_demo) {
            uint64_t expected = i == 0 ? UINT64_C(0x8000000000000000)
                                       : UINT64_C(0);
            if (lanes[1] != expected) {
                fprintf(stderr, "signed-zero input encoding failed\n");
                wirelog_session_destroy(advanced);
                wirelog_program_free(program);
                return -1;
            }
        }
    }
    output->wanted = relation;
    wirelog_error_t rc = wirelog_session_snapshot_typed(advanced,
            on_typed_aggregate, output);
    wirelog_session_destroy(advanced);
    wirelog_program_free(program);
    if (rc != WIRELOG_OK) {
        fprintf(stderr, "%s aggregate failed: %s\n", relation,
            wirelog_error_string(rc));
        return -1;
    }
    return 0;
}

int
main(void)
{
    static const char *names[] = { "negative", "positive", "precedence" };
    symbol_t symbols[3];
    output_t output = { .count = 0, .wanted = "result", .symbols = symbols,
                        .symbol_count = 3 };
    wirelog_easy_session_t *session = NULL;

    printf("Example 14: Arithmetic Operations\n");
    printf("=================================\n\n");

    if (wirelog_easy_open(ARITHMETIC_SRC, &session) != WIRELOG_OK) {
        fprintf(stderr, "wirelog_easy_open failed\n");
        return 1;
    }
    for (size_t i = 0; i < 3; i++) {
        symbols[i].name = names[i];
        symbols[i].id = wirelog_easy_intern(session, names[i]);
        if (symbols[i].id < 0) {
            wirelog_easy_close(session);
            return 1;
        }
    }

    /* B is non-zero for every row: division and remainder are defined. */
    wirelog_error_t rc = WIRELOG_OK;
    if (insert_sample(session, symbols[0].id, -17, 5, 2) != 0
        || insert_sample(session, symbols[1].id, 17, 5, 2) != 0
        || insert_sample(session, symbols[2].id, 8, 3, 2) != 0
        || (rc = wirelog_easy_snapshot(session, "result", on_result, &output))
        != WIRELOG_OK) {
        fprintf(stderr, "arithmetic evaluation failed: %s\n",
            wirelog_error_string(rc));
        wirelog_easy_close(session);
        return 1;
    }
    wirelog_easy_close(session);

    if (collect_aggregate("minimum", &output) != 0
        || collect_aggregate("maximum", &output) != 0
        || collect_aggregate("average_value", &output) != 0
        || collect_aggregate("sample_count", &output) != 0
        || collect_aggregate("zero_observed", &output) != 0
        || output.invalid) {
        fprintf(stderr, "aggregate evaluation failed\n");
        return 1;
    }

    qsort(output.lines, output.count, sizeof(output.lines[0]), line_compare);
    for (size_t i = 0; i < output.count; i++)
        printf("%s\n", output.lines[i]);
    printf("\nDone.\n");
    return output.count == 8 ? 0 : 1;
}
