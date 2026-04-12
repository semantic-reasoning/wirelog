/*
 * test_io_dispatch.c - .input dispatch test scaffold (TDD gate)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Three test scenarios for adapter-based .input dispatch:
 * dispatch_mock_adapter, dispatch_csv_default, dispatch_unknown_scheme.
 *
 * Part of #446 (I/O adapter umbrella).
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#ifndef TEST_DISPATCH_PRESENT
#define TEST_DISPATCH_PRESENT
#endif

/* ======================================================================== */
/* Test Harness                                                             */
/* ======================================================================== */

#define TEST(name) do { printf("  [TEST] %-50s ", name); } while (0)
#define PASS()     do { printf("PASS\n"); passed++; } while (0)
#define FAIL(msg)  do { printf("FAIL: %s\n", msg); failed++; } while (0)
#define SKIP(msg)  do { printf("SKIP: %s\n", msg); skipped++; } while (0)

static int passed = 0, failed = 0, skipped = 0;

/* ======================================================================== */
/* Includes for dispatch testing                                            */
/* ======================================================================== */

#ifdef TEST_DISPATCH_PRESENT

#include "wirelog/io/io_adapter.h"
#include "wirelog/io/io_ctx_internal.h"
#include "wirelog/ir/program.h"
#include "wirelog/ir/ir.h"       /* strdup_safe */
#include "wirelog/session_facts.h"
#include "wirelog/session.h"
#include "wirelog/backend.h"

/* ---- Mock backend (accepts any insert) ---- */

static int s_insert_called;

static int
mock_insert(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    (void)session; (void)relation; (void)data;
    (void)num_rows; (void)num_cols;
    s_insert_called = 1;
    return 0;
}

static const wl_compute_backend_t s_mock_backend = {
    .name = "mock",
    .session_create = NULL,
    .session_destroy = NULL,
    .session_insert = mock_insert,
    .session_remove = NULL,
    .session_step = NULL,
    .session_set_delta_cb = NULL,
    .session_snapshot = NULL,
};

/* ---- Mock I/O adapter ---- */

static int s_mock_read_called;
static const char *s_mock_received_key;

static int
mock_read_cb(wl_io_ctx_t *ctx, int64_t **out_data,
    uint32_t *out_nrows, void *user_data)
{
    (void)user_data;
    s_mock_read_called = 1;
    s_mock_received_key = wl_io_ctx_param(ctx, "key");

    /* Return a single row with one column */
    *out_data = (int64_t *)malloc(sizeof(int64_t));
    if (!*out_data)
        return -1;
    (*out_data)[0] = 42;
    *out_nrows = 1;
    return 0;
}

/* ---- Helper: build a minimal program with one .input relation ---- */

static struct wirelog_program *
make_program(const char *scheme, const char **pnames,
    const char **pvalues, uint32_t nparam)
{
    struct wirelog_program *prog =
        (struct wirelog_program *)calloc(1, sizeof(*prog));
    if (!prog)
        return NULL;

    prog->relation_count = 1;
    prog->relations = (wl_ir_relation_info_t *)calloc(1,
            sizeof(wl_ir_relation_info_t));
    if (!prog->relations) {
        free(prog);
        return NULL;
    }

    wl_ir_relation_info_t *rel = &prog->relations[0];
    rel->name = "events";
    rel->has_input = true;
    rel->column_count = 1;
    rel->columns = (wirelog_column_t *)calloc(1, sizeof(wirelog_column_t));
    if (!rel->columns) {
        free(prog->relations);
        free(prog);
        return NULL;
    }
    rel->columns[0].type = WIRELOG_TYPE_INT64;

    if (scheme)
        rel->input_io_scheme = strdup_safe(scheme);
    else
        rel->input_io_scheme = NULL;
    rel->input_param_names = (char **)pnames;
    rel->input_param_values = (char **)pvalues;
    rel->input_param_count = nparam;

    prog->intern = NULL;
    return prog;
}

static void
free_program(struct wirelog_program *prog)
{
    if (!prog)
        return;
    free(prog->relations[0].columns);
    free(prog->relations[0].input_io_scheme);
    free(prog->relations);
    free(prog);
}

#endif /* TEST_DISPATCH_PRESENT */

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

/*
 * test_dispatch_mock_adapter
 *
 * Register a mock adapter with scheme="mock". Build a minimal program
 * with has_input=true and input_io_scheme="mock". Call
 * wl_session_load_input_files. Assert the mock adapter's read callback
 * was invoked and received the correct params.
 */
static void
test_dispatch_mock_adapter(void)
{
    TEST("dispatch_mock_adapter");
#ifdef TEST_DISPATCH_PRESENT
    s_mock_read_called = 0;
    s_mock_received_key = NULL;
    s_insert_called = 0;

    /* Register mock adapter */
    wl_io_adapter_t mock = {0};
    mock.abi_version = WL_IO_ABI_VERSION;
    mock.scheme = "mock";
    mock.description = "test mock adapter";
    mock.read = mock_read_cb;
    mock.validate = NULL;
    mock.user_data = NULL;

    int reg_rc = wl_io_register_adapter(&mock);
    if (reg_rc != 0) {
        FAIL("failed to register mock adapter");
        return;
    }

    /* Build program with io="mock" and key="val" */
    const char *pnames[] = { "key" };
    const char *pvalues[] = { "val" };
    struct wirelog_program *prog = make_program("mock", pnames, pvalues, 1);
    if (!prog) {
        FAIL("failed to create program");
        wl_io_unregister_adapter("mock");
        return;
    }

    /* Create a mock session backed by our mock backend */
    wl_session_t sess;
    sess.backend = &s_mock_backend;

    int rc = wl_session_load_input_files(&sess, prog);

    int ok = (rc == 0 && s_mock_read_called == 1 && s_insert_called == 1);
    if (ok && s_mock_received_key && strcmp(s_mock_received_key, "val") == 0) {
        PASS();
    } else {
        FAIL("mock adapter not called correctly or param mismatch");
    }

    free_program(prog);
    wl_io_unregister_adapter("mock");
#else
    SKIP("dispatch rewrite not yet implemented (#458)");
#endif
}

/*
 * test_dispatch_csv_default
 *
 * Build a program with no io= (NULL => defaults to "csv"). Create a
 * temp CSV file and verify it routes to the built-in CSV adapter.
 */
static void
test_dispatch_csv_default(void)
{
    TEST("dispatch_csv_default");
#ifdef TEST_DISPATCH_PRESENT
    s_insert_called = 0;

    /* Create a temp CSV file (use platform-safe temp path) */
    const char *tmpdir = getenv("TMPDIR");
    if (!tmpdir) tmpdir = getenv("TMP");
    if (!tmpdir) tmpdir = getenv("TEMP");
    if (!tmpdir) tmpdir = "/tmp";
    char csv_path[512];
    snprintf(csv_path, sizeof(csv_path), "%s/wirelog_test_dispatch_default.csv",
        tmpdir);
    FILE *f = fopen(csv_path, "w");
    if (!f) {
        FAIL("could not create temp CSV file");
        return;
    }
    fprintf(f, "10\n20\n");
    fclose(f);

    /* Build program with no io= (defaults to "csv") */
    const char *pnames[] = { "filename" };
    const char *pvalues[] = { csv_path };
    struct wirelog_program *prog = make_program(NULL, pnames, pvalues, 1);
    if (!prog) {
        FAIL("failed to create program");
        remove(csv_path);
        return;
    }

    /* Mock session */
    wl_session_t sess;
    sess.backend = &s_mock_backend;

    int rc = wl_session_load_input_files(&sess, prog);

    if (rc == 0 && s_insert_called == 1) {
        PASS();
    } else {
        FAIL("csv default dispatch failed");
    }

    free_program(prog);
    remove(csv_path);
#else
    SKIP("dispatch rewrite not yet implemented (#458)");
#endif
}

/*
 * test_dispatch_unknown_scheme_error
 *
 * Build a program with io="nonexistent". Assert
 * wl_session_load_input_files returns an error (non-zero rc).
 */
static void
test_dispatch_unknown_scheme_error(void)
{
    TEST("dispatch_unknown_scheme_error");
#ifdef TEST_DISPATCH_PRESENT
    const char *pnames[] = { "filename" };
    const char *pvalues[] = { "x" };
    struct wirelog_program *prog = make_program("nonexistent",
            pnames, pvalues, 1);
    if (!prog) {
        FAIL("failed to create program");
        return;
    }

    /* Mock session */
    wl_session_t sess;
    sess.backend = &s_mock_backend;

    int rc = wl_session_load_input_files(&sess, prog);

    if (rc != 0) {
        PASS();
    } else {
        FAIL("expected non-zero rc for unknown scheme");
    }

    free_program(prog);
#else
    SKIP("dispatch rewrite not yet implemented (#458)");
#endif
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int main(void) {
    printf("=== test_io_dispatch ===\n");
    test_dispatch_mock_adapter();
    test_dispatch_csv_default();
    test_dispatch_unknown_scheme_error();
    printf("=== Results: %d passed, %d failed, %d skipped ===\n",
        passed, failed, skipped);
    return failed > 0 ? 1 : 0;
}
