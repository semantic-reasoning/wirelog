#include "columnar/internal.h"
#include "exec_plan_gen.h"
#include "session.h"
#include "wirelog/wirelog-extension.h"
#include "wirelog/wirelog-parser.h"

#include <stdio.h>
#include <string.h>

static int callback_mode;
static int callback_calls;
static int fail_on_call;
static const uint8_t *expected_string;
static size_t expected_string_length;

static int
check(int condition, const char *message);

static void
ignore_snapshot_tuple(const char *relation, const int64_t *row,
    uint32_t ncols, void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)user_data;
}

static int
check_snapshot_clears_extension_status(void)
{
    static const char *source =
        ".decl input(x: int64)\n"
        ".decl output(x: int64)\n"
        "output(x) :- input(x).\n";
    wirelog_error_t error = WIRELOG_OK;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    wl_plan_t *plan = NULL;
    wl_session_t *session = NULL;
    int rc = program ? wl_plan_from_program(program, &plan) : -1;

    if (rc == 0)
        rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc == 0) {
        /* Simulate status retained from an earlier scalar callback failure. */
        COL_SESSION(session)->extension_expr_status =
            WL_COLUMNAR_EXPR_CALLBACK_FAILURE;
        rc = wl_session_snapshot(session, ignore_snapshot_tuple, NULL);
    }
    int failures = check(rc == 0, "non-extension snapshot execution");
    if (session)
        failures += check(COL_SESSION(session)->extension_expr_status == 0,
                "snapshot clears stale extension status");
    wl_session_destroy(session);
    wl_plan_free(plan);
    wirelog_program_free(program);
    return failures;
}

static int
extension_callback(const wirelog_extension_value_t *args, uint32_t nargs,
    wirelog_extension_value_t *result, void *user_data)
{
    (void)user_data;
    callback_calls++;
    if (fail_on_call != 0 && callback_calls >= fail_on_call)
        return 1;
    if (callback_mode == 1)
        return 1;
    if (callback_mode == 2) {
        result->type = WIRELOG_EXTENSION_VALUE_INT64;
        result->size = sizeof(int64_t);
        result->as.int64_value = 1;
        return 0;
    }
    if (callback_mode == 3) {
        result->type = WIRELOG_EXTENSION_VALUE_BOOL;
        result->size = 0;
        result->as.bool_value = 1;
        return 0;
    }
    if (callback_mode == 4) {
        if (nargs != 1 || args[0].type != WIRELOG_EXTENSION_VALUE_STRING
            || args[0].size != expected_string_length
            || args[0].as.string_value.data == NULL
            || args[0].as.string_value.length != expected_string_length
            || memcmp(args[0].as.string_value.data, expected_string,
            expected_string_length) != 0)
            return 1;
    }
    result->type = WIRELOG_EXTENSION_VALUE_BOOL;
    result->size = sizeof(uint8_t);
    result->as.bool_value = callback_mode == 4 ? 1 : nargs > 0
        ? (args[0].type == WIRELOG_EXTENSION_VALUE_BOOL
            ? args[0].as.bool_value : args[0].as.int64_value != 0)
        : 1;
    return 0;
}

static int
run_filter_case(const uint8_t *buf, uint32_t size,
    wl_col_session_t *session, uint32_t expected_rows, int expected_rc)
{
    col_rel_t *input = col_rel_new_auto("input", 1);
    eval_stack_t stack;
    wl_plan_op_t op = { 0 };
    int64_t rows[] = { 1, 0, 1 };
    int rc;
    if (!input)
        return check(0, "input relation allocation");
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++)
        if (col_rel_append_row(input, &rows[i]) != 0)
            return check(0, "input relation append");
    op.filter_expr.data = (uint8_t *)buf;
    op.filter_expr.size = size;
    eval_stack_init(&stack);
    eval_stack_push(&stack, input, true);
    rc = wl_columnar_filter_op(&op, &stack, session);
    if (expected_rc != 0)
        return check(rc == expected_rc && stack.top == 0,
                   "filter error discards partial output");
    if (rc != 0 || stack.top != 1)
        return check(0, "filter execution");
    {
        eval_entry_t result = eval_stack_pop(&stack);
        int ok = result.rel && result.rel->nrows == expected_rows;
        if (result.owned)
            col_rel_destroy(result.rel);
        return check(ok, "filter row selection");
    }
}

static int
run_map_error_case(const uint8_t *buf, uint32_t size,
    wl_col_session_t *session, int expected_rc)
{
    col_rel_t *input = col_rel_new_auto("input", 1);
    wl_plan_expr_buffer_t expression = { (uint8_t *)buf, size };
    wl_plan_op_t op = { 0 };
    eval_stack_t stack;
    int64_t row = 1;
    int rc;

    if (!input || col_rel_append_row(input, &row) != 0) {
        col_rel_destroy(input);
        return check(0, "map input allocation");
    }
    op.map_exprs = &expression;
    op.map_expr_count = 1;
    op.project_count = 1;
    eval_stack_init(&stack);
    eval_stack_push(&stack, input, true);
    rc = col_op_map(&op, &stack, session);
    while (stack.top > 0) {
        eval_entry_t entry = eval_stack_pop(&stack);
        if (entry.owned)
            col_rel_destroy(entry.rel);
    }
    return check(rc == expected_rc, "map preserves extension status");
}

static int
check(int condition, const char *message)
{
    if (!condition)
        fprintf(stderr, "FAIL: %s\n", message);
    return condition ? 0 : 1;
}

static uint32_t
put_var(uint8_t *buf, uint32_t pos)
{
    buf[pos++] = (uint8_t)WL_PLAN_EXPR_VAR;
    buf[pos++] = 4;
    buf[pos++] = 0;
    memcpy(buf + pos, "col0", 4);
    return pos + 4;
}

static uint32_t
put_bool(uint8_t *buf, uint32_t pos, uint8_t value)
{
    buf[pos++] = (uint8_t)WL_PLAN_EXPR_BOOL;
    buf[pos++] = value;
    return pos;
}

static uint32_t
put_string_bytes(uint8_t *buf, uint32_t pos, const uint8_t *value,
    size_t len)
{
    buf[pos++] = (uint8_t)WL_PLAN_EXPR_CONST_STR;
    buf[pos++] = (uint8_t)len;
    buf[pos++] = (uint8_t)(len >> 8);
    memcpy(buf + pos, value, len);
    return pos + (uint32_t)len;
}

static uint32_t
put_string(uint8_t *buf, uint32_t pos, const char *value)
{
    return put_string_bytes(buf, pos, (const uint8_t *)value, strlen(value));
}

static uint32_t
put_call(uint8_t *buf, uint32_t pos, const char *name, uint32_t nargs)
{
    size_t len = strlen(name);
    buf[pos++] = (uint8_t)WL_PLAN_EXPR_EXTENSION_CALL;
    buf[pos++] = (uint8_t)len;
    buf[pos++] = (uint8_t)(len >> 8);
    memcpy(buf + pos, name, len);
    pos += (uint32_t)len;
    memcpy(buf + pos, &nargs, sizeof(nargs));
    return pos + sizeof(nargs);
}

static uint32_t
put_call_abi(uint8_t *buf, uint32_t pos, const char *name, uint32_t nargs,
    uint64_t identity, uint32_t version)
{
    size_t len = strlen(name);
    buf[pos++] = (uint8_t)WL_PLAN_EXPR_EXTENSION_CALL_ABI;
    buf[pos++] = (uint8_t)len;
    buf[pos++] = (uint8_t)(len >> 8);
    memcpy(buf + pos, name, len);
    pos += (uint32_t)len;
    buf[pos++] = (uint8_t)nargs;
    buf[pos++] = (uint8_t)(nargs >> 8);
    buf[pos++] = (uint8_t)(nargs >> 16);
    buf[pos++] = (uint8_t)(nargs >> 24);
    for (uint32_t shift = 0; shift < 8; shift++)
        buf[pos++] = (uint8_t)(identity >> (shift * 8));
    buf[pos++] = (uint8_t)version;
    buf[pos++] = (uint8_t)(version >> 8);
    buf[pos++] = (uint8_t)(version >> 16);
    buf[pos++] = (uint8_t)(version >> 24);
    return pos;
}

static int
run(const uint8_t *buf, uint32_t size,
    const wl_columnar_expr_context_t *ctx, int64_t *value,
    wl_columnar_expr_status_t expected)
{
    wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_OK;
    int64_t row = 7;
    int rc = wl_columnar_expr_eval_run_ctx(buf, size, &row, 1, value, ctx,
            &status);
    return check(rc == (int)expected && status == expected,
               "evaluator status");
}

int
main(void)
{
    uint32_t int_type = WIRELOG_EXTENSION_VALUE_INT64;
    uint32_t bool_type = WIRELOG_EXTENSION_VALUE_BOOL;
    wirelog_extension_descriptor_t descriptor = {
        WIRELOG_EXTENSION_ABI_VERSION, sizeof(descriptor), "test.pred", 1,
        &int_type, WIRELOG_EXTENSION_VALUE_BOOL, extension_callback, NULL, NULL
    };
    wirelog_extension_descriptor_t scalar_descriptor = {
        WIRELOG_EXTENSION_ABI_VERSION, sizeof(scalar_descriptor),
        "test.scalar", 1, &int_type, WIRELOG_EXTENSION_VALUE_INT64,
        extension_callback, NULL, NULL
    };
    wirelog_extension_registry_t *registry =
        wirelog_extension_registry_create();
    wirelog_extension_snapshot_t *snapshot;
    wl_columnar_expr_context_t ctx;
    uint8_t buf[64];
    uint32_t size;
    int64_t value = 0;
    int failures = 0;

    failures += check_snapshot_clears_extension_status();

    failures += check(registry != NULL, "registry create");
    descriptor.addon_abi_identity = UINT64_C(0x0123456789abcdef);
    descriptor.addon_abi_version = 7;
    scalar_descriptor.addon_abi_identity = UINT64_C(0x1020304050607080);
    scalar_descriptor.addon_abi_version = 9;
    failures += check(wirelog_extension_register(registry, &descriptor) == 0,
            "register predicate");
    failures += check(wirelog_extension_register(registry,
            &scalar_descriptor) == 0, "register scalar callback");
    snapshot = wirelog_extension_snapshot_acquire(registry);
    ctx.intern = NULL;
    ctx.extensions = snapshot;
    ctx.allow_extension_scalar_result = false;
    ctx.configured_worker_count = 1;
    ctx.active_worker_count = 1;
    ctx.parallel_execution = false;

    /* ABI-tagged calls use explicit little-endian identity/version fields. */
    size = put_var(buf, 0);
    size = put_call_abi(buf, size, "test.pred", 1,
            UINT64_C(0x0123456789abcdef), 7);
    failures += run(buf, size, &ctx, &value, WL_COLUMNAR_EXPR_OK);
    failures += check(value == 1, "ABI-tagged predicate result");
    callback_calls = 0;
    {
        uint32_t identity_pos = 7 + 1 + 2
            + (uint32_t)strlen("test.pred") + 4;
        buf[identity_pos] ^= 1;
        failures += run(buf, size, &ctx, &value,
                WL_COLUMNAR_EXPR_EXTENSION_ABI_MISMATCH);
        failures += check(callback_calls == 0,
                "identity mismatch suppresses callback");
        buf[identity_pos] ^= 1;
        buf[identity_pos + 8] ^= 1;
        failures += run(buf, size, &ctx, &value,
                WL_COLUMNAR_EXPR_EXTENSION_ABI_MISMATCH);
        failures += check(callback_calls == 0,
                "version mismatch suppresses callback");
        buf[identity_pos + 8] ^= 1;
        failures += run(buf, size - 1, &ctx, &value,
                WL_COLUMNAR_EXPR_EXTENSION_MALFORMED);
    }
    {
        wl_col_session_t abi_session = { 0 };
        abi_session.base.extension_snapshot = snapshot;
        abi_session.delta_pool = delta_pool_create(16, sizeof(col_rel_t),
                4096);
        size = put_var(buf, 0);
        size = put_call_abi(buf, size, "test.pred", 1,
                UINT64_C(0x0123456789abcdef), 7);
        failures += run_filter_case(buf, size, &abi_session, 2, 0);
        size = put_var(buf, 0);
        size = put_call_abi(buf, size, "test.scalar", 1,
                UINT64_C(0x1020304050607080), 9);
        abi_session.base.extension_snapshot = snapshot;
        callback_mode = 2;
        failures += run_map_error_case(buf, size, &abi_session, 0);
        callback_mode = 0;
        delta_pool_destroy(abi_session.delta_pool);
    }

    {
        wl_col_session_t session = { 0 };
        session.base.extension_snapshot = snapshot;
        session.intern = NULL;
        session.delta_pool = delta_pool_create(16, sizeof(col_rel_t), 4096);
        size = put_var(buf, 0);
        size = put_call(buf, size, "test.pred", 1);
        callback_mode = 0;
        callback_calls = 0;
        fail_on_call = 0;
        failures += run_filter_case(buf, size, &session, 2, 0);
        callback_calls = 0;
        fail_on_call = 2;
        failures += run_filter_case(buf, size, &session, 0,
                WL_COLUMNAR_EXPR_CALLBACK_FAILURE);
        fail_on_call = 0;
        delta_pool_destroy(session.delta_pool);
    }

    size = put_var(buf, 0);
    size = put_call(buf, size, "test.pred", 1);
    callback_mode = 0;
    failures += run(buf, size, &ctx, &value, WL_COLUMNAR_EXPR_OK);
    failures += check(value == 1, "true result");

    /* Legacy/policy-zero callbacks are valid at serial width but must not be
     * invoked when the actual evaluator width is concurrent. */
    callback_calls = 0;
    ctx.configured_worker_count = 4;
    ctx.active_worker_count = 2;
    ctx.parallel_execution = true;
    failures += run(buf, size, &ctx, &value,
            WL_COLUMNAR_EXPR_CALLBACK_POLICY);
    failures += check(callback_calls == 0,
            "unsafe callback is rejected before invocation");
    ctx.active_worker_count = 1;
    ctx.parallel_execution = false;
    failures += run(buf, size, &ctx, &value, WL_COLUMNAR_EXPR_OK);

    buf[0] = (uint8_t)WL_PLAN_EXPR_CONST_INT;
    memset(buf + 1, 0, 8);
    size = put_call(buf, 9, "test.pred", 1);
    failures += run(buf, size, &ctx, &value, WL_COLUMNAR_EXPR_OK);
    failures += check(value == 0, "false result");

    /* MAP/head evaluation may accept an INT64 scalar result, while ordinary
     * FILTER evaluation remains BOOL-only. */
    size = put_var(buf, 0);
    size = put_call(buf, size, "test.scalar", 1);
    callback_mode = 2;
    ctx.allow_extension_scalar_result = true;
    failures += run(buf, size, &ctx, &value, WL_COLUMNAR_EXPR_OK);
    failures += check(value == 1, "scalar result in map context");
    ctx.allow_extension_scalar_result = false;
    failures += run(buf, size, &ctx, &value,
            WL_COLUMNAR_EXPR_INVALID_RESULT);
    callback_mode = 0;

    size = put_var(buf, 0);
    size = put_call(buf, size, "test.pred", 1);

    callback_mode = 1;
    failures += run(buf, size, &ctx, &value,
            WL_COLUMNAR_EXPR_CALLBACK_FAILURE);
    callback_mode = 2;
    failures += run(buf, size, &ctx, &value,
            WL_COLUMNAR_EXPR_INVALID_RESULT);
    callback_mode = 3;
    failures += run(buf, size, &ctx, &value,
            WL_COLUMNAR_EXPR_INVALID_RESULT);
    callback_mode = 0;

    /* A BOOL argument is accepted and retains its type through the stack. */
    descriptor.arity = 1;
    descriptor.argument_types = &bool_type;
    failures += check(wirelog_extension_unregister(registry, "test.pred") == 0,
            "unregister predicate");
    failures += check(wirelog_extension_register(registry, &descriptor) == 0,
            "register bool predicate");
    wirelog_extension_snapshot_release(snapshot);
    snapshot = wirelog_extension_snapshot_acquire(registry);
    ctx.extensions = snapshot;
    size = put_bool(buf, 0, 1);
    size = put_call(buf, size, "test.pred", 1);
    callback_mode = 0;
    failures += run(buf, size, &ctx, &value, WL_COLUMNAR_EXPR_OK);
    failures += check(value == 1, "bool argument");

    /* INT64 is a deterministic type mismatch for a BOOL parameter. */
    buf[0] = (uint8_t)WL_PLAN_EXPR_CONST_INT;
    memset(buf + 1, 0, 8);
    size = put_call(buf, 9, "test.pred", 1);
    failures += run(buf, size, &ctx, &value,
            WL_COLUMNAR_EXPR_TYPE_MISMATCH);

    size = put_string(buf, 0, "unsupported");
    size = put_call(buf, size, "test.pred", 1);
    failures += run(buf, size, &ctx, &value,
            WL_COLUMNAR_EXPR_TYPE_MISMATCH);

    /* String literals are passed as borrowed plan-buffer bytes, including
     * empty and embedded-NUL values; they do not require an intern table. */
    {
        static const uint8_t utf8_nul[] = { 0xc3, 0xa9, 0x00, 0xf0, 0x9f,
                                            0x8c, 0x8d };
        uint32_t string_type = WIRELOG_EXTENSION_VALUE_STRING;
        descriptor.argument_types = &string_type;
        failures += check(wirelog_extension_unregister(registry,
                "test.pred") == 0, "unregister int predicate");
        failures += check(wirelog_extension_register(registry, &descriptor)
                == 0, "register string predicate");
        wirelog_extension_snapshot_release(snapshot);
        snapshot = wirelog_extension_snapshot_acquire(registry);
        ctx.extensions = snapshot;

        /* A typed string column variable is resolved through the intern
         * table and passed as borrowed bytes, not as its numeric ID. */
        {
            wl_intern_t *string_intern = wl_intern_create();
            int64_t string_id = string_intern
                ? wl_intern_put(string_intern, "café") : -1;
            int64_t string_row = string_id;
            wl_columnar_expr_status_t string_status = WL_COLUMNAR_EXPR_OK;
            int64_t string_value = 0;
            failures += check(string_intern != NULL && string_id >= 0,
                    "create string intern table");
            if (string_intern && string_id >= 0) {
                ctx.intern = string_intern;
                expected_string = (const uint8_t *)"caf\xc3\xa9";
                expected_string_length = 5;
                buf[0] = (uint8_t)WL_PLAN_EXPR_VAR_STRING;
                buf[1] = 4;
                buf[2] = 0;
                memcpy(buf + 3, "col0", 4);
                size = put_call(buf, 7, "test.pred", 1);
                callback_mode = 4;
                failures += check(wl_columnar_expr_eval_run_ctx(buf, size,
                        &string_row, 1, &string_value, &ctx, &string_status)
                        == WL_COLUMNAR_EXPR_OK
                        && string_status == WL_COLUMNAR_EXPR_OK
                        && string_value == 1,
                        "string column callback argument");
                ctx.intern = NULL;
            }
            wl_intern_free(string_intern);
        }

        expected_string = utf8_nul;
        expected_string_length = sizeof(utf8_nul);
        size = put_string_bytes(buf, 0, utf8_nul, sizeof(utf8_nul));
        size = put_call(buf, size, "test.pred", 1);
        callback_mode = 4;
        failures += run(buf, size, &ctx, &value, WL_COLUMNAR_EXPR_OK);
        failures += check(value == 1, "string literal bytes and length");

        expected_string = (const uint8_t *)"";
        expected_string_length = 0;
        size = put_string_bytes(buf, 0, expected_string, 0);
        size = put_call(buf, size, "test.pred", 1);
        failures += run(buf, size, &ctx, &value, WL_COLUMNAR_EXPR_OK);
        failures += check(value == 1, "empty string literal");

        /* A string literal remains incompatible with an INT64 descriptor. */
        descriptor.argument_types = &int_type;
        failures += check(wirelog_extension_unregister(registry,
                "test.pred") == 0, "unregister string predicate");
        failures += check(wirelog_extension_register(registry, &descriptor)
                == 0, "restore int predicate");
        wirelog_extension_snapshot_release(snapshot);
        snapshot = wirelog_extension_snapshot_acquire(registry);
        ctx.extensions = snapshot;
        failures += run(buf, size, &ctx, &value,
                WL_COLUMNAR_EXPR_TYPE_MISMATCH);
        descriptor.argument_types = &bool_type;
        failures += check(wirelog_extension_unregister(registry,
                "test.pred") == 0, "unregister temporary int predicate");
        failures += check(wirelog_extension_register(registry, &descriptor)
                == 0, "restore bool predicate");
        wirelog_extension_snapshot_release(snapshot);
        snapshot = wirelog_extension_snapshot_acquire(registry);
        ctx.extensions = snapshot;
        callback_mode = 0;
    }

    /* Truncated extension metadata is malformed, not an invocation. */
    buf[0] = (uint8_t)WL_PLAN_EXPR_EXTENSION_CALL;
    buf[1] = 4;
    buf[2] = 0;
    buf[3] = 't';
    failures += run(buf, 4, &ctx, &value,
            WL_COLUMNAR_EXPR_EXTENSION_MALFORMED);

    callback_mode = 0;
    {
        wl_col_session_t malformed_session = { 0 };
        malformed_session.base.extension_snapshot = snapshot;
        failures += run_filter_case(buf, 4, &malformed_session, 0,
                WL_COLUMNAR_EXPR_EXTENSION_MALFORMED);
    }

    /* Missing addon and wrong arity must not become false. */
    size = put_bool(buf, 0, 1);
    size = put_call(buf, size, "missing.pred", 1);
    failures += run(buf, size, &ctx, &value,
            WL_COLUMNAR_EXPR_MISSING_EXTENSION);
    size = put_call(buf, 0, "test.pred", 0);
    failures += run(buf, size, &ctx, &value,
            WL_COLUMNAR_EXPR_ARITY_MISMATCH);

    /* MAP/head must preserve the evaluator's extension status for its
    * coordinator instead of collapsing missing addons into ERANGE. */
    {
        wl_col_session_t map_session = { 0 };
        map_session.base.extension_snapshot = snapshot;
        size = put_bool(buf, 0, 1);
        size = put_call(buf, size, "missing.pred", 1);
        failures += run_map_error_case(buf, size, &map_session,
                WL_COLUMNAR_EXPR_MISSING_EXTENSION);
    }

    /* The acquired snapshot keeps the unregistered callback alive. */
    failures += check(wirelog_extension_unregister(registry, "test.pred") == 0,
            "unregister while snapshot is live");
    size = put_bool(buf, 0, 1);
    size = put_call(buf, size, "test.pred", 1);
    failures += run(buf, size, &ctx, &value, WL_COLUMNAR_EXPR_OK);
    failures += check(wirelog_extension_unregister(registry, "test.scalar")
            == 0, "unregister scalar callback");
    wirelog_extension_snapshot_release(snapshot);
    failures += check(wirelog_extension_registry_destroy(registry) == 0,
            "registry destroy");
    return failures != 0;
}
