/* #2299 original extension payload, policy, typed argument and lifetime oracles. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/wirelog-advanced.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog-internal.h"
#include "../wirelog/extension_registry_private.h"
#include "controlled_step_observation.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do{if (!(x)){fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                                  #x); exit(1);}}while (0)
extern int wl_columnar_expr_test_extension_decode(const uint8_t *, uint32_t,
    uint32_t *, uint32_t *, uint64_t *, wl_evaluation_control_t *, uint64_t);
extern int wl_columnar_expr_test_extension_invoke(const uint8_t *, uint32_t,
    uint32_t, uint64_t, uint8_t,
    const wirelog_extension_value_t *, uint32_t,
    const wl_columnar_expr_context_t *, int64_t *, bool *,
    wl_columnar_expr_status_t *, wl_evaluation_control_t *, uint64_t);
extern uint32_t wl_columnar_expr_test_callback_depth(void);
extern void (*wl_columnar_expr_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t, bool);
static int owner;
static wl_evaluation_control_t *begin(uint64_t limit){
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c) &&
        !wl_evaluation_control_attach(c,
        &owner) && !wl_evaluation_control_begin(c, &owner)); return c;
}
static uint64_t id(wl_evaluation_control_t *c){
    return wl_evaluation_control_active_attempt_id(c);
}
static void finish(wl_evaluation_control_t *c, int rc){
    CHECK(wl_evaluation_control_finish(c, &owner, rc > 0?rc:0, 0) == rc &&
        !wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
}
static uint32_t call(uint8_t *bytes, uint8_t tag, const char *name,
    uint16_t length, uint32_t nargs, uint64_t identity, uint32_t version){
    uint32_t n = 0; bytes[n++] = tag; bytes[n++] = (uint8_t)length;
    bytes[n++] = (uint8_t)(length>>8); memcpy(bytes+n, name, length);
    n += length;
    for (unsigned i = 0; i < 4; ++i)bytes[n++] = (uint8_t)(nargs>>(8*i));
    if (tag == WL_PLAN_EXPR_EXTENSION_CALL_ABI) {
        for (unsigned i = 0; i < 8;
            ++i) bytes[n++] = (uint8_t)(identity>>(8*i));
        for (unsigned i = 0; i < 4; ++i)bytes[n++] = (uint8_t)(version>>(8*i));
    }
    return n;
}
static void decode_oracles(void){
    for (unsigned abi = 0; abi < 2; ++abi) {
        uint8_t bytes[300];
        uint8_t tag =
            abi?WL_PLAN_EXPR_EXTENSION_CALL_ABI:WL_PLAN_EXPR_EXTENSION_CALL;
        uint32_t n = call(bytes, tag, "probe", 5, UINT32_MAX,
                UINT64_C(0x12345678abcdef01), 7);
        for (uint32_t length = 1; length <= n; ++length) {
            /* D0:3; D1:6; ABI D2:(11+5)=16; validbase16/ABI28. */
            unsigned packets = length < 3?3:length < 12?6:abi &&
                length < 24?16:abi?28:16;
            bool malformed = length < n;
            for (unsigned below = 0; below < 2; ++below) {
                wl_evaluation_control_t *c = begin(UINT64_C(224)*packets-below);
                uint32_t end = 999, start = 999; uint64_t packed = 999;
                int rc = wl_columnar_expr_test_extension_decode(bytes, length,
                        &end, &start, &packed, c, id(c));
                if (below)CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                        end == 999 && start == 999 && packed == 999);
                else CHECK(!rc && end == length &&
                        start == (malformed?UINT32_MAX:3) &&
                        packed == (malformed?0:((uint64_t)UINT32_MAX<<16)|5));
                uint64_t used = 0;
                CHECK(!wl_evaluation_control_test_active_consumed(c, id(c),
                    &used) && used == UINT64_C(224)*(packets-below));
                finish(c, rc);
            }
            for (unsigned prefix = 1; prefix <= packets; ++prefix) {
                wl_evaluation_control_t *c = begin(UINT64_C(224)*prefix-1);
                uint32_t end = 999, start = 999; uint64_t packed = 999;
                CHECK(wl_columnar_expr_test_extension_decode(bytes, length,
                    &end, &start, &packed, c,
                    id(c)) == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                    end == 999 && start == 999 && packed == 999);
                finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
            }
        }
        char maximum[256]; memset(maximum, 'a', 255); maximum[255] = 0;
        n = call(bytes, tag, maximum, 255, 0, 0, 0);
        for (unsigned below = 0; below < 2; ++below) {
            wl_evaluation_control_t *c = begin(UINT64_C(224)*(abi?278:266)-
                    below);
            uint32_t end = 999, start = 999; uint64_t packed = 999;
            int rc = wl_columnar_expr_test_extension_decode(bytes, n, &end,
                    &start, &packed, c, id(c));
            if (below)CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                    end == 999 && start == 999 && packed == 999);
            else CHECK(!rc && end == n && start == 3 && packed == 255);
            finish(c, rc);
        }
        const uint16_t invalid[] = {0, 256, UINT16_MAX};
        for (unsigned i = 0; i < 3; ++i) {
            bytes[0] = tag; bytes[1] = (uint8_t)invalid[i];
            bytes[2] = (uint8_t)(invalid[i]>>8);
            wl_evaluation_control_t *c = begin(6*224);
            uint32_t end = 999, start = 999; uint64_t packed = 999;
            CHECK(!wl_columnar_expr_test_extension_decode(bytes, 3, &end,
                &start, &packed, c,
                id(c)) && end == 3 && start == UINT32_MAX && !packed);
            finish(c, 0);
        }
    }
}
static unsigned calls;
static uint32_t actual_type = WIRELOG_EXTENSION_VALUE_BOOL,
    actual_size = sizeof(uint8_t);
static int callback_rc;
static bool callback_tls_effect;
static void callback_stop(void);
static int callback(const wirelog_extension_value_t *args, uint32_t nargs,
    wirelog_extension_value_t *result, void *data){
    (void)data;
    CHECK(nargs == 1 && args[0].type == WIRELOG_EXTENSION_VALUE_INT64 &&
        args[0].as.int64_value == 7);
    CHECK(!*wirelog_extension_last_error());
    if (callback_tls_effect)wl_extension_error_set("callback effect");
    callback_stop();
    ++calls;
    result->type = actual_type; result->size = actual_size;
    if (actual_type == WIRELOG_EXTENSION_VALUE_BOOL)result->as.bool_value = 1;
    else {
        result->as.int64_value = 42;
    }
    return callback_rc;
}
static void invoke_oracles(void){
    wirelog_extension_registry_t *registry =
        wirelog_extension_registry_create(); CHECK(registry);
    uint32_t type = WIRELOG_EXTENSION_VALUE_INT64;
    wirelog_extension_descriptor_t descriptor =
    {.abi_version = WIRELOG_EXTENSION_ABI_VERSION, .size = sizeof(descriptor),
     .name = "probe",
     .arity = 1,
     .argument_types = &type, .result_type = WIRELOG_EXTENSION_VALUE_INT64,
     .invoke = callback, .addon_abi_identity = 123, .addon_abi_version = 7};
    CHECK(!wirelog_extension_register(registry, &descriptor));
    wirelog_extension_snapshot_t *snapshot =
        wirelog_extension_snapshot_acquire(registry); CHECK(snapshot);
    wl_columnar_expr_context_t context = {.extensions = snapshot,
                                          .allow_extension_scalar_result = true,
                                          .session_key = &owner,
                                          .configured_worker_count = 4,
                                          .active_worker_count = 1};
    wirelog_extension_value_t arg = {.type = WIRELOG_EXTENSION_VALUE_INT64,
                                     .size = sizeof(int64_t),
                                     .as.int64_value = 7};
    for (unsigned abi = 0; abi < 2; ++abi) {
        uint8_t bytes[32],
            tag =
            abi?WL_PLAN_EXPR_EXTENSION_CALL_ABI:WL_PLAN_EXPR_EXTENSION_CALL;
        uint32_t n = call(bytes, tag, "probe", 5, 1, 123, 7);
        /* One entry probe: lookup4+R6+F1+B1+L10+E1+P6=29T.
         * Runtime name6T+ABI12T; sixteenfixed+A1 +lookup29 +lease5.
         * KeynonNULL but empty TLS gives D0, totals57T/base69T/ABI. */
        unsigned packets = abi?69:57;
        for (unsigned below = 0; below < 2; ++below) {
            wl_evaluation_control_t *c = begin(UINT64_C(224)*packets-below);
            int64_t value = 999; bool failed = false;
            wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_OK; calls = 0;
            wl_extension_error_set("prior");
            int rc = wl_columnar_expr_test_extension_invoke(bytes, n, 3,
                    (UINT64_C(1)<<16)|5, tag, &arg, 1, &context, &value,
                    &failed, &status, c, id(c));
            if (below)CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                    value == 999 && !failed && status == WL_COLUMNAR_EXPR_OK &&
                    calls == 1);
            else CHECK(!rc && value == 1 && !failed &&
                    status == WL_COLUMNAR_EXPR_OK && calls == 1);
            CHECK(!wl_columnar_expr_test_callback_depth()); finish(c, rc);
        }
        for (unsigned prefix = 1; prefix <= packets; ++prefix) {
            wl_evaluation_control_t *c = begin(UINT64_C(224)*prefix-1);
            int64_t value = 999; bool failed = false;
            wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_OK; calls = 0;
            int rc = wl_columnar_expr_test_extension_invoke(bytes, n, 3,
                    (UINT64_C(1)<<16)|5, tag, &arg, 1, &context, &value,
                    &failed, &status, c, id(c));
            CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && value == 999 &&
                !failed && status == WL_COLUMNAR_EXPR_OK &&
                !wl_columnar_expr_test_callback_depth()); finish(c, rc);
        }
    }
    wirelog_extension_snapshot_release(snapshot);
    CHECK(!wirelog_extension_unregister(registry,
        "probe") && !wirelog_extension_registry_destroy(registry));
}

static wirelog_extension_value_t expected_args[2];
static uint32_t expected_count;
static int parity_callback(const wirelog_extension_value_t *args,
    uint32_t count, wirelog_extension_value_t *result, void *data){
    (void)data;
    CHECK(count == expected_count && !*wirelog_extension_last_error());
    for (uint32_t i = 0; i < count; ++i) {
        CHECK(args[i].type == expected_args[i].type);
        if (args[i].type ==
            WIRELOG_EXTENSION_VALUE_STRING) CHECK(args[i].size ==
                expected_args[i].as.string_value.length &&
                args[i].as.string_value.length ==
                expected_args[i].as.string_value.length &&
                !memcmp(args[i].as.string_value.data,
                expected_args[i].as.string_value.data,
                args[i].as.string_value.length));
        else if (args[i].type ==
            WIRELOG_EXTENSION_VALUE_BOOL) CHECK(args[i].as.bool_value ==
                expected_args[i].as.bool_value && args[i].size == 1);
        else CHECK(args[i].as.int64_value == expected_args[i].as.int64_value &&
                args[i].size == sizeof(int64_t));
    }
    ++calls; result->type = actual_type; result->size = actual_size;
    if (actual_type == WIRELOG_EXTENSION_VALUE_BOOL)result->as.bool_value = 1;
    else {
        result->as.int64_value = 42;
    }
    return callback_rc;
}
static uint32_t integer(uint8_t *bytes, uint32_t pos, int64_t value){
    bytes[pos++] = WL_PLAN_EXPR_CONST_INT; memcpy(bytes+pos, &value, 8);
    return pos+8;
}
static uint32_t string_bytes(uint8_t *bytes, uint32_t pos, const char *text,
    uint16_t length){
    bytes[pos++] = WL_PLAN_EXPR_CONST_STR;
    bytes[pos++] = (uint8_t)length; bytes[pos++] = (uint8_t)(length>>8);
    memcpy(bytes+pos, text, length); return pos+length;
}
static void compare_scalar(const uint8_t *bytes, uint32_t n, const int64_t *row,
    uint32_t width, const wl_columnar_expr_context_t *context,
    wl_columnar_expr_status_t expected_status){
    int64_t raw = 999, value = 999;
    wl_columnar_expr_status_t raw_status = WL_COLUMNAR_EXPR_OK,
        status = WL_COLUMNAR_EXPR_OK;
    calls = 0; wl_extension_error_set("seed");
    int raw_rc = wl_columnar_expr_eval_run_ctx(bytes, n, row, width, &raw,
            context, &raw_status);
    unsigned raw_calls = calls; char diagnostic[256];
    snprintf(diagnostic, sizeof(diagnostic), "%s",
        wirelog_extension_last_error());
    CHECK(raw_status == expected_status && raw_rc == (int)raw_status);
    wl_evaluation_control_t *c = begin(0);
    bool supported = false, failed = false;
    wl_columnar_expr_attempt_scalar_t *program = NULL;
    CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(bytes, n, NULL,
        &program, &supported, c, id(c)) && supported && program);
    CHECK(wl_columnar_expr_scalar_schema_unknown(program));
    wirelog_column_type_t type = WIRELOG_TYPE_BOOL; supported = true;
    CHECK(!wl_columnar_expr_scalar_result_type_attempt_controlled(program, NULL,
        0, &type, &supported, c,
        id(c)) && !supported && type == WIRELOG_TYPE_BOOL);
    calls = 0; wl_extension_error_set("seed");
    CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(program, row, width,
        context, &value, &failed, &status, c, id(c)));
    CHECK(value == raw && failed == (raw_rc != 0) && status == raw_status &&
        calls == raw_calls &&
        !strcmp(diagnostic,
        wirelog_extension_last_error()) &&
        !wl_columnar_expr_test_callback_depth());
    wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
}
static void scalar_parity(void){
    for (unsigned abi = 0; abi < 2; ++abi)for (unsigned scenario = 0;
            scenario < 26;
            ++scenario){
#ifndef WL_MBEDTLS_ENABLED
            if (scenario == 25) continue; /* RFC producer requires its provider. */
#endif
            uint8_t bytes[2048],
                tag =
                abi?WL_PLAN_EXPR_EXTENSION_CALL_ABI:WL_PLAN_EXPR_EXTENSION_CALL;
            uint32_t n = 0;
            wl_intern_t *intern = wl_intern_create(); CHECK(intern);
            wirelog_extension_registry_t *registry =
                wirelog_extension_registry_create(); CHECK(registry);
            uint32_t arg_type = WIRELOG_EXTENSION_VALUE_INT64,
                result_type = WIRELOG_EXTENSION_VALUE_INT64;
            expected_count = 1;
            expected_args[0].type = WIRELOG_EXTENSION_VALUE_INT64;
            expected_args[0].as.int64_value = 7;
            actual_type = WIRELOG_EXTENSION_VALUE_BOOL;
            actual_size = sizeof(uint8_t); callback_rc = 0;
            uint32_t nargs = 1; int64_t row = 0; uint32_t width = 0;
            wl_columnar_expr_context_t context = {.intern = intern,
                                                  .allow_extension_scalar_result
                                                      = true,
                                                  .session_key = &owner,
                                                  .configured_worker_count = 4,
                                                  .active_worker_count = 1};
            wl_columnar_expr_status_t expected = WL_COLUMNAR_EXPR_OK;
            uint64_t identity = 123; uint32_t version = 7;
            if (scenario == 1 || scenario == 2) {
                bytes[n++] = WL_PLAN_EXPR_BOOL; bytes[n++] = 1;
                if (scenario == 1) {
                    arg_type = WIRELOG_EXTENSION_VALUE_BOOL;
                    expected_args[0].type = arg_type;
                    expected_args[0].as.bool_value = 1;
                    result_type = WIRELOG_EXTENSION_VALUE_BOOL;
                    actual_type = WIRELOG_EXTENSION_VALUE_INT64;
                    actual_size = 8;
                }else expected = WL_COLUMNAR_EXPR_TYPE_MISMATCH;
            }else if (scenario == 3) {
                n = integer(bytes, n, 7);
                arg_type = WIRELOG_EXTENSION_VALUE_BOOL;
                expected = WL_COLUMNAR_EXPR_TYPE_MISMATCH;
            }else if (scenario == 4) {
                n = string_bytes(bytes, n, "a\0b", 3);
                arg_type = WIRELOG_EXTENSION_VALUE_STRING;
                expected_args[0].type = arg_type;
                expected_args[0].as.string_value.data = "a\0b";
                expected_args[0].as.string_value.length = 3;
            }else if (scenario == 5) {
                bytes[n++] = WL_PLAN_EXPR_VAR_STRING; bytes[n++] = 4;
                bytes[n++] = 0; memcpy(bytes+n, "col0", 4); n += 4;
                row = wl_intern_put(intern, "abc"); CHECK(row >= 0); width = 1;
                arg_type = WIRELOG_EXTENSION_VALUE_STRING;
                expected_args[0].type = arg_type;
                expected_args[0].as.string_value.data = "abc";
                expected_args[0].as.string_value.length = 3;
            }else if (scenario == 6) {
                n = integer(bytes, n, 7); n = integer(bytes, n, 7);
                bytes[n++] = WL_PLAN_EXPR_CMP_EQ;
                expected_args[0].as.int64_value = 1;
            }else if (scenario == 24 || scenario == 25) {
                if (scenario == 24) {
                    n = string_bytes(bytes, n, "a", 1);
                    bytes[n++] = WL_PLAN_EXPR_STR_FN_TO_UPPER;
                } else {
                    const char *ns = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
                    n = string_bytes(bytes, n, ns, 36);
                    n = string_bytes(bytes, n, "www.example.com", 15);
                    bytes[n++] = WL_PLAN_EXPR_STR_FN_UUID5_RFC;
                }
                /* The earlier qualified producer supplies the value oracle.
                 * Raw callback ABI independently requires an INT64 intern ID,
                 * despite STRING schema metadata on this result. */
                int64_t produced = -1;
                wl_columnar_expr_status_t producer_status;
                CHECK(!wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0,
                    &produced, &context, &producer_status) && produced >= 0 &&
                    producer_status == WL_COLUMNAR_EXPR_OK &&
                    wl_intern_reverse(intern, produced));
                expected_args[0].as.int64_value = produced;
            }else if (scenario == 20) {
                for (unsigned i = 0; i < COL_FILTER_STACK;
                    ++i) n = integer(bytes, n, (int64_t)i);
                bytes[n++] = WL_PLAN_EXPR_BOOL; bytes[n++] = 1;
                expected_args[0].as.int64_value = COL_FILTER_STACK-1;
            }else if (scenario == 21) {
                expected_count = 0; nargs = 0;
            }else n = integer(bytes, n, 7);
            switch (scenario) {
            case 7: expected = WL_COLUMNAR_EXPR_MISSING_EXTENSION; break;
            case 8: identity = 124;
                if (abi) expected = WL_COLUMNAR_EXPR_EXTENSION_ABI_MISMATCH;
                break;
            case 9: version = 8;
                if (abi) expected = WL_COLUMNAR_EXPR_EXTENSION_ABI_MISMATCH;
                break;
            case 10: context.parallel_execution = true;
                context.active_worker_count = 2;
                expected = WL_COLUMNAR_EXPR_CALLBACK_POLICY; break;
            case 11: nargs = 0; expected = WL_COLUMNAR_EXPR_ARITY_MISMATCH;
                break;
            case 12: nargs = UINT32_MAX;
                expected = WL_COLUMNAR_EXPR_ARITY_MISMATCH; break;
            case 13: result_type = WIRELOG_EXTENSION_VALUE_STRING;
                expected = WL_COLUMNAR_EXPR_INVALID_RESULT; break;
            case 14: callback_rc = -1;
                expected = WL_COLUMNAR_EXPR_CALLBACK_FAILURE; break;
            case 15: actual_type = WIRELOG_EXTENSION_VALUE_STRING;
                expected = WL_COLUMNAR_EXPR_INVALID_RESULT; break;
            case 16: actual_size = 0;
                expected = WL_COLUMNAR_EXPR_INVALID_RESULT; break;
            case 17: actual_type = WIRELOG_EXTENSION_VALUE_INT64;
                actual_size = 1; expected = WL_COLUMNAR_EXPR_INVALID_RESULT;
                break;
            case 18: context.allow_extension_scalar_result = false;
                expected = WL_COLUMNAR_EXPR_INVALID_RESULT; break;
            case 19: result_type = WIRELOG_EXTENSION_VALUE_BOOL;
                actual_type = WIRELOG_EXTENSION_VALUE_INT64; actual_size = 8;
                break;
            default: break;
            }
            wirelog_extension_descriptor_t descriptor =
            {.abi_version = WIRELOG_EXTENSION_ABI_VERSION,
             .size = sizeof(descriptor), .name = "probe",
             .arity =
                 expected_count,
             .argument_types = expected_count?&arg_type:NULL,
             .result_type = result_type, .invoke = parity_callback,
             .addon_abi_identity =
                 123, .addon_abi_version = 7};
            CHECK(!wirelog_extension_register(registry, &descriptor));
            wirelog_extension_snapshot_t *snapshot =
                wirelog_extension_snapshot_acquire(registry); CHECK(snapshot);
            context.extensions = scenario == 7?NULL:snapshot;
            const char *name = scenario == 22?"missing":"probe";
            if (scenario == 22) expected = WL_COLUMNAR_EXPR_MISSING_EXTENSION;
            n += call(bytes+n, tag, name, (uint16_t)strlen(name), nargs,
                    identity, version);
            if (scenario == 23) n = integer(bytes, n, 999); /* Callback still runs; whole schema stays unknown. */
            compare_scalar(bytes, n, width?&row:NULL, width, &context,
                expected);
            wirelog_extension_snapshot_release(snapshot);
            CHECK(!wirelog_extension_unregister(registry,
                "probe") && !wirelog_extension_registry_destroy(registry));
            wl_intern_free(intern);
        }
    actual_type = WIRELOG_EXTENSION_VALUE_BOOL; actual_size = 1;
    callback_rc = 0;
}
static void malformed_parity(void){
    wirelog_extension_registry_t *registry =
        wirelog_extension_registry_create(); CHECK(registry);
    uint32_t type = WIRELOG_EXTENSION_VALUE_INT64;
    wirelog_extension_descriptor_t descriptor =
    {.abi_version = WIRELOG_EXTENSION_ABI_VERSION, .size = sizeof(descriptor),
     .name = "probe",
     .arity = 1,
     .argument_types = &type, .result_type = WIRELOG_EXTENSION_VALUE_INT64,
     .invoke = parity_callback, .addon_abi_identity = 123,
     .addon_abi_version = 7};
    CHECK(!wirelog_extension_register(registry, &descriptor));
    wirelog_extension_snapshot_t *snapshot =
        wirelog_extension_snapshot_acquire(registry); CHECK(snapshot);
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    wl_columnar_expr_context_t context = {.intern = intern,
                                          .extensions = snapshot,
                                          .allow_extension_scalar_result =
                                              true};
    expected_count = 1; expected_args[0].type = type;
    expected_args[0].as.int64_value = 7;
    for (unsigned abi = 0; abi < 2; ++abi) {
        uint8_t bytes[128],
            tag =
            abi?WL_PLAN_EXPR_EXTENSION_CALL_ABI:WL_PLAN_EXPR_EXTENSION_CALL;
        uint32_t base = integer(bytes, 0, 7),
            n = call(bytes+base, tag, "probe", 5, 1, 123, 7);
        for (uint32_t length = 1; length < n;
            ++length) compare_scalar(bytes, base+length, NULL, 0, &context,
                WL_COLUMNAR_EXPR_EXTENSION_MALFORMED);
        /* Earlier literal must intern before malformed marker; ignored trailing
         * bytes cannot execute a second literal or callback. */
        uint32_t count = wl_intern_count(intern);
        base = string_bytes(bytes, 0, "earlier", 7); bytes[base++] = tag;
        bytes[base++] = 0; bytes[base++] = 0;
        uint32_t end = string_bytes(bytes, base, "trailing", 8);
        compare_scalar(bytes, end, NULL, 0, &context,
            WL_COLUMNAR_EXPR_EXTENSION_MALFORMED);
        CHECK(wl_intern_count(intern) == count+(abi == 0));
        /* Earlier callback failure wins over the later malformed instruction. */
        base = integer(bytes, 0, 7);
        base += call(bytes+base, tag, "probe", 5, 1, 123, 7);
        bytes[base++] = tag; bytes[base++] = 0; bytes[base++] = 0;
        callback_rc = -1;
        compare_scalar(bytes, base, NULL, 0, &context,
            WL_COLUMNAR_EXPR_CALLBACK_FAILURE); callback_rc = 0;
    }
    wirelog_extension_snapshot_release(snapshot);
    CHECK(!wirelog_extension_unregister(registry,
        "probe") && !wirelog_extension_registry_destroy(registry));
    wl_intern_free(intern);
}

extern void (*wl_extension_control_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t);
static unsigned action, stop_phase; static uint64_t stop_progress, successor;
static bool stopped;
static wl_evaluation_control_t *callback_control;
static wl_evaluation_control_report_t original;
static void stop(wl_evaluation_control_t *c, unsigned phase, uint64_t progress,
    bool failed){
    (void)failed;
    if (stopped || phase != stop_phase || progress != stop_progress) return;
    stopped = true;
    if (action == 0)wl_evaluation_control_request_cancel(c);
    else if (action == 1)CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else if (action == 2) {
        CHECK(!wl_evaluation_control_finish(c, &owner, 0,
            0) && !wl_evaluation_control_report(c, &original) &&
            !wl_evaluation_control_reset(c) && !wl_evaluation_control_begin(c,
            &owner)); successor = id(c);
    }else {
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
        wl_evaluation_control_request_cancel(c);
    }
}
static void registry_stop(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress){
    stop(c, phase, progress, false);
}
static void callback_stop(void){
    if (callback_control) stop(callback_control, 777, 0, false);
}
static int expected_rc(void){
    return action == 0?WL_EVALUATION_CONTROL_CANCELLED:action ==
           1?WL_EVALUATION_CONTROL_WORK_LIMIT:action == 2?EINVAL:EOVERFLOW;
}
static void retry(wl_evaluation_control_t *c, int rc){
    if (action == 2) {
        wl_evaluation_control_report_t report; uint64_t used = 999;
        CHECK(!wl_evaluation_control_report(c,
            &report) && report.attempt_id == original.attempt_id &&
            report.consumed == original.consumed &&
            report.result == original.result &&
            report.stop_reason == original.stop_reason &&
            report.execution_status == original.execution_status &&
            report.cleanup_status == original.cleanup_status);
        CHECK(!wl_evaluation_control_test_active_consumed(c, successor,
            &used) && !used && !wl_evaluation_control_finish(c, &owner, 0, 0));
    }else CHECK(wl_evaluation_control_finish(c, &owner, rc > 0?rc:0, 0) == rc);
    CHECK(!wl_evaluation_control_reset(c) && !wl_evaluation_control_configure(c,
        0) && !wl_evaluation_control_begin(c, &owner));
}
static void decode_stops(void){
    /* Full ABI probe has entry/D0/D1/D2/final and reads at all23 payload
    * offsets except tag0: length1,2; name3..7; nargs8..11; ABI12..23. */
    uint8_t bytes[32];
    uint32_t n = call(bytes, WL_PLAN_EXPR_EXTENSION_CALL_ABI, "probe", 5, 1,
            123, 7);
    for (unsigned ordinal = 0; ordinal < 28; ++ordinal)for (action = 0;
            action < 4;
            ++action){
            if (!ordinal) {
                stop_phase = 450; stop_progress = 0;
            }else if (ordinal == 1) {
                stop_phase = 451; stop_progress = 1;
            }else if (ordinal == 4) {
                stop_phase = 453; stop_progress = 3;
            }else if (ordinal == 14) {
                stop_phase = 454; stop_progress = 12;
            }else if (ordinal == 27) {
                stop_phase = 455; stop_progress = 24;
            }else {
                stop_phase = 452;
                stop_progress = ordinal < 4?ordinal-1:ordinal <
                    14?ordinal-2:ordinal-3;
            }
            stopped = false; successor = 0;
            wl_evaluation_control_t *c = begin(action == 1?UINT64_MAX-1:0);
            uint32_t end = 999, start = 999; uint64_t packed = 999;
            wl_columnar_expr_test_checkpoint = stop;
            int rc = wl_columnar_expr_test_extension_decode(bytes, n, &end,
                    &start, &packed, c, id(c));
            wl_columnar_expr_test_checkpoint = NULL;
            CHECK(stopped && rc == expected_rc() && end == 999 &&
                start == 999 && packed == 999); retry(c, rc);
            CHECK(!wl_columnar_expr_test_extension_decode(bytes, n, &end,
                &start, &packed, c,
                id(c)) && end == n && start == 3 &&
                packed == ((UINT64_C(1)<<16)|5)); finish(c, 0);
        }
}
static void invoke_stops(void){
    wirelog_extension_registry_t *registry =
        wirelog_extension_registry_create(); CHECK(registry);
    uint32_t type = WIRELOG_EXTENSION_VALUE_INT64;
    wirelog_extension_descriptor_t descriptor =
    {.abi_version = WIRELOG_EXTENSION_ABI_VERSION, .size = sizeof(descriptor),
     .name = "probe",
     .arity = 1,
     .argument_types = &type, .result_type = WIRELOG_EXTENSION_VALUE_INT64,
     .invoke = callback, .addon_abi_identity = 123, .addon_abi_version = 7};
    CHECK(!wirelog_extension_register(registry, &descriptor));
    wirelog_extension_snapshot_t *snapshot =
        wirelog_extension_snapshot_acquire(registry); CHECK(snapshot);
    wl_columnar_expr_context_t context = {.extensions = snapshot,
                                          .allow_extension_scalar_result = true,
                                          .session_key = &owner,
                                          .active_worker_count = 1};
    wirelog_extension_value_t arg = {.type = WIRELOG_EXTENSION_VALUE_INT64,
                                     .size = sizeof(int64_t),
                                     .as.int64_value = 7};
    const unsigned phases[] = {500, 456, 457, 458, 501, 400, 410, 502, 503, 504,
                               505, 506, 507, 521, 508, 509, 510, 511, 420, 421,
                               422, 423, 424, 425, 512, 513, 514, 515, 777};
    for (unsigned phase = 0; phase < sizeof(phases)/sizeof(*phases);
        ++phase) for (action = 0;
            action < 4; ++action) {
            uint8_t bytes[32];
            uint32_t n = call(bytes, WL_PLAN_EXPR_EXTENSION_CALL_ABI, "probe",
                    5, 1, 123, 7);
            wl_evaluation_control_t *c = begin(action == 1?UINT64_MAX-1:0);
            int64_t value = 999; bool failed = false;
            wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_OK;
            stop_phase = phases[phase];
            stop_progress = stop_phase ==
                500?WL_PLAN_EXPR_EXTENSION_CALL_ABI:stop_phase == 457?5:0;
            stopped = false; successor = 0; calls = 0;
            callback_tls_effect = true; callback_control = c;
            wl_extension_error_set("prior");
            wl_columnar_expr_test_checkpoint = stop;
            wl_extension_control_test_checkpoint = registry_stop;
            int rc = wl_columnar_expr_test_extension_invoke(bytes, n, 3,
                    (UINT64_C(1)<<16)|5, WL_PLAN_EXPR_EXTENSION_CALL_ABI, &arg,
                    1, &context,
                    &value, &failed, &status, c, id(c));
            wl_columnar_expr_test_checkpoint = NULL;
            wl_extension_control_test_checkpoint = NULL;
            callback_control = NULL;
            CHECK(stopped && rc == expected_rc() && value == 999 && !failed &&
                status == WL_COLUMNAR_EXPR_OK &&
                !wl_columnar_expr_test_callback_depth() &&
                !registry->active_leases &&
                snapshot->entries[0]->references == 2);
            bool invoked = stop_phase == 513 || stop_phase == 514 ||
                stop_phase == 515 || stop_phase == 777;
            CHECK(calls == invoked);
            if (invoked) CHECK(!strcmp(wirelog_extension_last_error(),
                    "callback effect"));
            else if (stop_phase == 500 || stop_phase == 456 ||
                stop_phase == 457 || stop_phase == 458 || stop_phase == 501 ||
                stop_phase == 400 || stop_phase == 410)
                CHECK(!strcmp(wirelog_extension_last_error(), "prior"));
            else CHECK(!*wirelog_extension_last_error());
            retry(c, rc); callback_tls_effect = false;
            CHECK(!wl_columnar_expr_test_extension_invoke(bytes, n, 3,
                (UINT64_C(1)<<16)|5, WL_PLAN_EXPR_EXTENSION_CALL_ABI, &arg, 1,
                &context,
                &value, &failed, &status, c,
                id(c)) && value == 1 && !failed &&
                status == WL_COLUMNAR_EXPR_OK &&
                !wl_columnar_expr_test_callback_depth() &&
                !registry->active_leases); finish(c, 0);
        }
    /* Synthetic invalid metadata rejects safely without a large allocation. */
    uint8_t bytes[32];
    uint32_t n = call(bytes, WL_PLAN_EXPR_EXTENSION_CALL, "probe", 5, 1, 0, 0);
    wl_evaluation_control_t *c = begin(0); int64_t value = 999;
    bool failed = false; wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_OK;
    CHECK(wl_columnar_expr_test_extension_invoke(bytes, n, UINT32_MAX-1,
        (UINT64_C(1)<<16)|5, WL_PLAN_EXPR_EXTENSION_CALL, &arg, 1, &context,
        &value, &failed, &status, c,
        id(c)) == EINVAL && value == 999 && !failed &&
        status == WL_COLUMNAR_EXPR_OK); finish(c, EINVAL);
    wirelog_extension_snapshot_release(snapshot);
    CHECK(!wirelog_extension_unregister(registry,
        "probe") && !wirelog_extension_registry_destroy(registry));
    callback_tls_effect = false; callback_control = NULL;
}

static wirelog_extension_registry_t *life_registry, *nested_registry;
static wirelog_extension_snapshot_t *life_snapshot, *nested_snapshot;
static unsigned life_destroyed, life_nested_calls, life_nested_controlled;
static int nested_callback(const wirelog_extension_value_t *args,
    uint32_t count, wirelog_extension_value_t *result, void *data){
    (void)args; (void)count; (void)result; (void)data; ++life_nested_calls;
    return -1;
}
static void life_destroy(void *data){
    CHECK(data == &owner && wl_columnar_expr_test_callback_depth() == 1);
    ++life_destroyed;
    /* The actual lease release invokes destroy outside its own mutex. With
     * registration and snapshot already gone, destroying that registry here
     * is legal; any post-release access would use freed storage. */
    CHECK(life_registry && !wirelog_extension_registry_destroy(life_registry));
    life_registry = NULL;
    uint8_t bytes[32];
    uint32_t n = call(bytes, WL_PLAN_EXPR_EXTENSION_CALL, "nested", 6, 1, 0, 0);
    wl_columnar_expr_context_t context = {.extensions = nested_snapshot,
                                          .allow_extension_scalar_result = true,
                                          .session_key = &owner,
                                          .active_worker_count = 1};
    int64_t value = 999; wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_OK;
    if (life_nested_controlled) {
        wl_evaluation_control_t *c = begin(0); bool failed = false;
        wirelog_extension_value_t arg = {.type = WIRELOG_EXTENSION_VALUE_INT64,
                                         .size = 8, .as.int64_value = 7};
        CHECK(!wl_columnar_expr_test_extension_invoke(bytes, n, 3,
            (UINT64_C(1)<<16)|6, WL_PLAN_EXPR_EXTENSION_CALL, &arg, 1, &context,
            &value, &failed, &status, c,
            id(c)) && failed && status == WL_COLUMNAR_EXPR_CALLBACK_REENTRANT &&
            !value); finish(c, 0);
    }else {
        uint8_t program[64]; uint32_t base = integer(program, 0, 7);
        memcpy(program+base, bytes, n);
        CHECK(wl_columnar_expr_eval_run_ctx(program, base+n, NULL, 0, &value,
            &context, &status) == WL_COLUMNAR_EXPR_CALLBACK_REENTRANT &&
            status == WL_COLUMNAR_EXPR_CALLBACK_REENTRANT && !value);
    }
    CHECK(wl_columnar_expr_test_callback_depth() == 1 && !life_nested_calls);
    wl_extension_error_set("destroy effect");
    if (callback_control)stop(callback_control, 778, 0, false);
}
static int life_callback(const wirelog_extension_value_t *args, uint32_t count,
    wirelog_extension_value_t *result, void *data){
    (void)data;
    CHECK(count == 1 && args[0].type == WIRELOG_EXTENSION_VALUE_INT64 &&
        args[0].as.int64_value == 7 &&
        !*wirelog_extension_last_error() &&
        wl_columnar_expr_test_callback_depth() == 1); ++calls;
    CHECK(!wirelog_extension_unregister(life_registry, "probe"));
    wirelog_extension_snapshot_release(life_snapshot); life_snapshot = NULL;
    CHECK(!life_destroyed); wl_extension_error_set("invoke effect");
    if (callback_control)stop(callback_control, 777, 0, false);
    result->type = WIRELOG_EXTENSION_VALUE_BOOL; result->size = 1;
    result->as.bool_value = 1; return 0;
}
static void life_open(void){
    life_registry = wirelog_extension_registry_create(); CHECK(life_registry);
    uint32_t type = WIRELOG_EXTENSION_VALUE_INT64;
    wirelog_extension_descriptor_t descriptor =
    {.abi_version = WIRELOG_EXTENSION_ABI_VERSION, .size = sizeof(descriptor),
     .name = "probe",
     .arity = 1,
     .argument_types = &type, .result_type = WIRELOG_EXTENSION_VALUE_BOOL,
     .invoke = life_callback, .user_data = &owner,
     .destroy = life_destroy,
     .addon_abi_identity = 123, .addon_abi_version = 7};
    CHECK(!wirelog_extension_register(life_registry, &descriptor));
    life_snapshot = wirelog_extension_snapshot_acquire(life_registry);
    CHECK(life_snapshot);
    calls = 0; life_destroyed = life_nested_calls = 0;
}
static void lifetime(void){
    uint32_t type = WIRELOG_EXTENSION_VALUE_INT64;
    nested_registry = wirelog_extension_registry_create();
    CHECK(nested_registry);
    wirelog_extension_descriptor_t descriptor =
    {.abi_version = WIRELOG_EXTENSION_ABI_VERSION, .size = sizeof(descriptor),
     .name = "nested",
     .arity = 1,
     .argument_types = &type, .result_type = WIRELOG_EXTENSION_VALUE_BOOL,
     .invoke = nested_callback};
    CHECK(!wirelog_extension_register(nested_registry, &descriptor));
    nested_snapshot = wirelog_extension_snapshot_acquire(nested_registry);
    CHECK(nested_snapshot);
    for (unsigned abi = 0; abi < 2; ++abi)for (life_nested_controlled = 0;
            life_nested_controlled < 2; ++life_nested_controlled) {
            uint8_t bytes[64],
                tag =
                abi?WL_PLAN_EXPR_EXTENSION_CALL_ABI:WL_PLAN_EXPR_EXTENSION_CALL;
            uint32_t base = integer(bytes, 0, 7),
                n = base+call(bytes+base, tag, "probe", 5, 1, 123, 7);
            wl_columnar_expr_context_t context =
            {.allow_extension_scalar_result = true, .session_key = &owner,
             .active_worker_count = 1};
            /* Actual raw positive reference: final snapshot, unregister, registry
             * destruction and cross-evaluator reentrancy during destroy all legal. */
            life_open(); context.extensions = life_snapshot;
            int64_t value = 999;
            wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_OK;
            CHECK(!wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0, &value,
                &context,
                &status) && value == 1 && status == WL_COLUMNAR_EXPR_OK &&
                calls == 1 &&
                life_destroyed == 1 && !life_nested_calls && !life_registry &&
                !life_snapshot && !wl_columnar_expr_test_callback_depth() &&
                !strcmp(wirelog_extension_last_error(), "destroy effect"));
            for (unsigned target = 0; target < 2; ++target) for (action = 0;
                    action < 4; ++action) {
                    life_open(); context.extensions = life_snapshot;
                    wl_evaluation_control_t *c = begin(action ==
                            1?UINT64_MAX-1:0);
                    wl_columnar_expr_attempt_scalar_t *program = NULL;
                    bool supported = false, failed = false;
                    CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(
                            bytes, n, NULL, &program, &supported, c,
                            id(c)) && supported);
                    stop_phase = target?778:777; stop_progress = 0;
                    stopped = false; successor = 0; callback_control = c;
                    value = 999; status = WL_COLUMNAR_EXPR_OK;
                    int rc =
                        wl_columnar_expr_eval_scalar_attempt_controlled(program,
                            NULL, 0, &context, &value, &failed, &status, c,
                            id(c)); callback_control = NULL;
                    CHECK(stopped && rc == expected_rc() && value == 999 &&
                        !failed && status == WL_COLUMNAR_EXPR_OK &&
                        calls == 1 && life_destroyed == 1 &&
                        !life_registry && !life_snapshot &&
                        !life_nested_calls &&
                        !wl_columnar_expr_test_callback_depth() &&
                        !strcmp(wirelog_extension_last_error(),
                        "destroy effect"));
                    retry(c, rc); life_open();
                    context.extensions = life_snapshot;
                    CHECK(!wl_columnar_expr_eval_scalar_attempt_controlled(
                            program, NULL, 0, &context, &value, &failed,
                            &status, c, id(c)) &&
                        value == 1 && !failed &&
                        status == WL_COLUMNAR_EXPR_OK && calls == 1 &&
                        life_destroyed == 1 && !life_registry &&
                        !life_snapshot &&
                        !life_nested_calls &&
                        !wl_columnar_expr_test_callback_depth());
                    wl_columnar_expr_attempt_scalar_free(program); finish(c, 0);
                }
        }
    wirelog_extension_snapshot_release(nested_snapshot);
    CHECK(!wirelog_extension_unregister(nested_registry, "nested") &&
        !wirelog_extension_registry_destroy(nested_registry));
    nested_snapshot = NULL; nested_registry = NULL;
}

extern void (*wl_columnar_filter_test_scalar_host)(const wl_col_session_t *,
    const wl_columnar_filter_context_t *, bool, bool);
static struct {const wl_col_session_t *session;
               const wl_columnar_filter_context_t *context;} host_intervals[64];
static unsigned host_interval_depth;
static void host_interval(const wl_col_session_t *session,
    const wl_columnar_filter_context_t *context, bool map, bool enter){
    (void)map;
    if (enter) {
        CHECK(host_interval_depth < 64 && context &&
            context->identity == context && context->session == session &&
            context->thread_valid && context->busy &&
            context->control == session->base.evaluation_control);
        host_intervals[host_interval_depth].session = session;
        host_intervals[host_interval_depth].context = context;
        ++host_interval_depth;
    }else {
        CHECK(host_interval_depth &&
            host_intervals[host_interval_depth-1].session == session &&
            host_intervals[host_interval_depth-1].context == context);
        --host_interval_depth;
    }
}
extern void (*wl_columnar_filter_test_map_schema)(const col_rel_t *, bool);
extern unsigned wl_columnar_session_test_cleanup_owners(
    const wl_col_session_t *);
extern void (*wl_columnar_session_test_source_tail)(const wl_col_session_t *,
    const wl_session_source_finance_t *);
typedef struct {wirelog_extension_registry_t *registry;
                wirelog_extension_snapshot_t *snapshot;
                wirelog_program_t *program; wl_plan_t *plan;
                wl_session_t *session; wl_evaluation_control_t *control;
                int64_t input[3][2]; int balance[3];
                unsigned events, positive, negative, mode; bool filter, reject;
} host_t;
static host_t *host_active;
static void host_maybe_stop(void);
static unsigned host_fault;
static unsigned host_calls, host_entries, host_unknown, host_lookup,
    host_tags[2];
static int complete(wl_session_t *session){
    for (unsigned i = 0; i < 10000; ++i){
        int rc = wl_session_step(session); if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EAGAIN;
}
static int host_callback(const wirelog_extension_value_t *args, uint32_t count,
    wirelog_extension_value_t *result, void *data){
    (void)data;
    CHECK(host_active && count == 2 &&
        args[0].type == WIRELOG_EXTENSION_VALUE_INT64 &&
        args[1].type == WIRELOG_EXTENSION_VALUE_STRING);
    int64_t identity = args[0].as.int64_value;
    CHECK(identity >= 1 && identity <= 3);
    unsigned row = (unsigned)(identity-1);
    const char *text = wl_intern_reverse(host_active->plan->intern,
            host_active->input[row][1]);
    CHECK(text && args[1].as.string_value.length == strlen(text) &&
        !memcmp(args[1].as.string_value.data, text, strlen(text)));
    wl_col_session_t *sess = COL_SESSION(host_active->session);
    CHECK(sess->callback_configured_workers == 1 &&
        sess->callback_active_workers == 1 &&
        !sess->callback_parallel_execution &&
        sess->callback_session_key == sess);
    wl_session_t *attempt = host_active->session;
    CHECK(wl_session_try_destroy(&attempt) == EBUSY &&
        attempt == host_active->session);
    ++host_calls;
    if (host_active->filter || host_active->mode == 1) {
        result->type = WIRELOG_EXTENSION_VALUE_BOOL; result->size = 1;
        result->as.bool_value = host_active->filter?!host_active->reject &&
            identity == 1:1;
    }else {
        result->type = WIRELOG_EXTENSION_VALUE_INT64; result->size = 8;
        result->as.int64_value = identity*10;
    }
    host_maybe_stop();
    attempt = host_active->session;
    CHECK(wl_session_try_destroy(&attempt) == EBUSY &&
        attempt == host_active->session);
    if (host_fault && identity == 2 && (host_fault < 3 || text[0] == '!')) {
        if (host_fault == 1 || host_fault == 3) return -1;
        if (host_fault == 5) result->size = 0;
        else result->type = WIRELOG_EXTENSION_VALUE_STRING;
    }
    return 0;
}
static void host_capture(const char *name, const int64_t *row, uint32_t width,
    int32_t diff, void *arg){
    host_t *f = arg; if (!strcmp(name, "side")) {
        CHECK(width == 1 && row[0] == 1 && diff == 1); return;
    }
    CHECK(!strcmp(name,
        "out") && row[0] >= 1 && row[0] <= 3 && (diff == 1 || diff == -1));
    unsigned i = (unsigned)(row[0]-1);
    CHECK(width == (f->filter?2:3) && row[1] == f->input[i][1]);
    if (f->filter) CHECK(!i && !f->reject);
    else CHECK(row[2] == (f->mode == 1?1:(int64_t)(i+1)*10));
    f->balance[i] += diff; CHECK(f->balance[i] >= 0 && f->balance[i] <= 1);
    ++f->events; f->positive += diff == 1; f->negative += diff == -1;
}
static void host_contents(host_t *f, unsigned count){
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->nrows == count && out->ncols == (f->filter?2:3));
    bool seen[3] = {false, false, false};
    for (uint32_t row = 0; row < out->nrows; ++row) {
        int64_t identity = out->columns[0][row];
        CHECK(identity >= 1 && identity <= 3 && !seen[identity-1]);
        unsigned i = (unsigned)(identity-1); seen[i] = true;
        CHECK(out->columns[1][row] == f->input[i][1]);
        if (f->filter)CHECK(!i && !f->reject);
        else CHECK(out->columns[2][row] == (f->mode == 1?1:(int64_t)(i+1)*10));
    }
}
static void host_schema(const col_rel_t *relation, bool known){
    if (!known){
        CHECK(relation && !relation->column_types); ++host_unknown;
    }
}
static void host_observe(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed){
    (void)c; (void)progress; (void)failed; if (phase == 500){
        ++host_entries;
        CHECK(progress == WL_PLAN_EXPR_EXTENSION_CALL ||
            progress == WL_PLAN_EXPR_EXTENSION_CALL_ABI);
        ++host_tags[progress == WL_PLAN_EXPR_EXTENSION_CALL_ABI];
    }
}
static void host_registry_observe(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress){
    (void)c; (void)progress; host_lookup += phase == 400;
}
static host_t host_open(unsigned abi, bool controlled, bool filter,
    unsigned mode, unsigned route){
    host_t f = {.filter = filter, .mode = mode};
    f.registry = wirelog_extension_registry_create(); CHECK(f.registry);
    uint32_t types[] = {WIRELOG_EXTENSION_VALUE_INT64,
                        WIRELOG_EXTENSION_VALUE_STRING};
    wirelog_extension_descriptor_t descriptor =
    {.abi_version = WIRELOG_EXTENSION_ABI_VERSION, .size = sizeof(descriptor),
     .name = "test.probe",
     .arity = 2,
     .argument_types = types,
     .result_type = filter ||
         mode == 2?WIRELOG_EXTENSION_VALUE_BOOL:WIRELOG_EXTENSION_VALUE_INT64,
     .invoke = host_callback};
    if (abi) {
        descriptor.addon_abi_identity = 123; descriptor.addon_abi_version = 7;
    }
    CHECK(!wirelog_extension_register(f.registry, &descriptor));
    f.snapshot = wirelog_extension_snapshot_acquire(f.registry);
    CHECK(f.snapshot);
    char source[2048]; snprintf(source, sizeof(source),
        ".decl input(i:int64,s:string)\n.decl out(i:int64,s:string%s)\n%s"
        "%s%s%s", filter?"":",v:int64", route == 1?".decl gate(i:int64)\n":"",
        filter?"out(i,s) :- input(i,s), @call(\"test.probe\",i,s)":
        "out(i,s,@call(\"test.probe\",i,s)) :- input(i,s)",
        route == 1?",gate(i)":"",
        ".\n");
    if (route == 2)strcat(source,
            ".decl a(i:int64)\n.decl b(i:int64)\n.decl c(i:int64)\n.decl side(i:int64)\nside(i) :- a(i),b(i),c(i).\n");
    wirelog_error_t error; f.program = wirelog_parse_string(source, &error);
    int plan_rc = f.program?wl_plan_from_program_with_snapshot(f.program,
            f.snapshot, &f.plan):-99;
    if (!f.program || plan_rc)fprintf(stderr,
            "host setup abi=%u controlled=%d filter=%d mode=%u route=%u parse=%p plan=%d error=%d diag=%s source=%s\n",
            abi, controlled, filter, mode, route, (void *)f.program, plan_rc,
            error, wirelog_extension_last_error(), source);
    CHECK(f.program && !plan_rc);
    wl_session_options_t options; wl_session_options_init(&options);
    if (controlled) {
        CHECK(!wl_evaluation_control_create(0, &f.control));
        options.evaluation_control = f.control;
    }
    CHECK(!wl_session_create_with_snapshot_options(wl_backend_columnar(),
        f.plan, 1, f.snapshot, &options, &f.session));
    char long_text[4097]; memset(long_text, 'a', 4096); long_text[4096] = 0;
    const char *text[] = {"abc", "", long_text};
    for (unsigned i = 0; i < 3; ++i) {
        f.input[i][0] = (int64_t)i+1;
        f.input[i][1] = wl_intern_put(f.plan->intern, text[i]);
        CHECK(f.input[i][1] >= 0);
    }
    /* Callback binding is installed only after return at the final address. */
    return f;
}
static void host_bind(host_t *f){
    host_active = f; wl_session_set_delta_cb(f->session, host_capture, f);
    host_calls = host_entries = host_unknown = host_lookup = 0;
    host_tags[0] = host_tags[1] = 0;
    wl_columnar_filter_test_scalar_host = host_interval;
    wl_columnar_expr_test_checkpoint = host_observe;
    wl_extension_control_test_checkpoint = host_registry_observe;
    wl_columnar_filter_test_map_schema = host_schema;
}
static void host_unbind(void){
    CHECK(!host_interval_depth); wl_columnar_filter_test_scalar_host = NULL;
    wl_columnar_expr_test_checkpoint = NULL;
    wl_extension_control_test_checkpoint = NULL;
    wl_columnar_filter_test_map_schema = NULL; host_active = NULL;
}
static void host_close(host_t *f){
    host_unbind();
    wl_columnar_memory_governor_ref_t *g =
        COL_SESSION(f->session)->memory_governor;
    wl_columnar_memory_governor_ref_retain(g);
    wl_session_destroy(f->session); wl_plan_free(f->plan);
    wirelog_program_free(f->program);
    if (f->control) wl_evaluation_control_release(f->control);
    wirelog_extension_snapshot_release(f->snapshot);
    CHECK(!wirelog_extension_unregister(f->registry,
        "test.probe") && !wirelog_extension_registry_destroy(f->registry));
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
static void session_positive(void){
    for (unsigned abi = 0; abi < 2; ++abi)for (unsigned controlled = 0;
            controlled < 2;
            ++controlled)
            for (unsigned filter = 0; filter < 2; ++filter) for (unsigned mode =
                    0;
                    mode < (filter?2u:3u); ++mode){
                    host_t f = host_open(abi, controlled, filter, mode, 0);
                    f.reject = filter && mode; host_bind(&f);
                    int empty_rc = complete(f.session);
                    if (empty_rc) fprintf(stderr,
                            "empty host abi=%u controlled=%u filter=%u mode=%u rc=%d status=%d\n",
                            abi, controlled, filter, mode, empty_rc,
                            COL_SESSION(f.session)->extension_expr_status);
                    CHECK(!empty_rc); host_contents(&f, 0);
                    CHECK(!host_calls && !host_lookup);
                    for (unsigned i = 0; i < 3;
                        ++i) CHECK(!wl_session_insert(f.session, "input",
                            f.input[i], 1, 2));
                    int rc = complete(f.session);
                    if (rc) fprintf(stderr,
                            "extension host abi=%u controlled=%u filter=%u mode=%u rc=%d status=%d\n",
                            abi, controlled, filter, mode, rc,
                            COL_SESSION(f.session)->extension_expr_status);
                    CHECK(!rc); unsigned count = filter?!f.reject:3;
                    host_contents(&f, count);
                    CHECK(host_calls >= 3 && f.events == count &&
                        f.positive == count && !f.negative);
                    if (controlled) {
                        CHECK(host_entries >= 3 && host_tags[abi] >= 3 &&
                            !host_tags[!abi]);
                        if (!filter) CHECK(host_unknown && host_lookup >= 3);
                    }
                    if (count) {
                        CHECK(!wl_session_remove(f.session, "input", f.input[0],
                            1, 2) && !complete(f.session));
                        host_contents(&f, count-1);
                        CHECK(f.events == count+1 && f.negative == 1 &&
                            !f.balance[0]);
                        CHECK(!wl_session_insert(f.session, "input", f.input[0],
                            1, 2) && !complete(f.session));
                        host_contents(&f, count);
                        CHECK(f.events == count+2 && f.positive == count+1);
                    }
                    unsigned events = f.events;
                    CHECK(!complete(f.session) && f.events == events);
                    host_close(&f);
                }
}

static unsigned host_action, host_target, host_owner_reads, host_auth_proofs,
    host_route_current,
    host_filter_current;
static uint64_t host_progress, host_successor;
static bool host_stop_enabled, host_stopped;
static wl_evaluation_control_report_t host_original;
static void host_stop_now(wl_evaluation_control_t *c){
    CHECK(host_active && c == host_active->control && host_interval_depth);
    const wl_col_session_t *actual = host_intervals[host_interval_depth-
            1].session;
    const wl_columnar_filter_context_t *native =
        host_intervals[host_interval_depth-1].context;
    unsigned owners = wl_columnar_session_test_cleanup_owners(actual);
    wl_columnar_eval_serial_framed_attempt_t *parent = NULL;
    int context_rc = wl_columnar_eval_stack_framed_context(native->frame,
            (wl_col_session_t *)actual, &parent);
    int auth_rc =
        context_rc?context_rc:
        wl_columnar_eval_stack_framed_authenticate(native->frame, actual,
            parent);
    if (owners != (host_route_current == 1?1u:2u))fprintf(stderr,
            "actual owner route=%u filter=%u target=%u owners=%u same=%d nativeframe=%p activeframe=%p contextrc=%d authrc=%d fallback=%d capture=%d abort=%d keyid=%llu\n",
            host_route_current, host_filter_current, host_target, owners,
            actual == COL_SESSION(host_active->session), (void *)native->frame,
            (void *)actual->cleanup_active, context_rc, auth_rc,
            parent?parent->controlled_materialized_batch_fallback:-1,
            parent?parent->capture_only:-1, parent?parent->abort_supported:-1,
            (unsigned long long)(parent?parent->cleanup_receipt_key.receipt_id:
            0));
    CHECK(actual->base.evaluation_control == c &&
        actual->callback_session_key == COL_SESSION(host_active->session) &&
        native->control == c && native->attempt_id == id(c) &&
        native->frame == actual->cleanup_active && !auth_rc);
    CHECK(owners != 3);
    CHECK(!auth_rc && parent && parent->session == actual &&
        parent->frame == native->frame && parent->control == c &&
        parent->attempt_id == id(c));
    if (host_route_current == 1) {
        bool join = false;
        for (uint32_t i = 0; i < parent->plan->op_count;
            ++i) join |= parent->plan->ops[i].op == WL_PLAN_OP_JOIN;
        CHECK(owners == 1 && actual == COL_SESSION(host_active->session) &&
            join && parent->capture_only && parent->private_bindings &&
            parent->abort_supported);
        ++host_auth_proofs;
    }else CHECK(owners == 2);
    host_stopped = true;
    if (host_action == 0)wl_evaluation_control_request_cancel(c);
    else if (host_action == 1)CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else if (host_action == 2) {
        wl_evaluation_control_attempt_finish_result_t end =
            wl_evaluation_control_finish_attempt(c,
                host_active->session->operation_admission, id(c), 0, 0);
        CHECK(!end.protocol && end.report_valid && !end.report.result &&
            end.report.consumed); host_original = end.report;
        CHECK(!wl_evaluation_control_reset(c));
        wl_evaluation_control_attempt_begin_result_t next =
            wl_evaluation_control_begin_attempt(c,
                host_active->session->operation_admission);
        CHECK(!next.protocol && next.attempt_id != end.report.attempt_id);
        host_successor = next.attempt_id;
    }else {
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
        wl_evaluation_control_request_cancel(c);
    }
}
static void host_maybe_stop(void){
    if (host_stop_enabled && !host_stopped && host_target == 777 &&
        host_entries >= 2) host_stop_now(host_active->control);
}
static void host_control_hook(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed){
    host_observe(c, phase, progress, failed);
    if (!host_stop_enabled || host_stopped || phase != host_target ||
        progress != host_progress) return;
    if (phase == 41?host_calls < 1:host_entries < 2){
        return;
    }
    host_stop_now(c);
}
static void host_source_tail(const wl_col_session_t *session,
    const wl_session_source_finance_t *source){
    unsigned owners = wl_columnar_session_test_cleanup_owners(session);
    CHECK(owners != 3);
    if (source->charged == 96 && owners == 2)++host_owner_reads;
}
static void host_prepare_route(host_t *f, unsigned route){
    if (route == 1) {
        for (int64_t i = 1; i <= 3;
            ++i) CHECK(!wl_session_insert(f->session, "gate", &i, 1, 1));
    }
    if (route == 2) {
        int64_t one = 1; const char *names[] = {"a", "b", "c"};
        for (unsigned i = 0;
            i < 3;
            ++i) CHECK(!wl_session_insert(f->session, names[i], &one, 1, 1));
    }
}
static void host_check_end(host_t *f, int rc){
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f->control, &report));
    if (host_action == 2) {
        CHECK(report.attempt_id == host_original.attempt_id &&
            report.limit == host_original.limit &&
            report.consumed == host_original.consumed &&
            report.result == host_original.result &&
            report.stop_reason == host_original.stop_reason &&
            report.execution_status == host_original.execution_status &&
            report.cleanup_status == host_original.cleanup_status);
        uint64_t used = 999;
        CHECK(!wl_evaluation_control_test_active_consumed(f->control,
            host_successor, &used) && !used);
        wl_evaluation_control_attempt_finish_result_t end =
            wl_evaluation_control_finish_attempt(f->control,
                f->session->operation_admission,
                host_successor, 0, 0);
        CHECK(!end.protocol && end.report_valid && !end.report.consumed);
    }else CHECK(report.result == rc && !report.cleanup_status);
    CHECK(!COL_SESSION(f->session)->cleanup_active &&
        !COL_SESSION(f->session)->cleanup_pending &&
        !COL_SESSION(f->session)->cleanup_reserved_bytes &&
        !f->registry->active_leases && !wl_columnar_expr_test_callback_depth());
}
static void session_stops(void){
    const unsigned targets[] = {456, 458, 502, 521, 512, 513, 515, 777, 41};
    for (unsigned abi = 0; abi < 2; ++abi)for (unsigned route = 0; route < 3;
            ++route)
            for (unsigned filter = 0; filter < 2;
                ++filter) for (unsigned target = 0;
                    target < sizeof(targets)/sizeof(*targets); ++target){
                    if (!abi && targets[target] == 458) continue;
                    for (host_action = 0; host_action < 4; ++host_action) {
                        host_route_current = route;
                        host_filter_current = filter;
                        host_t f = host_open(abi, true, filter, 0, route);
                        host_bind(&f); host_prepare_route(&f, route);
                        CHECK(!wl_session_insert(f.session, "input", f.input[0],
                            1, 2) && !complete(f.session));
                        host_contents(&f, 1);
                        if (route == 2) CHECK(f.session->source_c1_admission);
                        col_rel_t *prior =
                            session_find_rel(COL_SESSION(f.session), "out");
                        col_relation_snapshot_t generation =
                            wl_columnar_relation_snapshot(prior);
                        for (unsigned i = 1; i < 3;
                            ++i) CHECK(!wl_session_insert(f.session, "input",
                                f.input[i], 1, 2));
                        host_calls = host_entries = host_owner_reads =
                            host_auth_proofs = 0;
                        host_successor = 0; host_target = targets[target];
                        host_progress = host_target == 41?512:0;
                        host_stop_enabled = true; host_stopped = false;
                        if (host_action ==
                            1) CHECK(!wl_evaluation_control_configure(f.control,
                                UINT64_MAX-1));
                        wl_columnar_expr_test_checkpoint = host_control_hook;
                        wl_columnar_session_test_source_tail = host_source_tail;
                        int rc = complete(f.session);
                        wl_columnar_expr_test_checkpoint = host_observe;
                        wl_columnar_session_test_source_tail = NULL;
                        host_stop_enabled = false;
                        if (!host_stopped) fprintf(stderr,
                                "host stop missing abi=%u route=%u filter=%u target=%u action=%u rc=%d entries=%u calls=%u\n",
                                abi, route, filter, host_target, host_action,
                                rc, host_entries, host_calls);
                        CHECK(host_stopped &&
                            (route == 1?host_auth_proofs:host_owner_reads) &&
                            rc ==
                            (host_action == 0?WL_EVALUATION_CONTROL_CANCELLED:
                            host_action ==
                            1?WL_EVALUATION_CONTROL_WORK_LIMIT:host_action ==
                            2?EINVAL:EOVERFLOW));
                        host_contents(&f, 1);
                        CHECK(f.events == 1 &&
                            prior == session_find_rel(COL_SESSION(f.session),
                            "out") &&
                            wl_columnar_relation_snapshot_equal(generation,
                            wl_columnar_relation_snapshot(prior)));
                        host_check_end(&f, rc);
                        int reset_rc = wl_evaluation_control_reset(f.control);
                        int configure_rc = reset_rc ? reset_rc :
                            wl_evaluation_control_configure(f.control, 0);
                        int resume_rc =
                            configure_rc ? configure_rc : complete(f.session);
                        if (reset_rc || configure_rc || resume_rc)
                            fprintf(stderr,
                                "host retry abi=%u route=%u filter=%u target=%u action=%u reset=%d configure=%d resume=%d phase=%u origin=%u\n",
                                abi, route, filter, host_target, host_action,
                                reset_rc, configure_rc, resume_rc,
                                COL_SESSION(
                                    f.session)->controlled_step_reset.phase,
                                COL_SESSION(
                                    f.session)->controlled_step_reset.origin);
                        CHECK(!reset_rc && !configure_rc && !resume_rc);
                        unsigned count = filter?1:3; host_contents(&f, count);
                        CHECK(f.events == count && f.positive == count &&
                            !f.negative);
                        CHECK(!wl_session_remove(f.session, "input", f.input[0],
                            1, 2) && !complete(f.session));
                        host_contents(&f, count-1);
                        CHECK(f.events == count+1 && f.negative == 1);
                        CHECK(!wl_session_insert(f.session, "input", f.input[0],
                            1, 2) && !complete(f.session));
                        host_contents(&f, count);
                        CHECK(f.events == count+2 && f.positive == count+1 &&
                            !complete(f.session) && f.events == count+2);
                        host_close(&f);
                    }
                }
}

static void host_failure_closed(host_t *f, int expected){
    CHECK(COL_SESSION(f->session)->extension_expr_status == expected &&
        !COL_SESSION(f->session)->cleanup_active &&
        !COL_SESSION(f->session)->cleanup_pending &&
        !COL_SESSION(f->session)->cleanup_reserved_bytes &&
        !f->registry->active_leases && !wl_columnar_expr_test_callback_depth());
    CHECK(wl_facade_session_error_code_with_budget(expected,
        COL_SESSION(f->session)->extension_expr_status,
        wl_columnar_session_budget_denied(COL_SESSION(f->session))) ==
        WIRELOG_ERR_EXEC);
    CHECK(!strcmp(wirelog_extension_last_error(),
        expected == WL_COLUMNAR_EXPR_CALLBACK_FAILURE?
        "scalar extension callback failed":
        "scalar extension returned an invalid result"));
    if (f->control) {
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f->control, &report) &&
            report.result == expected && !report.cleanup_status);
    }
}
static void host_errors(void){
    for (unsigned abi = 0; abi < 2; ++abi)
        for (unsigned controlled = 0; controlled < 2; ++controlled)
            for (unsigned route = 0; route < 2; ++route)
                for (unsigned filter = 0; filter < 2; ++filter)
                    for (host_fault = 3; host_fault <= 5; ++host_fault) {
                        host_t f = host_open(abi, controlled, filter, 0, route);
                        host_bind(&f); host_prepare_route(&f, route);
                        CHECK(!wl_session_insert(f.session, "input", f.input[0],
                            1, 2) &&
                            !complete(f.session)); host_contents(&f, 1);
                        col_rel_t *prior =
                            session_find_rel(COL_SESSION(f.session), "out");
                        col_relation_snapshot_t generation =
                            wl_columnar_relation_snapshot(prior);
                        f.input[1][1] = wl_intern_put(f.plan->intern, "!bad");
                        CHECK(f.input[1][1] >= 0);
                        CHECK(!wl_session_insert(f.session, "input", f.input[1],
                            1, 2) &&
                            !wl_session_insert(f.session, "input", f.input[2],
                            1, 2));
                        int expected = host_fault ==
                            3?WL_COLUMNAR_EXPR_CALLBACK_FAILURE:
                            WL_COLUMNAR_EXPR_INVALID_RESULT;
                        for (unsigned retry_bad = 0; retry_bad < 2;
                            ++retry_bad){
                            CHECK(complete(f.session) == expected);
                            host_failure_closed(&f, expected);
                            host_contents(&f, 1);
                            CHECK(f.events == 1 && f.positive == 1 &&
                                !f.negative);
                            /* Raw execution changes its private mutation counter.
                             * The observed controlled precommit contract also
                             * preserves the actual committed pointer/generation. */
                            if (controlled) CHECK(
                                    session_find_rel(COL_SESSION(f.session),
                                    "out") == prior &&
                                    wl_columnar_relation_snapshot_equal(
                                        generation,
                                        wl_columnar_relation_snapshot(prior)));
                            if (retry_bad == 0 &&
                                f.control) CHECK(!wl_evaluation_control_reset(
                                        f.control));
                        }
                        if (!controlled) {
                            /* Raw observed STEP retains its genuine delta
                             * observer and forbids mutation until the host
                             * repairs the cooperative callback and drains it. */
                            CHECK(wl_session_remove(f.session, "input",
                                f.input[1], 1, 2) == EBUSY &&
                                COL_SESSION(f.session)->delta_observer);
                            unsigned saved_fault = host_fault; host_fault = 0;
                            CHECK(!complete(f.session));
                            unsigned count = filter?1u:3u;
                            host_contents(&f, count);
                            CHECK(f.events == count && !f.negative);
                            host_close(&f); host_fault = saved_fault; continue;
                        }
                        wl_evaluation_control_report_t before, after;
                        if (f.control) CHECK(!wl_evaluation_control_report(
                                    f.control, &before));
                        int correction_rc = wl_session_remove(f.session,
                                "input", f.input[1], 1, 2);
                        CHECK(!correction_rc);
                        f.input[1][1] = wl_intern_put(f.plan->intern, "fixed");
                        CHECK(f.input[1][1] >= 0);
                        CHECK(!wl_session_insert(f.session, "input", f.input[1],
                            1, 2));
                        if (f.control) {
                            CHECK(!wl_evaluation_control_report(f.control,
                                &after) &&
                                after.attempt_id == before.attempt_id &&
                                after.result == before.result &&
                                after.consumed == before.consumed &&
                                after.limit == before.limit &&
                                after.execution_status ==
                                before.execution_status &&
                                after.cleanup_status == before.cleanup_status &&
                                after.stop_reason == before.stop_reason);
                            CHECK(!wl_evaluation_control_reset(f.control));
                        }
                        CHECK(!complete(f.session));
                        unsigned count = filter?1u:3u;
                        host_contents(&f, count);
                        CHECK(f.events == count && f.positive == count &&
                            !f.negative);
                        CHECK(!wl_session_remove(f.session, "input", f.input[0],
                            1, 2) && !complete(f.session));
                        host_contents(&f, count-1);
                        CHECK(f.events == count+1 && f.negative == 1);
                        CHECK(!wl_session_insert(f.session, "input", f.input[0],
                            1, 2) && !complete(f.session));
                        host_contents(&f, count);
                        CHECK(f.events == count+2 && f.positive == count+1 &&
                            !complete(f.session) && f.events == count+2);
                        host_close(&f);
                    }
    host_fault = 0;
}

extern void (*wl_columnar_session_test_join_forward_phase)(
    const wl_col_session_t *, unsigned, wl_evaluation_control_t *, uint64_t);
extern void (*wl_columnar_session_test_framed_grant_finished)(
    const wl_col_session_t *, wl_columnar_eval_serial_framed_host_action_t,
    wl_evaluation_control_t *, int);
extern int wl_columnar_session_test_producer_disposition_view(
    const wl_col_session_t *, uint64_t[6]);

typedef struct {
    host_t *fixture;
    unsigned phase, starts, reports, releases, acknowledgements;
    bool work, fired, closed;
    uint64_t original_id, source_id, source_identity;
} join_forward_probe_t;
static join_forward_probe_t *join_forward_probe;

static void
join_forward_phase(const wl_col_session_t *sess, unsigned phase,
    wl_evaluation_control_t *source, uint64_t source_id)
{
    join_forward_probe_t *probe = join_forward_probe;
    if (!probe || sess != COL_SESSION(probe->fixture->session)) return;
    if (!phase) ++probe->starts;
    if (phase == probe->phase && !probe->fired) {
        CHECK(source && source != probe->fixture->control && source_id
            && wl_evaluation_control_active_attempt_id(source) == source_id);
        probe->original_id = wl_evaluation_control_active_attempt_id(
            probe->fixture->control);
        CHECK(probe->original_id
            && wl_columnar_session_test_cleanup_owners(sess) == 1);
        probe->source_id = source_id;
        probe->source_identity = wl_evaluation_control_identity(source);
        probe->fired = true;
        if (probe->work)
            CHECK(wl_evaluation_control_charge_attempt(source, source_id,
                2304) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        else wl_evaluation_control_request_cancel(source);
    }
    if (phase == 9) {
        ++probe->reports;
        CHECK(source && source_id
            && !wl_evaluation_control_active_attempt_id(source));
        if (probe->fired && !probe->closed) {
            wl_evaluation_control_report_t report;
            uint64_t consumed = probe->phase == 6 ? 988 :
                probe->phase == 7 ? 1116 : 1308;
            CHECK(wl_evaluation_control_identity(source) ==
                probe->source_identity
                && !wl_evaluation_control_report(source, &report)
                && report.attempt_id == probe->source_id
                && report.limit == 2304 && report.consumed == consumed
                && !report.execution_status && !report.cleanup_status
                && report.stop_reason == (probe->work ?
                WL_EVALUATION_CONTROL_WORK_LIMIT :
                WL_EVALUATION_CONTROL_CANCELLED)
                && report.result == report.stop_reason);
        }
    }
    if (phase == 10) {
        CHECK(!source && !source_id);
        ++probe->releases;
        if (probe->fired) probe->closed = true;
    }
}

static void
join_forward_ack(const wl_col_session_t *sess,
    wl_columnar_eval_serial_framed_host_action_t action,
    wl_evaluation_control_t *funding, int protocol)
{
    (void)funding;
    if (join_forward_probe
        && sess == COL_SESSION(join_forward_probe->fixture->session)
        && action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK && !protocol)
        ++join_forward_probe->acknowledgements;
}

static void
join_forward_refusals(void)
{
    const unsigned phases[] = {6, 7, 8};
    for (unsigned abi = 0; abi < 2; ++abi)
        for (unsigned cut = 0; cut < 3; ++cut)
            for (unsigned work = 0; work < 2; ++work) {
                host_fault = 3;
                host_t fixture = host_open(abi, true, false, 0, 1);
                host_bind(&fixture); host_prepare_route(&fixture, 1);
                CHECK(!wl_session_insert(fixture.session, "input",
                    fixture.input[0], 1, 2) && !complete(fixture.session));
                host_contents(&fixture, 1);
                CHECK(fixture.events == 1);
                col_rel_t *committed = session_find_rel(
                    COL_SESSION(fixture.session), "out");
                col_relation_snapshot_t image =
                    wl_columnar_relation_snapshot(committed);
                wl_evaluation_control_report_t prior, after, failed;
                CHECK(!wl_evaluation_control_report(fixture.control, &prior));
                fixture.input[1][1] = wl_intern_put(fixture.plan->intern,
                        "!bad");
                CHECK(fixture.input[1][1] >= 0
                    && !wl_session_insert(fixture.session, "input",
                    fixture.input[1], 1, 2)
                    && !wl_session_insert(fixture.session, "input",
                    fixture.input[2], 1, 2));
                join_forward_probe_t probe = {
                    .fixture = &fixture, .phase = phases[cut],
                    .work = work != 0
                };
                CHECK(!wl_columnar_session_test_join_forward_phase
                    && !wl_columnar_session_test_framed_grant_finished);
                join_forward_probe = &probe;
                wl_columnar_session_test_join_forward_phase =
                    join_forward_phase;
                wl_columnar_session_test_framed_grant_finished =
                    join_forward_ack;
                for (unsigned turn = 0; turn < 10000 && !probe.closed; ++turn)
                    CHECK(wl_session_step(fixture.session) == EAGAIN);
                CHECK(probe.fired && probe.closed && probe.starts == 1
                    && probe.reports == 1 && probe.releases == 1
                    && probe.acknowledgements == 1
                    && wl_evaluation_control_active_attempt_id(fixture.control)
                    == probe.original_id
                    && wl_columnar_session_test_cleanup_owners(
                        COL_SESSION(fixture.session)) == 1);
                uint64_t disposition[6];
                CHECK(!wl_columnar_session_test_producer_disposition_view(
                        COL_SESSION(fixture.session), disposition)
                    && disposition[0] == (phases[cut] == 6 ? 0u : 1u)
                    && !disposition[4] && !disposition[5]);
                CHECK(!wl_evaluation_control_report(fixture.control, &after)
                    && wl_test_completed_step_report_equal(&prior, &after)
                    && fixture.events == 1
                    && session_find_rel(COL_SESSION(fixture.session), "out")
                    == committed
                    && wl_columnar_relation_snapshot_equal(image,
                    wl_columnar_relation_snapshot(committed)));
                CHECK(complete(fixture.session) ==
                    WL_COLUMNAR_EXPR_CALLBACK_FAILURE);
                wl_columnar_session_test_join_forward_phase = NULL;
                wl_columnar_session_test_framed_grant_finished = NULL;
                join_forward_probe = NULL;
                CHECK(probe.starts == 2 && probe.reports == 2
                    && probe.releases == 2 && probe.acknowledgements == 1);
                host_failure_closed(&fixture,
                    WL_COLUMNAR_EXPR_CALLBACK_FAILURE);
                host_contents(&fixture, 1);
                CHECK(fixture.events == 1
                    && !wl_evaluation_control_report(fixture.control, &failed)
                    && failed.attempt_id == probe.original_id
                    && failed.execution_status ==
                    WL_COLUMNAR_EXPR_CALLBACK_FAILURE);
                CHECK(!wl_session_remove(fixture.session, "input",
                    fixture.input[1], 1, 2));
                fixture.input[1][1] = wl_intern_put(fixture.plan->intern,
                        "fixed");
                CHECK(fixture.input[1][1] >= 0
                    && !wl_session_insert(fixture.session, "input",
                    fixture.input[1], 1, 2)
                    && !wl_evaluation_control_report(fixture.control, &after)
                    && wl_test_completed_step_report_equal(&failed, &after)
                    && !wl_evaluation_control_reset(fixture.control)
                    && !complete(fixture.session));
                host_contents(&fixture, 3);
                CHECK(fixture.events == 3 && fixture.positive == 3
                    && !fixture.negative);
                CHECK(!wl_session_remove(fixture.session, "input",
                    fixture.input[0], 1, 2) && !complete(fixture.session));
                host_contents(&fixture, 2);
                CHECK(fixture.events == 4 && fixture.negative == 1);
                CHECK(!wl_session_insert(fixture.session, "input",
                    fixture.input[0], 1, 2) && !complete(fixture.session));
                host_contents(&fixture, 3);
                CHECK(fixture.events == 5 && fixture.positive == 4
                    && !complete(fixture.session) && fixture.events == 5);
                host_close(&fixture);
            }
    host_fault = 0;
}
/* The existing creator test seam supplies normal internal control options;
 * facade construction/STEP and terminal error mapping are actual public calls.
 * Installed control configuration/continuation remains outside this issue. */
extern wl_session_t *wirelog_session_testhook_inner(wirelog_session_t *);
typedef struct {bool filter; unsigned fault, calls, events, positive, negative;
                unsigned snapshot_count, snapshot_mask; int balance[5];
} facade_t;
static int facade_callback(const wirelog_extension_value_t *args,
    uint32_t count,
    wirelog_extension_value_t *result, void *data){
    facade_t *f = data;
    CHECK(count == 1 && args[0].type == WIRELOG_EXTENSION_VALUE_INT64 &&
        args[0].as.int64_value >= 1 && args[0].as.int64_value <= 4 &&
        !*wirelog_extension_last_error()); ++f->calls;
    int64_t x = args[0].as.int64_value;
    result->type =
        f->filter?WIRELOG_EXTENSION_VALUE_BOOL:WIRELOG_EXTENSION_VALUE_INT64;
    result->size = f->filter?1:8;
    if (f->filter)result->as.bool_value = 1; else result->as.int64_value = x*10;
    if (x == 2) {
        if (f->fault == 1)return -1;
        if (f->fault == 2)result->type = WIRELOG_EXTENSION_VALUE_STRING;
        if (f->fault == 3)result->size = 0;
    }
    return 0;
}
static void facade_row(facade_t *f, const char *name, const int64_t *row,
    uint32_t width){
    CHECK(!strcmp(name,
        "out") && width == (f->filter?1u:2u) && row[0] >= 1 && row[0] <= 4);
    if (!f->filter)CHECK(row[1] == row[0]*10);
}
static void facade_delta(const char *name, const int64_t *row, uint32_t width,
    int32_t diff, void *data){
    facade_t *f = data; facade_row(f, name, row, width);
    CHECK(diff == 1 || diff == -1);
    f->balance[row[0]] += diff;
    CHECK(f->balance[row[0]] >= 0 && f->balance[row[0]] <= 1);
    ++f->events; f->positive += diff == 1; f->negative += diff == -1;
}
static void facade_snapshot(const char *name, const int64_t *row,
    uint32_t width, void *data){
    facade_t *f = data; facade_row(f, name, row, width);
    unsigned bit = 1u<<(unsigned)row[0]; CHECK(!(f->snapshot_mask&bit));
    f->snapshot_mask |= bit; ++f->snapshot_count;
}
static void facade_contents(wirelog_session_t *session, facade_t *f,
    unsigned mask, unsigned count){
    f->snapshot_count = f->snapshot_mask = 0;
    CHECK(wirelog_session_snapshot(session, facade_snapshot, f) == WIRELOG_OK &&
        f->snapshot_count == count && f->snapshot_mask == mask);
}
static wirelog_error_t facade_drive(wirelog_session_t *session,
    wl_evaluation_control_t *control){
    /* Public control continuation is not an installed API. The test's existing
     * inner/control inspection drives genuine pending calls through STEP. */
    uint64_t completed_before = 0, attempt = 0;
    wl_test_completed_step_t ended = {0};
    if (control) {
        wl_evaluation_control_report_t initial;
        int rc = wl_evaluation_control_report(control, &initial);
        CHECK(!rc || rc == ENOENT);
        if (!rc) completed_before = initial.attempt_id;
    }
    for (unsigned i = 0; i < 10000; ++i) {
        uint64_t before = control ?
            wl_evaluation_control_active_attempt_id(control) : 0;
        if (before) {
            CHECK(!ended.selected && (!attempt || attempt == before));
            attempt = before;
        }
        wirelog_error_t error = wirelog_session_step(session);
        if (!control) return error;
        wl_session_t *inner = wirelog_session_testhook_inner(session);
        CHECK(inner && inner->evaluation_control == control);
        uint64_t current = wl_evaluation_control_active_attempt_id(control);
        if (ended.selected) {
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(control, &report)
                && wl_test_completed_step_unchanged(&ended,
                inner->evaluation_control == control, current, &report));
        }
        if (current) {
            CHECK(!ended.selected && (!attempt || attempt == current));
            attempt = current;
        } else {
            if (attempt && wl_test_completed_step_observe(inner, control,
                current, attempt, &ended)) continue;
            /* Bootstrap and completed financial receipts are actual pending
             * metadata even before BEGIN or after matching Original FINISH.
             * An ended WORK result with no such debt still returns here. */
            wl_controlled_step_state_t state =
                inner->backend->session_controlled_step_state(inner);
            if (state == WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING)
                continue;
            if (state == WL_CONTROLLED_STEP_PENDING) {
                uint64_t binding[10];
                wl_evaluation_control_report_t report;
                int rc = wl_evaluation_control_report(control, &report);
                /* Cursor4's completed binding has a real id0 handoff before
                 * the first Native BEGIN. An actual ended WORK generation
                 * changes the Original report and must still return. */
                if (!wl_columnar_session_test_metadata_bind_view(
                        COL_SESSION(inner), binding)
                    && binding[0] == 2 && binding[1] == 4
                    && !binding[2] && !binding[3] && !binding[4]
                    && !binding[5] && !binding[6] && !binding[7]
                    && ((rc == ENOENT && !completed_before)
                    || (!rc && report.attempt_id == completed_before)))
                    continue;
            }
            return error;
        }
    }
    CHECK(false); return WIRELOG_ERR_EXEC;
}
extern int wl_columnar_expr_test_fail_allocation;
extern int wl_columnar_session_test_committed_metadata_bytes(
    const wl_col_session_t *, uint64_t *);
static wl_evaluation_control_t *facade_allocation_control;
static unsigned facade_allocation_progress, facade_allocation_hits;
static void facade_allocation_checkpoint(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress, bool failed){
    (void)failed;
    if (control != facade_allocation_control || phase != 76 ||
        progress != facade_allocation_progress)return;
    CHECK(!facade_allocation_hits &&
        wl_columnar_expr_test_fail_allocation == -1);
    ++facade_allocation_hits;
    wl_columnar_expr_test_checkpoint = NULL;
    wl_columnar_expr_test_fail_allocation = 0;
}
static void public_errors(void){
    for (unsigned abi = 0; abi < 2; ++abi)for (unsigned controlled = 0;
            controlled < 2;
            ++controlled)
            for (unsigned filter = 0; filter < 2; ++filter)
                for (unsigned fault = 1; fault <= 5; ++fault) {
                    bool allocation = fault >= 4;
                    if (allocation && !controlled) continue;
                    facade_t f = {.filter = filter, .fault = fault};
                    wirelog_extension_registry_t *registry =
                        wirelog_extension_registry_create(); CHECK(registry);
                    uint32_t type = WIRELOG_EXTENSION_VALUE_INT64;
                    wirelog_extension_descriptor_t d =
                    {.abi_version = WIRELOG_EXTENSION_ABI_VERSION,
                     .size = sizeof(d),
                     .name = "test.facade", .arity = 1,
                     .argument_types = &type,
                     .result_type =
                         filter?WIRELOG_EXTENSION_VALUE_BOOL:
                         WIRELOG_EXTENSION_VALUE_INT64,
                     .invoke =
                         facade_callback, .user_data = &f};
                    if (abi) {
                        d.addon_abi_identity = 123; d.addon_abi_version = 7;
                    }
                    CHECK(!wirelog_extension_register(registry, &d));
                    wirelog_extension_snapshot_t *snapshot =
                        wirelog_extension_snapshot_acquire(registry);
                    CHECK(snapshot);
                    const char *source = filter?
                        ".decl input(x:int64)\n.decl out(x:int64)\nout(x) :- input(x),@call(\"test.facade\",x).\n"
                        :
                        ".decl input(x:int64)\n.decl out(x:int64,y:int64)\nout(x,@call(\"test.facade\",x)) :- input(x).\n";
                    wirelog_error_t parse_error;
                    wirelog_program_t *program = wirelog_parse_string(source,
                            &parse_error); CHECK(program);
                    wirelog_session_t *session = NULL;
                    wl_evaluation_control_t *control = NULL;
                    wl_session_options_t options;
                    wl_session_options_init(&options);
                    if (controlled) {
                        CHECK(!wl_evaluation_control_create(0, &control));
                        options.evaluation_control = control;
                    }
                    wl_session_testhook_set_default_options(controlled?&options:
                        NULL);
                    wirelog_error_t created =
                        wirelog_session_create_with_snapshot(program,
                            WIRELOG_BACKEND_COLUMNAR, 1, snapshot, &session);
                    wl_session_testhook_set_default_options(NULL);
                    CHECK(created == WIRELOG_OK && session);
                    wl_session_t *inner =
                        wirelog_session_testhook_inner(session);
                    CHECK(inner && inner->evaluation_control == control);
                    wl_columnar_memory_governor_ref_t *g =
                        COL_SESSION(inner)->memory_governor;
                    wl_columnar_memory_governor_ref_retain(g);
                    CHECK(wirelog_session_set_delta_cb(session, facade_delta,
                        &f) == WIRELOG_OK);
                    int64_t one = 1, two = 2, three = 3, four = 4;
                    CHECK(wirelog_session_insert(session, "input", &one, 1,
                        1) == WIRELOG_OK &&
                        facade_drive(session,
                        control) == WIRELOG_OK && f.events == 1);
                    facade_contents(session, &f, 2, 1);
                    col_rel_t *committed = session_find_rel(COL_SESSION(inner),
                            "out");
                    CHECK(committed);
                    col_relation_snapshot_t committed_generation =
                        wl_columnar_relation_snapshot(committed);
                    CHECK(wirelog_session_insert(session, "input", &two, 1,
                        1) == WIRELOG_OK &&
                        wirelog_session_insert(session, "input", &three, 1,
                        1) == WIRELOG_OK);
                    uint64_t allocation_baseline = wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(g));
                    if (allocation) {
                        uint64_t prior_credit = 0;
                        CHECK(
                            !wl_columnar_session_test_committed_metadata_bytes(
                                COL_SESSION(inner), &prior_credit)
                            && !prior_credit);
                        /* Successful public STEP already retired the prior
                         * transaction's metadata and returned its credit. */
                    }
                    uint64_t failed_attempt = 0;
                    int semantic = allocation?WL_COLUMNAR_EXPR_OK:fault == 1?
                        WL_COLUMNAR_EXPR_CALLBACK_FAILURE:
                        WL_COLUMNAR_EXPR_INVALID_RESULT;
                    for (unsigned repeat = 0; repeat < (allocation?2u:1u);
                        ++repeat) {
                        if (repeat) CHECK(!wl_evaluation_control_reset(
                                    control));
                        unsigned prior_calls = f.calls;
                        wl_extension_error_set("seed public diagnostic");
                        if (allocation) {
                            facade_allocation_control = control;
                            facade_allocation_progress = fault-4;
                            facade_allocation_hits = 0;
                            wl_columnar_expr_test_checkpoint =
                                facade_allocation_checkpoint;
                        }
                        CHECK(facade_drive(session,
                            control) ==
                            (allocation?WIRELOG_ERR_MEMORY:WIRELOG_ERR_EXEC)
                            && COL_SESSION(inner)->extension_expr_status ==
                            semantic);
                        if (allocation) {
                            CHECK(facade_allocation_hits == 1 &&
                                !wl_columnar_expr_test_checkpoint
                                && wl_columnar_expr_test_fail_allocation ==
                                -1 && f.calls == prior_calls);
                            facade_allocation_control = NULL;
                            CHECK(!wl_evaluation_control_active_attempt_id(
                                    control)
                                && !COL_SESSION(inner)->cleanup_active &&
                                !COL_SESSION(inner)->cleanup_pending
                                && !COL_SESSION(inner)->cleanup_reserved_bytes
                                && !COL_SESSION(inner)->delta_observer &&
                                !COL_SESSION(inner)->delta_observer_build
                                && !COL_SESSION(
                                    inner)->delta_observer_prepare &&
                                !COL_SESSION(inner)->delta_publish_active
                                && !registry->active_leases &&
                                !wl_columnar_expr_test_callback_depth());
                            CHECK(wl_columnar_memory_reserved(
                                    wl_columnar_memory_governor_ref_get(g))
                                == allocation_baseline);
                            uint64_t remaining_credit = UINT64_MAX;
                            CHECK(
                                !
                                wl_columnar_session_test_committed_metadata_bytes
                                    (COL_SESSION(inner), &remaining_credit)
                                && !remaining_credit);
                            wl_evaluation_control_report_t failed_report;
                            CHECK(!wl_evaluation_control_report(control,
                                &failed_report)
                                && failed_report.result == ENOMEM &&
                                failed_report.execution_status == ENOMEM
                                && !failed_report.cleanup_status &&
                                failed_report.attempt_id != failed_attempt);
                            failed_attempt = failed_report.attempt_id;
                            CHECK(session_find_rel(COL_SESSION(inner),
                                "out") == committed
                                && wl_columnar_relation_snapshot_equal(
                                    committed_generation,
                                    wl_columnar_relation_snapshot(committed)));
                        }
                        CHECK(!strcmp(wirelog_extension_last_error(),
                            allocation?"seed public diagnostic":fault == 1?
                            "scalar extension callback failed":
                            "scalar extension returned an invalid result"));
                        CHECK(f.events == 1 && !f.negative);
                    }
                    wl_evaluation_control_report_t before, after;
                    if (controlled) {
                        CHECK(session_find_rel(COL_SESSION(inner),
                            "out") == committed && committed->nrows == 1 &&
                            wl_columnar_relation_snapshot_equal(
                                committed_generation,
                                wl_columnar_relation_snapshot(committed)));
                        CHECK(!wl_evaluation_control_report(control,
                            &before) &&
                            before.result == (allocation?ENOMEM:semantic) &&
                            !before.cleanup_status &&
                            !COL_SESSION(inner)->cleanup_active &&
                            !COL_SESSION(inner)->cleanup_pending &&
                            !COL_SESSION(inner)->cleanup_reserved_bytes);
                    }else {
                        CHECK(wirelog_session_remove(session, "input", &two, 1,
                            1) == WIRELOG_ERR_EXEC &&
                            COL_SESSION(inner)->delta_observer);
                        f.fault = 0;
                        CHECK(facade_drive(session, control) == WIRELOG_OK);
                        facade_contents(session, &f, 2|4|8, 3);
                        CHECK(f.events == 3 && f.positive == 3 && !f.negative);
                    }
                    wirelog_error_t removed = wirelog_session_remove(session,
                            "input", &two, 1, 1);
                    wirelog_error_t inserted = removed ==
                        WIRELOG_OK?wirelog_session_insert(session, "input",
                            &four, 1, 1):WIRELOG_ERR_EXEC;
                    CHECK(removed == WIRELOG_OK && inserted == WIRELOG_OK);
                    if (controlled) {
                        CHECK(!wl_evaluation_control_report(control,
                            &after) && after.attempt_id == before.attempt_id &&
                            after.result == before.result &&
                            after.consumed == before.consumed &&
                            after.limit == before.limit &&
                            after.execution_status == before.execution_status &&
                            after.cleanup_status == before.cleanup_status &&
                            after.stop_reason == before.stop_reason &&
                            !wl_evaluation_control_reset(control));
                    }
                    CHECK(facade_drive(session, control) == WIRELOG_OK);
                    facade_contents(session, &f, 2|8|16, 3);
                    unsigned expected_events = controlled?3u:5u,
                        expected_positive = controlled?3u:4u,
                        expected_negative = controlled?0u:1u;
                    CHECK(f.events == expected_events &&
                        f.positive == expected_positive &&
                        f.negative == expected_negative);
                    CHECK(wirelog_session_remove(session, "input", &three, 1,
                        1) == WIRELOG_OK &&
                        facade_drive(session,
                        control) == WIRELOG_OK &&
                        f.events == expected_events+1 &&
                        f.negative == expected_negative+1);
                    facade_contents(session, &f, 2|16, 2);
                    CHECK(wirelog_session_insert(session, "input", &three, 1,
                        1) == WIRELOG_OK &&
                        facade_drive(session,
                        control) == WIRELOG_OK &&
                        f.events == expected_events+2 &&
                        f.positive == expected_positive+1 &&
                        facade_drive(session,
                        control) == WIRELOG_OK &&
                        f.events == expected_events+2);
                    CHECK(!registry->active_leases &&
                        !wl_columnar_expr_test_callback_depth());
                    wirelog_session_destroy(session);
                    wirelog_program_free(program);
                    if (control) wl_evaluation_control_release(control);
                    wirelog_extension_snapshot_release(snapshot);
                    CHECK(!wirelog_extension_unregister(registry,
                        "test.facade") &&
                        !wirelog_extension_registry_destroy(registry));
                    CHECK(!wl_columnar_memory_reserved(
                            wl_columnar_memory_governor_ref_get(g)));
                    wl_columnar_memory_governor_ref_release(g);
                }
}

static wirelog_extension_registry_t *depth_registry;
static wirelog_extension_snapshot_t *depth_snapshot;
static unsigned depth_policy, depth_mode, depth_calls;
static int depth_keys[65]; static bool depth_nested;
static void depth_component(bool same_key){
    uint8_t bytes[32];
    uint32_t n = call(bytes, WL_PLAN_EXPR_EXTENSION_CALL, "depth", 5, 1, 0, 0);
    wirelog_extension_value_t arg = {.type = WIRELOG_EXTENSION_VALUE_INT64,
                                     .size = 8, .as.int64_value = 7};
    wl_columnar_expr_context_t context = {.extensions = depth_snapshot,
                                          .allow_extension_scalar_result = true,
                                          .session_key =
                                              &depth_keys[same_key?0:64],
                                          .configured_worker_count = 4,
                                          .active_worker_count = 2,
                                          .parallel_execution = true};
    bool reentrant = (depth_policy&WIRELOG_EXTENSION_CALLBACK_REENTRANT) != 0;
    unsigned level = wl_columnar_expr_test_callback_depth();
    CHECK(level == (same_key?1:64));
    /* name5=>runtime6+lookup29. Same-key nonreentrant fails afterD1:6+1;
     * same-key reentrant succeeds16+A1+lease5. At capacity:10+D+A1,
     * D64 unless reentrant, when D0. No capacity failure takes a lease. */
    unsigned packets = same_key?(reentrant?57:42):(reentrant?46:110);
    for (unsigned below = 0; below < 2; ++below) {
        wl_evaluation_control_t *c = begin(UINT64_C(224)*packets-below);
        int64_t value = 999; bool failed = false;
        wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_OK;
        depth_nested = true; unsigned before = depth_calls;
        int rc = wl_columnar_expr_test_extension_invoke(bytes, n, 3,
                (UINT64_C(1)<<16)|5, WL_PLAN_EXPR_EXTENSION_CALL, &arg, 1,
                &context,
                &value, &failed, &status, c, id(c)); depth_nested = false;
        if (below)CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                value == 999 &&
                !failed && status == WL_COLUMNAR_EXPR_OK);
        else if (same_key &&
            reentrant) CHECK(!rc && value == 1 && !failed &&
                status == WL_COLUMNAR_EXPR_OK);
        else CHECK(!rc && !value && failed &&
                status == WL_COLUMNAR_EXPR_CALLBACK_REENTRANT);
        CHECK(depth_calls == before+(same_key && reentrant) &&
            wl_columnar_expr_test_callback_depth() == level &&
            depth_registry->active_leases == level);
        finish(c, rc);
    }
    if (!same_key && !reentrant)for (unsigned offset = 0; offset < 64;
            ++offset) for (action = 0;
                action < 4; ++action){
                wl_evaluation_control_t *c = begin(action == 1?UINT64_MAX-1:0);
                int64_t value = 999; bool failed = false;
                wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_OK;
                stop_phase = 520; stop_progress = offset; stopped = false;
                successor = 0;
                wl_columnar_expr_test_checkpoint = stop; depth_nested = true;
                int rc = wl_columnar_expr_test_extension_invoke(bytes, n, 3,
                        (UINT64_C(1)<<16)|5, WL_PLAN_EXPR_EXTENSION_CALL, &arg,
                        1, &context,
                        &value, &failed, &status, c, id(c));
                depth_nested = false; wl_columnar_expr_test_checkpoint = NULL;
                CHECK(stopped && rc == expected_rc() && value == 999 &&
                    !failed && status == WL_COLUMNAR_EXPR_OK &&
                    wl_columnar_expr_test_callback_depth() == 64 &&
                    depth_registry->active_leases == 64); retry(c, rc);
                finish(c, 0);
            }
}
static int depth_callback(const wirelog_extension_value_t *args, uint32_t count,
    wirelog_extension_value_t *result, void *data){
    (void)data;
    CHECK(count == 1 && args[0].type == WIRELOG_EXTENSION_VALUE_INT64 &&
        args[0].as.int64_value == 7); ++depth_calls;
    unsigned level = wl_columnar_expr_test_callback_depth();
    CHECK(level >= 1 && level <= 64);
    if (!depth_nested) {
        if (depth_mode == 1)depth_component(true);
        else if (level < 64) {
            uint8_t bytes[64];
            uint32_t base = integer(bytes, 0, 7),
                n = base+call(bytes+base, WL_PLAN_EXPR_EXTENSION_CALL, "depth",
                    5, 1, 0, 0);
            wl_columnar_expr_context_t context = {.extensions = depth_snapshot,
                                                  .allow_extension_scalar_result
                                                      = true,
                                                  .session_key =
                                                      &depth_keys[level],
                                                  .active_worker_count = 1};
            int64_t value = 999;
            wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_OK;
            CHECK(!wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0, &value,
                &context,
                &status) && value == 1 && status == WL_COLUMNAR_EXPR_OK);
        }else depth_component(false);
    }
    result->type = WIRELOG_EXTENSION_VALUE_BOOL; result->size = 1;
    result->as.bool_value = 1; return 0;
}
static void policy_depth(void){
    for (unsigned reentrant = 0; reentrant < 2; ++reentrant)for (depth_mode = 0;
            depth_mode < 2; ++depth_mode) {
            depth_registry = wirelog_extension_registry_create();
            CHECK(depth_registry);
            uint32_t type = WIRELOG_EXTENSION_VALUE_INT64;
            depth_policy = WIRELOG_EXTENSION_CALLBACK_THREAD_SAFE|
                (reentrant?WIRELOG_EXTENSION_CALLBACK_REENTRANT:0);
            wirelog_extension_descriptor_t descriptor =
            {.abi_version = WIRELOG_EXTENSION_ABI_VERSION,
             .size = sizeof(descriptor), .name = "depth",
             .arity = 1,
             .argument_types = &type,
             .result_type = WIRELOG_EXTENSION_VALUE_BOOL,
             .invoke = depth_callback, .callback_policy = depth_policy};
            CHECK(!wirelog_extension_register(depth_registry, &descriptor));
            depth_snapshot = wirelog_extension_snapshot_acquire(depth_registry);
            CHECK(depth_snapshot);
            uint8_t bytes[64];
            uint32_t base = integer(bytes, 0, 7),
                n = base+call(bytes+base, WL_PLAN_EXPR_EXTENSION_CALL, "depth",
                    5, 1, 0, 0);
            wl_columnar_expr_context_t context = {.extensions = depth_snapshot,
                                                  .allow_extension_scalar_result
                                                      = true,
                                                  .session_key = &depth_keys[0],
                                                  .active_worker_count = 1};
            int64_t value = 999;
            wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_OK;
            depth_calls = 0;
            CHECK(!wl_columnar_expr_eval_run_ctx(bytes, n, NULL, 0, &value,
                &context,
                &status) && value == 1 && status == WL_COLUMNAR_EXPR_OK &&
                !wl_columnar_expr_test_callback_depth() &&
                !depth_registry->active_leases);
            wirelog_extension_snapshot_release(depth_snapshot);
            CHECK(!wirelog_extension_unregister(depth_registry,
                "depth") &&
                !wirelog_extension_registry_destroy(depth_registry));
            depth_registry = NULL; depth_snapshot = NULL;
        }
}

int main(int argc, char **argv){
    if (argc == 2 && !strcmp(argv[1], "join-forward")) {
        join_forward_refusals(); return 0;
    }
    if (argc != 1) return 2;
    decode_oracles(); invoke_oracles(); scalar_parity(); malformed_parity();
    decode_stops(); invoke_stops(); lifetime(); policy_depth();
    session_positive();
    session_stops(); host_errors(); join_forward_refusals(); public_errors();
    puts("bounded extension expressions: all checks passed"); return 0;
}
