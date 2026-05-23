#include <android/log.h>
#include <android/asset_manager.h>
#include <android/asset_manager_jni.h>
#include <errno.h>
#include <jni.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <wirelog/io/io_adapter.h>
#include <wirelog/wirelog.h>

#define LOG_TAG "wirelog-asset-adapter"
#define INITIAL_ROW_CAPACITY 64

typedef struct {
    JavaVM *vm;
    jobject java_asset_manager;
    AAssetManager *asset_manager;
} app_adapter_state_t;

static app_adapter_state_t g_adapter_state;

static void
log_error(const char *message)
{
    __android_log_print(ANDROID_LOG_ERROR, LOG_TAG, "%s", message);
}

static void
clear_asset_manager_state(void)
{
    JNIEnv *env = NULL;
    bool vm_attached = false;

    if (g_adapter_state.vm != NULL) {
        jint get_env_status = (*g_adapter_state.vm)->GetEnv((void **)&env,
                JNI_VERSION_1_6);
        if (get_env_status == JNI_EDETACHED) {
            if ((*g_adapter_state.vm)->AttachCurrentThread(
                    g_adapter_state.vm,
                    (void **)&env,
                    NULL) == JNI_OK) {
                vm_attached = true;
            } else {
                __android_log_print(ANDROID_LOG_WARN, LOG_TAG,
                    "cannot clear old AssetManager global ref: failed to attach");
            }
        } else if (get_env_status != JNI_OK) {
            __android_log_print(ANDROID_LOG_WARN, LOG_TAG,
                "cannot clear old AssetManager global ref: GetEnv failed");
        }
    }

    if (env != NULL && g_adapter_state.java_asset_manager != NULL) {
        (*env)->DeleteGlobalRef(env, g_adapter_state.java_asset_manager);
        g_adapter_state.java_asset_manager = NULL;
    } else if (g_adapter_state.java_asset_manager != NULL) {
        __android_log_print(ANDROID_LOG_WARN, LOG_TAG,
            "cannot clear old AssetManager global ref: JNIEnv unavailable");
        g_adapter_state.java_asset_manager = NULL;
    }

    if (vm_attached) {
        (*g_adapter_state.vm)->DetachCurrentThread(g_adapter_state.vm);
    }

    g_adapter_state.asset_manager = NULL;
}

static void
set_asset_manager(JNIEnv *env, jobject java_asset_manager)
{
    clear_asset_manager_state();
    if (java_asset_manager == NULL) {
        log_error("received NULL AssetManager");
        return;
    }

    jobject global_manager = (*env)->NewGlobalRef(env, java_asset_manager);
    if (global_manager == NULL) {
        log_error("failed to create global ref for AssetManager");
        return;
    }

    AAssetManager *asset_manager = AAssetManager_fromJava(env,
            java_asset_manager);
    if (asset_manager == NULL) {
        (*env)->DeleteGlobalRef(env, global_manager);
        log_error("failed to convert Java AssetManager to native");
        return;
    }

    g_adapter_state.java_asset_manager = global_manager;
    g_adapter_state.asset_manager = asset_manager;
}

static bool
all_columns_are_int64(wirelog_io_ctx_t *ctx)
{
    uint32_t ncols = wirelog_io_ctx_num_cols(ctx);
    for (uint32_t i = 0; i < ncols; ++i) {
        if (wirelog_io_ctx_col_type(ctx, i) != WIRELOG_TYPE_INT64) {
            return false;
        }
    }
    return true;
}

static int
android_asset_validate(wirelog_io_ctx_t *ctx, char *errbuf, size_t errbuf_len,
    void *user_data)
{
    app_adapter_state_t *state = (app_adapter_state_t *)user_data;
    if (!state || !state->asset_manager) {
        if (errbuf_len > 0) {
            snprintf(errbuf, errbuf_len,
                "asset adapter not configured with an AssetManager");
        }
        return -1;
    }

    const char *filename = wirelog_io_ctx_param(ctx, "filename");
    if (!filename || filename[0] == '\0') {
        if (errbuf_len > 0) {
            snprintf(errbuf, errbuf_len, "missing required io param: filename");
        }
        return -1;
    }

    if (!all_columns_are_int64(ctx)) {
        if (errbuf_len > 0) {
            snprintf(errbuf, errbuf_len,
                "wirelog-android adapter currently supports only int64 columns");
        }
        return -1;
    }

    return 0;
}

static void
free_rows(int64_t *parsed_rows)
{
    free(parsed_rows);
}

static int
append_value(int64_t *row_values, uint32_t ncols, uint32_t row_idx,
    uint32_t col_idx,
    const char *token, size_t token_len, char *errbuf, size_t errbuf_len)
{
    char copy[32];
    if (token_len == 0 || token_len >= sizeof(copy)) {
        if (errbuf_len > 0) {
            snprintf(errbuf, errbuf_len, "invalid int64 token in input data");
        }
        return -1;
    }

    memcpy(copy, token, token_len);
    copy[token_len] = '\0';

    char *tail = NULL;
    errno = 0;
    long long value = strtoll(copy, &tail, 10);
    if (errno == ERANGE) {
        if (errbuf_len > 0) {
            snprintf(errbuf, errbuf_len,
                "int64 token out of range: %s", copy);
        }
        return -1;
    }
    if (!tail || *tail != '\0') {
        if (errbuf_len > 0) {
            snprintf(errbuf, errbuf_len, "non-integer token: %s", copy);
        }
        return -1;
    }
    row_values[row_idx * ncols + col_idx] = value;
    return 0;
}

static int
read_asset_fully(AAsset *asset, char *buffer, size_t byte_count)
{
    size_t read_total = 0;
    while (read_total < byte_count) {
        int64_t chunk = AAsset_read(asset, buffer + read_total,
                byte_count - read_total);
        if (chunk < 0) {
            return -1;
        }
        if (chunk == 0) {
            return -1;
        }

        read_total += (size_t)chunk;
    }

    return 0;
}

static int
android_asset_read(wirelog_io_ctx_t *ctx, int64_t **out_data,
    uint32_t *out_nrows,
    void *user_data)
{
    app_adapter_state_t *state = (app_adapter_state_t *)user_data;
    if (!state || !state->asset_manager) {
        log_error("asset adapter not configured with an AssetManager");
        return -1;
    }

    const char *filename = wirelog_io_ctx_param(ctx, "filename");
    if (!filename || filename[0] == '\0') {
        log_error("missing required io param: filename");
        return -1;
    }

    if (!all_columns_are_int64(ctx)) {
        log_error(
            "wirelog-android adapter currently supports only int64 columns");
        return -1;
    }

    uint32_t ncols = wirelog_io_ctx_num_cols(ctx);
    if (ncols == 0) {
        log_error("invalid schema: expected at least one int64 column");
        return -1;
    }

    AAsset *asset = AAssetManager_open(state->asset_manager, filename,
            AASSET_MODE_STREAMING);
    if (!asset) {
        __android_log_print(ANDROID_LOG_ERROR, LOG_TAG,
            "unable to open asset: %s", filename);
        return -1;
    }

    size_t byte_count = (size_t)AAsset_getLength64(asset);
    if (byte_count == 0) {
        AAsset_close(asset);
        *out_data = NULL;
        *out_nrows = 0;
        return 0;
    }

    char *text = (char *)malloc(byte_count + 1);
    if (!text) {
        AAsset_close(asset);
        log_error("out of memory while reading asset");
        return -1;
    }

    if (read_asset_fully(asset, text, byte_count) != 0) {
        free(text);
        AAsset_close(asset);
        log_error("failed to read full asset payload");
        return -1;
    }
    AAsset_close(asset);
    text[byte_count] = '\0';

    size_t row_capacity = INITIAL_ROW_CAPACITY;
    size_t rows = 0;
    int64_t *parsed_rows = (int64_t *)malloc(row_capacity * ncols *
            sizeof(int64_t));
    if (!parsed_rows) {
        free(text);
        log_error("out of memory while allocating row buffer");
        return -1;
    }

    char *line_save = NULL;
    for (char *line = strtok_r(text, "\r\n", &line_save);
        line != NULL;
        line = strtok_r(NULL, "\r\n", &line_save)) {

        bool has_non_ws = false;
        for (const char *scan = line; *scan != '\0'; ++scan) {
            if (*scan != ' ' && *scan != '\t') {
                has_non_ws = true;
                break;
            }
        }
        if (!has_non_ws) {
            continue;
        }

        if (rows == row_capacity) {
            size_t next_capacity = row_capacity * 2;
            int64_t *grown = (int64_t *)realloc(
                parsed_rows,
                next_capacity * ncols * sizeof(int64_t));
            if (!grown) {
                free_rows(parsed_rows);
                free(text);
                log_error("out of memory while growing row buffer");
                return -1;
            }
            parsed_rows = grown;
            row_capacity = next_capacity;
        }

        uint32_t col_idx = 0;
        char token_error[96] = {0};
        char *token_save = NULL;
        for (char *token = strtok_r(line, ",", &token_save);
            token != NULL;
            token = strtok_r(NULL, ",", &token_save), ++col_idx) {

            if (col_idx >= ncols) {
                free_rows(parsed_rows);
                free(text);
                __android_log_print(ANDROID_LOG_ERROR, LOG_TAG,
                    "too many columns in row %zu", rows);
                return -1;
            }

            while (*token == ' ' || *token == '\t') {
                ++token;
            }
            char *token_end = token + strlen(token);
            while (token_end > token &&
                ((token_end[-1] == ' ') || (token_end[-1] == '\t'))) {
                --token_end;
            }

            if (append_value(parsed_rows, ncols, (uint32_t)rows, col_idx,
                token, (size_t)(token_end - token), token_error,
                sizeof(token_error)) != 0) {
                free_rows(parsed_rows);
                free(text);
                log_error(token_error);
                return -1;
            }
        }

        if (col_idx != ncols) {
            free_rows(parsed_rows);
            free(text);
            log_error("row column count does not match schema");
            return -1;
        }
        ++rows;
    }

    free(text);

    if ((uint64_t)rows > UINT32_MAX) {
        free_rows(parsed_rows);
        log_error("too many rows for API return type");
        return -1;
    }

    *out_data = parsed_rows;
    *out_nrows = (uint32_t)rows;
    return 0;
}

static wirelog_io_adapter_t g_asset_adapter = {
    .abi_version = WIRELOG_IO_ABI_VERSION,
    .scheme = "android_asset",
    .description = "Android AAsset-backed CSV adapter",
    .read = android_asset_read,
    .validate = android_asset_validate,
    .user_data = &g_adapter_state,
};

JNIEXPORT void JNICALL
Java_com_wirelog_asset_AssetAdapter_setAssetManager(JNIEnv *env, jclass clazz,
    jobject java_asset_manager)
{
    (void)clazz;
    set_asset_manager(env, java_asset_manager);
}

JNIEXPORT jint JNICALL
JNI_OnLoad(JavaVM *vm, void *reserved)
{
    (void)reserved;
    g_adapter_state.vm = vm;
    g_adapter_state.asset_manager = NULL;
    g_adapter_state.java_asset_manager = NULL;

    if (wirelog_io_register_adapter(&g_asset_adapter) != 0) {
        __android_log_print(ANDROID_LOG_ERROR, LOG_TAG,
            "wirelog_io_register_adapter failed: %s",
            wirelog_io_last_error());
        return JNI_ERR;
    }
    return JNI_VERSION_1_6;
}

JNIEXPORT void JNICALL
Java_com_wirelog_asset_AssetAdapter_unregister(JNIEnv *env, jclass clazz)
{
    (void)env;
    (void)clazz;
    clear_asset_manager_state();
    wirelog_io_unregister_adapter("android_asset");
}

JNIEXPORT void JNICALL
JNI_OnUnload(JavaVM *vm, void *reserved)
{
    (void)reserved;
    g_adapter_state.vm = vm;
    clear_asset_manager_state();
    wirelog_io_unregister_adapter("android_asset");
}
