# Android Integration

This document is the issue-#466 Android integration recipe for
`libwirelog.so` consumption and custom I/O adapter embedding.

## Packaging and distribution status

`wirelog` ships Android cross-compilation and source-integration support.
It does not currently publish artifacts to Maven Central or JitPack.
For now, consumption is:

- source inclusion with local build tooling, or
- consuming a local prefab `aar` you generate in your build pipeline.

Do not expect hosted `aar`/`prefab` coordinates from this repository at
this version.

## AAR / Prefab consumption pattern

The current v0.43 surface supports integration through Gradle/CMake
linkage and JNI; if you already produce a local Prefab-capable `.aar`,
consume it as a local artifact:

```gradle
dependencies {
    implementation(files("libs/wirelog-prefab-release.aar"))
}
```

If you do not have an `.aar`, include wirelog as a native source
dependency and run the documented Android cross build in CI-compatible mode.
The supported cross-build entries are:

- `cross/android-arm64.ini`
- `cross/android-x86_64.ini`

`armv7` is intentionally unsupported; wirelog's Android release policy is
64-bit only.

## Build compatibility contract

- API level: minimum `21` (per shipped cross-files).
- Page alignment:
  - `cross/android-arm64.ini` builds with the `aarch64` toolchain.
  - `meson build -Dandroid=true` adds
    `-Wl,-z,max-page-size=16384` for arm64-v8a.
- Toolchain pin: NDK `r27c` is pinned in `cross/android-arm64.ini`
  and `cross/android-x86_64.ini` as `ndk = '/opt/android-ndk-r27c'`.

The Android hard gate in CI enforces this alignment for arm64-v8a
artifacts before merge; this protects Play Store-upload constraints where
misaligned arm64 binaries can be rejected on API 35+.

## Register an adapter from JNI

Use the current public API names (`wirelog_io_register_adapter`,
`wirelog_io_find_adapter`, `wirelog_io_last_error`).

```c
#include <android/log.h>
#include <jni.h>
#include <wirelog/io/io_adapter.h>
#include <wirelog/wirelog.h>

typedef struct {
    JavaVM* vm;
    jstring context_key;
} app_jni_state_t;

static app_jni_state_t g_app_state;

static int android_asset_read(
    wirelog_io_ctx_t* ctx,
    int64_t** out_data,
    uint32_t* out_nrows,
    void* user_data
) {
    app_jni_state_t* state = (app_jni_state_t*)user_data;
    if (!state || !state->vm) {
        return -1;
    }

    JNIEnv* env = NULL;
    bool attached = false;
    int rc = (*state->vm)->GetEnv((void**)&env, JNI_VERSION_1_6);
    if (rc != JNI_OK) {
        if ((*state->vm)->AttachCurrentThread(
                state->vm,
                (void**)&env,
                NULL
            ) != 0) {
            return -1;
        }
        attached = true;
    }

    // Resolve input params and fill out_data/out_nrows here.
    (void)ctx;
    (void)out_data;
    (void)out_nrows;
    (void)state;

    if (attached) {
        (*state->vm)->DetachCurrentThread(state->vm);
    }
    return 0;
}

JNIEXPORT jint JNICALL
JNI_OnLoad(JavaVM* vm, void* reserved)
{
    (void)reserved;
    g_app_state.vm = vm;
    g_app_state.context_key = NULL;

    static wirelog_io_adapter_t adapter = {
        .abi_version = WIRELOG_IO_ABI_VERSION,
        .scheme = "android_asset",
        .description = "Android-local asset adapter",
        .read = android_asset_read,
        .validate = NULL,
        .user_data = NULL,
    };
    adapter.user_data = &g_app_state;

    if (wirelog_io_register_adapter(&adapter) != 0) {
        return JNI_ERR;
    }
    return JNI_VERSION_1_6;
}
```

### App side path handling

Android apps should resolve custom payloads through app storage, not URI
schemes that look like file paths.

- `android_asset://` is an adapter scheme, not a filesystem path.
- Convert packaged assets to app-private storage first, then pass
  filesystem paths from `Context.getFilesDir()`.

From Kotlin:

```kotlin
val assetDir: File = context.filesDir
val csvPath = File(assetDir, "seed/input.csv").absolutePath
// Pass csvPath via io params in your .input directive.
```

## JNI/threading contract

The native adapter callback may run on wirelog worker threads.
Any JNI call site must follow:

- call `AttachCurrentThread()` before using `JNIEnv*` on a worker
  thread not created by Java,
- keep the attached `JNIEnv*` only for that callback scope,
- pair each successful attachment with `DetachCurrentThread()` before return.

For callbacks that do not require JNI, keep work native only to reduce
JNI transitions.

Also confine each `wirelog_session_t` / easy-session handle to one
execution context. If multiple threads/tasks need one handle, serialize
those calls externally with a lock or single-thread executor.

## Session and resource cleanup

Because `wirelog` is embedded into the app process, avoid keeping session
handles global across unrelated work queues without external serialization.

For clean shutdown, unregister adapter handles before library unload in
native finalization paths.

## Follow-up work

`examples/android/asset_adapter` is planned for #466 follow-up.
It is not part of this atomic unit.

## Cross references

- `cross/android-arm64.ini`
- `cross/android-x86_64.ini`
- `docs/cross-compile-android.md`
- `docs/io-adapters.md`
- `wirelog/io/io_adapter.h`
- `.github/workflows/android.yml`
