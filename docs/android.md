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

The current v0.43 surface ships cross-build/source support only; it does not
publish hosted Maven/JitPack artifacts. If you already produce a local
Prefab-capable `.aar`, consume it as a local artifact:

```gradle
android {
    // Keep prefab generation enabled for native dependencies.
    buildFeatures {
        prefab true
    }
}

dependencies {
    implementation(files("libs/wirelog-prefab-release.aar"))
}

// If the .aar does not include a CMake package, skip this and use
// your own CMake wiring.
```

From `CMakeLists.txt`:

```cmake
find_package(wirelog REQUIRED CONFIG)
target_link_libraries(mynativelib PRIVATE wirelog::wirelog)
```

If you do not have an `.aar`, include wirelog as a native source
dependency and run the documented Android cross build in CI-compatible mode.
The supported cross-build entries are:

- `cross/android-arm64.ini`
- `cross/android-x86_64.ini`

`armv7` is intentionally unsupported; wirelog's Android release policy is
64-bit only.

## Build compatibility contract

- API level and target policy:
  - Native NDK API floor is `21` (from the cross-files).
  - Current Play distribution requirements for new apps/updates submitted from
    Aug 31 2025 on are API 35+ (Android 15); Wear/Auto/TV may have exception
    paths.
- Page-size and alignment:
  - `cross/android-arm64.ini` builds with the `aarch64` toolchain.
  - `meson build -Dandroid=true` applies
    `-Wl,-z,max-page-size=16384` for arm64-v8a.
  - CI validates arm64 PT_LOAD alignment with readelf-backed checks.
  - Android NDK guidance can also include
    `-Wl,-z,common-page-size=16384`; if your app applies it, align your
    entire native surface accordingly and validate every native library in your
    APK/AAB against current Android guidance.
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
#include <stdbool.h>
#include <jni.h>
#include <wirelog/io/io_adapter.h>
#include <wirelog/wirelog.h>

typedef struct {
    JavaVM* vm;
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
    int rc = (*state->vm)->GetEnv(state->vm, (void**)&env, JNI_VERSION_1_6);
    if (rc != JNI_OK) {
        if ((*state->vm)->AttachCurrentThread(
                state->vm,
                &env,
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
        __android_log_print(
            ANDROID_LOG_ERROR,
            "wirelog",
            "adapter register failed: %s",
            wirelog_io_last_error()
        );
        return JNI_ERR;
    }
    return JNI_VERSION_1_6;
}
```

### App side path handling

Built-in adapters (for example `io="csv"`) expect real filesystem paths.
Copy packaged assets to app-private storage and pass absolute paths to
input params:

From Kotlin:

```kotlin
val assetDir: File = context.filesDir
val csvPath = File(assetDir, "seed/input.csv").absolutePath
// Pass csvPath via io params in your .input directive.
```

Planned/custom `io="android_asset"` adapters are scheme-based.
In that design, the `filename` value is interpreted through
`AAssetManager` and is not a direct filesystem path. In that case,
`android_asset://...` is a scheme marker, not a local file path.

## JNI/threading contract

The native adapter callback in this integration path runs synchronously from
`wirelog_load_input_files()` on the invoking thread. It is not automatically
dispatched onto wirelog worker
threads.

When calling JNI from the callback, follow:

- call `GetEnv()` to detect whether the thread is attached,
- call `AttachCurrentThread()` only if the thread is not already attached,
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
native finalization paths using:

```c
wirelog_io_unregister_adapter("android_asset");
```

## Reference implementation

Use `examples/android/asset_adapter` as the reference implementation for a full
`android_asset` adapter backed by `AAssetManager`, including native JNI state
setup and `.input` usage.

- [Asset adapter example README](../examples/android/asset_adapter/README.md)

## Cross references

- `cross/android-arm64.ini`
- `cross/android-x86_64.ini`
- `docs/cross-compile-android.md`
- `docs/io-adapters.md`
- `wirelog/io/io_adapter.h`
- `.github/workflows/android.yml`
