# Android Asset Adapter Example

This example demonstrates a minimal `android_asset` I/O adapter backed by
`AAssetManager`. It uses only the public `wirelog` registration API:

- `wirelog_io_register_adapter`
- `wirelog_io_unregister_adapter`
- `wirelog_io_ctx_param`
- `wirelog_io_ctx_num_cols`
- `wirelog_io_ctx_col_type`
- `wirelog_io_last_error`

The adapter reads assets declared via `io="android_asset"` in `.input` and
parses comma/newline separated `int64` rows.

## Integration flow

1. Build the shared library from CMake (`asset_adapter`) and package as part
   of your app.
2. In JNI `OnLoad`, the adapter is registered.
3. Call the generated setter once at startup to provide
   `android.content.res.AssetManager`.

`JNI_OnLoad` cannot receive an `AssetManager` object directly. The example uses
a tiny Java/JNI bridge:

```java
public final class AssetAdapter {
    static {
        System.loadLibrary("asset_adapter");
    }

    public static native void setAssetManager(android.content.res.AssetManager manager);
}
```

Call from Java/Kotlin before the first `wirelog` run:

```kotlin
AssetAdapter.setAssetManager(context.assets)
```

This stores an `AAssetManager*` in native state as the adapter `user_data`
payload alongside the `JavaVM*`.

## CMake usage

```cmake
find_package(wirelog REQUIRED CONFIG)

add_library(asset_adapter SHARED
    asset_adapter.c
)

target_link_libraries(asset_adapter
    PRIVATE wirelog::wirelog
    PRIVATE android
    PRIVATE log
)
```

## `.input` usage

In your Wirelog program:

```wire
.input R(io="android_asset", filename="seed/input.csv")
```

Each non-empty line in `seed/input.csv` must contain one value per
`wirelog_io_ctx_num_cols()` column, all `int64`.

## Validation

The sample is intended as a reference implementation for Android integration.
If your workstation does not have Android NDK/CMake configured for this build
target, skip execution and treat this as compile-time reference documentation.
