# Cross-compiling wirelog for Android

Issue #464 lands the meson plumbing and NDK cross-files needed to build
`libwirelog.so` for Android-on-arm64 and Android-on-x86_64. This document
is the local-recipe companion. CI matrix coverage is a separate follow-up
issue.

## Supported ABIs

| ABI | Cross-file | Notes |
|---|---|---|
| `arm64-v8a` (aarch64) | `cross/android-arm64.ini` | Primary modern target. 16 KB page support via `-Wl,-z,max-page-size=16384` (Android 14+). |
| `x86_64` | `cross/android-x86_64.ini` | Emulator and rare 64-bit x86 devices. 4 KB pages. |

`armeabi-v7a` is intentionally not supported. Google Play Store has
required 64-bit uploads since 2019; armv7 is a separate toolchain
maintenance burden for near-zero modern delivery reach.

## Toolchain

NDK r27c (LTS) is the pinned version. The cross-files reference its
default install prefix `/opt/android-ndk-r27c`. To override:

1. **Symlink your NDK install** to the default prefix (simplest):
   ```sh
   sudo ln -s "$ANDROID_NDK_ROOT" /opt/android-ndk-r27c
   ```
2. **Edit the cross-file locally** and do NOT commit the edit. The `ndk`
   constant at the top of each cross-file is the single point of
   override.
3. **Use a sed-substituted copy** if you prefer not to symlink:
   ```sh
   sed "s|/opt/android-ndk-r27c|$ANDROID_NDK_ROOT|" \
       cross/android-arm64.ini > /tmp/android-arm64.ini
   meson setup build-android-arm64 \
       --cross-file /tmp/android-arm64.ini -Dandroid=true
   ```

Meson cross-files do not natively interpolate shell environment
variables; the `[constants]` section in each cross-file is the
substitution surface.

## Host platform

Each cross-file defaults to `host_tag = 'linux-x86_64'`. To build from
macOS or Windows host machines, change `host_tag` in the cross-file to
one of `darwin-x86_64` or `windows-x86_64`.

## Recipe

```sh
# Prerequisites: NDK r27c installed and either symlinked to
# /opt/android-ndk-r27c OR the cross-file edited / sed-substituted.

# arm64-v8a (modern primary)
meson setup build-android-arm64 \
    --cross-file cross/android-arm64.ini \
    -Dandroid=true \
    -Dtests=false \
    -DmbedTLS=disabled
meson compile -C build-android-arm64

# x86_64 (emulator)
meson setup build-android-x86_64 \
    --cross-file cross/android-x86_64.ini \
    -Dandroid=true \
    -Dtests=false \
    -DmbedTLS=disabled
meson compile -C build-android-x86_64
```

`-Dtests=false` is recommended because the host-only test harness is not
designed to run under cross-compilation. CLI-dependent tests in
`tests/meson.build` are gated on `if not is_android` anyway, but the
remainder of the suite assumes a host-runnable build.

`-DmbedTLS=disabled` is the default. The Android NDK does not ship
system mbedTLS; consumers needing cryptography on Android should vendor
mbedTLS as a meson subproject (out of scope for #464).

## Output

After `meson compile`, the shared library lands at:

| ABI | Path |
|---|---|
| arm64-v8a | `build-android-arm64/libwirelog.so` |
| x86_64 | `build-android-x86_64/libwirelog.so` |

Place the resulting `.so` into your APK's `app/src/main/jniLibs/<abi>/`
directory or load it via `System.loadLibrary("wirelog")` from a JNI
wrapper.

## What is NOT built on Android

The `wirelog_cli` executable is intentionally not built on Android.
There is no useful command-line entry point in the JNI / NDK app model.
The `meson.build` CLI driver section guards the `executable()` call on
`if not is_android`.

The four CLI-dependent tests in `tests/meson.build`
(`baseline_int_edges`, `baseline_sym_family`, `baseline_tab_nodes`,
`cli_version`) are similarly skipped on Android builds. All other tests
are excluded by `-Dtests=false`.

## Symbol visibility

The Android build uses the same `gnu_symbol_visibility: 'hidden'` policy
as Linux and macOS. Every public API on the 9 installed headers carries
`WIRELOG_API` (`wirelog/wirelog-export.h:25-29`), which resolves to
`__attribute__((visibility("default")))` on the NDK Clang toolchain.
Internal `wl_*`, `col_*`, `arr_*` symbols stay hidden from the dynamic
symbol table; the 53-entry ABI manifest at `abi/libwirelog-1.0.symbols`
applies unchanged to Android.

## Verification

The local build verifies that:

1. `cross/android-arm64.ini` + `-Dandroid=true` produces a `libwirelog.so`
   whose ELF header reports `EM_AARCH64` and `PT_LOAD` segments aligned
   to 0x4000 (16 KB):
   ```sh
   readelf -l build-android-arm64/libwirelog.so | grep -E '^\s+LOAD'
   # Each PT_LOAD row's Align column should read 0x4000.
   # Non-LOAD entries (PT_PHDR, PT_DYNAMIC, etc.) carry their own
   # alignment; only PT_LOAD honors -Wl,-z,max-page-size.
   ```
2. `cross/android-x86_64.ini` + `-Dandroid=true` produces a `libwirelog.so`
   whose ELF header reports `EM_X86_64`.

## Cross-references

- Issue #464 (this PR) -- meson plumbing + cross-files
- Issue #446 (CLOSED) -- Option C I/O adapter umbrella covering Android delivery
- Issue #458 (CLOSED) -- session_facts.c dispatch rewrite prerequisite
- `meson_options.txt` -- `android` option entry
- `meson.build` -- `wirelog_android_link_args` and CLI guard
- `tests/meson.build` -- `if not is_android` CLI-test cascade
