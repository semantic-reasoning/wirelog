# I/O Adapters

wirelog ships a built-in CSV adapter. For any other data source (pcap, Kafka,
eBPF, a custom binary format, ...) you can register your own **I/O adapter**
at runtime without rebuilding `libwirelog`.

Two integration paths are supported:

| Path | Mechanism | Use case |
|------|-----------|----------|
| **A** | Link your code against installed `libwirelog` | Libraries, embedded apps, Android/iOS |
| **B** | `wl --load-adapter=path.so` (opt-in `dlopen`) | CLI users who cannot re-link `wl` |

Both paths use the same adapter interface defined in
`<wirelog/io/io_adapter.h>`.

---

## 1. Adapter interface

```c
#include <wirelog/io/io_adapter.h>

typedef struct wirelog_io_adapter {
    uint32_t    abi_version;   /* must equal WIRELOG_IO_ABI_VERSION (currently 2) */
    const char *scheme;        /* e.g. "csv", "pcap"                        */
    const char *description;   /* human-readable, for diagnostics           */

    int (*read)(wirelog_io_ctx_t *ctx, int64_t **out_data, uint32_t *out_nrows,
                void *user_data);

    int (*validate)(wirelog_io_ctx_t *ctx, char *errbuf, size_t errbuf_len,
                    void *user_data);       /* optional, may be NULL */

    void *user_data;           /* passed verbatim to read()/validate()      */
} wirelog_io_adapter_t;
```

### Key fields

- **`abi_version`** -- must equal `WIRELOG_IO_ABI_VERSION`. The registry rejects
  mismatches at registration time, giving a clean error instead of a silent
  ABI break.
- **`read`** (required) -- produces a `malloc`-allocated, row-major `int64_t`
  buffer of size `*out_nrows * wirelog_io_ctx_num_cols(ctx)`. Ownership transfers
  to the caller; wirelog will `free()` it with the same libc `free()`.
- **`validate`** (optional) -- pre-flight parameter validation. Called before
  `read`. Writes at most `errbuf_len - 1` bytes into `errbuf` on error.
- **`user_data`** -- opaque pointer passed as the trailing argument to
  `read()` and `validate()`. Lets adapters carry per-instance state without
  globals (essential for Swift `@convention(c)`, JNI, and Rust `extern "C"`
  callbacks).

### Context accessors

The opaque `wirelog_io_ctx_t` provides safe, ABI-stable access to session
internals:

| Accessor | Description |
|----------|-------------|
| `wirelog_io_ctx_relation_name(ctx)` | Name of the target relation |
| `wirelog_io_ctx_num_cols(ctx)` | Physical row stride: `int64_t` slots per tuple (see below) |
| `wirelog_io_ctx_col_type(ctx, i)` | Type of physical slot `i` (`WIRELOG_TYPE_INT64`, `WIRELOG_TYPE_STRING`, ...) |
| `wirelog_io_ctx_param(ctx, key)` | `.input` parameter lookup (e.g. `"filename"`, `"delimiter"`) |
| `wirelog_io_ctx_intern_string(ctx, utf8)` | Intern a string, returns `int64_t` id for `WIRELOG_TYPE_STRING` columns |
| `wirelog_io_ctx_platform(ctx)` | Platform context pointer (e.g. `JavaVM *` on Android) |
| `wirelog_io_ctx_set_platform(ctx, ptr)` | Set platform context pointer; returns 0 on success |

Adapters never see `wl_intern_t`, `wl_ir_relation_info_t`, or any other
internal type. This is the ABI firewall -- the context struct is opaque and
its layout is private.

#### `num_cols` is physical, not declared (#985)

`wirelog_io_ctx_num_cols(ctx)` is the **physical row stride**: the number of
`int64_t` slots one tuple of the relation occupies. That is the number of
fields your `read()` must produce per row, and the width wirelog inserts the
returned buffer at. It is not the relation's declared column count.

A column declared as an `inline` compound occupies its full arity of
consecutive slots:

```
.decl inp(id: int64, p: pair/2 inline, s: symbol)
```

is three declared columns but `num_cols` 4, and its `.input` file carries
four fields. A `side` compound is a single handle slot, and so is every
scalar, so the two numbers agree for every relation that declares no inline
compound column.

`wirelog_io_ctx_col_type(ctx, i)` is indexed by the same physical position:
`i` runs over slots, not over declared columns. Each slot of an inline
compound reports `WIRELOG_TYPE_INT64`, because the slots carry raw `int64`
payload and the declared type of the compound column does not describe them.
For the relation above the types are `INT64, INT64, INT64, STRING`.

Before #985 both accessors reported the *declared* column count and the
declared per-column types, which named a stride the storage does not use. The
signatures and `WIRELOG_IO_ABI_VERSION` are unchanged; what changed is the
value for a relation with an inline compound column. An adapter that already
sizes its output from `num_cols` -- which this contract has always required
-- needs no change. One that reconstructs the stride from a schema of its own
will now disagree with wirelog for those relations.

---

## 2. Registration API

```c
int  wirelog_io_register_adapter(const wirelog_io_adapter_t *adapter);
int  wirelog_io_unregister_adapter(const char *scheme);
const wirelog_io_adapter_t *wirelog_io_find_adapter(const char *scheme);
const char *wirelog_io_last_error(void);
```

- All three mutation/query functions are **thread-safe** (process-global mutex).
- The thread-safety guarantee applies **within a single process image**. After
  `fork()` without `exec*()`, the registry is unsafe to call from the child
  unless the host has installed a `pthread_atfork()` reset handler; see
  [docs/THREADING.md §10](THREADING.md#10-fork-safety) for the supported
  child-process patterns and the uWSGI/mod_wsgi/gunicorn guidance.
- The adapter pointer must remain valid until `wirelog_io_unregister_adapter()` or
  process exit. Typical usage: pass `&static_const_struct`.
- The `scheme` string is copied into a fixed-size internal buffer at
  registration time; the caller's pointer may be freed afterwards.
- On failure, functions return `-1` and record a human-readable reason
  retrievable via `wirelog_io_last_error()` (thread-local).
- The registry holds up to `WIRELOG_IO_MAX_ADAPTERS` (32) entries.

---

## 3. Path A -- Library embedding

The user compiles only their own adapter code and links against the installed
`libwirelog`. **wirelog is not rebuilt.**

### Build

```bash
cc -o pcap_skeleton main.c $(pkg-config --cflags --libs wirelog)
```

### Complete example

The following is the Path A pcap skeleton from
[`examples/path_a_pcap_skeleton/main.c`](../examples/path_a_pcap_skeleton/main.c):

```c
/*
 * Path A pcap skeleton — standalone I/O adapter example
 *
 * Demonstrates how a user registers a custom I/O adapter with libwirelog
 * WITHOUT rebuilding the library.  The user compiles only this file and
 * links against the installed libwirelog.
 *
 * Build (after `meson install`):
 *   cc -o pcap_skeleton main.c $(pkg-config --cflags --libs wirelog)
 *
 * This is a compile-check skeleton; the read callback returns an empty
 * result set.  A real adapter would parse pcap data here.
 *
 * See also: Issue #446 (Option C design), Issue #462 (CI compile gate).
 */

#include <wirelog/io/io_adapter.h>

#include <stdio.h>
#include <stdlib.h>

/* ---------- adapter callbacks ---------- */

static int
pcap_validate(wirelog_io_ctx_t *ctx, char *errbuf, size_t errbuf_len,
    void *user_data)
{
    (void)user_data;

    const char *filename = wirelog_io_ctx_param(ctx, "filename");
    if (!filename) {
        snprintf(errbuf, errbuf_len, "missing required param 'filename'");
        return -1;
    }
    return 0;
}

static int
pcap_read(wirelog_io_ctx_t *ctx, int64_t **out_data, uint32_t *out_nrows,
    void *user_data)
{
    (void)ctx;
    (void)user_data;

    /*
     * Skeleton: return an empty result set.
     * A real adapter would open the pcap file, parse packets,
     * and fill a row-major int64_t buffer here.
     */
    *out_data = NULL;
    *out_nrows = 0;
    return 0;
}

/* ---------- adapter definition ---------- */

static const wirelog_io_adapter_t pcap_adapter = {
    .abi_version = WIRELOG_IO_ABI_VERSION,
    .scheme = "pcap",
    .description = "libpcap file reader (skeleton)",
    .read = pcap_read,
    .validate = pcap_validate,
    .user_data = NULL,
};

/* ---------- main ---------- */

int
main(void)
{
    if (wirelog_io_register_adapter(&pcap_adapter) != 0) {
        fprintf(stderr, "register failed: %s\n", wirelog_io_last_error());
        return 1;
    }

    const wirelog_io_adapter_t *found = wirelog_io_find_adapter("pcap");
    if (!found) {
        fprintf(stderr, "adapter lookup failed\n");
        return 1;
    }

    printf("registered adapter: scheme=%s desc=\"%s\"\n",
        found->scheme, found->description);

    wirelog_io_unregister_adapter("pcap");
    return 0;
}
```

### `.input` directive usage

Once an adapter is registered, a Datalog program can use it via the `io=`
parameter in `.input` directives:

```
.input packet(io="pcap", filename="capture.pcap")
```

If `io=` is omitted, the built-in `csv` adapter is used (backward compatible).

---

## 4. Path B -- CLI plugin via `dlopen` (opt-in)

> **Status**: Planned (#461). Not yet implemented.

The `wl` CLI is a pre-built binary that users cannot re-link. To use a custom
adapter with `wl run program.dl`, the CLI supports optional dynamic loading:

```bash
wl --load-adapter=/path/to/libwirelog-pcap.so run program.dl
```

### Compile-time gate

Path B is controlled by a meson option:

```bash
meson setup builddir -Dio_plugin_dlopen=true   # default: false
```

When disabled, `wl --load-adapter` prints an error and the `dlopen` code path
is not compiled in (no `-ldl` dependency). Path B is excluded from Android and
iOS builds.

### Plugin entry point

A user shared library must export exactly one symbol:

```c
WIRELOG_IO_PLUGIN_EXPORT
const wirelog_io_adapter_t *const *wirelog_io_plugin_entry(uint32_t *n_out, uint32_t abi_ver);
```

- `abi_ver` is the host's `WIRELOG_IO_ABI_VERSION`. The plugin should check it
  against its own compiled version and return `NULL` on mismatch.
- `*n_out` receives the number of adapters returned.
- The returned array of pointers must remain valid for the process lifetime.

The CLI calls `dlopen` + `dlsym("wirelog_io_plugin_entry")`, validates the ABI
version, and bulk-registers all returned adapters.

> **Note**: The symbols above (`WIRELOG_IO_PLUGIN_EXPORT`, `wirelog_io_plugin_entry`)
> are the proposed contract and are not yet defined in the header. They will
> be introduced in #461.

---

## 5. Ownership rules

The ownership contract is simple:

| Resource | Owner | Rule |
|----------|-------|------|
| `wirelog_io_adapter_t` struct | User | Must outlive registration (use `static const`) |
| `scheme` string | Copied | Caller may free after `wirelog_io_register_adapter()` returns |
| `int64_t *out_data` buffer | Transfers | Adapter `malloc`s, wirelog `free`s (same libc) |
| `wirelog_io_ctx_t` | wirelog | Valid only during `read()`/`validate()` call |
| Borrowed pointers from accessors | wirelog | Valid only during the enclosing callback |

---

## 6. ABI versioning policy

- The initial I/O adapter ABI version was `1` (the v0.30.0 interface). The
  current `WIRELOG_IO_ABI_VERSION` is `2`; it was introduced when the public
  adapter symbols were renamed for v0.40+.
- The registry rejects adapters with a mismatched version at registration time.
- A version bump is required when:
  - A field is added to or removed from `wirelog_io_adapter_t`.
  - The signature of `read()` or `validate()` changes.
  - The semantics of an accessor function change incompatibly.
- A version bump is **not** required for:
  - Adding new accessor functions to `wirelog_io_ctx_t`.
  - Adding new fields to the *end* of `wirelog_io_ctx_t` internals (opaque).

Since `libwirelog.so` is the only side that dereferences the adapter struct,
a version bump provides a clean break point for migration.

---

## 7. Thread safety

- **Registration**: `wirelog_io_register_adapter()`, `wirelog_io_unregister_adapter()`,
  and `wirelog_io_find_adapter()` all acquire a process-global mutex.
- **Built-in bootstrap**: The built-in `csv` adapter is auto-registered on the
  first call to `wirelog_io_find_adapter()` via a one-shot flag (`pthread_once`
  equivalent). User registration calls `ensure_builtins()` first, preventing
  races.
- **Adapter callbacks**: `read()` and `validate()` are called from the
  evaluation thread. The adapter itself is responsible for any internal
  synchronization (e.g., if it accesses shared state).
- **Error reporting**: `wirelog_io_last_error()` uses thread-local storage
  (`__thread` / `__declspec(thread)`), so error messages do not race across
  threads.

TSan CI gate (Issue #459) validates these invariants on every PR.
