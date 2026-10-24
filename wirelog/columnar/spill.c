/* Internal, process-local immutable spill storage (Issue #1372). */
#define _POSIX_C_SOURCE 200809L
#include "columnar/spill.h"
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#endif

#define WL_COLUMNAR_SPILL_HEADER 32u
#define WL_COLUMNAR_SPILL_PATH_EXTRA 64u
#define WL_COLUMNAR_SPILL_IO_CHUNK 4096u
#define WL_COLUMNAR_SPILL_MAX_INTERRUPTS 16u

static const unsigned char wl_columnar_spill_magic[8] = {
    'W', 'L', 'S', 'P', 'I', 'L', 'L', '1'
};

typedef struct {
    wl_columnar_memory_reservation_t reservation;
} wl_columnar_spill_allocation_t;

struct wl_columnar_spill_store {
    wl_columnar_spill_allocation_t allocation;
    wl_columnar_memory_governor_ref_t *governor;
    uint64_t quota, used, max_row_bytes, segments;
    size_t directory_bytes;
    bool retired;
    char directory[];
};

struct wl_columnar_spill_segment {
    wl_columnar_spill_allocation_t allocation;
    wl_columnar_spill_store_t *store;
    wl_intern_t *intern;
    uint64_t nrows, row_bytes, data_offset, file_bytes, readers;
    uint32_t ncols;
    wl_mutex_t io_lock;
#ifdef _WIN32
    HANDLE file;
#else
    int file;
#endif
    wirelog_column_type_t types[];
};

struct wl_columnar_spill_reader {
    wl_columnar_spill_allocation_t allocation;
    wl_columnar_spill_segment_t *segment;
    uint64_t buffer[];
};

#ifdef WL_TEST_SPILL
/* Test executable supplies deterministic I/O/allocation faults. Not exported
 * or present in the production library. Hook state is frozen during readers. */
extern int wl_columnar_spill_test_io(bool writing, uint64_t offset,
    size_t *length);
extern bool wl_columnar_spill_test_allocation(void);
#endif

static wl_columnar_spill_status_t
wl_columnar_spill_allocate(wl_columnar_spill_store_t *store, uint64_t bytes,
    void **out)
{
    wl_columnar_memory_reservation_t pending;
    *out = NULL;
    if (bytes > SIZE_MAX)
        return WL_COLUMNAR_SPILL_OVERFLOW;
    wl_columnar_memory_reservation_init(&pending);
    wl_columnar_memory_admission_status_t status =
        wl_columnar_memory_reserve_checked(
        wl_columnar_memory_governor_ref_get(store->governor), bytes, &pending);
    if (status > WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
            ? WL_COLUMNAR_SPILL_MEMORY : WL_COLUMNAR_SPILL_OVERFLOW;
    wl_columnar_spill_allocation_t *p = NULL;
    bool fail_allocation = false;
#ifdef WL_TEST_SPILL
    fail_allocation = wl_columnar_spill_test_allocation();
#endif
    if (!fail_allocation)
        p = calloc(1, (size_t)bytes);
    if (!p) {
        if (!wl_columnar_memory_rollback(&pending)) abort();
        return WL_COLUMNAR_SPILL_ALLOCATION;
    }
    wl_columnar_memory_reservation_init(&p->reservation);
    if (!wl_columnar_memory_reservation_move(&p->reservation, &pending)
        || !wl_columnar_memory_commit(&p->reservation, p)) abort();
    *out = p;
    return WL_COLUMNAR_SPILL_OK;
}

static void
wl_columnar_spill_free(void *ptr)
{
    wl_columnar_spill_allocation_t *p = ptr;
    if (!p) return;
    /* The token must outlive its storage, so move it to a private stack slot
     * before freeing and credit only after the physical allocation is gone. */
    wl_columnar_memory_reservation_t token;
    wl_columnar_memory_reservation_init(&token);
    if (!wl_columnar_memory_reservation_move(&token, &p->reservation)) abort();
    free(p);
    if (!wl_columnar_memory_release(&token)) abort();
}

static void
wl_columnar_spill_put(unsigned char *p, uint64_t value, unsigned bytes)
{
    for (unsigned i = 0; i < bytes;
        i++) p[i] = (unsigned char)(value >> (8 * i));
}

static uint64_t
wl_columnar_spill_get(const unsigned char *p, unsigned bytes)
{
    uint64_t value = 0;
    for (unsigned i = 0; i < bytes; i++) value |= (uint64_t)p[i] << (8 * i);
    return value;
}

wl_columnar_spill_status_t
wl_columnar_spill_floor(const char *directory, uint32_t ncols,
    wl_columnar_spill_floor_t *out)
{
    if (!directory || !directory[0] || !out || ncols == 0)
        return WL_COLUMNAR_SPILL_INVALID;
    size_t length = strlen(directory);
    if (length > 4096) return WL_COLUMNAR_SPILL_INVALID;
    uint64_t row = (uint64_t)ncols * 8 + 24;
    out->store_bytes = sizeof(wl_columnar_spill_store_t) + length + 1;
    out->segment_bytes = sizeof(wl_columnar_spill_segment_t)
        + (uint64_t)ncols * sizeof(wirelog_column_type_t);
    out->writer_bytes = sizeof(wl_columnar_spill_allocation_t) + row
        + length + WL_COLUMNAR_SPILL_PATH_EXTRA;
    out->reader_bytes = sizeof(wl_columnar_spill_reader_t) + row;
    if (out->segment_bytes > SIZE_MAX || out->writer_bytes > SIZE_MAX
        || out->reader_bytes > SIZE_MAX) return WL_COLUMNAR_SPILL_OVERFLOW;
    return WL_COLUMNAR_SPILL_OK;
}

wl_columnar_spill_status_t
wl_columnar_spill_store_open(const char *directory,
    wl_columnar_memory_governor_ref_t *governor, uint64_t quota,
    uint64_t max_row_bytes, wl_columnar_spill_store_t **out)
{
    if (!out) return WL_COLUMNAR_SPILL_INVALID;
    *out = NULL;
    wl_columnar_spill_floor_t floor;
    wl_columnar_spill_status_t rc = wl_columnar_spill_floor(directory, 1,
            &floor);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    if (!governor || !quota || max_row_bytes < 32)
        return WL_COLUMNAR_SPILL_INVALID;
#ifdef _WIN32
    DWORD attributes = GetFileAttributesA(directory);
    if (attributes == INVALID_FILE_ATTRIBUTES
        || !(attributes &
        FILE_ATTRIBUTE_DIRECTORY)) return WL_COLUMNAR_SPILL_IO;
#else
    struct stat st;
    if (stat(directory, &st) != 0 || !S_ISDIR(st.st_mode))
        return WL_COLUMNAR_SPILL_IO;
#endif
    wl_columnar_spill_store_t prototype = { .governor = governor };
    void *allocation;
    rc = wl_columnar_spill_allocate(&prototype, floor.store_bytes, &allocation);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    wl_columnar_spill_store_t *store = allocation;
    store->governor = governor;
    wl_columnar_memory_governor_ref_retain(governor);
    store->quota = quota;
    store->max_row_bytes = max_row_bytes;
    store->directory_bytes = strlen(directory) + 1;
    memcpy(store->directory, directory, store->directory_bytes);
    *out = store;
    return WL_COLUMNAR_SPILL_OK;
}

wl_columnar_spill_status_t
wl_columnar_spill_store_close(wl_columnar_spill_store_t *store)
{
    if (!store) return WL_COLUMNAR_SPILL_INVALID;
    if (store->retired) return WL_COLUMNAR_SPILL_CLOSED;
    if (store->segments) return WL_COLUMNAR_SPILL_BUSY;
    wl_columnar_memory_governor_ref_t *governor = store->governor;
    wl_columnar_spill_free(store);
    wl_columnar_memory_governor_ref_release(governor);
    return WL_COLUMNAR_SPILL_OK;
}

void
wl_columnar_spill_store_retire(wl_columnar_spill_store_t *store)
{
    if (!store) return;
    if (!store->segments) {
        (void)wl_columnar_spill_store_close(store);
        return;
    }
    store->retired = true;
}

bool
wl_columnar_spill_store_uses_governor(const wl_columnar_spill_store_t *store,
    const wl_columnar_memory_governor_ref_t *governor)
{
    return store && governor && store->governor == governor;
}

uint64_t
wl_columnar_spill_store_bytes(const wl_columnar_spill_store_t *store)
{
    return store ? store->used : 0;
}

static void
wl_columnar_spill_close_file(wl_columnar_spill_segment_t *s)
{
#ifdef _WIN32
    CloseHandle(s->file);
#else
    /* Do not retry close after EINTR: the descriptor may already be closed. */
    (void)close(s->file);
#endif
}

static wl_columnar_spill_status_t
wl_columnar_spill_file_error(void)
{
#ifdef _WIN32
    DWORD e = GetLastError();
    return e == ERROR_DISK_FULL || e == ERROR_HANDLE_DISK_FULL
        ? WL_COLUMNAR_SPILL_CAPACITY : WL_COLUMNAR_SPILL_IO;
#else
    return errno == ENOSPC || errno == EDQUOT
        ? WL_COLUMNAR_SPILL_CAPACITY : WL_COLUMNAR_SPILL_IO;
#endif
}

static wl_columnar_spill_status_t
wl_columnar_spill_create_file(wl_columnar_spill_segment_t *s, char *path,
    size_t capacity)
{
#ifdef _WIN32
    static LONG sequence;
    for (unsigned attempt = 0; attempt < 16; attempt++) {
        snprintf(path, capacity, "%s/wirelog-spill-%lu-%lu.tmp",
            s->store->directory,
            (unsigned long)GetCurrentProcessId(),
            (unsigned long)InterlockedIncrement(&sequence));
        s->file = CreateFileA(path, GENERIC_READ | GENERIC_WRITE | DELETE, 0,
                NULL, CREATE_NEW,
                FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE,
                NULL);
        if (s->file != INVALID_HANDLE_VALUE) return WL_COLUMNAR_SPILL_OK;
        if (GetLastError() !=
            ERROR_FILE_EXISTS) return wl_columnar_spill_file_error();
    }
    return WL_COLUMNAR_SPILL_IO;
#else
    snprintf(path, capacity, "%s/wirelog-spill-XXXXXX", s->store->directory);
    s->file = mkstemp(path);
    if (s->file < 0) return wl_columnar_spill_file_error();
    if (unlink(path) != 0) {
        wl_columnar_spill_close_file(s);
        /* Only the exact exclusively created artifact is eligible for cleanup. */
        (void)unlink(path);
        return WL_COLUMNAR_SPILL_IO;
    }
    if (fcntl(s->file, F_SETFD, FD_CLOEXEC) == -1) {
        wl_columnar_spill_close_file(s);
        return WL_COLUMNAR_SPILL_IO;
    }
    return WL_COLUMNAR_SPILL_OK;
#endif
}

static wl_columnar_spill_status_t
wl_columnar_spill_capacity(wl_columnar_spill_segment_t *s, uint64_t bytes)
{
#ifdef _WIN32
    ULARGE_INTEGER available;
    if (!GetDiskFreeSpaceExA(s->store->directory, &available, NULL, NULL))
        return WL_COLUMNAR_SPILL_IO;
    return available.QuadPart < bytes ? WL_COLUMNAR_SPILL_CAPACITY
        : WL_COLUMNAR_SPILL_OK;
#else
    struct statvfs st;
    if (fstatvfs(s->file, &st) != 0) return WL_COLUMNAR_SPILL_IO;
    if (st.f_frsize && st.f_bavail < (bytes / st.f_frsize
        + (bytes % st.f_frsize != 0))) return WL_COLUMNAR_SPILL_CAPACITY;
    return WL_COLUMNAR_SPILL_OK;
#endif
}

/* Caller owns the private writer or holds io_lock for a complete read. Both
 * file offset and scratch are protected; cancellation is checked per chunk. */
static wl_columnar_spill_status_t
wl_columnar_spill_io(wl_columnar_spill_segment_t *s, bool writing,
    uint64_t offset,
    unsigned char *buffer, size_t length, wl_columnar_spill_cancel_fn cancel,
    void *context)
{
    unsigned interrupted = 0;
    while (length) {
        if (cancel && cancel(context)) return WL_COLUMNAR_SPILL_CANCELLED;
        size_t chunk = length <
            WL_COLUMNAR_SPILL_IO_CHUNK ? length : WL_COLUMNAR_SPILL_IO_CHUNK;
        if (writing) {
            wl_columnar_spill_status_t rc = wl_columnar_spill_capacity(s,
                    chunk);
            if (rc != WL_COLUMNAR_SPILL_OK) return rc;
        }
#ifdef WL_TEST_SPILL
        int error = wl_columnar_spill_test_io(writing, offset, &chunk);
        if (error) {
            if (error == EINTR &&
                ++interrupted < WL_COLUMNAR_SPILL_MAX_INTERRUPTS) continue;
            return error ==
                   ENOSPC ? WL_COLUMNAR_SPILL_CAPACITY : WL_COLUMNAR_SPILL_IO;
        }
#endif
        if (!chunk || chunk > length) return WL_COLUMNAR_SPILL_IO;
#ifdef _WIN32
        LARGE_INTEGER position;
        position.QuadPart = (LONGLONG)offset;
        DWORD count = 0;
        if (!SetFilePointerEx(s->file, position, NULL, FILE_BEGIN)
            || !(writing ? WriteFile(s->file, buffer, (DWORD)chunk, &count,
            NULL)
                         : ReadFile(s->file, buffer, (DWORD)chunk, &count,
            NULL)))
            return wl_columnar_spill_file_error();
#else
        if ((uint64_t)(off_t)offset !=
            offset) return WL_COLUMNAR_SPILL_OVERFLOW;
        ssize_t count = writing ? pwrite(s->file, buffer, chunk, (off_t)offset)
                                : pread(s->file, buffer, chunk, (off_t)offset);
        if (count < 0) {
            if (errno == EINTR &&
                ++interrupted < WL_COLUMNAR_SPILL_MAX_INTERRUPTS) continue;
            return wl_columnar_spill_file_error();
        }
#endif
        if (count == 0) return WL_COLUMNAR_SPILL_IO;
        buffer += count;
        length -= (size_t)count;
        offset += (uint64_t)count;
    }
    return WL_COLUMNAR_SPILL_OK;
}

static wl_columnar_spill_status_t
wl_columnar_spill_validate_file(wl_columnar_spill_segment_t *s,
    unsigned char *buffer, wl_columnar_spill_cancel_fn cancel, void *context)
{
#ifdef _WIN32
    LARGE_INTEGER length;
    if (!GetFileSizeEx(s->file, &length)
        || (uint64_t)length.QuadPart != s->file_bytes)
        return WL_COLUMNAR_SPILL_IO;
#else
    struct stat st;
    if (fstat(s->file, &st) != 0 || st.st_size < 0
        || (uint64_t)st.st_size != s->file_bytes)
        return WL_COLUMNAR_SPILL_IO;
#endif
    wl_columnar_spill_status_t rc = wl_columnar_spill_io(s, false, 0,
            buffer, WL_COLUMNAR_SPILL_HEADER, cancel, context);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    if (memcmp(buffer, wl_columnar_spill_magic,
        sizeof(wl_columnar_spill_magic)) != 0
        || wl_columnar_spill_get(buffer + 8, 4) != 1
        || wl_columnar_spill_get(buffer + 12, 4) != s->ncols
        || wl_columnar_spill_get(buffer + 16, 8) != s->nrows
        || wl_columnar_spill_get(buffer + 24, 8) != s->row_bytes)
        return WL_COLUMNAR_SPILL_IO;
    rc = wl_columnar_spill_io(s, false, WL_COLUMNAR_SPILL_HEADER,
            buffer, (size_t)s->ncols * 4, cancel, context);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    for (uint32_t c = 0; c < s->ncols; c++)
        if (wl_columnar_spill_get(buffer + (size_t)c * 4, 4)
            != (uint32_t)s->types[c]) return WL_COLUMNAR_SPILL_IO;
    return WL_COLUMNAR_SPILL_OK;
}

#ifdef WL_TEST_SPILL
/* Called only with no concurrent I/O by the direct-source test target. */
int
wl_columnar_spill_test_damage(wl_columnar_spill_segment_t *s, bool truncate)
{
#ifdef _WIN32
    LARGE_INTEGER position;
    position.QuadPart = truncate ? 1 : 0;
    if (!SetFilePointerEx(s->file, position, NULL, FILE_BEGIN)) return -1;
    DWORD count;
    return truncate ? (SetEndOfFile(s->file) ? 0 : -1)
        : (WriteFile(s->file, "X", 1, &count, NULL) && count == 1 ? 0 : -1);
#else
    return truncate ? ftruncate(s->file, 1)
        : (pwrite(s->file, "X", 1, 0) == 1 ? 0 : -1);
#endif
}
#endif

wl_columnar_spill_status_t
wl_columnar_spill_create(wl_columnar_spill_store_t *store,
    const wirelog_column_type_t *types, uint32_t ncols, uint64_t nrows,
    wl_columnar_spill_source_fn source, void *source_context,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_spill_segment_t **out)
{
    return wl_columnar_spill_create_with_intern(store, types, ncols, nrows,
               source, source_context, NULL, cancel, cancel_context, out);
}

wl_columnar_spill_status_t
wl_columnar_spill_create_with_intern(wl_columnar_spill_store_t *store,
    const wirelog_column_type_t *types, uint32_t ncols, uint64_t nrows,
    wl_columnar_spill_source_fn source, void *source_context,
    wl_intern_t *intern,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    wl_columnar_spill_segment_t **out)
{
    if (!out) return WL_COLUMNAR_SPILL_INVALID;
    *out = NULL;
    if (!store || !types || !ncols || (nrows && !source))
        return WL_COLUMNAR_SPILL_INVALID;
    if (store->retired) return WL_COLUMNAR_SPILL_CLOSED;
    bool strings = false;
    for (uint32_t i = 0; i < ncols; i++) {
        if (types[i] == WIRELOG_TYPE_STRING && intern) {
            strings = true; continue;
        }
        if (types[i] != WIRELOG_TYPE_INT32 && types[i] != WIRELOG_TYPE_INT64
            && types[i] != WIRELOG_TYPE_UINT32 &&
            types[i] != WIRELOG_TYPE_UINT64
            && types[i] != WIRELOG_TYPE_FLOAT && types[i] != WIRELOG_TYPE_BOOL)
            return WL_COLUMNAR_SPILL_UNSUPPORTED;
    }
    uint64_t row_bytes = (uint64_t)ncols * 8 + 24;
    uint64_t data_offset = WL_COLUMNAR_SPILL_HEADER + (uint64_t)ncols * 4;
    if (nrows > (INT64_MAX - data_offset) / row_bytes)
        return WL_COLUMNAR_SPILL_OVERFLOW;
    uint64_t bytes = data_offset + nrows * row_bytes;
    if (row_bytes > store->max_row_bytes) return WL_COLUMNAR_SPILL_MEMORY;
    if (bytes > store->quota - store->used) return WL_COLUMNAR_SPILL_QUOTA;
    if (cancel && cancel(cancel_context)) return WL_COLUMNAR_SPILL_CANCELLED;
    wl_columnar_spill_floor_t floor;
    wl_columnar_spill_status_t rc = wl_columnar_spill_floor(store->directory,
            ncols, &floor);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    if (strings) {
        int pin = wl_intern_pin_acquire(intern, store->governor);
        if (pin) return pin == EPIPE ? WL_COLUMNAR_SPILL_CLOSED
            : pin == EOVERFLOW ? WL_COLUMNAR_SPILL_OVERFLOW
            : WL_COLUMNAR_SPILL_INVALID;
    }
    void *allocation = NULL;
    rc = wl_columnar_spill_allocate(store, floor.segment_bytes, &allocation);
    if (rc != WL_COLUMNAR_SPILL_OK) {
        if (strings) wl_intern_pin_release(intern);
        return rc;
    }
    wl_columnar_spill_segment_t *s = allocation;
    s->store = store;
    s->intern = strings ? intern : NULL;
    s->nrows = nrows;
    s->ncols = ncols;
    s->row_bytes = row_bytes;
    s->data_offset = data_offset;
    s->file_bytes = bytes;
    memcpy(s->types, types, (size_t)ncols * sizeof(*types));
    if (wl_mutex_init(&s->io_lock) != 0) {
        wl_columnar_spill_free(s);
        if (strings) wl_intern_pin_release(intern);
        return WL_COLUMNAR_SPILL_ALLOCATION;
    }
    void *scratch = NULL;
    bool opened = false;
    rc = wl_columnar_spill_allocate(store, floor.writer_bytes, &scratch);
    if (rc != WL_COLUMNAR_SPILL_OK) goto fail;
    unsigned char *buffer = (unsigned char *)scratch +
        sizeof(wl_columnar_spill_allocation_t);
    char *path = (char *)buffer + row_bytes;
    store->used += bytes;
    rc = wl_columnar_spill_create_file(s, path,
            store->directory_bytes + WL_COLUMNAR_SPILL_PATH_EXTRA - 1);
    if (rc != WL_COLUMNAR_SPILL_OK) goto reserved_fail;
    opened = true;
    rc = wl_columnar_spill_capacity(s, bytes);
    if (rc != WL_COLUMNAR_SPILL_OK) goto reserved_fail;
    memcpy(buffer, wl_columnar_spill_magic, sizeof(wl_columnar_spill_magic));
    wl_columnar_spill_put(buffer + 8, 1, 4);
    wl_columnar_spill_put(buffer + 12, ncols, 4);
    wl_columnar_spill_put(buffer + 16, nrows, 8);
    wl_columnar_spill_put(buffer + 24, row_bytes, 8);
    rc = wl_columnar_spill_io(s, true, 0, buffer, WL_COLUMNAR_SPILL_HEADER,
            cancel, cancel_context);
    if (rc != WL_COLUMNAR_SPILL_OK) goto reserved_fail;
    for (uint32_t i = 0; i < ncols;
        i++) wl_columnar_spill_put(buffer + (size_t)i * 4, types[i], 4);
    rc = wl_columnar_spill_io(s, true, WL_COLUMNAR_SPILL_HEADER, buffer,
            (size_t)ncols * 4,
            cancel, cancel_context);
    if (rc != WL_COLUMNAR_SPILL_OK) goto reserved_fail;
    for (uint64_t i = 0; i < nrows; i++) {
        if (cancel && cancel(cancel_context)) {
            rc = WL_COLUMNAR_SPILL_CANCELLED; goto reserved_fail;
        }
        wl_columnar_spill_metadata_t metadata = { 0 };
        rc = source(source_context, i, (uint64_t *)buffer, &metadata);
        if (rc != WL_COLUMNAR_SPILL_OK) goto reserved_fail;
        for (uint32_t c = 0; c < ncols; c++) {
            uint64_t lane;
            memcpy(&lane, buffer + (size_t)c * 8, 8);
            if (types[c] == WIRELOG_TYPE_STRING &&
                (lane > INT64_MAX || !wl_intern_reverse(intern,
                (int64_t)lane))) {
                rc = WL_COLUMNAR_SPILL_INVALID; goto reserved_fail;
            }
            wl_columnar_spill_put(buffer + (size_t)c * 8, lane, 8);
        }
        unsigned char *tail = buffer + (size_t)ncols * 8;
        wl_columnar_spill_put(tail, metadata.iteration, 4);
        wl_columnar_spill_put(tail + 4, metadata.stratum, 4);
        wl_columnar_spill_put(tail + 8, metadata.worker, 4);
        wl_columnar_spill_put(tail + 12, 0, 4);
        uint64_t difference;
        memcpy(&difference, &metadata.multiplicity, 8);
        wl_columnar_spill_put(tail + 16, difference, 8);
        rc = wl_columnar_spill_io(s, true, data_offset + i * row_bytes, buffer,
                (size_t)row_bytes,
                cancel, cancel_context);
        if (rc != WL_COLUMNAR_SPILL_OK) goto reserved_fail;
    }
    if (cancel && cancel(cancel_context)) {
        rc = WL_COLUMNAR_SPILL_CANCELLED; goto reserved_fail;
    }
    wl_columnar_spill_free(scratch);
    store->segments++;
    *out = s;
    return WL_COLUMNAR_SPILL_OK;
reserved_fail:
    store->used -= bytes;
    if (opened) wl_columnar_spill_close_file(s);
fail:
    wl_columnar_spill_free(scratch);
    wl_mutex_destroy(&s->io_lock);
    wl_columnar_spill_free(s);
    if (strings) wl_intern_pin_release(intern);
    return rc;
}

wl_columnar_spill_status_t
wl_columnar_spill_release(wl_columnar_spill_segment_t *s)
{
    if (!s) return WL_COLUMNAR_SPILL_INVALID;
    if (s->readers) return WL_COLUMNAR_SPILL_BUSY;
    wl_columnar_spill_close_file(s);
    wl_columnar_spill_store_t *store = s->store;
    wl_intern_t *intern = s->intern;
    store->used -= s->file_bytes;
    store->segments--;
    wl_mutex_destroy(&s->io_lock);
    wl_columnar_spill_free(s);
    if (intern) wl_intern_pin_release(intern);
    if (store->retired && !store->segments) {
        wl_columnar_memory_governor_ref_t *governor = store->governor;
        wl_columnar_spill_free(store);
        wl_columnar_memory_governor_ref_release(governor);
    }
    return WL_COLUMNAR_SPILL_OK;
}

wl_columnar_spill_status_t
wl_columnar_spill_acquire(wl_columnar_spill_segment_t *s,
    wl_columnar_spill_reader_t **out)
{
    if (!out) return WL_COLUMNAR_SPILL_INVALID;
    *out = NULL;
    if (!s || s->readers == UINT64_MAX) return WL_COLUMNAR_SPILL_INVALID;
    if (s->store->retired) return WL_COLUMNAR_SPILL_CLOSED;
    void *allocation;
    wl_columnar_spill_status_t rc = wl_columnar_spill_allocate(s->store,
            sizeof(wl_columnar_spill_reader_t) + s->row_bytes, &allocation);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    wl_columnar_spill_reader_t *reader = allocation;
    reader->segment = s;
    s->readers++;
    *out = reader;
    return WL_COLUMNAR_SPILL_OK;
}

void
wl_columnar_spill_reader_release(wl_columnar_spill_reader_t *reader)
{
    if (!reader) return;
    reader->segment->readers--;
    wl_columnar_spill_free(reader);
}

wl_columnar_spill_status_t
wl_columnar_spill_read(wl_columnar_spill_reader_t *reader, uint64_t ordinal,
    wl_columnar_spill_cancel_fn cancel, void *cancel_context,
    const uint64_t **lanes, wl_columnar_spill_metadata_t *metadata)
{
    if (lanes) *lanes = NULL;
    if (metadata) memset(metadata, 0, sizeof(*metadata));
    if (!reader || !lanes || !metadata) return WL_COLUMNAR_SPILL_INVALID;
    wl_columnar_spill_segment_t *s = reader->segment;
    if (ordinal >= s->nrows) return WL_COLUMNAR_SPILL_INVALID;
    unsigned char *buffer = (unsigned char *)reader->buffer;
    wl_mutex_lock(&s->io_lock);
    wl_columnar_spill_status_t rc = wl_columnar_spill_validate_file(s, buffer,
            cancel, cancel_context);
    if (rc == WL_COLUMNAR_SPILL_OK) rc = wl_columnar_spill_io(s, false,
                s->data_offset + ordinal * s->row_bytes, buffer,
                (size_t)s->row_bytes,
                cancel, cancel_context);
    wl_mutex_unlock(&s->io_lock);
    if (rc != WL_COLUMNAR_SPILL_OK) return rc;
    unsigned char *tail = buffer + (size_t)s->ncols * 8;
    if (wl_columnar_spill_get(tail + 12, 4) != 0) return WL_COLUMNAR_SPILL_IO;
    wl_columnar_spill_metadata_t decoded = {
        (uint32_t)wl_columnar_spill_get(tail, 4),
        (uint32_t)wl_columnar_spill_get(tail + 4, 4),
        (uint32_t)wl_columnar_spill_get(tail + 8, 4), 0
    };
    uint64_t difference = wl_columnar_spill_get(tail + 16, 8);
    memcpy(&decoded.multiplicity, &difference, 8);
    for (uint32_t c = 0; c < s->ncols; c++) {
        uint64_t lane = wl_columnar_spill_get(buffer + (size_t)c * 8, 8);
        if (s->types[c] == WIRELOG_TYPE_STRING &&
            (lane > INT64_MAX || !wl_intern_reverse(s->intern, (int64_t)lane)))
            return WL_COLUMNAR_SPILL_IO;
        memcpy(buffer + (size_t)c * 8, &lane, 8);
    }
    *metadata = decoded;
    *lanes = reader->buffer;
    return WL_COLUMNAR_SPILL_OK;
}

const wirelog_column_type_t *
wl_columnar_spill_schema(const wl_columnar_spill_segment_t *s,
    uint32_t *ncols, uint64_t *nrows)
{
    if (!s || !ncols || !nrows) return NULL;
    *ncols = s->ncols;
    *nrows = s->nrows;
    return s->types;
}

wl_columnar_spill_status_t
wl_columnar_spill_resolve_string(wl_columnar_spill_reader_t *reader,
    uint64_t id, const char **out)
{
    if (!out) return WL_COLUMNAR_SPILL_INVALID;
    *out = NULL;
    if (!reader || !reader->segment->intern || id > INT64_MAX)
        return WL_COLUMNAR_SPILL_INVALID;
    *out = wl_intern_reverse(reader->segment->intern, (int64_t)id);
    return *out ? WL_COLUMNAR_SPILL_OK : WL_COLUMNAR_SPILL_INVALID;
}
