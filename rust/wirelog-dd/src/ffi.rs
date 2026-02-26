/*
 * ffi.rs - C FFI entry points for wirelog DD executor
 *
 * These functions are exported with C linkage and called by the wirelog
 * C runtime.  They implement the contract defined in wirelog/ffi/dd_ffi.h.
 */

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};

use crate::dataflow::execute_plan;
use crate::ffi_types::WlFfiPlan;
use crate::plan_reader::read_plan;

/* ======================================================================== */
/* Callback type for result tuple delivery                                  */
/* ======================================================================== */

/// Function pointer type for receiving computed tuples.
/// Called once per output tuple during `wl_dd_execute_cb`.
///
/// - `relation`: null-terminated relation name
/// - `row`: array of i64 values (length = ncols)
/// - `ncols`: number of columns in the row
/// - `user_data`: opaque pointer passed through from the caller
pub type WlDdOnTupleFn = Option<
    unsafe extern "C" fn(
        relation: *const c_char,
        row: *const i64,
        ncols: u32,
        user_data: *mut c_void,
    ),
>;

/* ======================================================================== */
/* Worker Handle                                                            */
/* ======================================================================== */

/// Opaque worker handle exposed to C.
/// Stores configuration and EDB (input) data loaded before execution.
pub struct WlDdWorker {
    #[allow(dead_code)] // Used when timely::execute is wired up
    pub(crate) num_workers: u32,
    /// EDB data: relation_name -> Vec of rows, each row is Vec<i64>
    pub(crate) edb_data: HashMap<String, Vec<Vec<i64>>>,
    /// Column count per EDB relation
    pub(crate) edb_ncols: HashMap<String, u32>,
}

/* ======================================================================== */
/* FFI Entry Points                                                         */
/* ======================================================================== */

/// Create a DD worker pool.
///
/// # Safety
/// Called from C across FFI boundary.
#[no_mangle]
pub unsafe extern "C" fn wl_dd_worker_create(num_workers: u32) -> *mut WlDdWorker {
    if num_workers == 0 {
        return std::ptr::null_mut();
    }

    let worker = Box::new(WlDdWorker {
        num_workers,
        edb_data: HashMap::new(),
        edb_ncols: HashMap::new(),
    });
    Box::into_raw(worker)
}

/// Destroy a DD worker pool.
///
/// # Safety
/// `worker` must be a pointer returned by `wl_dd_worker_create`, or NULL.
#[no_mangle]
pub unsafe extern "C" fn wl_dd_worker_destroy(worker: *mut WlDdWorker) {
    if !worker.is_null() {
        // SAFETY: pointer was created by Box::into_raw in wl_dd_worker_create
        drop(Box::from_raw(worker));
    }
}

/// Load EDB (input) data into the worker before execution.
///
/// Each call appends rows to the named relation. Call multiple times
/// to load data for different relations.
///
/// # Safety
/// - `worker` must be a valid pointer from `wl_dd_worker_create`.
/// - `relation` must be a valid null-terminated C string.
/// - `data` must point to `num_rows * num_cols` contiguous i64 values.
///
/// Returns:
///    0: Success.
///   -2: Invalid arguments (NULL pointers or zero dimensions).
#[no_mangle]
pub unsafe extern "C" fn wl_dd_load_edb(
    worker: *mut WlDdWorker,
    relation: *const c_char,
    data: *const i64,
    num_rows: u32,
    num_cols: u32,
) -> c_int {
    if worker.is_null() || relation.is_null() {
        return -2;
    }
    if num_cols == 0 {
        return -2;
    }
    if num_rows == 0 {
        return 0; // No data to load, success
    }
    if data.is_null() {
        return -2;
    }

    // SAFETY: worker pointer from wl_dd_worker_create, relation is valid C string
    let worker = &mut *worker;
    let rel_name = match CStr::from_ptr(relation).to_str() {
        Ok(s) => s.to_owned(),
        Err(_) => return -2,
    };

    // Read rows from the flat data array
    let total = (num_rows as usize) * (num_cols as usize);
    let data_slice = std::slice::from_raw_parts(data, total);

    let rows: Vec<Vec<i64>> = data_slice
        .chunks_exact(num_cols as usize)
        .map(|chunk| chunk.to_vec())
        .collect();

    worker
        .edb_data
        .entry(rel_name.clone())
        .or_default()
        .extend(rows);
    worker.edb_ncols.insert(rel_name, num_cols);

    0
}

/// Execute a DD plan on the worker pool.
///
/// # Safety
/// `plan` and `worker` must be valid pointers or NULL.
#[no_mangle]
pub unsafe extern "C" fn wl_dd_execute(plan: *const WlFfiPlan, worker: *mut WlDdWorker) -> c_int {
    wl_dd_execute_cb(plan, worker, None, std::ptr::null_mut())
}

/// Execute a DD plan with result callback.
///
/// For each computed output tuple, calls `on_tuple` with the relation
/// name, row data, column count, and the caller's `user_data` pointer.
///
/// # Safety
/// - `plan` must be a valid FFI plan pointer.
/// - `worker` must be a valid worker pointer.
/// - `on_tuple` must be a valid function pointer (or NULL to discard results).
/// - `user_data` is passed through to the callback unchanged.
///
/// Returns:
///    0: Success.
///   -1: Execution error (DD runtime failure).
///   -2: Invalid arguments (NULL plan or worker).
#[no_mangle]
pub unsafe extern "C" fn wl_dd_execute_cb(
    plan: *const WlFfiPlan,
    worker: *mut WlDdWorker,
    on_tuple: WlDdOnTupleFn,
    user_data: *mut c_void,
) -> c_int {
    if plan.is_null() || worker.is_null() {
        return -2;
    }

    // Read FFI plan into safe Rust types
    let safe_plan = match read_plan(plan) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("[wirelog-dd] read_plan error: {:?}", e);
            return -1;
        }
    };

    let worker_ref = &*worker;

    // Execute the plan using the interpreter
    let result = match execute_plan(&safe_plan, &worker_ref.edb_data, worker_ref.num_workers) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[wirelog-dd] execute_plan error: {}", e);
            return -1;
        }
    };

    // Deliver results via callback
    if let Some(cb) = on_tuple {
        for (rel_name, rows) in &result.tuples {
            let c_rel = match CString::new(rel_name.as_str()) {
                Ok(s) => s,
                Err(_) => return -1,
            };
            for row in rows {
                cb(c_rel.as_ptr(), row.as_ptr(), row.len() as u32, user_data);
            }
        }
    }

    0
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_worker_create_returns_non_null() {
        unsafe {
            let w = wl_dd_worker_create(4);
            assert!(!w.is_null());
            wl_dd_worker_destroy(w);
        }
    }

    #[test]
    fn test_worker_create_zero_returns_null() {
        unsafe {
            let w = wl_dd_worker_create(0);
            assert!(w.is_null());
        }
    }

    #[test]
    fn test_worker_destroy_null_safe() {
        unsafe {
            wl_dd_worker_destroy(std::ptr::null_mut());
        }
    }

    #[test]
    fn test_execute_null_plan_returns_error() {
        unsafe {
            let w = wl_dd_worker_create(1);
            let rc = wl_dd_execute(std::ptr::null(), w);
            assert_eq!(rc, -2);
            wl_dd_worker_destroy(w);
        }
    }

    #[test]
    fn test_execute_null_worker_returns_error() {
        unsafe {
            let dummy = 1usize as *const WlFfiPlan;
            let rc = wl_dd_execute(dummy, std::ptr::null_mut());
            assert_eq!(rc, -2);
        }
    }

    // ---- EDB loading tests ----

    #[test]
    fn test_load_edb_null_worker() {
        unsafe {
            let name = CString::new("a").unwrap();
            let data: [i64; 2] = [1, 2];
            let rc = wl_dd_load_edb(std::ptr::null_mut(), name.as_ptr(), data.as_ptr(), 1, 2);
            assert_eq!(rc, -2);
        }
    }

    #[test]
    fn test_load_edb_null_relation() {
        unsafe {
            let w = wl_dd_worker_create(1);
            let data: [i64; 2] = [1, 2];
            let rc = wl_dd_load_edb(w, std::ptr::null(), data.as_ptr(), 1, 2);
            assert_eq!(rc, -2);
            wl_dd_worker_destroy(w);
        }
    }

    #[test]
    fn test_load_edb_zero_cols() {
        unsafe {
            let w = wl_dd_worker_create(1);
            let name = CString::new("a").unwrap();
            let rc = wl_dd_load_edb(w, name.as_ptr(), std::ptr::null(), 1, 0);
            assert_eq!(rc, -2);
            wl_dd_worker_destroy(w);
        }
    }

    #[test]
    fn test_load_edb_zero_rows_success() {
        unsafe {
            let w = wl_dd_worker_create(1);
            let name = CString::new("a").unwrap();
            let rc = wl_dd_load_edb(w, name.as_ptr(), std::ptr::null(), 0, 2);
            assert_eq!(rc, 0);
            wl_dd_worker_destroy(w);
        }
    }

    #[test]
    fn test_load_edb_stores_data() {
        unsafe {
            let w = wl_dd_worker_create(1);
            let name = CString::new("edge").unwrap();

            // 3 rows of 2 columns: (1,2), (3,4), (5,6)
            let data: [i64; 6] = [1, 2, 3, 4, 5, 6];
            let rc = wl_dd_load_edb(w, name.as_ptr(), data.as_ptr(), 3, 2);
            assert_eq!(rc, 0);

            // Verify stored data
            let worker = &*w;
            let rows = worker.edb_data.get("edge").unwrap();
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0], vec![1, 2]);
            assert_eq!(rows[1], vec![3, 4]);
            assert_eq!(rows[2], vec![5, 6]);
            assert_eq!(*worker.edb_ncols.get("edge").unwrap(), 2);

            wl_dd_worker_destroy(w);
        }
    }

    #[test]
    fn test_load_edb_multiple_relations() {
        unsafe {
            let w = wl_dd_worker_create(1);

            let name_a = CString::new("a").unwrap();
            let data_a: [i64; 2] = [10, 20];
            wl_dd_load_edb(w, name_a.as_ptr(), data_a.as_ptr(), 2, 1);

            let name_b = CString::new("b").unwrap();
            let data_b: [i64; 4] = [1, 2, 3, 4];
            wl_dd_load_edb(w, name_b.as_ptr(), data_b.as_ptr(), 2, 2);

            let worker = &*w;
            assert_eq!(worker.edb_data.len(), 2);
            assert_eq!(worker.edb_data["a"].len(), 2);
            assert_eq!(worker.edb_data["b"].len(), 2);
            assert_eq!(worker.edb_data["a"][0], vec![10]);
            assert_eq!(worker.edb_data["b"][1], vec![3, 4]);

            wl_dd_worker_destroy(w);
        }
    }

    #[test]
    fn test_load_edb_append_to_existing() {
        unsafe {
            let w = wl_dd_worker_create(1);
            let name = CString::new("a").unwrap();

            let data1: [i64; 2] = [1, 2];
            wl_dd_load_edb(w, name.as_ptr(), data1.as_ptr(), 1, 2);

            let data2: [i64; 2] = [3, 4];
            wl_dd_load_edb(w, name.as_ptr(), data2.as_ptr(), 1, 2);

            let worker = &*w;
            let rows = worker.edb_data.get("a").unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0], vec![1, 2]);
            assert_eq!(rows[1], vec![3, 4]);

            wl_dd_worker_destroy(w);
        }
    }

    // ---- Execute with callback tests ----

    #[test]
    fn test_execute_cb_null_plan() {
        unsafe {
            let w = wl_dd_worker_create(1);
            let rc = wl_dd_execute_cb(std::ptr::null(), w, None, std::ptr::null_mut());
            assert_eq!(rc, -2);
            wl_dd_worker_destroy(w);
        }
    }

    #[test]
    fn test_execute_cb_null_worker() {
        unsafe {
            let dummy = 1usize as *const WlFfiPlan;
            let rc = wl_dd_execute_cb(dummy, std::ptr::null_mut(), None, std::ptr::null_mut());
            assert_eq!(rc, -2);
        }
    }
}
