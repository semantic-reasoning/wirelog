/*
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

/*
 * session.rs - Persistent DD session with incremental delta support
 *
 * Implements a persistent DD session that keeps the dataflow alive
 * across multiple step() calls, supporting insert-only updates on
 * non-recursive (single-stratum) programs.
 *
 * Architecture:
 *   - Background thread runs timely::execute with a persistent dataflow
 *   - Commands sent via mpsc channel: Insert, Step, Shutdown
 *   - InputSession handles stored per-EDB relation for incremental input
 *   - Delta callback fires on inspect() for both positive and negative diffs
 *   - Timestamp type u64 enables epoch-based advancement
 *
 * MVP restrictions:
 *   - Single worker (num_workers == 1)
 *   - Non-recursive programs only (single stratum)
 */

use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::{c_char, c_void};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use differential_dataflow::input::Input;
use differential_dataflow::operators::{Iterate, Threshold};

use crate::dataflow::build_relation_plan;
use crate::plan_reader::SafePlan;

type Row = Vec<i64>;
type SnapshotState = HashMap<String, HashMap<Row, isize>>;

/* ======================================================================== */
/* Delta Callback                                                           */
/* ======================================================================== */

/// Holds the C delta callback function pointer and user data.
pub struct DeltaCallbackInfo {
    pub callback: unsafe extern "C" fn(*const c_char, *const i64, u32, i32, *mut c_void),
    pub user_data: *mut c_void,
}

// SAFETY: The callback is a C function pointer that is thread-safe.
// The user_data pointer is managed by the C side and must remain valid
// for the lifetime of the session.
unsafe impl Send for DeltaCallbackInfo {}
unsafe impl Sync for DeltaCallbackInfo {}

/* ======================================================================== */
/* Session Command                                                          */
/* ======================================================================== */

enum SessionCommand {
    Insert(String, Vec<Row>),
    Step(mpsc::Sender<Result<(), String>>),
    Remove(String, Vec<Row>),
    Snapshot(mpsc::Sender<Result<HashMap<String, Vec<Row>>, String>>),
    Shutdown,
}

/* ======================================================================== */
/* DdSession                                                                */
/* ======================================================================== */

/// Persistent DD session with background worker thread.
pub struct DdSession {
    cmd_tx: mpsc::Sender<SessionCommand>,
    worker_thread: Option<thread::JoinHandle<()>>,
    pub delta_callback: Arc<Mutex<Option<DeltaCallbackInfo>>>,
}

impl DdSession {
    /// Create a new persistent DD session.
    ///
    /// Spawns a background thread running timely::execute with a persistent
    /// dataflow.  The dataflow graph is built from the plan, with InputSession
    /// handles for each EDB relation.
    pub fn new(plan: SafePlan, edb_names: Vec<String>) -> Result<Self, String> {
        let (cmd_tx, cmd_rx) = mpsc::channel::<SessionCommand>();
        // Wrap receiver in Mutex to satisfy the Sync bound required by
        // timely::execute_directly.  Only one thread ever accesses it.
        let cmd_rx = Arc::new(Mutex::new(cmd_rx));
        let delta_callback: Arc<Mutex<Option<DeltaCallbackInfo>>> = Arc::new(Mutex::new(None));
        let delta_cb = delta_callback.clone();
        let snapshot_worker: Arc<Mutex<SnapshotState>> = Arc::new(Mutex::new(HashMap::new()));

        let worker_thread = thread::spawn(move || {
            timely::execute_directly(move |worker| {
                let mut next_epoch: u64 = 1;

                let (mut inputs, probe) = worker.dataflow::<u64, _, _>(|scope| {
                    let mut inputs = HashMap::new();
                    let mut collections = HashMap::new();

                    // Create input handles for all EDB relations
                    for rel_name in &edb_names {
                        let (input, coll) = scope.new_collection::<Row, isize>();
                        inputs.insert(rel_name.clone(), input);
                        collections.insert(rel_name.clone(), coll);
                    }

                    // Build dataflow graph from plan
                    let probe_handle = timely::dataflow::operators::probe::Handle::new();

                    for stratum in &plan.strata {
                        if stratum.is_recursive {
                            // Recursive stratum: use iterate() for fixpoint computation.
                            // Currently handles the single-recursive-relation case (common case).
                            if stratum.relations.len() == 1 {
                                let rel_plan = &stratum.relations[0];
                                let rel_name = rel_plan.name.clone();

                                // Seed with the base-case collection for this relation from prior
                                // non-recursive strata (e.g., tc(X,Y) :- edge(X,Y) base facts),
                                // or an empty collection if no base case exists.
                                let seed =
                                    collections.get(&rel_name).cloned().unwrap_or_else(|| {
                                        let (_, empty) =
                                            scope.new_collection_from(Vec::<Row>::new());
                                        empty
                                    });

                                let result = seed.iterate(|inner| {
                                    let mut inner_collections = HashMap::new();
                                    for (name, coll) in &collections {
                                        if name == &rel_name {
                                            // Use the iterating variable for the recursive relation
                                            inner_collections.insert(name.clone(), inner.clone());
                                        } else {
                                            inner_collections
                                                .insert(name.clone(), coll.enter(&inner.scope()));
                                        }
                                    }
                                    // Ensure recursive relation is in scope even if not in collections
                                    inner_collections
                                        .entry(rel_name.clone())
                                        .or_insert_with(|| inner.clone());

                                    let new_tuples =
                                        build_relation_plan(rel_plan, &inner_collections);
                                    inner.concat(&new_tuples).distinct()
                                });

                                let result = result.consolidate();
                                let rn = rel_name.clone();
                                let delta_cb_clone = delta_cb.clone();
                                let state_clone = snapshot_worker.clone();

                                result
                                    .inspect(move |&(ref row, _time, diff)| {
                                        if diff == 0 {
                                            return;
                                        }
                                        {
                                            let mut state = state_clone.lock().unwrap();
                                            let rel_state = state.entry(rn.clone()).or_default();
                                            let entry = rel_state.entry(row.clone()).or_insert(0);
                                            *entry += diff;
                                            if *entry == 0 {
                                                rel_state.remove(row);
                                            }
                                        }
                                        let cb_guard = delta_cb_clone.lock().unwrap();
                                        if let Some(ref cb_info) = *cb_guard {
                                            let diff_i32 = if diff > 0 { 1i32 } else { -1i32 };
                                            if let Ok(c_rel) = CString::new(rn.as_str()) {
                                                unsafe {
                                                    (cb_info.callback)(
                                                        c_rel.as_ptr(),
                                                        row.as_ptr(),
                                                        row.len() as u32,
                                                        diff_i32,
                                                        cb_info.user_data,
                                                    );
                                                }
                                            }
                                        }
                                    })
                                    .probe_with(&probe_handle);
                            }
                        } else {
                            for rel_plan in &stratum.relations {
                                let coll = build_relation_plan(rel_plan, &collections);
                                let coll = coll.consolidate();

                                let rel_name = rel_plan.name.clone();
                                // Make this derived relation available to subsequent strata
                                // (e.g., base-case tc available to the recursive tc stratum).
                                collections.insert(rel_name.clone(), coll.clone());
                                let delta_cb_clone = delta_cb.clone();
                                let state_clone = snapshot_worker.clone();

                                coll.inspect(move |&(ref row, _time, diff)| {
                                    if diff == 0 {
                                        return;
                                    }
                                    {
                                        let mut state = state_clone.lock().unwrap();
                                        let rel_state = state.entry(rel_name.clone()).or_default();
                                        let entry = rel_state.entry(row.clone()).or_insert(0);
                                        *entry += diff;
                                        if *entry == 0 {
                                            rel_state.remove(row);
                                        }
                                    }
                                    let cb_guard = delta_cb_clone.lock().unwrap();
                                    if let Some(ref cb_info) = *cb_guard {
                                        let diff_i32 = if diff > 0 { 1i32 } else { -1i32 };
                                        if let Ok(c_rel) = CString::new(rel_name.as_str()) {
                                            unsafe {
                                                (cb_info.callback)(
                                                    c_rel.as_ptr(),
                                                    row.as_ptr(),
                                                    row.len() as u32,
                                                    diff_i32,
                                                    cb_info.user_data,
                                                );
                                            }
                                        }
                                    }
                                })
                                .probe_with(&probe_handle);
                            }
                        }
                    }

                    (inputs, probe_handle)
                });

                // Command loop
                loop {
                    let msg = cmd_rx.lock().unwrap().recv();
                    match msg {
                        Ok(SessionCommand::Insert(rel, rows)) => {
                            if let Some(input) = inputs.get_mut(&rel) {
                                for row in rows {
                                    input.insert(row);
                                }
                            }
                        }
                        Ok(SessionCommand::Remove(rel, rows)) => {
                            if let Some(input) = inputs.get_mut(&rel) {
                                for row in rows {
                                    input.remove(row);
                                }
                            }
                        }
                        Ok(SessionCommand::Step(reply_tx)) => {
                            for input in inputs.values_mut() {
                                input.advance_to(next_epoch);
                                input.flush();
                            }
                            worker.step_while(|| probe.less_than(&next_epoch));
                            next_epoch += 1;
                            let _ = reply_tx.send(Ok(()));
                        }
                        Ok(SessionCommand::Snapshot(reply_tx)) => {
                            let state = snapshot_worker.lock().unwrap();
                            let result: HashMap<String, Vec<Row>> = state
                                .iter()
                                .map(|(rel, rows)| {
                                    let live: Vec<Row> = rows
                                        .iter()
                                        .filter_map(|(row, mult)| {
                                            if *mult > 0 {
                                                Some(row.clone())
                                            } else {
                                                None
                                            }
                                        })
                                        .collect();
                                    (rel.clone(), live)
                                })
                                .collect();
                            let _ = reply_tx.send(Ok(result));
                        }
                        Ok(SessionCommand::Shutdown) | Err(_) => break,
                    }
                }
            });
        });

        Ok(DdSession {
            cmd_tx,
            worker_thread: Some(worker_thread),
            delta_callback,
        })
    }

    /// Insert rows into an EDB relation.
    pub fn insert(&self, relation: &str, rows: Vec<Row>) -> Result<(), String> {
        self.cmd_tx
            .send(SessionCommand::Insert(relation.to_string(), rows))
            .map_err(|_| "session worker thread has exited".to_string())
    }

    /// Advance the session by one epoch, processing all pending inserts.
    pub fn step(&self) -> Result<(), String> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.cmd_tx
            .send(SessionCommand::Step(reply_tx))
            .map_err(|_| "session worker thread has exited".to_string())?;
        reply_rx
            .recv()
            .map_err(|_| "failed to receive step result".to_string())?
    }

    /// Remove rows from an EDB relation (retraction).
    pub fn session_remove(&self, relation: String, rows: Vec<Row>) -> Result<(), String> {
        self.cmd_tx
            .send(SessionCommand::Remove(relation, rows))
            .map_err(|_| "session worker thread has exited".to_string())
    }

    /// Return the current state of all output relations.
    pub fn session_snapshot(&self) -> Result<HashMap<String, Vec<Row>>, String> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.cmd_tx
            .send(SessionCommand::Snapshot(reply_tx))
            .map_err(|_| "session worker thread has exited".to_string())?;
        reply_rx
            .recv()
            .map_err(|_| "failed to receive snapshot result".to_string())?
    }

    /// Set or clear the delta callback.
    pub fn set_delta_callback(&self, cb_info: Option<DeltaCallbackInfo>) {
        let mut guard = self.delta_callback.lock().unwrap();
        *guard = cb_info;
    }
}

impl Drop for DdSession {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(SessionCommand::Shutdown);
        if let Some(handle) = self.worker_thread.take() {
            let _ = handle.join();
        }
    }
}
