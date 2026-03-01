/*
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

/*
 * dataflow.rs - Differential Dataflow plan execution
 *
 * Executes a SafePlan against in-memory EDB data using actual
 * Differential Dataflow operators via timely dataflow.
 *
 * Non-recursive strata build a DD operator graph in a single
 * worker.dataflow() scope.  Recursive strata use DD's iterate()
 * for fixed-point computation with distinct() for set semantics.
 *
 * Data model: all tuples are Vec<i64>, matching the wirelog i64
 * column representation used on the C side.
 */

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use differential_dataflow::collection::VecCollection;
use differential_dataflow::input::Input;
use differential_dataflow::operators::{Iterate, Join, Reduce, Threshold};
use timely::dataflow::Scope;

use crate::expr::{eval_filter, ExprOp, Value};
use crate::plan_reader::{SafeAggFn, SafeOp, SafePlan, SafeRelationPlan, SafeStratumPlan};

/* ======================================================================== */
/* Type Aliases                                                             */
/* ======================================================================== */

type Row = Vec<i64>;

/* ======================================================================== */
/* Result collection                                                        */
/* ======================================================================== */

/// Collected output tuples from execution.
#[derive(Debug, Default)]
pub struct ExecutionResult {
    /// relation_name -> Vec of rows
    pub tuples: HashMap<String, Vec<Row>>,
}

/* ======================================================================== */
/* Dataflow Execution                                                       */
/* ======================================================================== */

/// Execute a complete plan against provided EDB data.
///
/// Returns all computed IDB tuples grouped by relation name.
pub fn execute_plan(
    plan: &SafePlan,
    edb_data: &HashMap<String, Vec<Row>>,
    _num_workers: u32,
) -> Result<ExecutionResult, String> {
    // Clone plan and edb_data into owned values for the move closure
    let plan_owned = plan.clone();
    let edb_owned = edb_data.clone();

    // Shared result sink
    let results: Arc<Mutex<HashMap<String, Vec<Row>>>> = Arc::new(Mutex::new(HashMap::new()));
    let results_for_worker = results.clone();

    timely::execute(timely::Config::thread(), move |worker| {
        // Accumulate relation data across strata.
        // Start with EDB data; each stratum adds its IDB outputs.
        let mut relation_data: HashMap<String, Vec<Row>> = edb_owned.clone();

        for stratum in &plan_owned.strata {
            let stratum_results = if stratum.is_recursive {
                execute_recursive_stratum_dd(worker, stratum, &relation_data)
            } else {
                execute_non_recursive_stratum_dd(worker, stratum, &relation_data)
            };

            // Make newly computed relations available for subsequent strata
            for (name, rows) in &stratum_results {
                relation_data.insert(name.clone(), rows.clone());
            }

            // Store in final results (insert replaces prior strata's base case
            // since recursive strata produce the complete relation)
            let mut res = results_for_worker.lock().unwrap();
            for (name, rows) in stratum_results {
                res.insert(name, rows);
            }
        }
    })
    .map_err(|e| format!("Timely execution error: {:?}", e))?;

    let tuples = match Arc::try_unwrap(results) {
        Ok(mutex) => mutex.into_inner().unwrap(),
        Err(arc) => arc.lock().unwrap().clone(),
    };

    Ok(ExecutionResult { tuples })
}

/* ======================================================================== */
/* Non-Recursive Stratum (DD)                                               */
/* ======================================================================== */

/// Execute a non-recursive stratum using DD operators.
fn execute_non_recursive_stratum_dd<A: timely::communication::Allocate>(
    worker: &mut timely::worker::Worker<A>,
    stratum: &SafeStratumPlan,
    relation_data: &HashMap<String, Vec<Row>>,
) -> HashMap<String, Vec<Row>> {
    // If no data available, all relation results will be empty
    if relation_data.is_empty() {
        return stratum
            .relations
            .iter()
            .map(|r| (r.name.clone(), Vec::new()))
            .collect();
    }

    let results: Arc<Mutex<HashMap<String, Vec<Row>>>> = Arc::new(Mutex::new(HashMap::new()));
    let results_clone = results.clone();

    // Clone data needed inside the dataflow closure
    let relation_data_owned = relation_data.clone();
    let relations_owned = stratum.relations.clone();

    let probe = worker.dataflow::<(), _, _>(move |scope| {
        let probe = timely::dataflow::operators::probe::Handle::new();

        // Materialize all known relations as DD collections
        let mut collections = HashMap::new();
        for (name, rows) in &relation_data_owned {
            let (_, coll) = scope.new_collection_from(rows.clone());
            collections.insert(name.clone(), coll);
        }

        // Evaluate each relation plan
        for rel_plan in &relations_owned {
            let coll = build_relation_plan(rel_plan, &collections);
            // Consolidate to ensure diff cancellations (e.g. from antijoin)
            // are resolved before collecting results.
            let coll = coll.consolidate();
            // Attach result inspector
            let rel_name = rel_plan.name.clone();
            let results_ref = results_clone.clone();
            coll.inspect(move |&(ref data, _time, diff): &(Row, (), isize)| {
                if diff > 0 {
                    results_ref
                        .lock()
                        .unwrap()
                        .entry(rel_name.clone())
                        .or_default()
                        .push(data.clone());
                }
            })
            .probe_with(&probe);
        }

        probe
    });

    worker.step_while(|| !probe.done());

    match Arc::try_unwrap(results) {
        Ok(mutex) => mutex.into_inner().unwrap(),
        Err(arc) => arc.lock().unwrap().clone(),
    }
}

/* ======================================================================== */
/* Recursive Stratum (DD)                                                   */
/* ======================================================================== */

/// Execute a recursive stratum using DD's iterate().
fn execute_recursive_stratum_dd<A: timely::communication::Allocate>(
    worker: &mut timely::worker::Worker<A>,
    stratum: &SafeStratumPlan,
    relation_data: &HashMap<String, Vec<Row>>,
) -> HashMap<String, Vec<Row>> {
    let results: Arc<Mutex<HashMap<String, Vec<Row>>>> = Arc::new(Mutex::new(HashMap::new()));
    let results_clone = results.clone();

    let relation_data_owned = relation_data.clone();
    let relations_owned = stratum.relations.clone();

    // Collect the names of relations defined in this recursive stratum
    let recursive_rel_names: Vec<String> = relations_owned.iter().map(|r| r.name.clone()).collect();

    let probe = worker.dataflow::<(), _, _>(move |scope| {
        // Materialize EDB / prior-stratum collections
        let mut edb_collections = HashMap::new();
        for (name, rows) in &relation_data_owned {
            let (_, coll) = scope.new_collection_from(rows.clone());
            edb_collections.insert(name.clone(), coll);
        }

        // For recursive strata, we need to seed the recursive relations
        // with any existing data and then iterate.
        // We support one recursive relation per stratum (the common case).
        // For multiple mutually-recursive relations, we concatenate them
        // into a single tagged collection, iterate, then split.

        if recursive_rel_names.len() == 1 {
            // Single recursive relation (common case: e.g., transitive closure)
            let rel_name = &recursive_rel_names[0];
            let rel_plan = &relations_owned[0];

            // Seed: existing data for this relation (from prior stratum base case)
            let seed_data = relation_data_owned
                .get(rel_name)
                .cloned()
                .unwrap_or_default();
            let (_, seed_coll) = scope.new_collection_from(seed_data);

            let result = seed_coll.iterate(|inner| {
                // Bring EDB collections into the iterate scope
                let mut inner_collections = HashMap::new();
                for (name, coll) in &edb_collections {
                    inner_collections.insert(name.clone(), coll.enter(&inner.scope()));
                }
                // The iterating collection itself is available as `inner`
                inner_collections.insert(rel_name.clone(), inner.clone());

                let new_tuples = build_relation_plan(rel_plan, &inner_collections);
                inner.concat(&new_tuples).distinct()
            });

            // Post-process: consolidate recursive min/max results
            let result = if let Some((agg_fn, num_group_cols)) = find_reduce_in_plan(rel_plan) {
                apply_post_reduce(result, agg_fn, num_group_cols)
            } else {
                result
            };

            let rn = rel_name.clone();
            let results_ref = results_clone.clone();
            result
                .consolidate()
                .inspect(move |&(ref data, _time, diff): &(Row, _, isize)| {
                    if diff > 0 {
                        results_ref
                            .lock()
                            .unwrap()
                            .entry(rn.clone())
                            .or_default()
                            .push(data.clone());
                    }
                })
                .probe()
        } else {
            // Multiple mutually-recursive relations: tag rows with relation index
            // and iterate over the combined collection.
            // Tag format: prepend relation index as first element of row.

            // Seed all recursive relations
            let mut seed_rows: Vec<Row> = Vec::new();
            for (idx, rel_name) in recursive_rel_names.iter().enumerate() {
                if let Some(rows) = relation_data_owned.get(rel_name) {
                    for row in rows {
                        let mut tagged = vec![idx as i64];
                        tagged.extend(row.iter());
                        seed_rows.push(tagged);
                    }
                }
            }
            let (_, seed_coll) = scope.new_collection_from(seed_rows);

            let result = seed_coll.iterate(|inner| {
                let mut inner_edb = HashMap::new();
                for (name, coll) in &edb_collections {
                    inner_edb.insert(name.clone(), coll.enter(&inner.scope()));
                }

                let mut all_new: Option<VecCollection<_, Row>> = None;

                for (idx, rel_plan) in relations_owned.iter().enumerate() {
                    let tag = idx as i64;

                    let mut inner_collections = inner_edb.clone();
                    // Add all recursive relations (untagged) into scope
                    for (ridx, rname) in recursive_rel_names.iter().enumerate() {
                        let rtag = ridx as i64;
                        let rc = inner
                            .filter(move |row: &Row| row[0] == rtag)
                            .map(|row: Row| row[1..].to_vec());
                        inner_collections.insert(rname.clone(), rc);
                    }

                    let new_tuples = build_relation_plan(rel_plan, &inner_collections);
                    // Re-tag the results
                    let tagged_new = new_tuples.map(move |row: Row| {
                        let mut t = vec![tag];
                        t.extend(row.iter());
                        t
                    });

                    all_new = Some(match all_new {
                        Some(acc) => acc.concat(&tagged_new),
                        None => tagged_new,
                    });
                }

                let combined = match all_new {
                    Some(c) => inner.concat(&c),
                    None => inner.clone(),
                };
                combined.distinct()
            });

            // Split tagged results back into per-relation results
            for (idx, rel_name) in recursive_rel_names.iter().enumerate() {
                let tag = idx as i64;
                let rn = rel_name.clone();
                let results_ref = results_clone.clone();

                let per_rel = result
                    .filter(move |row: &Row| row[0] == tag)
                    .map(|row: Row| row[1..].to_vec());

                // Post-process: consolidate recursive min/max results
                let per_rel = if let Some((agg_fn, num_group_cols)) =
                    find_reduce_in_plan(&relations_owned[idx])
                {
                    apply_post_reduce(per_rel, agg_fn, num_group_cols)
                } else {
                    per_rel
                };

                per_rel
                    .consolidate()
                    .inspect(move |&(ref data, _time, diff): &(Row, _, isize)| {
                        if diff > 0 {
                            results_ref
                                .lock()
                                .unwrap()
                                .entry(rn.clone())
                                .or_default()
                                .push(data.clone());
                        }
                    })
                    .probe();
            }
            // Return a dummy probe from one of the result inspections
            result.probe()
        }
    });

    worker.step_while(|| !probe.done());

    match Arc::try_unwrap(results) {
        Ok(mutex) => mutex.into_inner().unwrap(),
        Err(arc) => arc.lock().unwrap().clone(),
    }
}

/* ======================================================================== */
/* Post-processing for recursive aggregation                                */
/* ======================================================================== */

/// Check if a relation plan contains a Reduce operation.
/// Returns the aggregation function and number of group-by columns.
fn find_reduce_in_plan(plan: &SafeRelationPlan) -> Option<(SafeAggFn, usize)> {
    for op in &plan.ops {
        if let SafeOp::Reduce {
            agg_fn,
            group_by_indices,
        } = op
        {
            return Some((*agg_fn, group_by_indices.len()));
        }
    }
    None
}

/// Apply a post-processing reduction to consolidate recursive aggregation.
///
/// After iterate() with concat+distinct, the result contains ALL derived
/// tuples across iterations.  For min/max aggregation, we must reduce
/// the accumulated tuples to keep only the aggregate value per group.
fn apply_post_reduce<G: Scope>(
    collection: VecCollection<G, Row>,
    agg_fn: SafeAggFn,
    num_group_cols: usize,
) -> VecCollection<G, Row>
where
    G::Timestamp: differential_dataflow::lattice::Lattice + Ord,
{
    let keyed = collection.map(move |row: Row| {
        let key: Row = row[..num_group_cols].to_vec();
        (key, row)
    });

    let reduced = keyed.reduce(
        move |_key: &Row, input: &[(&Row, isize)], output: &mut Vec<(i64, isize)>| {
            let agg_val = match agg_fn {
                SafeAggFn::Count => input.iter().map(|(_, c)| *c as i64).sum::<i64>(),
                SafeAggFn::Sum => {
                    let vi = if !input.is_empty() && !input[0].0.is_empty() {
                        input[0].0.len() - 1
                    } else {
                        0
                    };
                    input.iter().map(|(r, c)| r[vi] * (*c as i64)).sum()
                }
                SafeAggFn::Min => {
                    let vi = input[0].0.len() - 1;
                    input.iter().map(|(r, _)| r[vi]).min().unwrap_or(0)
                }
                SafeAggFn::Max => {
                    let vi = input[0].0.len() - 1;
                    input.iter().map(|(r, _)| r[vi]).max().unwrap_or(0)
                }
            };
            output.push((agg_val, 1));
        },
    );

    reduced.map(|(key, agg): (Row, i64)| {
        let mut r = key;
        r.push(agg);
        r
    })
}

/* ======================================================================== */
/* Relation Plan Builder (DD operator graph)                                */
/* ======================================================================== */

/// Build a DD operator graph for a single relation plan.
///
/// Translates the sequence of SafeOp instructions into chained DD
/// operators, returning the final Collection.
fn build_relation_plan<G: Scope>(
    rel_plan: &SafeRelationPlan,
    collections: &HashMap<String, VecCollection<G, Row>>,
) -> VecCollection<G, Row>
where
    G::Timestamp: differential_dataflow::lattice::Lattice + Ord,
{
    // `current` tracks the collection being built by the current operator chain.
    // `accumulated` collects results from prior UNION branches for Concat.
    let mut current: Option<VecCollection<G, Row>> = None;
    let mut accumulated: Option<VecCollection<G, Row>> = None;

    for op in &rel_plan.ops {
        match op {
            SafeOp::Variable { relation_name } => {
                // Save current to accumulator for CONCAT (UNION branches)
                if let Some(c) = current.take() {
                    accumulated = Some(match accumulated {
                        Some(acc) => acc.concat(&c),
                        None => c,
                    });
                }
                current = collections.get(relation_name).cloned();
                if current.is_none() {
                    // Empty collection for missing relations
                    // We need a scope reference -- get it from any existing collection
                    if let Some((_, any_coll)) = collections.iter().next() {
                        current = Some(any_coll.filter(|_: &Row| false));
                    }
                }
            }

            SafeOp::Map { indices, exprs } => {
                if let Some(c) = current.take() {
                    if !indices.is_empty() {
                        let indices = indices.clone();
                        let exprs = exprs.clone();
                        let has_exprs = exprs.iter().any(|e| e.is_some());
                        current = Some(c.map(move |row: Row| {
                            if has_exprs {
                                // Evaluate per-column expressions (head arithmetic)
                                let mut vars = HashMap::new();
                                for (i, val) in row.iter().enumerate() {
                                    vars.insert(format!("col{}", i), crate::expr::Value::Int(*val));
                                }
                                indices
                                    .iter()
                                    .enumerate()
                                    .map(|(col_idx, &idx)| {
                                        if let Some(Some(expr)) = exprs.get(col_idx) {
                                            crate::expr::eval_arith(expr, &vars).unwrap_or(0)
                                        } else {
                                            row[idx as usize]
                                        }
                                    })
                                    .collect()
                            } else {
                                indices.iter().map(|&i| row[i as usize]).collect()
                            }
                        }));
                    } else {
                        // Empty indices = identity pass-through
                        current = Some(c);
                    }
                }
            }

            SafeOp::Filter { expr } => {
                if let Some(c) = current.take() {
                    if !expr.is_empty() {
                        let expr = expr.clone();
                        current =
                            Some(c.filter(move |row: &Row| {
                                eval_filter_row(row, &expr).unwrap_or(false)
                            }));
                    } else {
                        current = Some(c);
                    }
                }
            }

            SafeOp::Join {
                right_relation,
                left_keys,
                right_keys: _,
                left_key_indices,
            } => {
                if let Some(c) = current.take() {
                    let key_count = left_keys.len();
                    let right_coll = collections.get(right_relation).cloned();

                    if let Some(right) = right_coll {
                        // Left: extract key by indices (or fall back to last k columns)
                        let k = key_count;
                        let lki = left_key_indices.clone();
                        let left_keyed = c.map(move |row: Row| {
                            let key: Row = if !lki.is_empty() {
                                lki.iter().map(|&i| row[i as usize]).collect()
                            } else {
                                row[row.len() - k..].to_vec()
                            };
                            (key, row)
                        });

                        // Right: key = first key_count columns, value = remaining cols
                        let right_keyed = right.map(move |row: Row| {
                            let key: Row = row[..key_count].to_vec();
                            let val: Row = row[key_count..].to_vec();
                            (key, val)
                        });

                        current = Some(left_keyed.join_map(
                            &right_keyed,
                            |_key: &Row, lrow: &Row, rval: &Row| {
                                let mut out = lrow.clone();
                                out.extend(rval.iter());
                                out
                            },
                        ));
                    } else {
                        // Right relation not found -> empty join result
                        current = Some(c.filter(|_: &Row| false));
                    }
                }
            }

            SafeOp::Antijoin {
                right_relation,
                left_keys,
                right_keys: _,
            } => {
                if let Some(c) = current.take() {
                    let key_count = left_keys.len();
                    let right_coll = collections.get(right_relation).cloned();

                    if let Some(right) = right_coll {
                        // Left: key = last key_count columns, value = full row
                        let k = key_count;
                        let left_keyed = c.map(move |row: Row| {
                            let key: Row = row[row.len() - k..].to_vec();
                            (key, row)
                        });

                        // Right: extract just the keys
                        let right_keys_coll = right.map(move |row: Row| row[..key_count].to_vec());

                        current =
                            Some(left_keyed.antijoin(&right_keys_coll).map(|(_key, row)| row));
                    } else {
                        // Right relation not found -> antijoin keeps everything
                        current = Some(c);
                    }
                }
            }

            SafeOp::Semijoin {
                right_relation,
                left_keys: _,
                right_keys: _,
                left_key_indices,
            } => {
                if let Some(c) = current.take() {
                    let right_coll = collections.get(right_relation).cloned();

                    if let Some(right) = right_coll {
                        let key_indices = left_key_indices.clone();
                        let key_count = key_indices.len();

                        // Left: extract key from specified column positions
                        let left_keyed = c.map(move |row: Row| {
                            let key: Row = key_indices.iter().map(|&i| row[i as usize]).collect();
                            (key, row)
                        });

                        // Right: key = first key_count columns
                        let right_keys_coll = right.map(move |row: Row| row[..key_count].to_vec());

                        // Semijoin: keep left rows whose key exists in right
                        current =
                            Some(left_keyed.semijoin(&right_keys_coll).map(|(_key, row)| row));
                    } else {
                        // Right relation not found -> semijoin filters everything
                        current = Some(c.filter(|_: &Row| false));
                    }
                }
            }

            SafeOp::Reduce {
                agg_fn,
                group_by_indices,
            } => {
                if let Some(c) = current.take() {
                    let gb = group_by_indices.clone();
                    let agg = *agg_fn;

                    // Key by group-by columns, value = full row
                    let keyed = c.map(move |row: Row| {
                        let key: Row = gb.iter().map(|&i| row[i as usize]).collect();
                        (key, row)
                    });

                    let reduced = keyed.reduce(
                        move |_key: &Row,
                              input: &[(&Row, isize)],
                              output: &mut Vec<(i64, isize)>| {
                            let agg_val = match agg {
                                SafeAggFn::Count => {
                                    input.iter().map(|(_, c)| *c as i64).sum::<i64>()
                                }
                                SafeAggFn::Sum => {
                                    // Sum the last non-group-by column
                                    let vi = if !input.is_empty() && !input[0].0.is_empty() {
                                        input[0].0.len() - 1
                                    } else {
                                        0
                                    };
                                    input.iter().map(|(r, c)| r[vi] * (*c as i64)).sum()
                                }
                                SafeAggFn::Min => {
                                    let vi = input[0].0.len() - 1;
                                    input.iter().map(|(r, _)| r[vi]).min().unwrap_or(0)
                                }
                                SafeAggFn::Max => {
                                    let vi = input[0].0.len() - 1;
                                    input.iter().map(|(r, _)| r[vi]).max().unwrap_or(0)
                                }
                            };
                            output.push((agg_val, 1));
                        },
                    );

                    // Output: key columns + aggregated value
                    current = Some(reduced.map(|(key, agg): (Row, i64)| {
                        let mut r = key;
                        r.push(agg);
                        r
                    }));
                }
            }

            SafeOp::Concat => {
                // Merge accumulated results from previous UNION branches
                // with the current branch's results.
                if let Some(c) = current.take() {
                    current = Some(match accumulated.take() {
                        Some(acc) => acc.concat(&c),
                        None => c,
                    });
                }
            }

            SafeOp::Consolidate => {
                // Set-semantic deduplication
                if let Some(c) = current.take() {
                    current = Some(c.distinct());
                }
            }
        }
    }

    // Return final collection (or empty if nothing was built)
    current.unwrap_or_else(|| {
        // Create an empty collection - find any scope reference
        if let Some((_, any_coll)) = collections.iter().next() {
            any_coll.filter(|_: &Row| false)
        } else {
            panic!("No collections available to create empty collection")
        }
    })
}

/* ======================================================================== */
/* Row-Level Operations                                                     */
/* ======================================================================== */

/// Evaluate a filter expression against a single row.
///
/// Column variables are named "col0", "col1", etc. matching the
/// positional column indices in the row.
fn eval_filter_row(row: &[i64], expr: &[ExprOp]) -> Result<bool, String> {
    let mut vars = HashMap::new();
    for (i, &val) in row.iter().enumerate() {
        vars.insert(format!("col{}", i), Value::Int(val));
    }
    eval_filter(expr, &vars).map_err(|e| e.to_string())
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_reader::*;

    // ---- Basic non-recursive tests ----

    #[test]
    fn test_execute_empty_plan() {
        let plan = SafePlan {
            strata: vec![],
            edb_relations: vec![],
        };
        let edb = HashMap::new();
        let result = execute_plan(&plan, &edb, 1).unwrap();
        assert!(result.tuples.is_empty());
    }

    #[test]
    fn test_variable_passthrough() {
        // tc(X,Y) :- edge(X,Y)
        let plan = SafePlan {
            strata: vec![SafeStratumPlan {
                stratum_id: 0,
                is_recursive: false,
                relations: vec![SafeRelationPlan {
                    name: "tc".to_string(),
                    ops: vec![SafeOp::Variable {
                        relation_name: "edge".to_string(),
                    }],
                }],
            }],
            edb_relations: vec!["edge".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert("edge".to_string(), vec![vec![1, 2], vec![2, 3], vec![3, 4]]);

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let tc = result.tuples.get("tc").unwrap();
        assert_eq!(tc.len(), 3);
        assert!(tc.contains(&vec![1, 2]));
        assert!(tc.contains(&vec![2, 3]));
        assert!(tc.contains(&vec![3, 4]));
    }

    #[test]
    fn test_map_projection() {
        // r(Y,X) :- edge(X,Y)  (swap columns)
        let plan = SafePlan {
            strata: vec![SafeStratumPlan {
                stratum_id: 0,
                is_recursive: false,
                relations: vec![SafeRelationPlan {
                    name: "r".to_string(),
                    ops: vec![
                        SafeOp::Variable {
                            relation_name: "edge".to_string(),
                        },
                        SafeOp::Map {
                            indices: vec![1, 0],
                            exprs: vec![],
                        },
                    ],
                }],
            }],
            edb_relations: vec!["edge".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert("edge".to_string(), vec![vec![1, 2], vec![3, 4]]);

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let r = result.tuples.get("r").unwrap();
        assert_eq!(r.len(), 2);
        assert!(r.contains(&vec![2, 1]));
        assert!(r.contains(&vec![4, 3]));
    }

    #[test]
    fn test_filter() {
        // r(X,Y) :- edge(X,Y), X > 1
        use crate::expr::ExprOp;
        let plan = SafePlan {
            strata: vec![SafeStratumPlan {
                stratum_id: 0,
                is_recursive: false,
                relations: vec![SafeRelationPlan {
                    name: "r".to_string(),
                    ops: vec![
                        SafeOp::Variable {
                            relation_name: "edge".to_string(),
                        },
                        SafeOp::Filter {
                            expr: vec![
                                ExprOp::Var("col0".to_string()),
                                ExprOp::ConstInt(1),
                                ExprOp::CmpGt,
                            ],
                        },
                    ],
                }],
            }],
            edb_relations: vec!["edge".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert("edge".to_string(), vec![vec![1, 2], vec![2, 3], vec![3, 4]]);

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let r = result.tuples.get("r").unwrap();
        assert_eq!(r.len(), 2);
        assert!(r.contains(&vec![2, 3]));
        assert!(r.contains(&vec![3, 4]));
    }

    #[test]
    fn test_join() {
        // result(X,Y,Z) :- a(X,Y), b(Y,Z)
        let plan = SafePlan {
            strata: vec![SafeStratumPlan {
                stratum_id: 0,
                is_recursive: false,
                relations: vec![SafeRelationPlan {
                    name: "result".to_string(),
                    ops: vec![
                        SafeOp::Variable {
                            relation_name: "a".to_string(),
                        },
                        SafeOp::Join {
                            right_relation: "b".to_string(),
                            left_keys: vec!["Y".to_string()],
                            right_keys: vec!["Y".to_string()],
                            left_key_indices: vec![],
                        },
                    ],
                }],
            }],
            edb_relations: vec!["a".to_string(), "b".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert("a".to_string(), vec![vec![1, 2], vec![3, 2]]);
        edb.insert("b".to_string(), vec![vec![2, 5], vec![2, 6]]);

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let r = result.tuples.get("result").unwrap();
        assert_eq!(r.len(), 4); // 2 left x 2 right matches
        assert!(r.contains(&vec![1, 2, 5]));
        assert!(r.contains(&vec![1, 2, 6]));
        assert!(r.contains(&vec![3, 2, 5]));
        assert!(r.contains(&vec![3, 2, 6]));
    }

    #[test]
    fn test_antijoin() {
        // result(X,Y) :- a(X,Y), not b(Y,_)
        let plan = SafePlan {
            strata: vec![SafeStratumPlan {
                stratum_id: 0,
                is_recursive: false,
                relations: vec![SafeRelationPlan {
                    name: "result".to_string(),
                    ops: vec![
                        SafeOp::Variable {
                            relation_name: "a".to_string(),
                        },
                        SafeOp::Antijoin {
                            right_relation: "b".to_string(),
                            left_keys: vec!["Y".to_string()],
                            right_keys: vec!["Y".to_string()],
                        },
                    ],
                }],
            }],
            edb_relations: vec!["a".to_string(), "b".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert("a".to_string(), vec![vec![1, 2], vec![3, 4], vec![5, 6]]);
        edb.insert("b".to_string(), vec![vec![2, 99]]);

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let r = result.tuples.get("result").unwrap();
        assert_eq!(r.len(), 2);
        assert!(r.contains(&vec![3, 4]));
        assert!(r.contains(&vec![5, 6]));
    }

    #[test]
    fn test_reduce_count() {
        // count_by_x(X, count) :- data(X, Y)
        let plan = SafePlan {
            strata: vec![SafeStratumPlan {
                stratum_id: 0,
                is_recursive: false,
                relations: vec![SafeRelationPlan {
                    name: "count_by_x".to_string(),
                    ops: vec![
                        SafeOp::Variable {
                            relation_name: "data".to_string(),
                        },
                        SafeOp::Reduce {
                            agg_fn: SafeAggFn::Count,
                            group_by_indices: vec![0],
                        },
                    ],
                }],
            }],
            edb_relations: vec!["data".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert(
            "data".to_string(),
            vec![vec![1, 10], vec![1, 20], vec![2, 30]],
        );

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let r = result.tuples.get("count_by_x").unwrap();
        assert_eq!(r.len(), 2);
        assert!(r.contains(&vec![1, 2])); // group 1: count=2
        assert!(r.contains(&vec![2, 1])); // group 2: count=1
    }

    #[test]
    fn test_consolidate_dedup() {
        let plan = SafePlan {
            strata: vec![SafeStratumPlan {
                stratum_id: 0,
                is_recursive: false,
                relations: vec![SafeRelationPlan {
                    name: "r".to_string(),
                    ops: vec![
                        SafeOp::Variable {
                            relation_name: "data".to_string(),
                        },
                        SafeOp::Consolidate,
                    ],
                }],
            }],
            edb_relations: vec!["data".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert("data".to_string(), vec![vec![1, 2], vec![1, 2], vec![3, 4]]);

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let r = result.tuples.get("r").unwrap();
        assert_eq!(r.len(), 2);
        assert!(r.contains(&vec![1, 2]));
        assert!(r.contains(&vec![3, 4]));
    }

    #[test]
    fn test_multi_stratum_chained() {
        // Stratum 0: middle(Y) :- edge(X,Y), Map[1]
        // Stratum 1: filtered(X,Y) :- edge(X,Y), Filter(col0 > 1)
        let plan = SafePlan {
            strata: vec![
                SafeStratumPlan {
                    stratum_id: 0,
                    is_recursive: false,
                    relations: vec![SafeRelationPlan {
                        name: "middle".to_string(),
                        ops: vec![
                            SafeOp::Variable {
                                relation_name: "edge".to_string(),
                            },
                            SafeOp::Map {
                                indices: vec![1],
                                exprs: vec![],
                            },
                        ],
                    }],
                },
                SafeStratumPlan {
                    stratum_id: 1,
                    is_recursive: false,
                    relations: vec![SafeRelationPlan {
                        name: "filtered".to_string(),
                        ops: vec![
                            SafeOp::Variable {
                                relation_name: "edge".to_string(),
                            },
                            SafeOp::Filter {
                                expr: vec![
                                    ExprOp::Var("col0".to_string()),
                                    ExprOp::ConstInt(1),
                                    ExprOp::CmpGt,
                                ],
                            },
                        ],
                    }],
                },
            ],
            edb_relations: vec!["edge".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert("edge".to_string(), vec![vec![1, 2], vec![2, 3], vec![3, 4]]);

        let result = execute_plan(&plan, &edb, 1).unwrap();

        let middle = result.tuples.get("middle").unwrap();
        assert_eq!(middle.len(), 3);
        assert!(middle.contains(&vec![2]));
        assert!(middle.contains(&vec![3]));
        assert!(middle.contains(&vec![4]));

        let filtered = result.tuples.get("filtered").unwrap();
        assert_eq!(filtered.len(), 2);
    }

    // ---- Recursive stratum tests ----

    #[test]
    fn test_transitive_closure_chain() {
        // edge: 1->2->3->4
        // Stratum 0 (non-recursive): tc(X,Y) :- edge(X,Y)
        // Stratum 1 (recursive):     tc(X,Z) :- tc(X,Y), edge(Y,Z), Map[0,2]
        let plan = SafePlan {
            strata: vec![
                SafeStratumPlan {
                    stratum_id: 0,
                    is_recursive: false,
                    relations: vec![SafeRelationPlan {
                        name: "tc".to_string(),
                        ops: vec![SafeOp::Variable {
                            relation_name: "edge".to_string(),
                        }],
                    }],
                },
                SafeStratumPlan {
                    stratum_id: 1,
                    is_recursive: true,
                    relations: vec![SafeRelationPlan {
                        name: "tc".to_string(),
                        ops: vec![
                            SafeOp::Variable {
                                relation_name: "tc".to_string(),
                            },
                            SafeOp::Join {
                                right_relation: "edge".to_string(),
                                left_keys: vec!["Y".to_string()],
                                right_keys: vec!["X".to_string()],
                                left_key_indices: vec![],
                            },
                            SafeOp::Map {
                                indices: vec![0, 2],
                                exprs: vec![],
                            },
                        ],
                    }],
                },
            ],
            edb_relations: vec!["edge".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert("edge".to_string(), vec![vec![1, 2], vec![2, 3], vec![3, 4]]);

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let tc = result.tuples.get("tc").unwrap();

        // Expected: (1,2), (2,3), (3,4), (1,3), (2,4), (1,4)
        assert_eq!(tc.len(), 6);
        assert!(tc.contains(&vec![1, 2]));
        assert!(tc.contains(&vec![2, 3]));
        assert!(tc.contains(&vec![3, 4]));
        assert!(tc.contains(&vec![1, 3]));
        assert!(tc.contains(&vec![2, 4]));
        assert!(tc.contains(&vec![1, 4]));
    }

    #[test]
    fn test_recursive_cycle() {
        // edge: 1->2->3->1 (cycle)
        // Should converge with 9 TC tuples (3 nodes, each reaches all 3)
        let plan = SafePlan {
            strata: vec![
                SafeStratumPlan {
                    stratum_id: 0,
                    is_recursive: false,
                    relations: vec![SafeRelationPlan {
                        name: "tc".to_string(),
                        ops: vec![SafeOp::Variable {
                            relation_name: "edge".to_string(),
                        }],
                    }],
                },
                SafeStratumPlan {
                    stratum_id: 1,
                    is_recursive: true,
                    relations: vec![SafeRelationPlan {
                        name: "tc".to_string(),
                        ops: vec![
                            SafeOp::Variable {
                                relation_name: "tc".to_string(),
                            },
                            SafeOp::Join {
                                right_relation: "edge".to_string(),
                                left_keys: vec!["Y".to_string()],
                                right_keys: vec!["X".to_string()],
                                left_key_indices: vec![],
                            },
                            SafeOp::Map {
                                indices: vec![0, 2],
                                exprs: vec![],
                            },
                        ],
                    }],
                },
            ],
            edb_relations: vec!["edge".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert("edge".to_string(), vec![vec![1, 2], vec![2, 3], vec![3, 1]]);

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let tc = result.tuples.get("tc").unwrap();

        // 3 nodes in a cycle: each node can reach every other node
        // (1,2), (2,3), (3,1), (1,3), (2,1), (3,2), (1,1), (2,2), (3,3)
        assert_eq!(tc.len(), 9);
    }

    #[test]
    fn test_empty_edb() {
        let plan = SafePlan {
            strata: vec![SafeStratumPlan {
                stratum_id: 0,
                is_recursive: false,
                relations: vec![SafeRelationPlan {
                    name: "tc".to_string(),
                    ops: vec![SafeOp::Variable {
                        relation_name: "edge".to_string(),
                    }],
                }],
            }],
            edb_relations: vec!["edge".to_string()],
        };

        let edb = HashMap::new();
        let result = execute_plan(&plan, &edb, 1).unwrap();
        let tc = result.tuples.get("tc").unwrap();
        assert!(tc.is_empty());
    }

    // ---- Non-recursive aggregation tests (sanity checks) ----

    #[test]
    fn test_reduce_min() {
        // result(x, min(y)) :- data(x, y)
        // data: (1,10), (1,20), (2,30), (2,5)
        // Expected: (1,10), (2,5)
        let plan = SafePlan {
            strata: vec![SafeStratumPlan {
                stratum_id: 0,
                is_recursive: false,
                relations: vec![SafeRelationPlan {
                    name: "result".to_string(),
                    ops: vec![
                        SafeOp::Variable {
                            relation_name: "data".to_string(),
                        },
                        SafeOp::Reduce {
                            agg_fn: SafeAggFn::Min,
                            group_by_indices: vec![0],
                        },
                    ],
                }],
            }],
            edb_relations: vec!["data".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert(
            "data".to_string(),
            vec![vec![1, 10], vec![1, 20], vec![2, 30], vec![2, 5]],
        );

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let r = result.tuples.get("result").unwrap();
        assert_eq!(r.len(), 2);
        assert!(r.contains(&vec![1, 10]));
        assert!(r.contains(&vec![2, 5]));
    }

    #[test]
    fn test_reduce_max() {
        // result(x, max(y)) :- data(x, y)
        // data: (1,10), (1,20), (2,30), (2,5)
        // Expected: (1,20), (2,30)
        let plan = SafePlan {
            strata: vec![SafeStratumPlan {
                stratum_id: 0,
                is_recursive: false,
                relations: vec![SafeRelationPlan {
                    name: "result".to_string(),
                    ops: vec![
                        SafeOp::Variable {
                            relation_name: "data".to_string(),
                        },
                        SafeOp::Reduce {
                            agg_fn: SafeAggFn::Max,
                            group_by_indices: vec![0],
                        },
                    ],
                }],
            }],
            edb_relations: vec!["data".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert(
            "data".to_string(),
            vec![vec![1, 10], vec![1, 20], vec![2, 30], vec![2, 5]],
        );

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let r = result.tuples.get("result").unwrap();
        assert_eq!(r.len(), 2);
        assert!(r.contains(&vec![1, 20]));
        assert!(r.contains(&vec![2, 30]));
    }

    // ---- Recursive aggregation tests (Issue #21) ----

    #[test]
    fn test_recursive_min_simple() {
        // Simple 2-node connected components:
        //   edge(1, 2)
        //   cc(x, x) :- edge(x, _).  cc(x, x) :- edge(_, x).  (base case)
        //   cc(y, min(c)) :- cc(x, c), edge(x, y).             (recursive)
        //
        // Base case: cc = {(1,1), (2,2)}
        // After recursion: cc(2, min(1)) from cc(1,1) ⋈ edge(1,2)
        // Expected final: cc = {(1,1), (2,1)}

        let plan = SafePlan {
            strata: vec![
                // Stratum 0 (non-recursive): cc(x,x) from edge endpoints
                SafeStratumPlan {
                    stratum_id: 0,
                    is_recursive: false,
                    relations: vec![SafeRelationPlan {
                        name: "cc".to_string(),
                        ops: vec![
                            SafeOp::Variable {
                                relation_name: "edge".to_string(),
                            },
                            SafeOp::Map {
                                indices: vec![0, 0],
                                exprs: vec![],
                            }, // (x,y) → (x,x)
                            SafeOp::Variable {
                                relation_name: "edge".to_string(),
                            },
                            SafeOp::Map {
                                indices: vec![1, 1],
                                exprs: vec![],
                            }, // (x,y) → (y,y)
                            SafeOp::Concat,
                            SafeOp::Consolidate,
                        ],
                    }],
                },
                // Stratum 1 (recursive): cc(y, min(c)) :- cc(x,c), edge(x,y)
                SafeStratumPlan {
                    stratum_id: 1,
                    is_recursive: true,
                    relations: vec![SafeRelationPlan {
                        name: "cc".to_string(),
                        ops: vec![
                            SafeOp::Variable {
                                relation_name: "cc".to_string(),
                            },
                            SafeOp::Map {
                                indices: vec![1, 0],
                                exprs: vec![],
                            }, // (x,c) → (c,x) put join key last
                            SafeOp::Join {
                                right_relation: "edge".to_string(),
                                left_keys: vec!["x".to_string()],
                                right_keys: vec!["x".to_string()],
                                left_key_indices: vec![],
                            },
                            // After join: (c, x, y) → project to (y, c)
                            SafeOp::Map {
                                indices: vec![2, 0],
                                exprs: vec![],
                            },
                            SafeOp::Reduce {
                                agg_fn: SafeAggFn::Min,
                                group_by_indices: vec![0],
                            },
                        ],
                    }],
                },
            ],
            edb_relations: vec!["edge".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert("edge".to_string(), vec![vec![1, 2]]);

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let cc = result.tuples.get("cc").unwrap();

        // Must contain the correct min labels only
        assert!(cc.contains(&vec![1, 1]), "cc should contain (1,1)");
        assert!(cc.contains(&vec![2, 1]), "cc should contain (2,1)");
        // Must NOT contain old base-case value superseded by min
        assert!(
            !cc.contains(&vec![2, 2]),
            "cc should NOT contain (2,2) — min(1) supersedes"
        );
        assert_eq!(cc.len(), 2, "cc should have exactly 2 tuples");
    }

    #[test]
    fn test_recursive_min_connected_components() {
        // Connected Components from Issue #21:
        //   edge(1,2). edge(2,3). edge(3,4).
        //   cc(x, x) :- edge(x, _).  cc(x, x) :- edge(_, x).
        //   cc(y, min(c)) :- cc(x, c), edge(x, y).
        //
        // Expected: all nodes get minimum label 1
        //   cc = {(1,1), (2,1), (3,1), (4,1)}

        let plan = SafePlan {
            strata: vec![
                // Stratum 0: base case
                SafeStratumPlan {
                    stratum_id: 0,
                    is_recursive: false,
                    relations: vec![SafeRelationPlan {
                        name: "cc".to_string(),
                        ops: vec![
                            SafeOp::Variable {
                                relation_name: "edge".to_string(),
                            },
                            SafeOp::Map {
                                indices: vec![0, 0],
                                exprs: vec![],
                            },
                            SafeOp::Variable {
                                relation_name: "edge".to_string(),
                            },
                            SafeOp::Map {
                                indices: vec![1, 1],
                                exprs: vec![],
                            },
                            SafeOp::Concat,
                            SafeOp::Consolidate,
                        ],
                    }],
                },
                // Stratum 1: recursive min
                SafeStratumPlan {
                    stratum_id: 1,
                    is_recursive: true,
                    relations: vec![SafeRelationPlan {
                        name: "cc".to_string(),
                        ops: vec![
                            SafeOp::Variable {
                                relation_name: "cc".to_string(),
                            },
                            SafeOp::Map {
                                indices: vec![1, 0],
                                exprs: vec![],
                            },
                            SafeOp::Join {
                                right_relation: "edge".to_string(),
                                left_keys: vec!["x".to_string()],
                                right_keys: vec!["x".to_string()],
                                left_key_indices: vec![],
                            },
                            SafeOp::Map {
                                indices: vec![2, 0],
                                exprs: vec![],
                            },
                            SafeOp::Reduce {
                                agg_fn: SafeAggFn::Min,
                                group_by_indices: vec![0],
                            },
                        ],
                    }],
                },
            ],
            edb_relations: vec!["edge".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert("edge".to_string(), vec![vec![1, 2], vec![2, 3], vec![3, 4]]);

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let cc = result.tuples.get("cc").unwrap();

        assert_eq!(
            cc.len(),
            4,
            "cc should have exactly 4 tuples (one per node)"
        );
        assert!(cc.contains(&vec![1, 1]));
        assert!(cc.contains(&vec![2, 1]));
        assert!(cc.contains(&vec![3, 1]));
        assert!(cc.contains(&vec![4, 1]));
    }

    #[test]
    fn test_recursive_max() {
        // Similar to CC but with max aggregation:
        //   edge(1,2). edge(2,3).
        //   label(x, x) :- edge(x, _).  label(x, x) :- edge(_, x).
        //   label(y, max(c)) :- label(x, c), edge(x, y).
        //
        // Base case: label = {(1,1), (2,2), (3,3)}
        // After recursion: labels propagate forward, max wins
        // Expected: label = {(1,1), (2,2), (3,3)}
        // (max propagates larger values, but 3 can't reach 1 or 2, so
        //  each node keeps its own label or gets a larger one from predecessor)
        // Actually: label(2, max(1,2))=2, label(3, max(2,3))=3
        // So max doesn't change anything here. Let me use reverse edges.

        // Better test: reverse edges so max propagates backward
        //   edge(2,1). edge(3,2).
        //   label(x, x) :- edge(x, _).  label(x, x) :- edge(_, x).
        //   label(y, max(c)) :- label(x, c), edge(x, y).
        //
        // Base: label = {(1,1), (2,2), (3,3)}
        // Iter 1: label(3,3) ⋈ edge(3,2) → label(2,3). label(2,2) ⋈ edge(2,1) → label(1,2).
        // Iter 2: label(2,3) ⋈ edge(2,1) → label(1,3).
        // Expected: label = {(1,3), (2,3), (3,3)}

        let plan = SafePlan {
            strata: vec![
                SafeStratumPlan {
                    stratum_id: 0,
                    is_recursive: false,
                    relations: vec![SafeRelationPlan {
                        name: "label".to_string(),
                        ops: vec![
                            SafeOp::Variable {
                                relation_name: "edge".to_string(),
                            },
                            SafeOp::Map {
                                indices: vec![0, 0],
                                exprs: vec![],
                            },
                            SafeOp::Variable {
                                relation_name: "edge".to_string(),
                            },
                            SafeOp::Map {
                                indices: vec![1, 1],
                                exprs: vec![],
                            },
                            SafeOp::Concat,
                            SafeOp::Consolidate,
                        ],
                    }],
                },
                SafeStratumPlan {
                    stratum_id: 1,
                    is_recursive: true,
                    relations: vec![SafeRelationPlan {
                        name: "label".to_string(),
                        ops: vec![
                            SafeOp::Variable {
                                relation_name: "label".to_string(),
                            },
                            SafeOp::Map {
                                indices: vec![1, 0],
                                exprs: vec![],
                            },
                            SafeOp::Join {
                                right_relation: "edge".to_string(),
                                left_keys: vec!["x".to_string()],
                                right_keys: vec!["x".to_string()],
                                left_key_indices: vec![],
                            },
                            SafeOp::Map {
                                indices: vec![2, 0],
                                exprs: vec![],
                            },
                            SafeOp::Reduce {
                                agg_fn: SafeAggFn::Max,
                                group_by_indices: vec![0],
                            },
                        ],
                    }],
                },
            ],
            edb_relations: vec!["edge".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert("edge".to_string(), vec![vec![2, 1], vec![3, 2]]);

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let label = result.tuples.get("label").unwrap();

        assert_eq!(
            label.len(),
            3,
            "label should have exactly 3 tuples (one per node)"
        );
        assert!(label.contains(&vec![1, 3]));
        assert!(label.contains(&vec![2, 3]));
        assert!(label.contains(&vec![3, 3]));
    }

    // ---- SSSP test (head arithmetic + recursive aggregation) ----

    #[test]
    fn test_sssp_head_arithmetic() {
        // SSSP on a simple 3-node chain:
        //   wedge(1, 2, 2).  wedge(2, 3, 1).
        //   dist(1, 0).
        //   dist(y, min(d + w)) :- dist(x, d), wedge(x, y, w).
        //
        // Expected: dist = {(1, 0), (2, 2), (3, 3)}

        use crate::expr::ExprOp;

        let plan = SafePlan {
            strata: vec![SafeStratumPlan {
                stratum_id: 0,
                is_recursive: true,
                relations: vec![SafeRelationPlan {
                    name: "dist".to_string(),
                    ops: vec![
                        SafeOp::Variable {
                            relation_name: "dist".to_string(),
                        },
                        // dist(x, d) → reorder to (d, x) with join key last
                        SafeOp::Map {
                            indices: vec![1, 0],
                            exprs: vec![],
                        },
                        SafeOp::Join {
                            right_relation: "wedge".to_string(),
                            left_keys: vec!["x".to_string()],
                            right_keys: vec!["x".to_string()],
                            left_key_indices: vec![],
                        },
                        // After join: (d, x, y, w) → project to (y, d+w)
                        SafeOp::Map {
                            indices: vec![2, 1],
                            exprs: vec![
                                None, // column 0: y = row[2]
                                Some(vec![
                                    ExprOp::Var("col0".to_string()),
                                    ExprOp::Var("col3".to_string()),
                                    ExprOp::ArithAdd,
                                ]), // column 1: d + w
                            ],
                        },
                        SafeOp::Reduce {
                            agg_fn: SafeAggFn::Min,
                            group_by_indices: vec![0],
                        },
                    ],
                }],
            }],
            edb_relations: vec!["wedge".to_string()],
        };

        let mut edb = HashMap::new();
        edb.insert("wedge".to_string(), vec![vec![1, 2, 2], vec![2, 3, 1]]);
        edb.insert("dist".to_string(), vec![vec![1, 0]]);

        let result = execute_plan(&plan, &edb, 1).unwrap();
        let dist = result.tuples.get("dist").unwrap();

        assert_eq!(dist.len(), 3, "dist should have 3 tuples, got {:?}", dist);
        assert!(dist.contains(&vec![1, 0]), "dist(1, 0) missing");
        assert!(dist.contains(&vec![2, 2]), "dist(2, 2) missing");
        assert!(dist.contains(&vec![3, 3]), "dist(3, 3) missing");
    }
}
