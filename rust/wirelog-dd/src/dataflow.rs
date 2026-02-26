/*
 * dataflow.rs - Interpreter-based plan execution (Phase 0)
 *
 * Executes a SafePlan against in-memory EDB data using a simple
 * interpreter.  Non-recursive strata run as a single pass.
 * Recursive strata iterate to a fixed point (max 1000 rounds).
 *
 * Phase 1 will replace this with actual Differential Dataflow
 * graph construction via timely/DD APIs.
 *
 * Data model: all tuples are Vec<i64>, matching the wirelog i64
 * column representation used on the C side.
 */

use std::collections::{HashMap, HashSet};

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
    let mut result = ExecutionResult::default();

    // Accumulate relation data across strata.
    // Start with EDB data; each stratum adds its IDB outputs.
    let mut relation_data: HashMap<String, Vec<Row>> = edb_data.clone();

    for stratum in &plan.strata {
        if stratum.is_recursive {
            execute_recursive_stratum(stratum, &mut relation_data, &mut result)?;
        } else {
            execute_non_recursive_stratum(stratum, &relation_data, &mut result)?;
        }

        // Make newly computed relations available for subsequent strata
        for (name, rows) in &result.tuples {
            relation_data.insert(name.clone(), rows.clone());
        }
    }

    Ok(result)
}

/* ======================================================================== */
/* Non-Recursive Stratum Execution                                          */
/* ======================================================================== */

/// Execute a non-recursive stratum (single pass).
///
/// Each relation plan is evaluated independently, consuming collections
/// from EDB or previously computed IDB relations.
fn execute_non_recursive_stratum(
    stratum: &SafeStratumPlan,
    relation_data: &HashMap<String, Vec<Row>>,
    result: &mut ExecutionResult,
) -> Result<(), String> {
    for rel_plan in &stratum.relations {
        let rows = evaluate_relation_plan(rel_plan, relation_data)?;
        result
            .tuples
            .entry(rel_plan.name.clone())
            .or_default()
            .extend(rows);
    }
    Ok(())
}

/* ======================================================================== */
/* Recursive Stratum Execution                                              */
/* ======================================================================== */

/// Maximum iterations for recursive fixed-point computation.
const MAX_ITERATIONS: u32 = 1000;

/// Execute a recursive stratum using iterated fixed-point.
///
/// Repeatedly evaluates all relation plans in the stratum until
/// no new tuples are produced (fixed point) or the iteration
/// limit is reached.
fn execute_recursive_stratum(
    stratum: &SafeStratumPlan,
    relation_data: &mut HashMap<String, Vec<Row>>,
    result: &mut ExecutionResult,
) -> Result<(), String> {
    // Collect all known tuples per relation in this stratum
    let mut known: HashMap<String, HashSet<Row>> = HashMap::new();

    // Seed with any existing data for these relations
    for rel_plan in &stratum.relations {
        let set = known.entry(rel_plan.name.clone()).or_default();
        if let Some(existing) = relation_data.get(&rel_plan.name) {
            for row in existing {
                set.insert(row.clone());
            }
        }
    }

    for _iteration in 0..MAX_ITERATIONS {
        let mut new_tuples_found = false;

        for rel_plan in &stratum.relations {
            let rows = evaluate_relation_plan(rel_plan, relation_data)?;
            let set = known.entry(rel_plan.name.clone()).or_default();

            for row in rows {
                if set.insert(row.clone()) {
                    new_tuples_found = true;
                    // Add to relation_data so next evaluation sees it
                    relation_data
                        .entry(rel_plan.name.clone())
                        .or_default()
                        .push(row);
                }
            }
        }

        if !new_tuples_found {
            break;
        }
    }

    // Collect final results
    for rel_plan in &stratum.relations {
        if let Some(set) = known.get(&rel_plan.name) {
            let rows: Vec<Row> = set.iter().cloned().collect();
            result.tuples.insert(rel_plan.name.clone(), rows);
        }
    }

    Ok(())
}

/* ======================================================================== */
/* Relation Plan Evaluation (Interpreter)                                   */
/* ======================================================================== */

/// Evaluate a relation plan by interpreting operators sequentially.
///
/// Each operator transforms an in-memory collection of rows.
fn evaluate_relation_plan(
    rel_plan: &SafeRelationPlan,
    relation_data: &HashMap<String, Vec<Row>>,
) -> Result<Vec<Row>, String> {
    let mut current: Vec<Row> = Vec::new();
    let mut accumulated: Vec<Row> = Vec::new();

    for op in &rel_plan.ops {
        match op {
            SafeOp::Variable { relation_name } => {
                // Save current to accumulator for CONCAT (UNION branches)
                if !current.is_empty() {
                    accumulated.append(&mut current);
                }
                current = relation_data
                    .get(relation_name)
                    .cloned()
                    .unwrap_or_default();
            }

            SafeOp::Map { indices } => {
                if !indices.is_empty() {
                    current = current
                        .into_iter()
                        .map(|row| indices.iter().map(|&i| row[i as usize]).collect())
                        .collect();
                }
                // Empty indices = identity pass-through (keep current as-is)
            }

            SafeOp::Filter { expr } => {
                if !expr.is_empty() {
                    current = filter_rows(current, expr)?;
                }
            }

            SafeOp::Join {
                right_relation,
                left_keys,
                right_keys,
            } => {
                let right = relation_data
                    .get(right_relation)
                    .cloned()
                    .unwrap_or_default();
                current = join_rows(&current, &right, left_keys, right_keys)?;
            }

            SafeOp::Antijoin {
                right_relation,
                left_keys,
                right_keys,
            } => {
                let right = relation_data
                    .get(right_relation)
                    .cloned()
                    .unwrap_or_default();
                current = antijoin_rows(&current, &right, left_keys, right_keys)?;
            }

            SafeOp::Reduce {
                agg_fn,
                group_by_indices,
            } => {
                current = reduce_rows(&current, agg_fn, group_by_indices)?;
            }

            SafeOp::Concat => {
                // Merge accumulated results from previous UNION branches
                // with the current branch's results.
                current.append(&mut accumulated);
            }

            SafeOp::Consolidate => {
                // Deduplicate
                current.sort();
                current.dedup();
            }
        }
    }

    Ok(current)
}

/* ======================================================================== */
/* Row-Level Operations                                                     */
/* ======================================================================== */

/// Filter rows using an expression evaluator.
fn filter_rows(rows: Vec<Row>, expr: &[ExprOp]) -> Result<Vec<Row>, String> {
    let mut result = Vec::new();
    for row in rows {
        if eval_filter_row(&row, expr).map_err(|e| format!("Filter error: {}", e))? {
            result.push(row);
        }
    }
    Ok(result)
}

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

/// Join two row collections on positional key columns.
///
/// Convention: the join key for the left side is the last `key_count`
/// columns; the join key for the right side is the first `key_count`
/// columns.  The output concatenates the left row with the right
/// row's non-key columns.
fn join_rows(
    left: &[Row],
    right: &[Row],
    left_keys: &[String],
    right_keys: &[String],
) -> Result<Vec<Row>, String> {
    if left_keys.len() != right_keys.len() {
        return Err("Join key count mismatch".to_string());
    }

    // Build a hash index on the right side (first key_count columns)
    let key_count = left_keys.len();
    let mut right_index: HashMap<Vec<i64>, Vec<&Row>> = HashMap::new();
    for row in right {
        let key: Vec<i64> = row.iter().take(key_count).cloned().collect();
        right_index.entry(key).or_default().push(row);
    }

    // Probe with left rows (last key_count columns as join key)
    let mut result = Vec::new();
    for lrow in left {
        let key: Vec<i64> = lrow.iter().rev().take(key_count).rev().cloned().collect();
        if let Some(matches) = right_index.get(&key) {
            for rrow in matches {
                let mut out = lrow.clone();
                out.extend(rrow.iter().skip(key_count));
                result.push(out);
            }
        }
    }

    Ok(result)
}

/// Antijoin: keep left rows that have NO match in right.
fn antijoin_rows(
    left: &[Row],
    right: &[Row],
    left_keys: &[String],
    right_keys: &[String],
) -> Result<Vec<Row>, String> {
    if left_keys.len() != right_keys.len() {
        return Err("Antijoin key count mismatch".to_string());
    }

    let key_count = left_keys.len();
    let mut right_keys_set: HashSet<Vec<i64>> = HashSet::new();
    for row in right {
        let key: Vec<i64> = row.iter().take(key_count).cloned().collect();
        right_keys_set.insert(key);
    }

    let result: Vec<Row> = left
        .iter()
        .filter(|lrow| {
            let key: Vec<i64> = lrow.iter().rev().take(key_count).rev().cloned().collect();
            !right_keys_set.contains(&key)
        })
        .cloned()
        .collect();

    Ok(result)
}

/// Reduce (aggregate) rows by group-by columns.
fn reduce_rows(
    rows: &[Row],
    agg_fn: &SafeAggFn,
    group_by_indices: &[u32],
) -> Result<Vec<Row>, String> {
    let mut groups: HashMap<Vec<i64>, Vec<&Row>> = HashMap::new();

    for row in rows {
        let key: Vec<i64> = group_by_indices.iter().map(|&i| row[i as usize]).collect();
        groups.entry(key).or_default().push(row);
    }

    let mut result = Vec::new();
    for (key, group) in groups {
        let agg_val = match agg_fn {
            SafeAggFn::Count => group.len() as i64,
            SafeAggFn::Sum => {
                // Sum the last non-group-by column
                let val_idx = if group[0].len() > group_by_indices.len() {
                    group[0].len() - 1
                } else {
                    0
                };
                group.iter().map(|r| r[val_idx]).sum()
            }
            SafeAggFn::Min => {
                let val_idx = group[0].len() - 1;
                group.iter().map(|r| r[val_idx]).min().unwrap_or(0)
            }
            SafeAggFn::Max => {
                let val_idx = group[0].len() - 1;
                group.iter().map(|r| r[val_idx]).max().unwrap_or(0)
            }
            SafeAggFn::Avg => {
                let val_idx = group[0].len() - 1;
                let sum: i64 = group.iter().map(|r| r[val_idx]).sum();
                sum / group.len() as i64
            }
        };

        let mut out = key;
        out.push(agg_val);
        result.push(out);
    }

    Ok(result)
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
                            SafeOp::Map { indices: vec![1] },
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
                            },
                            SafeOp::Map {
                                indices: vec![0, 2],
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
                            },
                            SafeOp::Map {
                                indices: vec![0, 2],
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
}
