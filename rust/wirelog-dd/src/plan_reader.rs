/*
 * plan_reader.rs - Convert unsafe FFI plan pointers into safe Rust types
 *
 * The C side passes a const wl_ffi_plan_t* across the FFI boundary.
 * This module reads all the C-owned data into owned Rust types so that
 * the dataflow builder can operate without any unsafe code.
 *
 * All string data is copied into Rust Strings.  Index arrays are copied
 * into Vec<u32>.  Expression buffers are deserialized into Vec<ExprOp>.
 */

use std::ffi::CStr;
use std::os::raw::c_char;

use crate::expr::{deserialize_expr, ExprOp};
use crate::ffi_types::{
    WlAggFn, WlFfiOp, WlFfiOpType, WlFfiPlan, WlFfiRelationPlan, WlFfiStratumPlan,
};

/* ======================================================================== */
/* Safe Rust Plan Types                                                     */
/* ======================================================================== */

/// Safe owned representation of a DD operator.
#[derive(Debug, Clone, PartialEq)]
pub enum SafeOp {
    /// Reference to an input collection (EDB or IDB).
    Variable { relation_name: String },

    /// Column projection.
    Map { indices: Vec<u32> },

    /// Predicate filter with deserialized expression.
    Filter { expr: Vec<ExprOp> },

    /// Equijoin on key columns.
    Join {
        right_relation: String,
        left_keys: Vec<String>,
        right_keys: Vec<String>,
    },

    /// Antijoin (negation).
    Antijoin {
        right_relation: String,
        left_keys: Vec<String>,
        right_keys: Vec<String>,
    },

    /// Aggregation with group-by.
    Reduce {
        agg_fn: SafeAggFn,
        group_by_indices: Vec<u32>,
    },

    /// Union of multiple collections.
    Concat,

    /// Deduplication.
    Consolidate,
}

/// Safe aggregation function enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SafeAggFn {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// Safe owned representation of a per-relation operator sequence.
#[derive(Debug, Clone, PartialEq)]
pub struct SafeRelationPlan {
    pub name: String,
    pub ops: Vec<SafeOp>,
}

/// Safe owned representation of a stratum.
#[derive(Debug, Clone, PartialEq)]
pub struct SafeStratumPlan {
    pub stratum_id: u32,
    pub is_recursive: bool,
    pub relations: Vec<SafeRelationPlan>,
}

/// Safe owned representation of a complete DD execution plan.
#[derive(Debug, Clone, PartialEq)]
pub struct SafePlan {
    pub strata: Vec<SafeStratumPlan>,
    pub edb_relations: Vec<String>,
}

/* ======================================================================== */
/* Plan Reader Errors                                                       */
/* ======================================================================== */

/// Errors that can occur when reading an FFI plan.
#[derive(Debug, PartialEq)]
#[allow(dead_code)] // Variants constructed via defensive checks on FFI input
pub enum PlanReadError {
    NullPointer(&'static str),
    InvalidUtf8(&'static str),
    InvalidOpType(u32),
    InvalidAggFn(u32),
    ExprDeserializeError(String),
}

impl std::fmt::Display for PlanReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanReadError::NullPointer(field) => write!(f, "NULL pointer: {}", field),
            PlanReadError::InvalidUtf8(field) => write!(f, "Invalid UTF-8: {}", field),
            PlanReadError::InvalidOpType(v) => write!(f, "Invalid op type: {}", v),
            PlanReadError::InvalidAggFn(v) => write!(f, "Invalid agg fn: {}", v),
            PlanReadError::ExprDeserializeError(e) => write!(f, "Expr error: {}", e),
        }
    }
}

impl std::error::Error for PlanReadError {}

/* ======================================================================== */
/* Unsafe Helpers                                                           */
/* ======================================================================== */

/// Read a null-terminated C string into an owned String.
///
/// # Safety
/// `ptr` must be a valid, null-terminated C string pointer.
unsafe fn read_cstr(ptr: *const c_char, field: &'static str) -> Result<String, PlanReadError> {
    if ptr.is_null() {
        return Err(PlanReadError::NullPointer(field));
    }
    CStr::from_ptr(ptr)
        .to_str()
        .map(|s| s.to_owned())
        .map_err(|_| PlanReadError::InvalidUtf8(field))
}

/// Read an array of C strings into a Vec<String>.
///
/// # Safety
/// `arr` must point to `count` valid null-terminated C string pointers.
unsafe fn read_cstr_array(
    arr: *const *const c_char,
    count: u32,
    field: &'static str,
) -> Result<Vec<String>, PlanReadError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if arr.is_null() {
        return Err(PlanReadError::NullPointer(field));
    }
    let ptrs = std::slice::from_raw_parts(arr, count as usize);
    ptrs.iter().map(|&p| read_cstr(p, field)).collect()
}

/// Read a u32 array into a Vec<u32>.
///
/// # Safety
/// `ptr` must point to `count` contiguous u32 values.
unsafe fn read_u32_array(
    ptr: *const u32,
    count: u32,
    field: &'static str,
) -> Result<Vec<u32>, PlanReadError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if ptr.is_null() {
        return Err(PlanReadError::NullPointer(field));
    }
    Ok(std::slice::from_raw_parts(ptr, count as usize).to_vec())
}

/* ======================================================================== */
/* Plan Reader                                                              */
/* ======================================================================== */

/// Read a single FFI operator into a safe Rust type.
///
/// # Safety
/// The `op` reference must point to a valid `WlFfiOp` with all pointer
/// fields valid for the duration of this call.
unsafe fn read_op(op: &WlFfiOp) -> Result<SafeOp, PlanReadError> {
    match op.op {
        WlFfiOpType::Variable => {
            let name = read_cstr(op.relation_name, "op.relation_name")?;
            Ok(SafeOp::Variable {
                relation_name: name,
            })
        }

        WlFfiOpType::Map => {
            let indices =
                read_u32_array(op.project_indices, op.project_count, "op.project_indices")?;
            Ok(SafeOp::Map { indices })
        }

        WlFfiOpType::Filter => {
            if op.filter_expr.data.is_null() || op.filter_expr.size == 0 {
                Ok(SafeOp::Filter { expr: Vec::new() })
            } else {
                let data =
                    std::slice::from_raw_parts(op.filter_expr.data, op.filter_expr.size as usize);
                let expr = deserialize_expr(data)
                    .map_err(|e| PlanReadError::ExprDeserializeError(e.to_string()))?;
                Ok(SafeOp::Filter { expr })
            }
        }

        WlFfiOpType::Join => {
            let right = read_cstr(op.right_relation, "op.right_relation")?;
            let lk = read_cstr_array(op.left_keys, op.key_count, "op.left_keys")?;
            let rk = read_cstr_array(op.right_keys, op.key_count, "op.right_keys")?;
            Ok(SafeOp::Join {
                right_relation: right,
                left_keys: lk,
                right_keys: rk,
            })
        }

        WlFfiOpType::Antijoin => {
            let right = read_cstr(op.right_relation, "op.right_relation")?;
            let lk = read_cstr_array(op.left_keys, op.key_count, "op.left_keys")?;
            let rk = read_cstr_array(op.right_keys, op.key_count, "op.right_keys")?;
            Ok(SafeOp::Antijoin {
                right_relation: right,
                left_keys: lk,
                right_keys: rk,
            })
        }

        WlFfiOpType::Reduce => {
            let agg = match op.agg_fn {
                WlAggFn::Count => SafeAggFn::Count,
                WlAggFn::Sum => SafeAggFn::Sum,
                WlAggFn::Min => SafeAggFn::Min,
                WlAggFn::Max => SafeAggFn::Max,
                WlAggFn::Avg => SafeAggFn::Avg,
            };
            let gb = read_u32_array(
                op.group_by_indices,
                op.group_by_count,
                "op.group_by_indices",
            )?;
            Ok(SafeOp::Reduce {
                agg_fn: agg,
                group_by_indices: gb,
            })
        }

        WlFfiOpType::Concat => Ok(SafeOp::Concat),
        WlFfiOpType::Consolidate => Ok(SafeOp::Consolidate),
    }
}

/// Read a per-relation plan.
///
/// # Safety
/// The `rp` reference must point to a valid `WlFfiRelationPlan`.
unsafe fn read_relation_plan(rp: &WlFfiRelationPlan) -> Result<SafeRelationPlan, PlanReadError> {
    let name = read_cstr(rp.name, "relation.name")?;
    let mut ops = Vec::with_capacity(rp.op_count as usize);
    if rp.op_count > 0 {
        if rp.ops.is_null() {
            return Err(PlanReadError::NullPointer("relation.ops"));
        }
        let op_slice = std::slice::from_raw_parts(rp.ops, rp.op_count as usize);
        for op in op_slice {
            ops.push(read_op(op)?);
        }
    }
    Ok(SafeRelationPlan { name, ops })
}

/// Read a stratum plan.
///
/// # Safety
/// The `sp` reference must point to a valid `WlFfiStratumPlan`.
unsafe fn read_stratum_plan(sp: &WlFfiStratumPlan) -> Result<SafeStratumPlan, PlanReadError> {
    let mut relations = Vec::with_capacity(sp.relation_count as usize);
    if sp.relation_count > 0 {
        if sp.relations.is_null() {
            return Err(PlanReadError::NullPointer("stratum.relations"));
        }
        let rel_slice = std::slice::from_raw_parts(sp.relations, sp.relation_count as usize);
        for rp in rel_slice {
            relations.push(read_relation_plan(rp)?);
        }
    }
    Ok(SafeStratumPlan {
        stratum_id: sp.stratum_id,
        is_recursive: sp.is_recursive,
        relations,
    })
}

/// Read a complete FFI plan into safe Rust types.
///
/// This is the main entry point. It copies all data from C-owned memory
/// into Rust-owned types so that no unsafe pointers escape.
///
/// # Safety
/// `plan` must be a valid pointer to a `WlFfiPlan` with all nested
/// pointers valid for the duration of this call.
pub unsafe fn read_plan(plan: *const WlFfiPlan) -> Result<SafePlan, PlanReadError> {
    if plan.is_null() {
        return Err(PlanReadError::NullPointer("plan"));
    }
    let plan = &*plan;

    // Read EDB relation names
    let edb_relations = read_cstr_array(plan.edb_relations, plan.edb_count, "plan.edb_relations")?;

    // Read strata
    let mut strata = Vec::with_capacity(plan.stratum_count as usize);
    if plan.stratum_count > 0 {
        if plan.strata.is_null() {
            return Err(PlanReadError::NullPointer("plan.strata"));
        }
        let strata_slice = std::slice::from_raw_parts(plan.strata, plan.stratum_count as usize);
        for sp in strata_slice {
            strata.push(read_stratum_plan(sp)?);
        }
    }

    Ok(SafePlan {
        strata,
        edb_relations,
    })
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi_types::WlFfiExprBuffer;
    use std::ffi::CString;

    // ---- Null pointer tests ----

    #[test]
    fn test_read_plan_null() {
        unsafe {
            let result = read_plan(std::ptr::null());
            assert_eq!(result, Err(PlanReadError::NullPointer("plan")));
        }
    }

    // ---- Empty plan ----

    #[test]
    fn test_read_empty_plan() {
        let plan = WlFfiPlan {
            strata: std::ptr::null(),
            stratum_count: 0,
            edb_relations: std::ptr::null(),
            edb_count: 0,
        };
        unsafe {
            let result = read_plan(&plan).unwrap();
            assert_eq!(result.strata.len(), 0);
            assert_eq!(result.edb_relations.len(), 0);
        }
    }

    // ---- EDB relation names ----

    #[test]
    fn test_read_plan_edb_relations() {
        let name_edge = CString::new("edge").unwrap();
        let name_node = CString::new("node").unwrap();
        let edb_ptrs = [name_edge.as_ptr(), name_node.as_ptr()];

        let plan = WlFfiPlan {
            strata: std::ptr::null(),
            stratum_count: 0,
            edb_relations: edb_ptrs.as_ptr(),
            edb_count: 2,
        };
        unsafe {
            let result = read_plan(&plan).unwrap();
            assert_eq!(result.edb_relations, vec!["edge", "node"]);
        }
    }

    // ---- Variable operator ----

    #[test]
    fn test_read_variable_op() {
        let name = CString::new("edge").unwrap();
        let rel_name = CString::new("tc").unwrap();

        let op = WlFfiOp {
            op: WlFfiOpType::Variable,
            relation_name: name.as_ptr(),
            right_relation: std::ptr::null(),
            left_keys: std::ptr::null(),
            right_keys: std::ptr::null(),
            key_count: 0,
            project_indices: std::ptr::null(),
            project_count: 0,
            filter_expr: WlFfiExprBuffer {
                data: std::ptr::null(),
                size: 0,
            },
            agg_fn: WlAggFn::Count,
            group_by_indices: std::ptr::null(),
            group_by_count: 0,
        };

        let ops = [op];
        let relation = WlFfiRelationPlan {
            name: rel_name.as_ptr(),
            ops: ops.as_ptr(),
            op_count: 1,
        };
        let relations = [relation];
        let stratum = WlFfiStratumPlan {
            stratum_id: 0,
            is_recursive: false,
            relations: relations.as_ptr(),
            relation_count: 1,
        };
        let strata = [stratum];
        let plan = WlFfiPlan {
            strata: strata.as_ptr(),
            stratum_count: 1,
            edb_relations: std::ptr::null(),
            edb_count: 0,
        };

        unsafe {
            let result = read_plan(&plan).unwrap();
            assert_eq!(result.strata.len(), 1);
            assert_eq!(result.strata[0].stratum_id, 0);
            assert!(!result.strata[0].is_recursive);
            assert_eq!(result.strata[0].relations.len(), 1);
            assert_eq!(result.strata[0].relations[0].name, "tc");
            assert_eq!(
                result.strata[0].relations[0].ops[0],
                SafeOp::Variable {
                    relation_name: "edge".to_string()
                }
            );
        }
    }

    // ---- Map operator ----

    #[test]
    fn test_read_map_op() {
        let rel_name = CString::new("r").unwrap();
        let indices: [u32; 2] = [1, 0];

        let op = WlFfiOp {
            op: WlFfiOpType::Map,
            relation_name: std::ptr::null(),
            right_relation: std::ptr::null(),
            left_keys: std::ptr::null(),
            right_keys: std::ptr::null(),
            key_count: 0,
            project_indices: indices.as_ptr(),
            project_count: 2,
            filter_expr: WlFfiExprBuffer {
                data: std::ptr::null(),
                size: 0,
            },
            agg_fn: WlAggFn::Count,
            group_by_indices: std::ptr::null(),
            group_by_count: 0,
        };

        let ops = [op];
        let relation = WlFfiRelationPlan {
            name: rel_name.as_ptr(),
            ops: ops.as_ptr(),
            op_count: 1,
        };
        let relations = [relation];
        let stratum = WlFfiStratumPlan {
            stratum_id: 0,
            is_recursive: false,
            relations: relations.as_ptr(),
            relation_count: 1,
        };
        let strata = [stratum];
        let plan = WlFfiPlan {
            strata: strata.as_ptr(),
            stratum_count: 1,
            edb_relations: std::ptr::null(),
            edb_count: 0,
        };

        unsafe {
            let result = read_plan(&plan).unwrap();
            assert_eq!(
                result.strata[0].relations[0].ops[0],
                SafeOp::Map {
                    indices: vec![1, 0]
                }
            );
        }
    }

    // ---- Filter operator with expression ----

    #[test]
    fn test_read_filter_op() {
        let rel_name = CString::new("r").unwrap();
        // Expr: X > 5 => VAR "X", CONST_INT 5, CMP_GT
        let mut expr_data: Vec<u8> = vec![0x01, 0x01, 0x00, 0x58]; // VAR "X"
        expr_data.push(0x02); // CONST_INT
        expr_data.extend_from_slice(&5i64.to_le_bytes());
        expr_data.push(0x23); // CMP_GT

        let op = WlFfiOp {
            op: WlFfiOpType::Filter,
            relation_name: std::ptr::null(),
            right_relation: std::ptr::null(),
            left_keys: std::ptr::null(),
            right_keys: std::ptr::null(),
            key_count: 0,
            project_indices: std::ptr::null(),
            project_count: 0,
            filter_expr: WlFfiExprBuffer {
                data: expr_data.as_ptr(),
                size: expr_data.len() as u32,
            },
            agg_fn: WlAggFn::Count,
            group_by_indices: std::ptr::null(),
            group_by_count: 0,
        };

        let ops = [op];
        let relation = WlFfiRelationPlan {
            name: rel_name.as_ptr(),
            ops: ops.as_ptr(),
            op_count: 1,
        };
        let relations = [relation];
        let stratum = WlFfiStratumPlan {
            stratum_id: 0,
            is_recursive: false,
            relations: relations.as_ptr(),
            relation_count: 1,
        };
        let strata = [stratum];
        let plan = WlFfiPlan {
            strata: strata.as_ptr(),
            stratum_count: 1,
            edb_relations: std::ptr::null(),
            edb_count: 0,
        };

        unsafe {
            let result = read_plan(&plan).unwrap();
            let filter_op = &result.strata[0].relations[0].ops[0];
            match filter_op {
                SafeOp::Filter { expr } => {
                    assert_eq!(expr.len(), 3);
                    assert_eq!(expr[0], ExprOp::Var("X".to_string()));
                    assert_eq!(expr[1], ExprOp::ConstInt(5));
                    assert_eq!(expr[2], ExprOp::CmpGt);
                }
                _ => panic!("Expected Filter op"),
            }
        }
    }

    // ---- Join operator ----

    #[test]
    fn test_read_join_op() {
        let rel_name = CString::new("tc").unwrap();
        let right_name = CString::new("edge").unwrap();
        let lk0 = CString::new("Y").unwrap();
        let rk0 = CString::new("X").unwrap();
        let left_key_ptrs = [lk0.as_ptr()];
        let right_key_ptrs = [rk0.as_ptr()];

        let op = WlFfiOp {
            op: WlFfiOpType::Join,
            relation_name: std::ptr::null(),
            right_relation: right_name.as_ptr(),
            left_keys: left_key_ptrs.as_ptr(),
            right_keys: right_key_ptrs.as_ptr(),
            key_count: 1,
            project_indices: std::ptr::null(),
            project_count: 0,
            filter_expr: WlFfiExprBuffer {
                data: std::ptr::null(),
                size: 0,
            },
            agg_fn: WlAggFn::Count,
            group_by_indices: std::ptr::null(),
            group_by_count: 0,
        };

        let ops = [op];
        let relation = WlFfiRelationPlan {
            name: rel_name.as_ptr(),
            ops: ops.as_ptr(),
            op_count: 1,
        };
        let relations = [relation];
        let stratum = WlFfiStratumPlan {
            stratum_id: 1,
            is_recursive: true,
            relations: relations.as_ptr(),
            relation_count: 1,
        };
        let strata = [stratum];
        let plan = WlFfiPlan {
            strata: strata.as_ptr(),
            stratum_count: 1,
            edb_relations: std::ptr::null(),
            edb_count: 0,
        };

        unsafe {
            let result = read_plan(&plan).unwrap();
            assert!(result.strata[0].is_recursive);
            assert_eq!(result.strata[0].stratum_id, 1);
            assert_eq!(
                result.strata[0].relations[0].ops[0],
                SafeOp::Join {
                    right_relation: "edge".to_string(),
                    left_keys: vec!["Y".to_string()],
                    right_keys: vec!["X".to_string()],
                }
            );
        }
    }

    // ---- Antijoin operator ----

    #[test]
    fn test_read_antijoin_op() {
        let rel_name = CString::new("not_edge").unwrap();
        let right_name = CString::new("edge").unwrap();
        let lk0 = CString::new("A").unwrap();
        let rk0 = CString::new("B").unwrap();
        let left_key_ptrs = [lk0.as_ptr()];
        let right_key_ptrs = [rk0.as_ptr()];

        let op = WlFfiOp {
            op: WlFfiOpType::Antijoin,
            relation_name: std::ptr::null(),
            right_relation: right_name.as_ptr(),
            left_keys: left_key_ptrs.as_ptr(),
            right_keys: right_key_ptrs.as_ptr(),
            key_count: 1,
            project_indices: std::ptr::null(),
            project_count: 0,
            filter_expr: WlFfiExprBuffer {
                data: std::ptr::null(),
                size: 0,
            },
            agg_fn: WlAggFn::Count,
            group_by_indices: std::ptr::null(),
            group_by_count: 0,
        };

        let ops = [op];
        let relation = WlFfiRelationPlan {
            name: rel_name.as_ptr(),
            ops: ops.as_ptr(),
            op_count: 1,
        };
        let relations = [relation];
        let stratum = WlFfiStratumPlan {
            stratum_id: 0,
            is_recursive: false,
            relations: relations.as_ptr(),
            relation_count: 1,
        };
        let strata = [stratum];
        let plan = WlFfiPlan {
            strata: strata.as_ptr(),
            stratum_count: 1,
            edb_relations: std::ptr::null(),
            edb_count: 0,
        };

        unsafe {
            let result = read_plan(&plan).unwrap();
            assert_eq!(
                result.strata[0].relations[0].ops[0],
                SafeOp::Antijoin {
                    right_relation: "edge".to_string(),
                    left_keys: vec!["A".to_string()],
                    right_keys: vec!["B".to_string()],
                }
            );
        }
    }

    // ---- Reduce operator ----

    #[test]
    fn test_read_reduce_op() {
        let rel_name = CString::new("count_rel").unwrap();
        let gb_indices: [u32; 1] = [0];

        let op = WlFfiOp {
            op: WlFfiOpType::Reduce,
            relation_name: std::ptr::null(),
            right_relation: std::ptr::null(),
            left_keys: std::ptr::null(),
            right_keys: std::ptr::null(),
            key_count: 0,
            project_indices: std::ptr::null(),
            project_count: 0,
            filter_expr: WlFfiExprBuffer {
                data: std::ptr::null(),
                size: 0,
            },
            agg_fn: WlAggFn::Sum,
            group_by_indices: gb_indices.as_ptr(),
            group_by_count: 1,
        };

        let ops = [op];
        let relation = WlFfiRelationPlan {
            name: rel_name.as_ptr(),
            ops: ops.as_ptr(),
            op_count: 1,
        };
        let relations = [relation];
        let stratum = WlFfiStratumPlan {
            stratum_id: 0,
            is_recursive: false,
            relations: relations.as_ptr(),
            relation_count: 1,
        };
        let strata = [stratum];
        let plan = WlFfiPlan {
            strata: strata.as_ptr(),
            stratum_count: 1,
            edb_relations: std::ptr::null(),
            edb_count: 0,
        };

        unsafe {
            let result = read_plan(&plan).unwrap();
            assert_eq!(
                result.strata[0].relations[0].ops[0],
                SafeOp::Reduce {
                    agg_fn: SafeAggFn::Sum,
                    group_by_indices: vec![0],
                }
            );
        }
    }

    // ---- Concat and Consolidate ----

    #[test]
    fn test_read_concat_consolidate_ops() {
        let rel_name = CString::new("r").unwrap();

        let op_concat = WlFfiOp {
            op: WlFfiOpType::Concat,
            relation_name: std::ptr::null(),
            right_relation: std::ptr::null(),
            left_keys: std::ptr::null(),
            right_keys: std::ptr::null(),
            key_count: 0,
            project_indices: std::ptr::null(),
            project_count: 0,
            filter_expr: WlFfiExprBuffer {
                data: std::ptr::null(),
                size: 0,
            },
            agg_fn: WlAggFn::Count,
            group_by_indices: std::ptr::null(),
            group_by_count: 0,
        };
        let op_consolidate = WlFfiOp {
            op: WlFfiOpType::Consolidate,
            ..op_concat
        };

        let ops = [op_concat, op_consolidate];
        let relation = WlFfiRelationPlan {
            name: rel_name.as_ptr(),
            ops: ops.as_ptr(),
            op_count: 2,
        };
        let relations = [relation];
        let stratum = WlFfiStratumPlan {
            stratum_id: 0,
            is_recursive: false,
            relations: relations.as_ptr(),
            relation_count: 1,
        };
        let strata = [stratum];
        let plan = WlFfiPlan {
            strata: strata.as_ptr(),
            stratum_count: 1,
            edb_relations: std::ptr::null(),
            edb_count: 0,
        };

        unsafe {
            let result = read_plan(&plan).unwrap();
            assert_eq!(result.strata[0].relations[0].ops[0], SafeOp::Concat);
            assert_eq!(result.strata[0].relations[0].ops[1], SafeOp::Consolidate);
        }
    }

    // ---- Multi-stratum plan (transitive closure) ----

    #[test]
    fn test_read_transitive_closure_plan() {
        // Stratum 0: tc(X,Y) :- edge(X,Y)  (non-recursive base case)
        // Stratum 1: tc(X,Z) :- tc(X,Y), edge(Y,Z)  (recursive)
        let edge_name = CString::new("edge").unwrap();
        let tc_name = CString::new("tc").unwrap();
        let edge_name2 = CString::new("edge").unwrap();
        let tc_name2 = CString::new("tc").unwrap();

        // Stratum 0: VARIABLE(edge)
        let s0_op = WlFfiOp {
            op: WlFfiOpType::Variable,
            relation_name: edge_name.as_ptr(),
            right_relation: std::ptr::null(),
            left_keys: std::ptr::null(),
            right_keys: std::ptr::null(),
            key_count: 0,
            project_indices: std::ptr::null(),
            project_count: 0,
            filter_expr: WlFfiExprBuffer {
                data: std::ptr::null(),
                size: 0,
            },
            agg_fn: WlAggFn::Count,
            group_by_indices: std::ptr::null(),
            group_by_count: 0,
        };
        let s0_ops = [s0_op];
        let s0_rel = WlFfiRelationPlan {
            name: tc_name.as_ptr(),
            ops: s0_ops.as_ptr(),
            op_count: 1,
        };
        let s0_rels = [s0_rel];

        // Stratum 1: VARIABLE(tc), JOIN(edge, Y=X)
        let lk = CString::new("Y").unwrap();
        let rk = CString::new("X").unwrap();
        let lk_ptrs = [lk.as_ptr()];
        let rk_ptrs = [rk.as_ptr()];

        let s1_op0 = WlFfiOp {
            op: WlFfiOpType::Variable,
            relation_name: tc_name2.as_ptr(),
            right_relation: std::ptr::null(),
            left_keys: std::ptr::null(),
            right_keys: std::ptr::null(),
            key_count: 0,
            project_indices: std::ptr::null(),
            project_count: 0,
            filter_expr: WlFfiExprBuffer {
                data: std::ptr::null(),
                size: 0,
            },
            agg_fn: WlAggFn::Count,
            group_by_indices: std::ptr::null(),
            group_by_count: 0,
        };
        let s1_op1 = WlFfiOp {
            op: WlFfiOpType::Join,
            relation_name: std::ptr::null(),
            right_relation: edge_name2.as_ptr(),
            left_keys: lk_ptrs.as_ptr(),
            right_keys: rk_ptrs.as_ptr(),
            key_count: 1,
            project_indices: std::ptr::null(),
            project_count: 0,
            filter_expr: WlFfiExprBuffer {
                data: std::ptr::null(),
                size: 0,
            },
            agg_fn: WlAggFn::Count,
            group_by_indices: std::ptr::null(),
            group_by_count: 0,
        };
        let s1_ops = [s1_op0, s1_op1];
        let s1_rel = WlFfiRelationPlan {
            name: tc_name2.as_ptr(),
            ops: s1_ops.as_ptr(),
            op_count: 2,
        };
        let s1_rels = [s1_rel];

        let strata = [
            WlFfiStratumPlan {
                stratum_id: 0,
                is_recursive: false,
                relations: s0_rels.as_ptr(),
                relation_count: 1,
            },
            WlFfiStratumPlan {
                stratum_id: 1,
                is_recursive: true,
                relations: s1_rels.as_ptr(),
                relation_count: 1,
            },
        ];

        let edb_name = CString::new("edge").unwrap();
        let edb_ptrs = [edb_name.as_ptr()];
        let plan = WlFfiPlan {
            strata: strata.as_ptr(),
            stratum_count: 2,
            edb_relations: edb_ptrs.as_ptr(),
            edb_count: 1,
        };

        unsafe {
            let result = read_plan(&plan).unwrap();

            assert_eq!(result.edb_relations, vec!["edge"]);
            assert_eq!(result.strata.len(), 2);

            // Stratum 0: non-recursive
            assert!(!result.strata[0].is_recursive);
            assert_eq!(result.strata[0].relations[0].name, "tc");
            assert_eq!(
                result.strata[0].relations[0].ops[0],
                SafeOp::Variable {
                    relation_name: "edge".to_string()
                }
            );

            // Stratum 1: recursive
            assert!(result.strata[1].is_recursive);
            assert_eq!(result.strata[1].relations[0].ops.len(), 2);
            assert_eq!(
                result.strata[1].relations[0].ops[1],
                SafeOp::Join {
                    right_relation: "edge".to_string(),
                    left_keys: vec!["Y".to_string()],
                    right_keys: vec!["X".to_string()],
                }
            );
        }
    }

    // ---- Error: null strata pointer with non-zero count ----

    #[test]
    fn test_read_plan_null_strata_with_count() {
        let plan = WlFfiPlan {
            strata: std::ptr::null(),
            stratum_count: 1,
            edb_relations: std::ptr::null(),
            edb_count: 0,
        };
        unsafe {
            let result = read_plan(&plan);
            assert_eq!(result, Err(PlanReadError::NullPointer("plan.strata")));
        }
    }

    // ---- Error: null relation name ----

    #[test]
    fn test_read_plan_null_relation_name() {
        let op = WlFfiOp {
            op: WlFfiOpType::Variable,
            relation_name: std::ptr::null(),
            right_relation: std::ptr::null(),
            left_keys: std::ptr::null(),
            right_keys: std::ptr::null(),
            key_count: 0,
            project_indices: std::ptr::null(),
            project_count: 0,
            filter_expr: WlFfiExprBuffer {
                data: std::ptr::null(),
                size: 0,
            },
            agg_fn: WlAggFn::Count,
            group_by_indices: std::ptr::null(),
            group_by_count: 0,
        };
        let ops = [op];

        let rel_name = CString::new("r").unwrap();
        let relation = WlFfiRelationPlan {
            name: rel_name.as_ptr(),
            ops: ops.as_ptr(),
            op_count: 1,
        };
        let relations = [relation];
        let stratum = WlFfiStratumPlan {
            stratum_id: 0,
            is_recursive: false,
            relations: relations.as_ptr(),
            relation_count: 1,
        };
        let strata = [stratum];
        let plan = WlFfiPlan {
            strata: strata.as_ptr(),
            stratum_count: 1,
            edb_relations: std::ptr::null(),
            edb_count: 0,
        };

        unsafe {
            let result = read_plan(&plan);
            assert_eq!(result, Err(PlanReadError::NullPointer("op.relation_name")));
        }
    }

    // ---- Empty filter expression (no-op) ----

    #[test]
    fn test_read_filter_empty_expr() {
        let rel_name = CString::new("r").unwrap();
        let op = WlFfiOp {
            op: WlFfiOpType::Filter,
            relation_name: std::ptr::null(),
            right_relation: std::ptr::null(),
            left_keys: std::ptr::null(),
            right_keys: std::ptr::null(),
            key_count: 0,
            project_indices: std::ptr::null(),
            project_count: 0,
            filter_expr: WlFfiExprBuffer {
                data: std::ptr::null(),
                size: 0,
            },
            agg_fn: WlAggFn::Count,
            group_by_indices: std::ptr::null(),
            group_by_count: 0,
        };
        let ops = [op];
        let relation = WlFfiRelationPlan {
            name: rel_name.as_ptr(),
            ops: ops.as_ptr(),
            op_count: 1,
        };
        let relations = [relation];
        let stratum = WlFfiStratumPlan {
            stratum_id: 0,
            is_recursive: false,
            relations: relations.as_ptr(),
            relation_count: 1,
        };
        let strata = [stratum];
        let plan = WlFfiPlan {
            strata: strata.as_ptr(),
            stratum_count: 1,
            edb_relations: std::ptr::null(),
            edb_count: 0,
        };

        unsafe {
            let result = read_plan(&plan).unwrap();
            assert_eq!(
                result.strata[0].relations[0].ops[0],
                SafeOp::Filter { expr: Vec::new() }
            );
        }
    }
}
