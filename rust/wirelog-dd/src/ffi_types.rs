/*
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

/*
 * ffi_types.rs - Rust mirrors of wirelog/ffi/dd_ffi.h types
 *
 * These #[repr(C)] types exactly match the C structs defined in dd_ffi.h.
 * The Rust side receives const pointers to these types from C and must
 * NOT free, modify, or retain any pointer beyond the FFI call duration.
 *
 * Layout contract: every struct here must have identical size and
 * alignment to its C counterpart.  Tests verify this with compile-time
 * and run-time assertions.
 */

use std::os::raw::c_char;

/* ======================================================================== */
/* Expression Tag Enum                                                      */
/* ======================================================================== */

/// Opcodes for serialized postfix expression encoding.
/// Mirrors `wl_ffi_expr_tag_t` from dd_ffi.h.
///
/// Stored as u8 in the expression buffer (not u32), but the C enum
/// is `int`-sized.  We define this as repr(u8) for buffer parsing;
/// the C enum is only used for documentation, not in struct layouts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WlFfiExprTag {
    Var = 0x01,
    ConstInt = 0x02,
    ConstStr = 0x03,
    Bool = 0x04,

    ArithAdd = 0x10,
    ArithSub = 0x11,
    ArithMul = 0x12,
    ArithDiv = 0x13,
    ArithMod = 0x14,

    CmpEq = 0x20,
    CmpNeq = 0x21,
    CmpLt = 0x22,
    CmpGt = 0x23,
    CmpLte = 0x24,
    CmpGte = 0x25,

    AggCount = 0x30,
    AggSum = 0x31,
    AggMin = 0x32,
    AggMax = 0x33,
}

impl WlFfiExprTag {
    /// Try to convert a raw u8 byte to an expression tag.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::Var),
            0x02 => Some(Self::ConstInt),
            0x03 => Some(Self::ConstStr),
            0x04 => Some(Self::Bool),
            0x10 => Some(Self::ArithAdd),
            0x11 => Some(Self::ArithSub),
            0x12 => Some(Self::ArithMul),
            0x13 => Some(Self::ArithDiv),
            0x14 => Some(Self::ArithMod),
            0x20 => Some(Self::CmpEq),
            0x21 => Some(Self::CmpNeq),
            0x22 => Some(Self::CmpLt),
            0x23 => Some(Self::CmpGt),
            0x24 => Some(Self::CmpLte),
            0x25 => Some(Self::CmpGte),
            0x30 => Some(Self::AggCount),
            0x31 => Some(Self::AggSum),
            0x32 => Some(Self::AggMin),
            0x33 => Some(Self::AggMax),
            _ => None,
        }
    }
}

/* ======================================================================== */
/* Serialized Expression Buffer                                             */
/* ======================================================================== */

/// Flat byte buffer containing a serialized RPN expression.
/// Mirrors `wl_ffi_expr_buffer_t` from dd_ffi.h.
///
/// - `data`: Pointer to byte buffer (owned by C, Rust borrows).
///   NULL if no expression.
/// - `size`: Number of bytes.  0 if no expression.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct WlFfiExprBuffer {
    pub data: *const u8,
    pub size: u32,
}

/* ======================================================================== */
/* Operator Type Enum                                                       */
/* ======================================================================== */

/// FFI-safe operator type.  Mirrors `wl_ffi_op_type_t` from dd_ffi.h.
/// Uses explicit integer values for ABI stability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum WlFfiOpType {
    Variable = 0,
    Map = 1,
    Filter = 2,
    Join = 3,
    Antijoin = 4,
    Reduce = 5,
    Concat = 6,
    Consolidate = 7,
    Semijoin = 8,
}

/* ======================================================================== */
/* Aggregation Function Enum                                                */
/* ======================================================================== */

/// Aggregation function type.  Mirrors `wl_agg_fn_t` from wirelog-types.h.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum WlAggFn {
    Count = 0,
    Sum = 1,
    Min = 2,
    Max = 3,
    Avg = 4,
}

/* ======================================================================== */
/* FFI Operator Node                                                        */
/* ======================================================================== */

/// FFI-safe flat operator descriptor.
/// Mirrors `wl_ffi_op_t` from dd_ffi.h.
///
/// Field usage by operator type:
///   VARIABLE:    relation_name
///   MAP:         project_indices, project_count
///   FILTER:      filter_expr
///   JOIN:        right_relation, left_keys, right_keys, key_count
///   ANTIJOIN:    right_relation, left_keys, right_keys, key_count
///   REDUCE:      agg_fn, group_by_indices, group_by_count
///   CONCAT:      (no fields)
///   CONSOLIDATE: (no fields)
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct WlFfiOp {
    pub op: WlFfiOpType,

    pub relation_name: *const c_char,
    pub right_relation: *const c_char,

    pub left_keys: *const *const c_char,
    pub right_keys: *const *const c_char,
    pub key_count: u32,

    pub project_indices: *const u32,
    pub project_count: u32,

    pub filter_expr: WlFfiExprBuffer,

    pub agg_fn: WlAggFn,
    pub group_by_indices: *const u32,
    pub group_by_count: u32,

    pub map_exprs: *const WlFfiExprBuffer,
    pub map_expr_count: u32,
}

/* ======================================================================== */
/* FFI Per-Relation Plan                                                    */
/* ======================================================================== */

/// FFI-safe operator sequence for a single IDB relation.
/// Mirrors `wl_ffi_relation_plan_t` from dd_ffi.h.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct WlFfiRelationPlan {
    pub name: *const c_char,
    pub ops: *const WlFfiOp,
    pub op_count: u32,
}

/* ======================================================================== */
/* FFI Stratum Plan                                                         */
/* ======================================================================== */

/// FFI-safe execution plan for a single stratum.
/// Mirrors `wl_ffi_stratum_plan_t` from dd_ffi.h.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct WlFfiStratumPlan {
    pub stratum_id: u32,
    pub is_recursive: bool,
    pub relations: *const WlFfiRelationPlan,
    pub relation_count: u32,
}

/* ======================================================================== */
/* FFI Full Plan                                                            */
/* ======================================================================== */

/// FFI-safe complete DD execution plan for a stratified program.
/// Mirrors `wl_ffi_plan_t` from dd_ffi.h.
/// This is the top-level structure passed across the C-to-Rust boundary.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct WlFfiPlan {
    pub strata: *const WlFfiStratumPlan,
    pub stratum_count: u32,
    pub edb_relations: *const *const c_char,
    pub edb_count: u32,
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    // ---- Expression tag byte values ----

    #[test]
    fn test_expr_tag_values() {
        assert_eq!(WlFfiExprTag::Var as u8, 0x01);
        assert_eq!(WlFfiExprTag::ConstInt as u8, 0x02);
        assert_eq!(WlFfiExprTag::ConstStr as u8, 0x03);
        assert_eq!(WlFfiExprTag::Bool as u8, 0x04);

        assert_eq!(WlFfiExprTag::ArithAdd as u8, 0x10);
        assert_eq!(WlFfiExprTag::ArithSub as u8, 0x11);
        assert_eq!(WlFfiExprTag::ArithMul as u8, 0x12);
        assert_eq!(WlFfiExprTag::ArithDiv as u8, 0x13);
        assert_eq!(WlFfiExprTag::ArithMod as u8, 0x14);

        assert_eq!(WlFfiExprTag::CmpEq as u8, 0x20);
        assert_eq!(WlFfiExprTag::CmpNeq as u8, 0x21);
        assert_eq!(WlFfiExprTag::CmpLt as u8, 0x22);
        assert_eq!(WlFfiExprTag::CmpGt as u8, 0x23);
        assert_eq!(WlFfiExprTag::CmpLte as u8, 0x24);
        assert_eq!(WlFfiExprTag::CmpGte as u8, 0x25);

        assert_eq!(WlFfiExprTag::AggCount as u8, 0x30);
        assert_eq!(WlFfiExprTag::AggSum as u8, 0x31);
        assert_eq!(WlFfiExprTag::AggMin as u8, 0x32);
        assert_eq!(WlFfiExprTag::AggMax as u8, 0x33);
    }

    #[test]
    fn test_expr_tag_from_byte_valid() {
        assert_eq!(WlFfiExprTag::from_byte(0x01), Some(WlFfiExprTag::Var));
        assert_eq!(WlFfiExprTag::from_byte(0x23), Some(WlFfiExprTag::CmpGt));
        assert_eq!(WlFfiExprTag::from_byte(0x33), Some(WlFfiExprTag::AggMax));
    }

    #[test]
    fn test_expr_tag_from_byte_invalid() {
        assert_eq!(WlFfiExprTag::from_byte(0x00), None);
        assert_eq!(WlFfiExprTag::from_byte(0x05), None);
        assert_eq!(WlFfiExprTag::from_byte(0xFF), None);
    }

    // ---- Operator type enum values ----

    #[test]
    fn test_op_type_values() {
        assert_eq!(WlFfiOpType::Variable as u32, 0);
        assert_eq!(WlFfiOpType::Map as u32, 1);
        assert_eq!(WlFfiOpType::Filter as u32, 2);
        assert_eq!(WlFfiOpType::Join as u32, 3);
        assert_eq!(WlFfiOpType::Antijoin as u32, 4);
        assert_eq!(WlFfiOpType::Reduce as u32, 5);
        assert_eq!(WlFfiOpType::Concat as u32, 6);
        assert_eq!(WlFfiOpType::Consolidate as u32, 7);
        assert_eq!(WlFfiOpType::Semijoin as u32, 8);
    }

    // ---- Agg function enum values ----

    #[test]
    fn test_agg_fn_values() {
        assert_eq!(WlAggFn::Count as u32, 0);
        assert_eq!(WlAggFn::Sum as u32, 1);
        assert_eq!(WlAggFn::Min as u32, 2);
        assert_eq!(WlAggFn::Max as u32, 3);
        assert_eq!(WlAggFn::Avg as u32, 4);
    }

    // ---- Expression buffer layout ----

    #[test]
    fn test_expr_buffer_size() {
        // data: *const u8 (8 bytes on 64-bit) + size: u32 (4 bytes)
        // Alignment: pointer (8), so total = 8 + 4 = 12, padded to 16
        let size = mem::size_of::<WlFfiExprBuffer>();
        assert_eq!(size, 16, "WlFfiExprBuffer size mismatch");
    }

    #[test]
    fn test_expr_buffer_align() {
        let align = mem::align_of::<WlFfiExprBuffer>();
        assert_eq!(align, 8, "WlFfiExprBuffer align mismatch");
    }

    // ---- Relation plan layout ----

    #[test]
    fn test_relation_plan_size() {
        // name: *const c_char (8) + ops: *const WlFfiOp (8) + op_count: u32 (4)
        // Padded to 24 (align 8)
        let size = mem::size_of::<WlFfiRelationPlan>();
        assert_eq!(size, 24, "WlFfiRelationPlan size mismatch");
    }

    #[test]
    fn test_relation_plan_align() {
        let align = mem::align_of::<WlFfiRelationPlan>();
        assert_eq!(align, 8, "WlFfiRelationPlan align mismatch");
    }

    // ---- Stratum plan layout ----

    #[test]
    fn test_stratum_plan_size() {
        // stratum_id: u32 (4) + is_recursive: bool (1) + padding (3)
        // + relations: *const (8) + relation_count: u32 (4) + padding (4)
        // = 24
        let size = mem::size_of::<WlFfiStratumPlan>();
        assert_eq!(size, 24, "WlFfiStratumPlan size mismatch");
    }

    #[test]
    fn test_stratum_plan_align() {
        let align = mem::align_of::<WlFfiStratumPlan>();
        assert_eq!(align, 8, "WlFfiStratumPlan align mismatch");
    }

    // ---- Full plan layout ----

    #[test]
    fn test_plan_size() {
        // strata: *const (8) + stratum_count: u32 (4) + padding (4)
        // + edb_relations: *const (8) + edb_count: u32 (4) + padding (4)
        // = 32
        let size = mem::size_of::<WlFfiPlan>();
        assert_eq!(size, 32, "WlFfiPlan size mismatch");
    }

    #[test]
    fn test_plan_align() {
        let align = mem::align_of::<WlFfiPlan>();
        assert_eq!(align, 8, "WlFfiPlan align mismatch");
    }

    // ---- Op struct layout (the biggest one) ----

    #[test]
    fn test_op_align() {
        let align = mem::align_of::<WlFfiOp>();
        assert_eq!(align, 8, "WlFfiOp align mismatch");
    }

    #[test]
    fn test_op_field_offsets() {
        // Verify that Rust and C field ordering produces consistent offsets.
        // We use a zeroed struct and check pointer arithmetic.
        let op = unsafe { mem::zeroed::<WlFfiOp>() };
        let base = &op as *const WlFfiOp as usize;

        let op_offset = &op.op as *const WlFfiOpType as usize - base;
        assert_eq!(op_offset, 0, "op field offset");

        let rn_offset = &op.relation_name as *const *const c_char as usize - base;
        assert_eq!(rn_offset, 8, "relation_name field offset");

        let rr_offset = &op.right_relation as *const *const c_char as usize - base;
        assert_eq!(rr_offset, 16, "right_relation field offset");

        let lk_offset = &op.left_keys as *const *const *const c_char as usize - base;
        assert_eq!(lk_offset, 24, "left_keys field offset");

        let rk_offset = &op.right_keys as *const *const *const c_char as usize - base;
        assert_eq!(rk_offset, 32, "right_keys field offset");

        let kc_offset = &op.key_count as *const u32 as usize - base;
        assert_eq!(kc_offset, 40, "key_count field offset");

        let pi_offset = &op.project_indices as *const *const u32 as usize - base;
        assert_eq!(pi_offset, 48, "project_indices field offset");

        let pc_offset = &op.project_count as *const u32 as usize - base;
        assert_eq!(pc_offset, 56, "project_count field offset");

        let fe_offset = &op.filter_expr as *const WlFfiExprBuffer as usize - base;
        assert_eq!(fe_offset, 64, "filter_expr field offset");

        let af_offset = &op.agg_fn as *const WlAggFn as usize - base;
        assert_eq!(af_offset, 80, "agg_fn field offset");

        let gbi_offset = &op.group_by_indices as *const *const u32 as usize - base;
        assert_eq!(gbi_offset, 88, "group_by_indices field offset");

        let gbc_offset = &op.group_by_count as *const u32 as usize - base;
        assert_eq!(gbc_offset, 96, "group_by_count field offset");

        let me_offset = &op.map_exprs as *const *const WlFfiExprBuffer as usize - base;
        assert_eq!(me_offset, 104, "map_exprs field offset");

        let mec_offset = &op.map_expr_count as *const u32 as usize - base;
        assert_eq!(mec_offset, 112, "map_expr_count field offset");
    }

    #[test]
    fn test_op_size() {
        // Total: 112 + 4 (map_expr_count) = 116, padded to 120 (align 8)
        let size = mem::size_of::<WlFfiOp>();
        assert_eq!(size, 120, "WlFfiOp size mismatch");
    }
}
