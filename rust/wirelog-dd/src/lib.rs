/*
 * wirelog-dd: Rust Differential Dataflow executor for wirelog
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#[allow(dead_code)] // Building blocks for dataflow execution
mod dataflow;
mod expr;
mod ffi;
pub mod ffi_types;
#[allow(dead_code)] // Used by dataflow execution
mod plan_reader;

// Re-export FFI entry points (they are #[no_mangle] extern "C")
pub use ffi::*;
