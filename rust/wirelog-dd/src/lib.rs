/*
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

/*
 * wirelog-dd: Rust Differential Dataflow executor for wirelog
 */

// Ensure DD crates are linked (used in dataflow module)
extern crate differential_dataflow;
extern crate timely;

mod dataflow;
mod expr;
mod ffi;
pub mod ffi_types;
mod plan_reader;

// Re-export FFI entry points (they are #[no_mangle] extern "C")
pub use ffi::*;
