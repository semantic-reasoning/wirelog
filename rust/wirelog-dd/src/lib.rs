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
mod session;

// Re-export FFI entry points (they are #[no_mangle] extern "C")
pub use ffi::*;

#[cfg(test)]
mod debug_tc_plan {
    use crate::ffi::*;
    use crate::plan_reader::*;

    #[test]
    fn print_tc_plan_structure() {
        // We can't easily call the C parser from Rust tests, but let's look at what
        // we can infer from the existing test structures
    }
}
