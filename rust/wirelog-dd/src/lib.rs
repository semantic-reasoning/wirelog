/*
 * wirelog-dd: Rust Differential Dataflow executor for wirelog
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

mod ffi;
pub mod ffi_types;

// Re-export FFI entry points (they are #[no_mangle] extern "C")
pub use ffi::*;
