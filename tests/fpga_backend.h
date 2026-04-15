/*
 * fpga_backend.h - Naive row-store "FPGA" backend for vtable validation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Test-only backend that validates the wl_compute_backend_t vtable
 * is truly pluggable.  Correctness matters, performance does not.
 */

#ifndef FPGA_BACKEND_H
#define FPGA_BACKEND_H

#include "../wirelog/backend.h"

/**
 * wl_backend_fpga:
 *
 * Obtain the singleton static vtable instance for the naive row-store
 * FPGA test backend.
 *
 * Returns: Pointer to the FPGA compute backend vtable.
 */
const wl_compute_backend_t *wl_backend_fpga(void);

#endif /* FPGA_BACKEND_H */
