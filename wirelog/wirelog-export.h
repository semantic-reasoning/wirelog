/*
 * wirelog-export.h - Symbol visibility macros
 *
 * Copyright (C) CleverPlant
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Licensed under LGPL-3.0-or-later
 */

/**
 * @file wirelog-export.h
 * @brief Public-symbol visibility and deprecation attribute macros.
 */

#ifndef WIRELOG_EXPORT_H
#define WIRELOG_EXPORT_H

#if defined(_WIN32) || defined(__CYGWIN__)
  #if defined(WIRELOG_STATIC)
    #define WIRELOG_PUBLIC
  #elif defined(WIRELOG_BUILDING)
    #define WIRELOG_PUBLIC __declspec(dllexport)
  #else
    #define WIRELOG_PUBLIC __declspec(dllimport)
  #endif
#elif defined(__GNUC__) && __GNUC__ >= 4
  #define WIRELOG_PUBLIC __attribute__((visibility("default")))
#else
  #define WIRELOG_PUBLIC
#endif

/* Public-API attribute macro.  WIRELOG_API is the canonical name for
 * the public-symbol visibility attribute and is the form used on
 * every prototype in the installed public headers.  The underlying
 * WIRELOG_PUBLIC macro is retained as a backward-compat alias for
 * downstream code that referenced it directly during the v0.40 cycle
 * (see docs/MIGRATION.md 0.30 -> 1.0). */
#define WIRELOG_API WIRELOG_PUBLIC

/* WIRELOG_DEPRECATED_SINCE(major, minor): annotate APIs scheduled for
 * removal.  Expands to a compile-time deprecation warning on
 * GCC / Clang / MSVC; a no-op on unknown compilers. */
#if defined(__GNUC__) || defined(__clang__)
  #define WIRELOG_DEPRECATED_SINCE(maj, min) \
          __attribute__((deprecated("deprecated since " #maj "." #min)))
#elif defined(_MSC_VER)
  #define WIRELOG_DEPRECATED_SINCE(maj, min) \
          __declspec(deprecated("deprecated since " #maj "." #min))
#else
  #define WIRELOG_DEPRECATED_SINCE(maj, min)
#endif

#endif /* WIRELOG_EXPORT_H */
