/*
 * wirelog-export.h - Symbol visibility macros
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#ifndef WIRELOG_EXPORT_H
#define WIRELOG_EXPORT_H

#if defined(_WIN32) || defined(__CYGWIN__)
  #ifdef WIRELOG_BUILDING
    #define WL_PUBLIC __declspec(dllexport)
  #else
    #define WL_PUBLIC __declspec(dllimport)
  #endif
#elif defined(__GNUC__) && __GNUC__ >= 4
  #define WL_PUBLIC __attribute__((visibility("default")))
#else
  #define WL_PUBLIC
#endif

#endif /* WIRELOG_EXPORT_H */
