/*
 * session_delta_callback.c - delta callback session context
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "session.h"

#if defined(_MSC_VER)
static __declspec(thread) wl_session_t *wl_active_delta_callback_session;
#else
static _Thread_local wl_session_t *wl_active_delta_callback_session;
#endif

wl_session_t *
wl_session_delta_callback_enter(wl_session_t *session)
{
    wl_session_t *previous = wl_active_delta_callback_session;
    wl_active_delta_callback_session = session;
    return previous;
}

void
wl_session_delta_callback_leave(wl_session_t *previous)
{
    wl_active_delta_callback_session = previous;
}

bool
wl_session_delta_callback_is_current(const wl_session_t *session)
{
    return session && wl_active_delta_callback_session == session;
}
