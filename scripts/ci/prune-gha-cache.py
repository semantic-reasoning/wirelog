#!/usr/bin/env python3
"""Keep the repository's GitHub Actions cache inside GitHub's 10 GiB quota.

Background (issue #1571, all numbers measured on 2026-09-12):
  * PR-CI sccache jobs: 23.7-40.7% hit rate, 8,597-11,066 cache-WRITE
    errors per job, 0 read errors, 0 timeouts (e.g. run 34685582951).
  * Full cache listing: 170,580 entries totalling 10.08 GiB, i.e. AT
    OVER GitHub's 10 GiB per-repository Actions cache quota.  Once the
    quota is full, every new cache write fails; reads and builds still
    work (sccache is best-effort).  That is the exact failure mode in
    #1571 (PR #1533: 10,481 write errors).
  * Every entry had been accessed within the previous ~2 days, so a
    "delete entries older than N days" policy deletes nothing.  The
    correct policy is LRU quota management.

What this does:
  Lists the repository's Actions caches (newest-accessed first), and when
  the total size exceeds --target-bytes (default 8 GiB = 80% of the
  quota), deletes the OLDEST-accessed entries first until the total is
  back under the target.  Safety rails:
    * entries accessed within --min-age-minutes (default 60) are never
      deleted, protecting caches in use by a job that is running now;
    * at most --max-deletes entries per run, and deletes are paced at
      ~0.85 s apart (~4,200/hour), under the 5,000/hour core rate limit;
    * HTTP 401/403 aborts the run (clean, non-zero exit, clear message);
      HTTP 429 aborts with "re-run later"; HTTP 404 is treated as
      success (the entry was already gone).
  Runs are idempotent: each run only deletes what is needed to get back
  under the target, so scheduled runs converge and then no-op.

Usage:
  python3 scripts/ci/prune-gha-cache.py --repo owner/name [--dry-run]
  (token from --token or $GITHUB_TOKEN; only used as the Authorization
  header, never printed)
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

DEFAULT_API_BASE = "https://api.github.com"
DEFAULT_TARGET_BYTES = 8 * 1024 ** 3  # 80% of the 10 GiB quota
DEFAULT_MIN_AGE_MINUTES = 60
DEFAULT_MAX_DELETES = 20000
PAGE_SIZE = 100
# Pacing stays under the 5,000/hour core rate limit with margin.
LIST_PAGE_PAUSE_S = 0.1
DELETE_PAUSE_S = 0.85
PROGRESS_EVERY = 200


def parse_ts(value):
    # GitHub timestamps: 2026-09-12T11:35:17.683701Z
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def make_caller(api_base, token):
    def call(method, path, body=None):
        url = api_base.rstrip("/") + path
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        }
        data = body
        if body is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
                code = resp.status
        except urllib.error.HTTPError as err:
            raw = err.read()
            code = err.code
        text = raw.decode("utf-8", "replace")
        try:
            payload = json.loads(text) if text else None
        except json.JSONDecodeError:
            payload = text
        return code, payload

    return call


def progress(msg):
    print(f"  [prune] {msg}", flush=True)


def list_caches(call, repo, max_pages):
    """Return (entries, api_total_count, truncated).  Aborts on 403/429."""
    entries = []
    api_total = None
    for page in range(1, max_pages + 1):
        path = f"/repos/{repo}/actions/caches?per_page={PAGE_SIZE}"
        if page > 1:
            path += f"&page={page}"
        code, payload = call("GET", path)
        if code == 429:
            raise RuntimeError("rate limited while listing caches (HTTP 429); re-run later")
        if code == 403:
            raise PermissionError(f"listing caches failed: HTTP 403 (no actions:read?)")
        if code != 200 or not isinstance(payload, dict):
            raise RuntimeError(f"listing caches failed: HTTP {code}: {payload!r}")
        if api_total is None:
            api_total = payload.get("total_count")
        batch = payload.get("actions_caches", [])
        entries.extend(batch)
        if len(batch) < PAGE_SIZE:
            return entries, api_total, False
        time.sleep(LIST_PAGE_PAUSE_S)
    return entries, api_total, True


def select_entries(entries, now, target_bytes, min_age_minutes, max_deletes):
    """Pick the oldest-accessed entries to delete to reach the target.

    Returns (to_delete, total_bytes, under_target_after, min_age_blocked).
    """
    total = sum(e.get("size_in_bytes", 0) for e in entries)
    to_free = total - target_bytes
    if to_free <= 0:
        return [], total, True, False

    # Oldest-accessed first (the API already lists newest-first; sort
    # defensively so selection is correct regardless of server ordering).
    ordered = sorted(entries, key=lambda e: (parse_ts(e["last_accessed_at"]), e["id"]))
    cutoff = now.timestamp() - min_age_minutes * 60
    chosen = []
    freed = 0
    min_age_blocked = False
    for e in ordered:
        if len(chosen) >= max_deletes:
            break
        if parse_ts(e["last_accessed_at"]).timestamp() < cutoff:
            chosen.append(e)
            freed += e.get("size_in_bytes", 0)
            if freed >= to_free:
                break
        else:
            # This (newer) entry is protected; every remaining entry in
            # this order is newer too, so stop: deleting further would
            # breach the min-age floor.
            min_age_blocked = True
            break
    under_after = (total - freed) <= target_bytes
    return chosen, total, under_after, min_age_blocked


def main(argv=None):
    parser = argparse.ArgumentParser(description="Keep the Actions cache under quota (LRU)")
    parser.add_argument("--repo", required=True, help="owner/name")
    parser.add_argument("--token", default=None, help="default: $GITHUB_TOKEN")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--target-bytes", type=int, default=DEFAULT_TARGET_BYTES)
    parser.add_argument("--min-age-minutes", type=int, default=DEFAULT_MIN_AGE_MINUTES)
    parser.add_argument("--max-deletes", type=int, default=DEFAULT_MAX_DELETES)
    parser.add_argument("--max-list-pages", type=int, default=2000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    token = args.token or os.environ.get("GITHUB_TOKEN")
    if not token:
        print("error: no token (pass --token or set GITHUB_TOKEN)", file=sys.stderr)
        return 2

    call = make_caller(args.api_base, token)
    now = datetime.now(timezone.utc)

    progress(f"repo={args.repo} target={args.target_bytes} "
             f"min_age_min={args.min_age_minutes} max_deletes={args.max_deletes} "
             f"dry_run={args.dry_run}")
    try:
        entries, api_total, truncated = list_caches(call, args.repo, args.max_list_pages)
    except (PermissionError, RuntimeError) as err:
        progress(f"aborted: {err}")
        return 1
    if api_total is not None and api_total > len(entries):
        progress(f"note: API reports {api_total} entries; listed {len(entries)} "
                 "(list capped or truncated)")

    to_delete, total, under_after, min_age_blocked = select_entries(
        entries, now, args.target_bytes, args.min_age_minutes, args.max_deletes)

    if args.dry_run:
        report = {
            "mode": "dry-run",
            "listed_entries": len(entries),
            "api_total_count": api_total,
            "total_bytes": total,
            "total_gib": round(total / 2 ** 30, 2),
            "target_bytes": args.target_bytes,
            "would_delete": len(to_delete),
            "would_delete_bytes": sum(e.get("size_in_bytes", 0) for e in to_delete),
            "under_target_after": under_after,
            "min_age_blocked": min_age_blocked,
            "truncated": truncated,
        }
        print(json.dumps(report, indent=2))
        return 0

    if not to_delete:
        report = {
            "mode": "no-op",
            "listed_entries": len(entries),
            "api_total_count": api_total,
            "total_bytes": total,
            "total_gib": round(total / 2 ** 30, 2),
            "target_bytes": args.target_bytes,
            "min_age_blocked": min_age_blocked,
            "note": ("total already under target"
                     if under_after else
                     "all excess entries are within the min-age floor; "
                     "re-run later"),
            "truncated": truncated,
        }
        print(json.dumps(report, indent=2))
        return 0

    deleted = 0
    already_gone = 0
    failed = 0
    freed_bytes = 0
    aborted = None
    started = time.monotonic()
    for i, entry in enumerate(to_delete):
        code, payload = call("DELETE", f"/repos/{args.repo}/actions/caches/{entry['id']}")
        if code in (200, 204):
            deleted += 1
            freed_bytes += entry.get("size_in_bytes", 0)
        elif code == 404:
            already_gone += 1
            freed_bytes += entry.get("size_in_bytes", 0)
        elif code in (401, 403):
            aborted = f"permission denied (HTTP {code}); stopping"
            break
        elif code == 429:
            aborted = "rate limited (HTTP 429); stopping, re-run later"
            break
        else:
            failed += 1
            progress(f"delete failed HTTP {code} id={entry.get('id')}: {payload!r}")
        if (i + 1) % PROGRESS_EVERY == 0:
            progress(f"progress {i + 1}/{len(to_delete)} deleted={deleted} "
                     f"gone={already_gone} failed={failed} "
                     f"elapsed={int(time.monotonic() - started)}s")
        if i + 1 < len(to_delete):
            time.sleep(DELETE_PAUSE_S)

    remaining = max(total - freed_bytes, 0)
    report = {
        "mode": "prune",
        "listed_entries": len(entries),
        "api_total_count": api_total,
        "total_bytes": total,
        "target_bytes": args.target_bytes,
        "deleted": deleted,
        "already_gone": already_gone,
        "failed": failed,
        "freed_bytes": freed_bytes,
        "freed_gib": round(freed_bytes / 2 ** 30, 2),
        "remaining_est_bytes": remaining,
        "remaining_est_gib": round(remaining / 2 ** 30, 2),
        "aborted": aborted,
        "min_age_blocked": min_age_blocked,
        "truncated": truncated,
    }
    print(json.dumps(report, indent=2))
    return 1 if aborted is not None else 0


if __name__ == "__main__":
    sys.exit(main())
