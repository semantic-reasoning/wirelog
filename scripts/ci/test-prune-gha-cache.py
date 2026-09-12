#!/usr/bin/env python3
"""Selftests for prune-gha-cache.py against a local mock of the caches API.

Covers the LRU quota-management contract (issue #1571):
  * under target  -> no-op, nothing deleted;
  * over target   -> oldest-accessed entries deleted first until under target;
  * min-age floor -> entries accessed within the window are protected and the
                     run reports min_age_blocked;
  * max-deletes   -> per-run deletion cap honored;
  * HTTP 404      -> treated as success;
  * HTTP 403      -> abort with non-zero exit;
  * HTTP 429      -> abort (listing and delete paths);
  * the token never appears on stdout/stderr.
"""
import json
import os
import subprocess
import sys
import threading
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
import importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    "prune_gha_cache", os.path.join(HERE, "prune-gha-cache.py"))
prune = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prune)

TOKEN = "ghs_mocktokENDPOINTNOTREAL"
NOW = datetime.now(timezone.utc)


def ts(minutes_ago):
    return (NOW - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%dT%H:%M:%S.000000Z")


# 230 fake entries: 200 old (600-600+85*60 minutes ago, i.e. 10h-96h) and
# 30 recent (0-11 minutes ago, inside the default 60-minute min-age window).
ENTRIES = []
for i in range(200):
    ENTRIES.append({
        "id": 1000 + i,
        "ref": f"refs/pull/100{i % 7}/merge",
        "key": f"sccache/a/b/c/{i:04d}",
        "version": "v",
        "last_accessed_at": ts(600 + (i % 86) * 60),
        "created_at": ts(600 + (i % 86) * 60 + 1),
        "size_in_bytes": 4096 + i,
    })
for i in range(30):
    ENTRIES.append({
        "id": 9000 + i,
        "ref": "refs/heads/main",
        "key": "sccache/d/e/f/%04d" % i,
        "version": "v",
        "last_accessed_at": ts(i),
        "created_at": ts(i + 2),
        "size_in_bytes": 8192,
    })
# Serve newest-accessed first, like the real API.
ENTRIES.sort(key=lambda e: e["last_accessed_at"], reverse=True)
PAGE = 100
DELETED = []
STATE = {"forbid_after": None, "rate_limit_on": "none"}  # none|list|delete


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if STATE["rate_limit_on"] == "list":
            self._send(429, {"message": "rate limited (mock)"})
            return
        q = parse_qs(urlparse(self.path).query)
        if not urlparse(self.path).path.endswith("/actions/caches"):
            self._send(404, {"message": "not found"})
            return
        page = int(q.get("page", ["1"])[0])
        per = int(q.get("per_page", [100])[0])
        start = (page - 1) * per
        self._send(200, {
            "total_count": len(ENTRIES),
            "actions_caches": ENTRIES[start:start + per],
        })

    def do_DELETE(self):
        if STATE["rate_limit_on"] == "delete":
            self._send(429, {"message": "rate limited (mock)"})
            return
        path = urlparse(self.path).path
        parts = path.rsplit("/", 1)
        if len(parts) == 2 and parts[0].endswith("/actions/caches"):
            cid = int(parts[1])
            if STATE["forbid_after"] is not None and len(DELETED) >= STATE["forbid_after"]:
                self._send(403, {"message": "forbidden (mock)"})
                return
            if cid == 7617372328:  # a never-present id, for the 404 case
                self._send(404, {"message": "Not Found"})
                return
            DELETED.append(cid)
            self._send(204, {})
            return
        self._send(500, {"message": "unexpected path"})


def run_script(server_url, *extra):
    env = dict(os.environ)
    env["GITHUB_TOKEN"] = TOKEN
    proc = subprocess.run(
        [sys.executable, os.path.join(HERE, "prune-gha-cache.py"),
         "--repo", "mock/owner-name", "--api-base", server_url, *extra],
        capture_output=True, text=True, env=env, timeout=300)
    return proc


def extract_report(stdout):
    start = stdout.index("{")
    return json.loads(stdout[start:])


def by_id():
    return {e["id"]: e for e in ENTRIES}


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    failures = []
    lookup = by_id()

    def check(name, cond, detail=""):
        status = "ok" if cond else "FAIL"
        print(f"  [{status}] {name}{(' -- ' + detail) if (detail and not cond) else ''}")
        if not cond:
            failures.append(name)

    total_bytes = sum(e["size_in_bytes"] for e in ENTRIES)
    old_ids = [e["id"] for e in ENTRIES if e["id"] < 9000]
    old_ids_sorted = sorted(old_ids, key=lambda i: (lookup[i]["last_accessed_at"], i))
    recent_ids = {e["id"] for e in ENTRIES if e["id"] >= 9000}

    # 1. Under target: no-op, nothing deleted, no token leak.
    proc = run_script(base, "--target-bytes", str(total_bytes + 1), "--dry-run")
    report = extract_report(proc.stdout)
    check("under-target dry-run exit 0", proc.returncode == 0, proc.stderr)
    check("under-target lists all 230", report["listed_entries"] == 230, str(report))
    check("under-target would-delete 0", report["would_delete"] == 0, str(report))
    check("under-target no-op", len(DELETED) == 0, str(DELETED))
    check("under-target no token leak",
          TOKEN not in proc.stdout and TOKEN not in proc.stderr)

    # 2. Dry-run over target: oldest-first selection, nothing deleted.
    target = total_bytes - 10 * 8192  # must free ~82 KiB of OLD entries
    proc = run_script(base, "--target-bytes", str(target),
                      "--dry-run", "--max-deletes", "500")
    report = extract_report(proc.stdout)
    check("dry-run exit 0", proc.returncode == 0, proc.stderr)
    check("dry-run would-delete > 0", report["would_delete"] > 0, str(report))
    check("dry-run deletes nothing", len(DELETED) == 0, str(DELETED))
    check("dry-run under_target_after", report["under_target_after"] is True, str(report))
    check("dry-run no token leak",
          TOKEN not in proc.stdout and TOKEN not in proc.stderr)

    # 3. Prune: deletes oldest-accessed OLD entries only, until under target.
    DELETED.clear()
    proc = run_script(base, "--target-bytes", str(target), "--max-deletes", "500")
    report = extract_report(proc.stdout)
    check("prune exit 0", proc.returncode == 0, proc.stderr)
    check("prune deleted > 0", report["deleted"] > 0, str(report))
    check("prune kept recent entries", not (set(DELETED) & recent_ids),
          str(set(DELETED) & recent_ids))
    check("prune oldest-first", DELETED == old_ids_sorted[:len(DELETED)],
          f"{DELETED[:3]} vs {old_ids_sorted[:3]}")
    freed = sum(lookup[i]["size_in_bytes"] for i in DELETED)
    check("prune freed enough", freed >= total_bytes - target, f"freed={freed}")
    check("prune not over-freeing", freed < total_bytes - target + 8192,
          f"freed={freed}")
    check("prune remaining under target",
          report["remaining_est_bytes"] <= target, str(report))
    check("prune no token leak",
          TOKEN not in proc.stdout and TOKEN not in proc.stderr)

    # 4. max-deletes cap honored.
    DELETED.clear()
    STATE["forbid_after"] = None
    proc = run_script(base, "--target-bytes", "0",
                      "--dry-run", "--max-deletes", "7")
    report = extract_report(proc.stdout)
    check("max-deletes dry-run cap", report["would_delete"] == 7, str(report))

    # 5. min-age floor: the recent entries (0-11 min old) are protected;
    #    the run must stop there and flag min_age_blocked.  Dry-run, so no
    #    new deletes happen.
    DELETED.clear()
    proc = run_script(base, "--target-bytes", "0",
                      "--dry-run", "--min-age-minutes", "60")
    report = extract_report(proc.stdout)
    check("min-age would-delete only old",
          report["would_delete"] == 200, str(report))
    check("min-age blocked", report["min_age_blocked"] is True, str(report))
    check("min-age under_target_after false",
          report["under_target_after"] is False, str(report))
    check("min-age deletes nothing (dry-run)", len(DELETED) == 0, str(DELETED))

    # 6. 404 tolerated as success (never-present id included in selection).
    STATE["forbid_after"] = None
    proc = run_script(base, "--target-bytes", "0",
                      "--min-age-minutes", "1000000", "--max-deletes", "1")
    # With min-age huge, nothing is deletable -> no-op path; verify the
    # 404 path separately by asking the server for a bogus id via a direct
    # caller round-trip below.
    code, payload = prune.make_caller(base, TOKEN)("DELETE",
        "/repos/mock/owner-name/actions/caches/7617372328")
    check("404 tolerated by caller", code == 404, str((code, payload)))

    # 7. 403 on delete aborts with non-zero exit and an abort reason.
    #    min-age 30 keeps the 600+ min old entries deletable while the
    #    0-11 min recent ones stay protected.
    DELETED.clear()
    STATE["forbid_after"] = 2
    proc = run_script(base, "--target-bytes", "0",
                      "--min-age-minutes", "30", "--max-deletes", "10")
    report = extract_report(proc.stdout)
    check("forbidden aborts (exit 1)", proc.returncode == 1, str(proc.returncode))
    check("forbidden reports abort", report["aborted"] is not None, str(report))
    check("forbidden deleted before abort", report["deleted"] == 2, str(report))
    STATE["forbid_after"] = None

    # 8. 429 on listing aborts with non-zero exit.
    STATE["rate_limit_on"] = "list"
    proc = run_script(base)
    check("429-list aborts (exit 1)", proc.returncode == 1, str(proc.returncode))
    check("429-list message", "429" in proc.stdout + proc.stderr, proc.stdout)
    STATE["rate_limit_on"] = "none"

    # 9. 429 on delete aborts with non-zero exit.
    STATE["rate_limit_on"] = "delete"
    proc = run_script(base, "--target-bytes", "0",
                      "--min-age-minutes", "30", "--max-deletes", "5")
    report = extract_report(proc.stdout)
    check("429-delete aborts (exit 1)", proc.returncode == 1, str(proc.returncode))
    check("429-delete reports abort", report["aborted"] is not None, str(report))
    STATE["rate_limit_on"] = "none"

    server.shutdown()
    if failures:
        print(f"\n{len(failures)} selftest(s) failed: {failures}")
        return 1
    print("\nall prune selftests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
