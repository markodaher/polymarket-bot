"""
Polymarket Dashboard
--------------------
Runs the market watcher in a background thread (with auto-restart) and
serves a status page, health check, and CSV download endpoints.

Endpoints:
    /                    — live dashboard (auto-refreshes every 30s)
    /health              — JSON health check; HTTP 500 if watcher dead or stale
    /download/logs       — download polymarket_log.csv
    /download/gaps       — download polymarket_gaps.csv
    /download/resolved   — download polymarket_resolved.csv
    /upload/resolved     — POST a CSV file to append rows to polymarket_resolved.csv
"""

import csv
import os
import threading
import time
from datetime import datetime, timezone

from flask import Flask, jsonify, request, send_file

import backfill_resolved
import polymarket_watcher as watcher

app = Flask(__name__)

LOG_FILE      = watcher.LOG_FILE
GAPS_FILE     = watcher.GAPS_FILE
RESOLVED_FILE = watcher.RESOLVED_FILE

# ─── SHARED WATCHER STATE ────────────────────────────────────────────────────

_watcher_thread   = None
_watcher_restarts = 0
_state_lock       = threading.Lock()

# ─── HTML TEMPLATE ───────────────────────────────────────────────────────────

def render_page(total_rows, last_markets, recent_gaps, recent_resolved):
    rows_html = "".join(
        f"<tr><td>{r['timestamp']}</td><td class='q'>{r['question']}</td>"
        f"<td>{r['yes_price']}</td><td>{r['no_price']}</td>"
        f"<td>{float(r['volume']):.0f}</td></tr>"
        for r in last_markets
    )
    gaps_html = "".join(
        f"<tr><td>{g['timestamp']}</td><td class='q'>{g['question']}</td>"
        f"<td>{g['prev_yes']}</td><td>{g['curr_yes']}</td>"
        f"<td class='{'up' if float(g['move']) > 0 else 'dn'}'>{float(g['move']):+.4f}</td></tr>"
        for g in recent_gaps
    ) or "<tr><td colspan='5' class='none'>No gaps detected yet</td></tr>"

    def outcome_badge(r):
        o = float(r['outcome'])
        return "<span class='yes'>YES ✓</span>" if o >= 0.99 else "<span class='no'>NO ✗</span>"

    resolved_html = "".join(
        f"<tr><td>{r['resolved_at']}</td><td class='q'>{r['question']}</td>"
        f"<td>{outcome_badge(r)}</td><td>{r['final_yes_price']}</td></tr>"
        for r in recent_resolved
    ) or "<tr><td colspan='4' class='none'>No resolved markets yet</td></tr>"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="30">
<title>Polymarket Watcher</title>
<style>
  body {{ font-family: monospace; background: #0d1117; color: #c9d1d9; padding: 2rem; }}
  h1   {{ color: #58a6ff; margin-bottom: .25rem; }}
  .sub {{ color: #8b949e; font-size: .85rem; margin-bottom: 2rem; }}
  h2   {{ color: #58a6ff; margin: 1.5rem 0 .5rem; }}
  table {{ border-collapse: collapse; width: 100%; max-width: 960px; }}
  th, td {{ text-align: left; padding: .35rem .6rem; border-bottom: 1px solid #21262d; font-size: .85rem; }}
  th   {{ color: #8b949e; }}
  .q   {{ max-width: 380px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
  .up  {{ color: #3fb950; }}
  .dn  {{ color: #f85149; }}
  .yes {{ color: #3fb950; font-weight: bold; }}
  .no  {{ color: #f85149; font-weight: bold; }}
  .badge {{ display: inline-block; background: #1f6feb; color: #fff;
            padding: .2rem .6rem; border-radius: 12px; font-size: 1rem; }}
  .none {{ color: #8b949e; }}
  nav  {{ margin-bottom: 1.5rem; font-size: .85rem; }}
  nav a {{ color: #58a6ff; margin-right: 1.2rem; text-decoration: none; }}
  nav a:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<h1>Polymarket Watcher</h1>
<div class="sub">Auto-refreshes every 30 s &nbsp;·&nbsp; {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC</div>
<nav>
  <a href="/health">⚡ Health</a>
  <a href="/download/logs">⬇ logs.csv</a>
  <a href="/download/gaps">⬇ gaps.csv</a>
  <a href="/download/resolved">⬇ resolved.csv</a>
</nav>

<h2>Total rows logged &nbsp;<span class="badge">{total_rows:,}</span></h2>

<h2>Last 5 markets logged</h2>
<table>
  <tr><th>Timestamp (UTC)</th><th>Question</th><th>YES</th><th>NO</th><th>Volume ($)</th></tr>
  {rows_html}
</table>

<h2>Recent gaps (&ge; 5¢ move)</h2>
<table>
  <tr><th>Timestamp (UTC)</th><th>Question</th><th>Prev YES</th><th>Curr YES</th><th>Move</th></tr>
  {gaps_html}
</table>

<h2>Recently resolved markets</h2>
<table>
  <tr><th>Resolved at (UTC)</th><th>Question</th><th>Outcome</th><th>Final YES price</th></tr>
  {resolved_html}
</table>
</body>
</html>"""

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def read_csv_tail(filepath, headers, n=5):
    """Return (last n rows as list of dicts, total row count)."""
    if not os.path.exists(filepath):
        return [], 0
    with open(filepath, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows[-n:], len(rows)


def get_last_poll_time():
    """Return the timestamp string from the last row of the log CSV, or None."""
    if not os.path.exists(LOG_FILE):
        return None
    try:
        with open(LOG_FILE, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        return rows[-1]["timestamp"] if rows else None
    except Exception:
        return None

# ─── ROUTES ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    last_markets, total_rows = read_csv_tail(LOG_FILE,      watcher.LOG_HEADERS,      5)
    recent_gaps,  _          = read_csv_tail(GAPS_FILE,     watcher.GAP_HEADERS,     10)
    recent_resolved, _       = read_csv_tail(RESOLVED_FILE, watcher.RESOLVED_HEADERS, 10)
    last_markets    = list(reversed(last_markets))
    recent_gaps     = list(reversed(recent_gaps))
    recent_resolved = list(reversed(recent_resolved))
    return render_page(total_rows, last_markets, recent_gaps, recent_resolved)


@app.route("/health")
def health():
    _, log_rows      = read_csv_tail(LOG_FILE,      watcher.LOG_HEADERS,      1)
    _, gaps_rows     = read_csv_tail(GAPS_FILE,     watcher.GAP_HEADERS,      1)
    _, resolved_rows = read_csv_tail(RESOLVED_FILE, watcher.RESOLVED_HEADERS, 1)

    last_poll_str      = get_last_poll_time()
    seconds_since_poll = None
    if last_poll_str:
        try:
            last_dt = datetime.strptime(last_poll_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            seconds_since_poll = int((datetime.now(timezone.utc) - last_dt).total_seconds())
        except Exception:
            pass

    with _state_lock:
        restarts = _watcher_restarts
    watcher_alive = _watcher_thread is not None and _watcher_thread.is_alive()

    poll_stale = seconds_since_poll is not None and seconds_since_poll > 300
    errors = []
    if not watcher_alive:
        errors.append("watcher thread is dead")
    if poll_stale:
        errors.append(f"last poll was {seconds_since_poll}s ago (threshold: 300s)")

    payload = {
        "status"                 : "error" if errors else "ok",
        "watcher_alive"          : watcher_alive,
        "watcher_restarts"       : restarts,
        "last_poll_timestamp"    : last_poll_str,
        "seconds_since_last_poll": seconds_since_poll,
        "rows"                   : {"log": log_rows, "gaps": gaps_rows, "resolved": resolved_rows},
        "errors"                 : errors,
    }
    return jsonify(payload), (500 if errors else 200)


def _send_csv(filepath, download_name):
    if not os.path.exists(filepath):
        return f"{download_name} not found", 404
    return send_file(
        os.path.abspath(filepath),
        mimetype="text/csv",
        as_attachment=True,
        download_name=download_name,
    )

@app.route("/download/logs")
def download_logs():
    return _send_csv(LOG_FILE, "polymarket_log.csv")

@app.route("/download/gaps")
def download_gaps():
    return _send_csv(GAPS_FILE, "polymarket_gaps.csv")

@app.route("/download/resolved")
def download_resolved():
    return _send_csv(RESOLVED_FILE, "polymarket_resolved.csv")


@app.route("/upload/resolved", methods=["POST"])
def upload_resolved():
    if "file" not in request.files:
        return jsonify({"error": "no file field in request"}), 400

    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "empty filename"}), 400

    try:
        text = f.read().decode("utf-8")
        reader = csv.DictReader(text.splitlines())
    except Exception as e:
        return jsonify({"error": f"could not parse CSV: {e}"}), 400

    # Validate columns match expected headers
    if reader.fieldnames != watcher.RESOLVED_HEADERS:
        return jsonify({
            "error": "CSV columns do not match expected headers",
            "expected": watcher.RESOLVED_HEADERS,
            "got": list(reader.fieldnames or []),
        }), 400

    # Load existing market IDs to skip duplicates
    _, existing_rows = read_csv_tail(RESOLVED_FILE, watcher.RESOLVED_HEADERS, n=0)
    existing_ids: set = set()
    if os.path.exists(RESOLVED_FILE):
        with open(RESOLVED_FILE, newline="", encoding="utf-8") as ef:
            for row in csv.DictReader(ef):
                existing_ids.add(row["market_id"])

    watcher.init_csv(RESOLVED_FILE, watcher.RESOLVED_HEADERS)

    appended = 0
    skipped = 0
    for row in reader:
        if row.get("market_id") in existing_ids:
            skipped += 1
            continue
        watcher.append_row(RESOLVED_FILE, watcher.RESOLVED_HEADERS, row)
        existing_ids.add(row["market_id"])
        appended += 1

    return jsonify({"appended": appended, "skipped_duplicates": skipped}), 200

# ─── BACKGROUND WATCHER (with auto-restart) ──────────────────────────────────

def _run_watcher_with_restart():
    global _watcher_restarts
    backoff = 5
    while True:
        try:
            watcher.main()
            # main() should loop forever; if it returns, treat as unexpected
            print("[WATCHER] main() returned unexpectedly — restarting in 5s")
            time.sleep(5)
        except Exception as e:
            with _state_lock:
                _watcher_restarts += 1
                count = _watcher_restarts
            print(f"[WATCHER] Crashed (restart #{count}): {e} — retrying in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ─── STARTUP BACKFILL ────────────────────────────────────────────────────────

def _run_backfill():
    try:
        print("[BACKFILL] Running startup backfill...")
        backfill_resolved.main()
        print("[BACKFILL] Done.")
    except Exception as e:
        print(f"[BACKFILL] Error: {e}")

# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 1. Backfill resolved markets from existing log data (non-blocking)
    threading.Thread(target=_run_backfill, daemon=True).start()

    # 2. Start watcher with auto-restart
    _watcher_thread = threading.Thread(target=_run_watcher_with_restart, daemon=True)
    _watcher_thread.start()

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
