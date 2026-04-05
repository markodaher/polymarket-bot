"""
Polymarket Dashboard
--------------------
Runs the market watcher in a background thread and serves a live
status page at / showing logged row count, last 5 markets, and recent gaps.
"""

import os
import csv
import threading
from datetime import datetime, timezone
from flask import Flask
import polymarket_watcher as watcher

app = Flask(__name__)

LOG_FILE      = watcher.LOG_FILE
GAPS_FILE     = watcher.GAPS_FILE
RESOLVED_FILE = watcher.RESOLVED_FILE

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
</style>
</head>
<body>
<h1>Polymarket Watcher</h1>
<div class="sub">Auto-refreshes every 30 s &nbsp;·&nbsp; {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC</div>

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

# ─── ROUTES ──────────────────────────────────────────────────────────────────

def read_csv_tail(filepath, headers, n=5):
    """Return last n rows of a CSV as list of dicts."""
    if not os.path.exists(filepath):
        return [], 0
    with open(filepath, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows[-n:], len(rows)

@app.route("/")
def index():
    last_markets, total_rows = read_csv_tail(LOG_FILE,      watcher.LOG_HEADERS,      5)
    recent_gaps,  _          = read_csv_tail(GAPS_FILE,     watcher.GAP_HEADERS,     10)
    recent_resolved, _       = read_csv_tail(RESOLVED_FILE, watcher.RESOLVED_HEADERS, 10)
    last_markets    = list(reversed(last_markets))
    recent_gaps     = list(reversed(recent_gaps))
    recent_resolved = list(reversed(recent_resolved))
    return render_page(total_rows, last_markets, recent_gaps, recent_resolved)

# ─── BACKGROUND WATCHER ──────────────────────────────────────────────────────

def start_watcher():
    watcher.main()

# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    t = threading.Thread(target=start_watcher, daemon=True)
    t.start()

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
