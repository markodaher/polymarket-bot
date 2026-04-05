"""
backfill_resolved.py
--------------------
Reads all unique market IDs from polymarket_log.csv, checks each one's
resolution status via the Polymarket CLOB API, and writes resolved markets
to polymarket_resolved.csv.

Safe to re-run: skips market IDs already present in polymarket_resolved.csv.

Usage:
    python backfill_resolved.py
"""

import csv
import os
import time
import requests
from datetime import datetime, timezone

from polymarket_watcher import (
    LOG_FILE,
    RESOLVED_FILE,
    RESOLVED_HEADERS,
    append_row,
    init_csv,
)

CLOB_API       = "https://clob.polymarket.com"
REQUEST_DELAY  = 0.15   # seconds between calls — ~6-7 req/s, well within limits


def load_logged_markets(filepath):
    """Return {market_id: question} for all unique IDs in the log CSV."""
    markets = {}
    with open(filepath, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mid = row["market_id"]
            if mid not in markets:
                markets[mid] = row["question"]
    return markets


def load_already_resolved(filepath):
    """Return set of market_ids already written to the resolved CSV."""
    if not os.path.exists(filepath):
        return set()
    with open(filepath, newline="", encoding="utf-8") as f:
        return {row["market_id"] for row in csv.DictReader(f)}


def check_resolution(condition_id):
    """
    Query the CLOB API for a market's resolution status.

    Returns (outcome, final_yes_price, resolved_at) if resolved, else None.
      outcome          — 1.0 if token[0] (YES/first outcome) won, 0.0 if it lost
      final_yes_price  — final price of token[0]
      resolved_at      — UTC timestamp string (now, since CLOB has no resolution timestamp)
    """
    try:
        resp = requests.get(f"{CLOB_API}/markets/{condition_id}", timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  [WARN] API error for {condition_id[:16]}…: {e}")
        return None

    if not data.get("closed"):
        return None

    tokens = data.get("tokens", [])
    if len(tokens) < 2:
        return None

    # Confirm at least one token has winner set (not voided)
    winners = [t for t in tokens if t.get("winner") is True]
    if not winners:
        return None

    yes_token = tokens[0]
    final_yes_price = round(float(yes_token.get("price", 0)), 4)
    outcome = 1.0 if yes_token.get("winner") is True else 0.0
    resolved_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    return outcome, final_yes_price, resolved_at


def main():
    print("=" * 55)
    print("  Polymarket Resolved Markets Backfill")
    print(f"  Source : {LOG_FILE}")
    print(f"  Output : {RESOLVED_FILE}")
    print("=" * 55)

    if not os.path.exists(LOG_FILE):
        print(f"[ERROR] {LOG_FILE} not found.")
        return

    logged   = load_logged_markets(LOG_FILE)
    resolved = load_already_resolved(RESOLVED_FILE)
    pending  = {mid: q for mid, q in logged.items() if mid not in resolved}

    print(f"  Unique markets in log   : {len(logged)}")
    print(f"  Already in resolved CSV : {len(resolved)}")
    print(f"  To check                : {len(pending)}")
    print()

    init_csv(RESOLVED_FILE, RESOLVED_HEADERS)

    found = 0
    skipped = 0

    for i, (market_id, question) in enumerate(pending.items(), 1):
        result = check_resolution(market_id)

        if result:
            outcome, final_yes_price, resolved_at = result
            append_row(RESOLVED_FILE, RESOLVED_HEADERS, {
                "market_id"       : market_id,
                "question"        : question[:80],
                "resolved_at"     : resolved_at,
                "outcome"         : outcome,
                "final_yes_price" : final_yes_price,
            })
            label = "YES ✓" if outcome == 1.0 else "NO  ✗"
            print(f"  [{i}/{len(pending)}] {label} final={final_yes_price} | {question[:55]}")
            found += 1
        else:
            skipped += 1

        if i % 50 == 0:
            print(f"  … {i}/{len(pending)} checked | {found} resolved so far")

        time.sleep(REQUEST_DELAY)

    print()
    print("=" * 55)
    print(f"  Done. {found} resolved markets written to {RESOLVED_FILE}")
    print(f"  {skipped} markets still active or unresolved.")
    print("=" * 55)


if __name__ == "__main__":
    main()
