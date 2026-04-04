"""
Polymarket Market Watcher
--------------------------
Polls Polymarket's public API every N seconds and logs price snapshots to CSV.
No wallet, no trades, no crypto needed. Pure observation.

Usage:
    pip install requests
    python polymarket_watcher.py

Output:
    polymarket_log.csv  — one row per market per poll cycle
    polymarket_gaps.csv — logs when price moves > ALERT_THRESHOLD in one cycle
"""

import requests
import csv
import json
import time
import os
from datetime import datetime, timezone

# ─── CONFIG ───────────────────────────────────────────────────────────────────

POLL_INTERVAL_SECONDS = 30        # how often to poll (30s is polite, don't go below 10)
LOG_FILE = "polymarket_log.csv"
GAPS_FILE = "polymarket_gaps.csv"
ALERT_THRESHOLD = 0.05            # flag moves >= 5 cents in one cycle
MAX_MARKETS = 50                  # how many markets to track per poll

# Filter by tag — options: "sports", "crypto", "politics", "business", None (all)
FILTER_TAG = None

# ─── POLYMARKET API ───────────────────────────────────────────────────────────

GAMMA_API = "https://gamma-api.polymarket.com"

def fetch_markets(tag=None, limit=50):
    """Fetch active markets from Polymarket Gamma API."""
    params = {
        "active": "true",
        "closed": "false",
        "limit": limit,
        "order": "volume",
        "ascending": "false"
    }
    if tag:
        params["tag"] = tag

    try:
        resp = requests.get(f"{GAMMA_API}/markets", params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to fetch markets: {e}")
        return []

def fetch_market_prices(condition_id):
    """
    Fetch current YES price for a specific market via CLOB API.
    Returns (yes_price, no_price) or (None, None) on failure.
    """
    try:
        resp = requests.get(
            f"https://clob.polymarket.com/last-trade-price",
            params={"token_id": condition_id},
            timeout=5
        )
        resp.raise_for_status()
        data = resp.json()
        price = float(data.get("price", 0))
        return round(price, 4), round(1 - price, 4)
    except Exception:
        return None, None

# ─── LOGGING ──────────────────────────────────────────────────────────────────

LOG_HEADERS = ["timestamp", "market_id", "question", "tag", "yes_price", "no_price", "volume"]
GAP_HEADERS = ["timestamp", "market_id", "question", "prev_yes", "curr_yes", "move"]

def init_csv(filepath, headers):
    """Create CSV with headers if it doesn't exist."""
    if not os.path.exists(filepath):
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
        print(f"[INIT] Created {filepath}")

def append_row(filepath, headers, row):
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writerow(row)

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  Polymarket Watcher — passive price logger")
    print(f"  Tag filter : {FILTER_TAG or 'all'}")
    print(f"  Poll every : {POLL_INTERVAL_SECONDS}s")
    print(f"  Log file   : {LOG_FILE}")
    print(f"  Gap alerts : {GAPS_FILE} (moves >= {ALERT_THRESHOLD*100:.0f}¢)")
    print("  Ctrl+C to stop")
    print("=" * 55)

    init_csv(LOG_FILE, LOG_HEADERS)
    init_csv(GAPS_FILE, GAP_HEADERS)

    # Track previous prices to detect gaps
    prev_prices = {}   # { market_id: yes_price }
    cycle = 0

    while True:
        cycle += 1
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[Cycle {cycle}] {now} — fetching markets...")

        markets = fetch_markets(tag=FILTER_TAG, limit=MAX_MARKETS)
        if not markets:
            print("  No markets returned. Retrying next cycle.")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        logged = 0
        alerts = 0

        for m in markets:
            market_id   = m.get("conditionId") or m.get("id", "")
            question    = m.get("question", "")[:80]
            volume      = m.get("volume", 0)
            tag_str     = ""

            # outcomePrices is a JSON string "[\"0.12\", \"0.88\"]" — YES is index 0
            raw_prices = m.get("outcomePrices")
            if not raw_prices:
                continue
            try:
                if isinstance(raw_prices, str):
                    prices = json.loads(raw_prices)
                else:
                    prices = raw_prices
                yes_price = round(float(prices[0]), 4)
                no_price  = round(float(prices[1]), 4)
            except Exception:
                continue

            # Log to main CSV
            row = {
                "timestamp" : now,
                "market_id" : market_id,
                "question"  : question,
                "tag"       : tag_str,
                "yes_price" : yes_price,
                "no_price"  : no_price,
                "volume"    : volume
            }
            append_row(LOG_FILE, LOG_HEADERS, row)
            logged += 1

            # Check for price gap vs previous cycle
            if market_id in prev_prices:
                prev = prev_prices[market_id]
                move = round(yes_price - prev, 4)
                if abs(move) >= ALERT_THRESHOLD:
                    gap_row = {
                        "timestamp" : now,
                        "market_id" : market_id,
                        "question"  : question,
                        "prev_yes"  : prev,
                        "curr_yes"  : yes_price,
                        "move"      : move
                    }
                    append_row(GAPS_FILE, GAP_HEADERS, gap_row)
                    direction = "▲" if move > 0 else "▼"
                    print(f"  {direction} GAP {move:+.4f} | {question[:50]}")
                    alerts += 1

            prev_prices[market_id] = yes_price

        print(f"  Logged {logged} markets | {alerts} gaps detected")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[STOPPED] Watcher shut down. Check your CSV files.")
