"""
signal_engine.py
----------------
Trading signal engine for Polymarket.

For each gap event in polymarket_gaps.csv (price move >= 5¢ in one cycle),
asks Claude to estimate the true YES probability given the question and the
pre-gap price (prev_yes). If Claude's estimate differs from the current
Polymarket price (curr_yes) by more than EDGE_THRESHOLD, logs a trade signal.

Signals are written to signals.csv and deduplicated by (market_id, timestamp).

Usage:
    ANTHROPIC_API_KEY=sk-ant-... python signal_engine.py          # one-shot
    ANTHROPIC_API_KEY=sk-ant-... python signal_engine.py --watch  # continuous

Output: signals.csv
"""

import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime

import anthropic

# ─── CONFIG ──────────────────────────────────────────────────────────────────

GAPS_FILE     = os.path.join(os.environ.get("DATA_DIR", os.path.dirname(__file__)), "polymarket_gaps.csv")
SIGNALS_FILE  = os.path.join(os.environ.get("DATA_DIR", os.path.dirname(__file__)), "signals.csv")
GAP_HEADERS   = ["timestamp", "market_id", "question", "prev_yes", "curr_yes", "move"]
SIGNAL_HEADERS = [
    "timestamp", "market_id", "question",
    "polymarket_price", "claude_estimate", "edge",
    "recommended_side", "confidence",
]

MODEL          = "claude-sonnet-4-20250514"
EDGE_THRESHOLD = 0.10   # minimum difference to flag as a signal
REQUEST_DELAY  = 0.4    # seconds between Claude calls
WATCH_INTERVAL = 60     # seconds between gap-file scans in --watch mode

SYSTEM_PROMPT = """\
You are a prediction market probability estimator. You will be given a binary \
market question and the market's YES price BEFORE a sudden price move occurred. \
Your job is to estimate the TRUE probability that the outcome is YES, independent \
of any crowd overreaction or underreaction.

Respond with ONLY a number between 0 and 1 (e.g. 0.72). No explanation, no text."""


# ─── CSV HELPERS ─────────────────────────────────────────────────────────────

def init_signals_csv():
    if not os.path.exists(SIGNALS_FILE):
        with open(SIGNALS_FILE, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=SIGNAL_HEADERS).writeheader()


def load_processed_keys():
    """Return set of (market_id, timestamp) already written to signals.csv."""
    if not os.path.exists(SIGNALS_FILE):
        return set()
    with open(SIGNALS_FILE, newline="", encoding="utf-8") as f:
        return {(r["market_id"], r["timestamp"]) for r in csv.DictReader(f)}


def append_signal(row):
    with open(SIGNALS_FILE, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=SIGNAL_HEADERS).writerow(row)


def load_gaps():
    """Return deduplicated list of gap dicts."""
    if not os.path.exists(GAPS_FILE):
        return []
    with open(GAPS_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    seen, unique = set(), []
    for r in rows:
        key = (r["market_id"], r["timestamp"])
        if key not in seen:
            seen.add(key)
            unique.append(r)
    return unique


# ─── CLAUDE ──────────────────────────────────────────────────────────────────

def ask_claude(client, question, prev_yes, move):
    direction = "up" if float(move) > 0 else "down"
    user_msg = (
        f"Question: {question}\n"
        f"Market YES price before the move: {prev_yes}\n"
        f"The price just moved {direction} by {abs(float(move)):.2f} in a single 30-second cycle.\n\n"
        f"What is the TRUE probability that the outcome is YES? "
        f"Reply with only a number between 0 and 1."
    )
    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=16,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = resp.content[0].text.strip()
        match = re.search(r"\d+\.?\d*", raw)
        if not match:
            return None
        return max(0.0, min(1.0, float(match.group())))
    except Exception as e:
        print(f"  [WARN] Claude error: {e}")
        return None


# ─── SIGNAL LOGIC ────────────────────────────────────────────────────────────

def process_gaps(client, gaps, processed_keys):
    new_signals = 0
    new_processed = 0

    for g in gaps:
        key = (g["market_id"], g["timestamp"])
        if key in processed_keys:
            continue

        processed_keys.add(key)
        new_processed += 1

        question   = g["question"]
        prev_yes   = float(g["prev_yes"])
        curr_yes   = float(g["curr_yes"])
        move       = float(g["move"])

        short_q = question[:52]
        print(f"  [{g['timestamp']}] {short_q!r:<54} prev={prev_yes:.2f} curr={curr_yes:.2f} move={move:+.2f}",
              end=" ", flush=True)

        claude_p = ask_claude(client, question, prev_yes, move)
        if claude_p is None:
            print("→ SKIP (API error)")
            time.sleep(REQUEST_DELAY)
            continue

        edge = round(claude_p - curr_yes, 4)   # positive = Claude thinks YES is underpriced

        if abs(edge) < EDGE_THRESHOLD:
            print(f"→ no signal  Claude={claude_p:.2f}  edge={edge:+.2f}")
            time.sleep(REQUEST_DELAY)
            continue

        side       = "YES" if edge > 0 else "NO"
        confidence = min(abs(edge) / 0.30, 1.0)   # scales 10–30¢ edge → 0.33–1.0

        signal = {
            "timestamp"       : g["timestamp"],
            "market_id"       : g["market_id"],
            "question"        : question,
            "polymarket_price": curr_yes,
            "claude_estimate" : claude_p,
            "edge"            : edge,
            "recommended_side": side,
            "confidence"      : round(confidence, 4),
        }
        append_signal(signal)
        new_signals += 1
        print(f"→ SIGNAL {side}  Claude={claude_p:.2f}  edge={edge:+.2f}  conf={confidence:.2f}")

        time.sleep(REQUEST_DELAY)

    return new_processed, new_signals


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch", action="store_true",
                        help="Re-scan gaps file continuously every 60s")
    args = parser.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("[ERROR] ANTHROPIC_API_KEY not set.")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    init_signals_csv()

    print("=" * 65)
    print("  Polymarket Signal Engine")
    print(f"  Model          : {MODEL}")
    print(f"  Edge threshold : {EDGE_THRESHOLD:.0%}")
    print(f"  Gaps file      : {GAPS_FILE}")
    print(f"  Signals file   : {SIGNALS_FILE}")
    print(f"  Mode           : {'watch (continuous)' if args.watch else 'one-shot'}")
    print("=" * 65)

    cycle = 0
    while True:
        cycle += 1
        processed_keys = load_processed_keys()
        gaps = load_gaps()
        pending = [g for g in gaps if (g["market_id"], g["timestamp"]) not in processed_keys]

        print(f"\n[Cycle {cycle}] {len(gaps)} total gaps | {len(pending)} unprocessed")

        if pending:
            processed, signals = process_gaps(client, pending, processed_keys)
            print(f"\n  Done: {processed} evaluated | {signals} signals written")
        else:
            print("  Nothing new to process.")

        if not args.watch:
            break

        print(f"\n  Sleeping {WATCH_INTERVAL}s...")
        time.sleep(WATCH_INTERVAL)


if __name__ == "__main__":
    main()
