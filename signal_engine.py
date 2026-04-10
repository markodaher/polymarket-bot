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
from datetime import datetime, timezone

import anthropic

# ─── CONFIG ──────────────────────────────────────────────────────────────────

_DATA_DIR      = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
GAPS_FILE      = os.path.join(_DATA_DIR, "polymarket_gaps.csv")
LOG_FILE       = os.path.join(_DATA_DIR, "polymarket_log.csv")
SIGNALS_FILE   = os.path.join(_DATA_DIR, "signals.csv")
EVALUATED_FILE = os.path.join(_DATA_DIR, "signal_evaluated.txt")  # all evaluated gap keys
GAP_HEADERS   = ["timestamp", "market_id", "question", "prev_yes", "curr_yes", "move"]
SIGNAL_HEADERS = [
    "timestamp", "market_id", "question",
    "polymarket_price", "claude_estimate", "edge",
    "recommended_side", "confidence",
]

MODEL               = "claude-sonnet-4-20250514"
EDGE_THRESHOLD      = 0.15   # minimum edge to flag as a signal
MIN_CONFIDENCE      = 0.50   # minimum confidence score
MIN_VOLUME          = 500    # minimum market volume ($)
MAX_DAYS_TO_RESOLVE = 7      # skip markets resolving more than 7 days out
MIN_PRICE           = 0.05   # skip markets with YES price below this (near-certain NO)
MAX_PRICE           = 0.95   # skip markets with YES price above this (near-certain YES)
REQUEST_DELAY       = 0.4    # seconds between Claude calls
WATCH_INTERVAL      = 60     # seconds between gap-file scans in --watch mode

# Keywords indicating an in-progress or already-completed event
_IN_PROGRESS_PATTERNS = re.compile(
    r"\b(set\s+\d|game\s+\d|map\s+\d|half\s+\d|quarter\s+\d|round\s+\d|period\s+\d"
    r"|1st\s+half|2nd\s+half|1st\s+quarter|inning\s+\d"
    r"|exact\s+score|correct\s+score"
    r"|\d+-\d+)\b",
    re.IGNORECASE,
)

SYSTEM_PROMPT = """\
You are a prediction market probability estimator. You will be given a binary \
market question and the market's YES price BEFORE a sudden price move occurred. \
Your job is to estimate the TRUE probability that the outcome is YES, independent \
of any crowd overreaction or underreaction.

Respond with ONLY a number between 0 and 1 (e.g. 0.72). No explanation, no text."""


# ─── QUESTION FILTER ─────────────────────────────────────────────────────────

_DATE_IN_QUESTION = re.compile(
    r"\b(\d{4}-\d{2}-\d{2}|(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{1,2}(?:,\s*\d{4})?|\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*(?:\s+\d{4})?)\b",
    re.IGNORECASE,
)

_MONTH_MAP = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
}

def _parse_date_in_question(text):
    """
    Return the first date found in text as a date object, or None.
    Handles: 2026-04-09, April 9, Apr 9, Apr 9 2026, 9 Apr, 9 Apr 2026.
    """
    for m in _DATE_IN_QUESTION.finditer(text):
        raw = m.group(0).strip()
        # ISO format
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            pass
        # "Month D, YYYY" / "Month D"
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%B %d", "%b %d",
                    "%d %B %Y", "%d %b %Y", "%d %B", "%d %b"):
            try:
                d = datetime.strptime(raw, fmt).date()
                # If year is missing, assume current year
                if d.year == 1900:
                    d = d.replace(year=datetime.now(timezone.utc).year)
                return d
            except ValueError:
                pass
    return None


def question_is_stale(question, today):
    """
    Return True if the question should be filtered out because it refers to
    an in-progress game segment or a specific date that is in the past.
    """
    if _IN_PROGRESS_PATTERNS.search(question):
        return True
    d = _parse_date_in_question(question)
    if d is not None and d < today:
        return True
    return False


# ─── CSV HELPERS ─────────────────────────────────────────────────────────────

def init_signals_csv():
    if not os.path.exists(SIGNALS_FILE):
        with open(SIGNALS_FILE, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=SIGNAL_HEADERS).writeheader()


def load_evaluated_keys():
    """
    Return set of (market_id, timestamp) for all gap events already evaluated
    this session — both those that fired a signal and those that didn't.
    Persisted in signal_evaluated.txt so restarts don't re-burn API calls.
    """
    keys = set()
    # Pull from signals.csv (definitive source for fired signals)
    if os.path.exists(SIGNALS_FILE):
        with open(SIGNALS_FILE, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                keys.add((r["market_id"], r["timestamp"]))
    # Pull from evaluated log (gaps that were checked but didn't fire)
    if os.path.exists(EVALUATED_FILE):
        with open(EVALUATED_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if "|" in line:
                    mid, ts = line.split("|", 1)
                    keys.add((mid, ts))
    return keys


def mark_evaluated(market_id, timestamp):
    """Persist a gap key so it's never re-evaluated after a restart."""
    with open(EVALUATED_FILE, "a", encoding="utf-8") as f:
        f.write(f"{market_id}|{timestamp}\n")


def append_signal(row):
    with open(SIGNALS_FILE, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=SIGNAL_HEADERS).writerow(row)


def load_market_meta():
    """
    Return ({market_id: volume}, {market_id: end_date_str}) from polymarket_log.csv.
    Uses the latest row seen per market (last row wins).
    end_date_str is "YYYY-MM-DD" or "" if not logged yet.
    """
    vol = {}
    end_dates = {}
    if not os.path.exists(LOG_FILE):
        return vol, end_dates
    with open(LOG_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mid = row.get("market_id", "")
            if not mid:
                continue
            try:
                vol[mid] = float(row["volume"])
            except (KeyError, ValueError):
                pass
            ed = row.get("end_date", "").strip()
            if ed:
                end_dates[mid] = ed
    return vol, end_dates


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

def process_gaps(client, gaps, processed_keys, vol_map, end_date_map):
    new_signals = 0
    new_processed = 0
    filtered_vol = 0
    filtered_date = 0
    filtered_price = 0
    filtered_stale = 0
    filtered_edge = 0
    filtered_conf = 0

    today = datetime.now(timezone.utc).date()

    for g in gaps:
        key = (g["market_id"], g["timestamp"])
        if key in processed_keys:
            continue

        processed_keys.add(key)
        mark_evaluated(g["market_id"], g["timestamp"])  # persist immediately
        new_processed += 1

        question = g["question"]
        prev_yes = float(g["prev_yes"])
        curr_yes = float(g["curr_yes"])
        move     = float(g["move"])

        # Volume pre-filter — skip Claude call entirely if market is too thin
        volume = vol_map.get(g["market_id"], 0)
        if volume < MIN_VOLUME:
            filtered_vol += 1
            continue

        # Resolution date filter — skip markets resolving too far out
        end_date_str = end_date_map.get(g["market_id"], "")
        if end_date_str:
            try:
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
                days_out = (end_date - today).days
                if days_out > MAX_DAYS_TO_RESOLVE:
                    filtered_date += 1
                    continue
            except ValueError:
                pass  # malformed date — don't filter

        # Price filter — skip near-certain markets (outcome already decided)
        if curr_yes < MIN_PRICE or curr_yes > MAX_PRICE:
            filtered_price += 1
            continue

        # Question filter — skip in-progress game segments or past-date markets
        if question_is_stale(question, today):
            filtered_stale += 1
            continue

        short_q = question[:52]
        print(f"  [{g['timestamp']}] {short_q!r:<54} prev={prev_yes:.2f} curr={curr_yes:.2f} move={move:+.2f}",
              end=" ", flush=True)

        claude_p = ask_claude(client, question, prev_yes, move)
        if claude_p is None:
            print("→ SKIP (API error)")
            time.sleep(REQUEST_DELAY)
            continue

        edge       = round(claude_p - curr_yes, 4)
        side       = "YES" if edge > 0 else "NO"
        confidence = min(abs(edge) / 0.30, 1.0)   # scales 15–30¢ edge → 0.50–1.0

        if abs(edge) < EDGE_THRESHOLD:
            filtered_edge += 1
            print(f"→ filtered  Claude={claude_p:.2f}  edge={edge:+.2f} (< {EDGE_THRESHOLD})")
            time.sleep(REQUEST_DELAY)
            continue

        if confidence < MIN_CONFIDENCE:
            filtered_conf += 1
            print(f"→ filtered  Claude={claude_p:.2f}  edge={edge:+.2f}  conf={confidence:.2f} (< {MIN_CONFIDENCE})")
            time.sleep(REQUEST_DELAY)
            continue

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
        print(f"→ SIGNAL {side}  Claude={claude_p:.2f}  edge={edge:+.2f}  conf={confidence:.2f}  vol=${volume:,.0f}")

        time.sleep(REQUEST_DELAY)

    skips = []
    if filtered_vol:   skips.append(f"{filtered_vol} thin volume (<${MIN_VOLUME:,})")
    if filtered_date:  skips.append(f"{filtered_date} too far out (>{MAX_DAYS_TO_RESOLVE}d)")
    if filtered_price: skips.append(f"{filtered_price} near-certain price (<{MIN_PRICE} or >{MAX_PRICE})")
    if filtered_stale: skips.append(f"{filtered_stale} stale/in-progress question")
    if skips:
        print(f"  Skipped: {', '.join(skips)}")

    return new_processed, new_signals


# ─── WATCH LOOP (callable from dashboard.py thread) ──────────────────────────

def watch_loop(client):
    """
    Infinite loop: every WATCH_INTERVAL seconds, pick up new gap events,
    evaluate them through Claude, and write qualifying signals.
    Called directly by dashboard.py's background thread.
    """
    init_signals_csv()
    cycle = 0
    while True:
        cycle += 1
        evaluated_keys    = load_evaluated_keys()
        vol_map, end_dates = load_market_meta()
        gaps              = load_gaps()
        pending           = [g for g in gaps
                             if (g["market_id"], g["timestamp"]) not in evaluated_keys]

        print(f"[SIGNAL Cycle {cycle}] {len(gaps)} gaps | {len(pending)} new | {len(vol_map)} markets")

        if pending:
            processed, signals = process_gaps(client, pending, evaluated_keys, vol_map, end_dates)
            print(f"[SIGNAL] Done: {processed} evaluated | {signals} signals written")

        time.sleep(WATCH_INTERVAL)


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
    print(f"  Edge threshold : >= {EDGE_THRESHOLD:.0%}")
    print(f"  Min confidence : >= {MIN_CONFIDENCE:.0%}")
    print(f"  Min volume     : >= ${MIN_VOLUME:,}")
    print(f"  Gaps file      : {GAPS_FILE}")
    print(f"  Signals file   : {SIGNALS_FILE}")
    print(f"  Mode           : {'watch (continuous)' if args.watch else 'one-shot'}")
    print("=" * 65)

    cycle = 0
    while True:
        cycle += 1
        evaluated_keys     = load_evaluated_keys()
        vol_map, end_dates = load_market_meta()
        gaps               = load_gaps()
        pending            = [g for g in gaps
                              if (g["market_id"], g["timestamp"]) not in evaluated_keys]

        print(f"\n[Cycle {cycle}] {len(gaps)} total gaps | {len(pending)} unprocessed | {len(vol_map)} markets with volume")

        if pending:
            processed, signals = process_gaps(client, pending, evaluated_keys, vol_map, end_dates)
            print(f"\n  Done: {processed} evaluated | {signals} signals written")
        else:
            print("  Nothing new to process.")

        if not args.watch:
            break

        print(f"\n  Sleeping {WATCH_INTERVAL}s...")
        time.sleep(WATCH_INTERVAL)


if __name__ == "__main__":
    main()
