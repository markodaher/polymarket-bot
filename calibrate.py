"""
calibrate.py
------------
Tests Claude's calibration on resolved Polymarket markets.

For each resolved market, finds the last price snapshot logged in
polymarket_log.csv BEFORE the resolved_at timestamp, and uses that
earlier price (plus how many hours before resolution it was) as context
for Claude's YES-probability estimate.

Computes Brier score, per-bucket accuracy, and overall accuracy.

Usage:
    pip install anthropic
    ANTHROPIC_API_KEY=sk-ant-... python calibrate.py
"""

import csv
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime

import anthropic

RESOLVED_FILE = os.path.join(os.path.dirname(__file__), "polymarket_resolved.csv")
LOG_FILE      = os.path.join(os.path.dirname(__file__), "polymarket_log.csv")
MODEL         = "claude-sonnet-4-20250514"
REQUEST_DELAY = 0.3   # seconds between API calls
TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"

SYSTEM_PROMPT = """\
You are a probability calibration assistant. You will be given a binary prediction \
market question and an earlier crowd price snapshot (NOT the final resolved price). \
Your job is to estimate the probability that the outcome is YES based on the question \
and the available crowd signal.

Respond with ONLY a number between 0 and 1 (e.g. 0.75). No explanation, no text, just the number."""


def load_log_index(filepath):
    """Return {market_id: sorted list of (datetime, yes_price)} from log CSV."""
    index = defaultdict(list)
    with open(filepath, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                dt = datetime.strptime(row["timestamp"], TIMESTAMP_FMT)
                price = float(row["yes_price"])
                index[row["market_id"]].append((dt, price))
            except Exception:
                continue
    for entries in index.values():
        entries.sort()
    return index


def last_price_before(log_index, market_id, resolved_dt):
    """
    Return (yes_price, hours_before_resolution) for the last log entry
    strictly before resolved_dt, or (None, None) if none found.
    """
    entries = log_index.get(market_id, [])
    before = [(dt, p) for dt, p in entries if dt < resolved_dt]
    if not before:
        return None, None
    last_dt, last_price = before[-1]
    hours_before = (resolved_dt - last_dt).total_seconds() / 3600
    return last_price, hours_before


def ask_claude(client, question, snapshot_price, hours_before):
    """Ask Claude for a YES probability estimate. Returns float or None."""
    hours_str = f"{hours_before:.1f} hours" if hours_before >= 1 else f"{hours_before*60:.0f} minutes"
    user_msg = (
        f"Question: {question}\n"
        f"Market YES price {hours_str} before resolution: {snapshot_price:.4f}\n\n"
        f"What is your probability estimate that the outcome is YES? "
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
        val = float(match.group())
        return max(0.0, min(1.0, val))
    except Exception as e:
        print(f"  [WARN] API error: {e}")
        return None


def brier_score(pairs):
    """pairs: list of (claude_p, actual_outcome)."""
    return sum((p - o) ** 2 for p, o in pairs) / len(pairs)


def bucket_label(p):
    if p < 0.30:
        return "0–30%"
    elif p < 0.70:
        return "30–70%"
    else:
        return "70–100%"


def main():
    for path in (RESOLVED_FILE, LOG_FILE):
        if not os.path.exists(path):
            print(f"[ERROR] {path} not found.")
            sys.exit(1)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("[ERROR] ANTHROPIC_API_KEY environment variable not set.")
        sys.exit(1)

    print("Loading log index...", flush=True)
    log_index = load_log_index(LOG_FILE)

    with open(RESOLVED_FILE, newline="", encoding="utf-8") as f:
        resolved_rows = list(csv.DictReader(f))

    client = anthropic.Anthropic(api_key=api_key)

    print("=" * 65)
    print(f"  Claude Calibration Test — {len(resolved_rows)} resolved markets")
    print(f"  Model : {MODEL}")
    print(f"  Price : last snapshot before resolution (from log CSV)")
    print("=" * 65)
    print()

    results  = []   # (question, snapshot_price, hours_before, actual, claude_p)
    skipped  = 0
    errors   = 0

    for i, row in enumerate(resolved_rows, 1):
        market_id  = row["market_id"]
        question   = row["question"]
        actual     = float(row["outcome"])
        resolved_dt = datetime.strptime(row["resolved_at"], TIMESTAMP_FMT)

        snapshot_price, hours_before = last_price_before(log_index, market_id, resolved_dt)
        if snapshot_price is None:
            print(f"[{i:3d}/{len(resolved_rows)}] SKIP (no prior log entry) — {question[:55]!r}")
            skipped += 1
            continue

        label = f"{hours_before:.1f}h prior"
        print(f"[{i:3d}/{len(resolved_rows)}] {question[:52]!r:<54} mkt={snapshot_price:.2f} ({label})",
              end=" ", flush=True)

        p = ask_claude(client, question, snapshot_price, hours_before)
        if p is None:
            print("ERROR")
            errors += 1
            time.sleep(REQUEST_DELAY)
            continue

        correct = (p >= 0.5) == (actual >= 0.5)
        marker  = "✓" if correct else "✗"
        results.append((question, snapshot_price, hours_before, actual, p))
        print(f"  Claude={p:.2f}  actual={'YES' if actual == 1.0 else 'NO '}  {marker}")
        time.sleep(REQUEST_DELAY)

    if not results:
        print("\n[ERROR] No successful estimates.")
        sys.exit(1)

    # ── Metrics ───────────────────────────────────────────────────────────────
    pairs       = [(p, o) for _, _, _, o, p in results]
    bs          = brier_score(pairs)
    overall_acc = sum(1 for p, o in pairs if (p >= 0.5) == (o >= 0.5)) / len(pairs)

    buckets = {"0–30%": [], "30–70%": [], "70–100%": []}
    for q, sp, hb, actual, p in results:
        buckets[bucket_label(p)].append((p, actual))

    # Market-price-only baseline (treat market price as the probability)
    mkt_pairs = [(sp, o) for _, sp, _, o, _ in results]
    mkt_bs    = brier_score(mkt_pairs)
    mkt_acc   = sum(1 for p, o in mkt_pairs if (p >= 0.5) == (o >= 0.5)) / len(mkt_pairs)

    print()
    print("=" * 65)
    print("  CALIBRATION REPORT")
    print("=" * 65)
    print(f"  Markets evaluated : {len(results)}  (skipped: {skipped}  errors: {errors})")
    print()
    print(f"  {'Metric':<28}  {'Claude':>8}  {'Market price':>12}")
    print(f"  {'-'*28}  {'-'*8}  {'-'*12}")
    print(f"  {'Brier score (↓ better)':<28}  {bs:>8.4f}  {mkt_bs:>12.4f}")
    print(f"  {'Overall accuracy':<28}  {overall_acc*100:>7.1f}%  {mkt_acc*100:>11.1f}%")
    print()
    print("  Accuracy by confidence bucket (Claude's estimate):")
    for label, bucket_pairs in buckets.items():
        if not bucket_pairs:
            print(f"    {label:10s}  — no predictions")
            continue
        acc    = sum(1 for p, o in bucket_pairs if (p >= 0.5) == (o >= 0.5)) / len(bucket_pairs)
        avg_p  = sum(p for p, o in bucket_pairs) / len(bucket_pairs)
        avg_o  = sum(o for p, o in bucket_pairs) / len(bucket_pairs)
        bias   = avg_p - avg_o
        print(f"    {label:10s}  n={len(bucket_pairs):3d}  acc={acc*100:5.1f}%  "
              f"avg_p={avg_p:.2f}  actual_rate={avg_o:.2f}  bias={bias:+.2f}")

    print()
    misses = [(abs(p - actual), q, sp, actual, p)
              for q, sp, _, actual, p in results
              if (p >= 0.5) != (actual >= 0.5)]
    misses.sort(reverse=True)
    if misses:
        print(f"  Top 5 biggest misses (wrong direction):")
        for diff, q, sp, actual, p in misses[:5]:
            print(f"    Claude={p:.2f}  actual={'YES' if actual==1.0 else 'NO '}  "
                  f"mkt={sp:.2f}  {q[:52]!r}")

    print()
    print("=" * 65)


if __name__ == "__main__":
    main()
