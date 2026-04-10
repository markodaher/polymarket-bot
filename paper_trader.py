"""
paper_trader.py
---------------
Simulated paper-trading engine driven by signals.csv.

Every cycle:
  1. Read signals.csv for new signals not yet turned into trades.
  2. Log each new signal as an open paper trade in paper_trades.csv,
     deducting $STAKE from paper_balance.txt.
  3. Cross-reference open trades against polymarket_resolved.csv;
     mark resolved trades won/lost and update the balance.

Payout model (binary prediction market):
  YES trade at entry_price p → payout = stake / p  if YES wins
  NO  trade at entry_price p → payout = stake / p  if NO  wins
  where p is the side's market price (1 - polymarket_price for NO trades).

Files:
  paper_trades.csv   — trade log
  paper_balance.txt  — single float, running balance

Usage (standalone):
    python paper_trader.py

Called as a library by dashboard.py:
    import paper_trader
    paper_trader.watch_loop()   # blocks forever
"""

import csv
import os
import time
from datetime import datetime, timezone

# ─── CONFIG ──────────────────────────────────────────────────────────────────

_DATA_DIR       = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
SIGNALS_FILE    = os.path.join(_DATA_DIR, "signals.csv")
RESOLVED_FILE   = os.path.join(_DATA_DIR, "polymarket_resolved.csv")
TRADES_FILE     = os.path.join(_DATA_DIR, "paper_trades.csv")
BALANCE_FILE    = os.path.join(_DATA_DIR, "paper_balance.txt")

STARTING_BALANCE = 30.00
STAKE            = 1.00     # fixed stake per trade ($)
DRY_RUN          = True     # if True, log trades but do not touch the balance
WATCH_INTERVAL   = 60       # seconds between cycles

TRADE_HEADERS = [
    "timestamp", "market_id", "question",
    "side", "entry_price", "stake", "potential_payout", "status",
]

# ─── BALANCE ─────────────────────────────────────────────────────────────────

def read_balance():
    if not os.path.exists(BALANCE_FILE):
        return STARTING_BALANCE
    try:
        with open(BALANCE_FILE, encoding="utf-8") as f:
            return float(f.read().strip())
    except (ValueError, OSError):
        return STARTING_BALANCE


def write_balance(amount):
    with open(BALANCE_FILE, "w", encoding="utf-8") as f:
        f.write(f"{amount:.4f}\n")


# ─── CSV HELPERS ─────────────────────────────────────────────────────────────

def init_trades_csv():
    if not os.path.exists(TRADES_FILE):
        with open(TRADES_FILE, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=TRADE_HEADERS).writeheader()
        print(f"[PAPER] Created {TRADES_FILE}")


def load_trades():
    if not os.path.exists(TRADES_FILE):
        return []
    with open(TRADES_FILE, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_trades(trades):
    with open(TRADES_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=TRADE_HEADERS)
        w.writeheader()
        w.writerows(trades)


def load_signals():
    if not os.path.exists(SIGNALS_FILE):
        return []
    with open(SIGNALS_FILE, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_resolved_map():
    """Return {market_id: outcome_float} for all resolved markets."""
    if not os.path.exists(RESOLVED_FILE):
        return {}
    with open(RESOLVED_FILE, newline="", encoding="utf-8") as f:
        return {r["market_id"]: float(r["outcome"]) for r in csv.DictReader(f)}


# ─── TRADE LOGIC ─────────────────────────────────────────────────────────────

MAX_PAYOUT_MULTIPLE = 20   # cap: never pay out more than 20x stake

def open_trade(signal, balance):
    """
    Create a new paper trade row from a signal dict.
    Returns (trade_row, updated_balance).

    Payout model (binary prediction market):
      YES trade: payout = stake / p          where p = polymarket YES price
      NO  trade: payout = stake / (1 - p)   where p = polymarket YES price
    Capped at MAX_PAYOUT_MULTIPLE × stake to avoid unrealistic payouts on
    near-zero priced markets.
    """
    side      = signal["recommended_side"]          # "YES" or "NO"
    mkt_price = float(signal["polymarket_price"])   # polymarket YES price

    # Price of the side we're buying
    if side == "YES":
        entry_price = mkt_price
    else:
        entry_price = 1.0 - mkt_price

    entry_price = max(round(entry_price, 4), 0.01)  # guard against zero

    raw_payout       = STAKE / entry_price
    potential_payout = round(min(raw_payout, STAKE * MAX_PAYOUT_MULTIPLE), 4)

    trade = {
        "timestamp"       : signal["timestamp"],
        "market_id"       : signal["market_id"],
        "question"        : signal["question"],
        "side"            : side,
        "entry_price"     : entry_price,
        "stake"           : STAKE,
        "potential_payout": potential_payout,
        "status"          : "open",
    }
    new_balance = balance if DRY_RUN else round(balance - STAKE, 4)
    return trade, new_balance


def settle_trades(trades, resolved_map, balance):
    """
    For each open trade whose market is in resolved_map, determine won/lost,
    update balance (unless DRY_RUN), and mark the trade.
    Returns (updated_trades, new_balance, changed).
    """
    changed = False
    for t in trades:
        if t["status"] != "open":
            continue
        outcome = resolved_map.get(t["market_id"])
        if outcome is None:
            continue

        side = t["side"]
        won  = (side == "YES" and outcome >= 0.99) or (side == "NO" and outcome <= 0.01)

        if won:
            t["status"] = "won"
            if not DRY_RUN:
                balance = round(balance + float(t["potential_payout"]), 4)
            dry_tag = " [DRY RUN]" if DRY_RUN else ""
            print(f"  [PAPER] WON  +${float(t['potential_payout']):.2f}{dry_tag}  {t['question'][:55]!r}")
        else:
            t["status"] = "lost"
            dry_tag = " [DRY RUN]" if DRY_RUN else ""
            print(f"  [PAPER] LOST -${float(t['stake']):.2f}{dry_tag}  {t['question'][:55]!r}")
        changed = True

    return trades, balance, changed


# ─── WATCH LOOP ──────────────────────────────────────────────────────────────

def watch_loop():
    """Infinite loop — called by dashboard.py's background thread."""
    init_trades_csv()
    if not os.path.exists(BALANCE_FILE):
        write_balance(STARTING_BALANCE)
    print(f"[PAPER] Starting — balance: ${read_balance():.2f}  dry_run={DRY_RUN}")

    cycle = 0
    while True:
        cycle += 1
        balance      = read_balance()
        trades       = load_trades()
        signals      = load_signals()
        resolved_map = load_resolved_map()

        # Keys already turned into trades (deduplicate by market_id + timestamp)
        existing_keys = {(t["market_id"], t["timestamp"]) for t in trades}

        # ── Open new trades ───────────────────────────────────────────────────
        new_trades = 0
        for sig in signals:
            key = (sig["market_id"], sig["timestamp"])
            if key in existing_keys:
                continue
            if balance < STAKE:
                print(f"[PAPER] Insufficient balance (${balance:.2f}) — skipping new trades.")
                break
            trade, balance = open_trade(sig, balance)
            trades.append(trade)
            existing_keys.add(key)
            new_trades += 1
            dry_tag = " [DRY RUN]" if DRY_RUN else ""
            print(f"  [PAPER] OPEN {trade['side']:<3}  entry={trade['entry_price']:.2f}"
                  f"  payout=${trade['potential_payout']:.2f}"
                  f"  bal=${balance:.2f}{dry_tag}  {trade['question'][:45]!r}")

        # ── Settle resolved trades ────────────────────────────────────────────
        trades, balance, settled = settle_trades(trades, resolved_map, balance)

        if new_trades or settled:
            save_trades(trades)
            write_balance(balance)

        open_count   = sum(1 for t in trades if t["status"] == "open")
        won_count    = sum(1 for t in trades if t["status"] == "won")
        lost_count   = sum(1 for t in trades if t["status"] == "lost")
        print(f"[PAPER Cycle {cycle}] bal=${balance:.2f}  "
              f"open={open_count}  won={won_count}  lost={lost_count}")

        time.sleep(WATCH_INTERVAL)


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    watch_loop()
