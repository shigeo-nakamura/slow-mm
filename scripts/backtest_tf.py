#!/usr/bin/env python3
"""
Backtest trend_follow strategy on market_data_30d.jsonl
Optimizes: tf_entry_threshold_bps, stop_loss_bps, tf_take_profit_bps, tf_trail_stop_bps
Target: at least 1 trade per 10 minutes, maximize PnL
"""
import json
import sys
from dataclasses import dataclass, field
from itertools import product

SYMBOL = "LIT"
EQUITY = 290.0       # approximate equity
ORDER_SIZE_PCT = 0.05 # 5% of equity per trade
EMA_SHORT = 5
EMA_LONG = 20
COOLDOWN_TICKS = 3   # cooldown in ticks (~60s at 20s interval)

@dataclass
class Result:
    entry_bps: float
    stop_bps: float
    tp_bps: float
    trail_bps: float
    trades: int
    wins: int
    pnl: float
    max_dd: float
    avg_hold_ticks: float
    trades_per_10min: float

def load_prices(path: str) -> list[tuple[int, float]]:
    """Load LIT mid prices from JSONL."""
    prices = []
    with open(path) as f:
        for line in f:
            row = json.loads(line)
            ts = row["timestamp"]
            lit = row["prices"].get(SYMBOL)
            if not lit:
                continue
            price = float(lit["price"])
            if price > 0:
                prices.append((ts, price))
    return prices

def run_backtest(prices, entry_bps, stop_bps, tp_bps, trail_bps) -> Result:
    ema_s = None
    ema_l = None
    alpha_s = 2.0 / (EMA_SHORT + 1)
    alpha_l = 2.0 / (EMA_LONG + 1)

    direction = 0  # 0=flat, 1=long, -1=short
    entry_price = 0.0
    peak_price = 0.0
    cooldown = 0

    trades = 0
    wins = 0
    pnl = 0.0
    peak_pnl = 0.0
    max_dd = 0.0
    hold_ticks_total = 0
    hold_ticks = 0

    for _, mid in prices:
        # Update EMA
        if ema_s is None:
            ema_s = mid
            ema_l = mid
        else:
            ema_s = alpha_s * mid + (1 - alpha_s) * ema_s
            ema_l = alpha_l * mid + (1 - alpha_l) * ema_l

        trend_bps = (ema_s - ema_l) / ema_l * 10000.0 if ema_l > 0 else 0.0

        if direction != 0:
            hold_ticks += 1
            # PnL in bps
            if direction == 1:
                pnl_bps = (mid - entry_price) / entry_price * 10000.0
                new_peak = max(peak_price, mid)
                trail_dist = (new_peak - mid) / new_peak * 10000.0
            else:
                pnl_bps = (entry_price - mid) / entry_price * 10000.0
                new_peak = min(peak_price, mid) if peak_price > 0 else mid
                trail_dist = (mid - new_peak) / new_peak * 10000.0
            peak_price = new_peak

            close = False
            # Stop-loss
            if pnl_bps <= -stop_bps:
                close = True
            # Take-profit
            elif pnl_bps >= tp_bps:
                close = True
            # Trailing stop (only in profit)
            elif pnl_bps > 0 and trail_dist >= trail_bps:
                close = True
            # Trend reversal
            elif direction == 1 and trend_bps < -(entry_bps / 2):
                close = True
            elif direction == -1 and trend_bps > (entry_bps / 2):
                close = True

            if close:
                size_usd = EQUITY * ORDER_SIZE_PCT
                size_tokens = size_usd / entry_price
                trade_pnl = pnl_bps / 10000.0 * entry_price * size_tokens
                pnl += trade_pnl
                if trade_pnl > 0:
                    wins += 1
                trades += 1
                hold_ticks_total += hold_ticks
                hold_ticks = 0
                direction = 0
                cooldown = COOLDOWN_TICKS

                peak_pnl = max(peak_pnl, pnl)
                dd = peak_pnl - pnl
                max_dd = max(max_dd, dd)
        else:
            if cooldown > 0:
                cooldown -= 1
                continue

            # Entry signal
            if abs(trend_bps) >= entry_bps:
                direction = 1 if trend_bps > 0 else -1
                entry_price = mid
                peak_price = mid
                hold_ticks = 0

    # Duration in minutes
    if len(prices) >= 2:
        duration_min = (prices[-1][0] - prices[0][0]) / 1000.0 / 60.0
    else:
        duration_min = 1.0

    trades_per_10min = trades / (duration_min / 10.0) if duration_min > 0 else 0

    avg_hold = hold_ticks_total / trades if trades > 0 else 0

    return Result(
        entry_bps=entry_bps,
        stop_bps=stop_bps,
        tp_bps=tp_bps,
        trail_bps=trail_bps,
        trades=trades,
        wins=wins,
        pnl=pnl,
        max_dd=max_dd,
        avg_hold_ticks=avg_hold,
        trades_per_10min=trades_per_10min,
    )

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "market_data_30d.jsonl"
    print(f"Loading {path}...")
    prices = load_prices(path)
    print(f"Loaded {len(prices)} price points")
    if len(prices) < 100:
        print("Not enough data")
        return

    duration_min = (prices[-1][0] - prices[0][0]) / 1000.0 / 60.0
    print(f"Duration: {duration_min:.0f} min ({duration_min/60/24:.1f} days)")
    print()

    # Parameter grid
    entry_bps_list = [2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0]
    stop_bps_list = [10.0, 15.0, 20.0, 30.0]
    tp_bps_list = [15.0, 25.0, 35.0, 50.0]
    trail_bps_list = [8.0, 12.0, 15.0, 20.0]

    total = len(entry_bps_list) * len(stop_bps_list) * len(tp_bps_list) * len(trail_bps_list)
    print(f"Running {total} parameter combinations...")

    results = []
    for entry_bps, stop_bps, tp_bps, trail_bps in product(
        entry_bps_list, stop_bps_list, tp_bps_list, trail_bps_list
    ):
        r = run_backtest(prices, entry_bps, stop_bps, tp_bps, trail_bps)
        results.append(r)

    # Filter: at least 1 trade per 10 min
    freq_ok = [r for r in results if r.trades_per_10min >= 1.0]
    print(f"\n{len(freq_ok)} / {len(results)} combos have >= 1 trade/10min")

    if not freq_ok:
        # Relax constraint
        freq_ok = [r for r in results if r.trades_per_10min >= 0.5]
        print(f"Relaxed to >= 0.5 trade/10min: {len(freq_ok)} combos")

    if not freq_ok:
        freq_ok = sorted(results, key=lambda r: -r.trades_per_10min)[:20]
        print(f"Showing top 20 by frequency")

    # Sort by PnL
    freq_ok.sort(key=lambda r: -r.pnl)

    print(f"\n{'='*100}")
    print(f"{'entry':>6} {'stop':>6} {'tp':>6} {'trail':>6} | {'trades':>7} {'wins':>5} {'winR':>5} | {'PnL':>8} {'maxDD':>7} | {'t/10m':>6} {'avgHold':>7}")
    print(f"{'='*100}")
    for r in freq_ok[:30]:
        win_rate = r.wins / r.trades * 100 if r.trades > 0 else 0
        hold_sec = r.avg_hold_ticks * 20  # ~20s per tick
        print(
            f"{r.entry_bps:>6.1f} {r.stop_bps:>6.1f} {r.tp_bps:>6.1f} {r.trail_bps:>6.1f} | "
            f"{r.trades:>7} {r.wins:>5} {win_rate:>4.0f}% | "
            f"${r.pnl:>7.2f} ${r.max_dd:>6.2f} | "
            f"{r.trades_per_10min:>5.2f} {hold_sec:>6.0f}s"
        )

    # Best overall (PnL > 0, freq >= 1)
    profitable = [r for r in freq_ok if r.pnl > 0]
    if profitable:
        best = profitable[0]
        print(f"\n*** BEST: entry={best.entry_bps} stop={best.stop_bps} tp={best.tp_bps} trail={best.trail_bps}")
        print(f"    PnL=${best.pnl:.2f} trades={best.trades} winRate={best.wins/best.trades*100:.0f}% maxDD=${best.max_dd:.2f} freq={best.trades_per_10min:.2f}/10min")
    else:
        print("\n*** No profitable combination found with desired frequency")
        # Show least negative
        best = freq_ok[0] if freq_ok else results[0]
        print(f"    Least bad: entry={best.entry_bps} stop={best.stop_bps} tp={best.tp_bps} trail={best.trail_bps}")
        print(f"    PnL=${best.pnl:.2f} trades={best.trades}")

if __name__ == "__main__":
    main()
