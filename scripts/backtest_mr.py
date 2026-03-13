#!/usr/bin/env python3
"""
Backtest mean-reversion strategy on market_data_30d.jsonl
Entry: EMA divergence exceeds threshold → trade AGAINST the trend (fade the move)
Exit: revert to mean (EMA converge) or stop-loss
"""
import json
import sys
from dataclasses import dataclass
from itertools import product

SYMBOL = "LIT"
EQUITY = 290.0
ORDER_SIZE_PCT = 0.05
DEFAULT_EMA_SHORT = 5
DEFAULT_EMA_LONG = 20
COOLDOWN_TICKS = 3

def load_prices(path):
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

@dataclass
class Result:
    entry_bps: float
    stop_bps: float
    tp_bps: float
    revert_bps: float  # close when EMA converges within this
    ema_short: int
    ema_long: int
    trades: int
    wins: int
    pnl: float
    max_dd: float
    avg_hold_ticks: float
    trades_per_10min: float

def run_backtest(prices, entry_bps, stop_bps, tp_bps, revert_bps,
                 ema_short=None, ema_long=None):
    ema_short = ema_short or DEFAULT_EMA_SHORT
    ema_long = ema_long or DEFAULT_EMA_LONG
    ema_s = None
    ema_l = None
    alpha_s = 2.0 / (ema_short + 1)
    alpha_l = 2.0 / (ema_long + 1)

    direction = 0
    entry_price = 0.0
    cooldown = 0

    trades = 0
    wins = 0
    pnl = 0.0
    peak_pnl = 0.0
    max_dd = 0.0
    hold_total = 0
    hold = 0

    for _, mid in prices:
        if ema_s is None:
            ema_s = mid
            ema_l = mid
        else:
            ema_s = alpha_s * mid + (1 - alpha_s) * ema_s
            ema_l = alpha_l * mid + (1 - alpha_l) * ema_l

        trend_bps = (ema_s - ema_l) / ema_l * 10000.0 if ema_l > 0 else 0.0

        if direction != 0:
            hold += 1
            if direction == 1:
                pnl_bps = (mid - entry_price) / entry_price * 10000.0
            else:
                pnl_bps = (entry_price - mid) / entry_price * 10000.0

            close = False
            # Stop-loss
            if pnl_bps <= -stop_bps:
                close = True
            # Take-profit
            elif pnl_bps >= tp_bps:
                close = True
            # Mean reversion: EMA converged back
            elif abs(trend_bps) <= revert_bps:
                close = True

            if close:
                size_usd = EQUITY * ORDER_SIZE_PCT
                size_tokens = size_usd / entry_price
                trade_pnl = pnl_bps / 10000.0 * entry_price * size_tokens
                pnl += trade_pnl
                if trade_pnl > 0:
                    wins += 1
                trades += 1
                hold_total += hold
                hold = 0
                direction = 0
                cooldown = COOLDOWN_TICKS
                peak_pnl = max(peak_pnl, pnl)
                max_dd = max(max_dd, peak_pnl - pnl)
        else:
            if cooldown > 0:
                cooldown -= 1
                continue
            # Mean reversion entry: FADE the trend
            if abs(trend_bps) >= entry_bps:
                # If EMA says uptrend → SHORT (expect reversion down)
                # If EMA says downtrend → LONG (expect reversion up)
                direction = -1 if trend_bps > 0 else 1
                entry_price = mid
                hold = 0

    duration_min = (prices[-1][0] - prices[0][0]) / 1000.0 / 60.0 if len(prices) >= 2 else 1.0
    trades_per_10min = trades / (duration_min / 10.0) if duration_min > 0 else 0
    avg_hold = hold_total / trades if trades > 0 else 0

    return Result(
        entry_bps=entry_bps, stop_bps=stop_bps, tp_bps=tp_bps,
        revert_bps=revert_bps, ema_short=ema_short, ema_long=ema_long,
        trades=trades, wins=wins, pnl=pnl,
        max_dd=max_dd, avg_hold_ticks=avg_hold,
        trades_per_10min=trades_per_10min,
    )

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "market_data_30d.jsonl"
    print(f"Loading {path}...")
    prices = load_prices(path)
    print(f"Loaded {len(prices)} ticks")
    duration_min = (prices[-1][0] - prices[0][0]) / 1000.0 / 60.0
    print(f"Duration: {duration_min:.0f} min ({duration_min/60/24:.1f} days)\n")

    entry_list = [2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0]
    stop_list = [5.0, 10.0, 15.0, 20.0, 30.0]
    tp_list = [3.0, 5.0, 8.0, 10.0, 15.0, 20.0]
    revert_list = [0.5, 1.0, 2.0, 3.0, 5.0]
    ema_short_list = [5, 10, 20]
    ema_long_list = [20, 40, 80]
    ema_pairs = [(s, l) for s in ema_short_list for l in ema_long_list if l > s]

    total = len(entry_list) * len(stop_list) * len(tp_list) * len(revert_list) * len(ema_pairs)
    print(f"Running {total} combinations ({len(ema_pairs)} EMA pairs)...")

    results = []
    for es, el in ema_pairs:
        for e, s, t, r in product(entry_list, stop_list, tp_list, revert_list):
            res = run_backtest(prices, e, s, t, r, ema_short=es, ema_long=el)
            results.append(res)

    # Filter by frequency
    freq_ok = [r for r in results if r.trades_per_10min >= 1.0]
    print(f"\n{len(freq_ok)} / {len(results)} have >= 1 trade/10min")

    if len(freq_ok) < 5:
        freq_ok = [r for r in results if r.trades_per_10min >= 0.5]
        print(f"Relaxed to >= 0.5/10min: {len(freq_ok)}")
    if len(freq_ok) < 5:
        freq_ok = sorted(results, key=lambda r: -r.trades_per_10min)[:50]

    freq_ok.sort(key=lambda r: -r.pnl)

    print(f"\n{'='*130}")
    print(f"{'EMA':>7} {'entry':>6} {'stop':>6} {'tp':>6} {'revert':>6} | {'trades':>7} {'wins':>5} {'winR':>5} | {'PnL':>8} {'maxDD':>7} | {'t/10m':>6} {'avgHold':>7}")
    print(f"{'='*130}")
    for r in freq_ok[:40]:
        wr = r.wins / r.trades * 100 if r.trades > 0 else 0
        hs = r.avg_hold_ticks * 20
        print(
            f"{r.ema_short:>2}/{r.ema_long:<3} {r.entry_bps:>6.1f} {r.stop_bps:>6.1f} {r.tp_bps:>6.1f} {r.revert_bps:>6.1f} | "
            f"{r.trades:>7} {r.wins:>5} {wr:>4.0f}% | "
            f"${r.pnl:>7.2f} ${r.max_dd:>6.2f} | "
            f"{r.trades_per_10min:>5.2f} {hs:>6.0f}s"
        )

    profitable = [r for r in freq_ok if r.pnl > 0]
    if profitable:
        best = profitable[0]
        print(f"\n*** BEST: ema={best.ema_short}/{best.ema_long} entry={best.entry_bps} stop={best.stop_bps} tp={best.tp_bps} revert={best.revert_bps}")
        print(f"    PnL=${best.pnl:.2f} trades={best.trades} winRate={best.wins/best.trades*100:.0f}% maxDD=${best.max_dd:.2f} freq={best.trades_per_10min:.2f}/10min")
    else:
        print("\n*** No profitable mean-reversion combo found either")

if __name__ == "__main__":
    main()
