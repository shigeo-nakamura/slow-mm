#!/usr/bin/env python3
"""
Backtest mean_reversion with spot hedge cost estimation.
Spot hedge adds ~10-20bps round-trip cost (spread on LIT/USDC spot).
"""
import json
import sys
from itertools import product

SYMBOL = "LIT"
EQUITY = 290.0
ORDER_SIZE_PCT = 0.05
EMA_SHORT = 5
EMA_LONG = 20
COOLDOWN_TICKS = 3

def load_data(path):
    prices = []
    with open(path) as f:
        for line in f:
            row = json.loads(line)
            ts = row["timestamp"]
            lit = row["prices"].get(SYMBOL)
            if not lit:
                continue
            price = float(lit["price"])
            bid = float(lit.get("bid_price") or 0)
            ask = float(lit.get("ask_price") or 0)
            if price > 0:
                spread_bps = 0.0
                if bid > 0 and ask > 0:
                    spread_bps = (ask - bid) / ((ask + bid) / 2) * 10000
                prices.append((ts, price, spread_bps))
    return prices

def run(prices, entry_bps, stop_bps, tp_bps, revert_bps, hedge_cost_bps):
    ema_s = ema_l = None
    alpha_s = 2.0 / (EMA_SHORT + 1)
    alpha_l = 2.0 / (EMA_LONG + 1)
    direction = 0
    entry_price = 0.0
    cooldown = 0
    trades = wins = 0
    pnl = 0.0
    peak_pnl = max_dd = 0.0

    for _, mid, _ in prices:
        if ema_s is None:
            ema_s = ema_l = mid
        else:
            ema_s = alpha_s * mid + (1 - alpha_s) * ema_s
            ema_l = alpha_l * mid + (1 - alpha_l) * ema_l
        trend = (ema_s - ema_l) / ema_l * 10000 if ema_l > 0 else 0

        if direction != 0:
            pnl_bps = ((mid - entry_price) / entry_price * 10000) * direction
            close = False
            if pnl_bps <= -stop_bps:
                close = True
            elif pnl_bps >= tp_bps:
                close = True
            elif abs(trend) <= revert_bps:
                close = True
            if close:
                size_usd = EQUITY * ORDER_SIZE_PCT
                # Net PnL = gross - hedge cost (entry + exit = 2x half-spread)
                net_bps = pnl_bps - hedge_cost_bps
                trade_pnl = net_bps / 10000 * size_usd
                pnl += trade_pnl
                if trade_pnl > 0:
                    wins += 1
                trades += 1
                direction = 0
                cooldown = COOLDOWN_TICKS
                peak_pnl = max(peak_pnl, pnl)
                max_dd = max(max_dd, peak_pnl - pnl)
        else:
            if cooldown > 0:
                cooldown -= 1
                continue
            if abs(trend) >= entry_bps:
                direction = -1 if trend > 0 else 1
                entry_price = mid

    duration_min = (prices[-1][0] - prices[0][0]) / 60000 if len(prices) >= 2 else 1
    return trades, wins, pnl, max_dd, trades / (duration_min / 10)

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "market_data_30d.jsonl"
    print(f"Loading {path}...")
    data = load_data(path)
    print(f"Loaded {len(data)} ticks")

    # Compute average spot spread
    spreads = [s for _, _, s in data if s > 0]
    avg_spread = sum(spreads) / len(spreads) if spreads else 0
    print(f"Average LIT spot spread: {avg_spread:.1f}bps")
    print()

    # Best params from previous backtest
    entry, stop, tp, revert = 15.0, 30.0, 5.0, 5.0

    print(f"Params: entry={entry} stop={stop} tp={tp} revert={revert}")
    print(f"{'hedge_cost':>12} | {'trades':>7} {'wins':>5} {'winR':>5} | {'PnL':>8} {'maxDD':>7} | {'t/10m':>6}")
    print("=" * 75)

    for hedge_cost in [0, 2, 5, 8, 10, 15, 20, 25, 30]:
        t, w, p, dd, freq = run(data, entry, stop, tp, revert, hedge_cost)
        wr = w / t * 100 if t > 0 else 0
        print(f"{hedge_cost:>10.0f}bps | {t:>7} {w:>5} {wr:>4.0f}% | ${p:>7.2f} ${dd:>6.2f} | {freq:>5.2f}")

    # Also test WITHOUT hedge (hedge_cost=0) but with different params
    print(f"\n{'='*75}")
    print("Optimization WITHOUT spot hedge (hedge_cost=0):")
    print(f"{'entry':>6} {'stop':>6} {'tp':>6} {'revert':>6} | {'trades':>7} {'winR':>5} | {'PnL':>8} | {'t/10m':>6}")
    print("=" * 75)
    best_pnl = -999
    best = None
    for e in [10, 15, 20]:
        for s in [20, 30]:
            for t in [3, 5, 8]:
                for r in [3, 5]:
                    tr, w, p, dd, freq = run(data, e, s, t, r, 0)
                    wr = w / tr * 100 if tr > 0 else 0
                    if freq >= 1.0 and p > best_pnl:
                        best_pnl = p
                        best = (e, s, t, r, tr, wr, p, freq)

    if best:
        e, s, t, r, tr, wr, p, freq = best
        print(f"BEST no-hedge: entry={e} stop={s} tp={t} revert={r}")
        print(f"  trades={tr} winR={wr:.0f}% PnL=${p:.2f} freq={freq:.2f}/10min")

    print(f"\n{'='*75}")
    print("Optimization WITH hedge cost=10bps (typical LIT/USDC half-spread):")
    best_pnl = -999
    best = None
    for e in [10, 15, 20]:
        for s in [20, 30, 50]:
            for t in [10, 15, 20, 25, 30]:
                for r in [3, 5, 8]:
                    tr, w, p, dd, freq = run(data, e, s, t, r, 10)
                    wr = w / tr * 100 if tr > 0 else 0
                    if freq >= 0.5 and p > best_pnl:
                        best_pnl = p
                        best = (e, s, t, r, tr, wr, p, dd, freq)
    if best:
        e, s, t, r, tr, wr, p, dd, freq = best
        print(f"BEST with 10bps hedge cost: entry={e} stop={s} tp={t} revert={r}")
        print(f"  trades={tr} winR={wr:.0f}% PnL=${p:.2f} maxDD=${dd:.2f} freq={freq:.2f}/10min")
    else:
        print("No profitable combo with 10bps hedge cost")

    print(f"\n{'='*75}")
    print("Optimization WITHOUT spot hedge at all:")
    best_pnl = -999
    best = None
    for e in [10, 15, 20]:
        for s in [20, 30, 50]:
            for t in [3, 5, 8, 10, 15]:
                for r in [2, 3, 5]:
                    tr, w, p, dd, freq = run(data, e, s, t, r, 0)
                    wr = w / tr * 100 if tr > 0 else 0
                    if freq >= 1.0 and p > best_pnl:
                        best_pnl = p
                        best = (e, s, t, r, tr, wr, p, dd, freq)
    if best:
        e, s, t, r, tr, wr, p, dd, freq = best
        print(f"BEST no hedge: entry={e} stop={s} tp={t} revert={r}")
        print(f"  trades={tr} winR={wr:.0f}% PnL=${p:.2f} maxDD=${dd:.2f} freq={freq:.2f}/10min")

if __name__ == "__main__":
    main()
