#!/usr/bin/env python3
"""
Backtest mean_reversion with regime detection:
- Volatility-based: only trade when realized vol is low (range-bound)
- Partial hedge: hedge 50% or 0% based on regime
"""
import json
import sys
import math
from itertools import product

SYMBOL = "LIT"
EQUITY = 290.0
ORDER_SIZE_PCT = 0.05
EMA_SHORT = 5
EMA_LONG = 20
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

def compute_realized_vol(history, window=30):
    """Compute realized volatility in bps from recent returns."""
    if len(history) < 2:
        return 0.0
    returns = []
    for i in range(max(0, len(history) - window), len(history) - 1):
        r = (history[i+1] - history[i]) / history[i] * 10000
        returns.append(r)
    if len(returns) < 2:
        return 0.0
    mean = sum(returns) / len(returns)
    var = sum((r - mean)**2 for r in returns) / len(returns)
    return math.sqrt(var)

def run(prices, entry_bps, stop_bps, tp_bps, revert_bps, hedge_cost_bps,
        vol_window=30, vol_threshold=None, use_trend_pause=False, trend_pause_bps=50):
    ema_s = ema_l = None
    alpha_s = 2.0 / (EMA_SHORT + 1)
    alpha_l = 2.0 / (EMA_LONG + 1)
    direction = 0
    entry_price = 0.0
    cooldown = 0
    trades = wins = 0
    pnl = 0.0
    peak_pnl = max_dd = 0.0
    skipped_vol = skipped_trend = 0
    price_history = []

    # Longer EMA for trend detection
    ema_trend_s = ema_trend_l = None
    alpha_ts = 2.0 / (10 + 1)  # EMA10
    alpha_tl = 2.0 / (50 + 1)  # EMA50

    for _, mid in prices:
        if ema_s is None:
            ema_s = ema_l = mid
            ema_trend_s = ema_trend_l = mid
        else:
            ema_s = alpha_s * mid + (1 - alpha_s) * ema_s
            ema_l = alpha_l * mid + (1 - alpha_l) * ema_l
            ema_trend_s = alpha_ts * mid + (1 - alpha_ts) * ema_trend_s
            ema_trend_l = alpha_tl * mid + (1 - alpha_tl) * ema_trend_l

        price_history.append(mid)
        trend = (ema_s - ema_l) / ema_l * 10000 if ema_l > 0 else 0
        macro_trend = (ema_trend_s - ema_trend_l) / ema_trend_l * 10000 if ema_trend_l > 0 else 0

        vol = compute_realized_vol(price_history, vol_window)

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

            # Regime filter: skip entry if volatility too high
            if vol_threshold is not None and vol > vol_threshold:
                skipped_vol += 1
                continue

            # Trend pause: skip if macro trend is strong
            if use_trend_pause and abs(macro_trend) > trend_pause_bps:
                skipped_trend += 1
                continue

            if abs(trend) >= entry_bps:
                direction = -1 if trend > 0 else 1
                entry_price = mid

    duration_min = (prices[-1][0] - prices[0][0]) / 60000 if len(prices) >= 2 else 1
    freq = trades / (duration_min / 10) if duration_min > 0 else 0
    return trades, wins, pnl, max_dd, freq, skipped_vol, skipped_trend

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "market_data_30d.jsonl"
    print(f"Loading {path}...")
    prices = load_prices(path)
    print(f"Loaded {len(prices)} ticks\n")

    # Baseline: no regime filter, no hedge
    print("=" * 80)
    print("1. BASELINE (no filter, no hedge)")
    t, w, p, dd, freq, _, _ = run(prices, 15, 30, 5, 5, 0)
    print(f"   PnL=${p:.2f} trades={t} winR={w/t*100:.0f}% maxDD=${dd:.2f} freq={freq:.2f}/10m")

    # With hedge cost
    print("\n2. BASELINE + 10bps hedge cost")
    t, w, p, dd, freq, _, _ = run(prices, 15, 30, 5, 5, 10)
    print(f"   PnL=${p:.2f} trades={t} winR={w/t*100:.0f}% maxDD=${dd:.2f}")

    # Volatility filter
    print("\n" + "=" * 80)
    print("3. VOLATILITY FILTER (skip high-vol = trending periods)")
    print(f"{'vol_thresh':>12} | {'trades':>6} {'winR':>5} | {'PnL':>8} {'maxDD':>7} | {'t/10m':>6} {'skipped':>8}")
    print("-" * 75)
    for vt in [3, 5, 7, 10, 15, 20, 30, None]:
        t, w, p, dd, freq, sv, _ = run(prices, 15, 30, 5, 5, 0, vol_threshold=vt)
        wr = w / t * 100 if t > 0 else 0
        label = f"{vt}bps" if vt else "None"
        print(f"{label:>12} | {t:>6} {wr:>4.0f}% | ${p:>7.2f} ${dd:>6.2f} | {freq:>5.2f} {sv:>8}")

    # Trend pause (EMA10 vs EMA50)
    print("\n" + "=" * 80)
    print("4. TREND PAUSE (skip when EMA10/EMA50 diverges > threshold)")
    print(f"{'trend_pause':>12} | {'trades':>6} {'winR':>5} | {'PnL':>8} {'maxDD':>7} | {'t/10m':>6} {'skipped':>8}")
    print("-" * 75)
    for tp in [20, 30, 50, 80, 100, 200]:
        t, w, p, dd, freq, _, st = run(prices, 15, 30, 5, 5, 0, use_trend_pause=True, trend_pause_bps=tp)
        wr = w / t * 100 if t > 0 else 0
        print(f"{tp:>10}bps | {t:>6} {wr:>4.0f}% | ${p:>7.2f} ${dd:>6.2f} | {freq:>5.2f} {st:>8}")

    # Combined: vol filter + trend pause, no hedge
    print("\n" + "=" * 80)
    print("5. COMBINED (vol filter + trend pause, no hedge)")
    best_p = -999
    best = None
    for vt in [5, 10, 15, 20, None]:
        for tp in [30, 50, 80, 200]:
            t, w, p, dd, freq, sv, st = run(prices, 15, 30, 5, 5, 0, vol_threshold=vt, use_trend_pause=True, trend_pause_bps=tp)
            if t > 0 and freq >= 0.5 and p > best_p:
                best_p = p
                best = (vt, tp, t, w, p, dd, freq, sv, st)
    if best:
        vt, tp, t, w, p, dd, freq, sv, st = best
        wr = w / t * 100
        print(f"   BEST: vol_thresh={vt}, trend_pause={tp}bps")
        print(f"   PnL=${p:.2f} trades={t} winR={wr:.0f}% maxDD=${dd:.2f} freq={freq:.2f}/10m")
        print(f"   Skipped: vol={sv} trend={st}")

    # Summary recommendation
    print("\n" + "=" * 80)
    print("RECOMMENDATION:")
    print("  Compare: no-hedge baseline=$165, best-filtered no-hedge, hedge-with-cost")

if __name__ == "__main__":
    main()
