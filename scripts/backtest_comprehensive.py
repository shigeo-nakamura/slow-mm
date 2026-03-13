#!/usr/bin/env python3
"""
Comprehensive backtest: MR, Trend-Follow, MR+Hedge, MR+TrendPause
with realistic spread costs applied on both entry and exit.

Usage: python3 backtest_comprehensive.py [market_data_30d.jsonl]
"""
import json
import sys
from itertools import product

SYMBOL = "LIT"
EQUITY = 970.0
ORDER_SIZE_PCT = 0.10
EMA_SHORT = 5
EMA_LONG = 20
EMA_MACRO_SHORT = 10
EMA_MACRO_LONG = 50
COOLDOWN_TICKS = 3

SPREAD_BPS_LIST = [8, 12, 16, 20]

# Parameter grids
ENTRY_LIST = [10, 15, 20, 30, 50]
STOP_LIST = [20, 30, 50, 80]
TP_LIST = [10, 15, 20, 30, 50]
REVERT_LIST = [3, 5, 10]
TRAIL_STOP_LIST = [10, 20, 30]
MACRO_PAUSE_LIST = [50, 80, 120]
HEDGE_EXTRA_BPS = 10

MIN_TRADES_PER_10MIN = 0.5


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


def precompute_emas(prices):
    """Precompute all EMA values to avoid redundant calculation per combo."""
    alpha_s = 2.0 / (EMA_SHORT + 1)
    alpha_l = 2.0 / (EMA_LONG + 1)
    alpha_ms = 2.0 / (EMA_MACRO_SHORT + 1)
    alpha_ml = 2.0 / (EMA_MACRO_LONG + 1)

    n = len(prices)
    ema_short = [0.0] * n
    ema_long = [0.0] * n
    ema_macro_short = [0.0] * n
    ema_macro_long = [0.0] * n
    trend_bps = [0.0] * n
    macro_bps = [0.0] * n

    es = el = ems = eml = prices[0][1]
    for i, (_, mid) in enumerate(prices):
        if i == 0:
            es = el = ems = eml = mid
        else:
            es = alpha_s * mid + (1 - alpha_s) * es
            el = alpha_l * mid + (1 - alpha_l) * el
            ems = alpha_ms * mid + (1 - alpha_ms) * ems
            eml = alpha_ml * mid + (1 - alpha_ml) * eml

        ema_short[i] = es
        ema_long[i] = el
        ema_macro_short[i] = ems
        ema_macro_long[i] = eml
        trend_bps[i] = (es - el) / el * 10000.0 if el > 0 else 0.0
        macro_bps[i] = (ems - eml) / eml * 10000.0 if eml > 0 else 0.0

    return trend_bps, macro_bps


def backtest_mr(prices, trend_bps_arr, half_spread_bps, entry_bps, stop_bps, tp_bps, revert_bps):
    """Mean Reversion: fade EMA divergence, exit on TP/SL/revert."""
    direction = 0
    entry_mid = 0.0
    cooldown = 0
    trades = 0
    wins = 0
    pnl = 0.0
    peak_pnl = 0.0
    max_dd = 0.0
    hold_total = 0
    hold = 0

    for i in range(len(prices)):
        mid = prices[i][1]
        tb = trend_bps_arr[i]

        if direction != 0:
            hold += 1
            # PnL in bps relative to entry mid, minus spread on both sides
            if direction == 1:  # long: bought at mid+half, selling at mid-half
                raw_bps = (mid - entry_mid) / entry_mid * 10000.0
            else:  # short: sold at mid-half, buying at mid+half
                raw_bps = (entry_mid - mid) / entry_mid * 10000.0
            # Spread cost already accounted: half on entry + half on exit = full spread
            pnl_bps = raw_bps - 2.0 * half_spread_bps

            close = False
            if pnl_bps <= -stop_bps:
                close = True
            elif pnl_bps >= tp_bps:
                close = True
            elif abs(tb) <= revert_bps:
                close = True

            if close:
                size_usd = EQUITY * ORDER_SIZE_PCT
                trade_pnl = pnl_bps / 10000.0 * size_usd
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
            if abs(tb) >= entry_bps:
                direction = -1 if tb > 0 else 1
                entry_mid = mid
                hold = 0

    avg_hold = hold_total / trades if trades > 0 else 0
    return trades, wins, pnl, max_dd, avg_hold


def backtest_tf(prices, trend_bps_arr, half_spread_bps, entry_bps, stop_bps, tp_bps, trail_stop_bps):
    """Trend Follow: follow EMA direction, exit on SL/TP/trailing stop/reversal."""
    direction = 0
    entry_mid = 0.0
    cooldown = 0
    trades = 0
    wins = 0
    pnl = 0.0
    peak_pnl = 0.0
    max_dd = 0.0
    hold_total = 0
    hold = 0
    best_pnl_bps = 0.0

    for i in range(len(prices)):
        mid = prices[i][1]
        tb = trend_bps_arr[i]

        if direction != 0:
            hold += 1
            if direction == 1:
                raw_bps = (mid - entry_mid) / entry_mid * 10000.0
            else:
                raw_bps = (entry_mid - mid) / entry_mid * 10000.0
            pnl_bps = raw_bps - 2.0 * half_spread_bps

            if pnl_bps > best_pnl_bps:
                best_pnl_bps = pnl_bps

            close = False
            if pnl_bps <= -stop_bps:
                close = True
            elif pnl_bps >= tp_bps:
                close = True
            # Trailing stop: if we pulled back trail_stop_bps from peak
            elif best_pnl_bps > 0 and (best_pnl_bps - pnl_bps) >= trail_stop_bps:
                close = True
            # Trend reversal: EMA flipped against our direction
            elif (direction == 1 and tb < -entry_bps) or (direction == -1 and tb > entry_bps):
                close = True

            if close:
                size_usd = EQUITY * ORDER_SIZE_PCT
                trade_pnl = pnl_bps / 10000.0 * size_usd
                pnl += trade_pnl
                if trade_pnl > 0:
                    wins += 1
                trades += 1
                hold_total += hold
                hold = 0
                direction = 0
                cooldown = COOLDOWN_TICKS
                best_pnl_bps = 0.0
                peak_pnl = max(peak_pnl, pnl)
                max_dd = max(max_dd, peak_pnl - pnl)
        else:
            if cooldown > 0:
                cooldown -= 1
                continue
            if abs(tb) >= entry_bps:
                # Follow the trend: EMA up → LONG, EMA down → SHORT
                direction = 1 if tb > 0 else -1
                entry_mid = mid
                hold = 0
                best_pnl_bps = 0.0

    avg_hold = hold_total / trades if trades > 0 else 0
    return trades, wins, pnl, max_dd, avg_hold


def backtest_mr_hedge(prices, trend_bps_arr, half_spread_bps, entry_bps, stop_bps, tp_bps, revert_bps):
    """MR + Spot Hedge: same as MR but add extra round-trip hedge cost per trade."""
    trades, wins, pnl, max_dd, avg_hold = backtest_mr(
        prices, trend_bps_arr, half_spread_bps, entry_bps, stop_bps, tp_bps, revert_bps
    )
    # Each trade incurs an additional hedge round-trip cost
    hedge_cost_per_trade = HEDGE_EXTRA_BPS / 10000.0 * EQUITY * ORDER_SIZE_PCT
    pnl -= trades * hedge_cost_per_trade
    # Recompute max_dd conservatively (approximate: add total hedge cost spread evenly)
    return trades, wins, pnl, max_dd, avg_hold


def backtest_mr_trend_pause(prices, trend_bps_arr, macro_bps_arr, half_spread_bps,
                            entry_bps, stop_bps, tp_bps, revert_bps, macro_pause_bps):
    """MR + Trend Pause: skip entry when macro EMA divergence is too strong."""
    direction = 0
    entry_mid = 0.0
    cooldown = 0
    trades = 0
    wins = 0
    pnl = 0.0
    peak_pnl = 0.0
    max_dd = 0.0
    hold_total = 0
    hold = 0

    for i in range(len(prices)):
        mid = prices[i][1]
        tb = trend_bps_arr[i]
        mb = macro_bps_arr[i]

        if direction != 0:
            hold += 1
            if direction == 1:
                raw_bps = (mid - entry_mid) / entry_mid * 10000.0
            else:
                raw_bps = (entry_mid - mid) / entry_mid * 10000.0
            pnl_bps = raw_bps - 2.0 * half_spread_bps

            close = False
            if pnl_bps <= -stop_bps:
                close = True
            elif pnl_bps >= tp_bps:
                close = True
            elif abs(tb) <= revert_bps:
                close = True

            if close:
                size_usd = EQUITY * ORDER_SIZE_PCT
                trade_pnl = pnl_bps / 10000.0 * size_usd
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
            # Skip entry if macro trend is too strong (adverse to MR)
            if abs(mb) >= macro_pause_bps:
                continue
            if abs(tb) >= entry_bps:
                direction = -1 if tb > 0 else 1
                entry_mid = mid
                hold = 0

    avg_hold = hold_total / trades if trades > 0 else 0
    return trades, wins, pnl, max_dd, avg_hold


def fmt_row(params_str, trades, wins, pnl, max_dd, avg_hold, trades_per_10min, tick_sec):
    wr = wins / trades * 100 if trades > 0 else 0
    hold_s = avg_hold * tick_sec
    return (
        f"{params_str} | {trades:>6} {wins:>5} {wr:>4.0f}% | "
        f"${pnl:>8.2f} ${max_dd:>7.2f} | {trades_per_10min:>5.2f} {hold_s:>6.0f}s"
    )


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "market_data_30d.jsonl"
    print(f"Loading {path}...")
    prices = load_prices(path)
    n = len(prices)
    print(f"Loaded {n} ticks")
    if n < 2:
        print("Not enough data")
        return

    duration_ms = prices[-1][0] - prices[0][0]
    duration_min = duration_ms / 1000.0 / 60.0
    tick_sec = duration_ms / 1000.0 / (n - 1) if n > 1 else 20.0
    print(f"Duration: {duration_min:.0f} min ({duration_min/60/24:.1f} days)")
    print(f"Avg tick interval: {tick_sec:.1f}s")
    print(f"Equity: ${EQUITY:.0f}, size: {ORDER_SIZE_PCT*100:.0f}%")
    print()

    print("Precomputing EMAs...")
    trend_bps_arr, macro_bps_arr = precompute_emas(prices)

    # Grand summary: best result per strategy per spread
    grand_summary = {}  # (strategy, spread) -> (pnl, params_str, trades, wins, max_dd)

    def update_grand(strategy, spread, pnl, params_str, trades, wins, max_dd):
        key = (strategy, spread)
        if key not in grand_summary or pnl > grand_summary[key][0]:
            grand_summary[key] = (pnl, params_str, trades, wins, max_dd)

    # ---------- 1. Mean Reversion ----------
    print("=" * 120)
    print("STRATEGY 1: MEAN REVERSION (MR)")
    print("=" * 120)
    for spread_bps in SPREAD_BPS_LIST:
        half_spread = spread_bps / 2.0
        print(f"\n--- Spread: {spread_bps} bps (half={half_spread} bps) ---")
        results = []
        for e, s, t, r in product(ENTRY_LIST, STOP_LIST, TP_LIST, REVERT_LIST):
            trades, wins, pnl, max_dd, avg_hold = backtest_mr(
                prices, trend_bps_arr, half_spread, e, s, t, r
            )
            tpm = trades / (duration_min / 10.0) if duration_min > 0 else 0
            if tpm >= MIN_TRADES_PER_10MIN:
                params = f"e={e:>3} s={s:>3} t={t:>3} r={r:>3}"
                results.append((pnl, params, trades, wins, max_dd, avg_hold, tpm))
        results.sort(key=lambda x: -x[0])
        hdr_params = "                params"
        print(f"\n{hdr_params} | {'trades':>6} {'wins':>5} {'winR':>5} | {'PnL':>9} {'maxDD':>8} | {'t/10m':>5} {'hold':>6}")
        print("-" * 100)
        for row in results[:10]:
            pnl, params, trades, wins, max_dd, avg_hold, tpm = row
            print(fmt_row(params, trades, wins, pnl, max_dd, avg_hold, tpm, tick_sec))
        if results:
            best = results[0]
            update_grand("MR", spread_bps, best[0], best[1], best[2], best[3], best[4])
        print(f"  ({len(results)} combos with >= {MIN_TRADES_PER_10MIN} trades/10min)")

    # ---------- 2. Trend Follow ----------
    print("\n" + "=" * 120)
    print("STRATEGY 2: TREND FOLLOW (TF)")
    print("=" * 120)
    for spread_bps in SPREAD_BPS_LIST:
        half_spread = spread_bps / 2.0
        print(f"\n--- Spread: {spread_bps} bps (half={half_spread} bps) ---")
        results = []
        for e, s, t, tr in product(ENTRY_LIST, STOP_LIST, TP_LIST, TRAIL_STOP_LIST):
            trades, wins, pnl, max_dd, avg_hold = backtest_tf(
                prices, trend_bps_arr, half_spread, e, s, t, tr
            )
            tpm = trades / (duration_min / 10.0) if duration_min > 0 else 0
            if tpm >= MIN_TRADES_PER_10MIN:
                params = f"e={e:>3} s={s:>3} t={t:>3} tr={tr:>3}"
                results.append((pnl, params, trades, wins, max_dd, avg_hold, tpm))
        results.sort(key=lambda x: -x[0])
        hdr_params = "                 params"
        print(f"\n{hdr_params} | {'trades':>6} {'wins':>5} {'winR':>5} | {'PnL':>9} {'maxDD':>8} | {'t/10m':>5} {'hold':>6}")
        print("-" * 100)
        for row in results[:10]:
            pnl, params, trades, wins, max_dd, avg_hold, tpm = row
            print(fmt_row(params, trades, wins, pnl, max_dd, avg_hold, tpm, tick_sec))
        if results:
            best = results[0]
            update_grand("TF", spread_bps, best[0], best[1], best[2], best[3], best[4])
        print(f"  ({len(results)} combos with >= {MIN_TRADES_PER_10MIN} trades/10min)")

    # ---------- 3. MR + Spot Hedge ----------
    print("\n" + "=" * 120)
    print(f"STRATEGY 3: MR + SPOT HEDGE (extra {HEDGE_EXTRA_BPS}bps round-trip per trade)")
    print("=" * 120)
    for spread_bps in SPREAD_BPS_LIST:
        half_spread = spread_bps / 2.0
        print(f"\n--- Spread: {spread_bps} bps (half={half_spread} bps) + hedge {HEDGE_EXTRA_BPS}bps ---")
        results = []
        for e, s, t, r in product(ENTRY_LIST, STOP_LIST, TP_LIST, REVERT_LIST):
            trades, wins, pnl, max_dd, avg_hold = backtest_mr_hedge(
                prices, trend_bps_arr, half_spread, e, s, t, r
            )
            tpm = trades / (duration_min / 10.0) if duration_min > 0 else 0
            if tpm >= MIN_TRADES_PER_10MIN:
                params = f"e={e:>3} s={s:>3} t={t:>3} r={r:>3}"
                results.append((pnl, params, trades, wins, max_dd, avg_hold, tpm))
        results.sort(key=lambda x: -x[0])
        hdr_params = "                params"
        print(f"\n{hdr_params} | {'trades':>6} {'wins':>5} {'winR':>5} | {'PnL':>9} {'maxDD':>8} | {'t/10m':>5} {'hold':>6}")
        print("-" * 100)
        for row in results[:10]:
            pnl, params, trades, wins, max_dd, avg_hold, tpm = row
            print(fmt_row(params, trades, wins, pnl, max_dd, avg_hold, tpm, tick_sec))
        if results:
            best = results[0]
            update_grand("MR+Hedge", spread_bps, best[0], best[1], best[2], best[3], best[4])
        print(f"  ({len(results)} combos with >= {MIN_TRADES_PER_10MIN} trades/10min)")

    # ---------- 4. MR + Trend Pause ----------
    print("\n" + "=" * 120)
    print("STRATEGY 4: MR + TREND PAUSE (skip entry when macro EMA10/50 divergence > threshold)")
    print("=" * 120)
    for spread_bps in SPREAD_BPS_LIST:
        half_spread = spread_bps / 2.0
        print(f"\n--- Spread: {spread_bps} bps (half={half_spread} bps) ---")
        results = []
        for e, s, t, r, mp in product(ENTRY_LIST, STOP_LIST, TP_LIST, REVERT_LIST, MACRO_PAUSE_LIST):
            trades, wins, pnl, max_dd, avg_hold = backtest_mr_trend_pause(
                prices, trend_bps_arr, macro_bps_arr, half_spread, e, s, t, r, mp
            )
            tpm = trades / (duration_min / 10.0) if duration_min > 0 else 0
            if tpm >= MIN_TRADES_PER_10MIN:
                params = f"e={e:>3} s={s:>3} t={t:>3} r={r:>3} mp={mp:>4}"
                results.append((pnl, params, trades, wins, max_dd, avg_hold, tpm))
        results.sort(key=lambda x: -x[0])
        hdr_params = "                      params"
        print(f"\n{hdr_params} | {'trades':>6} {'wins':>5} {'winR':>5} | {'PnL':>9} {'maxDD':>8} | {'t/10m':>5} {'hold':>6}")
        print("-" * 100)
        for row in results[:10]:
            pnl, params, trades, wins, max_dd, avg_hold, tpm = row
            print(fmt_row(params, trades, wins, pnl, max_dd, avg_hold, tpm, tick_sec))
        if results:
            best = results[0]
            update_grand("MR+TrendPause", spread_bps, best[0], best[1], best[2], best[3], best[4])
        print(f"  ({len(results)} combos with >= {MIN_TRADES_PER_10MIN} trades/10min)")

    # ---------- Grand Summary ----------
    print("\n" + "#" * 120)
    print("GRAND SUMMARY: Best result per strategy per spread level")
    print("#" * 120)
    strategies = ["MR", "TF", "MR+Hedge", "MR+TrendPause"]
    print(f"\n{'Strategy':<16} {'Spread':>6} | {'PnL':>9} {'maxDD':>8} {'trades':>7} {'winR':>5} | Best params")
    print("-" * 110)
    for spread_bps in SPREAD_BPS_LIST:
        for strat in strategies:
            key = (strat, spread_bps)
            if key in grand_summary:
                pnl, params, trades, wins, max_dd = grand_summary[key]
                wr = wins / trades * 100 if trades > 0 else 0
                print(
                    f"{strat:<16} {spread_bps:>4}bp | "
                    f"${pnl:>8.2f} ${max_dd:>7.2f} {trades:>7} {wr:>4.0f}% | {params}"
                )
            else:
                print(f"{strat:<16} {spread_bps:>4}bp |   (no qualifying combos)")
        print()

    # Overall best
    if grand_summary:
        best_key = max(grand_summary, key=lambda k: grand_summary[k][0])
        best = grand_summary[best_key]
        strat, spread = best_key
        pnl, params, trades, wins, max_dd = best
        wr = wins / trades * 100 if trades > 0 else 0
        print(f"*** OVERALL BEST: {strat} @ {spread}bps spread")
        print(f"    {params}")
        print(f"    PnL=${pnl:.2f}  trades={trades}  winRate={wr:.0f}%  maxDD=${max_dd:.2f}")

    # Reality check at 16bps (median observed spread)
    print("\n" + "-" * 80)
    print("REALITY CHECK at 16bps spread (median observed):")
    for strat in strategies:
        key = (strat, 16)
        if key in grand_summary:
            pnl, params, trades, wins, max_dd = grand_summary[key]
            wr = wins / trades * 100 if trades > 0 else 0
            verdict = "PROFITABLE" if pnl > 0 else "UNPROFITABLE"
            print(f"  {strat:<16}: ${pnl:>8.2f} ({verdict}) | {params}")
        else:
            print(f"  {strat:<16}: no qualifying combos")


if __name__ == "__main__":
    main()
