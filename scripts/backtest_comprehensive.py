#!/usr/bin/env python3
"""
Comprehensive backtest: MR, Trend-Follow, MR+Hedge, MR+TrendPause
with spread-filtered data (wide-spread ticks removed before EMA/backtest).

Usage: python3 backtest_comprehensive.py [market_data_30d.jsonl] [--max-spread 50]
"""
import sys
import os
from itertools import product

sys.path.insert(0, os.path.dirname(__file__))
from backtest_lib import (
    load_ticks, precompute_emas,
    backtest_mr, backtest_tf, backtest_mr_hedge, backtest_mr_trend_pause,
)

EQUITY = 970.0
ORDER_SIZE_PCT = 0.10
HEDGE_EXTRA_BPS = 10

# Spread costs to model on top of the filtered data
# (residual half-spread on entry/exit within the max_spread window)
SPREAD_BPS_LIST = [0, 4, 8, 12, 16]

# Parameter grids
ENTRY_LIST = [5, 10, 15, 20, 30, 50]
STOP_LIST = [10, 20, 30, 50, 80]
TP_LIST = [5, 10, 15, 20, 30, 50]
REVERT_LIST = [3, 5, 10]
TRAIL_STOP_LIST = [10, 20, 30]
MACRO_PAUSE_LIST = [50, 80, 120]

MIN_TRADES_PER_10MIN = 0.5


def fmt_row(params_str, trades, wins, pnl, max_dd, avg_hold, trades_per_10min, tick_sec):
    wr = wins / trades * 100 if trades > 0 else 0
    hold_s = avg_hold * tick_sec
    return (
        f"{params_str} | {trades:>6} {wins:>5} {wr:>4.0f}% | "
        f"${pnl:>8.2f} ${max_dd:>7.2f} | {trades_per_10min:>5.2f} {hold_s:>6.0f}s"
    )


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "market_data_30d.jsonl"
    max_spread = 50.0
    for i, arg in enumerate(sys.argv):
        if arg == "--max-spread" and i + 1 < len(sys.argv):
            max_spread = float(sys.argv[i + 1])

    print(f"Loading {path} (max_spread={max_spread}bps)...")
    prices, stats = load_ticks(path, max_spread_bps=max_spread)
    n = len(prices)
    print(f"Loaded {stats['kept']}/{stats['total']} ticks "
          f"(skipped: {stats['skipped_wide']} wide spread, {stats['skipped_missing']} missing bid/ask)")
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

    print("Precomputing EMAs (on spread-filtered data)...")
    trend_bps_arr, macro_bps_arr = precompute_emas(prices)

    grand_summary = {}

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
        print(f"\n--- Residual spread cost: {spread_bps} bps (half={half_spread} bps) ---")
        results = []
        for e, s, t, r in product(ENTRY_LIST, STOP_LIST, TP_LIST, REVERT_LIST):
            trades, wins, pnl, max_dd, avg_hold = backtest_mr(
                prices, trend_bps_arr, e, s, t, r,
                equity=EQUITY, order_size_pct=ORDER_SIZE_PCT, half_spread_bps=half_spread,
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
        print(f"\n--- Residual spread cost: {spread_bps} bps (half={half_spread} bps) ---")
        results = []
        for e, s, t, tr in product(ENTRY_LIST, STOP_LIST, TP_LIST, TRAIL_STOP_LIST):
            trades, wins, pnl, max_dd, avg_hold = backtest_tf(
                prices, trend_bps_arr, e, s, t, tr,
                equity=EQUITY, order_size_pct=ORDER_SIZE_PCT, half_spread_bps=half_spread,
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
        print(f"\n--- Residual spread cost: {spread_bps} bps + hedge {HEDGE_EXTRA_BPS}bps ---")
        results = []
        for e, s, t, r in product(ENTRY_LIST, STOP_LIST, TP_LIST, REVERT_LIST):
            trades, wins, pnl, max_dd, avg_hold = backtest_mr_hedge(
                prices, trend_bps_arr, e, s, t, r,
                equity=EQUITY, order_size_pct=ORDER_SIZE_PCT, half_spread_bps=half_spread,
                hedge_extra_bps=HEDGE_EXTRA_BPS,
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
        print(f"\n--- Residual spread cost: {spread_bps} bps ---")
        results = []
        for e, s, t, r, mp in product(ENTRY_LIST, STOP_LIST, TP_LIST, REVERT_LIST, MACRO_PAUSE_LIST):
            trades, wins, pnl, max_dd, avg_hold = backtest_mr_trend_pause(
                prices, trend_bps_arr, macro_bps_arr,
                e, s, t, r, mp,
                equity=EQUITY, order_size_pct=ORDER_SIZE_PCT, half_spread_bps=half_spread,
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
    print(f"GRAND SUMMARY: Best result per strategy (data filtered at max_spread={max_spread}bps)")
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

    if grand_summary:
        best_key = max(grand_summary, key=lambda k: grand_summary[k][0])
        best = grand_summary[best_key]
        strat, spread = best_key
        pnl, params, trades, wins, max_dd = best
        wr = wins / trades * 100 if trades > 0 else 0
        print(f"*** OVERALL BEST: {strat} @ {spread}bps residual spread")
        print(f"    {params}")
        print(f"    PnL=${pnl:.2f}  trades={trades}  winRate={wr:.0f}%  maxDD=${max_dd:.2f}")


if __name__ == "__main__":
    main()
