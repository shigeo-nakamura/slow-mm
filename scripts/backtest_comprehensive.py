#!/usr/bin/env python3
"""
Comprehensive backtest: MR, Trend-Follow, MR+Hedge, MR+TrendPause
with spread-filtered data (wide-spread ticks removed before EMA/backtest).

Usage: python3 backtest_comprehensive.py [market_data_30d.jsonl] [--max-spread 50] [--symbol HYPE]
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

# EMA grids
EMA_SHORT_LIST = [3, 5, 8, 12]
EMA_LONG_LIST = [15, 20, 30, 50]
EMA_MACRO_SHORT_LIST = [8, 10, 15]
EMA_MACRO_LONG_LIST = [40, 50, 80]

# Regime grids
REGIME_MID_LIST = [5.0, 10.0, 15.0, 20.0]
REGIME_MAX_SCALE_LIST = [1.5, 2.0, 2.5, 3.0]

MIN_TRADES_PER_10MIN = 0.5


def parse_args():
    path = "market_data_30d.jsonl"
    max_spread = 50.0
    symbol = None
    args = sys.argv[1:]
    positional_done = False
    i = 0
    while i < len(args):
        if args[i] == "--max-spread" and i + 1 < len(args):
            max_spread = float(args[i + 1])
            i += 2
        elif args[i] == "--symbol" and i + 1 < len(args):
            symbol = args[i + 1]
            i += 2
        elif not args[i].startswith("--") and not positional_done:
            path = args[i]
            positional_done = True
            i += 1
        else:
            i += 1
    return path, max_spread, symbol


def fmt_row(params_str, trades, wins, pnl, max_dd, avg_hold, trades_per_10min, tick_sec):
    wr = wins / trades * 100 if trades > 0 else 0
    hold_s = avg_hold * tick_sec
    return (
        f"{params_str} | {trades:>6} {wins:>5} {wr:>4.0f}% | "
        f"${pnl:>8.2f} ${max_dd:>7.2f} | {trades_per_10min:>5.2f} {hold_s:>6.0f}s"
    )


def main():
    path, max_spread, symbol = parse_args()

    if symbol:
        os.environ["BACKTEST_SYMBOL"] = symbol

    print(f"Loading {path} (symbol={os.environ.get('BACKTEST_SYMBOL', 'LIT')}, max_spread={max_spread}bps)...")
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

    # Scale EMA grids for data interval vs bot interval (5s)
    BOT_INTERVAL = 5
    data_interval = stats.get("data_interval_secs", BOT_INTERVAL)
    ema_scale = max(1, round(data_interval / BOT_INTERVAL))
    scaled_ema_short = sorted(set(max(1, v // ema_scale) for v in EMA_SHORT_LIST))
    scaled_ema_long = sorted(set(max(2, v // ema_scale) for v in EMA_LONG_LIST))
    scaled_ema_macro_short = sorted(set(max(1, v // ema_scale) for v in EMA_MACRO_SHORT_LIST))
    scaled_ema_macro_long = sorted(set(max(2, v // ema_scale) for v in EMA_MACRO_LONG_LIST))
    print(f"EMA scale: {ema_scale}x (data={data_interval}s, bot={BOT_INTERVAL}s)")
    print(f"  Scaled EMA grids: short={scaled_ema_short} long={scaled_ema_long} "
          f"macro_short={scaled_ema_macro_short} macro_long={scaled_ema_macro_long}")
    print()

    grand_summary = {}

    def update_grand(strategy, spread, pnl, params_str, trades, wins, max_dd):
        key = (strategy, spread)
        if key not in grand_summary or pnl > grand_summary[key][0]:
            grand_summary[key] = (pnl, params_str, trades, wins, max_dd)

    # ---------- 1. MR with EMA + Regime grid ----------
    print("=" * 130)
    print("STRATEGY 1: MEAN REVERSION (MR) with EMA/Regime optimization")
    print("=" * 130)

    # Phase 1: Find top-5 unique EMA combos using representative MR params
    print("\n--- Phase 1: EMA screening (0bp spread, default regime) ---")
    ema_scores = {}  # (es, el, ems, eml) -> best_pnl
    for es, el, ems, eml in product(scaled_ema_short, scaled_ema_long,
                                     scaled_ema_macro_short, scaled_ema_macro_long):
        if es >= el or ems >= eml:
            continue
        t_arr, m_arr = precompute_emas(prices, es, el, ems, eml)
        best_pnl = -float("inf")
        for e, s, t, r in product([10, 20, 30], [30, 50, 80], [5, 10, 15], [5, 10]):
            trades, wins, pnl, max_dd, avg_hold = backtest_mr(
                prices, t_arr, e, s, t, r,
                equity=EQUITY, order_size_pct=ORDER_SIZE_PCT, half_spread_bps=0.0,
                macro_bps_arr=m_arr,
            )
            tpm = trades / (duration_min / 10.0) if duration_min > 0 else 0
            if tpm >= MIN_TRADES_PER_10MIN and pnl > best_pnl:
                best_pnl = pnl
        ema_scores[(es, el, ems, eml)] = best_pnl

    top_emas = sorted(ema_scores.items(), key=lambda x: -x[1])[:5]
    # Always include default EMA (bot values scaled for data interval) for comparison
    default_ema = (max(1, 5 // ema_scale), max(2, 20 // ema_scale),
                   max(1, 10 // ema_scale), max(2, 50 // ema_scale))
    top_ema_keys = [k for k, _ in top_emas]
    if default_ema not in top_ema_keys:
        top_ema_keys.append(default_ema)

    print(f"\nTop EMA candidates (screened from {len(ema_scores)} combos):")
    for k, v in top_emas:
        marker = " (default)" if k == default_ema else ""
        print(f"  es={k[0]:<3} el={k[1]:<3} ms={k[2]:<3} ml={k[3]:<3} -> screening PnL=${v:.2f}{marker}")
    if default_ema not in [k for k, _ in top_emas]:
        v = ema_scores.get(default_ema, -float("inf"))
        print(f"  es={default_ema[0]:<3} el={default_ema[1]:<3} ms={default_ema[2]:<3} ml={default_ema[3]:<3} -> screening PnL=${v:.2f} (default, added)")

    # Phase 2: Full MR + Regime grid for each top EMA candidate
    print(f"\n--- Phase 2: Full grid search for top {len(top_ema_keys)} EMA candidates ---")
    overall_best = {}  # spread -> (pnl, params_str, trades, wins, max_dd, es, el, ems, eml, rm, rms)

    for ema_idx, (es, el, ems, eml) in enumerate(top_ema_keys):
        t_arr, m_arr = precompute_emas(prices, es, el, ems, eml)
        marker = " (default)" if (es, el, ems, eml) == default_ema else ""
        print(f"\n  [{ema_idx+1}/{len(top_ema_keys)}] EMA s={es}/l={el}/ms={ems}/ml={eml}{marker}")

        for spread_bps in SPREAD_BPS_LIST:
            half_spread = spread_bps / 2.0
            local_best_pnl = -float("inf")
            local_best = None
            for e, s, t, r in product(ENTRY_LIST, STOP_LIST, TP_LIST, REVERT_LIST):
                for rm, rms in product(REGIME_MID_LIST, REGIME_MAX_SCALE_LIST):
                    trades, wins, pnl, max_dd, avg_hold = backtest_mr(
                        prices, t_arr, e, s, t, r,
                        equity=EQUITY, order_size_pct=ORDER_SIZE_PCT,
                        half_spread_bps=half_spread,
                        macro_bps_arr=m_arr, regime_mid=rm, max_scale=rms,
                    )
                    tpm = trades / (duration_min / 10.0) if duration_min > 0 else 0
                    if tpm >= MIN_TRADES_PER_10MIN and pnl > local_best_pnl:
                        local_best_pnl = pnl
                        local_best = (pnl, f"e={e:>3} s={s:>3} t={t:>3} r={r:>3}",
                                      trades, wins, max_dd, es, el, ems, eml, rm, rms)

            if local_best is not None:
                pnl = local_best[0]
                prev = overall_best.get(spread_bps)
                if prev is None or pnl > prev[0]:
                    overall_best[spread_bps] = local_best
                wr = local_best[3] / local_best[2] * 100 if local_best[2] > 0 else 0
                print(f"    {spread_bps:>2}bp: ${pnl:>8.2f} maxDD=${local_best[4]:>7.2f} "
                      f"trades={local_best[2]:>5} wr={wr:.0f}% {local_best[1]} "
                      f"regime mid={local_best[9]} scale={local_best[10]}")

    # Print MR summary and update grand_summary
    print(f"\n--- MR Best per spread (across all EMA/regime combos) ---")
    print(f"{'Spread':>6} | {'PnL':>9} {'maxDD':>8} {'trades':>7} {'winR':>5} | {'MR params':<24} | EMA | Regime")
    print("-" * 130)
    best_es = best_el = best_ems = best_eml = best_rm = best_rms = None
    for spread_bps in SPREAD_BPS_LIST:
        if spread_bps in overall_best:
            ob = overall_best[spread_bps]
            pnl, params, trades, wins, max_dd, es, el, ems, eml, rm, rms = ob
            wr = wins / trades * 100 if trades > 0 else 0
            print(f"{spread_bps:>4}bp | ${pnl:>8.2f} ${max_dd:>7.2f} {trades:>7} {wr:>4.0f}% | {params} | "
                  f"s={es*ema_scale}/l={el*ema_scale}/ms={ems*ema_scale}/ml={eml*ema_scale} (bot) | mid={rm}/scale={rms}")
            update_grand("MR", spread_bps, pnl, params, trades, wins, max_dd)
            if best_es is None:  # Use 0bp best for downstream strategies
                best_es, best_el, best_ems, best_eml = es, el, ems, eml
                best_rm, best_rms = rm, rms

    if best_es is None:
        best_es, best_el, best_ems, best_eml = 5, 20, 10, 50
        best_rm, best_rms = 10.0, 2.0

    best_trend_arr, best_macro_arr = precompute_emas(prices, best_es, best_el, best_ems, best_eml)

    # ---------- 2. Trend Follow (default EMA) ----------
    print("\n" + "=" * 130)
    print("STRATEGY 2: TREND FOLLOW (TF)")
    print("=" * 130)
    default_trend_arr, _ = precompute_emas(prices)
    for spread_bps in SPREAD_BPS_LIST:
        half_spread = spread_bps / 2.0
        print(f"\n--- Residual spread cost: {spread_bps} bps (half={half_spread} bps) ---")
        results = []
        for e, s, t, tr in product(ENTRY_LIST, STOP_LIST, TP_LIST, TRAIL_STOP_LIST):
            trades, wins, pnl, max_dd, avg_hold = backtest_tf(
                prices, default_trend_arr, e, s, t, tr,
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
    print("\n" + "=" * 130)
    print(f"STRATEGY 3: MR + SPOT HEDGE (extra {HEDGE_EXTRA_BPS}bps round-trip per trade)")
    print("=" * 130)
    for spread_bps in SPREAD_BPS_LIST:
        half_spread = spread_bps / 2.0
        print(f"\n--- Residual spread cost: {spread_bps} bps + hedge {HEDGE_EXTRA_BPS}bps ---")
        results = []
        for e, s, t, r in product(ENTRY_LIST, STOP_LIST, TP_LIST, REVERT_LIST):
            trades, wins, pnl, max_dd, avg_hold = backtest_mr_hedge(
                prices, best_trend_arr, e, s, t, r,
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
    print("\n" + "=" * 130)
    print("STRATEGY 4: MR + TREND PAUSE (skip entry when macro EMA divergence > threshold)")
    print("=" * 130)
    for spread_bps in SPREAD_BPS_LIST:
        half_spread = spread_bps / 2.0
        print(f"\n--- Residual spread cost: {spread_bps} bps ---")
        results = []
        for e, s, t, r, mp in product(ENTRY_LIST, STOP_LIST, TP_LIST, REVERT_LIST, MACRO_PAUSE_LIST):
            trades, wins, pnl, max_dd, avg_hold = backtest_mr_trend_pause(
                prices, best_trend_arr, best_macro_arr,
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
    print("\n" + "#" * 130)
    sym = os.environ.get('BACKTEST_SYMBOL', 'LIT')
    print(f"GRAND SUMMARY [{sym}]: Best result per strategy "
          f"(EMA bot: s={best_es*ema_scale}/l={best_el*ema_scale}"
          f"/ms={best_ems*ema_scale}/ml={best_eml*ema_scale}, "
          f"regime mid={best_rm}/scale={best_rms})")
    print("#" * 130)
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
        print(f"    EMA (bot): short={best_es*ema_scale} long={best_el*ema_scale} "
              f"macro_short={best_ems*ema_scale} macro_long={best_eml*ema_scale}")
        print(f"    Regime: mid={best_rm} max_scale={best_rms}")


if __name__ == "__main__":
    main()
