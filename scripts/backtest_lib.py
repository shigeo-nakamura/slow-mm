#!/usr/bin/env python3
"""
Shared backtest library for slow-mm mean-reversion strategies.

Provides:
- load_ticks(): Load JSONL data with spread filtering (skip ticks where spread > max_spread_bps)
- precompute_emas(): Compute EMA arrays from filtered ticks
- backtest_mr(): Mean Reversion backtest
- backtest_tf(): Trend Follow backtest
- backtest_mr_hedge(): MR + Spot Hedge backtest
- backtest_mr_trend_pause(): MR + Trend Pause backtest
"""
import json
import time

SYMBOL = "LIT"
DEFAULT_EMA_SHORT = 5
DEFAULT_EMA_LONG = 20
DEFAULT_EMA_MACRO_SHORT = 10
DEFAULT_EMA_MACRO_LONG = 50
COOLDOWN_TICKS = 3


def load_ticks(path, max_spread_bps=50.0, lookback_days=None):
    """Load price ticks from JSONL, filtering out wide-spread ticks.

    Returns list of (timestamp_ms, mid_price) tuples where spread <= max_spread_bps.
    Ticks with missing bid/ask are skipped.
    Stats include spread distribution (median/mean half-spread in bps).
    """
    ticks = []
    cutoff_ms = 0
    if lookback_days is not None:
        cutoff_ms = (time.time() - lookback_days * 86400) * 1000

    skipped_wide = 0
    skipped_missing = 0
    total = 0
    spreads_bps = []

    with open(path) as f:
        for line in f:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = row.get("timestamp", 0)
            if cutoff_ms and ts < cutoff_ms:
                continue
            lit = row.get("prices", {}).get(SYMBOL)
            if not lit:
                continue
            total += 1

            bid_str = lit.get("bid_price")
            ask_str = lit.get("ask_price")
            mid = float(lit.get("price", 0))

            if not mid or mid <= 0:
                skipped_missing += 1
                continue

            # If bid/ask available, compute actual spread and filter
            if bid_str and ask_str:
                bid = float(bid_str)
                ask = float(ask_str)
                # Sanity check: bid/ask must be within 10% of mid price
                # (guards against cross-symbol data contamination)
                if bid > 0 and ask > 0 and bid < ask:
                    if abs(bid - mid) / mid > 0.10 or abs(ask - mid) / mid > 0.10:
                        skipped_missing += 1
                        # Fall through to use mid only (bid/ask is from wrong symbol)
                    else:
                        spread_bps = (ask - bid) / ((ask + bid) / 2) * 10000.0
                        if spread_bps > max_spread_bps:
                            skipped_wide += 1
                            continue
                        spreads_bps.append(spread_bps)
                        mid = (bid + ask) / 2.0
                else:
                    skipped_missing += 1

            ticks.append((ts, mid))

    # Spread statistics
    if spreads_bps:
        spreads_bps.sort()
        median_spread = spreads_bps[len(spreads_bps) // 2]
        mean_spread = sum(spreads_bps) / len(spreads_bps)
    else:
        median_spread = 0.0
        mean_spread = 0.0

    return ticks, {"total": total, "kept": len(ticks),
                    "skipped_wide": skipped_wide, "skipped_missing": skipped_missing,
                    "median_spread_bps": round(median_spread, 2),
                    "mean_spread_bps": round(mean_spread, 2),
                    "median_half_spread_bps": round(median_spread / 2, 2)}


def precompute_emas(prices, ema_short=None, ema_long=None,
                    ema_macro_short=None, ema_macro_long=None):
    """Precompute EMA trend and macro arrays from price ticks."""
    ema_short = ema_short or DEFAULT_EMA_SHORT
    ema_long = ema_long or DEFAULT_EMA_LONG
    ema_macro_short = ema_macro_short or DEFAULT_EMA_MACRO_SHORT
    ema_macro_long = ema_macro_long or DEFAULT_EMA_MACRO_LONG

    alpha_s = 2.0 / (ema_short + 1)
    alpha_l = 2.0 / (ema_long + 1)
    alpha_ms = 2.0 / (ema_macro_short + 1)
    alpha_ml = 2.0 / (ema_macro_long + 1)

    n = len(prices)
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

        trend_bps[i] = (es - el) / el * 10000.0 if el > 0 else 0.0
        macro_bps[i] = (ems - eml) / eml * 10000.0 if eml > 0 else 0.0

    return trend_bps, macro_bps


def backtest_mr(prices, trend_bps_arr, entry_bps, stop_bps, tp_bps, revert_bps,
                equity=970.0, order_size_pct=0.10, half_spread_bps=0.0):
    """Mean Reversion: fade EMA divergence, exit on TP/SL/revert."""
    direction = 0
    entry_mid = 0.0
    cooldown = 0
    trades = wins = 0
    pnl = peak_pnl = max_dd = 0.0
    hold_total = hold = 0

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

            close = False
            if pnl_bps <= -stop_bps:
                close = True
            elif pnl_bps >= tp_bps:
                close = True
            elif abs(tb) <= revert_bps:
                close = True

            if close:
                size_usd = equity * order_size_pct
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


def backtest_tf(prices, trend_bps_arr, entry_bps, stop_bps, tp_bps, trail_stop_bps,
                equity=970.0, order_size_pct=0.10, half_spread_bps=0.0):
    """Trend Follow: follow EMA direction, exit on SL/TP/trailing stop/reversal."""
    direction = 0
    entry_mid = 0.0
    cooldown = 0
    trades = wins = 0
    pnl = peak_pnl = max_dd = 0.0
    hold_total = hold = 0
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
            elif best_pnl_bps > 0 and (best_pnl_bps - pnl_bps) >= trail_stop_bps:
                close = True
            elif (direction == 1 and tb < -entry_bps) or (direction == -1 and tb > entry_bps):
                close = True

            if close:
                size_usd = equity * order_size_pct
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
                direction = 1 if tb > 0 else -1
                entry_mid = mid
                hold = 0
                best_pnl_bps = 0.0

    avg_hold = hold_total / trades if trades > 0 else 0
    return trades, wins, pnl, max_dd, avg_hold


def backtest_mr_hedge(prices, trend_bps_arr, entry_bps, stop_bps, tp_bps, revert_bps,
                      equity=970.0, order_size_pct=0.10, half_spread_bps=0.0,
                      hedge_extra_bps=10.0):
    """MR + Spot Hedge: same as MR but add extra round-trip hedge cost per trade."""
    trades, wins, pnl, max_dd, avg_hold = backtest_mr(
        prices, trend_bps_arr, entry_bps, stop_bps, tp_bps, revert_bps,
        equity, order_size_pct, half_spread_bps,
    )
    hedge_cost_per_trade = hedge_extra_bps / 10000.0 * equity * order_size_pct
    pnl -= trades * hedge_cost_per_trade
    return trades, wins, pnl, max_dd, avg_hold


def backtest_mr_trend_pause(prices, trend_bps_arr, macro_bps_arr,
                            entry_bps, stop_bps, tp_bps, revert_bps, macro_pause_bps,
                            equity=970.0, order_size_pct=0.10, half_spread_bps=0.0):
    """MR + Trend Pause: skip entry when macro EMA divergence is too strong."""
    direction = 0
    entry_mid = 0.0
    cooldown = 0
    trades = wins = 0
    pnl = peak_pnl = max_dd = 0.0
    hold_total = hold = 0

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
                size_usd = equity * order_size_pct
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
            if abs(mb) >= macro_pause_bps:
                continue
            if abs(tb) >= entry_bps:
                direction = -1 if tb > 0 else 1
                entry_mid = mid
                hold = 0

    avg_hold = hold_total / trades if trades > 0 else 0
    return trades, wins, pnl, max_dd, avg_hold


def result_dict(prices, trades, wins, pnl, max_dd):
    """Build a standard result dict for reporting."""
    duration_min = (prices[-1][0] - prices[0][0]) / 60000 if len(prices) >= 2 else 1
    freq = trades / (duration_min / 10) if duration_min > 0 else 0
    win_rate = wins / trades * 100 if trades > 0 else 0
    return {
        "trades": trades,
        "wins": wins,
        "win_rate": round(win_rate, 1),
        "pnl": round(pnl, 2),
        "max_dd": round(max_dd, 2),
        "trades_per_10min": round(freq, 2),
        "duration_days": round(duration_min / 60 / 24, 1),
    }
