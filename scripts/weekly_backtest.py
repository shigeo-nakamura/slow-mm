#!/usr/bin/env python3
"""
Weekly backtest for mean_reversion strategy.
Reads last 7 days from market_data_365d.jsonl, runs backtest with current config,
writes alert to debot-dashboard status directory if edge is lost.

Usage: python3 weekly_backtest.py [data_file] [config_file]
Cron:  0 6 * * 1  python3 /opt/slow-mm/scripts/weekly_backtest.py
"""
import json
import os
import sys
import time
import yaml
from datetime import datetime

# Defaults
DATA_FILE = os.getenv(
    "BACKTEST_DATA_FILE",
    "/home/ec2-user/pairtrade/market_data_365d.jsonl",
)
CONFIG_FILE = os.getenv(
    "MM_CONFIG_PATH",
    "/opt/slow-mm/configs/mm/slow-mm.yaml",
)
STATUS_DIR = os.getenv(
    "DEBOT_STATUS_DIR",
    "/home/ec2-user/debot_status",
)
STATUS_ID = os.getenv("DEBOT_STATUS_ID", "slow-mm")
ALERT_FILE = os.path.join(STATUS_DIR, STATUS_ID, "backtest_alert.json")
SYMBOL = "LIT"
EQUITY = 290.0
EMA_SHORT = 5
EMA_LONG = 20
LOOKBACK_DAYS = 7

def load_prices(path, lookback_days):
    """Load prices from JSONL, keep only last N days."""
    prices = []
    cutoff_ms = (time.time() - lookback_days * 86400) * 1000

    with open(path) as f:
        for line in f:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = row.get("timestamp", 0)
            if ts < cutoff_ms:
                continue
            lit = row.get("prices", {}).get(SYMBOL)
            if not lit:
                continue
            price = float(lit.get("price", 0))
            if price > 0:
                prices.append((ts, price))
    return prices

def run_backtest(prices, entry_bps, stop_bps, tp_bps, revert_bps, cooldown_ticks=3):
    ema_s = ema_l = None
    alpha_s = 2.0 / (EMA_SHORT + 1)
    alpha_l = 2.0 / (EMA_LONG + 1)
    direction = 0
    entry_price = 0.0
    cooldown = 0
    trades = wins = 0
    pnl = 0.0
    peak_pnl = max_dd = 0.0
    order_size_pct = 0.05

    for _, mid in prices:
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
                size_usd = EQUITY * order_size_pct
                trade_pnl = pnl_bps / 10000 * size_usd
                pnl += trade_pnl
                if trade_pnl > 0:
                    wins += 1
                trades += 1
                direction = 0
                cooldown = cooldown_ticks
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

def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)

def main():
    data_file = sys.argv[1] if len(sys.argv) > 1 else DATA_FILE
    config_file = sys.argv[2] if len(sys.argv) > 2 else CONFIG_FILE

    print(f"[BACKTEST] Loading config: {config_file}")
    cfg = load_config(config_file)
    entry_bps = cfg.get("tf_entry_threshold_bps", 15.0)
    stop_bps = cfg.get("stop_loss_bps", 30.0)
    tp_bps = cfg.get("tf_take_profit_bps", 5.0)
    revert_bps = cfg.get("mr_revert_bps", 5.0)

    print(f"[BACKTEST] Loading data: {data_file} (last {LOOKBACK_DAYS} days)")
    prices = load_prices(data_file, LOOKBACK_DAYS)
    print(f"[BACKTEST] Loaded {len(prices)} ticks")

    if len(prices) < 100:
        print("[BACKTEST] Not enough data, skipping")
        return

    # Run with current config
    result = run_backtest(prices, entry_bps, stop_bps, tp_bps, revert_bps)
    print(f"[BACKTEST] Current config: PnL=${result['pnl']} winR={result['win_rate']}% "
          f"trades={result['trades']} maxDD=${result['max_dd']} freq={result['trades_per_10min']}/10m")

    # Also try a grid to find best
    best_pnl = result["pnl"]
    best_params = {"entry": entry_bps, "stop": stop_bps, "tp": tp_bps, "revert": revert_bps}
    for e in [10, 15, 20]:
        for s in [20, 30, 50]:
            for t in [3, 5, 8, 10]:
                for r in [3, 5]:
                    res = run_backtest(prices, e, s, t, r)
                    if res["trades_per_10min"] >= 0.5 and res["pnl"] > best_pnl:
                        best_pnl = res["pnl"]
                        best_params = {"entry": e, "stop": s, "tp": t, "revert": r}

    best_result = run_backtest(prices, best_params["entry"], best_params["stop"],
                                best_params["tp"], best_params["revert"])

    # Determine alert level
    if result["pnl"] < 0:
        alert_level = "DANGER"
        alert_msg = f"MR edge LOST: PnL=${result['pnl']}/7d. Consider stopping bot."
    elif result["pnl"] < best_pnl * 0.5 and best_pnl > 0:
        alert_level = "WARNING"
        alert_msg = f"MR suboptimal: current=${result['pnl']} vs best=${best_pnl}/7d"
    else:
        alert_level = "OK"
        alert_msg = f"MR edge OK: PnL=${result['pnl']}/7d winR={result['win_rate']}%"

    if best_params != {"entry": entry_bps, "stop": stop_bps, "tp": tp_bps, "revert": revert_bps}:
        alert_msg += f" | Better params: entry={best_params['entry']} stop={best_params['stop']} tp={best_params['tp']} revert={best_params['revert']} (PnL=${best_pnl})"

    print(f"[BACKTEST] {alert_level}: {alert_msg}")

    # Write alert file for dashboard
    alert = {
        "ts": int(time.time() * 1000),
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "alert_level": alert_level,
        "alert_msg": alert_msg,
        "current_config": {
            "entry_bps": entry_bps,
            "stop_bps": stop_bps,
            "tp_bps": tp_bps,
            "revert_bps": revert_bps,
        },
        "current_result": result,
        "best_params": best_params,
        "best_result": best_result,
        "data_days": LOOKBACK_DAYS,
        "data_ticks": len(prices),
    }

    os.makedirs(os.path.dirname(ALERT_FILE), exist_ok=True)
    tmp = ALERT_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(alert, f, indent=2)
    os.rename(tmp, ALERT_FILE)
    print(f"[BACKTEST] Alert written to {ALERT_FILE}")

if __name__ == "__main__":
    main()
