#!/usr/bin/env python3
"""
Weekly backtest for mean_reversion strategy.
Reads last 7 days from market_data_365d.jsonl, runs backtest with current config,
auto-updates YAML if better params found, writes alert to dashboard.

Usage: python3 weekly_backtest.py [data_file] [config_file]
Cron:  0 6 * * 1  python3 /opt/slow-mm/scripts/weekly_backtest.py
"""
import os
import re
import shutil
import sys
import time
import yaml
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from backtest_lib import (load_ticks, precompute_emas, backtest_mr,
                          backtest_mr_trend_pause, result_dict)

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
LOOKBACK_DAYS = 7
# Data interval vs bot interval: data is 20s, bot is 5s → scale EMA periods by 4x
# When running grid search on 20s data, use EMA/4. When writing YAML, multiply back by 4.
DATA_INTERVAL_SECS = 20
BOT_INTERVAL_SECS = 5
EMA_SCALE = DATA_INTERVAL_SECS // BOT_INTERVAL_SECS  # 4


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def update_yaml_params(config_path, params, old_result, new_result):
    """Update YAML config with new params, keeping a .prev backup for rollback."""
    prev_path = config_path + ".prev"
    shutil.copy2(config_path, prev_path)
    print(f"[BACKTEST] Backup saved: {prev_path}")

    with open(config_path) as f:
        content = f.read()

    # EMA periods in params are for data interval; scale back to bot interval for YAML
    replacements = {
        "tf_entry_threshold_bps": float(params["entry"]),
        "stop_loss_bps": float(params["stop"]),
        "tf_take_profit_bps": float(params["tp"]),
        "mr_revert_bps": float(params["revert"]),
        "ema_short_periods": int(params.get("ema_short", 1)) * EMA_SCALE,
        "ema_long_periods": int(params.get("ema_long", 5)) * EMA_SCALE,
        "mr_trend_pause_bps": int(params.get("macro_pause", 0)),
    }
    for key, val in replacements.items():
        content = re.sub(
            rf'^({key}:\s*)[\d.]+',
            rf'\g<1>{val}',
            content,
            flags=re.MULTILINE,
        )

    content = re.sub(
        r'^# Backtest:.*$',
        f'# Backtest: PnL=${new_result["pnl"]}/7d, '
        f'{new_result["win_rate"]}% win, '
        f'maxDD ${new_result["max_dd"]}, '
        f'{new_result["trades_per_10min"]} trades/10min '
        f'(auto-updated {datetime.utcnow().strftime("%Y-%m-%d")})',
        content,
        flags=re.MULTILINE,
    )

    tmp_path = config_path + ".tmp"
    with open(tmp_path, "w") as f:
        f.write(content)
    os.rename(tmp_path, config_path)
    print(f"[BACKTEST] Updated {config_path}: entry={params['entry']} stop={params['stop']} "
          f"tp={params['tp']} revert={params['revert']}")


def main():
    import json

    data_file = sys.argv[1] if len(sys.argv) > 1 else DATA_FILE
    config_file = sys.argv[2] if len(sys.argv) > 2 else CONFIG_FILE

    print(f"[BACKTEST] Loading config: {config_file}")
    cfg = load_config(config_file)
    entry_bps = cfg.get("tf_entry_threshold_bps", 15.0)
    stop_bps = cfg.get("stop_loss_bps", 30.0)
    tp_bps = cfg.get("tf_take_profit_bps", 5.0)
    revert_bps = cfg.get("mr_revert_bps", 5.0)
    max_spread = cfg.get("max_spread_bps", 50.0)
    equity = cfg.get("equity_usd_fallback", 500.0)
    order_size_pct = cfg.get("order_size_pct", 0.10)
    # EMA periods in config are for bot interval (5s).
    # Scale down for 20s data backtest.
    ema_short = max(1, cfg.get("ema_short_periods", 5) // EMA_SCALE)
    ema_long = max(2, cfg.get("ema_long_periods", 20) // EMA_SCALE)
    macro_pause_bps = cfg.get("mr_trend_pause_bps", 0)

    print(f"[BACKTEST] Loading data: {data_file} (last {LOOKBACK_DAYS} days, max_spread={max_spread}bps)")
    prices, stats = load_ticks(data_file, max_spread_bps=max_spread, lookback_days=LOOKBACK_DAYS)
    half_spread = stats.get("median_half_spread_bps", 0.0)
    print(f"[BACKTEST] Loaded {stats['kept']}/{stats['total']} ticks "
          f"(skipped: {stats['skipped_wide']} wide spread, {stats['skipped_missing']} missing bid/ask)")
    print(f"[BACKTEST] Spread cost: median={stats.get('median_spread_bps', 0)}bps "
          f"mean={stats.get('mean_spread_bps', 0)}bps → half_spread={half_spread}bps per side")

    if len(prices) < 100:
        print("[BACKTEST] Not enough data, skipping")
        return

    trend_bps_arr, macro_bps_arr = precompute_emas(prices, ema_short, ema_long)

    # Run with current config
    if macro_pause_bps > 0:
        trades, wins, pnl, max_dd, _ = backtest_mr_trend_pause(
            prices, trend_bps_arr, macro_bps_arr,
            entry_bps, stop_bps, tp_bps, revert_bps, macro_pause_bps,
            equity=equity, order_size_pct=order_size_pct,
            half_spread_bps=half_spread,
        )
    else:
        trades, wins, pnl, max_dd, _ = backtest_mr(
            prices, trend_bps_arr, entry_bps, stop_bps, tp_bps, revert_bps,
            equity=equity, order_size_pct=order_size_pct,
            half_spread_bps=half_spread,
        )
    result = result_dict(prices, trades, wins, pnl, max_dd)
    print(f"[BACKTEST] Current config: PnL=${result['pnl']} winR={result['win_rate']}% "
          f"trades={result['trades']} maxDD=${result['max_dd']} freq={result['trades_per_10min']}/10m "
          f"ema={ema_short}/{ema_long} pause={macro_pause_bps}bps")

    # Grid search for best params (including EMA periods and trend pause)
    best_pnl = result["pnl"]
    best_params = {"entry": entry_bps, "stop": stop_bps, "tp": tp_bps, "revert": revert_bps,
                   "ema_short": ema_short, "ema_long": ema_long, "macro_pause": macro_pause_bps}

    # EMA grid for 20s data (bot values = these × EMA_SCALE)
    ema_short_list = [1, 2, 3, 5]
    ema_long_list = [5, 10, 15, 20]
    macro_pause_list = [0, 30, 50, 80]

    for es, el in [(s, l) for s in ema_short_list for l in ema_long_list if l > s]:
        t_arr, m_arr = precompute_emas(prices, es, el)
        for e in [5, 10, 15, 20, 30]:
            for s in [10, 20, 30, 50]:
                for t in [3, 5, 8, 10, 15, 20]:
                    for r in [3, 5, 10]:
                        for mp in macro_pause_list:
                            if mp > 0:
                                tr, wi, pn, md, _ = backtest_mr_trend_pause(
                                    prices, t_arr, m_arr, e, s, t, r, mp,
                                    equity=equity, order_size_pct=order_size_pct,
                                    half_spread_bps=half_spread,
                                )
                            else:
                                tr, wi, pn, md, _ = backtest_mr(
                                    prices, t_arr, e, s, t, r,
                                    equity=equity, order_size_pct=order_size_pct,
                                    half_spread_bps=half_spread,
                                )
                            res = result_dict(prices, tr, wi, pn, md)
                            if res["trades_per_10min"] >= 0.5 and res["pnl"] > best_pnl:
                                best_pnl = res["pnl"]
                                best_params = {"entry": e, "stop": s, "tp": t, "revert": r,
                                               "ema_short": es, "ema_long": el, "macro_pause": mp}

    bp = best_params
    t_arr, m_arr = precompute_emas(prices, bp["ema_short"], bp["ema_long"])
    if bp["macro_pause"] > 0:
        best_trades, best_wins, best_pnl_val, best_max_dd, _ = backtest_mr_trend_pause(
            prices, t_arr, m_arr,
            bp["entry"], bp["stop"], bp["tp"], bp["revert"], bp["macro_pause"],
            equity=equity, order_size_pct=order_size_pct,
            half_spread_bps=half_spread,
        )
    else:
        best_trades, best_wins, best_pnl_val, best_max_dd, _ = backtest_mr(
            prices, t_arr,
            bp["entry"], bp["stop"], bp["tp"], bp["revert"],
            equity=equity, order_size_pct=order_size_pct,
            half_spread_bps=half_spread,
        )
    best_result = result_dict(prices, best_trades, best_wins, best_pnl_val, best_max_dd)

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

    current_params = {"entry": entry_bps, "stop": stop_bps, "tp": tp_bps, "revert": revert_bps,
                      "ema_short": ema_short, "ema_long": ema_long, "macro_pause": macro_pause_bps}
    params_changed = best_params != current_params
    if params_changed:
        alert_msg += (f" | Better params: entry={best_params['entry']} stop={best_params['stop']} "
                      f"tp={best_params['tp']} revert={best_params['revert']} "
                      f"ema={best_params['ema_short']}/{best_params['ema_long']} "
                      f"pause={best_params['macro_pause']}bps (PnL=${best_pnl})")

    print(f"[BACKTEST] {alert_level}: {alert_msg}")

    # Auto-update YAML config if better params found
    config_updated = False
    old_params = current_params
    if params_changed and best_pnl > 0:
        try:
            update_yaml_params(config_file, best_params, result, best_result)
            config_updated = True
            alert_msg += " | CONFIG AUTO-UPDATED (restart to apply)"
            print(f"[BACKTEST] Config auto-updated: {config_file}")
        except Exception as e:
            alert_msg += f" | Config update FAILED: {e}"
            print(f"[BACKTEST] Config update failed: {e}")

    # Write alert file for dashboard
    alert = {
        "ts": int(time.time() * 1000),
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "alert_level": alert_level,
        "alert_msg": alert_msg,
        "current_config": old_params,
        "current_result": result,
        "best_params": best_params,
        "best_result": best_result,
        "config_updated": config_updated,
        "spread_filter": {"max_spread_bps": max_spread, **stats},
        "data_days": LOOKBACK_DAYS,
        "data_ticks": stats["kept"],
    }

    os.makedirs(os.path.dirname(ALERT_FILE), exist_ok=True)
    tmp = ALERT_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(alert, f, indent=2)
    os.rename(tmp, ALERT_FILE)
    print(f"[BACKTEST] Alert written to {ALERT_FILE}")


if __name__ == "__main__":
    main()
