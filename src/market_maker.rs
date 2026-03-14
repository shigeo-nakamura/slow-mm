use anyhow::{Context, Result};
use dex_connector::{DexConnector, OrderSide};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::env;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{sleep, Duration};

use crate::status_reporter::{PositionInfo, StatusReporter};
use crate::trade::execution::dex_connector_box::DexConnectorBox;

// ---------------------------------------------------------------------------
// Constants / defaults
// ---------------------------------------------------------------------------

const DEFAULT_INTERVAL_SECS: u64 = 60;
const DEFAULT_SPREAD_BPS: f64 = 10.0;
const DEFAULT_ORDER_SIZE_PCT: f64 = 0.02;
const DEFAULT_MAX_INVENTORY_PCT: f64 = 0.10;
const DEFAULT_SKEW_FACTOR: f64 = 0.5;
const DEFAULT_MAX_LEVERAGE: u32 = 20;
const DEFAULT_EQUITY_USD: f64 = 500.0;
const DEFAULT_ORDER_LEVELS: usize = 1;
const DEFAULT_LEVEL_SPACING_BPS: f64 = 5.0;
const DEFAULT_HEDGE_THRESHOLD_RATIO: f64 = 0.6;
const DEFAULT_HEDGE_CLOSE_THRESHOLD_RATIO: f64 = 0.3;
const DEFAULT_INVENTORY_HARD_LIMIT_MULT: f64 = 2.0;
const DEFAULT_STALE_ORDER_SECS: u64 = 300;
const DEFAULT_VOLATILITY_WINDOW: usize = 60;
const DEFAULT_VOLATILITY_SPREAD_MULT: f64 = 1.0;
const DEFAULT_MIN_SPREAD_BPS: f64 = 3.0;
const DEFAULT_MAX_SPREAD_BPS: f64 = 50.0;
const DEFAULT_OB_DEPTH: usize = 5;
const DEFAULT_FORCE_CLOSE_MULT: f64 = 1.5;
const DEFAULT_FORCE_CLOSE_COOLDOWN_SECS: u64 = 120;
const DEFAULT_OB_IMBALANCE_FACTOR: f64 = 1.0;
const DEFAULT_FUNDING_RATE_FACTOR: f64 = 1.0;
const DEFAULT_TREND_WINDOW: usize = 5;
const DEFAULT_TREND_THRESHOLD_BPS: f64 = 3.0;
const DEFAULT_EMA_SHORT_PERIODS: usize = 5;
const DEFAULT_EMA_LONG_PERIODS: usize = 20;
const DEFAULT_TREND_STRENGTH_THRESHOLD: f64 = 2.0;
const DEFAULT_AGGRESSIVE_UNWIND_BPS: f64 = 0.0;
const DEFAULT_POST_FILL_SPREAD_MULT: f64 = 2.0;
const DEFAULT_POST_FILL_DECAY_SECS: u64 = 30;
const DEFAULT_INVENTORY_SPREAD_MULT: f64 = 1.0;
const DEFAULT_CAPTURE_MIN_SPREAD_BPS: f64 = 5.0;
const DEFAULT_CAPTURE_CLOSE_TIMEOUT_SECS: u64 = 30;
const DEFAULT_CAPTURE_SCAN_INTERVAL_SECS: u64 = 10;
const DEFAULT_CAPTURE_POLL_INTERVAL_SECS: u64 = 2;
const DEFAULT_PRICE_DECIMALS: u32 = 1;
const DEFAULT_SIZE_DECIMALS: u32 = 5;
const DEFAULT_STOP_LOSS_BPS: f64 = 30.0;
const DEFAULT_TF_COOLDOWN_SECS: u64 = 60;
const DEFAULT_TF_ENTRY_THRESHOLD_BPS: f64 = 20.0;
const DEFAULT_TF_TAKE_PROFIT_BPS: f64 = 50.0;
const DEFAULT_TF_TRAIL_STOP_BPS: f64 = 20.0;
const DEFAULT_MR_REVERT_BPS: f64 = 5.0;
const DEFAULT_MR_TREND_PAUSE_BPS: f64 = 0.0; // 0 = disabled

// ---------------------------------------------------------------------------
// YAML config
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct MmYaml {
    symbol: Option<String>,
    dry_run: Option<bool>,
    interval_secs: Option<u64>,
    spread_bps: Option<f64>,
    order_size_pct: Option<f64>,
    max_inventory_pct: Option<f64>,
    skew_factor: Option<f64>,
    max_leverage: Option<u32>,
    equity_usd_fallback: Option<f64>,
    order_levels: Option<usize>,
    level_spacing_bps: Option<f64>,
    hedge_enabled: Option<bool>,
    hedge_threshold_ratio: Option<f64>,
    hedge_close_threshold_ratio: Option<f64>,
    inventory_hard_limit_mult: Option<f64>,
    force_close_mult: Option<f64>,
    force_close_cooldown_secs: Option<u64>,
    stale_order_secs: Option<u64>,
    volatility_window: Option<usize>,
    volatility_spread_mult: Option<f64>,
    min_spread_bps: Option<f64>,
    max_spread_bps: Option<f64>,
    ob_depth: Option<usize>,
    ob_imbalance_factor: Option<f64>,
    funding_rate_factor: Option<f64>,
    trend_window: Option<usize>,
    trend_threshold_bps: Option<f64>,
    ema_short_periods: Option<usize>,
    ema_long_periods: Option<usize>,
    trend_strength_threshold: Option<f64>,
    aggressive_unwind_bps: Option<f64>,
    post_fill_spread_mult: Option<f64>,
    post_fill_decay_secs: Option<u64>,
    inventory_spread_mult: Option<f64>,
    strategy_mode: Option<String>,
    capture_min_spread_bps: Option<f64>,
    capture_close_timeout_secs: Option<u64>,
    capture_scan_interval_secs: Option<u64>,
    capture_poll_interval_secs: Option<u64>,
    price_decimals: Option<u32>,
    size_decimals: Option<u32>,
    spot_hedge_symbol: Option<String>,
    stop_loss_bps: Option<f64>,
    tf_cooldown_secs: Option<u64>,
    tf_entry_threshold_bps: Option<f64>,
    tf_take_profit_bps: Option<f64>,
    tf_trail_stop_bps: Option<f64>,
    mr_revert_bps: Option<f64>,
    mr_trend_pause_bps: Option<f64>,
}

// ---------------------------------------------------------------------------
// Public config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MmConfig {
    pub symbol: String,
    pub dry_run: bool,
    pub interval_secs: u64,
    /// Base half-spread in bps (each side from mid)
    pub spread_bps: f64,
    /// Size per order level as fraction of equity (e.g. 0.02 = 2%)
    pub order_size_pct: f64,
    /// Max unhedged inventory as fraction of equity (e.g. 0.10 = 10%)
    pub max_inventory_pct: f64,
    /// How much to skew quotes toward inventory-reducing side (0..1)
    pub skew_factor: f64,
    pub max_leverage: u32,
    pub equity_usd: f64,
    /// Number of order levels on each side
    pub order_levels: usize,
    /// Extra bps between each level
    pub level_spacing_bps: f64,
    /// Whether to use a hedge account
    pub hedge_enabled: bool,
    /// Hedge when net exposure (USD) exceeds equity * this ratio
    pub hedge_threshold_ratio: f64,
    /// Close hedge when net exposure (USD) drops below equity * this ratio
    pub hedge_close_threshold_ratio: f64,
    /// Hard inventory limit = max_inventory * this mult; stop quoting on that side
    pub inventory_hard_limit_mult: f64,
    /// Force close when inventory exceeds hard_limit * this mult (market order)
    pub force_close_mult: f64,
    /// Cooldown between force-close actions (seconds)
    pub force_close_cooldown_secs: u64,
    /// Cancel and re-place orders after this many seconds even if mid hasn't moved
    pub stale_order_secs: u64,
    /// Number of mid-price samples for volatility estimation
    pub volatility_window: usize,
    /// Spread = base_spread + vol * this
    pub volatility_spread_mult: f64,
    pub min_spread_bps: f64,
    pub max_spread_bps: f64,
    pub ob_depth: usize,
    /// Shift spread based on order book imbalance (0 = disabled)
    pub ob_imbalance_factor: f64,
    /// Bias skew toward funding-rate-favorable side (0 = disabled)
    pub funding_rate_factor: f64,
    /// Number of recent mid samples to detect short-term trend
    pub trend_window: usize,
    /// Trend threshold in bps: pause quoting on trend side if exceeded
    pub trend_threshold_bps: f64,
    /// EMA short period for trend detection
    pub ema_short_periods: usize,
    /// EMA long period for trend detection
    pub ema_long_periods: usize,
    /// EMA trend strength threshold in bps: pause BOTH sides when |EMA_short - EMA_long| exceeds this
    pub trend_strength_threshold: f64,
    /// When holding inventory, place unwind order at entry_price ± this bps (0 = disabled)
    /// Ensures minimum profit on unwind while being more aggressive than normal spread
    pub aggressive_unwind_bps: f64,
    /// Spread multiplier immediately after a fill (decays over time)
    pub post_fill_spread_mult: f64,
    /// Seconds for post-fill spread boost to decay back to 1.0
    pub post_fill_decay_secs: u64,
    /// Spread widens proportionally to inventory: spread *= (1 + |inv_ratio| * this)
    pub inventory_spread_mult: f64,
    /// Strategy mode: "passive_mm" (default) or "reactive_capture"
    pub strategy_mode: String,
    /// Minimum OB spread in bps to trigger a capture order
    pub capture_min_spread_bps: f64,
    /// Timeout in seconds to force-close a capture position
    pub capture_close_timeout_secs: u64,
    /// Seconds between OB scans in reactive capture (Scanning phase)
    pub capture_scan_interval_secs: u64,
    /// Seconds between fill-poll checks (MakerPending/ClosePending phase)
    pub capture_poll_interval_secs: u64,
    /// Decimal places for price rounding (e.g. 1 for BTC $70000.0, 4 for LIT $1.1050)
    pub price_decimals: u32,
    /// Decimal places for size rounding (e.g. 5 for BTC 0.00100, 2 for LIT 209.42)
    pub size_decimals: u32,
    /// Spot symbol for delta-neutral hedging (e.g. "LIT/USDC"). Empty = disabled.
    pub spot_hedge_symbol: String,
    /// Stop-loss in bps from entry price (trend_follow mode)
    pub stop_loss_bps: f64,
    /// Cooldown seconds after stop-loss before next entry (trend_follow mode)
    pub tf_cooldown_secs: u64,
    /// EMA trend strength threshold in bps to trigger entry (trend_follow mode)
    pub tf_entry_threshold_bps: f64,
    /// Take-profit in bps from entry price (trend_follow mode)
    pub tf_take_profit_bps: f64,
    /// Trailing stop in bps below peak (trend_follow mode)
    pub tf_trail_stop_bps: f64,
    /// Close when EMA divergence reverts within this bps (mean_reversion mode)
    pub mr_revert_bps: f64,
    /// Pause MR entries when EMA10/EMA50 macro trend exceeds this bps (0 = disabled)
    pub mr_trend_pause_bps: f64,
}

impl MmConfig {
    pub fn from_env_or_yaml() -> Result<Self> {
        let config_path = env::var("MM_CONFIG_PATH")
            .ok()
            .filter(|v| !v.trim().is_empty());
        if let Some(path) = config_path {
            return Self::from_yaml_path(path);
        }
        Self::from_env()
    }

    fn from_yaml_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let file = File::open(path_ref)
            .with_context(|| format!("failed to open MM config {}", path_ref.display()))?;
        let yaml: MmYaml = serde_yaml::from_reader(file)
            .with_context(|| format!("failed to parse MM config {}", path_ref.display()))?;

        let mut cfg = MmConfig {
            symbol: yaml.symbol.unwrap_or_else(|| "BTC".to_string()),
            dry_run: yaml.dry_run.unwrap_or(true),
            interval_secs: yaml.interval_secs.unwrap_or(DEFAULT_INTERVAL_SECS),
            spread_bps: yaml.spread_bps.unwrap_or(DEFAULT_SPREAD_BPS),
            order_size_pct: yaml.order_size_pct.unwrap_or(DEFAULT_ORDER_SIZE_PCT),
            max_inventory_pct: yaml.max_inventory_pct.unwrap_or(DEFAULT_MAX_INVENTORY_PCT),
            skew_factor: yaml.skew_factor.unwrap_or(DEFAULT_SKEW_FACTOR),
            max_leverage: yaml.max_leverage.unwrap_or(DEFAULT_MAX_LEVERAGE),
            equity_usd: yaml.equity_usd_fallback.unwrap_or(DEFAULT_EQUITY_USD),
            order_levels: yaml.order_levels.unwrap_or(DEFAULT_ORDER_LEVELS),
            level_spacing_bps: yaml.level_spacing_bps.unwrap_or(DEFAULT_LEVEL_SPACING_BPS),
            hedge_enabled: yaml.hedge_enabled.unwrap_or(true),
            hedge_threshold_ratio: yaml
                .hedge_threshold_ratio
                .unwrap_or(DEFAULT_HEDGE_THRESHOLD_RATIO),
            hedge_close_threshold_ratio: yaml
                .hedge_close_threshold_ratio
                .unwrap_or(DEFAULT_HEDGE_CLOSE_THRESHOLD_RATIO),
            inventory_hard_limit_mult: yaml
                .inventory_hard_limit_mult
                .unwrap_or(DEFAULT_INVENTORY_HARD_LIMIT_MULT),
            force_close_mult: yaml.force_close_mult.unwrap_or(DEFAULT_FORCE_CLOSE_MULT),
            force_close_cooldown_secs: yaml
                .force_close_cooldown_secs
                .unwrap_or(DEFAULT_FORCE_CLOSE_COOLDOWN_SECS),
            stale_order_secs: yaml.stale_order_secs.unwrap_or(DEFAULT_STALE_ORDER_SECS),
            volatility_window: yaml.volatility_window.unwrap_or(DEFAULT_VOLATILITY_WINDOW),
            volatility_spread_mult: yaml
                .volatility_spread_mult
                .unwrap_or(DEFAULT_VOLATILITY_SPREAD_MULT),
            min_spread_bps: yaml.min_spread_bps.unwrap_or(DEFAULT_MIN_SPREAD_BPS),
            max_spread_bps: yaml.max_spread_bps.unwrap_or(DEFAULT_MAX_SPREAD_BPS),
            ob_depth: yaml.ob_depth.unwrap_or(DEFAULT_OB_DEPTH),
            ob_imbalance_factor: yaml
                .ob_imbalance_factor
                .unwrap_or(DEFAULT_OB_IMBALANCE_FACTOR),
            funding_rate_factor: yaml
                .funding_rate_factor
                .unwrap_or(DEFAULT_FUNDING_RATE_FACTOR),
            trend_window: yaml.trend_window.unwrap_or(DEFAULT_TREND_WINDOW),
            trend_threshold_bps: yaml
                .trend_threshold_bps
                .unwrap_or(DEFAULT_TREND_THRESHOLD_BPS),
            ema_short_periods: yaml
                .ema_short_periods
                .unwrap_or(DEFAULT_EMA_SHORT_PERIODS),
            ema_long_periods: yaml
                .ema_long_periods
                .unwrap_or(DEFAULT_EMA_LONG_PERIODS),
            trend_strength_threshold: yaml
                .trend_strength_threshold
                .unwrap_or(DEFAULT_TREND_STRENGTH_THRESHOLD),
            aggressive_unwind_bps: yaml
                .aggressive_unwind_bps
                .unwrap_or(DEFAULT_AGGRESSIVE_UNWIND_BPS),
            post_fill_spread_mult: yaml
                .post_fill_spread_mult
                .unwrap_or(DEFAULT_POST_FILL_SPREAD_MULT),
            post_fill_decay_secs: yaml
                .post_fill_decay_secs
                .unwrap_or(DEFAULT_POST_FILL_DECAY_SECS),
            inventory_spread_mult: yaml
                .inventory_spread_mult
                .unwrap_or(DEFAULT_INVENTORY_SPREAD_MULT),
            strategy_mode: yaml
                .strategy_mode
                .unwrap_or_else(|| "passive_mm".to_string()),
            capture_min_spread_bps: yaml
                .capture_min_spread_bps
                .unwrap_or(DEFAULT_CAPTURE_MIN_SPREAD_BPS),
            capture_close_timeout_secs: yaml
                .capture_close_timeout_secs
                .unwrap_or(DEFAULT_CAPTURE_CLOSE_TIMEOUT_SECS),
            capture_scan_interval_secs: yaml
                .capture_scan_interval_secs
                .unwrap_or(DEFAULT_CAPTURE_SCAN_INTERVAL_SECS),
            capture_poll_interval_secs: yaml
                .capture_poll_interval_secs
                .unwrap_or(DEFAULT_CAPTURE_POLL_INTERVAL_SECS),
            price_decimals: yaml.price_decimals.unwrap_or(DEFAULT_PRICE_DECIMALS),
            size_decimals: yaml.size_decimals.unwrap_or(DEFAULT_SIZE_DECIMALS),
            spot_hedge_symbol: yaml.spot_hedge_symbol.unwrap_or_default(),
            stop_loss_bps: yaml.stop_loss_bps.unwrap_or(DEFAULT_STOP_LOSS_BPS),
            tf_cooldown_secs: yaml.tf_cooldown_secs.unwrap_or(DEFAULT_TF_COOLDOWN_SECS),
            tf_entry_threshold_bps: yaml.tf_entry_threshold_bps.unwrap_or(DEFAULT_TF_ENTRY_THRESHOLD_BPS),
            tf_take_profit_bps: yaml.tf_take_profit_bps.unwrap_or(DEFAULT_TF_TAKE_PROFIT_BPS),
            tf_trail_stop_bps: yaml.tf_trail_stop_bps.unwrap_or(DEFAULT_TF_TRAIL_STOP_BPS),
            mr_revert_bps: yaml.mr_revert_bps.unwrap_or(DEFAULT_MR_REVERT_BPS),
            mr_trend_pause_bps: yaml.mr_trend_pause_bps.unwrap_or(DEFAULT_MR_TREND_PAUSE_BPS),
        };
        cfg.apply_env_overrides();
        Ok(cfg)
    }

    fn from_env() -> Result<Self> {
        let mut cfg = MmConfig {
            symbol: env::var("SYMBOL").unwrap_or_else(|_| "BTC".to_string()),
            dry_run: env::var("DRY_RUN")
                .unwrap_or_else(|_| "true".to_string())
                .to_lowercase()
                == "true",
            interval_secs: parse_env("INTERVAL_SECS", DEFAULT_INTERVAL_SECS),
            spread_bps: parse_env("SPREAD_BPS", DEFAULT_SPREAD_BPS),
            order_size_pct: parse_env("ORDER_SIZE_PCT", DEFAULT_ORDER_SIZE_PCT),
            max_inventory_pct: parse_env("MAX_INVENTORY_PCT", DEFAULT_MAX_INVENTORY_PCT),
            skew_factor: parse_env("SKEW_FACTOR", DEFAULT_SKEW_FACTOR),
            max_leverage: parse_env("MAX_LEVERAGE", DEFAULT_MAX_LEVERAGE),
            equity_usd: parse_env("EQUITY_USD_FALLBACK", DEFAULT_EQUITY_USD),
            order_levels: parse_env("ORDER_LEVELS", DEFAULT_ORDER_LEVELS),
            level_spacing_bps: parse_env("LEVEL_SPACING_BPS", DEFAULT_LEVEL_SPACING_BPS),
            hedge_enabled: env::var("HEDGE_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .to_lowercase()
                == "true",
            hedge_threshold_ratio: parse_env(
                "HEDGE_THRESHOLD_RATIO",
                DEFAULT_HEDGE_THRESHOLD_RATIO,
            ),
            hedge_close_threshold_ratio: parse_env(
                "HEDGE_CLOSE_THRESHOLD_RATIO",
                DEFAULT_HEDGE_CLOSE_THRESHOLD_RATIO,
            ),
            inventory_hard_limit_mult: parse_env(
                "INVENTORY_HARD_LIMIT_MULT",
                DEFAULT_INVENTORY_HARD_LIMIT_MULT,
            ),
            force_close_mult: parse_env("FORCE_CLOSE_MULT", DEFAULT_FORCE_CLOSE_MULT),
            force_close_cooldown_secs: parse_env(
                "FORCE_CLOSE_COOLDOWN_SECS",
                DEFAULT_FORCE_CLOSE_COOLDOWN_SECS,
            ),
            stale_order_secs: parse_env("STALE_ORDER_SECS", DEFAULT_STALE_ORDER_SECS),
            volatility_window: parse_env("VOLATILITY_WINDOW", DEFAULT_VOLATILITY_WINDOW),
            volatility_spread_mult: parse_env(
                "VOLATILITY_SPREAD_MULT",
                DEFAULT_VOLATILITY_SPREAD_MULT,
            ),
            min_spread_bps: parse_env("MIN_SPREAD_BPS", DEFAULT_MIN_SPREAD_BPS),
            max_spread_bps: parse_env("MAX_SPREAD_BPS", DEFAULT_MAX_SPREAD_BPS),
            ob_depth: parse_env("OB_DEPTH", DEFAULT_OB_DEPTH),
            ob_imbalance_factor: parse_env("OB_IMBALANCE_FACTOR", DEFAULT_OB_IMBALANCE_FACTOR),
            funding_rate_factor: parse_env("FUNDING_RATE_FACTOR", DEFAULT_FUNDING_RATE_FACTOR),
            trend_window: parse_env("TREND_WINDOW", DEFAULT_TREND_WINDOW),
            trend_threshold_bps: parse_env("TREND_THRESHOLD_BPS", DEFAULT_TREND_THRESHOLD_BPS),
            ema_short_periods: parse_env("EMA_SHORT_PERIODS", DEFAULT_EMA_SHORT_PERIODS),
            ema_long_periods: parse_env("EMA_LONG_PERIODS", DEFAULT_EMA_LONG_PERIODS),
            trend_strength_threshold: parse_env(
                "TREND_STRENGTH_THRESHOLD",
                DEFAULT_TREND_STRENGTH_THRESHOLD,
            ),
            aggressive_unwind_bps: parse_env(
                "AGGRESSIVE_UNWIND_BPS",
                DEFAULT_AGGRESSIVE_UNWIND_BPS,
            ),
            post_fill_spread_mult: parse_env(
                "POST_FILL_SPREAD_MULT",
                DEFAULT_POST_FILL_SPREAD_MULT,
            ),
            post_fill_decay_secs: parse_env(
                "POST_FILL_DECAY_SECS",
                DEFAULT_POST_FILL_DECAY_SECS,
            ),
            inventory_spread_mult: parse_env(
                "INVENTORY_SPREAD_MULT",
                DEFAULT_INVENTORY_SPREAD_MULT,
            ),
            strategy_mode: env::var("STRATEGY_MODE")
                .unwrap_or_else(|_| "passive_mm".to_string()),
            capture_min_spread_bps: parse_env(
                "CAPTURE_MIN_SPREAD_BPS",
                DEFAULT_CAPTURE_MIN_SPREAD_BPS,
            ),
            capture_close_timeout_secs: parse_env(
                "CAPTURE_CLOSE_TIMEOUT_SECS",
                DEFAULT_CAPTURE_CLOSE_TIMEOUT_SECS,
            ),
            capture_scan_interval_secs: parse_env(
                "CAPTURE_SCAN_INTERVAL_SECS",
                DEFAULT_CAPTURE_SCAN_INTERVAL_SECS,
            ),
            capture_poll_interval_secs: parse_env(
                "CAPTURE_POLL_INTERVAL_SECS",
                DEFAULT_CAPTURE_POLL_INTERVAL_SECS,
            ),
            price_decimals: parse_env("PRICE_DECIMALS", DEFAULT_PRICE_DECIMALS),
            size_decimals: parse_env("SIZE_DECIMALS", DEFAULT_SIZE_DECIMALS),
            spot_hedge_symbol: env::var("SPOT_HEDGE_SYMBOL").unwrap_or_default(),
            stop_loss_bps: parse_env("STOP_LOSS_BPS", DEFAULT_STOP_LOSS_BPS),
            tf_cooldown_secs: parse_env("TF_COOLDOWN_SECS", DEFAULT_TF_COOLDOWN_SECS),
            tf_entry_threshold_bps: parse_env("TF_ENTRY_THRESHOLD_BPS", DEFAULT_TF_ENTRY_THRESHOLD_BPS),
            tf_take_profit_bps: parse_env("TF_TAKE_PROFIT_BPS", DEFAULT_TF_TAKE_PROFIT_BPS),
            tf_trail_stop_bps: parse_env("TF_TRAIL_STOP_BPS", DEFAULT_TF_TRAIL_STOP_BPS),
            mr_revert_bps: parse_env("MR_REVERT_BPS", DEFAULT_MR_REVERT_BPS),
            mr_trend_pause_bps: parse_env("MR_TREND_PAUSE_BPS", DEFAULT_MR_TREND_PAUSE_BPS),
        };
        cfg.apply_env_overrides();
        Ok(cfg)
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(v) = env::var("SYMBOL") {
            self.symbol = v;
        }
        if let Ok(v) = env::var("DRY_RUN") {
            self.dry_run = v.to_lowercase() == "true";
        }
    }
}

fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ---------------------------------------------------------------------------
// Reactive Capture state machine
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum CapturePhase {
    /// Waiting for spread opportunity
    Scanning,
    /// Both bid and ask Post-Only orders placed, waiting for fills
    BothPending {
        bid_order_id: String,
        ask_order_id: String,
        bid_price: f64,
        ask_price: f64,
        size: f64,
        placed_at: Instant,
    },
    /// One side filled, waiting for the other side to fill (with requoting)
    OneFilled {
        remaining_order_id: String,
        filled_side: OrderSide,
        filled_price: f64,
        filled_size: f64,
        other_price: f64,
        other_size: f64,
        filled_at: Instant,
        last_requote_at: Instant,
        requote_count: u32,
    },
}

struct CaptureState {
    phase: CapturePhase,
    captures: u64,
    capture_pnl: f64,
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

pub struct MmEngine {
    cfg: MmConfig,
    /// Main MM account connector
    main_conn: Arc<dyn DexConnector + Send + Sync>,
    /// Hedge account connector (optional)
    hedge_conn: Option<Arc<dyn DexConnector + Send + Sync>>,
    /// Cached account equity in USD (main only, used for position sizing)
    equity_cache: f64,
    /// Cached hedge account equity in USD
    hedge_equity_cache: f64,
    last_equity_fetch: Option<Instant>,
    /// Signed inventory: positive = long, negative = short (in token units)
    inventory: f64,
    /// Current hedge position size on hedge account (signed)
    hedge_position: f64,
    /// Initial spot LIT balance at startup (to compute hedge delta)
    initial_spot_lit_balance: Option<f64>,
    /// Recent mid prices for volatility estimation
    mid_prices: Vec<f64>,
    /// Track active order IDs on main account
    active_bid_ids: Vec<String>,
    active_ask_ids: Vec<String>,
    /// When orders were last placed
    last_order_time: Option<Instant>,
    /// Cumulative realized PnL (USD) from inventory changes
    realized_pnl: f64,
    /// Last mid price used for PnL tracking
    last_mid: Option<f64>,
    /// Cached max_inventory_tokens for use in instant hedge threshold
    cached_max_inv: f64,
    /// Whether we are in maintenance wind-down mode
    in_maintenance_wind_down: bool,
    /// Last time force-close was triggered (cooldown)
    last_force_close: Option<Instant>,
    /// Cached funding rate (refreshed with equity)
    cached_funding_rate: f64,
    /// Entry price of current position (from exchange)
    position_entry_price: Option<f64>,
    /// EMA short value for trend detection
    ema_short: Option<f64>,
    /// EMA long value for trend detection
    ema_long: Option<f64>,
    /// Last time a fill was detected (for post-fill spread boost)
    last_fill_time: Option<Instant>,
    /// Running stats
    total_trades: u64,
    total_bid_fills: u64,
    total_ask_fills: u64,
    /// Dashboard status reporter
    status_reporter: Option<StatusReporter>,
    /// State for reactive spread capture strategy
    capture_state: CaptureState,
    /// Trend-follow: direction of current position (None = flat)
    tf_direction: Option<OrderSide>,
    /// Trend-follow: entry price of current trend position
    tf_entry_price: Option<f64>,
    /// Trend-follow: peak price seen since entry (for trailing stop)
    tf_peak_price: Option<f64>,
    /// Trend-follow: last stop-loss time (for cooldown)
    tf_last_stop_time: Option<Instant>,
    /// Consecutive stop-loss count (resets on TP or ema_reverted)
    tf_consecutive_sl: u32,
    /// Trend-follow: number of trades taken
    tf_trades: u64,
    /// Trend-follow: cumulative realized PnL
    tf_pnl: f64,
    /// Macro EMA short (EMA10) for trend pause
    macro_ema_short: Option<f64>,
    /// Macro EMA long (EMA50) for trend pause
    macro_ema_long: Option<f64>,
    /// Current regime: "range" or "trend"
    regime: &'static str,
}

impl MmEngine {
    pub async fn new(cfg: MmConfig) -> Result<Self> {
        let use_spot_hedge = !cfg.spot_hedge_symbol.is_empty();

        // Token list: include spot symbol so main_conn can trade both perp and spot
        let mut tokens = vec![cfg.symbol.clone()];
        if use_spot_hedge {
            tokens.push(cfg.spot_hedge_symbol.clone());
        }

        // Create main connector (handles both perp and spot via single nonce sequence)
        let main_conn = DexConnectorBox::create_lighter("", cfg.dry_run, &tokens)
            .await
            .context("failed to create main connector")?;
        main_conn
            .start()
            .await
            .context("failed to start main connector")?;

        // Set leverage on main account
        if let Err(e) = main_conn.set_leverage(&cfg.symbol, cfg.max_leverage).await {
            log::warn!("[INIT] Failed to set leverage on main: {:?}", e);
        }

        if use_spot_hedge {
            log::info!(
                "[INIT] Spot hedge enabled: {} (using main connector, no separate hedge_conn)",
                cfg.spot_hedge_symbol
            );
        }

        // Create hedge connector only for legacy sub-account hedge (NOT spot hedge)
        let hedge_conn = if cfg.hedge_enabled && !use_spot_hedge {
            match DexConnectorBox::create_lighter("HEDGE_", cfg.dry_run, &tokens).await {
                Ok(hc) => {
                    if let Err(e) = hc.start().await {
                        log::error!("[INIT] Failed to start hedge connector: {:?}", e);
                        None
                    } else {
                        if let Err(e) = hc.set_leverage(&cfg.symbol, cfg.max_leverage).await {
                            log::warn!("[INIT] Failed to set leverage on hedge: {:?}", e);
                        }
                        log::info!("[INIT] Hedge connector ready (sub-account)");
                        Some(Arc::new(hc) as Arc<dyn DexConnector + Send + Sync>)
                    }
                }
                Err(e) => {
                    log::warn!("[INIT] Hedge connector not available: {:?}", e);
                    None
                }
            }
        } else {
            None
        };

        let equity_fallback = cfg.equity_usd;
        let status_reporter = StatusReporter::from_env(cfg.dry_run, cfg.interval_secs);
        Ok(Self {
            cfg,
            main_conn: Arc::new(main_conn),
            hedge_conn,
            equity_cache: equity_fallback,
            hedge_equity_cache: 0.0,
            last_equity_fetch: None,
            inventory: 0.0,
            hedge_position: 0.0,
            initial_spot_lit_balance: None,
            mid_prices: Vec::new(),
            active_bid_ids: Vec::new(),
            active_ask_ids: Vec::new(),
            last_order_time: None,
            realized_pnl: 0.0,
            last_mid: None,
            cached_max_inv: 0.0,
            in_maintenance_wind_down: false,
            last_force_close: None,
            cached_funding_rate: 0.0,
            position_entry_price: None,
            ema_short: None,
            ema_long: None,
            last_fill_time: None,
            total_trades: 0,
            total_bid_fills: 0,
            total_ask_fills: 0,
            status_reporter,
            capture_state: CaptureState {
                phase: CapturePhase::Scanning,
                captures: 0,
                capture_pnl: 0.0,
            },
            tf_direction: None,
            tf_entry_price: None,
            tf_peak_price: None,
            tf_last_stop_time: None,
            tf_consecutive_sl: 0,
            tf_trades: 0,
            tf_pnl: 0.0,
            macro_ema_short: None,
            macro_ema_long: None,
            regime: "range",
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        log::info!(
            "[CONFIG] symbol={} dry_run={}",
            self.cfg.symbol,
            self.cfg.dry_run
        );
        log::info!(
            "[CONFIG] spread_bps={} order_size_pct={} max_inventory_pct={} order_levels={} skew_factor={}",
            self.cfg.spread_bps,
            self.cfg.order_size_pct,
            self.cfg.max_inventory_pct,
            self.cfg.order_levels,
            self.cfg.skew_factor,
        );
        log::info!(
            "[CONFIG] hedge_enabled={} hedge_threshold_ratio={} hedge_close_threshold_ratio={}",
            self.cfg.hedge_enabled,
            self.cfg.hedge_threshold_ratio,
            self.cfg.hedge_close_threshold_ratio,
        );
        log::info!(
            "[CONFIG] volatility_window={} vol_spread_mult={} min_spread_bps={} max_spread_bps={}",
            self.cfg.volatility_window,
            self.cfg.volatility_spread_mult,
            self.cfg.min_spread_bps,
            self.cfg.max_spread_bps,
        );

        // Wait for WebSocket to warm up so cached open orders are populated
        log::info!("[MM] Waiting for WebSocket to warm up...");
        sleep(Duration::from_secs(5)).await;

        // Clean slate on startup: cancel all orders and close all positions
        log::info!("[MM] Cancelling all main orders on startup...");
        self.cancel_all_main_orders().await;
        // Retry once after a short wait in case WebSocket cache was still loading
        sleep(Duration::from_secs(2)).await;
        self.cancel_all_main_orders().await;

        log::info!("[MM] Closing all main positions on startup...");
        if let Err(e) = self
            .main_conn
            .close_all_positions(Some(self.cfg.symbol.clone()))
            .await
        {
            log::warn!("[MM] Failed to close main positions on startup: {:?}", e);
        }
        if let Some(ref hedge) = self.hedge_conn {
            log::info!("[MM] Cancelling hedge orders on startup...");
            if let Err(e) = hedge
                .cancel_all_orders(Some(self.cfg.symbol.clone()))
                .await
            {
                log::warn!("[MM] Failed to cancel hedge orders on startup: {:?}", e);
            }
            log::info!("[MM] Closing hedge positions on startup...");
            if let Err(e) = hedge
                .close_all_positions(Some(self.cfg.symbol.clone()))
                .await
            {
                log::warn!("[MM] Failed to close hedge positions on startup: {:?}", e);
            }
        }
        // Cancel any stale spot hedge orders on startup
        if !self.cfg.spot_hedge_symbol.is_empty() {
            log::info!("[MM] Cancelling spot hedge orders on startup...");
            if let Err(e) = self
                .main_conn
                .cancel_all_orders(Some(self.cfg.spot_hedge_symbol.clone()))
                .await
            {
                log::warn!("[MM] Failed to cancel spot hedge orders on startup: {:?}", e);
            }
        }
        self.inventory = 0.0;
        self.hedge_position = 0.0;
        self.realized_pnl = 0.0;

        log::info!("[MM] Strategy mode: {}", self.cfg.strategy_mode);

        match self.cfg.strategy_mode.as_str() {
            "reactive_capture" => self.run_reactive().await,
            "trend_follow" => self.run_trend_follow().await,
            "mean_reversion" => self.run_mean_reversion().await,
            _ => self.run_passive_mm().await,
        }
    }

    async fn run_passive_mm(&mut self) -> Result<()> {
        let mut ticker = tokio::time::interval(Duration::from_secs(self.cfg.interval_secs));
        let mut fill_checker = tokio::time::interval(Duration::from_secs(2));
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = self.step().await {
                        log::error!("[MM] step failed: {:?}", e);
                    }
                }
                _ = fill_checker.tick() => {
                    if self.check_fills_and_rebalance().await {
                        // Fill detected → immediate cancel & re-quote with updated inventory
                        log::info!("[MM] Immediate rebalance after fill");
                        if let Err(e) = self.step().await {
                            log::error!("[MM] rebalance step failed: {:?}", e);
                        }
                        // Reset ticker so we don't double-step soon after
                        ticker.reset();
                    }
                }
                _ = sigterm.recv() => {
                    log::info!("[MM] SIGTERM received, shutting down...");
                    break;
                }
            }
        }
        self.shutdown().await;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Reactive spread capture strategy
    // -----------------------------------------------------------------------

    async fn run_reactive(&mut self) -> Result<()> {
        log::info!("[CAPTURE] Starting reactive spread capture strategy");
        log::info!(
            "[CAPTURE] min_spread_bps={} close_timeout={}s scan_interval={}s poll_interval={}s",
            self.cfg.capture_min_spread_bps,
            self.cfg.capture_close_timeout_secs,
            self.cfg.capture_scan_interval_secs,
            self.cfg.capture_poll_interval_secs,
        );
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM");
        loop {
            // Phase-adaptive interval: fast poll when waiting for fills, slower when scanning
            let interval = match &self.capture_state.phase {
                CapturePhase::Scanning => Duration::from_secs(self.cfg.capture_scan_interval_secs),
                _ => Duration::from_secs(self.cfg.capture_poll_interval_secs),
            };

            tokio::select! {
                _ = sleep(interval) => {
                    if let Err(e) = self.capture_step().await {
                        log::error!("[CAPTURE] step failed: {:?}", e);
                    }
                }
                _ = sigterm.recv() => {
                    log::info!("[CAPTURE] SIGTERM received");
                    break;
                }
            }
        }
        self.shutdown().await;
        Ok(())
    }

    async fn capture_step(&mut self) -> Result<()> {
        if self.check_maintenance().await {
            return Ok(());
        }

        let phase = self.capture_state.phase.clone();
        match phase {
            CapturePhase::Scanning => self.capture_scan().await,
            CapturePhase::BothPending { placed_at, .. } => {
                self.capture_check_fills().await?;
                // If still both pending and stale, cancel all
                if matches!(self.capture_state.phase, CapturePhase::BothPending { .. }) {
                    if placed_at.elapsed().as_secs() > self.cfg.capture_close_timeout_secs {
                        log::info!("[CAPTURE] Both orders stale ({}s), cancelling", self.cfg.capture_close_timeout_secs);
                        let _ = self.main_conn.cancel_all_orders(Some(self.cfg.symbol.clone())).await;
                        self.capture_state.phase = CapturePhase::Scanning;
                    }
                }
                Ok(())
            }
            CapturePhase::OneFilled { filled_at, filled_side, filled_price, filled_size, other_price, other_size, last_requote_at, requote_count, .. } => {
                self.capture_check_close_fill().await?;
                // If still waiting, check for requote or force close
                if matches!(self.capture_state.phase, CapturePhase::OneFilled { .. }) {
                    if filled_at.elapsed().as_secs() > self.cfg.capture_close_timeout_secs {
                        // Final timeout: force close with market
                        log::warn!("[CAPTURE] Final timeout ({}s, {} requotes), force closing", self.cfg.capture_close_timeout_secs, requote_count);
                        let _ = self.main_conn.cancel_all_orders(Some(self.cfg.symbol.clone())).await;
                        self.capture_force_close(filled_side, filled_price, filled_size, other_price).await?;
                    } else if last_requote_at.elapsed().as_secs() >= 10 {
                        // Requote every 10 seconds at current best price
                        self.capture_requote(filled_side, filled_price, filled_size, other_size, filled_at, requote_count).await?;
                    }
                }
                Ok(())
            }
        }?;

        // Update dashboard status
        if self.status_reporter.is_some() {
            let positions = self.build_position_info().await;
            let reporter = self.status_reporter.as_mut().unwrap();
            reporter.update_equity(self.equity_cache);
            if let Err(err) = reporter.write_snapshot_if_due(&positions) {
                log::warn!("[STATUS] failed to write status: {:?}", err);
            }
        }
        Ok(())
    }

    /// Scan OB, place both bid and ask Post-Only orders if spread is wide enough
    async fn capture_scan(&mut self) -> Result<()> {
        let ob = self
            .main_conn
            .get_order_book(&self.cfg.symbol, self.cfg.ob_depth)
            .await
            .context("failed to get order book")?;

        let best_bid = ob.bids.first().map(|l| l.price.to_f64().unwrap_or(0.0)).unwrap_or(0.0);
        let best_ask = ob.asks.first().map(|l| l.price.to_f64().unwrap_or(0.0)).unwrap_or(0.0);

        if best_bid <= 0.0 || best_ask <= 0.0 || best_bid >= best_ask {
            return Ok(());
        }

        let mid = (best_bid + best_ask) / 2.0;
        let spread_bps = (best_ask - best_bid) / mid * 10_000.0;

        // Update tracking
        self.mid_prices.push(mid);
        if self.mid_prices.len() > self.cfg.volatility_window {
            self.mid_prices.drain(0..self.mid_prices.len() - self.cfg.volatility_window);
        }
        self.update_ema(mid);
        self.last_mid = Some(mid);

        // Refresh equity periodically
        if self.last_equity_fetch.map(|t| t.elapsed().as_secs() > 300).unwrap_or(true) {
            self.refresh_equity().await;
        }

        // If we have residual inventory, unwind with market order first
        if self.inventory.abs() > 0.0001 {
            let unwind_side = if self.inventory > 0.0 { OrderSide::Short } else { OrderSide::Long };
            let size_dec = self.round_size(self.inventory.abs());
            if size_dec > Decimal::ZERO {
                log::info!("[CAPTURE] Unwinding residual inv={:.6} with market {:?}", self.inventory, unwind_side);
                match self.main_conn.create_order(&self.cfg.symbol, size_dec, unwind_side, None, None, true, None).await {
                    Ok(_) => {
                        sleep(Duration::from_secs(1)).await;
                        self.sync_inventory_from_exchange().await;
                        log::info!("[CAPTURE] Unwind complete, inv={:.6}", self.inventory);
                    }
                    Err(e) => log::error!("[CAPTURE] Failed to unwind: {:?}", e),
                }
            }
        }

        if spread_bps < self.cfg.capture_min_spread_bps {
            log::debug!("[CAPTURE] spread={:.1}bps < min={:.1}bps, waiting", spread_bps, self.cfg.capture_min_spread_bps);
            return Ok(());
        }

        if spread_bps > self.cfg.max_spread_bps {
            log::info!("[CAPTURE] spread={:.1}bps > max={:.1}bps, skipping (OB gap)", spread_bps, self.cfg.max_spread_bps);
            return Ok(());
        }

        // Check EMA trend - skip during strong trends
        let ema_trend = self.compute_ema_trend_bps();
        if ema_trend.abs() > self.cfg.trend_strength_threshold {
            log::info!("[CAPTURE] Trend {:.1}bps, skipping", ema_trend);
            return Ok(());
        }

        let equity = self.equity_cache;
        let leverage = self.cfg.max_leverage as f64;
        let order_size = equity * self.cfg.order_size_pct * leverage / mid;
        let size_dec = self.round_size(order_size);

        // Place both bid and ask at best prices
        let bid_price_dec = self.round_price(best_bid);
        let ask_price_dec = self.round_price(best_ask);

        if size_dec <= Decimal::ZERO || bid_price_dec <= Decimal::ZERO || ask_price_dec <= Decimal::ZERO {
            return Ok(());
        }

        log::info!(
            "[CAPTURE] spread={:.1}bps, placing BOTH GTT: Long@{} + Short@{} size={}",
            spread_bps, bid_price_dec, ask_price_dec, size_dec
        );

        // Place bid (Long) Post-Only
        let bid_result = self.main_conn.create_order(
            &self.cfg.symbol, size_dec, OrderSide::Long,
            Some(bid_price_dec), Some(-2), false, Some(self.cfg.stale_order_secs),
        ).await;

        let bid_order_id = match bid_result {
            Ok(resp) => resp.order_id,
            Err(e) => {
                log::error!("[CAPTURE] Failed to place bid: {:?}", e);
                return Ok(());
            }
        };

        // Place ask (Short) Post-Only
        let ask_result = self.main_conn.create_order(
            &self.cfg.symbol, size_dec, OrderSide::Short,
            Some(ask_price_dec), Some(-2), false, Some(self.cfg.stale_order_secs),
        ).await;

        let ask_order_id = match ask_result {
            Ok(resp) => resp.order_id,
            Err(e) => {
                log::error!("[CAPTURE] Failed to place ask, cancelling bid: {:?}", e);
                let _ = self.main_conn.cancel_order(&self.cfg.symbol, &bid_order_id).await;
                return Ok(());
            }
        };

        self.capture_state.phase = CapturePhase::BothPending {
            bid_order_id,
            ask_order_id,
            bid_price: bid_price_dec.to_f64().unwrap_or(best_bid),
            ask_price: ask_price_dec.to_f64().unwrap_or(best_ask),
            size: size_dec.to_f64().unwrap_or(order_size),
            placed_at: Instant::now(),
        };
        Ok(())
    }

    /// Check if either side filled while both are pending
    async fn capture_check_fills(&mut self) -> Result<()> {
        let (bid_price, ask_price, size) = match &self.capture_state.phase {
            CapturePhase::BothPending { bid_price, ask_price, size, .. } => (*bid_price, *ask_price, *size),
            _ => return Ok(()),
        };

        let fills = self.main_conn.get_filled_orders(&self.cfg.symbol).await?;
        if fills.orders.is_empty() {
            return Ok(());
        }

        let mut total_size = 0.0;
        let mut total_value = 0.0;
        let mut filled_side: Option<OrderSide> = None;
        for fill in &fills.orders {
            let sz = fill.filled_size.and_then(|s| s.to_f64()).unwrap_or(0.0);
            let val = fill.filled_value.and_then(|v| v.to_f64()).unwrap_or(0.0);
            total_size += sz;
            total_value += val;
            if filled_side.is_none() {
                filled_side = fill.filled_side;
            }
            let _ = self.main_conn.clear_filled_order(&self.cfg.symbol, &fill.trade_id).await;
        }

        if total_size <= 0.0 || filled_side.is_none() {
            return Ok(());
        }

        let side = filled_side.unwrap();
        let fill_price = if total_value > 0.0 { total_value / total_size } else {
            match side { OrderSide::Long => bid_price, OrderSide::Short => ask_price }
        };

        // Determine other side's price
        let (other_price, _other_size) = match side {
            OrderSide::Long => (ask_price, size),   // bid filled, waiting for ask
            OrderSide::Short => (bid_price, size),   // ask filled, waiting for bid
        };

        log::info!(
            "[CAPTURE] {:?} FILLED: size={:.6} price={:.6}, requoting other side immediately (was @ {:.6})",
            side, total_size, fill_price, other_price
        );

        // Cancel the other order and immediately requote at current best price
        let _ = self.main_conn.cancel_all_orders(Some(self.cfg.symbol.clone())).await;

        let ob = self.main_conn.get_order_book(&self.cfg.symbol, self.cfg.ob_depth).await;
        let new_other_price = match ob {
            Ok(ref ob_snap) => {
                let (close_side_price, close_side) = match side {
                    OrderSide::Long => {
                        let p = ob_snap.asks.first().map(|l| l.price.to_f64().unwrap_or(0.0)).unwrap_or(0.0);
                        (p, OrderSide::Short)
                    }
                    OrderSide::Short => {
                        let p = ob_snap.bids.first().map(|l| l.price.to_f64().unwrap_or(0.0)).unwrap_or(0.0);
                        (p, OrderSide::Long)
                    }
                };
                if close_side_price > 0.0 {
                    let price_dec = self.round_price(close_side_price);
                    let size_dec = self.round_size(size);
                    if price_dec > Decimal::ZERO && size_dec > Decimal::ZERO {
                        log::info!(
                            "[CAPTURE] Immediate requote: {:?}@{} (entry {:?}@{:.6})",
                            close_side, price_dec, side, fill_price
                        );
                        let _ = self.main_conn.create_order(
                            &self.cfg.symbol, size_dec, close_side,
                            Some(price_dec), Some(-2), true, Some(self.cfg.stale_order_secs),
                        ).await;
                    }
                    close_side_price
                } else {
                    other_price
                }
            }
            Err(_) => other_price,
        };

        let now = Instant::now();
        self.capture_state.phase = CapturePhase::OneFilled {
            remaining_order_id: String::new(),
            filled_side: side,
            filled_price: fill_price,
            filled_size: total_size,
            other_price: new_other_price,
            other_size: size,
            filled_at: now,
            last_requote_at: now,
            requote_count: 1, // already did first requote
        };
        Ok(())
    }

    /// Check if the close side (other order) has filled
    async fn capture_check_close_fill(&mut self) -> Result<()> {
        let (filled_side, filled_price, filled_size) = match &self.capture_state.phase {
            CapturePhase::OneFilled { filled_side, filled_price, filled_size, .. } => (*filled_side, *filled_price, *filled_size),
            _ => return Ok(()),
        };

        let fills = self.main_conn.get_filled_orders(&self.cfg.symbol).await?;
        if fills.orders.is_empty() {
            return Ok(());
        }

        let mut close_size = 0.0;
        let mut close_value = 0.0;
        for fill in &fills.orders {
            let sz = fill.filled_size.and_then(|s| s.to_f64()).unwrap_or(0.0);
            let val = fill.filled_value.and_then(|v| v.to_f64()).unwrap_or(0.0);
            close_size += sz;
            close_value += val;
            let _ = self.main_conn.clear_filled_order(&self.cfg.symbol, &fill.trade_id).await;
        }

        if close_size <= 0.0 {
            return Ok(());
        }

        let close_price = if close_value > 0.0 { close_value / close_size } else { 0.0 };

        // Cancel any remaining orders
        let _ = self.main_conn.cancel_all_orders(Some(self.cfg.symbol.clone())).await;
        self.sync_inventory_from_exchange().await;
        self.capture_state.captures += 1;

        // Calculate PnL: Long@bid then Short@ask = ask - bid profit
        let pnl = match filled_side {
            OrderSide::Long => (close_price - filled_price) * close_size.min(filled_size),
            OrderSide::Short => (filled_price - close_price) * close_size.min(filled_size),
        };
        self.capture_state.capture_pnl += pnl;

        log::info!(
            "[CAPTURE] Round-trip #{}: {:?}@{:.6} + close@{:.6}, pnl=${:.4}, total=${:.4}, inv={:.6}",
            self.capture_state.captures, filled_side, filled_price, close_price,
            pnl, self.capture_state.capture_pnl, self.inventory
        );

        self.capture_state.phase = CapturePhase::Scanning;
        Ok(())
    }

    /// Requote the close side at current best bid/ask
    async fn capture_requote(
        &mut self,
        filled_side: OrderSide,
        filled_price: f64,
        filled_size: f64,
        other_size: f64,
        filled_at: Instant,
        requote_count: u32,
    ) -> Result<()> {
        // Cancel existing close order
        let _ = self.main_conn.cancel_all_orders(Some(self.cfg.symbol.clone())).await;

        // Get current OB for best price
        let ob = self
            .main_conn
            .get_order_book(&self.cfg.symbol, self.cfg.ob_depth)
            .await
            .context("failed to get order book for requote")?;

        let best_bid = ob.bids.first().map(|l| l.price.to_f64().unwrap_or(0.0)).unwrap_or(0.0);
        let best_ask = ob.asks.first().map(|l| l.price.to_f64().unwrap_or(0.0)).unwrap_or(0.0);

        if best_bid <= 0.0 || best_ask <= 0.0 {
            log::warn!("[CAPTURE] Requote: invalid OB, skipping");
            return Ok(());
        }

        // Close side: if we bought (Long), we need to sell (Short) at best_ask
        //             if we sold (Short), we need to buy (Long) at best_bid
        let (close_side, close_price) = match filled_side {
            OrderSide::Long => (OrderSide::Short, best_ask),
            OrderSide::Short => (OrderSide::Long, best_bid),
        };

        let close_price_dec = self.round_price(close_price);
        let size_dec = self.round_size(other_size);

        if size_dec <= Decimal::ZERO || close_price_dec <= Decimal::ZERO {
            return Ok(());
        }

        let new_count = requote_count + 1;
        log::info!(
            "[CAPTURE] Requote #{}: {:?}@{} (entry {:?}@{:.6}, elapsed {}s)",
            new_count, close_side, close_price_dec, filled_side, filled_price,
            filled_at.elapsed().as_secs()
        );

        match self.main_conn.create_order(
            &self.cfg.symbol, size_dec, close_side,
            Some(close_price_dec), Some(-2), true, Some(self.cfg.stale_order_secs),
        ).await {
            Ok(_) => {
                let now = Instant::now();
                self.capture_state.phase = CapturePhase::OneFilled {
                    remaining_order_id: String::new(),
                    filled_side,
                    filled_price,
                    filled_size,
                    other_price: close_price,
                    other_size,
                    filled_at,
                    last_requote_at: now,
                    requote_count: new_count,
                };
            }
            Err(e) => {
                log::error!("[CAPTURE] Requote failed: {:?}", e);
            }
        }
        Ok(())
    }

    /// Force close when the other side doesn't fill within timeout
    async fn capture_force_close(
        &mut self,
        filled_side: OrderSide,
        filled_price: f64,
        _filled_size: f64,
        other_price: f64,
    ) -> Result<()> {
        self.sync_inventory_from_exchange().await;

        if self.inventory.abs() < 0.0001 {
            log::info!("[CAPTURE] No inventory to force-close, back to scanning");
            self.capture_state.phase = CapturePhase::Scanning;
            return Ok(());
        }

        let close_side = if self.inventory > 0.0 { OrderSide::Short } else { OrderSide::Long };
        let size_dec = self.round_size(self.inventory.abs());

        if size_dec <= Decimal::ZERO {
            self.capture_state.phase = CapturePhase::Scanning;
            return Ok(());
        }

        log::warn!(
            "[CAPTURE] Force closing: market {:?} size={} (open {:?}@{:.6}, target was {:.6})",
            close_side, size_dec, filled_side, filled_price, other_price
        );

        match self.main_conn.create_order(
            &self.cfg.symbol, size_dec, close_side, None, None, true, None,
        ).await {
            Ok(_) => {
                sleep(Duration::from_secs(1)).await;
                self.sync_inventory_from_exchange().await;
                self.capture_state.captures += 1;
                log::warn!(
                    "[CAPTURE] Force-closed #{}, inv={:.6}",
                    self.capture_state.captures, self.inventory
                );
                self.capture_state.phase = CapturePhase::Scanning;
            }
            Err(e) => {
                log::error!("[CAPTURE] Force close failed: {:?}", e);
                self.capture_state.phase = CapturePhase::Scanning;
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Trend Follow strategy
    // -----------------------------------------------------------------------

    async fn run_trend_follow(&mut self) -> Result<()> {
        log::info!(
            "[TF] Starting trend_follow: entry_threshold={}bps stop_loss={}bps take_profit={}bps trail_stop={}bps cooldown={}s",
            self.cfg.tf_entry_threshold_bps,
            self.cfg.stop_loss_bps,
            self.cfg.tf_take_profit_bps,
            self.cfg.tf_trail_stop_bps,
            self.cfg.tf_cooldown_secs,
        );

        let mut ticker = tokio::time::interval(Duration::from_secs(self.cfg.interval_secs));
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");

        // Initial equity + EMA warm-up
        self.refresh_equity().await;

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = self.tf_step().await {
                        log::error!("[TF] step failed: {:?}", e);
                    }
                }
                _ = sigterm.recv() => {
                    log::info!("[TF] SIGTERM received, shutting down...");
                    break;
                }
            }
        }
        // On shutdown: close perp position, cancel orders
        self.tf_close_position_market("shutdown").await;
        self.cancel_all_main_orders().await;
        // Cancel spot hedge orders
        if !self.cfg.spot_hedge_symbol.is_empty() {
            let _ = self.main_conn.cancel_all_orders(Some(self.cfg.spot_hedge_symbol.clone())).await;
        }
        log::info!(
            "[TF] Shutdown complete. trades={} pnl={:.2}",
            self.tf_trades,
            self.tf_pnl
        );
        Ok(())
    }

    async fn tf_step(&mut self) -> Result<()> {
        // 0. Maintenance check
        if self.check_maintenance().await {
            if self.tf_direction.is_some() {
                self.tf_close_position_market("maintenance").await;
            }
            return Ok(());
        }

        // 1. Get mid price from order book
        let ob = self
            .main_conn
            .get_order_book(&self.cfg.symbol, self.cfg.ob_depth)
            .await
            .context("failed to fetch order book")?;

        let best_bid = ob.bids.first().map(|l| l.price.to_f64().unwrap_or(0.0)).unwrap_or(0.0);
        let best_ask = ob.asks.first().map(|l| l.price.to_f64().unwrap_or(0.0)).unwrap_or(0.0);
        if best_bid <= 0.0 || best_ask <= 0.0 {
            log::warn!("[TF] Invalid OB: bid={} ask={}", best_bid, best_ask);
            return Ok(());
        }
        let mid = (best_bid + best_ask) / 2.0;
        let spread_bps = (best_ask - best_bid) / mid * 10_000.0;

        let wide_spread = spread_bps > self.cfg.max_spread_bps;

        if wide_spread {
            if self.tf_direction.is_some() {
                let extreme_spread = spread_bps > self.cfg.max_spread_bps * 4.0;
                if extreme_spread {
                    log::warn!(
                        "[TF] Extreme spread {:.1}bps (>{:.0}bps), skipping SL check — mid unreliable (bid={:.4} ask={:.4})",
                        spread_bps, self.cfg.max_spread_bps * 4.0, best_bid, best_ask
                    );
                    return Ok(());
                }
                log::warn!(
                    "[TF] Wide spread {:.1}bps but have position, checking stop-loss only",
                    spread_bps
                );
                let direction = self.tf_direction.as_ref().unwrap().clone();
                let entry = self.tf_entry_price.unwrap_or(best_bid);
                let exit_price = match direction {
                    OrderSide::Long => best_bid,
                    _ => best_ask,
                };
                let pnl_bps = match direction {
                    OrderSide::Long => (exit_price - entry) / entry * 10_000.0,
                    _ => (entry - exit_price) / entry * 10_000.0,
                };
                if pnl_bps <= -(self.cfg.stop_loss_bps) {
                    log::warn!("[TF] STOP-LOSS during wide spread: pnl={:+.1}bps", pnl_bps);
                    self.tf_close_position_market("stop_loss").await;
                    self.tf_last_stop_time = Some(Instant::now());
                }
                return Ok(());
            }
            log::debug!(
                "[TF] Wide spread {:.1}bps > max {:.1}bps, skipping tick",
                spread_bps, self.cfg.max_spread_bps
            );
            return Ok(());
        }

        // 2. Update EMA
        self.update_ema(mid);
        self.last_mid = Some(mid);

        // 3. Refresh equity periodically
        self.refresh_equity().await;

        // 4. Sync position from exchange
        self.sync_inventory_from_exchange().await;

        // 5. Compute EMA trend signal
        let ema_trend_bps = self.compute_ema_trend_bps();

        // 6. If we have a position, manage it (stop-loss / take-profit / trailing stop)
        if let Some(direction) = self.tf_direction.clone() {
            let entry = self.tf_entry_price.unwrap_or(mid);
            // Use exit-side price (bid for Long, ask for Short) instead of mid
            // to avoid false triggers from wide spreads on thin order books
            let exit_price = match direction {
                OrderSide::Long => best_bid,
                _ => best_ask,
            };
            let pnl_bps = match direction {
                OrderSide::Long => (exit_price - entry) / entry * 10_000.0,
                _ => (entry - exit_price) / entry * 10_000.0,
            };

            // Update peak price for trailing stop (use exit-side price)
            let peak = self.tf_peak_price.unwrap_or(exit_price);
            let new_peak = match direction {
                OrderSide::Long => peak.max(exit_price),
                _ => if peak == 0.0 { exit_price } else { peak.min(exit_price) },
            };
            self.tf_peak_price = Some(new_peak);

            // Trailing stop: distance from peak
            let trail_bps = match direction {
                OrderSide::Long => (new_peak - exit_price) / new_peak * 10_000.0,
                _ => (exit_price - new_peak) / new_peak * 10_000.0,
            };

            log::info!(
                "[TF] Position {:?}: pnl={:+.1}bps trail={:.1}bps peak={:.4} exit_price={:.4} ema_trend={:+.1}bps",
                direction, pnl_bps, trail_bps, new_peak, exit_price, ema_trend_bps
            );

            // Stop-loss
            if pnl_bps <= -(self.cfg.stop_loss_bps) {
                log::warn!("[TF] STOP-LOSS triggered: pnl={:+.1}bps", pnl_bps);
                self.tf_close_position_market("stop_loss").await;
                self.tf_last_stop_time = Some(Instant::now());
                return Ok(());
            }

            // Take-profit
            if pnl_bps >= self.cfg.tf_take_profit_bps {
                log::info!("[TF] TAKE-PROFIT triggered: pnl={:+.1}bps", pnl_bps);
                self.tf_close_position_market("take_profit").await;
                return Ok(());
            }

            // Trailing stop (only after position is in profit)
            if pnl_bps > 0.0 && trail_bps >= self.cfg.tf_trail_stop_bps {
                log::info!("[TF] TRAILING-STOP triggered: trail={:.1}bps pnl={:+.1}bps", trail_bps, pnl_bps);
                self.tf_close_position_market("trail_stop").await;
                return Ok(());
            }

            // Trend reversal exit: if EMA flips against position, exit
            let trend_reversed = match direction {
                OrderSide::Long => ema_trend_bps < -(self.cfg.tf_entry_threshold_bps / 2.0),
                _ => ema_trend_bps > self.cfg.tf_entry_threshold_bps / 2.0,
            };
            if trend_reversed {
                log::info!("[TF] TREND-REVERSAL exit: ema={:+.1}bps direction={:?}", ema_trend_bps, direction);
                self.tf_close_position_market("trend_reversal").await;
                return Ok(());
            }

            // Report status
            self.tf_report_status(mid, pnl_bps).await;
        } else {
            // 7. No position — check for entry signal
            // Cooldown check
            if let Some(last_stop) = self.tf_last_stop_time {
                if last_stop.elapsed() < Duration::from_secs(self.cfg.tf_cooldown_secs) {
                    let remaining = self.cfg.tf_cooldown_secs - last_stop.elapsed().as_secs();
                    log::debug!("[TF] Cooldown: {}s remaining", remaining);
                    return Ok(());
                }
            }

            // Entry: EMA trend exceeds threshold
            if ema_trend_bps.abs() >= self.cfg.tf_entry_threshold_bps {
                let side = if ema_trend_bps > 0.0 {
                    OrderSide::Long
                } else {
                    OrderSide::Short
                };

                // Calculate position size: order_size_pct of equity
                let equity = self.equity_cache;
                let size_usd = equity * self.cfg.order_size_pct;
                let size_tokens = size_usd / mid;
                let size_dec = self.round_size(size_tokens);

                if size_dec <= Decimal::ZERO {
                    log::warn!("[TF] Computed size is zero, equity={:.2}", equity);
                    return Ok(());
                }

                log::info!(
                    "[TF] ENTRY signal: ema={:+.1}bps → {:?} size={} (${:.2}) mid={:.4}",
                    ema_trend_bps, side, size_dec, size_usd, mid
                );

                // Place market order on perp
                match self.main_conn
                    .create_order(&self.cfg.symbol, size_dec, side.clone(), None, None, false, None)
                    .await
                {
                    Ok(_) => {
                        self.tf_direction = Some(side.clone());
                        self.tf_entry_price = Some(mid);
                        self.tf_peak_price = Some(mid);
                        self.tf_trades += 1;
                        log::info!("[TF] Entry placed: {:?} size={} @ ~{:.4}", side, size_dec, mid);

                        // Wait for fill then sync
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        self.sync_inventory_from_exchange().await;

                        // Update entry price from actual fill if available
                        if let Some(ep) = self.position_entry_price {
                            self.tf_entry_price = Some(ep);
                            log::info!("[TF] Actual entry price: {:.4}", ep);
                        }

                        // Immediate spot hedge
                        self.tf_spot_hedge().await;
                    }
                    Err(e) => {
                        log::error!("[TF] Entry order failed: {:?}", e);
                    }
                }
            } else {
                log::debug!("[TF] Flat, waiting for signal: ema={:+.1}bps (threshold={}bps)", ema_trend_bps, self.cfg.tf_entry_threshold_bps);
            }

            // Report flat status
            self.tf_report_status(mid, 0.0).await;
        }

        Ok(())
    }

    /// Close the current trend-follow perp position with a market order
    async fn tf_close_position_market(&mut self, reason: &str) {
        if self.tf_direction.is_none() {
            return;
        }
        let direction = self.tf_direction.as_ref().unwrap().clone();
        let entry = self.tf_entry_price.unwrap_or(0.0);
        let mid = self.last_mid.unwrap_or(0.0);

        // Snapshot equity before close to compute actual realized PnL
        let equity_before = self.equity_cache;

        // Close via close_all_positions for reliability
        log::info!("[TF] Closing position ({}): {:?} entry={:.4} mid={:.4}", reason, direction, entry, mid);
        if let Err(e) = self
            .main_conn
            .close_all_positions(Some(self.cfg.symbol.clone()))
            .await
        {
            log::error!("[TF] Failed to close position: {:?}", e);
            // Fallback: try market order in opposite direction
            let close_side = match direction {
                OrderSide::Long => OrderSide::Short,
                _ => OrderSide::Long,
            };
            let size = self.round_size(self.inventory.abs());
            if size > Decimal::ZERO {
                let _ = self.main_conn
                    .create_order(&self.cfg.symbol, size, close_side, None, None, false, None)
                    .await;
            }
        }

        // Wait for fill then refresh equity from exchange for accurate PnL
        tokio::time::sleep(Duration::from_secs(1)).await;
        self.last_equity_fetch = None; // force refresh
        self.refresh_equity().await;
        self.sync_inventory_from_exchange().await;

        // Compute realized PnL from actual equity change (not mid price)
        let pnl = self.equity_cache - equity_before;
        self.tf_pnl += pnl;

        log::info!(
            "[TF] Closed ({}): pnl={:+.2} cumulative={:+.2} trades={} (equity {:.2} → {:.2})",
            reason, pnl, self.tf_pnl, self.tf_trades, equity_before, self.equity_cache
        );

        // Unwind spot hedge
        self.tf_spot_hedge().await;

        // Reset state
        self.tf_direction = None;
        self.tf_entry_price = None;
        self.tf_peak_price = None;
    }

    /// Spot hedge: keep delta-neutral by hedging perp inventory on spot market
    async fn tf_spot_hedge(&mut self) {
        if self.cfg.spot_hedge_symbol.is_empty() {
            return;
        }
        let mid = self.last_mid.unwrap_or(0.0);
        if mid <= 0.0 {
            return;
        }

        let net_exposure = self.inventory + self.hedge_position;
        if net_exposure.abs() < 1.0 {
            return;
        }

        let hedge_side = if net_exposure > 0.0 {
            OrderSide::Short // net long → sell spot
        } else {
            OrderSide::Long // net short → buy spot
        };
        let hedge_size = self.round_size(net_exposure.abs());
        if hedge_size <= Decimal::ZERO {
            return;
        }

        log::info!(
            "[TF-HEDGE] net_exp={:.2} → {:?} {} size={}",
            net_exposure, hedge_side, self.cfg.spot_hedge_symbol, hedge_size
        );

        match self.main_conn
            .create_order(&self.cfg.spot_hedge_symbol, hedge_size, hedge_side.clone(), None, None, false, None)
            .await
        {
            Ok(_) => {
                log::info!("[TF-HEDGE] Spot hedge placed: {:?} size={}", hedge_side, hedge_size);
                tokio::time::sleep(Duration::from_secs(1)).await;
                self.sync_inventory_from_exchange().await;
            }
            Err(e) => {
                log::error!("[TF-HEDGE] Failed: {:?}", e);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Mean Reversion strategy
    // -----------------------------------------------------------------------

    async fn run_mean_reversion(&mut self) -> Result<()> {
        log::info!(
            "[MR] Starting mean_reversion: entry={}bps stop={}bps tp={}bps revert={}bps cooldown={}s interval={}s",
            self.cfg.tf_entry_threshold_bps,
            self.cfg.stop_loss_bps,
            self.cfg.tf_take_profit_bps,
            self.cfg.mr_revert_bps,
            self.cfg.tf_cooldown_secs,
            self.cfg.interval_secs,
        );

        let mut ticker = tokio::time::interval(Duration::from_secs(self.cfg.interval_secs));
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");

        self.refresh_equity().await;

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = self.mr_step().await {
                        log::error!("[MR] step failed: {:?}", e);
                    }
                }
                _ = sigterm.recv() => {
                    log::info!("[MR] SIGTERM received, shutting down...");
                    break;
                }
            }
        }
        self.tf_close_position_market("shutdown").await;
        self.cancel_all_main_orders().await;
        if !self.cfg.spot_hedge_symbol.is_empty() {
            let _ = self.main_conn.cancel_all_orders(Some(self.cfg.spot_hedge_symbol.clone())).await;
        }
        log::info!("[MR] Shutdown. trades={} pnl={:.2}", self.tf_trades, self.tf_pnl);
        Ok(())
    }

    async fn mr_step(&mut self) -> Result<()> {
        if self.check_maintenance().await {
            if self.tf_direction.is_some() {
                self.tf_close_position_market("maintenance").await;
            }
            return Ok(());
        }

        // 1. Get mid price
        let ob = self
            .main_conn
            .get_order_book(&self.cfg.symbol, self.cfg.ob_depth)
            .await
            .context("failed to fetch order book")?;

        let best_bid = ob.bids.first().map(|l| l.price.to_f64().unwrap_or(0.0)).unwrap_or(0.0);
        let best_ask = ob.asks.first().map(|l| l.price.to_f64().unwrap_or(0.0)).unwrap_or(0.0);
        if best_bid <= 0.0 || best_ask <= 0.0 {
            return Ok(());
        }
        let mid = (best_bid + best_ask) / 2.0;
        let spread_bps = (best_ask - best_bid) / mid * 10_000.0;

        let wide_spread = spread_bps > self.cfg.max_spread_bps;

        // If spread is wide but we have a position, still check stop-loss using mid.
        // However, if spread is extremely wide (>4× max_spread), mid itself is unreliable
        // (e.g. one side of the book is nearly empty), so skip SL check entirely and wait
        // for the order book to normalize.
        if wide_spread {
            if self.tf_direction.is_some() {
                let extreme_spread = spread_bps > self.cfg.max_spread_bps * 4.0;
                if extreme_spread {
                    log::warn!(
                        "[MR] Extreme spread {:.1}bps (>{:.0}bps), skipping SL check — mid unreliable (bid={:.4} ask={:.4})",
                        spread_bps, self.cfg.max_spread_bps * 4.0, best_bid, best_ask
                    );
                    return Ok(());
                }
                let direction = self.tf_direction.as_ref().unwrap().clone();
                let entry = self.tf_entry_price.unwrap_or(mid);
                let pnl_bps = match direction {
                    OrderSide::Long => (mid - entry) / entry * 10_000.0,
                    _ => (entry - mid) / entry * 10_000.0,
                };
                log::warn!(
                    "[MR] Wide spread {:.1}bps but have position, checking stop-loss with mid={:.4} (bid={:.4} ask={:.4}) pnl={:+.1}bps",
                    spread_bps, mid, best_bid, best_ask, pnl_bps
                );
                if pnl_bps <= -(self.cfg.stop_loss_bps) {
                    log::warn!("[MR] STOP-LOSS during wide spread: pnl={:+.1}bps (mid-based)", pnl_bps);
                    self.tf_close_position_market("stop_loss").await;
                    self.tf_consecutive_sl += 1;
                    let multiplier = 1u64 << self.tf_consecutive_sl.min(4);
                    log::info!(
                        "[MR] Consecutive SL #{}: cooldown {}s ({}x)",
                        self.tf_consecutive_sl, self.cfg.tf_cooldown_secs * multiplier, multiplier
                    );
                    self.tf_last_stop_time = Some(Instant::now());
                }
                return Ok(());
            }
            log::debug!(
                "[MR] Wide spread {:.1}bps > max {:.1}bps, skipping tick (bid={:.4} ask={:.4})",
                spread_bps, self.cfg.max_spread_bps, best_bid, best_ask
            );
            return Ok(());
        }

        // 2. Update EMA (only with clean mid prices)
        self.update_ema(mid);
        self.update_macro_ema(mid);
        self.last_mid = Some(mid);

        // 3. Refresh equity periodically
        self.refresh_equity().await;

        // 4. Sync position from exchange
        self.sync_inventory_from_exchange().await;

        // 5. EMA divergence
        let ema_trend_bps = self.compute_ema_trend_bps();
        let macro_trend_bps = self.compute_macro_trend_bps();

        // 6. Position management
        if let Some(direction) = self.tf_direction.clone() {
            let entry = self.tf_entry_price.unwrap_or(mid);
            // Use exit-side price (bid for Long, ask for Short) instead of mid
            // to avoid false triggers from wide spreads on thin order books
            let exit_price = match direction {
                OrderSide::Long => best_bid,
                _ => best_ask,
            };
            let pnl_bps = match direction {
                OrderSide::Long => (exit_price - entry) / entry * 10_000.0,
                _ => (entry - exit_price) / entry * 10_000.0,
            };

            log::info!(
                "[MR] Position {:?}: pnl={:+.1}bps ema={:+.1}bps mid={:.4} exit_price={:.4}",
                direction, pnl_bps, ema_trend_bps, mid, exit_price
            );

            let mut close = false;
            let mut reason = "";

            // Regime-aware take-profit: scale TP with macro trend strength
            let regime_mid_threshold = 5.0_f64;
            let macro_abs = macro_trend_bps.abs();
            let regime_scale = if self.regime == "range" {
                1.0
            } else {
                (macro_abs / regime_mid_threshold).max(1.0).min(3.0)
            };
            let effective_tp_bps = self.cfg.tf_take_profit_bps * regime_scale;

            // Stop-loss
            if pnl_bps <= -(self.cfg.stop_loss_bps) {
                close = true;
                reason = "stop_loss";
            }
            // Take-profit (regime-scaled)
            else if pnl_bps >= effective_tp_bps {
                close = true;
                reason = "take_profit";
            }
            // Mean reversion exit: EMA converged back
            else if ema_trend_bps.abs() <= self.cfg.mr_revert_bps {
                close = true;
                reason = "ema_reverted";
            }

            if close {
                log::info!("[MR] CLOSE ({}): pnl={:+.1}bps ema={:+.1}bps", reason, pnl_bps, ema_trend_bps);
                self.tf_close_position_market(reason).await;
                if reason == "stop_loss" {
                    self.tf_consecutive_sl += 1;
                    let multiplier = 1u64 << self.tf_consecutive_sl.min(4); // 2x, 4x, 8x, 16x cap
                    let cooldown = self.cfg.tf_cooldown_secs * multiplier;
                    log::info!(
                        "[MR] Consecutive SL #{}: cooldown {}s ({}x)",
                        self.tf_consecutive_sl, cooldown, multiplier
                    );
                } else {
                    if self.tf_consecutive_sl > 0 {
                        log::info!("[MR] Consecutive SL streak reset (was {})", self.tf_consecutive_sl);
                    }
                    self.tf_consecutive_sl = 0;
                }
                self.tf_last_stop_time = Some(Instant::now());
            }

            self.tf_report_status(mid, pnl_bps).await;
        } else {
            // 7. Check cooldown (exponential backoff on consecutive stop-losses)
            if let Some(last) = self.tf_last_stop_time {
                let multiplier = if self.tf_consecutive_sl > 0 {
                    1u64 << self.tf_consecutive_sl.min(4)
                } else {
                    1
                };
                let cooldown_secs = self.cfg.tf_cooldown_secs * multiplier;
                if last.elapsed() < Duration::from_secs(cooldown_secs) {
                    return Ok(());
                }
            }

            // 8. Regime detection with hysteresis to prevent chattering.
            //    Uses asymmetric thresholds: higher to enter trend, lower to exit back to range.
            //    macro_trend_bps = EMA10 vs EMA50 divergence (slow indicator)
            let regime_enter_threshold = 8.0; // bps: range → trend requires > 8bps
            let regime_exit_threshold = 3.0;  // bps: trend → range requires < 3bps
            let regime_mid_threshold = 5.0;   // bps: used for regime scaling baseline
            let macro_abs = macro_trend_bps.abs();
            let new_regime = match self.regime {
                "range" => if macro_abs > regime_enter_threshold { "trend" } else { "range" },
                _ => if macro_abs < regime_exit_threshold { "range" } else { "trend" },
            };
            if new_regime != self.regime {
                log::info!(
                    "[MR] Regime change: {} → {} (macro={:+.1}bps)",
                    self.regime, new_regime, macro_trend_bps
                );
                self.regime = new_regime;
            }

            // Scale entry/tp/stop based on regime
            // Range: use config values as-is (optimized for mean-reversion)
            // Trend: scale up entry threshold (harder to enter) and tp (bigger target)
            let regime_scale = if self.regime == "range" {
                1.0
            } else {
                // Linear scale: 1.0 at mid_threshold, 2.0 at 2×mid_threshold, capped at 3.0
                (macro_abs / regime_mid_threshold).max(1.0).min(3.0)
            };
            let effective_entry_bps = self.cfg.tf_entry_threshold_bps * regime_scale;
            let effective_tp_bps = self.cfg.tf_take_profit_bps * regime_scale;

            // Trend pause: skip if macro trend is extremely strong
            if self.cfg.mr_trend_pause_bps > 0.0 && macro_abs > self.cfg.mr_trend_pause_bps {
                log::debug!("[MR] Trend pause: macro={:+.1}bps > {}bps", macro_trend_bps, self.cfg.mr_trend_pause_bps);
                return Ok(());
            }

            // 9. Entry: FADE the trend (mean reversion)
            if ema_trend_bps.abs() >= effective_entry_bps {
                // Uptrend → SHORT (expect reversion down)
                // Downtrend → LONG (expect reversion up)
                let side = if ema_trend_bps > 0.0 {
                    OrderSide::Short
                } else {
                    OrderSide::Long
                };

                let equity = self.equity_cache;
                let size_usd = equity * self.cfg.order_size_pct;
                let size_tokens = size_usd / mid;
                let size_dec = self.round_size(size_tokens);

                if size_dec <= Decimal::ZERO {
                    return Ok(());
                }

                log::info!(
                    "[MR] ENTRY: ema={:+.1}bps → {:?} size={} (${:.2}) mid={:.4} [{}] entry≥{:.0}bps tp≥{:.0}bps",
                    ema_trend_bps, side, size_dec, size_usd, mid,
                    self.regime, effective_entry_bps, effective_tp_bps
                );

                match self.main_conn
                    .create_order(&self.cfg.symbol, size_dec, side.clone(), None, None, false, None)
                    .await
                {
                    Ok(_) => {
                        self.tf_direction = Some(side.clone());
                        self.tf_entry_price = Some(mid);
                        self.tf_peak_price = Some(mid);
                        self.tf_trades += 1;
                        log::info!("[MR] Entry placed: {:?} size={} @ ~{:.4}", side, size_dec, mid);

                        tokio::time::sleep(Duration::from_secs(2)).await;
                        self.sync_inventory_from_exchange().await;

                        if let Some(ep) = self.position_entry_price {
                            self.tf_entry_price = Some(ep);
                        }

                        self.tf_spot_hedge().await;
                    }
                    Err(e) => {
                        log::error!("[MR] Entry failed: {:?}", e);
                    }
                }
            } else {
                log::debug!("[MR] Flat, ema={:+.1}bps (threshold={:.0}bps) [{}] macro={:+.1}bps", ema_trend_bps, effective_entry_bps, self.regime, macro_trend_bps);
            }

            self.tf_report_status(mid, 0.0).await;
        }

        Ok(())
    }

    /// Report trend-follow status to dashboard
    async fn tf_report_status(&mut self, _mid: f64, _pnl_bps: f64) {
        if self.status_reporter.is_some() {
            let total_equity = self.equity_cache + self.hedge_equity_cache;
            let positions = self.build_position_info().await;
            let reporter = self.status_reporter.as_mut().unwrap();
            reporter.update_equity(total_equity);
            if let Err(err) = reporter.write_snapshot_if_due(&positions) {
                log::warn!("[TF-STATUS] failed to write status: {:?}", err);
            }
        }
    }

    async fn shutdown(&mut self) {
        log::info!("[MM] Cancelling all main orders...");
        self.cancel_all_main_orders().await;
        log::info!("[MM] Closing all main positions...");
        if let Err(e) = self
            .main_conn
            .close_all_positions(Some(self.cfg.symbol.clone()))
            .await
        {
            log::warn!("[MM] Failed to close main positions: {:?}", e);
        }
        // Cancel spot hedge orders on shutdown (via main_conn)
        if !self.cfg.spot_hedge_symbol.is_empty() {
            log::info!("[MM] Cancelling spot hedge orders...");
            if let Err(e) = self
                .main_conn
                .cancel_all_orders(Some(self.cfg.spot_hedge_symbol.clone()))
                .await
            {
                log::warn!("[MM] Failed to cancel spot hedge orders: {:?}", e);
            }
        }
        if let Some(ref hedge) = self.hedge_conn {
            log::info!("[MM] Cancelling hedge orders...");
            if let Err(e) = hedge
                .cancel_all_orders(Some(self.cfg.symbol.clone()))
                .await
            {
                log::warn!("[MM] Failed to cancel hedge orders: {:?}", e);
            }
            log::info!("[MM] Closing hedge positions...");
            if let Err(e) = hedge
                .close_all_positions(Some(self.cfg.symbol.clone()))
                .await
            {
                log::warn!("[MM] Failed to close hedge positions: {:?}", e);
            }
        }
        log::info!("[MM] Shutdown complete.");
    }

    // -----------------------------------------------------------------------
    // Maintenance wind-down
    // -----------------------------------------------------------------------

    async fn check_maintenance(&mut self) -> bool {
        // Check if maintenance is coming within 1 hour
        let upcoming = self.main_conn.is_upcoming_maintenance(1).await;

        if upcoming && !self.in_maintenance_wind_down {
            // Entering wind-down: cancel all orders and close all positions
            log::warn!("[MM] Maintenance detected within 1 hour — entering wind-down mode");
            self.in_maintenance_wind_down = true;

            // Cancel all main orders
            self.cancel_all_main_orders().await;

            // Close main positions
            if let Err(e) = self
                .main_conn
                .close_all_positions(Some(self.cfg.symbol.clone()))
                .await
            {
                log::warn!("[MM] Failed to close main positions for maintenance: {:?}", e);
            }

            // Cancel spot hedge orders for maintenance
            if !self.cfg.spot_hedge_symbol.is_empty() {
                if let Err(e) = self
                    .main_conn
                    .cancel_all_orders(Some(self.cfg.spot_hedge_symbol.clone()))
                    .await
                {
                    log::warn!("[MM] Failed to cancel spot hedge orders for maintenance: {:?}", e);
                }
            }

            // Close hedge positions (legacy sub-account)
            if let Some(ref hedge) = self.hedge_conn {
                if let Err(e) = hedge
                    .cancel_all_orders(Some(self.cfg.symbol.clone()))
                    .await
                {
                    log::warn!("[MM] Failed to cancel hedge orders for maintenance: {:?}", e);
                }
                if let Err(e) = hedge
                    .close_all_positions(Some(self.cfg.symbol.clone()))
                    .await
                {
                    log::warn!("[MM] Failed to close hedge positions for maintenance: {:?}", e);
                }
            }

            self.inventory = 0.0;
            self.hedge_position = 0.0;
            log::info!("[MM] Wind-down complete — waiting for maintenance to end");
        } else if !upcoming && self.in_maintenance_wind_down {
            log::info!("[MM] Maintenance window ended — resuming trading");
            self.in_maintenance_wind_down = false;
        }

        self.in_maintenance_wind_down
    }

    // -----------------------------------------------------------------------
    // Main loop step
    // -----------------------------------------------------------------------

    async fn step(&mut self) -> Result<()> {
        // 0. Check maintenance schedule
        if self.check_maintenance().await {
            log::debug!("[MM] In maintenance wind-down, skipping step");
            return Ok(());
        }

        // 1. Get order book first to check if mid has moved enough to warrant requote
        let ob = self
            .main_conn
            .get_order_book(&self.cfg.symbol, self.cfg.ob_depth)
            .await
            .context("failed to get order book")?;

        let best_bid = ob
            .bids
            .first()
            .map(|l| l.price.to_f64().unwrap_or(0.0))
            .unwrap_or(0.0);
        let best_ask = ob
            .asks
            .first()
            .map(|l| l.price.to_f64().unwrap_or(0.0))
            .unwrap_or(0.0);

        if best_bid <= 0.0 || best_ask <= 0.0 || best_bid >= best_ask {
            log::warn!("[MM] Invalid order book: bid={} ask={}", best_bid, best_ask);
            return Ok(());
        }

        let mid = (best_bid + best_ask) / 2.0;

        // 1b. Skip requote if mid hasn't moved enough (save TX rate limit)
        // Requote when mid moves > min_spread_bps/4 from last quote, or no orders are active
        let has_active_orders = !self.active_bid_ids.is_empty() || !self.active_ask_ids.is_empty();
        if has_active_orders {
            if let Some(prev_mid) = self.last_mid {
                let move_bps = ((mid - prev_mid) / prev_mid).abs() * 10_000.0;
                if move_bps < self.cfg.min_spread_bps / 4.0 {
                    // Mid barely moved — keep existing orders, just update EMA/volatility
                    self.mid_prices.push(mid);
                    if self.mid_prices.len() > self.cfg.volatility_window {
                        self.mid_prices.drain(0..self.mid_prices.len() - self.cfg.volatility_window);
                    }
                    self.update_ema(mid);
                    self.last_mid = Some(mid);
                    // Still check fills and refresh equity
                    self.check_fills_and_rebalance().await;
                    self.process_fills().await;
                    self.refresh_equity().await;
                    let total_equity = self.equity_cache + self.hedge_equity_cache;
                    if self.status_reporter.is_some() {
                        let positions = self.build_position_info().await;
                        let reporter = self.status_reporter.as_mut().unwrap();
                        reporter.update_equity(total_equity);
                        let _ = reporter.write_snapshot_if_due(&positions);
                    }
                    return Ok(());
                }
            }
        }

        // 1c. Cancel existing orders before placing new ones
        self.cancel_all_main_orders().await;

        // 2. Update mid price history for volatility
        self.mid_prices.push(mid);
        if self.mid_prices.len() > self.cfg.volatility_window {
            self.mid_prices
                .drain(0..self.mid_prices.len() - self.cfg.volatility_window);
        }

        // 2b. Update EMA-based trend detector
        self.update_ema(mid);

        // 3. Sync inventory from exchange positions
        self.sync_inventory_from_exchange().await;

        // 4. Process fills (check what got filled since last step)
        self.process_fills().await;

        // 5. Fetch equity for position sizing
        self.refresh_equity().await;
        let equity = self.equity_cache;

        // 6. Compute dynamic order size and inventory limits from equity
        let leverage = self.cfg.max_leverage as f64;
        let order_size_tokens = equity * self.cfg.order_size_pct * leverage / mid;
        let max_inventory_tokens = equity * self.cfg.max_inventory_pct * leverage / mid;
        self.cached_max_inv = max_inventory_tokens;

        // 7. Compute dynamic spread based on volatility
        let mut effective_spread_bps = self.compute_effective_spread();

        // 7a. Post-fill spread boost: widen spread right after a fill to avoid adverse selection
        let post_fill_mult = if let Some(fill_time) = self.last_fill_time {
            let elapsed = fill_time.elapsed().as_secs_f64();
            let decay_secs = self.cfg.post_fill_decay_secs as f64;
            if elapsed < decay_secs {
                let t = 1.0 - elapsed / decay_secs; // 1.0 → 0.0
                let mult = 1.0 + (self.cfg.post_fill_spread_mult - 1.0) * t;
                log::debug!("[MM] Post-fill spread boost: {:.2}x (elapsed={:.0}s)", mult, elapsed);
                mult
            } else {
                1.0
            }
        } else {
            1.0
        };
        effective_spread_bps *= post_fill_mult;

        // 7b. Inventory-based spread widening: asymmetric — only widen on position-increasing side
        let (inv_spread_bid_mult, inv_spread_ask_mult) =
            if self.cfg.inventory_spread_mult > 0.0 && max_inventory_tokens > 0.0 {
                let inv_ratio = (self.inventory.abs() / max_inventory_tokens).min(1.0);
                let widen = 1.0 + inv_ratio * self.cfg.inventory_spread_mult;
                if inv_ratio > 0.1 {
                    if self.inventory > 0.0 {
                        // Long → widen BID (buying increases risk), keep ASK tight (selling reduces)
                        log::debug!(
                            "[MM] Inventory spread: long inv_ratio={:.2} BID mult={:.2}x ASK mult=1.00x",
                            inv_ratio, widen
                        );
                        (widen, 1.0)
                    } else {
                        // Short → widen ASK (selling increases risk), keep BID tight (buying reduces)
                        log::debug!(
                            "[MM] Inventory spread: short inv_ratio={:.2} BID mult=1.00x ASK mult={:.2}x",
                            inv_ratio, widen
                        );
                        (1.0, widen)
                    }
                } else {
                    (1.0, 1.0)
                }
            } else {
                (1.0, 1.0)
            };

        // Clamp spread after all adjustments
        effective_spread_bps = effective_spread_bps.clamp(self.cfg.min_spread_bps, self.cfg.max_spread_bps);

        // 7c. EMA-based trend detection: pause BOTH sides when trend is strong
        let ema_trend_bps = self.compute_ema_trend_bps();
        let trend_bps = self.compute_trend_bps(); // keep legacy for logging
        let mut trend_pause_bid = false;
        let mut trend_pause_ask = false;

        if ema_trend_bps.abs() > self.cfg.trend_strength_threshold {
            // Strong trend detected — pause new-position side,
            // BUT always allow the position-reducing (unwind) side.
            //
            // Unwind side depends on INVENTORY, not trend direction:
            //   long  (inv>0) → unwind via ASK (sell)
            //   short (inv<0) → unwind via BID (buy)
            //   flat  (inv=0) → nothing to unwind, pause both
            if self.inventory > 0.0 {
                // Long position: always allow ASK to unwind, pause BID
                trend_pause_bid = true;
            } else if self.inventory < 0.0 {
                // Short position: always allow BID to unwind, pause ASK
                trend_pause_ask = true;
            } else {
                // Flat: pause both to avoid opening new positions in trend
                trend_pause_bid = true;
                trend_pause_ask = true;
            }
            let direction = if ema_trend_bps > 0.0 { "UP" } else { "DOWN" };
            let unwind_side = if self.inventory > 0.0 { "ASK(unwind)" }
                else if self.inventory < 0.0 { "BID(unwind)" }
                else { "none" };
            log::info!(
                "[MM] TREND {} detected: EMA {:.1}bps > {:.1}bps, legacy={:.1}bps — allowing {} inv={:.6}",
                direction, ema_trend_bps.abs(), self.cfg.trend_strength_threshold, trend_bps,
                unwind_side, self.inventory
            );
        } else if trend_bps.abs() > self.cfg.trend_threshold_bps {
            // Moderate trend — pause position-increasing side only,
            // always allow position-reducing (unwind) side.
            // Same principle as strong trend: base on inventory, not trend direction.
            if self.inventory > 0.0 {
                // Long: pause BID (don't add), allow ASK (unwind)
                trend_pause_bid = true;
                log::info!("[MM] Moderate trend {:.1}bps: pausing BID (long inv={:.6})",
                    trend_bps, self.inventory);
            } else if self.inventory < 0.0 {
                // Short: pause ASK (don't add), allow BID (unwind)
                trend_pause_ask = true;
                log::info!("[MM] Moderate trend {:.1}bps: pausing ASK (short inv={:.6})",
                    trend_bps, self.inventory);
            } else {
                // Flat: pause the side that follows trend to avoid opening into momentum
                if trend_bps > 0.0 {
                    trend_pause_bid = true;
                } else {
                    trend_pause_ask = true;
                }
                log::info!("[MM] Moderate trend {:.1}bps: pausing momentum side (flat)",
                    trend_bps);
            }
        }

        // 8. Compute skew based on inventory
        let inv_skew_bps = self.compute_skew_bps(max_inventory_tokens);

        // 8a. OB imbalance shift: if bids thicker, price likely to rise → raise bid (more aggressive buy)
        let ob_shift_bps = if self.cfg.ob_imbalance_factor != 0.0 {
            let bid_depth: f64 = ob
                .bids
                .iter()
                .map(|l| l.size.to_f64().unwrap_or(0.0))
                .sum();
            let ask_depth: f64 = ob
                .asks
                .iter()
                .map(|l| l.size.to_f64().unwrap_or(0.0))
                .sum();
            let total = bid_depth + ask_depth;
            if total > 0.0 {
                // imbalance: +1 = all bids, -1 = all asks
                let imbalance = (bid_depth - ask_depth) / total;
                // Positive imbalance (more bids) → negative shift → pull ask closer (sell into strength)
                let shift = -imbalance * self.cfg.ob_imbalance_factor * effective_spread_bps * 0.5;
                log::debug!(
                    "[MM] OB imbalance: bid_depth={:.4} ask_depth={:.4} imbalance={:.2} shift_bps={:.2}",
                    bid_depth, ask_depth, imbalance, shift
                );
                shift
            } else {
                0.0
            }
        } else {
            0.0
        };

        // 8b. Funding rate bias: FR>0 means longs pay shorts → favor short (reduce bid, widen ask)
        let fr_skew_bps = if self.cfg.funding_rate_factor != 0.0 && self.cached_funding_rate != 0.0
        {
            // FR is typically small (e.g., 0.0001 = 1bps per period)
            // Convert to bps and scale by factor
            let fr_bps = self.cached_funding_rate * 10_000.0 * self.cfg.funding_rate_factor;
            log::debug!(
                "[MM] FR bias: rate={:.6} fr_skew_bps={:.2}",
                self.cached_funding_rate,
                fr_bps
            );
            fr_bps
        } else {
            0.0
        };

        let skew_bps = inv_skew_bps + ob_shift_bps + fr_skew_bps;

        let half_spread_bps = effective_spread_bps / 2.0;
        // Clamp offsets to >= 0 to prevent crossing mid (placing bid above mid or ask below mid)
        let bid_offset_bps = (half_spread_bps * inv_spread_bid_mult + skew_bps).max(0.0);
        let ask_offset_bps = (half_spread_bps * inv_spread_ask_mult - skew_bps).max(0.0);

        // 9. Determine if we should quote on each side
        let hard_limit = max_inventory_tokens * self.cfg.inventory_hard_limit_mult;
        let quote_bid = self.inventory < hard_limit && !trend_pause_bid;
        let quote_ask = self.inventory > -hard_limit && !trend_pause_ask;

        // 8a. Force-close if inventory exceeds hard_limit * force_close_mult
        let force_close_limit = hard_limit * self.cfg.force_close_mult;
        let cooldown_ok = self
            .last_force_close
            .map(|t| t.elapsed().as_secs() >= self.cfg.force_close_cooldown_secs)
            .unwrap_or(true);
        if self.inventory.abs() > force_close_limit && cooldown_ok {
            let excess = self.inventory.abs() - hard_limit;
            let side = if self.inventory > 0.0 {
                OrderSide::Short
            } else {
                OrderSide::Long
            };
            let size = self.round_size(excess);
            if size > Decimal::ZERO {
                log::warn!(
                    "[MM] FORCE CLOSE: inv={:.6} exceeds force_limit={:.6}, closing excess={:.6} side={:?}",
                    self.inventory, force_close_limit, excess, side
                );
                match self
                    .main_conn
                    .create_order(&self.cfg.symbol, size, side, None, None, false, None)
                    .await
                {
                    Ok(_) => {
                        log::warn!("[MM] FORCE CLOSE placed: {:?} size={}", side, size);
                        self.last_force_close = Some(Instant::now());
                        // Re-sync inventory after force close
                        sleep(Duration::from_secs(2)).await;
                        self.sync_inventory_from_exchange().await;
                    }
                    Err(e) => log::error!("[MM] FORCE CLOSE failed: {:?}", e),
                }
            }
        }

        // 8b. Place new orders (cancel already done at step start)
        let mut new_bid_ids = Vec::new();
        let mut new_ask_ids = Vec::new();

        // Compute aggressive unwind prices if we have a position and feature is enabled
        let aggressive_unwind_ask = if self.inventory > 0.0
            && self.cfg.aggressive_unwind_bps > 0.0
        {
            self.position_entry_price.map(|ep| ep * (1.0 + self.cfg.aggressive_unwind_bps / 10_000.0))
        } else {
            None
        };
        let aggressive_unwind_bid = if self.inventory < 0.0
            && self.cfg.aggressive_unwind_bps > 0.0
        {
            self.position_entry_price.map(|ep| ep * (1.0 - self.cfg.aggressive_unwind_bps / 10_000.0))
        } else {
            None
        };

        // Place bid orders (buy side)
        if quote_bid {
            for level in 0..self.cfg.order_levels {
                let level_extra_bps = level as f64 * self.cfg.level_spacing_bps;
                let total_offset = bid_offset_bps + level_extra_bps;
                let mut bid_price = mid * (1.0 - total_offset / 10_000.0);

                // Aggressive unwind: if short, raise bid toward entry to close faster
                if let Some(unwind_price) = aggressive_unwind_bid {
                    if unwind_price > bid_price {
                        log::info!(
                            "[MM] BID aggressive unwind: {:.2} → {:.2} (entry={:.2})",
                            bid_price, unwind_price, self.position_entry_price.unwrap_or(0.0)
                        );
                        bid_price = unwind_price;
                    }
                }

                if bid_price <= 0.0 {
                    continue;
                }

                let bid_price_dec = self.round_price(bid_price);
                let size_dec = self.round_size(order_size_tokens);

                if size_dec <= Decimal::ZERO || bid_price_dec <= Decimal::ZERO {
                    continue;
                }

                // POST_ONLY (spread=-2) ensures maker-only execution, no taker fills
                match self
                    .main_conn
                    .create_order(
                        &self.cfg.symbol,
                        size_dec,
                        OrderSide::Long,
                        Some(bid_price_dec),
                        Some(-2),
                        false,
                        Some(self.cfg.stale_order_secs),
                    )
                    .await
                {
                    Ok(resp) => {
                        log::info!(
                            "[MM] BID L{} price={} size={}",
                            level,
                            bid_price_dec,
                            size_dec
                        );
                        new_bid_ids.push(resp.order_id);
                    }
                    Err(e) => {
                        log::warn!("[MM] BID L{} rejected (POST_ONLY): {:?}", level, e);
                    }
                }
            }
        } else {
            log::info!(
                "[MM] Skipping bid quotes: inventory {:.6} >= hard limit {:.6}",
                self.inventory,
                hard_limit
            );
        }

        // Place ask orders (sell side)
        if quote_ask {
            for level in 0..self.cfg.order_levels {
                let level_extra_bps = level as f64 * self.cfg.level_spacing_bps;
                let total_offset = ask_offset_bps + level_extra_bps;
                let mut ask_price = mid * (1.0 + total_offset / 10_000.0);

                // Aggressive unwind: if long, lower ask toward entry to close faster
                if let Some(unwind_price) = aggressive_unwind_ask {
                    if unwind_price < ask_price {
                        log::info!(
                            "[MM] ASK aggressive unwind: {:.2} → {:.2} (entry={:.2})",
                            ask_price, unwind_price, self.position_entry_price.unwrap_or(0.0)
                        );
                        ask_price = unwind_price;
                    }
                }

                if ask_price <= 0.0 {
                    continue;
                }

                let ask_price_dec = self.round_price(ask_price);
                let size_dec = self.round_size(order_size_tokens);

                if size_dec <= Decimal::ZERO || ask_price_dec <= Decimal::ZERO {
                    continue;
                }

                match self
                    .main_conn
                    .create_order(
                        &self.cfg.symbol,
                        size_dec,
                        OrderSide::Short,
                        Some(ask_price_dec),
                        Some(-2),
                        false,
                        Some(self.cfg.stale_order_secs),
                    )
                    .await
                {
                    Ok(resp) => {
                        log::info!(
                            "[MM] ASK L{} price={} size={}",
                            level,
                            ask_price_dec,
                            size_dec
                        );
                        new_ask_ids.push(resp.order_id);
                    }
                    Err(e) => {
                        log::warn!("[MM] ASK L{} rejected (POST_ONLY): {:?}", level, e);
                    }
                }
            }
        } else {
            log::info!(
                "[MM] Skipping ask quotes: inventory {:.6} <= -hard limit {:.6}",
                self.inventory,
                -hard_limit
            );
        }

        self.active_bid_ids = new_bid_ids;
        self.active_ask_ids = new_ask_ids;
        self.last_order_time = Some(Instant::now());

        // 9. Manage hedge position (legacy sub-account only)
        // Spot hedge is handled exclusively by check_fills_and_rebalance() to avoid double-firing.
        if self.cfg.hedge_enabled && self.hedge_conn.is_some() {
            self.manage_hedge(max_inventory_tokens, order_size_tokens).await;
        }

        // 10. Log status
        let total_equity = equity + self.hedge_equity_cache;
        log::info!(
            "[MM] mid={:.2} equity={:.2} hedge_equity={:.2} total_equity={:.2} inv={:.6} hedge={:.6} net_exposure={:.6} spread_bps={:.1} skew_bps={:.1} pnl_realized={:.4} trades={} bids_filled={} asks_filled={} entry={:.2}",
            mid,
            equity,
            self.hedge_equity_cache,
            total_equity,
            self.inventory,
            self.hedge_position,
            self.inventory + self.hedge_position,
            effective_spread_bps,
            skew_bps,
            self.realized_pnl,
            self.total_trades,
            self.total_bid_fills,
            self.total_ask_fills,
            self.position_entry_price.unwrap_or(0.0),
        );

        // 11. Write dashboard status
        if self.status_reporter.is_some() {
            let positions = self.build_position_info().await;
            let reporter = self.status_reporter.as_mut().unwrap();
            reporter.update_equity(total_equity);
            if let Err(err) = reporter.write_snapshot_if_due(&positions) {
                log::warn!("[STATUS] failed to write status: {:?}", err);
            }
        }

        self.last_mid = Some(mid);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Inventory sync from exchange
    // -----------------------------------------------------------------------

    async fn refresh_equity(&mut self) {
        // Refresh equity at most every 5 minutes to avoid excessive API calls
        let should_refresh = self
            .last_equity_fetch
            .map(|t| t.elapsed() > Duration::from_secs(300))
            .unwrap_or(true);
        if !should_refresh {
            return;
        }
        match self.main_conn.get_balance(None).await {
            Ok(bal) => {
                let equity = bal.equity.to_f64().unwrap_or(0.0);
                if equity > 0.0 {
                    self.equity_cache = equity;
                    self.last_equity_fetch = Some(Instant::now());
                    log::debug!("[MM] Main equity refreshed: {:.2}", equity);
                }
            }
            Err(e) => {
                log::warn!("[MM] Failed to get balance, using cached equity {:.2}: {:?}", self.equity_cache, e);
            }
        }
        // Also refresh hedge/spot equity
        if !self.cfg.spot_hedge_symbol.is_empty() {
            // Spot hedge: compute spot equity from LIT token balance × mid price
            let base_token = self.cfg.spot_hedge_symbol.split('/').next().unwrap_or("");
            let mid = self.last_mid.unwrap_or(0.0);
            if !base_token.is_empty() && mid > 0.0 {
                match self.main_conn.get_combined_balance().await {
                    Ok(combined) => {
                        let mut spot_equity = 0.0;
                        for asset in &combined.spot_assets {
                            let bal = asset.balance.to_f64().unwrap_or(0.0);
                            if asset.symbol == "USDC" || asset.symbol == "USDT" {
                                spot_equity += bal;
                            } else if asset.symbol == base_token {
                                spot_equity += bal * mid;
                            }
                            // skip unknown tokens
                        }
                        self.hedge_equity_cache = spot_equity;
                        log::debug!(
                            "[MM] Spot equity: ${:.2} (assets: {:?})",
                            spot_equity,
                            combined.spot_assets.iter()
                                .map(|a| format!("{}={:.2}", a.symbol, a.balance.to_f64().unwrap_or(0.0)))
                                .collect::<Vec<_>>()
                        );
                    }
                    Err(e) => {
                        log::warn!("[MM] Failed to get combined balance: {:?}", e);
                    }
                }
            }
        } else if let Some(ref hedge) = self.hedge_conn {
            match hedge.get_balance(None).await {
                Ok(bal) => {
                    let equity = bal.equity.to_f64().unwrap_or(0.0);
                    if equity > 0.0 {
                        self.hedge_equity_cache = equity;
                        log::debug!("[MM] Hedge equity refreshed: {:.2}", equity);
                    }
                }
                Err(e) => {
                    log::warn!("[MM] Failed to get hedge balance: {:?}", e);
                }
            }
        }
        // Refresh funding rate
        if self.cfg.funding_rate_factor != 0.0 {
            match self.main_conn.get_ticker(&self.cfg.symbol, None).await {
                Ok(ticker) => {
                    self.cached_funding_rate = ticker
                        .funding_rate
                        .and_then(|r| r.to_f64())
                        .unwrap_or(0.0);
                    log::debug!("[MM] Funding rate refreshed: {:.6}", self.cached_funding_rate);
                }
                Err(e) => {
                    log::warn!("[MM] Failed to get funding rate: {:?}", e);
                }
            }
        }
    }

    async fn sync_inventory_from_exchange(&mut self) {
        match self.main_conn.get_positions().await {
            Ok(positions) => {
                let pos = positions
                    .iter()
                    .find(|p| p.symbol == self.cfg.symbol && p.size > Decimal::ZERO);
                if let Some(p) = pos {
                    let size_f = p.size.to_f64().unwrap_or(0.0);
                    self.inventory = if p.sign > 0 { size_f } else { -size_f };
                    self.position_entry_price = p.entry_price.and_then(|ep| ep.to_f64());
                } else {
                    self.inventory = 0.0;
                    self.position_entry_price = None;
                }
            }
            Err(e) => {
                log::warn!("[MM] Failed to get positions: {:?}", e);
            }
        }

        // Sync hedge position
        if !self.cfg.spot_hedge_symbol.is_empty() {
            // Spot hedge: derive hedge_position from actual LIT balance on exchange
            // hedge_position = current_LIT_balance - initial_LIT_balance
            // positive = holding more LIT (hedged short perp), negative = sold LIT (hedged long perp)
            let base_token = self.cfg.spot_hedge_symbol.split('/').next().unwrap_or("").to_string();
            if !base_token.is_empty() {
                match self.main_conn.get_combined_balance().await {
                    Ok(combined) => {
                        let current_lit = combined.spot_assets.iter()
                            .find(|a| a.symbol == base_token)
                            .map(|a| a.balance.to_f64().unwrap_or(0.0))
                            .unwrap_or(0.0);
                        // Record initial balance on first sync
                        if self.initial_spot_lit_balance.is_none() {
                            self.initial_spot_lit_balance = Some(current_lit);
                            log::info!("[SPOT-HEDGE] Initial {} balance: {:.2}", base_token, current_lit);
                        }
                        let initial = self.initial_spot_lit_balance.unwrap_or(0.0);
                        let old_hp = self.hedge_position;
                        self.hedge_position = current_lit - initial;
                        if (old_hp - self.hedge_position).abs() > 0.01 {
                            log::info!(
                                "[SPOT-HEDGE] Synced: {} balance={:.2} initial={:.2} hedge_pos={:.2} (was {:.2})",
                                base_token, current_lit, initial, self.hedge_position, old_hp
                            );
                        }
                    }
                    Err(e) => {
                        log::warn!("[SPOT-HEDGE] Failed to get combined balance for sync: {:?}", e);
                    }
                }
            }
        } else if let Some(ref hedge) = self.hedge_conn {
            // Legacy sub-account hedge
            match hedge.get_positions().await {
                Ok(positions) => {
                    let pos = positions
                        .iter()
                        .find(|p| p.symbol == self.cfg.symbol && p.size > Decimal::ZERO);
                    if let Some(p) = pos {
                        let size_f = p.size.to_f64().unwrap_or(0.0);
                        self.hedge_position = if p.sign > 0 { size_f } else { -size_f };
                    } else {
                        self.hedge_position = 0.0;
                    }
                }
                Err(e) => {
                    log::warn!("[MM] Failed to get hedge positions: {:?}", e);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Fill processing
    // -----------------------------------------------------------------------

    async fn process_fills(&mut self) {
        match self.main_conn.get_filled_orders(&self.cfg.symbol).await {
            Ok(fills) => {
                for fill in &fills.orders {
                    let size = fill.filled_size.and_then(|s| s.to_f64()).unwrap_or(0.0);
                    let fill_value = fill.filled_value.and_then(|v| v.to_f64()).unwrap_or(0.0);

                    if size <= 0.0 {
                        continue;
                    }

                    // filled_value = size * price, so derive fill_price
                    let fill_price = if fill_value > 0.0 { fill_value / size } else { 0.0 };

                    // Realized PnL from spread capture
                    if let Some(mid) = self.last_mid {
                        if fill_price > 0.0 {
                            let pnl = match fill.filled_side {
                                Some(OrderSide::Long) => (mid - fill_price) * size,
                                _ => (fill_price - mid) * size,
                            };
                            self.realized_pnl += pnl;
                        }
                    }

                    self.total_trades += 1;
                    self.last_fill_time = Some(Instant::now());
                    match fill.filled_side {
                        Some(OrderSide::Long) => {
                            self.total_bid_fills += 1;
                            log::info!("[FILL] LONG size={:.6} price={:.6}", size, fill_price);
                        }
                        _ => {
                            self.total_ask_fills += 1;
                            log::info!("[FILL] SHORT size={:.6} price={:.6}", size, fill_price);
                        }
                    }

                    // Clear processed fill
                    let _ = self
                        .main_conn
                        .clear_filled_order(&self.cfg.symbol, &fill.trade_id)
                        .await;
                }
            }
            Err(e) => {
                log::warn!("[MM] Failed to get filled orders: {:?}", e);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Volatility-adjusted spread
    // -----------------------------------------------------------------------

    fn compute_effective_spread(&self) -> f64 {
        if self.mid_prices.len() < 2 {
            return self.cfg.spread_bps;
        }

        // Compute realized volatility as stdev of log returns in bps
        let n = self.mid_prices.len();
        let mut log_returns = Vec::with_capacity(n - 1);
        for i in 1..n {
            if self.mid_prices[i - 1] > 0.0 {
                log_returns.push((self.mid_prices[i] / self.mid_prices[i - 1]).ln());
            }
        }
        if log_returns.is_empty() {
            return self.cfg.spread_bps;
        }

        let mean = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
        let var =
            log_returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / log_returns.len() as f64;
        let vol_bps = var.sqrt() * 10_000.0;

        let dynamic_spread = self.cfg.spread_bps + vol_bps * self.cfg.volatility_spread_mult;

        dynamic_spread.clamp(self.cfg.min_spread_bps, self.cfg.max_spread_bps)
    }

    // -----------------------------------------------------------------------
    // Inventory skew
    // -----------------------------------------------------------------------

    fn compute_skew_bps(&self, max_inventory_tokens: f64) -> f64 {
        if max_inventory_tokens <= 0.0 {
            return 0.0;
        }
        // Normalized inventory: -1 to +1
        let inv_ratio = (self.inventory / max_inventory_tokens).clamp(-1.0, 1.0);
        // Skew in bps: positive when long (pushes bid down, ask down => easier to sell)
        inv_ratio * self.cfg.skew_factor * self.cfg.spread_bps
    }

    /// Short-term trend in bps: positive = price rising, negative = falling.
    /// Uses last N mid prices (trend_window).
    fn compute_trend_bps(&self) -> f64 {
        let w = self.cfg.trend_window;
        if self.mid_prices.len() < w + 1 {
            return 0.0;
        }
        let recent = &self.mid_prices[self.mid_prices.len() - w - 1..];
        let first = recent[0];
        let last = recent[w];
        if first <= 0.0 {
            return 0.0;
        }
        (last - first) / first * 10_000.0
    }

    /// Update EMA short and long with a new mid price.
    fn update_ema(&mut self, mid: f64) {
        let alpha_short = 2.0 / (self.cfg.ema_short_periods as f64 + 1.0);
        let alpha_long = 2.0 / (self.cfg.ema_long_periods as f64 + 1.0);

        self.ema_short = Some(match self.ema_short {
            Some(prev) => alpha_short * mid + (1.0 - alpha_short) * prev,
            None => mid,
        });
        self.ema_long = Some(match self.ema_long {
            Some(prev) => alpha_long * mid + (1.0 - alpha_long) * prev,
            None => mid,
        });
    }

    /// Compute EMA-based trend strength in bps.
    /// Positive = short EMA above long EMA (uptrend), negative = downtrend.
    fn compute_ema_trend_bps(&self) -> f64 {
        match (self.ema_short, self.ema_long) {
            (Some(short), Some(long)) if long > 0.0 => {
                (short - long) / long * 10_000.0
            }
            _ => 0.0,
        }
    }

    /// Update macro EMA (EMA10/EMA50) for trend pause detection.
    fn update_macro_ema(&mut self, mid: f64) {
        let alpha_s = 2.0 / (10.0 + 1.0);
        let alpha_l = 2.0 / (50.0 + 1.0);
        self.macro_ema_short = Some(match self.macro_ema_short {
            Some(prev) => alpha_s * mid + (1.0 - alpha_s) * prev,
            None => mid,
        });
        self.macro_ema_long = Some(match self.macro_ema_long {
            Some(prev) => alpha_l * mid + (1.0 - alpha_l) * prev,
            None => mid,
        });
    }

    /// Compute macro trend strength in bps (EMA10 vs EMA50).
    fn compute_macro_trend_bps(&self) -> f64 {
        match (self.macro_ema_short, self.macro_ema_long) {
            (Some(s), Some(l)) if l > 0.0 => (s - l) / l * 10_000.0,
            _ => 0.0,
        }
    }

    // -----------------------------------------------------------------------
    // Hedge management
    // -----------------------------------------------------------------------

    /// Hedge management on each step cycle.
    /// Spot hedge: keep delta-neutral by hedging full perp inventory on spot.
    /// Sub-account hedge: threshold-based hedging (legacy).
    /// Legacy sub-account hedge only. Spot hedge is handled by check_fills_and_rebalance().
    async fn manage_hedge(&mut self, _max_inventory_tokens: f64, _order_size_tokens: f64) {
        let hedge_conn = match &self.hedge_conn {
            Some(c) => c.clone(),
            None => return,
        };

        let mid = self.last_mid.unwrap_or(0.0);
        if mid <= 0.0 {
            return;
        }

        {
            // Legacy sub-account hedge with threshold
            let net_exposure = self.inventory + self.hedge_position;
            let exposure_usd = net_exposure.abs() * mid;
            let equity = self.equity_cache + self.hedge_equity_cache;
            let hedge_threshold_usd = equity * self.cfg.hedge_threshold_ratio;
            let close_threshold_usd = equity * self.cfg.hedge_close_threshold_ratio;

            if exposure_usd > hedge_threshold_usd {
                let excess_usd = exposure_usd - hedge_threshold_usd;
                let excess_tokens = excess_usd / mid;
                let side = if net_exposure > 0.0 {
                    OrderSide::Short
                } else {
                    OrderSide::Long
                };
                let size = self.round_size(excess_tokens);
                if size <= Decimal::ZERO {
                    return;
                }
                log::info!(
                    "[HEDGE-STEP] exposure=${:.2} threshold=${:.2} excess=${:.2} side={:?} size={}",
                    exposure_usd, hedge_threshold_usd, excess_usd, side, size
                );
                match hedge_conn
                    .create_order(&self.cfg.symbol, size, side, None, None, false, None)
                    .await
                {
                    Ok(_) => log::info!("[HEDGE-STEP] Placed: {:?} size={}", side, size),
                    Err(e) => log::error!("[HEDGE-STEP] Failed: {:?}", e),
                }
            } else if exposure_usd <= close_threshold_usd && self.hedge_position.abs() > 0.0 {
                let inv_usd = self.inventory.abs() * mid;
                if inv_usd <= close_threshold_usd {
                    let side = if self.hedge_position > 0.0 {
                        OrderSide::Short
                    } else {
                        OrderSide::Long
                    };
                    let size = self.round_size(self.hedge_position.abs());
                    if size <= Decimal::ZERO {
                        return;
                    }
                    log::info!(
                        "[HEDGE-STEP] Closing hedge: inv_usd=${:.2} close_threshold=${:.2} side={:?} size={}",
                        inv_usd, close_threshold_usd, side, size
                    );
                    match hedge_conn
                        .create_order(&self.cfg.symbol, size, side, None, None, false, None)
                        .await
                    {
                        Ok(_) => log::info!("[HEDGE-STEP] Hedge closed: {:?} size={}", side, size),
                        Err(e) => log::error!("[HEDGE-STEP] Failed to close hedge: {:?}", e),
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Real-time fill detection → instant rebalance & hedge
    // -----------------------------------------------------------------------

    /// Called every 2s to detect new fills. Returns true if fills were detected,
    /// triggering an immediate rebalance cycle in the main loop.
    async fn check_fills_and_rebalance(&mut self) -> bool {
        if self.in_maintenance_wind_down {
            return false;
        }

        // Check for new fills via WebSocket cache
        let fills = match self.main_conn.get_filled_orders(&self.cfg.symbol).await {
            Ok(f) => f,
            Err(_) => return false,
        };

        if fills.orders.is_empty() {
            return false;
        }

        // Process fills and update inventory
        let mut fill_delta = 0.0_f64; // signed: positive = bought, negative = sold
        for fill in &fills.orders {
            let size = fill.filled_size.and_then(|s| s.to_f64()).unwrap_or(0.0);
            let fill_value = fill.filled_value.and_then(|v| v.to_f64()).unwrap_or(0.0);
            if size <= 0.0 {
                continue;
            }
            // filled_value = size * price, so derive fill_price
            let fill_price = if fill_value > 0.0 { fill_value / size } else { 0.0 };
            // Realized PnL: spread captured = (fill_price - mid) * size for sells,
            //                                  (mid - fill_price) * size for buys
            // This measures profit from providing liquidity above/below mid.
            if let Some(mid) = self.last_mid {
                if fill_price > 0.0 {
                    let pnl = match fill.filled_side {
                        Some(OrderSide::Long) => (mid - fill_price) * size,
                        _ => (fill_price - mid) * size,
                    };
                    self.realized_pnl += pnl;
                }
            }
            match fill.filled_side {
                Some(OrderSide::Long) => {
                    fill_delta += size;
                    self.total_bid_fills += 1;
                    log::info!("[FILL-RT] LONG size={:.6} price={:.6}", size, fill_price);
                }
                _ => {
                    fill_delta -= size;
                    self.total_ask_fills += 1;
                    log::info!("[FILL-RT] SHORT size={:.6} price={:.6}", size, fill_price);
                }
            }
            self.total_trades += 1;
            let _ = self
                .main_conn
                .clear_filled_order(&self.cfg.symbol, &fill.trade_id)
                .await;
        }

        if fill_delta.abs() < 1e-10 {
            return false;
        }

        // Update inventory from exchange position for accuracy
        self.sync_inventory_from_exchange().await;

        log::info!(
            "[FILL-RT] Fill detected (delta={:+.6}), triggering immediate rebalance",
            fill_delta
        );

        // Spot hedge on fill detection — hedge only when net_exposure exceeds threshold
        let use_spot_hedge = !self.cfg.spot_hedge_symbol.is_empty();
        if use_spot_hedge {
            // Check net exposure AFTER inventory sync (inventory updated above)
            let net_exposure = self.inventory + self.hedge_position;
            let mid = self.last_mid.unwrap_or(0.0);
            // Hedge when net exposure exceeds 10% of max_inventory
            let hedge_threshold = self.cached_max_inv * 0.1;
            if net_exposure.abs() > hedge_threshold && mid > 0.0 {
                let hedge_side = if net_exposure > 0.0 {
                    OrderSide::Short // net long → sell spot
                } else {
                    OrderSide::Long // net short → buy spot
                };
                let hedge_size = self.round_size(net_exposure.abs());
                if hedge_size > Decimal::ZERO {
                    log::info!(
                        "[SPOT-HEDGE-RT] net_exp={:.2} threshold={:.2} → spot {:?} {} size={}",
                        net_exposure, hedge_threshold, hedge_side, self.cfg.spot_hedge_symbol, hedge_size
                    );
                    // Market order for guaranteed instant fill
                    match self.main_conn
                        .create_order(&self.cfg.spot_hedge_symbol, hedge_size, hedge_side, None, None, false, None)
                        .await
                    {
                        Ok(_) => {
                            log::info!("[SPOT-HEDGE-RT] Placed: {:?} size={}", hedge_side, hedge_size);
                            // Re-sync hedge_position from actual spot balance
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            self.sync_inventory_from_exchange().await;
                        }
                        Err(e) => {
                            log::error!("[SPOT-HEDGE-RT] Failed: {:?}", e);
                        }
                    }
                }
            } else if net_exposure.abs() > 1.0 {
                log::debug!(
                    "[SPOT-HEDGE-RT] net_exp={:.2} below threshold={:.2}, deferring hedge",
                    net_exposure, hedge_threshold
                );
            }
        } else if self.cfg.hedge_enabled {
            if let Some(ref hedge_conn) = self.hedge_conn {
                // Legacy sub-account hedge with threshold
                let net_exposure = self.inventory + self.hedge_position;
                let mid = self.last_mid.unwrap_or(0.0);
                if mid > 0.0 {
                    let exposure_usd = net_exposure.abs() * mid;
                    let equity = self.equity_cache + self.hedge_equity_cache;
                    let hedge_threshold_usd = equity * self.cfg.hedge_threshold_ratio;

                    if exposure_usd > hedge_threshold_usd {
                        let excess_usd = exposure_usd - hedge_threshold_usd;
                        let excess_tokens = excess_usd / mid;
                        let hedge_side = if net_exposure > 0.0 {
                            OrderSide::Short
                        } else {
                            OrderSide::Long
                        };
                        let hedge_size = self.round_size(excess_tokens);
                        if hedge_size > Decimal::ZERO {
                            log::info!(
                                "[HEDGE-RT] Instant hedge: exposure=${:.2} threshold=${:.2} excess=${:.2} side={:?} size={}",
                                exposure_usd, hedge_threshold_usd, excess_usd, hedge_side, hedge_size
                            );
                            match hedge_conn
                                .create_order(&self.cfg.symbol, hedge_size, hedge_side, None, None, false, None)
                                .await
                            {
                                Ok(_) => {
                                    log::info!("[HEDGE-RT] Instant hedge placed: {:?} size={}", hedge_side, hedge_size);
                                    self.sync_inventory_from_exchange().await;
                                }
                                Err(e) => {
                                    log::error!("[HEDGE-RT] Failed to place instant hedge: {:?}", e);
                                }
                            }
                        }
                    }
                }
            }
        }

        true // fills detected → trigger rebalance
    }

    // -----------------------------------------------------------------------
    // Cancel helpers
    // -----------------------------------------------------------------------

    async fn cancel_all_main_orders(&mut self) {
        if let Err(e) = self
            .main_conn
            .cancel_all_orders(Some(self.cfg.symbol.clone()))
            .await
        {
            log::warn!("[MM] Failed to cancel all orders: {:?}", e);
        }
        self.active_bid_ids.clear();
        self.active_ask_ids.clear();
    }

    // -----------------------------------------------------------------------
    // Status reporting
    // -----------------------------------------------------------------------

    async fn build_position_info(&self) -> Vec<PositionInfo> {
        let mut result = Vec::new();
        if let Ok(positions) = self.main_conn.get_positions().await {
            for p in &positions {
                if p.size > Decimal::ZERO {
                    result.push(PositionInfo {
                        symbol: p.symbol.clone(),
                        side: if p.sign > 0 {
                            "long".to_string()
                        } else {
                            "short".to_string()
                        },
                        size: p.size.to_string(),
                        entry_price: p.entry_price.map(|ep| ep.to_string()),
                    });
                }
            }
        }
        // Report spot hedge position from tracking (not from exchange)
        if !self.cfg.spot_hedge_symbol.is_empty() && self.hedge_position.abs() > 0.01 {
            result.push(PositionInfo {
                symbol: format!("{}_SPOT_HEDGE", self.cfg.spot_hedge_symbol),
                side: if self.hedge_position > 0.0 {
                    "long".to_string()
                } else {
                    "short".to_string()
                },
                size: format!("{:.2}", self.hedge_position.abs()),
                entry_price: None,
            });
        }
        // Legacy sub-account hedge positions
        if let Some(ref hedge) = self.hedge_conn {
            if let Ok(positions) = hedge.get_positions().await {
                for p in &positions {
                    if p.size > Decimal::ZERO {
                        result.push(PositionInfo {
                            symbol: format!("{}_HEDGE", p.symbol),
                            side: if p.sign > 0 {
                                "long".to_string()
                            } else {
                                "short".to_string()
                            },
                            size: p.size.to_string(),
                            entry_price: p.entry_price.map(|ep| ep.to_string()),
                        });
                    }
                }
            }
        }
        result
    }

    // -----------------------------------------------------------------------
    // Price/size rounding helpers
    // -----------------------------------------------------------------------

    fn round_price(&self, price: f64) -> Decimal {
        Decimal::from_f64(price)
            .unwrap_or(Decimal::ZERO)
            .round_dp(self.cfg.price_decimals)
    }

    fn round_size(&self, size: f64) -> Decimal {
        Decimal::from_f64(size)
            .unwrap_or(Decimal::ZERO)
            .round_dp(self.cfg.size_decimals)
    }
}
