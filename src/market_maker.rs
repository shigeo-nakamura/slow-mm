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
const DEFAULT_POST_FILL_SPREAD_MULT: f64 = 2.0;
const DEFAULT_POST_FILL_DECAY_SECS: u64 = 30;
const DEFAULT_INVENTORY_SPREAD_MULT: f64 = 1.0;

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
    post_fill_spread_mult: Option<f64>,
    post_fill_decay_secs: Option<u64>,
    inventory_spread_mult: Option<f64>,
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
    /// Spread multiplier immediately after a fill (decays over time)
    pub post_fill_spread_mult: f64,
    /// Seconds for post-fill spread boost to decay back to 1.0
    pub post_fill_decay_secs: u64,
    /// Spread widens proportionally to inventory: spread *= (1 + |inv_ratio| * this)
    pub inventory_spread_mult: f64,
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
            post_fill_spread_mult: yaml
                .post_fill_spread_mult
                .unwrap_or(DEFAULT_POST_FILL_SPREAD_MULT),
            post_fill_decay_secs: yaml
                .post_fill_decay_secs
                .unwrap_or(DEFAULT_POST_FILL_DECAY_SECS),
            inventory_spread_mult: yaml
                .inventory_spread_mult
                .unwrap_or(DEFAULT_INVENTORY_SPREAD_MULT),
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
    /// Last time a fill was detected (for post-fill spread boost)
    last_fill_time: Option<Instant>,
    /// Running stats
    total_trades: u64,
    total_bid_fills: u64,
    total_ask_fills: u64,
    /// Dashboard status reporter
    status_reporter: Option<StatusReporter>,
}

impl MmEngine {
    pub async fn new(cfg: MmConfig) -> Result<Self> {
        let tokens = vec![cfg.symbol.clone()];

        // Create main connector
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

        // Create hedge connector if enabled
        let hedge_conn = if cfg.hedge_enabled {
            match DexConnectorBox::create_lighter("HEDGE_", cfg.dry_run, &tokens).await {
                Ok(hc) => {
                    if let Err(e) = hc.start().await {
                        log::error!("[INIT] Failed to start hedge connector: {:?}", e);
                        None
                    } else {
                        if let Err(e) = hc.set_leverage(&cfg.symbol, cfg.max_leverage).await {
                            log::warn!("[INIT] Failed to set leverage on hedge: {:?}", e);
                        }
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
            last_fill_time: None,
            total_trades: 0,
            total_bid_fills: 0,
            total_ask_fills: 0,
            status_reporter,
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
        self.inventory = 0.0;
        self.hedge_position = 0.0;
        self.realized_pnl = 0.0;

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
                    self.check_fills_and_hedge().await;
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

            // Close hedge positions
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

        // 1. Get order book to determine mid price
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

        // 2. Update mid price history for volatility
        self.mid_prices.push(mid);
        if self.mid_prices.len() > self.cfg.volatility_window {
            self.mid_prices
                .drain(0..self.mid_prices.len() - self.cfg.volatility_window);
        }

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

        // 7c. Trend detection: pause position-increasing side, allow position-reducing side
        let trend_bps = self.compute_trend_bps();
        let mut trend_pause_bid = false;
        let mut trend_pause_ask = false;
        if trend_bps.abs() > self.cfg.trend_threshold_bps {
            if trend_bps > 0.0 {
                if self.inventory > 0.0 {
                    // Rising + long → pause BID (stop buying), allow ASK (reduce)
                    trend_pause_bid = true;
                    log::info!("[MM] Trend UP {:.1}bps > {:.1}bps: pausing BID (long inv={:.6}), allowing ASK to reduce",
                        trend_bps, self.cfg.trend_threshold_bps, self.inventory);
                } else {
                    // Rising + flat/short → pause ASK (avoid adverse selection buying high)
                    trend_pause_ask = true;
                    log::info!("[MM] Trend UP {:.1}bps > {:.1}bps: pausing ASK (inv={:.6})",
                        trend_bps, self.cfg.trend_threshold_bps, self.inventory);
                }
            } else {
                if self.inventory < 0.0 {
                    // Falling + short → pause ASK (stop selling), allow BID (reduce)
                    trend_pause_ask = true;
                    log::info!("[MM] Trend DOWN {:.1}bps > {:.1}bps: pausing ASK (short inv={:.6}), allowing BID to reduce",
                        trend_bps.abs(), self.cfg.trend_threshold_bps, self.inventory);
                } else {
                    // Falling + flat/long → pause BID (avoid adverse selection buying low)
                    trend_pause_bid = true;
                    log::info!("[MM] Trend DOWN {:.1}bps > {:.1}bps: pausing BID (inv={:.6})",
                        trend_bps.abs(), self.cfg.trend_threshold_bps, self.inventory);
                }
            }
        }

        // 8. Compute skew based on inventory
        let inv_skew_bps = self.compute_skew_bps(max_inventory_tokens);

        // 8a. OB imbalance shift: if bids are thicker, price likely to rise → shift ask closer
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
        let bid_offset_bps = half_spread_bps * inv_spread_bid_mult + skew_bps;
        let ask_offset_bps = half_spread_bps * inv_spread_ask_mult - skew_bps;

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

        // 8b. Cancel existing orders and place new ones
        self.cancel_all_main_orders().await;

        let mut new_bid_ids = Vec::new();
        let mut new_ask_ids = Vec::new();

        // Place bid orders (buy side)
        if quote_bid {
            for level in 0..self.cfg.order_levels {
                let level_extra_bps = level as f64 * self.cfg.level_spacing_bps;
                let total_offset = bid_offset_bps + level_extra_bps;
                let bid_price = mid * (1.0 - total_offset / 10_000.0);

                if bid_price <= 0.0 {
                    continue;
                }

                let bid_price_dec = self.round_price(bid_price);
                let size_dec = self.round_size(order_size_tokens);

                if size_dec <= Decimal::ZERO || bid_price_dec <= Decimal::ZERO {
                    continue;
                }

                match self
                    .main_conn
                    .create_order(
                        &self.cfg.symbol,
                        size_dec,
                        OrderSide::Long,
                        Some(bid_price_dec),
                        None,
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
                        log::error!("[MM] Failed to place bid L{}: {:?}", level, e);
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
                let ask_price = mid * (1.0 + total_offset / 10_000.0);

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
                        None,
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
                        log::error!("[MM] Failed to place ask L{}: {:?}", level, e);
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

        // 9. Manage hedge position
        if self.cfg.hedge_enabled && self.hedge_conn.is_some() {
            self.manage_hedge(max_inventory_tokens, order_size_tokens).await;
        }

        // 10. Log status
        let total_equity = equity + self.hedge_equity_cache;
        log::info!(
            "[MM] mid={:.2} equity={:.2} hedge_equity={:.2} total_equity={:.2} inv={:.6} hedge={:.6} net_exposure={:.6} spread_bps={:.1} skew_bps={:.1} pnl_realized={:.4} trades={} bids_filled={} asks_filled={}",
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
        // Also refresh hedge equity
        if let Some(ref hedge) = self.hedge_conn {
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
                } else {
                    self.inventory = 0.0;
                }
            }
            Err(e) => {
                log::warn!("[MM] Failed to get positions: {:?}", e);
            }
        }

        // Sync hedge position
        if let Some(ref hedge) = self.hedge_conn {
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
                            log::info!("[FILL] LONG size={:.6} price={:.2}", size, fill_price);
                        }
                        _ => {
                            self.total_ask_fills += 1;
                            log::info!("[FILL] SHORT size={:.6} price={:.2}", size, fill_price);
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

    // -----------------------------------------------------------------------
    // Hedge management
    // -----------------------------------------------------------------------

    /// Safety-net hedge on each step cycle.
    /// Same threshold logic as instant hedge — equity-normalized USD exposure.
    /// Also handles closing hedge when exposure returns below close_threshold.
    async fn manage_hedge(&mut self, _max_inventory_tokens: f64, _order_size_tokens: f64) {
        let hedge_conn = match &self.hedge_conn {
            Some(c) => c.clone(),
            None => return,
        };

        let net_exposure = self.inventory + self.hedge_position;
        let mid = self.last_mid.unwrap_or(0.0);
        if mid <= 0.0 {
            return;
        }
        let exposure_usd = net_exposure.abs() * mid;
        let equity = self.equity_cache + self.hedge_equity_cache;
        let hedge_threshold_usd = equity * self.cfg.hedge_threshold_ratio;
        let close_threshold_usd = equity * self.cfg.hedge_close_threshold_ratio;

        if exposure_usd > hedge_threshold_usd {
            // Hedge excess beyond threshold
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
            // Only close hedge when main inventory has genuinely reduced.
            // Check if inventory (main only) is small enough that the hedge is no longer needed.
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
            } else {
                log::debug!(
                    "[HEDGE-STEP] Net exposure low but inventory=${:.2} still above close_threshold=${:.2}, keeping hedge",
                    inv_usd, close_threshold_usd
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Real-time fill detection → instant hedge
    // -----------------------------------------------------------------------

    /// Called every 2s to detect new fills and hedge immediately.
    /// This avoids waiting for the full 60s step cycle.
    async fn check_fills_and_hedge(&mut self) {
        if !self.cfg.hedge_enabled || self.hedge_conn.is_none() || self.in_maintenance_wind_down {
            return;
        }

        // Check for new fills via WebSocket cache
        let fills = match self.main_conn.get_filled_orders(&self.cfg.symbol).await {
            Ok(f) => f,
            Err(_) => return,
        };

        if fills.orders.is_empty() {
            return;
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
                    log::info!("[FILL-RT] LONG size={:.6} price={:.2}", size, fill_price);
                }
                _ => {
                    fill_delta -= size;
                    self.total_ask_fills += 1;
                    log::info!("[FILL-RT] SHORT size={:.6} price={:.2}", size, fill_price);
                }
            }
            self.total_trades += 1;
            let _ = self
                .main_conn
                .clear_filled_order(&self.cfg.symbol, &fill.trade_id)
                .await;
        }

        if fill_delta.abs() < 1e-10 {
            return;
        }

        // Update inventory from exchange position for accuracy
        self.sync_inventory_from_exchange().await;

        // Only hedge if net_exposure (in USD) exceeds equity * hedge_threshold_ratio.
        // Below threshold, inventory skew handles rebalancing naturally,
        // preserving spread revenue. Hedge is insurance for large moves only.
        let net_exposure = self.inventory + self.hedge_position;
        let mid = self.last_mid.unwrap_or(0.0);
        if mid <= 0.0 {
            return;
        }
        let exposure_usd = net_exposure.abs() * mid;
        let equity = self.equity_cache + self.hedge_equity_cache;
        let hedge_threshold_usd = equity * self.cfg.hedge_threshold_ratio;

        if exposure_usd <= hedge_threshold_usd {
            log::debug!(
                "[HEDGE-RT] exposure=${:.2} within threshold=${:.2} ({:.0}% of equity), skipping",
                exposure_usd,
                hedge_threshold_usd,
                self.cfg.hedge_threshold_ratio * 100.0
            );
            return;
        }

        // Hedge only the excess beyond threshold, keeping some inventory for spread revenue
        let excess_usd = exposure_usd - hedge_threshold_usd;
        let excess_tokens = excess_usd / mid;
        let hedge_conn = self.hedge_conn.as_ref().unwrap().clone();
        let hedge_side = if net_exposure > 0.0 {
            OrderSide::Short
        } else {
            OrderSide::Long
        };
        let hedge_size = self.round_size(excess_tokens);
        if hedge_size <= Decimal::ZERO {
            return;
        }

        log::info!(
            "[HEDGE-RT] Instant hedge: exposure=${:.2} threshold=${:.2} excess=${:.2} side={:?} size={}",
            exposure_usd,
            hedge_threshold_usd,
            excess_usd,
            hedge_side,
            hedge_size
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
        // Round to 1 decimal place for BTC (Lighter uses specific tick sizes)
        // This should be adapted per-symbol
        Decimal::from_f64(price)
            .unwrap_or(Decimal::ZERO)
            .round_dp(1)
    }

    fn round_size(&self, size: f64) -> Decimal {
        // Round to 5 decimal places for BTC
        Decimal::from_f64(size).unwrap_or(Decimal::ZERO).round_dp(5)
    }
}
