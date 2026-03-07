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
use tokio::time::{sleep, Duration};

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
    stale_order_secs: Option<u64>,
    volatility_window: Option<usize>,
    volatility_spread_mult: Option<f64>,
    min_spread_bps: Option<f64>,
    max_spread_bps: Option<f64>,
    ob_depth: Option<usize>,
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
    /// Hedge when inventory exceeds max_inventory * this ratio
    pub hedge_threshold_ratio: f64,
    /// Close hedge when inventory drops below max_inventory * this ratio
    pub hedge_close_threshold_ratio: f64,
    /// Hard inventory limit = max_inventory * this mult; stop quoting on that side
    pub inventory_hard_limit_mult: f64,
    /// Cancel and re-place orders after this many seconds even if mid hasn't moved
    pub stale_order_secs: u64,
    /// Number of mid-price samples for volatility estimation
    pub volatility_window: usize,
    /// Spread = base_spread + vol * this
    pub volatility_spread_mult: f64,
    pub min_spread_bps: f64,
    pub max_spread_bps: f64,
    pub ob_depth: usize,
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
            stale_order_secs: yaml.stale_order_secs.unwrap_or(DEFAULT_STALE_ORDER_SECS),
            volatility_window: yaml.volatility_window.unwrap_or(DEFAULT_VOLATILITY_WINDOW),
            volatility_spread_mult: yaml
                .volatility_spread_mult
                .unwrap_or(DEFAULT_VOLATILITY_SPREAD_MULT),
            min_spread_bps: yaml.min_spread_bps.unwrap_or(DEFAULT_MIN_SPREAD_BPS),
            max_spread_bps: yaml.max_spread_bps.unwrap_or(DEFAULT_MAX_SPREAD_BPS),
            ob_depth: yaml.ob_depth.unwrap_or(DEFAULT_OB_DEPTH),
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
            stale_order_secs: parse_env("STALE_ORDER_SECS", DEFAULT_STALE_ORDER_SECS),
            volatility_window: parse_env("VOLATILITY_WINDOW", DEFAULT_VOLATILITY_WINDOW),
            volatility_spread_mult: parse_env(
                "VOLATILITY_SPREAD_MULT",
                DEFAULT_VOLATILITY_SPREAD_MULT,
            ),
            min_spread_bps: parse_env("MIN_SPREAD_BPS", DEFAULT_MIN_SPREAD_BPS),
            max_spread_bps: parse_env("MAX_SPREAD_BPS", DEFAULT_MAX_SPREAD_BPS),
            ob_depth: parse_env("OB_DEPTH", DEFAULT_OB_DEPTH),
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
    /// Cached account equity in USD
    equity_cache: f64,
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
    /// Running stats
    total_trades: u64,
    total_bid_fills: u64,
    total_ask_fills: u64,
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
        Ok(Self {
            cfg,
            main_conn: Arc::new(main_conn),
            hedge_conn,
            equity_cache: equity_fallback,
            last_equity_fetch: None,
            inventory: 0.0,
            hedge_position: 0.0,
            mid_prices: Vec::new(),
            active_bid_ids: Vec::new(),
            active_ask_ids: Vec::new(),
            last_order_time: None,
            realized_pnl: 0.0,
            last_mid: None,
            total_trades: 0,
            total_bid_fills: 0,
            total_ask_fills: 0,
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

        // Cancel any leftover orders on startup
        self.cancel_all_main_orders().await;

        // Sync inventory from exchange position
        self.sync_inventory_from_exchange().await;

        // Allow websocket to warm up
        sleep(Duration::from_secs(3)).await;

        let mut ticker = tokio::time::interval(Duration::from_secs(self.cfg.interval_secs));
        loop {
            ticker.tick().await;
            if let Err(e) = self.step().await {
                log::error!("[MM] step failed: {:?}", e);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Main loop step
    // -----------------------------------------------------------------------

    async fn step(&mut self) -> Result<()> {
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

        // 7. Compute dynamic spread based on volatility
        let effective_spread_bps = self.compute_effective_spread();

        // 8. Compute skew based on inventory
        let skew_bps = self.compute_skew_bps(max_inventory_tokens);

        let half_spread_bps = effective_spread_bps / 2.0;
        let bid_offset_bps = half_spread_bps + skew_bps; // positive skew pushes bid down when long
        let ask_offset_bps = half_spread_bps - skew_bps; // positive skew pulls ask down when long

        // 9. Determine if we should quote on each side
        let hard_limit = max_inventory_tokens * self.cfg.inventory_hard_limit_mult;
        let quote_bid = self.inventory < hard_limit;
        let quote_ask = self.inventory > -hard_limit;

        // 8. Cancel existing orders and place new ones
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
                        None,
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
                        None,
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
        let unrealized_pnl = self.estimate_unrealized_pnl(mid);
        log::info!(
            "[MM] mid={:.2} equity={:.2} order_size={:.6} max_inv={:.6} inv={:.6} hedge={:.6} net_exposure={:.6} spread_bps={:.1} skew_bps={:.1} pnl_realized={:.4} pnl_unrealized={:.4} trades={} bids_filled={} asks_filled={}",
            mid,
            equity,
            order_size_tokens,
            max_inventory_tokens,
            self.inventory,
            self.hedge_position,
            self.inventory + self.hedge_position,
            effective_spread_bps,
            skew_bps,
            self.realized_pnl,
            unrealized_pnl,
            self.total_trades,
            self.total_bid_fills,
            self.total_ask_fills,
        );

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
                    log::debug!("[MM] Equity refreshed: {:.2}", equity);
                }
            }
            Err(e) => {
                log::warn!("[MM] Failed to get balance, using cached equity {:.2}: {:?}", self.equity_cache, e);
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
                    let value = fill.filled_value.and_then(|v| v.to_f64()).unwrap_or(0.0);

                    if size <= 0.0 {
                        continue;
                    }

                    self.total_trades += 1;
                    match fill.filled_side {
                        Some(OrderSide::Long) => {
                            self.total_bid_fills += 1;
                            log::info!("[FILL] LONG size={:.6} value={:.2}", size, value);
                        }
                        _ => {
                            self.total_ask_fills += 1;
                            log::info!("[FILL] SHORT size={:.6} value={:.2}", size, value);
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

    // -----------------------------------------------------------------------
    // Hedge management
    // -----------------------------------------------------------------------

    async fn manage_hedge(&mut self, max_inventory_tokens: f64, order_size_tokens: f64) {
        let hedge_conn = match &self.hedge_conn {
            Some(c) => c.clone(),
            None => return,
        };

        let abs_inv = self.inventory.abs();
        let hedge_threshold = max_inventory_tokens * self.cfg.hedge_threshold_ratio;
        let close_threshold = max_inventory_tokens * self.cfg.hedge_close_threshold_ratio;

        // Desired hedge = -inventory (delta neutral)
        // But only hedge when above threshold
        let desired_hedge = if abs_inv >= hedge_threshold {
            -self.inventory
        } else if abs_inv <= close_threshold && self.hedge_position.abs() > 0.0 {
            // Inventory has decreased — close the hedge
            0.0
        } else {
            // In between — keep existing hedge
            return;
        };

        let hedge_delta = desired_hedge - self.hedge_position;
        if hedge_delta.abs() < order_size_tokens * 0.5 {
            return; // Delta too small to bother
        }

        let side = if hedge_delta > 0.0 {
            OrderSide::Long
        } else {
            OrderSide::Short
        };
        let size = self.round_size(hedge_delta.abs());
        if size <= Decimal::ZERO {
            return;
        }

        // Use market order (no price) for hedge to ensure fill
        log::info!(
            "[HEDGE] Adjusting: current={:.6} desired={:.6} delta={:.6} side={:?}",
            self.hedge_position,
            desired_hedge,
            hedge_delta,
            side
        );

        match hedge_conn
            .create_order(&self.cfg.symbol, size, side, None, None, false, None)
            .await
        {
            Ok(_resp) => {
                log::info!("[HEDGE] Order placed: {:?} size={} (market)", side, size);
            }
            Err(e) => {
                log::error!("[HEDGE] Failed to place hedge order: {:?}", e);
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
    // PnL estimation
    // -----------------------------------------------------------------------

    fn estimate_unrealized_pnl(&self, mid: f64) -> f64 {
        // Unrealized PnL from main inventory at current mid
        // This is a rough estimate; actual PnL depends on entry prices
        let net_exposure = self.inventory + self.hedge_position;
        if let Some(last_mid) = self.last_mid {
            net_exposure * (mid - last_mid)
        } else {
            0.0
        }
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
