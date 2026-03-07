use chrono::Utc;
use serde::Serialize;
use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Debug, Serialize)]
struct StatusPosition {
    symbol: String,
    side: String,
    size: String,
    entry_price: Option<String>,
}

#[derive(Debug, Serialize)]
struct StatusSnapshot {
    ts: i64,
    updated_at: String,
    id: Option<String>,
    agent: Option<String>,
    dex: String,
    dry_run: bool,
    backtest_mode: bool,
    interval_secs: u64,
    positions_ready: bool,
    position_count: usize,
    has_position: bool,
    positions: Vec<StatusPosition>,
    pnl_total: f64,
    pnl_today: f64,
    pnl_source: String,
}

#[derive(Debug, Serialize, serde::Deserialize)]
struct EquityBaseline {
    date: String,
    equity: f64,
}

#[derive(Debug, Serialize)]
struct EquityHistoryEntry {
    ts: i64,
    equity: f64,
}

pub struct PositionInfo {
    pub symbol: String,
    pub side: String,
    pub size: String,
    pub entry_price: Option<String>,
}

pub struct StatusReporter {
    path: PathBuf,
    id: Option<String>,
    dry_run: bool,
    interval_secs: u64,
    snapshot_every: Duration,
    last_snapshot: Option<Instant>,
    pnl_total: f64,
    pnl_today: f64,
    pnl_today_date: chrono::NaiveDate,
    equity_day_start: f64,
    equity_day_start_set: bool,
    equity_baseline_path: PathBuf,
    equity_history_path: PathBuf,
    last_equity_history_ts: Option<i64>,
}

impl StatusReporter {
    pub fn from_env(dry_run: bool, interval_secs: u64) -> Option<Self> {
        let enabled = env::var("DEBOT_STATUS_ENABLED")
            .ok()
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                !(v == "0" || v == "false" || v == "no")
            })
            .unwrap_or(true);
        if !enabled {
            return None;
        }

        let id = env::var("DEBOT_STATUS_ID")
            .ok()
            .map(|v| sanitize_id(&v))
            .filter(|v| !v.is_empty());

        let path = env::var("DEBOT_STATUS_PATH")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                env::var("DEBOT_STATUS_DIR")
                    .ok()
                    .filter(|v| !v.trim().is_empty())
                    .map(PathBuf::from)
                    .map(|dir| match &id {
                        Some(id) => dir.join(id).join("status.json"),
                        None => dir.join("status.json"),
                    })
            })
            .or_else(|| {
                env::var("HOME")
                    .ok()
                    .map(|home| PathBuf::from(home).join("debot_status"))
                    .map(|base| match &id {
                        Some(id) => base.join(id).join("status.json"),
                        None => base.join("status.json"),
                    })
            })
            .unwrap_or_else(|| PathBuf::from("status.json"));

        let equity_baseline_path = path.with_extension("equity.json");
        let equity_history_path = path.with_extension("equity_history.jsonl");
        let interval_secs_clamped = interval_secs.max(1);
        let snapshot_every = {
            let target_secs = 60_u64;
            let n = ((target_secs + interval_secs_clamped - 1) / interval_secs_clamped).max(1);
            Duration::from_secs(interval_secs_clamped.saturating_mul(n).max(1))
        };

        let mut reporter = Self {
            path,
            id,
            dry_run,
            interval_secs,
            snapshot_every,
            last_snapshot: None,
            pnl_total: 0.0,
            pnl_today: 0.0,
            pnl_today_date: Utc::now().date_naive(),
            equity_day_start: 0.0,
            equity_day_start_set: false,
            equity_baseline_path,
            equity_history_path,
            last_equity_history_ts: None,
        };
        reporter.load_equity_baseline();
        log::info!("[STATUS] path={}", reporter.path.display());
        Some(reporter)
    }

    pub fn update_equity(&mut self, equity: f64) {
        let today = Utc::now().date_naive();
        self.pnl_total = equity;
        if !self.equity_day_start_set || self.pnl_today_date != today {
            self.pnl_today_date = today;
            self.equity_day_start = equity;
            self.equity_day_start_set = true;
            self.persist_equity_baseline();
        }
        if self.equity_day_start_set {
            self.pnl_today = equity - self.equity_day_start;
        }
        self.append_equity_history(equity);
    }

    pub fn write_snapshot(&mut self, positions: &[PositionInfo]) -> io::Result<()> {
        self.reset_daily_if_needed();
        let now = Utc::now();
        let status_positions: Vec<StatusPosition> = positions
            .iter()
            .map(|p| StatusPosition {
                symbol: p.symbol.clone(),
                side: p.side.clone(),
                size: p.size.clone(),
                entry_price: p.entry_price.clone(),
            })
            .collect();

        let snapshot = StatusSnapshot {
            ts: now.timestamp_millis(),
            updated_at: now.to_rfc3339(),
            id: self.id.clone(),
            agent: Some("slow-mm".to_string()),
            dex: "lighter".to_string(),
            dry_run: self.dry_run,
            backtest_mode: false,
            interval_secs: self.interval_secs,
            positions_ready: true,
            position_count: status_positions.len(),
            has_position: !status_positions.is_empty(),
            positions: status_positions,
            pnl_total: self.pnl_total,
            pnl_today: self.pnl_today,
            pnl_source: "equity".to_string(),
        };

        let payload = serde_json::to_string(&snapshot)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = self.path.with_extension("tmp");
        fs::write(&tmp_path, payload)?;
        fs::rename(tmp_path, &self.path)?;
        Ok(())
    }

    pub fn write_snapshot_if_due(&mut self, positions: &[PositionInfo]) -> io::Result<bool> {
        let due = self
            .last_snapshot
            .map(|t| t.elapsed() >= self.snapshot_every)
            .unwrap_or(true);
        if !due {
            return Ok(false);
        }
        self.write_snapshot(positions)?;
        self.last_snapshot = Some(Instant::now());
        Ok(true)
    }

    fn load_equity_baseline(&mut self) {
        let data = match fs::read_to_string(&self.equity_baseline_path) {
            Ok(d) => d,
            Err(_) => return,
        };
        let baseline: EquityBaseline = match serde_json::from_str(&data) {
            Ok(b) => b,
            Err(_) => return,
        };
        let Ok(date) = chrono::NaiveDate::parse_from_str(&baseline.date, "%Y-%m-%d") else {
            return;
        };
        self.equity_day_start = baseline.equity;
        self.pnl_today_date = date;
        self.equity_day_start_set = true;
    }

    fn persist_equity_baseline(&self) {
        let baseline = EquityBaseline {
            date: self.pnl_today_date.format("%Y-%m-%d").to_string(),
            equity: self.equity_day_start,
        };
        let Ok(payload) = serde_json::to_string(&baseline) else {
            return;
        };
        if let Some(parent) = self.equity_baseline_path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let _ = fs::write(&self.equity_baseline_path, payload);
    }

    fn append_equity_history(&mut self, equity: f64) {
        let now_ts = Utc::now().timestamp();
        let interval = 300i64; // 5-minute intervals
        let bucket = now_ts / interval * interval;
        if let Some(last) = self.last_equity_history_ts {
            if bucket <= last {
                return;
            }
        }
        self.last_equity_history_ts = Some(bucket);
        let entry = EquityHistoryEntry {
            ts: bucket,
            equity,
        };
        let Ok(line) = serde_json::to_string(&entry) else {
            return;
        };
        if let Some(parent) = self.equity_history_path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let _ = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.equity_history_path)
            .and_then(|mut f| {
                use std::io::Write;
                writeln!(f, "{}", line)
            });
    }

    fn reset_daily_if_needed(&mut self) {
        if !self.equity_day_start_set {
            return;
        }
        let today = Utc::now().date_naive();
        if today != self.pnl_today_date {
            self.pnl_today_date = today;
            self.equity_day_start = self.pnl_total;
            self.persist_equity_baseline();
        }
        self.pnl_today = self.pnl_total - self.equity_day_start;
    }
}

fn sanitize_id(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
        .collect()
}
