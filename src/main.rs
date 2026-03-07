use chrono::{DateTime, FixedOffset, Utc};
use env_logger::Builder;
use log::LevelFilter;
use slow_mm::market_maker::{MmConfig, MmEngine};
use std::env;
use std::io::Write;
use std::str::FromStr;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let offset_seconds = env::var("TIMEZONE_OFFSET")
        .unwrap_or_else(|_| "3600".to_string())
        .parse::<i32>()
        .expect("Invalid TIMEZONE_OFFSET");
    let offset = FixedOffset::east_opt(offset_seconds).expect("Invalid offset");
    Builder::from_default_env()
        .format(move |buf, record| {
            let utc_now: DateTime<Utc> = Utc::now();
            let local_now = utc_now.with_timezone(&offset);
            writeln!(
                buf,
                "{} [{}] - {}",
                local_now.format("%Y-%m-%dT%H:%M:%S%z"),
                record.level(),
                record.args()
            )
        })
        .filter(
            None,
            LevelFilter::from_str(
                &env::var("RUST_LOG").unwrap_or_else(|_| {
                    "debug,tokio_tungstenite=info,tungstenite=info".to_string()
                }),
            )
            .unwrap_or(LevelFilter::Debug),
        )
        .init();

    let dex_connector_git = option_env!("DEX_CONNECTOR_GIT_HASH").unwrap_or("unknown");
    log::info!("dex-connector git: {}", dex_connector_git);
    log::info!("Starting slow market-maker...");
    let cfg = MmConfig::from_env_or_yaml().expect("invalid mm config");
    let mut engine = MmEngine::new(cfg)
        .await
        .expect("failed to initialize mm engine");
    engine
        .run()
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}
