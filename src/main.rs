use anyhow::Result;

use config::Config;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

mod config;
mod exchange;
mod error;
mod websocket;
mod writer;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_line_number(true))
        .with(EnvFilter::from_default_env())
        .init();
    let config = Config::read()?;

    Ok(())
}
