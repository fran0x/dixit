//! # Main Application
//! This program collects RFQ (Request for Quote) data from Coinbase via WebSocket,
//! processes the data, and stores it in Parquet files for further analysis.
//!
//! ## Overview
//! - Configures and initializes the application settings using the `config` module.
//! - Uses the `websocket` module to connect to the Coinbase WebSocket feed and handle messages.
//! - Persists processed data into Parquet files using the `persister` module.
//! - Defines data structures in the `model` module to represent RFQ records and errors.
//!
//! ## Workflow
//! 1. Initialize the application and parse arguments.
//! 2. Set up a communication channel between the WebSocket handler and the persister.
//! 3. Launch tasks to handle WebSocket connections and data persistence.
//! 4. Process and persist data until the application is stopped.

use anyhow::Result;
use futures::future::join_all;
use tokio::sync::mpsc;
use tracing::error;

use config::{init, Venue};
use model::Record;

#[tokio::main]
async fn main() -> Result<()> {
    // initialize application settings and read command line arguments
    let args = init();

    // create a channel to send data from the websocket to the persister
    let (tx, rx) = mpsc::channel::<Record>(100);

    // launch the persister
    let persister = tokio::spawn(async move {
        if let Err(e) = persister::run(args.venue, rx).await {
            error!("persisted error: {e}");
        }
    });

    // launch the websocket
    let websocket = tokio::spawn(async move {
        if let Err(e) = match args.venue {
            Venue::Coinbase => websocket::run(tx, coinbase::WS_URL, coinbase::subscribe, coinbase::handle).await,
        } {
            error!("websocket error: {e}");
        }
    });

    join_all(vec![persister, websocket]).await;

    Ok(())
}

mod config {
    //! Handles application configuration and initialization.
    //!
    //! ## Features
    //! - Defines the [`Venue`] enum to specify supported venues (e.g., Coinbase).
    //! - Parses command-line arguments using [`clap`].
    //! - Configures logging with environment-based filtering.
    //!
    //! ## Example
    //! ```rust
    //! use config::init;
    //!
    //! let args = init();
    //! println!("Selected venue: {}", args.venue);
    //! ```

    use std::fmt;

    use clap::{Parser, ValueEnum};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    #[derive(Debug, Clone, Copy, PartialEq, ValueEnum)]
    pub enum Venue {
        Coinbase,
    }

    impl fmt::Display for Venue {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let status_str = match self {
                Venue::Coinbase => "coinbase",
            };
            write!(f, "{}", status_str)
        }
    }

    #[derive(Debug, Clone, Copy, Parser)]
    #[clap(author, version, about, long_about = None)]
    pub struct Args {
        #[clap(short, long, value_enum)]
        pub venue: Venue,
    }

    pub fn init() -> Args {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_line_number(true))
            .with(EnvFilter::from_default_env())
            .init();

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        Args::parse()
    }
}

mod model {
    //! Defines data structures for RFQ records and venue-specific data.
    //!
    //! ## Features
    //! - `Record`: Represents a single RFQ record, which could be valid data, skipped messages, or errors.
    //! - `VenueData`: Wraps venue-specific data types for RFQ processing.

    use crate::coinbase::RfqMatch;

    pub enum VenueData {
        CoinbaseRfqMatch(RfqMatch),
    }

    pub enum Record {
        Data {
            exchange: String,
            channel: String,
            symbol: String,
            data: VenueData,
        },
        Skip {
            message: String,
        },
        Error {
            message: String,
            reason: String,
        },
    }
}

mod persister {
    //! Persists RFQ data into Parquet files for long-term storage and analysis.
    //!
    //! ## Features
    //! - Configures output directories and files using the `dixit_persist` crate.
    //! - Processes incoming RFQ records from an `mpsc::Receiver`.
    //! - Handles valid data, skips irrelevant records, and logs errors.

    use std::{env, sync::LazyLock};

    use anyhow::{Ok, Result};
    use dixit_persist::{config::PersistConfig, writer::TableWriter};
    use tokio::sync::mpsc::Receiver;
    use tracing::{error, info};

    use crate::{
        config::Venue,
        model::{Record, VenueData},
    };

    static OUTPUT_FOLDER: LazyLock<String> = LazyLock::new(|| {
        let mut path_buf = env::current_dir().unwrap();
        path_buf.push("output");
        path_buf.into_os_string().into_string().expect("invalid path")
    });

    pub async fn run(venue: Venue, mut rx: Receiver<Record>) -> Result<()> {
        let config = PersistConfig::new(&OUTPUT_FOLDER, &venue.to_string());
        let mut writer = TableWriter::new(&venue.to_string(), &config)?;

        while let Some(record) = rx.recv().await {
            match record {
                Record::Data {
                    data: VenueData::CoinbaseRfqMatch(rfq_match),
                    exchange,
                    channel,
                    symbol,
                } => {
                    info!("[{exchange}] [{channel}] [{symbol}]: {:?}", rfq_match);
                    writer.begin()?.record(&rfq_match)?.end()?;
                    writer.flush_if_needed()?;
                }
                Record::Skip { message } => info!("skip data: {message}"),
                Record::Error { message, reason } => {
                    error!("{message}: {reason}");
                    break;
                }
            }
        }

        writer.flush()?;
        Ok(())
    }
}

mod websocket {
    //! Manages WebSocket connections to receive RFQ data in real time.
    //!
    //! ## Features
    //! - Establishes a WebSocket connection using `tokio-tungstenite`.
    //! - Sends subscription messages to start receiving data.
    //! - Processes incoming messages and forwards them to the data channel.

    use anyhow::{anyhow, Result};
    use futures::{SinkExt, StreamExt};
    use tokio::net::TcpStream;
    use tokio::sync::mpsc::Sender;
    use tokio_tungstenite::{
        connect_async_tls_with_config,
        tungstenite::{client::IntoClientRequest, Message},
        MaybeTlsStream, WebSocketStream,
    };

    use crate::model::Record;

    pub async fn run(
        tx: Sender<Record>,
        ws_url: &str,
        subscribe_fn: impl Fn() -> Message,
        handle_fn: impl Fn(Message) -> Record,
    ) -> Result<()> {
        let mut stream = connect(ws_url).await?;

        stream.send(subscribe_fn()).await?;

        while let Some(message) = stream.next().await {
            match message {
                Ok(message) => {
                    let record = handle_fn(message);
                    tx.send(record).await?;
                }
                Err(e) => return Err(anyhow!(e)),
            }
        }

        Ok(())
    }

    async fn connect(ws_url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let request = ws_url.into_client_request()?;
        let (stream, _) = connect_async_tls_with_config(request, None, true, None).await?;
        Ok(stream)
    }
}

mod coinbase {
    //! Contains Coinbase-specific WebSocket handling logic.
    //!
    //! ## Features
    //! - Subscribes to RFQ data channels on the Coinbase WebSocket feed.
    //! - Parses incoming messages into RFQ match records or errors.

    use chrono::{DateTime, Utc};
    use dixit_persist_macros::Persist;
    use rust_decimal::Decimal;
    use serde::Deserialize;
    use serde_json::{from_str, json};
    use tokio_tungstenite::tungstenite::Message;

    use crate::model::{Record, VenueData};

    pub const EXCHANGE: &str = "coinbase";
    pub const WS_URL: &str = "wss://ws-feed.exchange.coinbase.com";

    pub fn subscribe() -> Message {
        let subscription = json!({
            "type": "subscribe",
            "channels": ["rfq_matches"]
        });
        Message::Text(subscription.to_string())
    }

    pub fn handle(message: Message) -> Record {
        match message {
            Message::Text(string) => {
                if let Ok(rfq_match) = from_str::<RfqMatch>(&string) {
                    if rfq_match.channel == "rfq_match" {
                        return Record::Data {
                            exchange: EXCHANGE.to_string(),
                            channel: rfq_match.channel.clone(),
                            symbol: rfq_match.product_id.clone(),
                            data: VenueData::CoinbaseRfqMatch(rfq_match),
                        };
                    }
                } else if let Ok(rfq_error) = from_str::<RfqError>(&string) {
                    if rfq_error.channel == "error" {
                        return Record::Error {
                            message: rfq_error.message,
                            reason: rfq_error.reason,
                        };
                    }
                }
                Record::Skip { message: string }
            }
            _ => Record::Skip {
                message: "no text".to_owned(),
            },
        }
    }

    #[derive(Deserialize, Debug, Persist)]
    pub struct RfqMatch {
        #[serde(rename = "type")]
        pub channel: String,
        pub maker_order_id: String,
        pub taker_order_id: String,
        pub time: DateTime<Utc>,
        pub trade_id: u64,
        pub product_id: String,
        pub size: Decimal,
        pub price: Decimal,
        pub side: String,
    }

    #[derive(Deserialize, Debug)]
    pub struct RfqError {
        #[serde(rename = "type")]
        pub channel: String,
        pub message: String,
        pub reason: String,
    }
}
