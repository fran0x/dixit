use anyhow::Result;
use futures::future::join_all;
use tokio::sync::mpsc;

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
        persister::run(rx).await;
    });

    // launch the websocket
    let websocket = tokio::spawn(async move {
        match args.venue {
            Venue::Coinbase => websocket::run(tx, coinbase::WS_URL, coinbase::subscribe, coinbase::handle).await,
        }
    });

    join_all(vec![persister, websocket]).await;

    Ok(())
}

mod config {
    use clap::{Parser, ValueEnum};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    #[derive(Debug, Clone, PartialEq, ValueEnum)]
    pub enum Venue {
        Coinbase,
    }

    #[derive(Debug, Parser)]
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
    use tokio::sync::mpsc::Receiver;
    use tracing::{error, info};

    use crate::model::{Record, VenueData};

    pub async fn run(mut rx: Receiver<Record>) {
        while let Some(record) = rx.recv().await {
            match record {
                Record::Data {
                    data: VenueData::CoinbaseRfqMatch(_),
                    ..
                } => info!("coinbase data"),
                Record::Skip { message } => info!("skip data: {message}"),
                Record::Error { message, reason } => {
                    error!("{message}: {reason}");
                    break;
                }
            }
        }
    }
}

mod websocket {
    use anyhow::Result;
    use futures::{SinkExt, StreamExt};
    use tokio::net::TcpStream;
    use tokio::sync::mpsc::Sender;
    use tokio_tungstenite::{
        connect_async_tls_with_config,
        tungstenite::{client::IntoClientRequest, Message},
        MaybeTlsStream, WebSocketStream,
    };
    use tracing::error;

    use crate::model::Record;

    pub async fn run(
        tx: Sender<Record>,
        ws_url: &str,
        subscribe_fn: impl Fn() -> Message,
        handle_fn: impl Fn(Message) -> Record,
    ) {
        let mut stream = match connect(ws_url).await {
            Ok(s) => s,
            Err(e) => {
                error!("websocket connect error: {:?}", e);
                return;
            }
        };

        if let Err(e) = stream.send(subscribe_fn()).await {
            error!("websocket send error: {:?}", e);
            return;
        }

        while let Some(message) = stream.next().await {
            match message {
                Ok(message) => {
                    let record = handle_fn(message);
                    if let Err(e) = tx.send(record).await {
                        error!("channel send error: {:?}", e);
                        return;
                    }
                }
                Err(e) => {
                    error!("websocket error: {:?}", e);
                    break;
                }
            }
        }
    }

    async fn connect(ws_url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let request = ws_url.into_client_request()?;
        let (stream, _) = connect_async_tls_with_config(request, None, true, None).await?;
        Ok(stream)
    }
}

mod coinbase {
    use chrono::{DateTime, Utc};
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

    #[derive(Deserialize, Debug)]
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
