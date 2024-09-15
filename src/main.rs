use anyhow::Result;
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tracing::error;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

// extern crate parquet;
// #[macro_use] extern crate parquet_derive;

mod config;
use config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    // set tracing and load the configuration
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_line_number(true))
        .with(EnvFilter::from_default_env())
        .init();
    let config = Config::read()?;

    // create a channel to send data from the websocket to the persister
    let (tx, rx) = mpsc::channel::<Record>(100);

    // spawn the persister
    let persister = tokio::spawn(async move {
        if let Err(e) = persister::handle(rx).await {
            error!("Persister error: {:?}", e);
        }
    });

    // connect to the websocket and start sending messages to the persister
    let mut stream = websocket::connect(&config.ws_url).await?;
    stream.send(coinbase::subscribe()).await?;

    while let Some(message) = stream.next().await {
        match message {
            Ok(message) => tx.send(coinbase::handle(message)).await?,
            Err(e) => {
                error!("Websocket error: {:?}", e);
                break;
            }
        }
    }

    // housekeeping
    drop(tx);
    persister.await?;

    Ok(())
}

pub enum Record {
    Data {
        exchange: String,
        channel: String,
        symbol: String,
        data: String,
    },
    Skip,
}

mod websocket {
    use anyhow::Result;
    use tokio::net::TcpStream;
    use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest, MaybeTlsStream, WebSocketStream};

    pub async fn connect(ws_url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let request = ws_url.into_client_request()?;
        let (stream, _) = connect_async(request).await?;
        Ok(stream)
    }
}

mod persister {
    use anyhow::Result;
    use tokio::sync::mpsc::Receiver;

    use crate::Record;

    pub async fn handle(rx: Receiver<Record>) -> Result<()> {
        todo!()
    }
}

mod coinbase {
    // use serde::Deserialize;
    use serde_json::{from_str, json, Value};
    use tokio_tungstenite::tungstenite::Message;

    use crate::{util::field, Record};

    const EXCHANGE: &str = "coinbase";

    pub fn subscribe() -> Message {
        let subscription = json!({
            "type": "subscribe",
            "channels": ["rfq_matches"]
        });
        Message::Text(subscription.to_string())
    }

    pub fn handle(message: Message) -> Record {
        match message {
            Message::Text(string) => match from_str::<Value>(&string) {
                Ok(value) => Record::Data {
                    exchange: EXCHANGE.to_string(),
                    channel: field(&value, "type"),
                    symbol: field(&value, "product_id"),
                    data: string,
                },
                _ => Record::Skip,
            },
            _ => Record::Skip,
        }
    }

    // #[derive(ParquetRecordWriter, Deserialize, Debug)]
    // pub struct RfqMatch {
    //     #[serde(rename = "type")]
    //     pub channel: String,
    //     pub maker_order_id: String,
    //     pub taker_order_id: String,
    //     pub time: String,
    //     pub trade_id: u64,
    //     pub product_id: String,
    //     pub size: f64,
    //     pub price: f64,
    //     pub side: String,
    // }
}

mod util {
    use serde_json::Value;

    #[inline]
    pub fn field(value: &Value, key: &str) -> String {
        value[key].as_str().unwrap_or_default().to_owned()
    }
}
