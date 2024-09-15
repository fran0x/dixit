use anyhow::Result;
use futures::{StreamExt, SinkExt};
use tokio::sync::mpsc;
use tracing::error;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

extern crate parquet;
#[macro_use] extern crate parquet_derive;

mod config;
use config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_line_number(true))
        .with(EnvFilter::from_default_env())
        .init();
    let config = Config::read()?;

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
            Ok(message) => {
                tx.send(coinbase::handle(message)).await?
            }
            Err(e) => {
                error!("Websocket error: {:?}", e);
                break;
            }
        }
    }
    drop(tx);

    persister.await?;

    Ok(())
}

pub enum Record {
    Data { exchange: String, channel: String, data: String },
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
    use serde_json::json;
    use tokio_tungstenite::tungstenite::Message;

    use crate::Record;

    pub fn subscribe() -> Message {
        let subscription = json!({
            "type": "subscribe",
            "channels": ["rfq_matches"]
        });
        Message::Text(subscription.to_string())
    }

    pub fn handle(_message: Message) -> Record {
        Record::Skip
    }
}