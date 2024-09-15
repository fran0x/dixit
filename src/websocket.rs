use anyhow::Result;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::{client::IntoClientRequest, Message}, MaybeTlsStream, WebSocketStream};

use crate::error::Error;

pub type WSMessage = Message;
pub type WSStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect(ws_url: &str) -> Result<WSStream> {
    let request = ws_url.into_client_request().map_err(Error::WebSocketURL)?;
    let (stream, _) = connect_async(request).await.map_err(Error::WebSocketConnection)?;
    Ok(stream)
}