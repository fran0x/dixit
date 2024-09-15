use thiserror::Error;
use tokio_tungstenite::tungstenite;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid WebSocket URL")]
    WebSocketURL(#[source] tungstenite::error::Error),
    #[error("Failed to establish WebSocket connection")]
    WebSocketConnection(#[source] tungstenite::error::Error),
}