use parquet::errors::ParquetError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PersistError {
    #[error("other error: {0}")]
    Other(String),
    #[error("parquet error: {0}")]
    ParquetError(String),
    #[error("io error: {0}")]
    IOError(#[from] std::io::Error),
}

impl From<ParquetError> for PersistError {
    fn from(err: ParquetError) -> Self {
        Self::ParquetError(err.to_string())
    }
}
