use crate::config::PersistConfig;
use crate::error::PersistError;
use crate::row::RowBuffer;
use crate::Persistable;

use anyhow::Result;
use itertools::Itertools;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::{Type, TypePtr};
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{error, info, warn};

const BUFFERED_ROWS: usize = 1_000_000;

pub struct TableWriter {
    flush_size: usize,
    pub current_file_path: PathBuf,
    file_index: usize,
    buffer: RowBuffer,
    enabled: bool,
    fields: Vec<TypePtr>,
    schema: Option<Arc<Type>>,
    pub auto_flush: bool,
}

impl Drop for TableWriter {
    fn drop(&mut self) {
        if !self.buffer.is_empty() {
            if let Err(e) = self.flush() {
                error!("failed to flush file {:?}", e)
            }
        }
    }
}

pub struct RowBuilder<'a> {
    writer: &'a mut TableWriter,
}

impl<'a> RowBuilder<'a> {
    pub fn new(writer: &'a mut TableWriter) -> Self {
        Self { writer }
    }

    pub fn record<T: Persistable>(self, record: &T) -> Result<Self, PersistError> {
        if self.writer.enabled {
            if self.writer.schema.is_none() {
                T::schema(&mut self.writer.fields, None, None, None);
            }

            record.append(&mut self.writer.buffer)?;
        }
        Ok(self)
    }

    pub fn end(&mut self) -> Result<(), PersistError> {
        if self.writer.enabled {
            if self.writer.schema.is_none() {
                info!(
                    "created table {:?} {:?}",
                    self.writer.current_file_path,
                    self.writer
                        .fields
                        .iter()
                        .map(|f| format!("{}:{:?}", f.name(), f.get_physical_type()))
                        .collect_vec()
                );

                self.writer.schema.replace(Arc::new(
                    Type::group_type_builder("schema")
                        .with_fields(self.writer.fields.clone())
                        .build()?,
                ));
            }

            if self.writer.auto_flush {
                self.writer.flush_if_needed()?;
            }
        }
        Ok(())
    }
}

impl TableWriter {
    pub fn new(path_prefix: &str, persist_config: &PersistConfig) -> Result<Self> {
        let enabled = (persist_config.tables.is_empty() || persist_config.tables.contains(path_prefix))
            && !persist_config.directory.is_empty();

        if !enabled {
            info!("ignoring parquet persistence for {path_prefix} as its not mentioned in persist config {persist_config}");
        }

        let mut path = PathBuf::from(&persist_config.directory);
        path.push(path_prefix);
        if !persist_config.directory.is_empty() {
            if !persist_config.keep {
                warn!("deleting directory {:?}", &path);
                let _ = fs::remove_dir_all(Path::new(&path));
            }
            fs::create_dir_all(&path)?;
        }

        Ok(TableWriter {
            flush_size: BUFFERED_ROWS,
            current_file_path: path,
            file_index: 0,
            enabled,
            buffer: RowBuffer::default(),
            fields: vec![],
            schema: None,
            auto_flush: true,
        })
    }

    pub fn begin(&mut self) -> Result<RowBuilder> {
        if self.enabled {
            if self.buffer.len() >= self.flush_size {
                self.flush()?;
            }

            self.buffer.begin();
        }
        Ok(RowBuilder::new(self))
    }

    pub fn flush(&mut self) -> Result<(), PersistError> {
        if self.buffer.is_empty() || !self.enabled {
            return Ok(());
        }

        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| PersistError::Other("schema has not been created".to_string()))?
            .clone();

        let level = ZstdLevel::try_new(1)
            .map_err(|e| PersistError::Other(format!("cannot select correct parquet compression level - {:?}", e)))?;
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::ZSTD(level))
                .build(),
        );

        let mut buf;
        loop {
            buf = PathBuf::from(&self.current_file_path);
            buf.push(format!("{:0>9}.parquet", self.file_index));
            self.file_index += 1;
            if !buf.as_path().exists() {
                break;
            }
        }
        info!("saving {:?}", buf);
        let mut writer = SerializedFileWriter::new(File::create_new(buf)?, schema, props)
            .map_err(|e| PersistError::Other(format!("cannot create parquet serialiser - {:?}", e)))?;

        self.buffer.record(&mut writer).map_err(|e| {
            PersistError::Other(format!(
                "failed to write to parquet {:?} - {:?}",
                self.current_file_path, e
            ))
        })?;

        let result = writer
            .close()
            .map_err(|e| PersistError::Other(format!("failed to close parquet writer - {:?}", e)))?;

        info!("written {} rows", result.num_rows);

        Ok(())
    }

    pub fn flush_if_needed(&mut self) -> Result<(), PersistError> {
        if self.buffer.len() >= self.flush_size {
            self.flush()?
        }
        Ok(())
    }
}
