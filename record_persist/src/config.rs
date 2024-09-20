use std::{collections::HashSet, fmt};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct PersistConfig {
    #[serde(default)]
    pub directory: String,
    /// if set to true will append to existing files, when false will remove all existing parquet files
    #[serde(default)]
    pub keep: bool,
    /// if set will only record tables matching that name, if empty will assume you want to persist everything
    #[serde(default)]
    pub tables: HashSet<String>,
}

impl PersistConfig {
    pub fn new(directory: &str, table: &str) -> Self {
        let mut tables = HashSet::new();
        tables.insert(table.to_owned());

        Self {
            directory: directory.to_owned(),
            keep: false,
            tables,
        }
    }
}

impl fmt::Display for PersistConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PersistConfig {{ directory: \"{}\", keep: {}, tables: {:?} }}",
            self.directory,
            self.keep,
            if self.tables.is_empty() {
                "all".to_string()
            } else {
                format!("{:?}", self.tables)
            }
        )
    }
}
