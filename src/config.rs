use anyhow::{Ok, Result};
use clap::Parser;
use std::fs;
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    config: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub name: String,
    pub ws_url: String,
    pub channels: Vec<String>,
}

impl Config {
    pub fn read() -> Result<Config> {
        let input = fs::read(Args::parse().config)?;
        let config = serde_yaml::from_slice::<Config>(input.as_slice())?;
        Ok(config)
    }
}