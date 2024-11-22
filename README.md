[![Build Badge]][build] [![License Badge]][license]

[Build Badge]: https://github.com/fran0x/dixit/actions/workflows/rust.yml/badge.svg
[build]: https://github.com/fran0x/dixit/actions

[License Badge]: https://img.shields.io/badge/License-MIT-blue.svg  
[license]: LICENSE

# dixit

**dixit** is a Rust-based tool that collects [CoinBase RFQ data via WebSocket](https://docs.cdp.coinbase.com/exchange/docs/websocket-channels#rfq-matches-channel), stores it as Parquet files, and enables data analysis using [Jupyter](https://jupyter.org/) notebooks.

This project includes three crates:
1. `dixit`: Pulls and persists data from Coinbase.
2. `dixit_persist`: Handles struct persistence in Parquet files.
3. `dixit_persist_macros`: Provides a macro for easy persistence.

For more on the persist crates, see the corresponding [README](dixit_persist/README.md).

## Getting Started

You'll need Rust, [Cargo](https://doc.rust-lang.org/cargo), and [Just](https://github.com/casey/just) for data collection, and Python with a virtual environment for data analysis.

Run `just` in the command line to view available commands.

## Step by Step

1. Run `just coinbase` to continuously download data from Coinbase and generate Parquet files in the `output` folder.
2. Stop the task when needed.
3. Go to the [local](local) folder and follow the instructions in the corresponding [README](local/README.md) to launch a notebook for analyzing Coinbase data.

_Note: The default notebook reads sample data from the `local` folder but can easily be configured to read from the `output` folder._

## Next Steps

To extend the project and support other venues just replicate the approach used in [dixit::main::coinbase](dixit/src/main.rs).
