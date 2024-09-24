Here’s an improved version of your README:

---

[![Build Badge]][build] [![License Badge]][license]

[Build Badge]: https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2Ffran0x%record0x%2Fbadge%3Fref%3Dmain&style=flat&label=build  
[build]: https://actions-badge.atrox.dev/fran0x/record0x/goto?ref=main

[License Badge]: https://img.shields.io/badge/License-MIT-blue.svg  
[license]: LICENSE

# record0x

**record0x** is a Rust-based tool that collects [CoinBase RFQ data via WebSocket](https://docs.cdp.coinbase.com/exchange/docs/websocket-channels#rfq-matches-channel), stores it as Parquet files, and enables data analysis using [Jupyter](https://jupyter.org/) notebooks.

This project includes three crates:
1. `record_bin`: Pulls and persists data from Coinbase.
2. `record_persist`: Handles struct persistence in Parquet files.
3. `record_persist_derive`: Provides a macro for easy persistence.

For more on the persist crates, see the corresponding [README](record_persist/README.md).

## Getting Started

You'll need Rust, [Cargo](https://doc.rust-lang.org/cargo), and [Just](https://github.com/casey/just) for data collection, and Python with a virtual environment for data analysis.

Run `just` in the command line to view available commands.

## Step by Step

1. Run `just coinbase` to continuously download data from Coinbase and generate Parquet files in the `output` folder.
2. Stop the task when needed.
3. Go to the [local](local) folder and follow the instructions in the corresponding [README](local/README.md) to launch a notebook for analyzing Coinbase data.

_Note: The default notebook reads sample data from the `local` folder but can easily be configured to read from the `output` folder._