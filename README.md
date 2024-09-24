# record0x

**record0x** is a Rust-based tool that collects CoinBase RFQ data via WebSocket, stores it as Parquet files, and enables analysis with Jupyter notebooks.

## Getting Started

You'll need Rust, [Cargo](https://doc.rust-lang.org/cargo), and [Just](https://github.com/casey/just) for data collection, and Python with a virtual environment for data analysis. 

Run `just` in the command line to list available commands.

## Data Analysis

Analyze the data using Jupyter notebooks in the [local](local) directory. More details in [local/README.md](local/README.md).