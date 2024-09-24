# Project Overview

This project contains two Jupyter notebooks:

1. **[Order Book](orderbook.ipynb)**: This notebook is used to plot an order book persisted in Parquet format as a test for the `persist_derive` crate.
2. **[Coinbase RFQs](coinbase.ipynb)**: This notebook plots RFQs from Coinbase.

### Step by Step

```bash
# Install prerequisites via Homebrew
brew install pyenv pyenv-virtualenv poetry

# Create and activate the virtual environment
pyenv install 3.11
pyenv virtualenv 3.11 <your-env-name>
pyenv activate <your-env-name>

# Install dependencies
poetry install

# Use VSCode to select the virtual environment and run the notebook (see below)
```

### Open the Jupyter Notebook in VSCode

1. Open the project folder in **VSCode**.
2. Use the command palette (`Ctrl + Shift + P` or `Cmd + Shift + P` on macOS) to select the Python interpreter.
3. Choose the virtual environment created (`<your-env-name>`).
4. Open your Jupyter notebook, ensuring the virtual environment is selected as the kernel.
