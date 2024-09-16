# print options
default:
    @just --list --unsorted

# install cargo tools
init:
    cargo upgrade --incompatible
    cargo update

# check formatting and clippy
check:
    cargo check
    cargo fmt --all -- --check
    cargo clippy --all-targets --all-features

# automatically fix clippy warnings
fix:
    cargo fmt --all
    cargo clippy --allow-dirty --allow-staged --fix

# execute the tests
test:
    cargo test
    forge test --root ./contracts --gas-report

# build project (debug profile)
build:
    cargo build --all-targets

# build project (release profile)
release:
    cargo build --all-targets --release

# launch coinbase
coinbase:
    RUST_LOG=info cargo run -- -v coinbase