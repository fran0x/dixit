# Persist Crates

This project includes two crates for handling Parquet persistence:

1. **`dixit_persist`**: Implements the logic for persisting structs in Parquet files.
2. **`dixit_persist_macros`**: Provides a procedural macro to simplify struct persistence.

### Example Usage:

```rust
use dixit_persist_macros::Persist;

#[derive(Debug, Clone, Persist)]
pub struct OrderBook {
    pub exchange_id: u32,
    pub symbol_id: u32,
    // other fields...
    pub exchange_ts: u64,
    pub internal_ts: u64,
}
```

For more details refer to the test file [`writer.rs`](tests/writer.rs).

---

⚠️ **Warning**: These crates are work in progress, subject to breaking changes.
