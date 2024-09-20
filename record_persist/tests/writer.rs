use std::{env, sync::LazyLock};

mod orderbook;

static TMP_FOLDER: LazyLock<String> = LazyLock::new(|| {
    let mut path_buf = env::current_dir().unwrap();
    path_buf.push("target");
    //path_buf.push("debug");
    path_buf.push("test");
    path_buf.into_os_string().into_string().expect("invalid path")
});

#[cfg(test)]
mod tests {
    use anyhow::{Ok, Result};

    use record_persist::{config::PersistConfig, writer::TableWriter};

    use crate::orderbook::OrderBook;
    use crate::TMP_FOLDER;

    #[test]
    fn test_random_simple_instance() {
        let instance = OrderBook::random_instance(100, 200, 50.0);
        //print!("{instance}");
        assert!(instance.exchange_id >= 1 && instance.exchange_id < 1000);
        assert!(instance.symbol_id >= 1 && instance.symbol_id < 1000);
        assert!(!instance.buys.is_empty());
        assert!(!instance.sells.is_empty());
    }

    #[test]
    fn test_persist_derive() -> Result<()> {
        let config = PersistConfig::new(&TMP_FOLDER, "orderbook");
        let mut writer = TableWriter::new("orderbook", &config)?;

        let mut instance = OrderBook::random_instance(100, 200, 50.0);
        writer.begin()?.record(&instance)?.end()?;

        for _ in 0..10 {
            instance = instance.tick();
            writer.begin()?.record(&instance)?.end()?;
            writer.flush_if_needed()?;
        }
        writer.flush()?;

        Ok(())
    }
}
