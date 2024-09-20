#[cfg(test)]
mod tests {
    use anyhow::{Ok, Result};
    use std::collections::HashSet;
    
    use record_persist::{config::PersistConfig, writer::TableWriter};

    use crate::fixture::{get_tmp_folder, OrderBook};


    #[test]
    fn test_random_simple_instance() {
        let instance = OrderBook::random_instance(100, 200, 50.0);
        print!("{instance}");
        assert!(instance.exchange_id >= 1 && instance.exchange_id < 1000);
        assert!(instance.symbol_id >= 1 && instance.symbol_id < 1000);
        assert!(!instance.buys.is_empty());
        assert!(!instance.sells.is_empty());
    }

    #[test]
    fn test_persist_derive() -> Result<()> {
        let mut tables = HashSet::new();
        tables.insert("simple".to_string());

        let config = PersistConfig {
            directory: get_tmp_folder(),
            keep: false,
            tables,
        };

        let mut writer = TableWriter::new("simple", &config)?;

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

mod fixture {
    use std::{collections::HashMap, env, fmt, time::{SystemTime, UNIX_EPOCH}};
    use rand::Rng;

    use record_persist_derive::Persist;
    
    pub fn get_tmp_folder() -> String {
        let mut path_buf = env::current_dir().unwrap();
        path_buf.push("target");
        path_buf.push("debug");
        path_buf.push("test");
        path_buf.into_os_string().into_string().expect("invalid path")
    }

    #[derive(Debug, Clone, Persist)]
    pub struct PriceLevel {
        pub price: f64,
        pub quantity: f64,
    }
    
    impl fmt::Display for PriceLevel {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "px: {:.2}, qty: {:.2}", self.price, self.quantity)
        }
    }

    pub type Trade = PriceLevel;

    #[derive(Debug, Clone, Persist)]
    pub struct OrderBook {
        pub exchange_id: u32,
        pub symbol_id: u32,
        pub buys: Vec<PriceLevel>,
        pub sells: Vec<PriceLevel>,
        pub tob: (PriceLevel, PriceLevel),
        pub trades: Vec<Trade>,
        pub properties: HashMap<String, String>,
        pub healthy: bool,
        pub stale: bool,
        pub ignore: bool,
        pub exchange_ts: u64,
        pub internal_ts: u64,
    }
    
    impl OrderBook {
        pub fn random_instance(exchange_id: u32, symbol_id: u32, mid_price: f64) -> Self {
            let mut rng = rand::thread_rng();
    
            let buy_quantities = (0..5)
                .map(|_| rng.gen_range(1.0..10.0))
                .collect::<Vec<f64>>();
            let sell_quantities = (0..5)
                .map(|_| rng.gen_range(1.0..10.0))
                .collect::<Vec<f64>>();

            let buys: Vec<PriceLevel> = buy_quantities
                .iter()
                .enumerate()
                .map(|(i, &quantity)| PriceLevel {
                    price: mid_price - (i + 1) as f64 * rng.gen_range(0.1..1.0),
                    quantity,
                })
                .collect();
            let sells: Vec<PriceLevel> = sell_quantities
                .iter()
                .enumerate()
                .map(|(i, &quantity)| PriceLevel {
                    price: mid_price + (i + 1) as f64 * rng.gen_range(0.1..1.0),
                    quantity,
                })
                .collect();
            let tob = (buys[0].clone(), sells[0].clone());
    
            let trades: Vec<Trade> = (0..rng.gen_range(1..=5))
            .map(|_| Trade {
                price: rng.gen_range(tob.0.price..=tob.1.price),
                quantity: rng.gen_range(1.0..10.0),
            })
            .collect();

            let mut properties = HashMap::new();
            properties.insert("service".to_string(), "test".to_string());
    
            let exchange_ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_nanos() as u64;
            let internal_ts = exchange_ts + rng.gen_range(1_000..100_000);

            OrderBook {
                exchange_id,
                symbol_id,
                buys,
                sells,
                tob,
                trades,
                properties,
                healthy: rng.gen_bool(0.5),
                stale: rng.gen_bool(0.5),
                ignore: rng.gen_bool(0.5),
                exchange_ts,
                internal_ts,
            }
        }

        pub fn tick(&self) -> Self {
            let mut rng = rand::thread_rng();
    
            let mid_price = ((self.tob.0.price + self.tob.1.price) / 2.0) + rng.gen_range(-0.5..0.5);
    
            let buys: Vec<PriceLevel> = self.buys.iter()
                .enumerate()
                .map(|(i, pl)| PriceLevel {
                    price: mid_price - i as f64 * rng.gen_range(0.1..1.0),
                    quantity: pl.quantity,
                })
                .collect();
            let sells: Vec<PriceLevel> = self.sells.iter()
                .enumerate()
                .map(|(i, pl)| PriceLevel {
                    price: mid_price + i as f64 * rng.gen_range(0.1..1.0),
                    quantity: pl.quantity,
                })
                .collect();
            let tob = (buys[0].clone(), sells[0].clone());

            let trades: Vec<Trade> = (0..rng.gen_range(1..=5))
            .map(|_| Trade {
                price: rng.gen_range(tob.0.price..=tob.1.price),
                quantity: rng.gen_range(1.0..10.0),
            })
            .collect();

            let exchange_ts = self.exchange_ts + rng.gen_range(1_000..10_000);
            let internal_ts = exchange_ts + rng.gen_range(1_000..100_000);

            OrderBook {
                exchange_id: self.exchange_id,
                symbol_id: self.symbol_id,
                buys,
                sells,
                tob,
                trades,
                properties: self.properties.clone(),
                healthy: self.healthy,
                stale: self.stale,
                ignore: self.ignore,
                exchange_ts,
                internal_ts,
            }
        }
    }

    impl fmt::Display for OrderBook {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "[exchange_id: {}, symbol_id: {}]\n\
                exchange_ts: {}\n\
                internal_ts: {}\n\
                top: ({}, {})\n\
                buys: [\n{}\n]\n\
                sells: [\n{}\n]\n\
                trades: [\n{}\n]",
                self.exchange_id,
                self.symbol_id,
                self.exchange_ts,
                self.internal_ts,
                self.tob.0.price,
                self.tob.1.price,
                self.buys.iter().map(|b| format!("  {}", b)).collect::<Vec<String>>().join(",\n"),
                self.sells.iter().map(|s| format!("  {}", s)).collect::<Vec<String>>().join(",\n"),
                self.trades.iter().map(|s| format!("  {}", s)).collect::<Vec<String>>().join(",\n"),
            )
        }
    }
}