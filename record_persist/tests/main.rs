#[cfg(test)]
mod tests {
    use anyhow::{Ok, Result};
    use std::collections::HashSet;
    
    use record_persist::{config::PersistConfig, writer::TableWriter};

    use crate::fixture::{get_tmp_folder, Simple};


    #[test]
    fn test_random_simple_instance() {
        let instance = Simple::random_instance();

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
        for _ in 0..5 {
            let instance = Simple::random_instance();
            writer.begin()?.record(&instance)?.end()?;
            writer.flush_if_needed()?;
        }

        writer.flush()?;

        Ok(())
    }
}

mod fixture {
    use std::{collections::HashMap, env};
    use rand::Rng;

    use record_persist_derive::Persist;
    
    pub fn get_tmp_folder() -> String {
        let mut path_buf = env::current_dir().unwrap();
        path_buf.push("target");
        path_buf.push("debug");
        path_buf.push("test");
        path_buf.into_os_string().into_string().expect("invalid path")
    }

    #[derive(Debug, Clone, Default, Persist)]
    pub struct Simple {
        pub exchange_id: u32,
        pub symbol_id: u32,
        pub buys: Vec<f64>,
        pub sells: Vec<f64>,
        pub tob: (f64, f64),

        pub properties: HashMap<String, String>,

        pub healthy: bool,
        #[persist(ignore = false)]
        pub stale: bool,
        #[persist(ignore = true)]
        pub ignore: bool,

        #[persist_timestamp(unit = "ns")]
        pub internal_ts: u64,
        #[persist_timestamp(unit = "ns")]
        pub exchange_ts: Option<u64>,
    }

    impl Simple {
        pub fn random_instance() -> Self {
            let mut rng = rand::thread_rng();
            let buys = (0..rng.gen_range(1..5)).map(|_| rng.gen_range(0.0..100.0)).collect();
            let sells = (0..rng.gen_range(1..5)).map(|_| rng.gen_range(0.0..100.0)).collect();
    
            let mut properties = HashMap::new();
            properties.insert("property1".to_string(), "value1".to_string());
            properties.insert("property2".to_string(), "value2".to_string());
    
            Simple {
                exchange_id: rng.gen_range(1..1000),
                symbol_id: rng.gen_range(1..1000),
                buys,
                sells,
                tob: (rng.gen_range(0.0..100.0), rng.gen_range(0.0..100.0)),
                properties,
                healthy: rng.gen_bool(0.5),
                stale: rng.gen_bool(0.5),
                ignore: rng.gen_bool(0.5),
                internal_ts: rng.gen_range(1_000_000..1_000_000_000),
                exchange_ts: if rng.gen_bool(0.5) {
                    Some(rng.gen_range(1_000_000..1_000_000_000))
                } else {
                    None
                },
            }
        }
    }
}