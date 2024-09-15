use crate::websocket::WSMessage;

#[derive(Clone, Debug)]
pub enum Record {
    Data { exchange: String, channel: String, data: String },
    Skip,
}

pub trait Recorded {
    fn subscribe(&self) -> Vec<WSMessage>;
    fn handle(&self, _data: &str) -> Record;
}

macro_rules! exchange {
    ($module_name:ident) => {
        pub mod $module_name {
            use crate::config::Config;

            pub struct Exchange {
                config: Config,
            }

            impl Exchange {
                pub fn new(config: Config) -> Self {
                    Exchange { config }
                }
            }
        }
    };
}

exchange!(coinbase);

impl Recorded for coinbase::Exchange {
    fn subscribe(&self) -> Vec<WSMessage> {
        todo!()
    }

    fn handle(&self, _data: &str) -> Record {
        todo!()
    }
}