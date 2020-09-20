extern crate config;
use crate::utils::logger::create_log;
use config::Value;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use std::collections::HashMap;
use std::sync::Arc;

lazy_static! {
    static ref CURRENT_CONFIG: Arc<ClientConfig> = Arc::new(read());
    static ref LOG: slog::Logger = create_log();
}
  
pub fn create_consumer() -> StreamConsumer {
    CURRENT_CONFIG.clone().create().unwrap()
}

fn read() -> ClientConfig {
    let mut kafka_config = ClientConfig::new();
    let mut settings = config::Config::new();
    settings
        .merge(config::File::with_name("config/local.toml"))
        .unwrap();

    let settings = settings.try_into::<HashMap<String, Value>>().unwrap();

    for (config_key, config_value) in settings {
        match config_value.into_str() {
            Ok(v) => kafka_config.set(&config_key.replace("_", "."), &v),
            Err(_) => continue,
        };
    }
    kafka_config.set("enable.auto.commit", "false");
    kafka_config
}
