use config::Value;
use rdkafka::config::ClientConfig;
use std::collections::HashMap;
extern crate config;

pub fn read_config() -> (ClientConfig, String) {
    let mut kafka_config = ClientConfig::new();
    let mut settings = config::Config::new();
    settings
        .merge(config::File::with_name("config/kafka_config.toml"))
        .unwrap();

    let topics = match settings.get_str("topics") {
        Ok(t) => t,
        Err(e) => panic!(e),
    };
    let settings = settings.try_into::<HashMap<String, Value>>().unwrap();

    for (config_key, config_value) in settings {
        if config_key == "topics" {
            continue;
        }
        match config_value.into_str() {
            Ok(v) => kafka_config.set(&config_key.replace("_", "."), &v),
            Err(_) => continue,
        };
    }
    (kafka_config, topics)
}
