use rdkafka::config::ClientConfig;
use std::collections::HashMap;
extern crate config;

pub fn read_config() -> ClientConfig {
    let mut kafka_config = ClientConfig::new();
    let mut settings = config::Config::default();
    settings.merge(config::File::with_name("kafka_config.toml")).unwrap();
    
    for (config_key, config_value) in &settings.try_into::<HashMap<String, String>>().unwrap() {
        kafka_config.set(&config_key.replace("_", "."), &config_value);
    };
    return kafka_config;
}