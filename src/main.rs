use rdkafka::config::ClientConfig;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Config {
    bootstrap_servers: String,
    security_protocol: String,
    sasl_mechanism: String,
    sasl_username: String,
    sasl_password: String,
    topic: String,
}

fn read_config() -> (ClientConfig, String) {
    let mut kafka_config = ClientConfig::new();
    match envy::from_env::<Config>() {
        Ok(config) => {
            kafka_config.set("bootstrap.servers", &config.bootstrap_servers);
            kafka_config.set("security.protocol", &config.security_protocol);
            kafka_config.set("sasl.mechanism", &config.sasl_mechanism);
            kafka_config.set("sasl.username", &config.sasl_username);
            kafka_config.set("sasl.password", &config.sasl_password);
            (kafka_config, config.topic)
        }
        Err(error) => panic!("Couldn't read LANG ({})", error),
    }
}

fn main() {
    println!("Config baby {:?}", read_config().0);
    println!("TOPICO {:?}", read_config().1);
}
