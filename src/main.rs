extern crate num_cpus;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use helpers::parser::parse_message;
use helpers::reader::read_config;
use helpers::utils::{OffsetRecord, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{Message, OwnedMessage};
use std::time::Duration;
mod helpers;

async fn fetch_highwatermarks(
    config: ClientConfig,
    owned_message: OwnedMessage,
) -> Result<OffsetRecord> {
    let key = owned_message.key().unwrap_or(&[]);
    let payload = owned_message.payload().unwrap_or(&[]);
    match parse_message(key, payload) {
        Ok(OffsetRecord::OffsetCommit {
            group,
            topic,
            partition,
            offset,
        }) => {
            let consumer: StreamConsumer = config.create().unwrap();
            let high_watermarks = &consumer
                .fetch_watermarks(&topic, partition, Duration::from_millis(1000))
                .expect("TIME OUT FETCHING WATERMARKS");
            Ok(OffsetRecord::GroupOffsetLag {
                group: group,
                topic: topic,
                partition: partition,
                offset: offset,
                lag: high_watermarks.1 - offset,
            })
        }
        Ok(_) => Ok(OffsetRecord::Metadata),
        Err(e) => return Err(e),
    }
}

async fn consume(config: ClientConfig) {
    let consumer: StreamConsumer = config.create().unwrap();
    consumer
        .subscribe(&["__consumer_offsets"])
        .expect("Can't subscribe to specified topic");
    while let Some(message) = consumer.start().next().await {
        match message {
            Ok(message) => {
                let owned_config = config.to_owned();
                let owned_message = message.detach();
                let lag = tokio::task::spawn_blocking(|| {
                    fetch_highwatermarks(owned_config, owned_message)
                }).await.expect("Impossible to calculate lag.");
                println!("{:?}", lag.await);
            }
            Err(e) => println!("{:?}", e),
        }
    }
}

#[tokio::main]
async fn main() {
    (0..num_cpus::get() as usize)
        .map(|_| tokio::spawn(consume(read_config())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}
