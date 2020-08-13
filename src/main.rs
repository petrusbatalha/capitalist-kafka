use futures::stream::FuturesUnordered;
use futures::{StreamExt};
use helpers::parser::parse_message;
use helpers::reader::read_config;
use helpers::utils::{ConsumerUpdate, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{Message, OwnedMessage};
use std::collections::HashMap;
use std::convert::TryInto;
use std::time::Duration;
mod helpers;

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ConsumerOffsetKey {
    group: String,
    topic: String,
    partition: i32,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TopicPartitionKey {
    topic: String,
    partition: i32,
}

async fn fetch_topics_highwatermarks(
    config: (ClientConfig, String),
) -> Result<HashMap<TopicPartitionKey, i64>> {
    let consumer: StreamConsumer = config.0.create().unwrap();
    let mut topics: HashMap<TopicPartitionKey, i64> = HashMap::new();
    let metadata = &consumer
        .fetch_metadata(None, Duration::from_secs(1))
        .unwrap();
    for t in metadata.topics() {
        let topic = t.name().to_string();
        let partitions: i32 = t.partitions().len().try_into().unwrap();
        for p in 0..partitions {
            let high_watermarks = &consumer
                .fetch_watermarks(t.name(), p, Duration::from_secs(1))
                .unwrap();
            let topic_partition_key = TopicPartitionKey {
                topic: topic.clone(),
                partition: p,
            };
            topics.insert(topic_partition_key, high_watermarks.1);
        }
    }
    Ok(topics)
}

async fn fetch_highwatermarks(config: (ClientConfig, String), owned_message: OwnedMessage) -> Result<ConsumerUpdate> {
    let key = owned_message.key().unwrap_or(&[]);
    let payload = owned_message.payload().unwrap_or(&[]);
    match parse_message(key, payload) {
        Ok(ConsumerUpdate::OffsetCommit {
            group,
            topic,
            partition,
            offset,
        }) => {
            let consumer: StreamConsumer = config.0.create().unwrap();
            let high_watermarks = &consumer
                .fetch_watermarks(&topic, partition, Duration::from_secs(1))
                .unwrap();
            Ok(ConsumerUpdate::GroupOffsetLag {
                group: group,
                topic: topic,
                partition: partition,
                offset: offset,
                lag: high_watermarks.1 - offset,
            })
        },
        Ok(_) => Ok(ConsumerUpdate::Metadata),
        Err(e) => return Err(e),
    }
}

async fn consume(config: (ClientConfig, String)) {
    let consumer: StreamConsumer = config.0.create().unwrap();
    consumer
        .subscribe(&["__consumer_offsets"])
        .expect("Can't subscribe to specified topic"); 
    while let Some(message) = consumer.start().next().await {
        match message {
            Ok(message) => {
                let owned_config = config.to_owned();
                let owned_message = message.detach();
                let lag = tokio::task::spawn_blocking(|| fetch_highwatermarks(owned_config, owned_message)).await.expect("nao foi possivel calcular o lag");
                println!("{:?}", lag.await);
            },
            Err(e) => println!("{:?}", e)
        }
    }
}

#[tokio::main]
async fn main() {
    let config = read_config();
    (0..2 as usize)
        .map(|_| tokio::spawn(consume(config.clone())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}