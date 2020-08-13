extern crate num_cpus;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use futures::TryStreamExt;
use helpers::parser::parse_message;
use helpers::reader::read_config;
use helpers::utils::OffsetRecord;
use log::{info, warn};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{Message, OwnedMessage};
use std::time::Duration;
mod helpers;

fn fetch_highwatermarks(config: ClientConfig, owned_message: OwnedMessage) {
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
            match &consumer.fetch_watermarks(&topic, partition, Duration::from_millis(1000)) {
                Ok(hwms) => {
                    let group_offset_lag = OffsetRecord::GroupOffsetLag {
                        group: group,
                        topic: topic,
                        partition: partition,
                        offset: offset,
                        lag: hwms.1 - offset,
                    };
                    println!("{:?}", group_offset_lag)
                }
                Err(e) => warn!("Error to process High Topic Watermarks, {:?}", e),
            }
        }
        _ => (),
    }
}

async fn consume(config: ClientConfig) {
    let consumer: StreamConsumer = config.create().unwrap();
    consumer.subscribe(&["__consumer_offsets"])
            .expect("Can't subscribe to __consumer_offset topic. ERR: {}");
    let stream_processor = consumer.start().try_for_each(|borrowed_message| {
        let owned_config = config.to_owned();
        async move {
            let owned_message = borrowed_message.detach();
            tokio::spawn(async move {
                tokio::task::spawn_blocking(|| fetch_highwatermarks(owned_config, owned_message)).await.expect("Failed to calculate lag");
            });
            Ok(())
        }
    });
    info!("Starting event loop");
    stream_processor.await.expect("stream processing failed");
    info!("Stream processing terminated");
}

#[tokio::main]
async fn main() {
    (0..num_cpus::get())
        .map(|_| tokio::spawn(consume(read_config())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}
