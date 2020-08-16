#[macro_use]
extern crate lazy_static;
extern crate slog_term;
extern crate num_cpus;
extern crate slog_async;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use futures::TryStreamExt;
use helpers::parser::parse_message;
use helpers::reader::read_config;
use helpers::utils::OffsetRecord;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{Message, OwnedMessage};
use slog::*;

use std::time::Duration;

mod helpers;

lazy_static! {
    static ref LOG: slog::Logger = create_log();
}

fn create_log() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().force_plain().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

fn fetch_highwatermarks(config: ClientConfig, owned_message: OwnedMessage) {
    let key = owned_message.key().unwrap_or(&[]);
    let payload = owned_message.payload().unwrap_or(&[]);
    info!(LOG, "Fetching watermarks...");
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
                    info!(LOG, "{:?}", group_offset_lag)
                }
                Err(e) => {
                    warn!(LOG, "Error to process High Topic Watermarks, {}", e)
                }
            }
        }
        _ => (),
    }
}

async fn consume(config: ClientConfig) {
    let consumer: StreamConsumer = config.create().unwrap();
    info!(LOG, "Kafka config -> {:?}", config);
    consumer
        .subscribe(&["__consumer_offsets"])
        .expect("Can't subscribe to __consumer_offset topic. ERR");
    let stream_processor = consumer.start().try_for_each(|borrowed_message| {
        let owned_config = config.to_owned();
        async move {
            let owned_message = borrowed_message.detach();
            tokio::spawn(async move {
                tokio::task::spawn_blocking(|| fetch_highwatermarks(owned_config, owned_message))
                    .await
                    .expect("Failed to calculate lag");
            });
            Ok(())
        }
    });
    stream_processor.await.expect("stream processing failed");
}

#[tokio::main]
async fn main() {
    info!(LOG, "Starting the lag manager.");

    (0..num_cpus::get())
        .map(|_| tokio::spawn(consume(read_config())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}
