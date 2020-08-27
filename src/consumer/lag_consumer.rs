extern crate bincode;
extern crate slog_async;
extern crate slog_term;
use crate::helpers::config_reader::read;
use crate::helpers::parser::parse_message;
use crate::helpers::utils::{LagKey, LagPayload, OffsetRecord};
use crate::store::lag_store::put_lag;
use chrono::prelude::*;
use chrono::Utc;
use futures::TryStreamExt;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Timestamp;
use rdkafka::message::{Message, OwnedMessage};
use slog::*;
use std::time::Duration;

lazy_static! {
    static ref LOG: slog::Logger = create_log();
}

fn create_log() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().force_plain().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

pub async fn consume() {
    let consumer: StreamConsumer = read().create().unwrap();
    consumer
        .subscribe(&["__consumer_offsets"])
        .expect("Can't subscribe to __consumer_offset topic. ERR");

    let stream_processor = consumer
        .start()
        .try_for_each(|borrowed_message| async move {
            info!(LOG, "Fetching watermarks...");
            let owned_message = borrowed_message.detach();
            tokio::spawn(async move {
                tokio::task::spawn_blocking(|| push_group_data(owned_message))
                    .await
                    .expect("Failed to calculate lag");
            });
            Ok(())
        });
    stream_processor
        .await
        .expect("Failed to start stream consumer.");
}

pub async fn fetch_watermarks() {
    let watermark_consumer: StreamConsumer = read().create().unwrap();
    let metadata = watermark_consumer.fetch_metadata(None, Duration::from_secs(1)).expect("errou");
    for topic in metadata.topics() {
        let partitions: i32 = topic.partitions().len() as i32;
        for p in 0..partitions {
            let hwms =
                watermark_consumer.fetch_watermarks(topic.name(), p, Duration::from_millis(100)).unwrap();
            println!(
                "Topic {}, Partition: {} , HWMS: {}",
                topic.name(),
                p,
                hwms.1
            );
        }
    }
}

fn push_group_data(owned_message: OwnedMessage) {
    let timestamp = owned_message.timestamp();
    let key = owned_message.key().unwrap_or(&[]);
    let payload = owned_message.payload().unwrap_or(&[]);
    match parse_message(key, payload) {
        Ok(OffsetRecord::OffsetCommit {
            group,
            topic,
            partition,
            offset,
        }) => {
            let group_key = LagKey::new(group, topic, partition);
            let group_payload = LagPayload::new(offset, parse_date(timestamp));
            put_lag(bincode::serialize(&group_key).unwrap(), bincode::serialize(&group_payload).unwrap());
        },
        Err(e) => warn!(LOG, "Error to process High Topic Watermarks, {}", e),
        _ => (),
    }
}


fn parse_date(timestamp: Timestamp) -> String {
    let t = timestamp.to_millis();
    let naive = NaiveDateTime::from_timestamp(t.unwrap() / 1000, 0);
    let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}
