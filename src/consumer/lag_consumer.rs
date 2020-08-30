extern crate bincode;
extern crate slog_async;
extern crate slog_term;
use crate::helpers::config_reader::read;
use crate::helpers::parser::parse_message;
use crate::helpers::utils::{GroupKey, GroupLag, GroupPayload, OffsetRecord, TopicPartition};
use crate::store::lag_store::{get_all_groups, put};
use chrono::prelude::*;
use chrono::Utc;
use futures::{join, TryStreamExt};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Timestamp;
use rdkafka::message::{Message, OwnedMessage};
use slog::*;
use std::collections::HashMap;
use std::sync::Arc;
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
            let group_key = GroupKey::new(group, TopicPartition::new(topic, partition));
            let group_payload = GroupPayload::new(offset, parse_date(timestamp));
            put(
                bincode::serialize(&group_key).unwrap(),
                bincode::serialize(&group_payload).unwrap(),
            );
        }
        Err(e) => warn!(LOG, "Error to process High Topic Watermarks, {}", e),
        _ => (),
    }
}

pub async fn calculate_lag() {
    info!(LOG, "CALCULATING MOTHERFUCKING LAG...");
    let groups_future = get_all_groups();
    let hwms_future = get_hwms();
    let result = join!(hwms_future, groups_future);
    let hwms = result.0;
    let groups = result.1;
    let mut all_lag: Vec<GroupLag> = Vec::new();
    for (k, v) in groups {
        let topic_hwms = hwms.get(&k.topic_partition);
        let group_lag = GroupLag {
            topic: k.topic_partition.topic,
            partition: k.topic_partition.partition,
            group: k.group,
            lag: topic_hwms.unwrap() - v.group_offset,
            last_commit: v.commit_timestamp,
        };
        all_lag.push(group_lag);
    }
    put(
        b"last_calculated_lag".to_vec(),
        bincode::serialize(&all_lag).unwrap(),
    );
}

async fn get_hwms() -> HashMap<TopicPartition, i64> {
    let watermark_consumer: StreamConsumer = read().create().unwrap();
    let metadata = &watermark_consumer
        .fetch_metadata(None, Duration::from_secs(1))
        .expect("errou");
    let mut topics: HashMap<TopicPartition, i64> = HashMap::new();
    for topic in metadata.topics() {
        let partitions_count: i32 = topic.partitions().len() as i32;
        for p in 0..partitions_count {
            let hwms = watermark_consumer
                .fetch_watermarks(&topic.name(), p, Duration::from_millis(100))
                .unwrap();
            topics.insert(TopicPartition::new(topic.name().to_string(), p), hwms.1);
        }
    }
    topics
}

fn parse_date(timestamp: Timestamp) -> String {
    let t = timestamp.to_millis();
    let naive = NaiveDateTime::from_timestamp(t.unwrap() / 1000, 0);
    let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}
