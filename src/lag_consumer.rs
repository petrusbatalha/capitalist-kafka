extern crate bincode;
extern crate slog_async;
extern crate slog_term;
use crate::db_client::{DBClient, LagDB};
use rdkafka::config::ClientConfig;
use crate::config_reader::read;
use crate::parser::{parse_message, parse_date};
use crate::types::{Group, TopicPartition};
use futures::{join, TryStreamExt};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{Message, OwnedMessage};
use slog::*;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;

lazy_static! {
    static ref LOG: slog::Logger = create_log();
    pub static ref LAG_CONSUMER: LagConsumer = 
    LagConsumer {
        lag_db: Arc::new(
            LagDB {
                lag_db: rocksdb::DB::open_default("/tmp/rocksdb".to_string()).unwrap()
            }),
        config: read(),
    };
}

#[derive (Clone)]
pub struct LagConsumer {
    lag_db: Arc<LagDB>,
    config: ClientConfig,
}

impl LagConsumer {
    pub async fn consume(&'static self) {
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
                    tokio::task::spawn_blocking(move || self.push_group_data(owned_message))
                        .await
                        .expect("Failed to push commits");
                });
                Ok(())
            });
        stream_processor
            .await
            .expect("Failed to start stream consumer.");
    }
    
    fn push_group_data(&self, owned_message: OwnedMessage) {
        let timestamp = owned_message.timestamp();
        let key = owned_message.key().unwrap_or(&[]);
        let payload = owned_message.payload().unwrap_or(&[]);
        match parse_message(key, payload) {
            Ok(Group::OffsetCommit {
                group,
                topic,
                partition,
                offset,
            }) => {
                let group_key = Group::GroupKey {
                    group: group, 
                    topic_partition: TopicPartition {
                        topic,
                        partition
                    },
                };
                let group_payload = Group::GroupPayload {
                    group_offset: offset, 
                    commit_timestamp: parse_date(timestamp),
                };
                self.lag_db.put(
                    bincode::serialize(&group_key).unwrap(),
                    bincode::serialize(&group_payload).unwrap(),
                );
            }
            Err(e) => warn!(LOG, "Error to process High Topic Watermarks, {}", e),
            _ => (),
        }
    }
    
    async fn calculate_lag(&self) {
        info!(LOG, "CALCULATING LAG...");
        let groups_future = self.lag_db.get_all();
        let hwms_future = self.get_hwms();
        let result = join!(hwms_future, groups_future);
        let hwms = result.0;
        let groups = result.1;
        let mut all_lag: Vec<Group> = Vec::new();
        for (k, v) in groups {
            match (k, v) {
                (Group::GroupKey{
                    group,
                    topic_partition,
                }, Group::GroupPayload{
                    group_offset,
                    commit_timestamp
                }) => {
                    let topic_hwms = hwms.get(&topic_partition);
                    let group_lag = Group::GroupLag {
                        topic: topic_partition.topic,
                        partition: topic_partition.partition,
                        group: group,
                        lag: topic_hwms.unwrap() - group_offset,
                        last_commit: commit_timestamp,
                    };
                    all_lag.push(group_lag);
                    self.lag_db.put(
                        b"last_calculated_lag".to_vec(),
                        bincode::serialize(&all_lag).unwrap(),
                    );
                },
                _ => {},  
            }
        }
    }
    
    async fn get_hwms(&self) -> HashMap<TopicPartition, i64> {
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

    pub async fn lag_calc_update(&self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            self.calculate_lag().await;
        }
    }

    pub fn get_lag(&self) -> Option<Vec<Group>> {
        self.lag_db.get(b"last_calculated_lag".to_vec())
    }
}

fn create_log() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().force_plain().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}
