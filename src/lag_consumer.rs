extern crate bincode;
extern crate slog_async;
extern crate slog_term;
use crate::config_reader::read;
use crate::db_client::{DBClient, LagDB};
use crate::parser::{parse_date, parse_member_assignment, parse_message};
use crate::types::{Group, GroupData, GroupMember, Lag, Partition, Topic};
use futures::TryStreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{Message, OwnedMessage};
use slog::*;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

lazy_static! {
    static ref LOG: slog::Logger = create_log();
    static ref METADATA_CONSUMER: StreamConsumer = read().create().unwrap();
    pub static ref LAG_CONSUMER: LagConsumer = LagConsumer {
        lag_db: Arc::new(LagDB {
            lag_db: rocksdb::DB::open_default("/tmp/rocksdb".to_string()).unwrap()
        }),
        config: read(),
    };
}

#[derive(Clone)]
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
                info!(LOG, "Consuming messages...");
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
            Ok(GroupData::OffsetCommit {
                group,
                topic,
                partition,
                offset,
            }) => {
                let group_key = GroupData::GroupKey { group: group };
                let serialized_group_key = bincode::serialize(&group_key).unwrap();
                let group_payload: Option<GroupData> = match self.lag_db.get(serialized_group_key) {
                    Some(GroupData::GroupPayload { mut payload, topic }) => {
                        payload.insert(partition, (offset, parse_date(timestamp)));
                        Some(GroupData::GroupPayload { topic, payload })
                    }
                    None => {
                        let mut map: HashMap<i32, (i64, String)> = HashMap::new();
                        map.insert(partition, (offset, parse_date(timestamp)));
                        Some(GroupData::GroupPayload {
                            topic: topic,
                            payload: map,
                        })
                    }
                    _ => None,
                };
                match group_payload {
                    Some(payload) => {
                        self.lag_db.put(
                            bincode::serialize(&group_key).unwrap(),
                            bincode::serialize(&payload).unwrap(),
                        );
                    }
                    _ => (),
                }
            }
            _ => (),
        }
    }

    pub fn get_lag(&self, group: &str) -> Option<GroupData> {
        match self.lag_db.get(
            bincode::serialize(&GroupData::GroupKey {
                group: group.to_string(),
            })
            .unwrap(),
        ) {
            Some(GroupData::GroupPayload { payload, topic }) => {
                let mut partitions_lag: Vec<Lag> = Vec::new();
                for (partition, value) in payload {
                    let hwms = self.get_hwms(topic.clone(), partition);
                    let partition_lag = Lag {
                        partition: partition,
                        lag: hwms.1 - value.0,
                        timestamp: value.1,
                    };
                    partitions_lag.push(partition_lag);
                }
                Some(GroupData::GroupLag {
                    group: group.to_string(),
                    topic: topic,
                    lag: partitions_lag,
                })
            }
            _ => None,
        }
    }

    fn get_hwms(&self, topic: String, partition: i32) -> (i64, i64) {
        METADATA_CONSUMER
            .fetch_watermarks(&topic, partition, Duration::from_millis(100))
            .unwrap_or((-1, -1))
    }

    pub fn fetch_groups(&self) -> Option<Vec<Group>> {
        match METADATA_CONSUMER.fetch_group_list(None, Duration::from_millis(100)) {
            Ok(group_list) => {
                let mut groups: Vec<Group> = Vec::new();
                for g in group_list.groups() {
                    let members: Vec<GroupMember> = g
                        .members()
                        .iter()
                        .map(|m| {
                            let mut assigns = Vec::new();
                            if let Some(assignment) = m.assignment() {
                                let mut payload_rdr = Cursor::new(assignment);
                                assigns = parse_member_assignment(&mut payload_rdr)
                                    .expect("Parse member assignment failed");
                            };
                            GroupMember {
                                id: m.id().to_string(),
                                client_id: m.client_id().to_string(),
                                client_host: m.client_host().to_string(),
                                assignments: assigns,
                            }
                        })
                        .collect::<Vec<_>>();
                    groups.push(Group {
                        name: g.name().to_string(),
                        state: g.state().to_string(),
                        members: members,
                    });
                }
                Some(groups)
            }
            _ => None,
        }
    }

    pub fn fetch_topics(&self) -> Option<Vec<Topic>> {
        match METADATA_CONSUMER.fetch_metadata(None, Duration::from_millis(100)) {
            Ok(metadata) => {
                let mut topics: Vec<Topic> = Vec::new();
                for topic in metadata.topics() {
                    let mut partitions = Vec::with_capacity(topic.partitions().len());
                    for partition in topic.partitions() {
                        partitions.push(Partition {
                            id: partition.id(),
                        })
                    }
                    topics.push(Topic {
                        name: topic.name().to_string(),
                        partitions: partitions,
                    });
                }
                Some(topics)
            }
            _ => None,
        }
    }
}

fn create_log() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().force_plain().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}
