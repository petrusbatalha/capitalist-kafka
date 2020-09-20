extern crate bincode;
use super::repository::GroupDB;
use crate::db_client::DBClient;
use crate::parser::{parse_date, parse_member_assignment, parse_message};
use crate::types::{Group, GroupData, GroupMember, Lag};
use crate::utils::logger::create_log;
use crate::consumer_provider::{create_consumer, get_timeout, CONSUMER};
use slog::{info};
use futures::TryStreamExt;
use rdkafka::consumer::Consumer;
use rdkafka::message::{Message, OwnedMessage};
use std::collections::HashMap;
use std::io::Cursor;

lazy_static! {
    static ref LOG: slog::Logger = create_log();
}

pub async fn start() {
    let consumer = create_consumer();
    consumer
        .subscribe(&["__consumer_offsets"])
        .expect("Can't subscribe to __consumer_offset topic. ERR");

    let stream_processor = consumer
        .start()
        .try_for_each(|borrowed_message| async move {
            info!(LOG, "Consuming messages...");
            let owned_message = borrowed_message.detach();
            tokio::spawn(async move {
                tokio::task::spawn_blocking(move || push_group_data(owned_message))
                    .await
                    .expect("Failed to push commits");
            });
            Ok(())
        });
    stream_processor
        .await
        .expect("Failed to start stream consumer.");
}

pub fn get_lag(group: &str) -> Option<GroupData> {
    match GroupDB::get(
        bincode::serialize(&GroupData::GroupKey {
            group: group.to_string(),
        })
        .unwrap(),
    ) {
        Some(GroupData::GroupPayload { payload, topic }) => {
            let mut partitions_lag: Vec<Lag> = Vec::new();
            for (partition, value) in payload {
                let hwms = get_hwms(topic.clone(), partition);
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

pub fn fetch_groups() -> Option<Vec<Group>> {
    match CONSUMER.fetch_group_list(None, get_timeout()) {
        Ok(group_list) => {
            let mut groups: Vec<Group> = Vec::new();
            for g in group_list.groups() {
                if g.name().to_string().eq("manager_local") {
                    continue;
                }
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

fn push_group_data(owned_message: OwnedMessage) {
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
            let group_payload: Option<GroupData> = match GroupDB::get(serialized_group_key) {
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
                    GroupDB::put(
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

fn get_hwms(topic: String, partition: i32) -> (i64, i64) {
    CONSUMER
        .fetch_watermarks(&topic, partition, get_timeout())
        .unwrap_or((-1, -1))
}