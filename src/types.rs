use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Eq, Hash, Serialize, Deserialize, PartialEq, Debug)]
pub enum Group {
    GroupKey {
        group: String,
        topic_partition: TopicPartition,
    },
    GroupPayload {
        group_offset: i64,
        commit_timestamp: String,
    },
    GroupLag {
        topic: String,
        partition: i32,
        group: String,
        lag: i64,
        last_commit: String,
    },
    OffsetCommit {
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
    },
    None,
}

impl fmt::Display for Group {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Group::GroupLag {
                topic,
                partition,
                group,
                lag,
                last_commit,
            } => write!(
                f,
                "Topic: {}, 
                    Partition: {},
                    Group: {},
                    Lag: {},
                    Last Commit: {}",
                topic, partition, group, lag, last_commit
            ),
            _ => Ok(()),
        }
    }
}

#[derive(Serialize, Deserialize, Hash, Eq, PartialEq, Debug)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: String, partition: i32) -> Self {
        TopicPartition {
            topic: topic,
            partition: partition,
        }
    }
}
