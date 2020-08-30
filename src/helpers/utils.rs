use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Serialize, Deserialize)]
pub struct AllLag {
    #[serde(rename = "GroupLag")]
    group_lag: Vec<GroupLag>,
}

#[derive(Serialize, Deserialize, Hash, Eq, PartialEq, Debug)]
pub struct GroupLag {
    pub topic: String,
    pub partition: i32,
    pub group: String,
    pub lag: i64,
    pub last_commit: String,
}

impl fmt::Display for GroupLag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Topic: {}, 
            Partition: {},
            Group: {},
            Lag: {},
            Last Commit: {}",
            self.topic, self.partition, self.group, self.lag, self.last_commit
        )
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

#[derive(Eq, Hash, Serialize, Deserialize, PartialEq, Debug)]
pub struct GroupKey {
    pub group: String,
    pub topic_partition: TopicPartition,
}

impl GroupKey {
    pub fn new(group: String, topic_partition: TopicPartition) -> Self {
        GroupKey {
            group: group,
            topic_partition: topic_partition,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct GroupPayload {
    pub group_offset: i64,
    pub commit_timestamp: String,
}

impl GroupPayload {
    pub fn new(group_offset: i64, commit_timestamp: String) -> Self {
        GroupPayload {
            group_offset: group_offset,
            commit_timestamp: commit_timestamp,
        }
    }
}

#[derive(Debug)]
pub enum OffsetRecord {
    Metadata,
    OffsetTombstone {
        group: String,
        topic: String,
        partition: i32,
    },
    OffsetCommit {
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
    },
}
