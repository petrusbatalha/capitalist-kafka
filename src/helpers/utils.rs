use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct LagKey {
    group: String,
    topic: String,
    partition: i32,
}

impl LagKey {
    pub fn new(group: String, topic: String, partition: i32) -> Self {
        LagKey {
            group: group,
            topic: topic,
            partition: partition,
        }
    }
}

impl fmt::Display for LagKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "group: {}, topic: {}, partition: {}",
            self.group, self.topic, self.partition
        )
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct LagPayload {
    group: String,
    topic: String,
    partition: i32,
    commit_timestamp: String,
    group_offset: i64,
    topic_offset: i64,
    lag: i64,
}

impl LagPayload {
    pub fn new(
        group: String,
        topic: String,
        partition: i32,
        commit_timestamp: String,
        group_offset: i64,
        topic_offset: i64,
        lag: i64,
    ) -> Self {
        LagPayload {
            group: group,
            topic: topic,
            partition: partition,
            commit_timestamp: commit_timestamp,
            group_offset: group_offset,
            topic_offset: topic_offset,
            lag: lag,
        }
    }
}

impl fmt::Display for LagPayload {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Group: {}, Topic: {}, Partition: {}, Commit Timestamp: {}, Group Offset: {}, Topic_offset: {}, lag: {}",
            self.group, self.topic, self.partition, self.commit_timestamp, self.group_offset, self.topic_offset, self.lag
        )
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
