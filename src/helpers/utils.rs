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
    group_offset: i64,
    commit_timestamp: String,
}

impl LagPayload {
    pub fn new(
        group_offset: i64,
        commit_timestamp: String,
    ) -> Self {
        LagPayload {
            group_offset: group_offset,
            commit_timestamp: commit_timestamp,
        }
    }
}

impl fmt::Display for LagPayload {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Group Offset: {}, Commit Timestamp: {}, ",
            self.group_offset, self.commit_timestamp
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
