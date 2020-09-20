use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct GroupMember {
    pub id: String,
    pub client_id: String,
    pub client_host: String,
    #[serde(default)]
    pub assignments: Vec<MemberAssignment>,
}

#[derive(Serialize, Deserialize)]
pub struct Topic {
    pub name: String,
    pub partitions: Vec<Partition>,
}

#[derive(Serialize, Deserialize)]
pub struct Partition {
    pub id: i32,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct Group {
    pub name: String,
    pub state: String,
    pub members: Vec<GroupMember>,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct MemberAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(Eq, Serialize, Deserialize, PartialEq, Debug)]
pub struct Lag {
    pub partition: i32,
    pub lag: i64,
    pub timestamp: String,
}

#[derive(Eq, Serialize, Deserialize, PartialEq, Debug)]
pub enum GroupData {
    GroupKey {
        group: String,
    },
    GroupPayload {
        topic: String,
        payload: HashMap<i32, (i64, String)>,
    },
    GroupLag {
        topic: String,
        group: String,
        lag: Vec<Lag>,
    },
    OffsetCommit {
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
    },
    None,
}
