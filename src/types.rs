use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Eq, Serialize, Deserialize, PartialEq, Debug)]
pub struct Lag {
    pub partition: i32,
    pub lag: i64,
    pub timestamp: String,
}

#[derive(Eq, Serialize, Deserialize, PartialEq, Debug)]
pub enum Group {
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
