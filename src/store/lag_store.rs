extern crate bincode;
use crate::helpers::utils::{GroupKey, GroupLag, GroupPayload};
use rocksdb::IteratorMode;
use std::collections::HashMap;
use std::sync::Arc;

lazy_static! {
    static ref ROCKS_DB: Arc<rocksdb::DB> =
        Arc::new(rocksdb::DB::open_default("/tmp/rocksdb".to_string()).unwrap());
}

pub type Result<T> = std::result::Result<T, std::boxed::Box<bincode::ErrorKind>>;

pub fn put(k: Vec<u8>, v: Vec<u8>) -> bool {
    ROCKS_DB.put(k, v).is_ok()
}

pub fn get(k: Vec<u8>) -> Option<Vec<GroupLag>> {
    let value = ROCKS_DB.get(k);
    match value {
        Ok(Some(v)) => {
            let payload: Vec<GroupLag> = bincode::deserialize(&v).unwrap();
            Some(payload)
        }
        _ => None,
    }
}

pub async fn get_all_groups() -> HashMap<GroupKey, GroupPayload> {
    let iter = ROCKS_DB.iterator(IteratorMode::Start);
    let mut group_map: HashMap<GroupKey, GroupPayload> = HashMap::new();
    for (group_key, group_payload) in iter {
        let key: Result<GroupKey> = bincode::deserialize(&group_key);
        let payload: Result<GroupPayload> = bincode::deserialize(&group_payload);
        let key: Option<GroupKey> = match key {
            Ok(k) => Some(k),
            Err(_) => None,
        };
        let payload: Option<GroupPayload> = match payload {
            Ok(p) => Some(p),
            Err(_) => None,
        };
        match (key, payload) {
            (Some(k), Some(p)) => group_map.insert(k, p),
            _ => None,
        };
    }
    group_map
}
