use std::collections::HashMap;
extern crate bincode;
use crate::types::Group;
use async_trait::async_trait;
use rocksdb::IteratorMode;
use rocksdb::DB;

pub type Result<T> = std::result::Result<T, std::boxed::Box<bincode::ErrorKind>>;

pub struct LagDB {
    pub lag_db: DB,
}

#[async_trait]
pub trait DBClient<T> {
    fn put(&self, k: Vec<u8>, v: Vec<u8>) -> bool;
    fn get(&self, k: Vec<u8>) -> Option<Vec<T>>;
    async fn get_all(&self) -> HashMap<T, T>;
}

#[async_trait]
impl DBClient<Group> for LagDB {
    fn put(&self, k: Vec<u8>, v: Vec<u8>) -> bool {
        self.lag_db.put(k, v).is_ok()
    }

    fn get(&self, k: Vec<u8>) -> Option<Vec<Group>> {
        let value = self.lag_db.get(k);
        match value {
            Ok(Some(v)) => {
                let payload: Vec<Group> = bincode::deserialize(&v).unwrap();
                Some(payload)
            }
            _ => None,
        }
    }

    async fn get_all(&self) -> HashMap<Group, Group> {
        let iter = self.lag_db.iterator(IteratorMode::Start);
        let mut group_map: HashMap<Group, Group> = HashMap::new();
        for (group_key, group_payload) in iter {
            let key: Result<Group> = bincode::deserialize(&group_key);
            let payload: Result<Group> = bincode::deserialize(&group_payload);
            let key: Option<Group> = match key {
                Ok(k) => Some(k),
                Err(_) => None,
            };
            let payload: Option<Group> = match payload {
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
}
