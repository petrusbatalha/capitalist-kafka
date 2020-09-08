extern crate bincode;
use crate::types::Group;
use async_trait::async_trait;
use rocksdb::DB;

pub struct LagDB {
    pub lag_db: DB,
}

#[async_trait]
pub trait DBClient<T> {
    fn put(&self, k: Vec<u8>, v: Vec<u8>) -> bool;
    fn get(&self, k: Vec<u8>) -> Option<T>;
}

#[async_trait]
impl DBClient<Group> for LagDB {
    fn put(&self, k: Vec<u8>, v: Vec<u8>) -> bool {
        self.lag_db.put(k, v).is_ok()
    }

    fn get(&self, k: Vec<u8>) -> Option<Group> {
        let value = self.lag_db.get(k);
        match value {
            Ok(Some(v)) => {
                let payload: Group = bincode::deserialize(&v).unwrap();
                Some(payload)
            }
            _ => None,
        }
    }
}
