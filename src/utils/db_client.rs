extern crate bincode;
use async_trait::async_trait;
use rocksdb::DB;
use std::sync::{Arc};

lazy_static! {
    static ref ROCKS_DB: Arc<DB> = Arc::new(
        rocksdb::DB::open_default("./rocksdb".to_string()).unwrap()
    );
}

pub fn get_db() -> Arc<DB> {
    ROCKS_DB.clone()
}

#[async_trait]
pub trait DBClient<T> {
    fn put(k: Vec<u8>, v: Vec<u8>) -> bool;
    fn get(k: Vec<u8>) -> Option<T>;
}
