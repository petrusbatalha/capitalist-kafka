use crate::helpers::utils::{LagKey, LagPayload};
use rocksdb;
use std::sync::Arc;

lazy_static! {
    static ref ROCKS_DB: Arc<rocksdb::DB> =
        Arc::new(rocksdb::DB::open_default("/tmp/rocksdb".to_string()).unwrap());
}

pub fn put_lag(k: Vec<u8>, v: Vec<u8>) -> bool {
    ROCKS_DB.put(k, v).is_ok()
}

pub fn get_lag(lag: &LagKey) -> Option<String> {
    let key = bincode::serialize(&lag).unwrap();
    let value = ROCKS_DB.get(key);
    match value {
        Ok(Some(v)) => {
            let payload: LagPayload = bincode::deserialize(&v).unwrap();
            Some(payload.to_string())
        }
        _ => None,
    }
}
