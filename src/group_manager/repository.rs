use crate::db_client::{get_db, DBClient};
use crate::types::GroupData;
use async_trait::async_trait;

pub struct GroupDB {}

#[async_trait]
impl DBClient<GroupData> for GroupDB {
    fn put(k: Vec<u8>, v: Vec<u8>) -> bool {
        get_db().put(k, v).is_ok()
    }

    fn get(k: Vec<u8>) -> Option<GroupData> {
        let value = get_db().get(k);
        match value {
            Ok(Some(v)) => {
                let payload: GroupData = bincode::deserialize(&v).unwrap();
                Some(payload)
            }
            _ => None,
        }
    }
}
