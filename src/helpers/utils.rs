use rdkafka::message::Timestamp;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

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
    GroupOffsetLag {
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
        lag: i64,
        timestamp: Timestamp,
    },
}
