pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug)]
pub enum ConsumerUpdate {
    Metadata,
    OffsetCommit {
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
    },
    OffsetTombstone {
        group: String,
        topic: String,
        partition: i32,
    },
}