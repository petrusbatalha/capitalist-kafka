use crate::consumer_provider::create_consumer;
use crate::types::{Topic, Partition};
use rdkafka::consumer::Consumer;
use std::time::Duration; 

pub fn fetch_topics() -> Option<Vec<Topic>> {
    match create_consumer().fetch_metadata(None, Duration::from_millis(100)) {
        Ok(metadata) => {
            let mut topics: Vec<Topic> = Vec::new();
            for topic in metadata.topics() {
                let mut partitions = Vec::with_capacity(topic.partitions().len());
                for partition in topic.partitions() {
                    partitions.push(Partition {
                        id: partition.id(),
                    })
                }
                topics.push(Topic {
                    name: topic.name().to_string(),
                    partitions: partitions,
                });
            }
            Some(topics)
        }
        _ => None,
    }
}