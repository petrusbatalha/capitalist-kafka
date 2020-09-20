use crate::consumer_provider::{CONSUMER, get_timeout};
use crate::types::{Topic, Partition};
use rdkafka::consumer::Consumer;

pub fn fetch_topics() -> Option<Vec<Topic>> {
    match CONSUMER.fetch_metadata(None, get_timeout()) {
        Ok(metadata) => {
            let mut topics: Vec<Topic> = Vec::new();
            for topic in metadata.topics() {
                if topic.name().to_string().eq("__consumer_offsets") {
                    continue;
                }
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