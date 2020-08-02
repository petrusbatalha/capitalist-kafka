use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use std::boxed::Box;
use std::convert::TryInto;
use std::time::Duration;
mod config_reader;

pub struct Topic {
    pub name: String,
    pub partition_high_watermarks: Vec<(i32, i64)>,
}

fn fetch_topics_highwatermarks(consumer: &StreamConsumer) -> Result<Vec<Topic>, Box<dyn std::error::Error>> {
    let mut topics: Vec<Topic> = Vec::new();
    let metadata = &consumer.fetch_metadata(None, Duration::from_secs(1))?;
    for t in metadata.topics() {
        let mut partition_high_watermarks: Vec<(i32, i64)> = Vec::new();
        let partitions: i32 = t.partitions().len().try_into().unwrap();
        for p in 0..partitions {
            let high_watermarks =
                &consumer.fetch_watermarks(t.name(), p, Duration::from_secs(1))?;
            partition_high_watermarks.push((p, high_watermarks.1));
        }
        let topic = Topic {
            name: t.name().to_string(),
            partition_high_watermarks: partition_high_watermarks,
        };
        &topics.push(topic);
    }
    Ok(topics)
}

fn main() -> Result<(), Box<dyn std::error::Error>>{
    let config = config_reader::read_config();
    let consumer: StreamConsumer = config.0.create().unwrap();
    &consumer.start();
    let topics_watermarks = fetch_topics_highwatermarks(&consumer)?;
    
    println!("{:?}", &topics_watermarks.len());
    Ok(())
}
