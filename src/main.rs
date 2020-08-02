use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use std::convert::TryInto;
use std::time::Duration;
use std::boxed::Box;
mod config_reader;

pub type TopicName = String;
pub type TopicPartition = (TopicName, i32);
pub type BrokerId = i32;

fn main() -> Result<(),Box<dyn std::error::Error>> {
    let config = config_reader::read_config();
    let consumer: StreamConsumer = config.0.create().expect("ERRITO");

    //consumer.subscribe(&vec![config.1.as_str()].as_slice());
    consumer.start();
    //let watermarks = consumer.fetch_watermarks(Some(""), 1, Duration::from_secs(1));
    //println!("{:?}", &watermarks);
    let metadata = &consumer.fetch_metadata(None, Duration::from_secs(1)).expect("errou");
    for topic in metadata.topics() {
        let partitions: i32 = topic.partitions().len().try_into().unwrap();
        for p in 0..partitions {
          let watermarks = &consumer.fetch_watermarks(topic.name(), p, Duration::from_secs(1))?;
          print!("{:?}", &watermarks);
        }
    }
    Ok(())
}

