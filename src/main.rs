use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::time::Duration;
mod config_reader;

pub type TopicName = String;
pub type TopicPartition = (TopicName, i32);
pub type BrokerId = i32;

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct Partition {
    pub id: i32,
    pub leader: BrokerId,
    pub replicas: Vec<BrokerId>,
    pub isr: Vec<BrokerId>,
}

impl Partition {
    fn new(
        id: i32,
        leader: BrokerId,
        mut replicas: Vec<BrokerId>,
        mut isr: Vec<BrokerId>,
    ) -> Partition {
        replicas.sort();
        isr.sort();
        Partition {
            id,
            leader,
            replicas,
            isr,
        }
    }
}

fn main() {
    let mut mapito: HashMap<String, Partition> = HashMap::new(); 
    let config = config_reader::read_config();
    let consumer: StreamConsumer = config.0.create().expect("ERRITO");

    //consumer.subscribe(&vec![config.1.as_str()].as_slice());
    consumer.start();
    //let watermarks = consumer.fetch_watermarks(Some(""), 1, Duration::from_secs(1));
    //println!("{:?}", &watermarks);
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(1)).expect("errou");
    for topic in metadata.topics() {
        for p in topic.partitions() {
            let partition = Partition::new(
                p.id(),
                p.leader(),
                p.replicas().to_owned(),
                p.isr().to_owned(),
            );
            print!("{:?}", partition);
        }
        //consumer.fetch_watermarks(topic.name(), topic., timeout: T)
    }
}

