use futures::StreamExt;
use helpers::config_reader;
use helpers::parser::parse_message;
use helpers::utils::ConsumerUpdate;
use helpers::utils::Result;
use lazy_static::lazy_static;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Mutex;
use std::time::Duration;
mod helpers;

lazy_static! {
    static ref TOPICS: Mutex<HashMap<String, HashMap<i32, i64>>> = {
        let map = Mutex::new(HashMap::new());
        map
    };
}

fn fetch_topics_highwatermarks(consumer: &StreamConsumer) -> Result<&TOPICS> {
    let metadata = &consumer.fetch_metadata(None, Duration::from_secs(1))?;
    for t in metadata.topics() {
        let mut partitions_watermarks = HashMap::new();
        let topic = t.name().to_string();
        let partitions: i32 = t.partitions().len().try_into().unwrap();
        for p in 0..partitions {
            let high_watermarks =
                &consumer.fetch_watermarks(t.name(), p, Duration::from_secs(1))?;
            &partitions_watermarks.insert(p, high_watermarks.1);
        }
        TOPICS.lock().unwrap().insert(topic, partitions_watermarks);
    }
    Ok(&TOPICS)
}

#[tokio::main]
async fn main() {
    let config = config_reader::read_config();
    let consumer: StreamConsumer = config.0.create().unwrap();
    consumer
        .subscribe(&["__consumer_offsets"])
        .expect("Can't subscribe to specified topic");
    let mut message_stream = consumer.start();
    while let Some(message) = message_stream.next().await {
        match message {
            Ok(m) => {
                let key = m.key().unwrap_or(&[]);
                let payload = m.payload().unwrap_or(&[]);
                match parse_message(key, payload) {
                    Ok(ConsumerUpdate::OffsetCommit {
                        group,
                        topic,
                        partition,
                        offset,
                    }) => {
                        match fetch_topics_highwatermarks(&consumer) {
                            Ok(_) => {
                                let partition_high_wms = TOPICS.lock().unwrap()[&topic][&partition];
                                let partition_lag = (&partition_high_wms - 1) - (&offset - 1);
                                let group_offset_lag = ConsumerUpdate::GroupOffsetLag {
                                    group: group,
                                    topic: topic,
                                    partition: partition,
                                    offset: offset,
                                    partition_lag: partition_lag,
                                };
                                println!("{:?}", group_offset_lag);
                            }
                            Err(_) => (),
                        };
                    }
                    Ok(_) => {}
                    Err(e) => println!("{:?}", e),
                }
            }
            Err(e) => println!("{:?}", e),
        }
    }
}
