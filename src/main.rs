use futures::StreamExt;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use std::convert::TryInto;
use std::time::Duration;
use std::collections::{HashMap};
mod helpers;
use helpers::utils::Result as Result;
use helpers::parser::parse_message;
use helpers::config_reader;
use helpers::utils::ConsumerUpdate;

type Topic = HashMap<String, HashMap<i32, i64>>;

fn fetch_topics_highwatermarks(consumer: &StreamConsumer) -> Result<Topic> {
   let mut topics: Topic = HashMap::new();

   let metadata = &consumer.fetch_metadata(None, Duration::from_secs(1))?;
   for t in metadata.topics() {
      let mut partitions_watermarks = HashMap::new();
       let topic =  t.name().to_string();
       let partitions: i32 = t.partitions().len().try_into().unwrap();
       for p in 0..partitions {
           let high_watermarks =
               &consumer.fetch_watermarks(t.name(), p, Duration::from_secs(1))?;
            &partitions_watermarks.insert(p, high_watermarks.1);
        }  
   &topics.insert(topic, partitions_watermarks);
   }
   Ok(topics)
}

#[tokio::main]
async fn main() {
    let config = config_reader::read_config();
    let consumer: StreamConsumer = config.0.create().unwrap();
    consumer.subscribe(&["__consumer_offsets"]).expect("Can't subscribe to specified topic");
    let mut message_stream = consumer.start();
    while let Some(message) = message_stream.next().await {
        match message {
            Ok(m) => {
                let key = m.key().unwrap_or(&[]);
                let payload = m.payload().unwrap_or(&[]);
                let offset_topic = m.offset();
                match parse_message(key, payload) {
                    Ok(ConsumerUpdate::OffsetCommit {
                        group,
                        topic,
                        partition,
                        offset,
                    }) => {
                        let wms_map = match fetch_topics_highwatermarks(&consumer) {
                            Ok(w) =>  {
                                let partition_high_wms = w[&topic][&partition];
                                println!("High partition watermark {:?}", &partition_high_wms);
                            },
                            Err(e) => (),
                        };
                        println!("
                                  Offset Topico {:?},
                                  Offset Group: {:?},
                                  Topic: {:?},
                                  Partition: {:?},
                                  Grupo: {:?}",
                        offset_topic-1, offset-1, topic, partition, group);
                    }
                    
                    Ok(_) => {}
                    Err(e) => println!("{:?}", e),
                }
            }
            Err(e) => println!("{:?}", e),
        }
    }
}
