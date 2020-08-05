use byteorder::{BigEndian, ReadBytesExt};
use futures::StreamExt;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use std::io::{BufRead, Cursor};
use std::convert::TryInto;
use std::time::Duration;
use std::collections::{HashMap};
use std::boxed::Box;
use std::str;
mod config_reader;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

type Topic = HashMap<String, HashMap<i32, i64>>;

#[derive(Debug)]
enum ConsumerUpdate {
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

pub fn read_str<'a>(rdr: &'a mut Cursor<&[u8]>) -> Result<&'a str> {
    let len = (rdr.read_i16::<BigEndian>())? as usize;
    let pos = rdr.position() as usize;
    let slice = str::from_utf8(&rdr.get_ref()[pos..(pos + len)]).expect("a");
    rdr.consume(len);
    Ok(slice)
}

pub fn read_string(rdr: &mut Cursor<&[u8]>) -> Result<String> {
    read_str(rdr).map(str::to_string)
}

fn parse_group_offset(
    key_rdr: &mut Cursor<&[u8]>,
    payload_rdr: &mut Cursor<&[u8]>,
) -> Result<ConsumerUpdate> {
    let group = read_string(key_rdr)?;
    let topic = read_string(key_rdr)?;
    let partition = key_rdr.read_i32::<BigEndian>()?;
    if !payload_rdr.get_ref().is_empty() {
        let _version = payload_rdr.read_i16::<BigEndian>()?;
        let offset = payload_rdr.read_i64::<BigEndian>()?;
        Ok(ConsumerUpdate::OffsetCommit {
            group,
            topic,
            partition,
            offset,
        })
    } else {
        Ok(ConsumerUpdate::OffsetTombstone {
            group,
            topic,
            partition,
        })
    }
}

fn parse_message(key: &[u8], payload: &[u8]) -> Result<ConsumerUpdate> {
    let mut key_rdr = Cursor::new(key);
    let key_version = key_rdr.read_i16::<BigEndian>()?;
    match key_version {
        0 | 1 => parse_group_offset(&mut key_rdr, &mut Cursor::new(payload)),
        2 => Ok(ConsumerUpdate::Metadata),
        _ => panic!(),
    }
}

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
                        let wms = fetch_topics_highwatermarks(&consumer);
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
