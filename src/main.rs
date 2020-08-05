use rdkafka::consumer::stream_consumer::StreamConsumer;
use byteorder::{BigEndian, ReadBytesExt};
use futures::StreamExt;
use std::io::{BufRead, Cursor, Error};
use std::str;
use rdkafka::consumer::{Consumer};
use rdkafka::message::{Message};
mod config_reader;


pub fn read_str<'a>(rdr: &'a mut Cursor<&[u8]>) -> i16 {
   let k = (rdr.read_i16::<BigEndian>()).expect("Error");
   k
}

#[tokio::main]
async fn main() {
    let config = config_reader::read_config();
    let consumer: StreamConsumer = config.0.create().unwrap();
    consumer.subscribe(&["__consumer_offsets"]).expect("Can't subscribe to specified topic");
    let mut message_stream = consumer.start();
    while let Some(message) = message_stream.next().await {
        match message {
            Err(e) => println!("Kafka error: {}", e),
            Ok(m) => {
                //let key = m.key().unwrap_or(&[]);
                //let payload = m.payload().unwrap_or(&[]);
                let msg = m.key().unwrap_or(&[]);
                let mut cursor = Cursor::new(msg);
                let keyver = read_str(&mut cursor);
                if keyver == 1 {
                   let msg_string = str::from_utf8(msg);
                   println!("{:?}", msg_string);
                }
                //let mut payload_rdr = Cursor::new(payload);
                //parse_group_offset(& mut key_rdr, & mut payload_rdr)

                }
            }
        }
    }

//fn main() {
  // let key : &[u8] = b"0, 1, 0, 13, 109, 97, 110, 97, 103, 101, 114, 95, 108, 111, 99, 97, 108, 0, 18, 95, 95, 99, 111, 110, 115, 117, 109, 101, 114, 95, 111, 102, 102, 115, 101, 116, 115, 0, 0, 0, 29";
   //let payload : &[u8] = b"0, 3, 0, 0, 0, 0, 0, 0, 2, 206, 255, 255, 255, 255, 0, 0, 0, 0, 1, 115, 187, 56, 92, 186";

   
   //let mut key_rdr : Cursor<&[u8]> = Cursor::new(key);
   //let slice = key_rdr.read_i16::<BigEndian>().expect("t");
   //let pay = payload_rdr.read_i64::<BigEndian>();
   //let mut payload_rdr : Cursor<&[u8]> = Cursor::new(payload);
   //let len = (rdr.read_i16::<BigEndian>()).expect("t") as usize;
   //let pos = rdr.position() as usize;
   //let slice = str::from_utf8(&rdr.get_ref()[pos..(pos + len)]).expect("bla");
   //rdr.consume(len);
   //println!("PAY {:?}", pay);
   //println!("{:?}", slice);
//}