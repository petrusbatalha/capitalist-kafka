extern crate slog_async;
extern crate slog_term;
use crate::helpers::parser::parse_message;
use crate::helpers::config_reader::read;
use crate::helpers::utils::OffsetRecord;
use crate::exporters::exporter::push_metrics;
use futures::TryStreamExt;
use slog::*;
use std::time::Duration;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{Message, OwnedMessage};

lazy_static! {
    static ref WATERMARK_CONSUMER: StreamConsumer = read().create().unwrap();
    static ref LOG: slog::Logger = create_log();
}

fn create_log() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().force_plain().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

pub async fn consume() {
    let consumer: StreamConsumer = read().create().unwrap();
    consumer
        .subscribe(&["__consumer_offsets"])
        .expect("Can't subscribe to __consumer_offset topic. ERR");
    let stream_processor = consumer
        .start().try_for_each(|borrowed_message| async move {
            let owned_message = borrowed_message.detach();
            tokio::spawn(async move {
                tokio::task::spawn_blocking(|| fetch_highwatermarks(owned_message))
                    .await
                    .expect("Failed to calculate lag");
            });
            Ok(())
        });
    stream_processor
        .await
        .expect("Failed to start stream consumer.");
}

fn fetch_highwatermarks(owned_message: OwnedMessage) {
    let timestamp = owned_message.timestamp();
    let key = owned_message.key().unwrap_or(&[]);
    let payload = owned_message.payload().unwrap_or(&[]);

    info!(LOG, "Fetching watermarks...");
    match parse_message(key, payload) {
        Ok(OffsetRecord::OffsetCommit {
            group,
            topic,
            partition,
            offset,
        }) => {
            match &WATERMARK_CONSUMER.fetch_watermarks(
                &topic,
                partition,
                Duration::from_millis(1000),
            ) {
                Ok(hwms) => {
                    let lag = hwms.1 - offset;
                    push_metrics(OffsetRecord::GroupOffsetLag {
                        group,
                        topic,
                        partition,
                        offset,
                        lag,
                        timestamp,
                    })
                }
                Err(e) => warn!(LOG, "Error to process High Topic Watermarks, {}", e),
            }
        }
        _ => (),
    }
}
