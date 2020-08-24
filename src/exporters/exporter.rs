use crate::helpers::utils::OffsetRecord;
use prometheus::{Encoder, GaugeVec, Opts, Registry, TextEncoder};
use rdkafka::message::Timestamp;
use std::sync::Mutex;
extern crate chrono;
use chrono::prelude::*;

const SIZE: usize = 10000;

lazy_static! {
    static ref METRICS: Mutex<Vec<u8>> = Mutex::new(Vec::with_capacity(SIZE));
    static ref KAFKA_LAG_METRIC: (Registry, GaugeVec) = {
        let registry = Registry::new();
        let lag_opts = Opts::new("kafka_consumergroup_lag", "Lag for a consumer group.");
        let lag_gauge = GaugeVec::new(
            lag_opts,
            &["topic", "partition", "group", "timestamp", "offset"],
        )
        .unwrap();
        registry.register(Box::new(lag_gauge.clone())).unwrap();
        (registry, lag_gauge)
    };
}

pub fn push_metrics(lag: OffsetRecord) {
    match lag {
        OffsetRecord::GroupOffsetLag {
            group,
            topic,
            partition,
            offset,
            lag,
            timestamp,
        } => {
            let mut lag_labels = Vec::new();
            let mut buffer: Vec<u8> = Vec::new();
            let partition = &*partition.to_string();
            let offset = &offset.to_string();
            let timestamp = &*parse_date(timestamp);
            lag_labels.push(&*topic);
            lag_labels.push(partition);
            lag_labels.push(&*group);
            lag_labels.push(offset);
            lag_labels.push(timestamp);
            KAFKA_LAG_METRIC
                .1
                .with_label_values(&lag_labels)
                .set(lag as f64);
            let metric_families = KAFKA_LAG_METRIC.0.gather();
            let encoder = TextEncoder::new();
            &encoder.encode(&metric_families, &mut buffer);
            METRICS.lock().unwrap().append(&mut buffer);
            clear_cache()
        }
        _ => (),
    }
}

fn parse_date(timestamp: Timestamp) -> String {
    let t = timestamp.to_millis();
    let naive = NaiveDateTime::from_timestamp(t.unwrap(), 0);
    let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);
    datetime.format("%Y-%m-%d %H:%M:%S.fff").to_string()
}

pub fn retrieve_metrics() -> Vec<u8> {
    METRICS.lock().unwrap().clone()
}

pub fn clear_cache() {
    if METRICS.lock().unwrap().len() > SIZE {
        METRICS.lock().unwrap().clear();
    }
}
