#[macro_use]
extern crate lazy_static;
extern crate slog_term;
extern crate num_cpus;
extern crate slog_async;
use futures::TryStreamExt;
use helpers::parser::parse_message;
use std::convert::Infallible;
use helpers::reader::read_config;
use helpers::utils::OffsetRecord;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{Message, OwnedMessage};
use std::time::Duration;
use hyper::http::StatusCode;
use prometheus::{GaugeVec, TextEncoder, Opts, Registry, Encoder};
use std::sync::Mutex;
use std::net::{Ipv4Addr, SocketAddr, IpAddr};
use slog::*;
extern crate prometheus;
use hyper::{service::{make_service_fn, service_fn}, Body, Request, Response, Server,};
mod helpers;

lazy_static! {
    static ref LOG: slog::Logger = create_log();
    static ref KAFKA_LAG_METRIC: (Registry, GaugeVec) = {
        let registry = Registry::new();
        let lag_opts = Opts::new("kafka_consumergroup_lag", "Lag for a consumer group.");
        let lag_gauge = GaugeVec::new(lag_opts, &["topic", "partition", "group"]).unwrap();
        registry.register(Box::new(lag_gauge.clone())).unwrap();
        (registry, lag_gauge)
    };
    static ref METRICS: Mutex<Vec<u8>> = Mutex::new(vec![]);
}

fn create_log() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().force_plain().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

fn fetch_highwatermarks(config: ClientConfig, owned_message: OwnedMessage) {
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
            let consumer: StreamConsumer = config.create().unwrap();
            let mut buffer = Vec::<u8>::new();
            match &consumer.fetch_watermarks(&topic, partition, Duration::from_millis(1000)) {
                Ok(hwms) => {
                    let partition = &*partition.to_string();
                    let mut lag_labels = Vec::new();
                    lag_labels.push(&*topic);
                    lag_labels.push(partition);
                    lag_labels.push(&*group);
                    KAFKA_LAG_METRIC.1.with_label_values(&lag_labels).set((hwms.1 - offset) as f64);
                    let metric_families = KAFKA_LAG_METRIC.0.gather();
                    let encoder = TextEncoder::new();
                    &encoder.encode(&metric_families, &mut buffer);
                    METRICS.lock().unwrap().append(&mut buffer);
                    buffer.clear();
                }
                Err(e) => {
                    warn!(LOG, "Error to process High Topic Watermarks, {}", e)
                }
            }
        }
        _ => (),
    }
}

async fn get_lag_metrics(_req: Request<Body>) -> std::result::Result<Response<Body>, Infallible> {
    if _req.uri().path() != "/metrics" {
        Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(hyper::Body::empty())
            .unwrap())
    } else if _req.method() != "GET" {
        Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(hyper::Body::empty())
            .unwrap())
    } else {
        let resp = match Response::builder().body(Body::from(METRICS.lock().unwrap().clone())) {
            Ok(r) => r,
            Err(err) => {
                error!(LOG, "internal server error {:?}", err);
                Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(hyper::Body::empty())
                .unwrap()
            },
        };
        Ok(resp)
    }
}

async fn consume(config: ClientConfig) {
    let consumer: StreamConsumer = config.create().unwrap();
    info!(LOG, "Kafka config -> {:?}", config);
    consumer.subscribe(&["__consumer_offsets"]).expect("Can't subscribe to __consumer_offset topic. ERR");
    let stream_processor = consumer.start().try_for_each(|borrowed_message| {
        let owned_config = config.to_owned();
        async move {
            let owned_message = borrowed_message.detach();
            tokio::spawn(async move {
                tokio::task::spawn_blocking(|| fetch_highwatermarks(owned_config, owned_message))
                    .await
                    .expect("Failed to calculate lag");
            });
            Ok(())
        }
    });
    stream_processor.await.expect("Failed to start stream consumer.");
}

#[tokio::main]
async fn main() {
    let socket_addr = SocketAddr::from(([127, 0, 0, 1], 32666));

    let _consume = tokio::task::spawn(consume(read_config()));

    let service = make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(get_lag_metrics))
    });

    let server = Server::bind(&socket_addr).serve(service);
    info!(LOG, "starting exporter on http://{:?}", socket_addr);
    if let Err(e) = server.await {
        error!(LOG, "server error: {}", e);
    }
}
