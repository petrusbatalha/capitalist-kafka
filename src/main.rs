#[macro_use]
extern crate lazy_static;
extern crate num_cpus;
extern crate slog_async;
extern crate slog_term;
use exporters::exporter::{push_metrics, retrieve_metrics};
use futures::TryStreamExt;
use helpers::config_reader::read;
use helpers::parser::parse_message;
use helpers::utils::OffsetRecord;
use hyper::http::StatusCode;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{Message, OwnedMessage};
use slog::*;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;
extern crate prometheus;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
mod exporters;
mod helpers;

lazy_static! {
    static ref LOG: slog::Logger = create_log();
    static ref WATERMARK_CONSUMER: StreamConsumer = read().create().unwrap();
}

fn create_log() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().force_plain().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
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
        let resp = match Response::builder().body(Body::from(retrieve_metrics())) {
            Ok(r) => r,
            Err(err) => {
                error!(LOG, "internal server error {:?}", err);
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(hyper::Body::empty())
                    .unwrap()
            }
        };
        Ok(resp)
    }
}

async fn consume() {
    let consumer: StreamConsumer = read().create().unwrap();
    consumer
        .subscribe(&["__consumer_offsets"])
        .expect("Can't subscribe to __consumer_offset topic. ERR");
    let stream_processor = consumer
        .start()
        .try_for_each(|borrowed_message| async move {
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

#[tokio::main]
async fn main() {
    let socket_addr = SocketAddr::from(([127, 0, 0, 1], 32666));

    let _consume = tokio::task::spawn(consume());

    let service =
        make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(get_lag_metrics)) });

    let server = Server::bind(&socket_addr).serve(service);
    info!(LOG, "starting exporter on http://{:?}", socket_addr);
    if let Err(e) = server.await {
        error!(LOG, "server error: {}", e);
    }
}
