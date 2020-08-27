#[macro_use]
extern crate lazy_static;
extern crate bincode;
extern crate prometheus;
use crate::consumer::lag_consumer::consume;
use crate::helpers::utils::LagKey;
use crate::store::lag_store::get_lag;
use slog::*;
use std::convert::From;
use warp::hyper::Body;
use warp::{
    http::{Response, StatusCode},
    Filter,
};
mod consumer;
mod helpers;
mod store;

lazy_static! {
    static ref LOG: slog::Logger = create_log();
}

fn create_log() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().force_plain().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

#[tokio::main]
async fn main() {
    let _consume = tokio::task::spawn(consume());

    let lag = warp::path::param()
        .and(warp::path::param())
        .and(warp::path::param())
        .map(|group: String, topic: String, partition: i32| {
            let key = LagKey::new(group, topic, partition);
            match get_lag(&key) {
                Some(v) => Response::builder().body(Body::from(v)),
                None => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from("Lag not found")),
            }
        });
    warp::serve(lag).run(([127, 0, 0, 1], 32666)).await;
}
