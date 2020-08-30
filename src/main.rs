#[macro_use]
extern crate lazy_static;
extern crate bincode;
use crate::consumer::lag_consumer::{calculate_lag, consume};
use crate::helpers::utils::{GroupKey, TopicPartition};
use crate::store::lag_store::get;
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
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

    tokio::task::spawn({
        interval.tick().await;
        calculate_lag()
    });

    tokio::task::spawn(consume());

    let lag = warp::path("lag").map(|| match get(b"last_calculated_lag".to_vec()) {
        Some(v) => warp::reply::with_status(warp::reply::json(&v), StatusCode::OK),
        None => {
            warp::reply::with_status(warp::reply::json(&"Lag not found"), StatusCode::NOT_FOUND)
        }
    });
    warp::serve(lag).run(([127, 0, 0, 1], 32666)).await;
}
