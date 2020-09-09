#[macro_use]
extern crate lazy_static;
extern crate bincode;
mod config_reader;
mod db_client;
mod lag_consumer;
mod parser;
mod types;
use lag_consumer::LAG_CONSUMER;
use slog::*;
use warp::{http::StatusCode, Filter};

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
    tokio::task::spawn(LAG_CONSUMER.consume());

    let cors = warp::cors().allow_any_origin();

    let groups = warp::path("groups").map(|group: String| match LAG_CONSUMER.fetch_groups() {
        Some(v) => warp::reply::with_status(warp::reply::json(&v), StatusCode::OK),
        None => {
            warp::reply::with_status(warp::reply::json(&"Lag not found"), StatusCode::NOT_FOUND)
        }
    });

    let lag = warp::path("lag")
        .and(warp::path::param())
        .map(|group: String| match LAG_CONSUMER.get_lag(&group) {
            Some(v) => warp::reply::with_status(warp::reply::json(&v), StatusCode::OK),
            None => {
                warp::reply::with_status(warp::reply::json(&"Lag not found"), StatusCode::NOT_FOUND)
            }
        })
        .with(cors)
        .or(groups);
    warp::serve(lag).run(([127, 0, 0, 1], 32666)).await;
}
