#[macro_use]
extern crate lazy_static;
extern crate bincode;
mod types;
mod lag_consumer;
mod db_client;
mod config_reader;
mod parser;
use slog::*;
use warp::{http::StatusCode, Filter};
use lag_consumer::{LAG_CONSUMER};

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
    tokio::task::spawn(LAG_CONSUMER.lag_calc_update());

    let cors = warp::cors().allow_any_origin();

    let lag = warp::path("lag").map(move || match LAG_CONSUMER.get_lag() {
        Some(v) => warp::reply::with_status(warp::reply::json(&v), StatusCode::OK),
        None => warp::reply::with_status(warp::reply::json(&"Lag not found"), StatusCode::NOT_FOUND),
    }).with(cors);
    warp::serve(lag).run(([127, 0, 0, 1], 32666)).await;
}
