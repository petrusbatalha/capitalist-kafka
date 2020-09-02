#[macro_use]
extern crate lazy_static;
extern crate bincode;
use crate::consumer::lag_consumer::{calculate_lag, consume};
use crate::store::lag_store::get;
use slog::*;
use warp::{http::StatusCode, Filter};
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

async fn lag_calc_update() {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
    loop {
        interval.tick().await;
        calculate_lag().await;
    }
}

#[tokio::main]
async fn main() {
    tokio::task::spawn(lag_calc_update());
    tokio::task::spawn(consume());

    let cors = warp::cors().allow_any_origin();

    let lag = warp::path("lag").map(|| match get(b"last_calculated_lag".to_vec()) {
        Some(v) => warp::reply::with_status(warp::reply::json(&v), StatusCode::OK),
        None => {
            warp::reply::with_status(warp::reply::json(&"Lag not found"), StatusCode::NOT_FOUND)
        }
    }).with(cors);
    warp::serve(lag).run(([127, 0, 0, 1], 32666)).await;
}
