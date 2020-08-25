#[macro_use]
extern crate lazy_static;
extern crate prometheus;
use std::convert::Infallible;
use lag_consumer::consume;
use routers::metrics::get_lag_metrics;
use std::net::SocketAddr;
use slog::*;
use hyper::{service::{make_service_fn, service_fn}, Server};
mod routers;
mod exporters;
mod lag_consumer;
mod helpers;

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
    let socket_addr = SocketAddr::from(([127, 0, 0, 1], 32666));

    let _consume = tokio::task::spawn(consume());

    let service = make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(get_lag_metrics)) });

    let server = Server::bind(&socket_addr).serve(service);
    info!(LOG, "starting exporter on http://{:?}", socket_addr);
    if let Err(e) = server.await {
        error!(LOG, "server error: {}", e);
    }
}
