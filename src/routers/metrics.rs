use crate::exporters::exporter::{retrieve_metrics};
use hyper::http::StatusCode;
use std::convert::Infallible;
use slog::*;
use hyper::{Body, Request, Response,};

lazy_static! {
    static ref LOG: slog::Logger = create_log();
}

fn create_log() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().force_plain().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

pub async fn get_lag_metrics(_req: Request<Body>) -> std::result::Result<Response<Body>, Infallible> {
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