use warp::{http::StatusCode, Filter, filters::BoxedFilter, Reply};
use super::manager::{fetch_topics};

pub fn topics()  -> BoxedFilter<(impl Reply,)> {
    warp::path("topics").map(|| match fetch_topics() {
        Some(v) => warp::reply::with_status(warp::reply::json(&v), StatusCode::OK),
        None => warp::reply::with_status(
            warp::reply::json(&"Topics not found"),
            StatusCode::NOT_FOUND,
        ),
    }).boxed()
}