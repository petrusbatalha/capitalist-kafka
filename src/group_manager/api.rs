use super::manager::{fetch_groups, get_lag};
use warp::{http::StatusCode, Filter, filters::BoxedFilter, Reply};


pub fn group_list() ->  BoxedFilter<(impl Reply,)> {
    warp::path("groups").map(|| match fetch_groups() {
        Some(v) => warp::reply::with_status(warp::reply::json(&v), StatusCode::OK),
        None => warp::reply::with_status(
            warp::reply::json(&"Groups not found"),
            StatusCode::NOT_FOUND,
        ),
    }).boxed()
}

pub fn groups_lag() -> BoxedFilter<(impl Reply,)> {
    warp::path("lag")
        .and(warp::path::param())
        .map(|group: String| match get_lag(&group) {
            Some(v) => warp::reply::with_status(warp::reply::json(&v), StatusCode::OK),
            None => {
                warp::reply::with_status(warp::reply::json(&"Lag not found"), StatusCode::NOT_FOUND)
            }
        })
        .boxed()
}
