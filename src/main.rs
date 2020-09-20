#[macro_use]
extern crate lazy_static;
extern crate bincode;
mod utils;
mod group_manager;
mod metadata_manager;
use utils::{consumer_provider, db_client, logger::create_log, parser, types};
use group_manager::api::{group_list, groups_lag};
use metadata_manager::api::{topics};
use warp::{Filter};

lazy_static! {
    static ref LOG: slog::Logger = create_log();
}

#[tokio::main]
async fn main() {
    tokio::task::spawn(group_manager::manager::start());
    let cors = warp::cors().allow_any_origin();
    let routes =
                group_list()
                .or(groups_lag())
                .or(topics())
                .with(cors);
    warp::serve(routes).run(([127, 0, 0, 1], 32666)).await;
}
