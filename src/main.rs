extern crate actix;
extern crate actix_web;
extern crate actix_redis;
extern crate serde;

#[allow(unused_imports)]
#[macro_use]
extern crate serde_json;
#[macro_use] extern crate serde_derive;
extern crate futures;
#[macro_use]
extern crate redis_async;


#[macro_use] extern crate failure;

extern crate tokio_core;

extern crate env_logger;
#[macro_use]
extern crate log;

extern crate config;

use actix_web::{http, server, App};
use config::{ConfigError, Config, File, Environment};

mod pingme;
mod stats_update;
mod updater;

#[derive(Debug, Deserialize)]
struct MonitorSettings {
    address : ::std::net::SocketAddr,
    redis_address : String
}

impl MonitorSettings {
    fn load() -> Result<Self, ConfigError> {
        let mut config = Config::new();

        config
            .set_default("address", "0.0.0.0:8081")?
            .set_default("redis_address", "127.0.0.1:6379")?
            .merge(File::with_name("golem-monitor").required(false))?
            .merge(Environment::with_prefix("golem_monitor"))?;

        config.try_into()
    }
}

fn main() {

    if ::std::env::var("RUST_LOG").is_err() {
        ::std::env::set_var("RUST_LOG", "actix_web=info,actix_redis=info,golem_monitor_rust=info")
    }
    env_logger::init();

    let sys = actix::System::new("golem-monitor");

    let MonitorSettings { address, redis_address } = MonitorSettings::load().unwrap();

    info!("Starting server on {}", &address);

    server::new(
         move || {
            use actix_redis::RedisActor;

            let redis_actor = RedisActor::start(redis_address.clone());

            let update_handler_root = stats_update::UpdateHandler::new(redis_actor.clone());
            let update_handler_update = stats_update::UpdateHandler::new(redis_actor);

            App::new()
                .middleware(actix_web::middleware::Logger::default())
                .route("/ping-me", http::Method::POST, pingme::ping_me)
                .resource("/", |r| {
                    r.method(http::Method::POST).h(update_handler_root)
                })
                .resource("/update", |r| {
                    r.method(http::Method::POST).h(update_handler_update)
                })

        })
        .bind(address).unwrap()
        .start();

    let _ = sys.run();
}

