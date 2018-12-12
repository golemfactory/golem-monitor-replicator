extern crate actix;
extern crate actix_redis;
extern crate actix_web;
extern crate serde;

extern crate url;

#[cfg_attr(test, macro_use)]
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate futures;

extern crate tokio_core;

extern crate env_logger;
#[macro_use]
extern crate log;

extern crate config;

use actix_web::{http, server, App, HttpMessage, HttpRequest, HttpResponse};
use config::{Config, ConfigError, Environment, File};
use std::net::IpAddr;

#[cfg_attr(feature = "stats_update", macro_use)]
extern crate failure;
#[cfg_attr(feature = "stats_update", macro_use)]
extern crate redis_async;
#[cfg(feature = "stats_update")]
mod stats_update;
#[cfg(feature = "stats_update")]
mod updater;

#[cfg(feature = "pingme")]
extern crate bytes;
#[cfg(feature = "pingme")]
extern crate nom;
#[cfg(feature = "pingme")]
mod pingme;

mod redis_tools;

#[cfg(feature = "list_nodes")]
mod list_nodes;

pub fn get_client_ip(r: &HttpRequest) -> Option<IpAddr> {
    use std::str::FromStr;

    let forwarded_for: Option<&http::header::HeaderValue> = r.headers().get("x-forwarded-for");

    match forwarded_for {
        Some(ref v) => v.to_str().ok().and_then(|a| IpAddr::from_str(a).ok()),
        _ => r.peer_addr().map(|a| a.ip()),
    }
}

#[derive(Debug, Deserialize)]
struct MonitorSettings {
    address: ::std::net::SocketAddr,
    redis_address: String,
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

#[cfg(feature = "list_nodes")]
fn route_list_nodes(redis_address: String) -> impl Fn(App) -> App {
    info!("mounting /dump");
    move |app: App| {
        use actix_redis::RedisActor;
        let redis_actor = RedisActor::start(redis_address.clone());
        app.resource("/dump", |r| {
            r.method(http::Method::GET)
                .h(list_nodes::ListNodesHandler::new(
                    redis_actor,
                    list_nodes::ListType::ExportCSV,
                ))
        })
    }
}

#[cfg(not(feature = "list_nodes"))]
fn route_list_nodes(_: String) -> impl Fn(App) -> App {
    |app| app
}

#[cfg(feature = "pingme")]
fn route_pingme(app: App) -> App {
    info!("mounting ping-me");
    app.route("/ping-me", http::Method::POST, pingme::ping_me)
}

#[cfg(not(feature = "pingme"))]
fn route_pingme(app: App) -> App {
    app
}

#[cfg(feature = "stats_update")]
fn route_stats_update(redis_address: String) -> impl Fn(App) -> App {
    info!("mounting stats update");
    use actix_redis::RedisActor;

    move |app: App| -> App {
        let redis_actor = RedisActor::start(redis_address.clone());

        let update_handler_root = stats_update::UpdateHandler::new(redis_actor.clone());
        let update_handler_update = stats_update::UpdateHandler::new(redis_actor);

        app.resource("/", move |r| {
            r.method(http::Method::GET).h(|_r| {
                HttpResponse::MovedPermanenty()
                    .header("Location", "/show")
                    .finish()
            });
            r.method(http::Method::POST).h(update_handler_root)
        })
        .resource("/update", |r| {
            r.method(http::Method::POST).h(update_handler_update)
        })
    }
}

#[cfg(not(feature = "stats_update"))]
fn route_stats_update(_: String) -> impl Fn(App) -> App {
    |app| app
}

fn main() {
    if ::std::env::var("RUST_LOG").is_err() {
        ::std::env::set_var(
            "RUST_LOG",
            "actix_web=info,actix_redis=info,golem_monitor_rust=info",
        )
    }
    env_logger::init();

    let sys = actix::System::new("golem-monitor");

    let MonitorSettings {
        address,
        redis_address,
    } = MonitorSettings::load().unwrap();

    info!("Starting server on {}", &address);

    server::new(move || {
        App::new()
            .middleware(actix_web::middleware::Logger::default())
            .configure(route_pingme)
            .configure(route_list_nodes(redis_address.clone()))
            .configure(route_stats_update(redis_address.clone()))
    })
    .bind(address)
    .unwrap()
    .start();

    let _ = sys.run();
}
