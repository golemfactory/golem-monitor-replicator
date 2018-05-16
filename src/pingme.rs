#![allow(unused_imports)]

use futures::prelude::*;
use futures::future;
use actix::{Arbiter};
use actix_web::{self, http, HttpRequest, AsyncResponder, HttpMessage, HttpResponse};
use actix_web::error::{JsonPayloadError, ResponseError};
use std::net::{IpAddr, SocketAddr};
use tokio_core::net::TcpStream;
use tokio_core::reactor;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_TIMEOUT : Duration = Duration::from_secs(5);
const MAX_PORTS : usize = 5;

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
struct  PingMe {
    timestamp : f64,
    port : Option<u16>,
    #[serde(default)]
    ports : Vec<u16>
}

impl PingMe {

    fn ports(&self) -> Vec<u16> {
        let mut ports = self.ports.clone();
        if let Some(port) = self.port {
            ports.push(port)
        }
        ports
    }
}

#[derive(Serialize,Debug)]
struct PingMeResult {
    success : bool,
    description : String,
    port_statuses : Vec<PortStatus>,
    time_diff : f64
}

#[inline]
fn extract_address(r : &HttpRequest) -> Option<IpAddr> {
    use std::str::FromStr;

    let forwarded_for : Option<&http::header::HeaderValue> = r.headers().get("x-forwarded-for");

    match forwarded_for {
        Some(ref v) => v.to_str().ok().and_then(|a| IpAddr::from_str(a).ok()),
        _ => r.peer_addr().map(|a| a.ip())
    }
}

#[derive(Serialize,Debug)]
struct PortStatus {
    port :u16,
    is_open : bool,
    description : &'static str
}

fn ping_address(addr : &IpAddr, port : u16) -> Box<Future<Item=PortStatus, Error=actix_web::Error>> {
    let p = port;
    let pp = port;
    let p3 = port;
    let addr = SocketAddr::new(*addr, port);

    let timeout =
        reactor::Timeout::new(DEFAULT_TIMEOUT, Arbiter::handle()).unwrap()
            .and_then(move |_| future::ok(PortStatus { port: p3, is_open: false, description: "timeout"}));

    Box::new(
        timeout.select(
        TcpStream::connect(&addr, Arbiter::handle())
            .map_err(|e| actix_web::error::ErrorInternalServerError(format!("io: {}", e)))
            .and_then(move |_| {
                 Ok(PortStatus { port: p, is_open: true, description: "open" })
            })
            .or_else(move |_e| {
                Ok(PortStatus { port: pp, is_open: false, description: "unreachable"})
            }))
            .map_err(|e|
                actix_web::error::ErrorInternalServerError(format!("io: {}", e.0)))
            .and_then(|(result, _other)|
                future::ok(result))
            )
}

fn ping_multi(addr : &IpAddr, ports : &Vec<u16>) -> Box<Future<Item=Vec<PortStatus>, Error=actix_web::Error>> {
    let p : Vec<Box<Future<Item=PortStatus, Error=actix_web::Error>>> = ports.iter().map(|port| {
        ping_address(addr, *port)
    }).collect();

    Box::new(future::join_all(p))
}

fn duration_to_secs(d : &Duration) -> f64 {
    let secs = d.as_secs() as f64;
    let milis = (d.subsec_nanos() / 1000000) as f64;

    secs + milis * 0.001f64
}

fn time_diff(base : SystemTime, other : SystemTime) -> f64 {
    if base < other {
        -duration_to_secs(&other.duration_since(base).unwrap())

    }
    else {
        duration_to_secs(&base.duration_since(other).unwrap())
    }
}

pub fn ping_me(r : HttpRequest) -> Box<Future<Item = HttpResponse, Error=actix_web::Error>> {
    let system_time = SystemTime::now();
    let peer_address = extract_address(&r);

    r.json()
        .map_err(|e| {
            actix_web::error::ErrorBadRequest(format!("{}", e))
        })
        .and_then(|b : PingMe| {
            let ports = b.ports();

            if ports.len() > MAX_PORTS {
                future::err(actix_web::error::ErrorBadRequest("too many ports"))
            }
            else {
                future::ok((b, ports))
            }
        })
        .and_then(move |(b, ports) : (PingMe, Vec<u16>)| {
            let timestamp = UNIX_EPOCH + Duration::from_millis((b.timestamp*1000.0f64) as u64);



            let l : Box<Future<Item = Vec<PortStatus>, Error = actix_web::Error>>  = match peer_address {
                Some(ref addr) => ping_multi(&addr, &ports),
                _ =>   Box::new(future::err(actix_web::error::ErrorInternalServerError("source address not valid")))
            };

            l.and_then(move |port_statuses| {
                let success = port_statuses.iter().all(|port_status| port_status.is_open);
                let mut description = String::new();

                for l in port_statuses.iter()
                    .map(|port_status| format!("{}: {}", port_status.port, port_status.description)) {
                    if !description.is_empty() {
                        description.push('\n');
                    }
                    description.push_str(&l);
                }


                Ok(HttpResponse::Ok().json(PingMeResult {
                    success, description, port_statuses, time_diff: time_diff(system_time, timestamp)
                }).into())
            })
        })
        .or_else(|e : actix_web::Error| {
            debug!("Error {:?}", &e);

            let mut resp = e.cause().error_response();
            resp.set_body(format!("{}",e));
            Ok(resp)
        })
       .responder()
}


#[cfg(test)]
mod tests {

    use super::*;
    use serde_json;


    #[test]
    fn test_parse() {
        let p : PingMe = serde_json::from_str("{\"port\": 2020, \"timestamp\": 12.0}").unwrap();
        assert_eq!(p.port.unwrap(), 2020);
        assert!(p.timestamp > 11.0);
    }

}