#![allow(unused_imports)]

use futures::prelude::*;
use futures::future;
use actix::Arbiter;
use actix_web::{self, http, HttpRequest, AsyncResponder, HttpMessage, HttpResponse, Body};
use actix_web::error::{JsonPayloadError, ResponseError};
use std::net::{IpAddr, SocketAddr};
use tokio_core::net::TcpStream;
use tokio_core::reactor;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use url::form_urlencoded::parse;
use bytes::Bytes;
use nom::AsBytes;
use super::get_client_ip;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PORTS: usize = 5;

#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
struct PingMe {
    timestamp: f64,
    port: Option<u16>,
    #[serde(default)]
    ports: Vec<u16>,
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

#[derive(Serialize, Debug)]
struct PingMeResult {
    success: bool,
    description: String,
    port_statuses: Vec<PortStatus>,
    time_diff: f64,
}

#[derive(Serialize, Debug)]
struct PortStatus {
    port: u16,
    is_open: bool,
    description: &'static str,
}

fn ping_address(
    addr: &IpAddr,
    port: u16,
) -> Box<Future<Item = PortStatus, Error = actix_web::Error>> {
    let addr = SocketAddr::new(*addr, port);

    let timeout = reactor::Timeout::new(DEFAULT_TIMEOUT, Arbiter::handle())
        .unwrap()
        .and_then(move |_| {
            Ok(PortStatus {
                port,
                is_open: false,
                description: "timeout",
            })
        });

    let ping = TcpStream::connect(&addr, Arbiter::handle())
        .and_then(move |_tcp| {
            Ok(PortStatus {
                port,
                is_open: true,
                description: "open",
            })
        })
        .or_else(move |_err| {
            Ok(PortStatus {
                port,
                is_open: false,
                description: "unreachable",
            })
        });

    Box::new(
        timeout
            .select(ping)
            .map_err(|(err, _next)| {
                actix_web::error::ErrorInternalServerError(format!("io: {}", err))
            })
            .and_then(|(result, _next)| future::ok(result)),
    )
}

fn ping_multi(
    addr: &IpAddr,
    ports: &Vec<u16>,
) -> Box<Future<Item = Vec<PortStatus>, Error = actix_web::Error>> {
    let p: Vec<Box<Future<Item = PortStatus, Error = actix_web::Error>>> =
        ports.iter().map(|port| ping_address(addr, *port)).collect();

    Box::new(future::join_all(p))
}

fn duration_to_secs(d: &Duration) -> f64 {
    let secs = d.as_secs() as f64;
    let milis = (d.subsec_nanos() / 1000000) as f64;

    secs + milis * 0.001f64
}

fn time_diff(base: SystemTime, other: SystemTime) -> f64 {
    if base < other {
        -duration_to_secs(&other.duration_since(base).unwrap())

    } else {
        duration_to_secs(&base.duration_since(other).unwrap())
    }
}

pub fn ping_me(r: HttpRequest) -> Box<Future<Item = HttpResponse, Error = actix_web::Error>> {
    let system_time = SystemTime::now();
    let client_ip = get_client_ip(&r);

    r.body()
        .and_then(|b| Ok(parse_url_params(b.as_bytes())))
        .map_err(|e| actix_web::error::ErrorBadRequest(format!("{}", e)))
        .and_then(|b: PingMe| {
            let ports = b.ports();

            if ports.len() > MAX_PORTS {
                future::err(actix_web::error::ErrorBadRequest("too many ports"))
            } else {
                future::ok((b, ports))
            }
        })
        .and_then(move |(b, ports): (PingMe, Vec<u16>)| {
            let timestamp = UNIX_EPOCH + Duration::from_millis((b.timestamp * 1000.0f64) as u64);

            let l: Box<Future<Item = Vec<PortStatus>, Error = actix_web::Error>> =
                match client_ip {
                    Some(ref addr) => ping_multi(&addr, &ports),
                    _ => Box::new(future::err(actix_web::error::ErrorInternalServerError(
                        "source address not valid",
                    ))),
                };

            l.and_then(move |port_statuses| {
                let success = port_statuses.iter().all(|port_status| port_status.is_open);
                let mut description = String::new();

                for l in port_statuses.iter().map(|port_status| {
                    format!("{}: {}", port_status.port, port_status.description)
                })
                {
                    if !description.is_empty() {
                        description.push('\n');
                    }
                    description.push_str(&l);
                }


                Ok(
                    HttpResponse::Ok()
                        .json(PingMeResult {
                            success,
                            description,
                            port_statuses,
                            time_diff: time_diff(system_time, timestamp),
                        })
                        .into(),
                )
            })
        })
        .or_else(|e: actix_web::Error| {
            debug!("Error {:?}", &e);

            let mut resp = e.cause().error_response();
            resp.set_body(format!("{}", e));
            Ok(resp)
        })
        .responder()
}

fn parse_url_params(input: &[u8]) -> PingMe {
    let mut ping_me = PingMe {
        timestamp: 0f64,
        port: None,
        ports: Vec::new(),
    };
    for (k, v) in parse(input) {
        match k.as_ref() {
            "timestamp" => {
                if let Ok(t) = v.parse() {
                    ping_me.timestamp = t;
                }
            }
            "ports" => {
                if let Ok(p) = v.parse() {
                    ping_me.ports.push(p);
                }
            }
            "port" => ping_me.port = v.parse().ok(),
            _ => println!("unknown param {}={}", k, v),
        }
    }

    ping_me
}

#[cfg(test)]
mod tests {

    use super::*;
    use serde_json;


    #[test]
    fn test_parse() {
        let p: PingMe = serde_json::from_str("{\"port\": 2020, \"timestamp\": 12.0}").unwrap();
        assert_eq!(p.port.unwrap(), 2020);
        assert!(p.timestamp > 11.0);
    }

    #[test]
    fn test_empty() {
        let ping_me = parse_url_params("".as_bytes());
        assert_eq!(
            ping_me,
            PingMe {
                ports: vec![],
                port: None,
                timestamp: 0f64,
            }
        );
    }

    #[test]
    fn test_timestamp() {
        let ping_me = parse_url_params("timestamp=3.14".as_bytes());
        assert_eq!(
            ping_me,
            PingMe {
                ports: vec![],
                port: None,
                timestamp: 3.14,
            }
        );
    }

    #[test]
    fn test_parse_single_port() {
        let ping_me = parse_url_params("port=37&timestamp=7".as_bytes());
        assert_eq!(
            ping_me,
            PingMe {
                ports: vec![],
                port: Some(37),
                timestamp: 7f64,
            }
        );
    }

    #[test]
    fn test_parse_multiple_ports() {
        let ping_me = parse_url_params(
            "ports=40102&ports=40103&ports=3282&timestamp=1530717930.2452438".as_bytes(),
        );
        assert_eq!(
            ping_me,
            PingMe {
                ports: vec![40102, 40103, 3282],
                port: None,
                timestamp: 1530717930.2452438,
            }
        );
    }

    #[test]
    fn test_parse_unknown() {
        let ping_me = parse_url_params(
            "portsa=40102&portsb=40103&ports=3282&timestamp=1530717930.2452438"
                .as_bytes(),
        );
        assert_eq!(
            ping_me,
            PingMe {
                ports: vec![3282],
                port: None,
                timestamp: 1530717930.2452438,
            }
        );
    }

}
