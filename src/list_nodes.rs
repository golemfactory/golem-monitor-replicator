use actix::{fut::*, prelude::*};
use actix_redis::{Command, RedisActor, RespValue};
use actix_web::dev::Handler;
use actix_web::{self, HttpRequest, HttpResponse};
use futures::future;
use futures::prelude::*;
use http::StatusCode;
use std::collections::HashMap;

pub enum ListType {
    ExportCSV,
    JSON,
}

pub struct ListNodesHandler {
    redis_actor: Addr<Unsync, RedisActor>,
    list_type: ListType,
}

impl ListNodesHandler {
    pub fn new(redis_actor: Addr<Unsync, RedisActor>, list_type: ListType) -> ListNodesHandler {
        ListNodesHandler {
            redis_actor,
            list_type: list_type,
        }
    }
}

impl Actor for ListNodesHandler {
    type Context = Context<Self>;
    /*fn started(&mut self, ctx: &mut Self::Context) {

    }*/
}

impl ListNodesHandler {
    fn list_keys(&self) -> impl Future<Item = Vec<String>, Error = actix_web::Error> {
        self.redis_actor
            .send(Command(resp_array!["KEYS", "nodeinfo.*"]))
            .map_err(|_e| actix_web::error::ErrorInternalServerError("Error sending KEYS command."))
            .and_then(|response| {
                let answer = match response {
                    Ok(a) => a,
                    Err(e) => return future::err(actix_web::error::ErrorInternalServerError(e)),
                };
                let all_nodes = match answer {
                    RespValue::Array(all_nodes) => all_nodes,
                    _ => {
                        return future::err(actix_web::error::ErrorInternalServerError(
                            "invalid redis response",
                        ))
                    }
                };
                let nodes: Result<Vec<String>, _> = all_nodes
                    .into_iter()
                    .map(|elem| match elem {
                        RespValue::BulkString(vec8) => match std::str::from_utf8(&vec8[9..]) {
                            Ok(str) => Ok(str.to_string()),
                            Err(e) => Err(actix_web::error::ErrorInternalServerError(e)),
                        },
                        _ => Err(actix_web::error::ErrorInternalServerError(
                            "invalid redis response",
                        )),
                    })
                    .collect();

                future::result(nodes)
            })
    }
}

fn extract_string(resp_value: RespValue) -> Result<String, actix_web::Error> {
    match resp_value {
        RespValue::BulkString(vec8) => match std::str::from_utf8(vec8.as_ref()) {
            Ok(str) => Ok(str.to_string()),
            Err(e) => Err(actix_web::error::ErrorInternalServerError(e)),
        },
        RespValue::SimpleString(str) => Ok(str),
        _ => Err(actix_web::error::ErrorInternalServerError(
            "invalid redis response",
        )),
    }
}

fn extract_node(
    redis_actor: Addr<Unsync, RedisActor>,
    node_id: String,
) -> impl Future<Item = HashMap<String, String>, Error = actix_web::Error> {
    redis_actor
        .send(Command(resp_array!["HGETALL", node_id]))
        .map_err(|_| actix_web::error::ErrorInternalServerError("Error sending HGETALL command."))
        .and_then(|result| {
            let resp_arr: Result<HashMap<String, String>, _> = match result {
                Ok(RespValue::Array(keys_values)) => keys_values
                    .chunks(2)
                    /*.map(|it| {
                        println!("{:?}", it);
                        it
                    })*/
                    .map(|two_elems| match two_elems {
                        &[ref key, ref val] => {
                            Ok((extract_string(key.clone())?, extract_string(val.clone())?))
                        }
                        _ => Err(actix_web::error::ErrorInternalServerError(
                            "invalid redis response",
                        )),
                    })
                    .collect(),
                _ => Err(actix_web::error::ErrorInternalServerError(
                    "invalid redis response",
                )),
            };
            resp_arr
        })
}

impl Handler<()> for ListNodesHandler {
    type Result = Box<Future<Item = HttpResponse, Error = actix_web::Error>>;

    fn handle(&mut self, _r: HttpRequest<()>) -> <Self as Handler<()>>::Result {
        let redis_actor = self.redis_actor.clone();

        Box::new(self.list_keys().and_then(move |nodes| {
            future::join_all(
                nodes
                    .into_iter()
                    .map(|node_id| {
                        extract_node(redis_actor.clone(), format!("nodeinfo.{}", node_id))
                    })
                    .collect::<Vec<_>>(),
            )
            .and_then(|results| {
                future::ok(HttpResponse::build(StatusCode::OK).body(format!("{:?}", results)))
            })
        }))
    }
}
