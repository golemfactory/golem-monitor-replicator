use actix::prelude::*;
use actix_redis::{Command, RedisActor, RespValue};
use actix_web::dev::Handler;
use actix_web::{self, HttpMessage, HttpRequest, HttpResponse};
use futures::future;
use futures::prelude::*;
use http::StatusCode;

pub enum ListType {
    ExportCSV,
    JSON,
}

pub struct ListNodesHandler {
    list_creator: Addr<Unsync, RedisActor>,
    list_type: ListType,
}

impl ListNodesHandler {
    pub fn new(redis_actor: Addr<Unsync, RedisActor>, list_type: ListType) -> ListNodesHandler {
        ListNodesHandler {
            list_creator: redis_actor,
            list_type: list_type,
        }
    }
}

impl Handler<()> for ListNodesHandler {
    type Result = Box<Future<Item = HttpResponse, Error = actix_web::Error>>;

    fn handle(&mut self, _r: HttpRequest<()>) -> <Self as Handler<()>>::Result {
        Box::new(
            self.list_creator
                .send(Command(resp_array!["KEYS", "nodeinfo.*"]))
                .map_err(|_e| {
                    actix_web::error::ErrorInternalServerError("Error sending KEYS command.")
                })
                .and_then(|redis_answer| {
                    future::ok(
                        HttpResponse::build(StatusCode::OK).body(match redis_answer {
                            Ok(RespValue::Array(all_nodes)) => {
                                let converted_iter =
                                    all_nodes.iter().map(|arr_elem: &RespValue| match arr_elem {
                                        RespValue::BulkString(vec8) => {
                                            match String::from_utf8((&vec8[9..]).to_vec()) {
                                                Ok(str) => str,
                                                _ => "Expected UTF8 bytes.".to_string(),
                                            }
                                        }
                                        _ => "Expected BulkString.".to_string(),
                                    });
                                let result : Vec<String> = converted_iter.collect();
                                "node_id\n".to_string() + &result.join("\n")
                            }
                            _ => "Expected array.".to_string(),
                        }),
                    )
                }),
        )
    }
}
