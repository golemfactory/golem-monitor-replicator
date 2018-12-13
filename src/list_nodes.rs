use actix::{fut::*, prelude::*};
use actix_redis::{Command, RedisActor, RespValue};
use actix_web::dev::Handler;
use actix_web::{self, HttpRequest, HttpResponse};
use futures::future;
use futures::prelude::*;
use http::StatusCode;
use std::collections::HashMap;

/* from output column name to redis field name  */
fn map_csv_field(s: &str) -> &str {
    match s {
        "last_seen" => "timestamp",
        "performance_general" => "estimated_performance",
        "performance_lux" => "estimated_lux_performance",
        "performance_blender" => "estimated_blender_performance",
        "allowed_resource_size" => "max_resource_size",
        "allowed_resource_memory" => "max_memory_size",
        "cpu_cores" => "num_cores",
        "subtasks_success" => "completed",
        "subtasks_error" => "tasks_with_errors",
        "subtasks_timeout" => "tasks_with_timeout",
        "task_protocol_version" => "protocol_version_task",
        "p2p_protocol_version" => "protocol_version_p2p",
        v => v,
    }
}

fn obfuscate_ip(s: String) -> String {
    if let Some(it) = s.split(".").next() {
        return format!("{}.x.x.x", it);
    }
    s
}

static CSV_FIELDS: &[&str] = &[
    "node_id",
    "node_name",
    "version",
    "last_seen",
    "os",
    "os_system",
    "os_release",
    "os_version",
    "os_windows_edition",
    "os_linux_distribution",
    "ip",
    "start_port",
    "end_port",
    "performance_general",
    "performance_blender",
    "performance_lux",
    "allowed_resource_size",
    "allowed_resource_memory",
    "cpu_cores",
    "min_price",
    "max_price",
    "subtasks_success",
    "subtasks_error",
    "subtasks_timeout",
    "p2p_protocol_version",
    "task_protocol_version",
    "tasks_requested",
    "known_tasks",
    "supported_tasks",
    "rs_tasks_cnt",
    "rs_finished_task_cnt",
    "rs_requested_subtasks_cnt",
    "rs_collected_results_cnt",
    "rs_verified_results_cnt",
    "rs_timed_out_subtasks_cnt",
    "rs_not_downloadable_subtasks_cnt",
    "rs_failed_subtasks_cnt",
    "rs_work_offers_cnt",
    "rs_finished_ok_cnt",
    "rs_finished_ok_total_time",
    "rs_finished_with_failures_cnt",
    "rs_finished_with_failures_total_time",
    "rs_failed_cnt",
    "rs_failed_total_time",
];

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
        .send(Command(resp_array!["HGETALL", node_id.clone()]))
        .map_err(|_| actix_web::error::ErrorInternalServerError("Error sending HGETALL command."))
        .and_then(move |result| {
            let resp_arr: Result<HashMap<String, String>, _> = match result {
                Ok(RespValue::Array(keys_values)) => keys_values
                    .chunks(2)
                    .map(|two_elems| match two_elems {
                        &[ref key, ref val] => {
                            Ok((extract_string(key.clone())?, extract_string(val.clone())?))
                        }
                        _ => Err(actix_web::error::ErrorInternalServerError(
                            "invalid redis response",
                        )),
                    })
                    .chain(std::iter::once(Ok((
                        "node_id".to_string(),
                        String::from(&node_id[9..]),
                    ))))
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
                let mut csv_writer = csv::Writer::from_writer(vec![]);

                match csv_writer.write_record(CSV_FIELDS) {
                    Err(e) => {
                        return future::err(actix_web::error::ErrorInternalServerError(
                            e.to_string(),
                        ))
                    }
                    _ => (),
                }
                for mut row in results {
                    let csv_record = CSV_FIELDS.into_iter().map(|column_name| {
                        match row.remove(map_csv_field(*column_name)) {
                            Some(value) => {
                                if *column_name == "ip" {
                                    obfuscate_ip(value)
                                } else {
                                    value
                                }
                            }
                            None => "".to_string(),
                        }
                    });
                    match csv_writer.write_record(csv_record) {
                        Err(e) => {
                            return future::err(actix_web::error::ErrorInternalServerError(
                                e.to_string(),
                            ))
                        }
                        _ => (),
                    }
                }
                let body_text = match csv_writer.into_inner() {
                    Ok(buf) => match String::from_utf8(buf) {
                        Ok(str) => str,
                        Err(e) => {
                            return future::err(actix_web::error::ErrorInternalServerError(
                                e.to_string(),
                            ))
                        }
                    },
                    Err(e) => {
                        return future::err(actix_web::error::ErrorInternalServerError(
                            e.to_string(),
                        ))
                    }
                };
                future::ok(HttpResponse::build(StatusCode::OK).body(body_text))
            })
        }))
    }
}
