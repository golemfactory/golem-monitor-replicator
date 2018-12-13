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

/*
1) "min_price"
 2) "100000000000000000"
 3) "num_cores"
 4) "7"
 5) "known_tasks"
 6) "29"
 7) "rs_failed_total_time"
 8) "0"
 9) "rs_finished_with_failures_cnt"
10) "0"
11) "protocol_version_monitor"
12) "1"
13) "computation_time_failure"
14) "631963"
15) "timestamp"
16) "1544525105011"
17) "tasks_with_timeout"
18) "69"
19) "expenditure"
20) "0"
21) "rs_finished_ok_cnt"
22) "0"
23) "completed"
24) "1632"
25) "rs_work_offers_cnt"
26) "0"
27) "tasks_requested"
28) "461679"
29) "computation_time_success"
30) "1663344"
31) "rs_failed_subtasks_cnt"
32) "0"
33) "rs_finished_task_cnt"
34) "0"
35) "EMA_computation_time_failure"
36) "2774.4831908156843"
37) "income"
38) "0"
39) "node_name"
40) "grunt"
41) "EMA_computation_time_success"
42) "2716.9542692225373"
43) "sessid"
44) "327a1434-0899-4883-876d-d6e331442b3b"
45) "version"
46) "0.16.1"
47) "net"
48) "mainnet"
49) "max_price"
50) "100000000000000000000"
51) "rs_verified_results_cnt"
52) "0"
53) "max_resource_size"
54) "2097152"
55) "rs_not_downloadable_subtasks_cnt"
56) "0"
57) "ip"
58) "206.174.118.78"
59) "tasks_with_errors"
60) "183"
61) "max_memory_size"
62) "9437184"
63) "supported_tasks"
64) "0"
65) "rs_finished_with_failures_total_time"
66) "0"
67) "rs_tasks_cnt"
68) "0"
69) "rs_collected_results_cnt"
70) "0"
71) "rs_requested_subtasks_cnt"
72) "0"
73) "start_port"
74) "40102"
75) "cliid"
76) "0022abe5111a3679242a906d03ac3fef4c3d903c8d7be1a4f089fe15eea9063615e975a833989ffd4cee2e42d758ad27f616bab8ead9564dad05ad068ebe556d"
77) "protocol_version_task"
78) "27"
79) "rs_failed_cnt"
80) "0"
81) "rs_timed_out_subtasks_cnt"
82) "0"
83) "rs_finished_ok_total_time"
84) "0"
85) "os"
86) "win32"
87) "protocol_version_p2p"
88) "27"
89) "end_port"
90) "60102"
*/

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
