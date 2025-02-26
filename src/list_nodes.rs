use actix::prelude::*;
use actix_redis::RedisActor;
use actix_web::http::header::LastModified;
use actix_web::{self, http, App, HttpRequest, HttpResponse};
use futures::future;
use futures::prelude::*;
use redis_tools::*;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn route_list_nodes(
    redis_address: String,
    remove_inactive_after: Option<Duration>,
) -> impl Fn(App) -> App {
    move |app: App| {
        use actix_redis::RedisActor;
        let redis_actor = RedisActor::start(redis_address.clone());
        let redis_actor_j = redis_actor.clone();

        let csv_header = bytes::Bytes::from(CSV_FIELDS.join(",") + "\n");

        app.resource("/dump", move |r| {
            r.get().with(move |_: HttpRequest<_>| {
                let redis_iter = redis_actor.clone();
                let generated = LastModified(SystemTime::now().into());

                let csv_framed = redis_actor
                    .as_redis_handle()
                    .scan("nodeinfo.*".into(), 10)
                    .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))
                    .map(move |key_chunk| dump_csv_for_keys(&redis_iter, key_chunk))
                    .buffer_unordered(2);

                let header = csv_header.clone();
                let cvs_framed_with_header = futures::stream::once(Ok(header)).chain(csv_framed);

                Ok::<_, actix_web::Error>(
                    HttpResponse::Ok()
                        .content_type("text/x-csv")
                        .header("cache-control", "public, max-age=30")
                        .header(http::header::LAST_MODIFIED, generated)
                        .header(
                            "content-disposition",
                            "attachment; filename=\"golem-stats.csv\"",
                        )
                        .write_buffer_capacity(10240)
                        .streaming(cvs_framed_with_header),
                )
            })
        })
        .resource("/v1/nodes", move |r| {
            r.get().with(move |_: HttpRequest<_>| {
                let redis = redis_actor_j.clone();
                let redis_rem = redis_actor_j.clone();
                let remove_inactive_after = remove_inactive_after.clone();
                let now = SystemTime::now();

                let json = redis_actor_j
                    .as_redis_handle()
                    .scan_set("active_nodes".into(), 10)
                    .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))
                    .map(move |chunk| {
                        let redis = redis.clone();
                        futures::stream::iter_ok(
                            chunk
                                .into_iter()
                                .map(move |node_id| {
                                    redis
                                        .as_redis_handle()
                                        .get_hash(format!("nodeinfo.{}", node_id))
                                        .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))
                                        .and_then(|mut node| {
                                            if let Some(ip_value) = node.get_mut("ip") {
                                                *ip_value = obfuscate_ip(ip_value.to_string())
                                            }
                                            Ok((node_id, node))
                                        })
                                })
                                .into_iter()
                        )
                    })
                    .flatten()
                    .map(move |node_fut| {
                        let redis_rem = redis_rem.clone();
                        if let Some(timeout) = remove_inactive_after {
                            future::Either::B(node_fut.and_then(move |(node_id, node)| {
                                if let Some(ts) = node.get("timestamp").and_then(|s| s.parse().ok()) {
                                    let ts = UNIX_EPOCH + Duration::from_millis(ts) + timeout;
                                    if ts < now {
                                        debug!("removing: ts={:?} now={:?} tmo={:?}", ts, now, timeout);
                                        future::Either::B(
                                            redis_rem
                                                .as_redis_handle()
                                                .remove_from_set("active_nodes".into(), node_id)
                                                .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))
                                                .and_then(|_| Ok(node)))
                                    }
                                    else {
                                        future::Either::A(future::ok(node))
                                    }
                                }
                                else {
                                    future::Either::A(future::ok(node))
                                }
                            }))
                        }
                        else {
                            future::Either::A(node_fut.and_then(|(_, node)| Ok(node)))
                        }
                    })
                    .buffered(50)
/*                    .zip(futures::stream::iter_ok(0..))
                    .and_then(move |(node, idx) : (HashMap<String, String>, u64)| {


                        if idx == 0 {
                            Ok(bytes::Bytes::from(format!(
                                "[{}",
                                serde_json::to_string(&node)?
                            )))
                        } else {
                            Ok(bytes::Bytes::from(format!(
                                ",\n{}",
                                serde_json::to_string(&node)?
                            )))
                        }
                    })
                    .chain(futures::stream::once(Ok("]".into())))*/;
                let json = ::stream_utils::stream_json_array(10240, 12288,
                                                             |e| actix_web::error::ErrorInternalServerError(e.to_string()), json);

                Ok::<_, actix_web::Error>(
                    HttpResponse::Ok()
                        .content_type("application/json")
                        .header("cache-control", "public, max-age=30")
                        .streaming(json),
                )
            })
        })
    }
}

fn dump_csv_for_keys(
    redis: &Addr<Unsync, RedisActor>,
    keys: Vec<String>,
) -> impl Future<Item = bytes::Bytes, Error = actix_web::Error> {
    future::join_all(
        keys.into_iter()
            .map(|key| {
                redis
                    .as_redis_handle()
                    .get_hash(key)
                    .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))
            })
            .collect::<Vec<_>>(),
    )
    .and_then(|nodes: Vec<HashMap<String, String>>| {
        let buf = Vec::with_capacity(10240);

        let mut csv_writer = csv::Writer::from_writer(buf);
        for mut node in nodes {
            csv_writer
                .write_record(CSV_FIELDS.iter().map(|field_id| {
                    match node.remove(map_csv_field(*field_id)) {
                        Some(value) => match *field_id {
                            "ip" => obfuscate_ip(value),
                            _ => value,
                        },
                        None => String::default(),
                    }
                }))
                .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?
        }
        csv_writer.flush()?;
        Ok(csv_writer
            .into_inner()
            .map_err(|e| actix_web::error::ErrorInternalServerError(e.to_string()))?
            .into())
    })
}

/* from output column name to redis field name  */
fn map_csv_field(s: &str) -> &str {
    match s {
        "node_id" => "cliid",
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

fn obfuscate_ip<S: AsRef<str>>(s: S) -> String {
    if let Some(it) = s.as_ref().split(".").next() {
        return format!("{}.x.x.x", it);
    }
    "x.x.x.x".to_string()
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
