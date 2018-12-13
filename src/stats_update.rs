use super::get_client_ip;
use actix::prelude::*;
use actix_redis::RedisActor;
use actix_web::dev::Handler;
use actix_web::{self, AsyncResponder, HttpMessage, HttpRequest, HttpResponse};
use futures::future;
use futures::future::Future;
use serde;
use serde::de;
use serde::de::MapAccess;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer};
use serde_json::{self, Value};
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::net::IpAddr;
use std::str::FromStr;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use updater::{UpdateMap, UpdateRedis, UpdateVal, Updater};

#[derive(Deserialize, Debug)]
struct Envelope<T> {
    proto_ver: u64,
    data: T,
}

#[derive(Deserialize, Debug)]
struct ObjectEnvelope<T> {
    #[serde(rename = "type")]
    ctype: String,
    obj: T,
}

#[derive(Deserialize, Debug)]
struct GolemRequest {
    cliid: String,
    timestamp: f64, // ignored, system time will be used

    #[serde(flatten)]
    body: GolemRequestBody,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type")]
enum GolemRequestBody {
    Login {
        metadata: Option<Metadata>,

        #[serde(default)]
        protocol_versions: HashMap<String, Value>,
        sessid: Option<String>,
        nvgpu: Option<NVGPU>,

        #[serde(flatten)]
        extra: HashMap<String, Value>,
    },
    Logout {
        metadata: Option<Metadata>,

        #[serde(default)]
        protocol_versions: HashMap<String, Value>,

        #[serde(flatten)]
        extra: HashMap<String, Value>,
    },
    Stats {
        #[serde(default)]
        known_tasks: u64,
        #[serde(default)]
        supported_tasks: u64,
        #[serde(default)]
        computed_tasks: u64,
        #[serde(default)]
        tasks_with_errors: u64,
        #[serde(default)]
        tasks_with_timeout: u64,
        #[serde(default)]
        tasks_requested: u64,
        #[serde(flatten)]
        extra: HashMap<String, Value>,
    },
    VMSnapshot {
        #[serde(flatten)]
        extra: HashMap<String, Value>,
    },
    P2PSnapshot {
        #[serde(flatten)]
        extra: HashMap<String, Value>,
    },
    RequestorStats {
        #[serde(default)]
        tasks_cnt: u64,
        #[serde(default)]
        finished_task_cnt: u64,
        #[serde(default)]
        requested_subtasks_cnt: u64,
        #[serde(default)]
        collected_results_cnt: u64,
        #[serde(default)]
        verified_results_cnt: u64,
        #[serde(default)]
        timed_out_subtasks_cnt: u64,
        #[serde(default)]
        not_downloadable_subtasks_cnt: u64,
        #[serde(default)]
        failed_subtasks_cnt: u64,
        #[serde(default)]
        work_offers_cnt: u64,
        #[serde(default)]
        finished_ok_cnt: u64,
        #[serde(default)]
        finished_ok_total_time: f64,
        #[serde(default)]
        finished_with_failures_cnt: u64,
        #[serde(default)]
        finished_with_failures_total_time: f64,
        #[serde(default)]
        failed_cnt: u64,
        #[serde(default)]
        failed_total_time: f64,
    },
    TaskComputer {
        #[serde(flatten)]
        extra: HashMap<String, Value>,
    },
    NodeInfo {
        #[serde(flatten)]
        extra: HashMap<String, Value>,
    },
}

#[derive(Deserialize, Serialize, Debug)]
struct Metadata {
    net: Option<String>,
    version: Option<String>,
    #[serde(deserialize_with = "string_or_struct")]
    settings: Settings,
    #[serde(deserialize_with = "string_or_struct")]
    #[serde(default)]
    os_info: OSInfo,
    os: Option<String>, // Backwards-compatibility

    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

#[derive(Deserialize, Serialize, Debug)]
struct NVGPU {
    is_supported: bool,
}

#[derive(Deserialize, Serialize, Debug)]
struct Settings {
    start_port: Option<u16>,
    end_port: Option<u16>,
    estimated_blender_performance: Option<String>,
    estimated_lux_performance: Option<String>,
    estimated_performance: Option<f64>,
    max_memory_size: Option<f64>,
    max_price: Option<u64>,
    min_price: Option<u64>,
    max_resource_size: Option<f64>,
    node_name: Option<String>,
    num_cores: Option<u32>,

    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

impl FromStr for Settings {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        let env: ObjectEnvelope<Settings> = serde_json::from_str(s)?;
        Ok(env.obj)
    }
}

#[derive(Deserialize, Serialize, Debug, Default)]
struct OSInfo {
    platform: Option<String>,
    system: Option<String>,
    release: Option<String>,
    version: Option<String>,
    windows_edition: Option<String>,
    linux_distribution: Option<[String; 3]>,

    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

impl FromStr for OSInfo {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        if s.is_empty() {
            Ok(OSInfo::default())
        } else {
            let env: ObjectEnvelope<OSInfo> = serde_json::from_str(s)?;
            Ok(env.obj)
        }
    }
}

fn string_or_struct<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: Deserialize<'de> + FromStr<Err = serde_json::Error>,
    D: Deserializer<'de>,
{
    // This is a Visitor that forwards string types to T's `FromStr` impl and
    // forwards map types to T's `Deserialize` impl. The `PhantomData` is to
    // keep the compiler from complaining about T being an unused generic type
    // parameter. We need T in order to know the Value type for the Visitor
    // impl.
    struct StringOrStruct<T>(PhantomData<fn() -> T>);

    impl<'de, T> Visitor<'de> for StringOrStruct<T>
    where
        T: Deserialize<'de> + FromStr<Err = serde_json::Error>,
    {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or map")
        }

        fn visit_str<E>(self, value: &str) -> Result<T, E>
        where
            E: de::Error,
        {
            FromStr::from_str(value).map_err(|e| serde::de::Error::custom(e))
        }

        fn visit_map<M>(self, visitor: M) -> Result<T, M::Error>
        where
            M: MapAccess<'de>,
        {
            // `MapAccessDeserializer` is a wrapper that turns a `MapAccess`
            // into a `Deserializer`, allowing it to be used as the input to T's
            // `Deserialize` implementation. T then deserializes itself using
            // the entries from the map visitor.
            Deserialize::deserialize(de::value::MapAccessDeserializer::new(visitor))
        }
    }

    deserializer.deserialize_any(StringOrStruct(PhantomData))
}

#[derive(Serialize, Debug)]
struct NodeInfoOutput {
    cliid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sessid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ip: Option<IpAddr>,
    timestamp: u64,
    #[serde(flatten)]
    metadata: MetadataOutput,
    #[serde(flatten)]
    stats: StatsOutput,
    #[serde(flatten)]
    requestor_stats: RequestorStatsOutput,
    #[serde(flatten)]
    nvgpu: NVGPUOutput,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

#[derive(Serialize, Debug, Default)]
struct MetadataOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    net: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_blender_performance: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_lux_performance: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_performance: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_memory_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_price: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_price: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_resource_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    num_cores: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    os: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    os_system: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    os_release: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    os_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    os_windows_edition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    os_linux_distribution: Option<String>,
}

#[derive(Serialize, Debug, Default)]
struct StatsOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    known_tasks: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    supported_tasks: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tasks_requested: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tasks_with_errors: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tasks_with_timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed: Option<u64>,
}

#[derive(Serialize, Debug, Default)]
struct RequestorStatsOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_tasks_cnt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_finished_task_cnt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_requested_subtasks_cnt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_collected_results_cnt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_verified_results_cnt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_timed_out_subtasks_cnt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_not_downloadable_subtasks_cnt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_failed_subtasks_cnt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_work_offers_cnt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_finished_ok_cnt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_finished_ok_total_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_finished_with_failures_cnt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_finished_with_failures_total_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_failed_cnt: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rs_failed_total_time: Option<f64>,
}

#[derive(Serialize, Debug, Default)]
struct NVGPUOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    nvgpu_is_supported: Option<bool>,
}

fn protocol_versions_to_map(protocol_versions: &HashMap<String, Value>) -> HashMap<String, Value> {
    protocol_versions
        .iter()
        .map(|ent| (format!("protocol_version_{}", ent.0), ent.1.clone()))
        .collect()
}

fn now_in_millis() -> u64 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let secs = now.as_secs() as u64;
    let millis = (now.subsec_nanos() / 1000000) as u64;

    secs * 1000 + millis
}

// The signature of this function should be
// fn to_node_info(cliid: String, body: GolemRequestBody, ip: Option<IpAddr>) -> Option<NodeInfoOutput>
// but, tests are written in a way that makes refactoring this difficult
// TODO refactor this after tests will be executed differently
fn to_node_info(envelope: Envelope<GolemRequest>, ip: Option<IpAddr>) -> Option<NodeInfoOutput> {
    let GolemRequest { cliid, body, .. } = envelope.data;

    let timestamp = now_in_millis();

    debug!("req type: {:?}", body);

    match body {
        GolemRequestBody::Login {
            metadata,
            protocol_versions,
            sessid,
            nvgpu,
            ..
        } => Some(NodeInfoOutput {
            cliid,
            sessid,
            ip,
            timestamp,
            extra: protocol_versions_to_map(&protocol_versions),
            stats: StatsOutput::default(),
            requestor_stats: RequestorStatsOutput::default(),
            nvgpu: match nvgpu {
                None => NVGPUOutput::default(),
                Some(nvgpu) => NVGPUOutput {
                    nvgpu_is_supported: Some(nvgpu.is_supported),
                },
            },
            metadata: metadata
                .map(|m| MetadataOutput {
                    net: m.net,
                    version: m.version,
                    start_port: m.settings.start_port,
                    end_port: m.settings.end_port,
                    estimated_blender_performance: m.settings.estimated_blender_performance,
                    estimated_lux_performance: m.settings.estimated_lux_performance,
                    estimated_performance: m.settings.estimated_performance,
                    max_memory_size: m.settings.max_memory_size.map(|f| f.trunc() as u64),
                    max_price: m.settings.max_price,
                    min_price: m.settings.min_price,
                    max_resource_size: m.settings.max_resource_size.map(|f| f.trunc() as u64),
                    node_name: m.settings.node_name,
                    num_cores: m.settings.num_cores,
                    os: m.os_info.platform.or(m.os),
                    os_system: m.os_info.system,
                    os_release: m.os_info.release,
                    os_version: m.os_info.version,
                    os_windows_edition: m.os_info.windows_edition,
                    os_linux_distribution: match m.os_info.linux_distribution {
                        Some(x) => Some(x.join(" ")),
                        None => None,
                    },
                }).unwrap_or(MetadataOutput::default()),
        }),

        GolemRequestBody::RequestorStats {
            tasks_cnt,
            finished_task_cnt,
            requested_subtasks_cnt,
            collected_results_cnt,
            verified_results_cnt,
            timed_out_subtasks_cnt,
            not_downloadable_subtasks_cnt,
            failed_subtasks_cnt,
            work_offers_cnt,
            finished_ok_cnt,
            finished_ok_total_time,
            finished_with_failures_cnt,
            finished_with_failures_total_time,
            failed_cnt,
            failed_total_time,
        } => Some(NodeInfoOutput {
            cliid,
            sessid: Option::None,
            ip,
            timestamp,
            metadata: MetadataOutput::default(),
            nvgpu: NVGPUOutput::default(),
            extra: HashMap::new(),
            stats: StatsOutput::default(),
            requestor_stats: RequestorStatsOutput {
                rs_tasks_cnt: Some(tasks_cnt),
                rs_finished_task_cnt: Some(finished_task_cnt),
                rs_requested_subtasks_cnt: Some(requested_subtasks_cnt),
                rs_collected_results_cnt: Some(collected_results_cnt),
                rs_verified_results_cnt: Some(verified_results_cnt),
                rs_timed_out_subtasks_cnt: Some(timed_out_subtasks_cnt),
                rs_not_downloadable_subtasks_cnt: Some(not_downloadable_subtasks_cnt),
                rs_failed_cnt: Some(failed_cnt),
                rs_failed_subtasks_cnt: Some(failed_subtasks_cnt),
                rs_failed_total_time: Some(failed_total_time),
                rs_finished_ok_cnt: Some(finished_ok_cnt),
                rs_finished_ok_total_time: Some(finished_ok_total_time),
                rs_finished_with_failures_cnt: Some(finished_with_failures_cnt),
                rs_finished_with_failures_total_time: Some(finished_with_failures_total_time),
                rs_work_offers_cnt: Some(work_offers_cnt),
            },
        }),
        GolemRequestBody::Stats {
            known_tasks,
            supported_tasks,
            computed_tasks,
            tasks_with_errors,
            tasks_with_timeout,
            tasks_requested,
            ..
        } => Some(NodeInfoOutput {
            cliid,
            sessid: Option::None,
            ip,
            timestamp,
            metadata: MetadataOutput::default(),
            nvgpu: NVGPUOutput::default(),
            requestor_stats: RequestorStatsOutput::default(),
            extra: HashMap::default(),
            stats: StatsOutput {
                known_tasks: Some(known_tasks),
                supported_tasks: Some(supported_tasks),
                completed: Some(computed_tasks),
                tasks_with_errors: Some(tasks_with_errors),
                tasks_with_timeout: Some(tasks_with_timeout),
                tasks_requested: Some(tasks_requested),
            },
        }),

        v => {
            warn!("unsupported info: {:?}", v);
            None
        }
    }
}

pub struct UpdateHandler {
    updater: Addr<Unsync, Updater>,
}

impl UpdateHandler {
    pub fn new(redis_actor: Addr<Unsync, RedisActor>) -> UpdateHandler {
        UpdateHandler {
            updater: Updater::start(redis_actor),
        }
    }
}

#[derive(Debug)]
enum ConvertError {
    JSONError(serde_json::Error),
    InvalidJson,
}

impl From<serde_json::Error> for ConvertError {
    fn from(err: serde_json::Error) -> Self {
        ConvertError::JSONError(err)
    }
}

fn to_hash_map<T: serde::Serialize>(input: &T) -> Result<HashMap<String, String>, ConvertError> {
    if let serde_json::Value::Object(map) = serde_json::to_value(input)? {
        Ok(map
            .iter()
            .filter_map(|(k, v)| match v {
                serde_json::Value::String(s) => Some((k.clone(), s.clone())),
                serde_json::Value::Number(n) => Some((k.clone(), n.to_string())),
                serde_json::Value::Bool(b) => Some((k.clone(), format!("{}", b))),
                _ => None,
            }).collect())
    } else {
        Err(ConvertError::InvalidJson)
    }
}

fn push_node_info(
    updater: &Addr<Unsync, Updater>,
    node_info: &NodeInfoOutput,
) -> Box<Future<Item = HttpResponse, Error = actix_web::Error>> {
    debug!("nodeinfo {:?}", &node_info);

    if let Ok(map) = to_hash_map(&node_info) {
        let msg = UpdateRedis::UpdateRedisMap(UpdateMap {
            collection: "nodeinfo".to_string(),
            key: node_info.cliid.clone(),
            value: map,
        });
        push_msg_to_redis(updater, msg)
    } else {
        Box::new(future::err(actix_web::error::ErrorInternalServerError(
            "gen node_info",
        )))
    }
}

fn push_p2pstats(
    cliid: String,
    updater: &Addr<Unsync, Updater>,
    value: String,
) -> Box<Future<Item = HttpResponse, Error = actix_web::Error>> {
    debug!("p2pstats {:?}", &value);
    let msg = UpdateRedis::UpdateRedisVal(UpdateVal {
        collection: "p2pstats".to_string(),
        key: cliid.to_string(),
        value: value.to_string(),
    });

    push_msg_to_redis(updater, msg)
}

fn push_msg_to_redis(
    updater: &Addr<Unsync, Updater>,
    update_msg: UpdateRedis,
) -> Box<Future<Item = HttpResponse, Error = actix_web::Error>> {
    Box::new(
        updater
            .send(update_msg)
            .map_err(|_e| actix_web::error::ErrorInternalServerError("send error"))
            .and_then(|r| match r {
                Ok(_v) => future::ok(HttpResponse::Ok().into()),
                Err(e) => future::err(actix_web::error::ErrorInternalServerError(format!(
                    "save: {}",
                    e
                ))),
            }),
    )
}

impl Handler<()> for UpdateHandler {
    type Result = Box<Future<Item = HttpResponse, Error = actix_web::Error>>;

    fn handle(&mut self, req: HttpRequest<()>) -> <Self as Handler<()>>::Result {
        let updater = self.updater.clone();
        let client_ip = get_client_ip(&req);

        if let Some(ip) = client_ip {
            debug!("client IP {:?}", ip)
        } else {
            info!("no client IP")
        }

        req.json()
            .from_err()
            .and_then(move |envelope: Envelope<GolemRequest>| {
                if let GolemRequest {
                    cliid,
                    body: GolemRequestBody::P2PSnapshot { extra },
                    ..
                } = envelope.data
                {
                    match serde_json::to_string(&extra) {
                        Ok(extra) => push_p2pstats(cliid, &updater, extra),
                        Err(_e) => Box::new(future::ok(HttpResponse::Ok().into())), // This branch will never be executed
                    }
                } else {
                    match to_node_info(envelope, client_ip) {
                        Some(node_info) => push_node_info(&updater, &node_info),
                        None => Box::new(future::ok(HttpResponse::Ok().into())),
                    }
                }
            }).or_else(|e: actix_web::Error| {
                let mut resp = e.as_response_error().error_response();
                warn!("processing request, error={:?}", &e);
                resp.set_body(format!("{}", e));
                future::ok(resp)
            }).responder()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn parse_login() {
        let input = include_str!("../test/login.json");

        let r: Envelope<GolemRequest> = serde_json::from_str(input).unwrap();

        {
            println!("envelop {:?}", &r);
            let settings: &Settings = match &r.data.body {
                &GolemRequestBody::Login { ref metadata, .. } => {
                    &metadata.as_ref().unwrap().settings
                }
                _ => panic!("login expected"),
            };
            assert_eq!(&settings.extra["use_ipv6"], &json!(0));
        }

        let output = to_node_info(r, None).unwrap();
        println!(
            "pretty json {}",
            serde_json::to_string_pretty(&output).unwrap()
        );
        assert_eq!(output.nvgpu.nvgpu_is_supported, None);
    }

    #[test]
    fn parse_login_nvgpu() {
        let input = include_str!("../test/login-nvgpu.json");

        let r: Envelope<GolemRequest> = serde_json::from_str(input).unwrap();
        let output = to_node_info(r, None).unwrap();
        println!(
            "pretty json {}",
            serde_json::to_string_pretty(&output).unwrap()
        );
        assert_eq!(output.nvgpu.nvgpu_is_supported.unwrap(), true);
    }

    #[test]
    fn parse_login_legacy() {
        let input = include_str!("../test/login-legacy.json");
        let _result: Envelope<GolemRequest> = serde_json::from_str(input).unwrap();
    }

    #[test]
    fn parse_login_max_memory_size_f64() {
        let input = include_str!("../test/login-f64.json");
        let _result: Envelope<GolemRequest> = serde_json::from_str(input).unwrap();
    }

    #[test]
    fn parse_login_max_resource_size_f64() {
        let input = include_str!("../test/login-resource-float.json");
        let _result: Envelope<GolemRequest> = serde_json::from_str(input).unwrap();
    }

    #[test]
    fn parse_stats() {
        let input = include_str!("../test/stats.json");
        let output = to_node_info(serde_json::from_str(input).unwrap(), None);
        println!(
            "pretty json {}",
            serde_json::to_string_pretty(&output.unwrap()).unwrap()
        );
    }

    #[test]
    fn parse_requestor_stats() {
        let input = include_str!("../test/requestor-stats.json");
        let output = to_node_info(serde_json::from_str(input).unwrap(), None);
        println!(
            "pretty json {}",
            serde_json::to_string_pretty(&output.unwrap()).unwrap()
        );
    }

    #[test]
    fn parse_stats_output() {
        let input = include_str!("../test/stats.json");
        let map = to_hash_map(&to_node_info(serde_json::from_str(input).unwrap(), None)).unwrap();
        println!("output map {:?}", map);
        assert_eq!(map.get("tasks_requested").unwrap(), "22518");
    }

    #[test]
    fn parse_requestor_stats_output() {
        let input = include_str!("../test/requestor-stats.json");
        let map = to_hash_map(&to_node_info(serde_json::from_str(input).unwrap(), None)).unwrap();
        println!("output map {:?}", map);
        assert_eq!(map.get("rs_finished_ok_total_time").unwrap(), "3.14");
    }
}
