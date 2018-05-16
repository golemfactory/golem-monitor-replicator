
use std::collections::HashMap;
use serde_json::{Value, self};
use serde::{Deserialize, Deserializer};
use std::str::FromStr;
use std::marker::PhantomData;
use serde::de::Visitor;
use serde::de::MapAccess;
use serde::de;
use std::fmt;

#[derive(Deserialize, Debug)]
struct Envelope<T> {
    proto_ver: u64,
    data : T
}

#[derive(Deserialize, Debug)]
struct ObjectEnvelope<T> {
    #[serde(rename = "type")]
    ctype: String,
    obj : T
}

#[derive(Deserialize, Debug)]
struct StatsRequest {
    cliid : String,
    timestamp : f64,
    sessid : String,

    #[serde(flatten)]
    body : StatsRequestBody
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
enum StatsRequestBody {
    Login {
        metadata : Option<Metadata>,

        #[serde(default)]
        protocol_versions: HashMap<String, Value>,

        #[serde(flatten)]
        extra: HashMap<String, Value>,
    },
    Logout {
        metadata : Option<Metadata>,

        #[serde(default)]
        protocol_versions: HashMap<String, Value>,

        #[serde(flatten)]
        extra: HashMap<String, Value>,
    },
    Stats {
        #[serde(default)]
        known_tasks : u64,
        #[serde(default)]
        supported_tasks : u64,
        #[serde(default)]
        computed_tasks : u64,
        #[serde(default)]
        tasks_with_errors : u64,
        #[serde(default)]
        tasks_with_timeout : u64,
        #[serde(default)]
        tasks_requested : u64,
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
        tasks_cnt : u64,
        #[serde(default)]
        finished_task_cnt : u64,
        #[serde(default)]
        requested_subtasks_cnt : u64,
        #[serde(default)]
        collected_results_cnt : u64,
        #[serde(default)]
        verified_results_cnt : u64,
        #[serde(default)]
        timed_out_subtasks_cnt : u64,
        #[serde(default)]
        not_downloadable_subtasks_cnt : u64,
        #[serde(default)]
        failed_subtasks_cnt : u64,
        #[serde(default)]
        work_offers_cnt : u64,
        #[serde(default)]
        finished_ok_cnt : u64,
        #[serde(default)]
        finished_ok_total_time : f64,
        #[serde(default)]
        finished_with_failures_cnt :u64,
        #[serde(default)]
        finished_with_failures_total_time : f64,
        #[serde(default)]
        failed_cnt : u64,
        #[serde(default)]
        failed_total_time : f64
    },

    TaskComputer {
        #[serde(flatten)]
        extra: HashMap<String, Value>,
    },
    NodeInfo {
        #[serde(flatten)]
        extra: HashMap<String, Value>,
    }

}

#[derive(Deserialize, Debug)]
struct Metadata {
    net : Option<String>,
    os : Option<String>,
    version : Option<String>,
    #[serde(deserialize_with = "string_or_struct")]
    settings : Settings,

    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

#[derive(Deserialize, Debug)]
struct NodeInfo {
    cliid : String,
    timestamp : u64,
    ip : Option<String>,
    node_name : Option<String>,
}

#[derive(Deserialize, Debug)]
struct Settings {
    start_port : Option<u16>,
    end_port: Option<u16>,
    estimated_blender_performance : Option<String>,
    estimated_lux_performance : Option<String>,
    estimated_performance : Option<f64>,
    max_memory_size : Option<u64>,
    max_price : Option<u64>,
    min_price: Option<u64>,
    max_resource_size : Option<u64>,
    node_name : Option<String>,
    num_cores: Option<u32>,

    #[serde(flatten)]
    extra: HashMap<String, Value>
}

impl FromStr for Settings {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        let env : ObjectEnvelope<Settings> = serde_json::from_str(s)?;
        Ok(env.obj)
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
            Ok(FromStr::from_str(value).unwrap())
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
struct OutputRecord {
    cliid : String,
    timestamp : f64,
    #[serde(flatten)]
    metadata : MedataOutput,

    #[serde(flatten)]
    stats : StatsOutput,


    #[serde(flatten)]
    requestor_stats : RequestorStatsOutput,

    #[serde(flatten)]
    extra : HashMap<String, Value>,
}

#[derive(Serialize, Debug, Default)]
struct MedataOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    net : Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    os : Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version : Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_port : Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_blender_performance : Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_lux_performance : Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_performance : Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_memory_size : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_price : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_price: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_resource_size : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_name : Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    num_cores: Option<u32>
}

#[derive(Serialize, Debug, Default)]
struct StatsOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    known_tasks : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    supported_tasks : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tasks_requested : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tasks_with_errors : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tasks_with_timeout : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed: Option<u64>
}

#[derive(Serialize, Debug, Default)]
struct RequestorStatsOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    req_tasks_cnt : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    req_finished_task_cnt : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    req_requested_subtasks_cnt : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    req_collected_results_cnt : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    req_verified_results_cnt : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    req_timed_out_subtasks_cnt : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    req_not_downloadable_subtasks_cnt : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    req_failed_subtasks_cnt : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    req_work_offers_cnt : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    req_finished_ok_cnt : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    req_finished_ok_total_time : Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    req_finished_with_failures_cnt : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    req_finished_with_failures_total_time : Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    req_failed_cnt : Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    req_failed_total_time : Option<f64>
}

fn protocol_versions_to_map(protocol_versions: &HashMap<String, Value>) -> HashMap<String, Value> {
    protocol_versions.iter().map(|ent |
        (format!("protocol_versions_{}", ent.0), ent.1.clone()))
        .collect()
}

fn get_update_node(env: Envelope<StatsRequest>) -> Option<OutputRecord> {
    let StatsRequest { cliid, timestamp, sessid: _, body } = env.data;


    match body {
        StatsRequestBody::Login { metadata, protocol_versions, .. } =>
            Some(OutputRecord {
                cliid,
                timestamp,
                extra: protocol_versions_to_map(&protocol_versions),
                stats: StatsOutput::default(),
                requestor_stats: RequestorStatsOutput::default(),
                metadata: metadata.map(|m| MedataOutput {
                    net: m.net,
                    os: m.os,
                    version: m.version,
                    start_port: m.settings.start_port,
                    end_port: m.settings.end_port,
                    estimated_blender_performance: m.settings.estimated_blender_performance,
                    estimated_lux_performance: m.settings.estimated_lux_performance,
                    estimated_performance: m.settings.estimated_performance,
                    max_memory_size: m.settings.max_memory_size,
                    max_price: m.settings.max_price,
                    min_price: m.settings.min_price,
                    max_resource_size: m.settings.max_resource_size,
                    node_name: m.settings.node_name,
                    num_cores: m.settings.num_cores,
                }).unwrap_or(MedataOutput::default()),
            }),

        StatsRequestBody::RequestorStats {
            tasks_cnt, finished_task_cnt, requested_subtasks_cnt,
            collected_results_cnt, verified_results_cnt,
            timed_out_subtasks_cnt,
            not_downloadable_subtasks_cnt,
            failed_subtasks_cnt,
            work_offers_cnt,
            finished_ok_cnt,
            finished_ok_total_time,
            finished_with_failures_cnt,
            finished_with_failures_total_time,
            failed_cnt,
            failed_total_time
        } => Some(OutputRecord {
            cliid,
            timestamp,
            metadata: MedataOutput::default(),
            extra: HashMap::new(),
            stats: StatsOutput::default(),
            requestor_stats: RequestorStatsOutput {
                req_tasks_cnt: Some(tasks_cnt),
                req_finished_task_cnt: Some(finished_task_cnt),
                req_requested_subtasks_cnt: Some(requested_subtasks_cnt),
                req_collected_results_cnt: Some(collected_results_cnt),
                req_verified_results_cnt: Some(verified_results_cnt),
                req_timed_out_subtasks_cnt: Some(timed_out_subtasks_cnt),
                req_not_downloadable_subtasks_cnt: Some(not_downloadable_subtasks_cnt),
                req_failed_cnt: Some(failed_cnt),
                req_failed_subtasks_cnt: Some(failed_subtasks_cnt),
                req_failed_total_time: Some(failed_total_time),
                req_finished_ok_cnt: Some(finished_ok_cnt),
                req_finished_ok_total_time: Some(finished_ok_total_time),
                req_finished_with_failures_cnt: Some(finished_with_failures_cnt),
                req_finished_with_failures_total_time: Some(finished_with_failures_total_time),
                req_work_offers_cnt: Some(work_offers_cnt),
            },
        }),
        StatsRequestBody::Stats {
            known_tasks,
            supported_tasks,
            computed_tasks,
            tasks_with_errors,
            tasks_with_timeout,
            tasks_requested,
            ..
        } => Some(OutputRecord {
            cliid,
            timestamp,
            metadata: MedataOutput::default(),
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
            warn!("unsuported info: {:?}", v);
            None
        }
    }
}

use actix_web::dev::Handler;
use actix_web::{HttpResponse, HttpRequest, AsyncResponder, HttpMessage, self};
use actix_redis::{RedisActor};
use actix::prelude::*;
use futures::future::Future;
use futures::{future};
use updater::{Updater, UpdateKey};

pub struct UpdateHandler {
    updater : Addr<Unsync, Updater>
}

impl UpdateHandler {
    pub fn new(redis_actor : Addr<Unsync, RedisActor>) -> UpdateHandler {


        UpdateHandler {
            updater : Updater::start(redis_actor)
        }
    }

}

fn update_kv(updater : &Addr<Unsync, Updater>, output : &OutputRecord) -> Box<Future<Item = HttpResponse, Error=actix_web::Error>> {
    let key = format!("node.{}", output.cliid);
    //let script = "local t1 = cjson.decode(redis.call('get', ARGV[1])); local t2 = cjson.decode(ARGV[2]); for k,v in pairs(t2) do t1[k] = v end; return redis.call('set', ARGV[1], cjson.encode(t1));";
    if let Ok(json) = serde_json::to_string(&output) {
        return Box::new(
            updater.send(UpdateKey { key, value: json })
                .map_err(|_e| actix_web::error::ErrorInternalServerError("send error"))
                .and_then(|r| match r {
                    Ok(_v) => {
                        future::ok(HttpResponse::Ok().into())
                    },
                    Err(e) => future::err(actix_web::error::ErrorInternalServerError(format!("save: {}", e)))
                })
        )
    }

    Box::new(future::err(actix_web::error::ErrorInternalServerError("gen output")))
}

impl Handler<()> for UpdateHandler {
    type Result = Box<Future<Item = HttpResponse, Error=actix_web::Error>>;

    fn handle(&mut self, req: HttpRequest<()>) -> <Self as Handler<()>>::Result {
        let updater = self.updater.clone();

        req.json()
            .map_err(|e| actix_web::error::ErrorBadRequest(format!("{}", e)))
            .and_then(|b:Envelope<StatsRequest>|
                Ok(get_update_node(b))
            )
            .and_then(move |r| {
                let out : Box<Future<Item = HttpResponse, Error=actix_web::Error>> = match r {
                    Some(output) => update_kv(&updater, &output),
                    None => Box::new(future::ok(HttpResponse::Ok().into()))
                };
                out
            }).or_else(|e : actix_web::Error| {
                let mut resp = e.cause().error_response();
                warn!("processing request, error={:?}", &e);
                resp.set_body(format!("{}",e));
                future::ok(resp)
            })
            .responder()

    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use serde_json;

    #[test]
    fn parse_login() {
        let input = include_str!("../test/login.json");


        let r: Envelope<StatsRequest> = serde_json::from_str(input).unwrap();

        {
            println!("{:?}", &r);
            let settings: &Settings = match &r.data.body {
                &StatsRequestBody::Login { ref metadata, .. } => &metadata.as_ref().unwrap().settings,
                _ => panic!("login expected")
            };
            assert_eq!(&settings.extra["use_ipv6"], &json!(0));
        }

        let output = get_update_node(r);
        println!("{}", serde_json::to_string_pretty(&output.unwrap()).unwrap());
    }

    #[test]
    fn parse_stats() {
        let input = include_str!("../test/stats.json");


        let r: Envelope<StatsRequest> = serde_json::from_str(input).unwrap();

        let output = get_update_node(r);
        println!("{}", serde_json::to_string_pretty(&output.unwrap()).unwrap());
    }


    #[test]
    fn parse_requestor_stats() {
        let input = include_str!("../test/requestor-stats.json");


        let r: Envelope<StatsRequest> = serde_json::from_str(input).unwrap();

        let output = get_update_node(r);
        println!("{}", serde_json::to_string_pretty(&output.unwrap()).unwrap());
    }

}