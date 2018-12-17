use actix::prelude::*;
use actix_redis::{Command, RedisActor, RespError, RespValue};
use futures::prelude::*;
use std::collections::HashMap;
use std::time::Duration;

pub trait RespValueExt: Sized {
    type Error;

    fn into_string(self) -> Result<String, Self::Error>;

    fn into_pair(self) -> Result<(Self, Self), Self::Error>;

    fn into_vec(self) -> Result<Vec<Self>, Self::Error>;
}

impl RespValueExt for RespValue {
    type Error = RespError;

    fn into_string(self) -> Result<String, RespError> {
        match self {
            RespValue::BulkString(vec8) => match std::str::from_utf8(vec8.as_ref()) {
                Ok(str) => Ok(str.to_string()),
                Err(e) => Err(RespError::Internal(format!("utf-8 format: {}", e))),
            },
            RespValue::SimpleString(str) => Ok(str),
            _ => Err(RespError::Internal("Invalid response".into())),
        }
    }

    fn into_pair(self) -> Result<(Self, Self), RespError> {
        match self {
            RespValue::Array(mut arr) => {
                if arr.len() >= 2 {
                    Ok((
                        std::mem::replace(&mut arr[0], RespValue::Nil),
                        std::mem::replace(&mut arr[1], RespValue::Nil),
                    ))
                } else {
                    Err(RespError::Internal("pair expected".into()))
                }
            }
            _ => Err(RespError::Internal("pair expected".into())),
        }
    }

    fn into_vec(self) -> Result<Vec<Self>, Self::Error> {
        match self {
            RespValue::Array(v) => Ok(v),
            _ => Err(RespError::Internal("array expected".into())),
        }
    }
}

pub trait AsRedisHandle {
    fn as_redis_handle(&self) -> RedisHandle<'_>;
}

pub struct RedisHandle<'a> {
    actor: &'a Addr<Unsync, RedisActor>,
}

impl AsRedisHandle for Addr<Unsync, RedisActor> {
    fn as_redis_handle(&self) -> RedisHandle<'_> {
        RedisHandle { actor: self }
    }
}

impl<'a> RedisHandle<'a> {
    pub fn scan(
        &self,
        pattern: String,
        count: usize,
    ) -> impl Stream<Item = Vec<String>, Error = actix_redis::RespError> {
        let actor = self.actor.clone();

        scan_with_query(move |cursor| {
            actor
                .send(Command(resp_array![
                    "SCAN",
                    cursor.to_string(),
                    "MATCH",
                    pattern.clone(),
                    "COUNT",
                    count.to_string()
                ]))
                .timeout(Duration::from_secs(2))
        })
    }

    pub fn scan_set(
        &self,
        set_key: String,
        count: usize,
    ) -> impl Stream<Item = Vec<String>, Error = RespError> {
        let actor = self.actor.clone();
        scan_with_query(move |cursor| {
            actor
                .send(Command(resp_array![
                    "SSCAN",
                    set_key.to_string(),
                    cursor.to_string(),
                    "COUNT",
                    count.to_string()
                ]))
                .timeout(Duration::from_secs(2))
                .map_err(|e| {
                    match e {
                        MailboxError::Timeout => error!("timeout on scan set"),
                        MailboxError::Closed => error!("closed"),
                    }
                    e
                })
        })
    }

    pub fn get_hash(
        &self,
        key: String,
    ) -> impl Future<Item = HashMap<String, String>, Error = RespError> {
        self.actor
            .send(Command(resp_array!["HGETALL", key]))
            .timeout(Duration::from_secs(5))
            .map_err(|_e| RespError::Internal("mailbox".into()))
            .and_then(|r| {
                r.map_err(|e| RespError::Internal(format!("{}", e)))?
                    .into_vec()?
                    .chunks(2)
                    .map(|chunk| match chunk {
                        &[ref key, ref val] => {
                            Ok((key.clone().into_string()?, val.clone().into_string()?))
                        }
                        _ => Err(RespError::Internal("pair expected".into())),
                    })
                    .collect::<Result<HashMap<String, String>, _>>()
            })
    }
}

struct ScanStream<Fetch, FetchFut> {
    fut: Option<FetchFut>,
    poll_fn: Fetch,
}

fn scan_with_query<QueryBuilder, QueryBuilderResult>(
    builder: QueryBuilder,
) -> impl Stream<Item = Vec<String>, Error = RespError>
where
    QueryBuilder: Fn(u64) -> QueryBuilderResult,
    QueryBuilderResult: Future<Item = Result<RespValue, actix_redis::Error>, Error = MailboxError>,
{
    ScanStream::new(move |cursor| {
        builder(cursor)
            .map_err(|_e| RespError::Internal("mailbox".into()))
            .and_then(|result| match result {
                Ok(val) => {
                    let (cursor_val, data_val) = val.into_pair()?;

                    Ok::<_, RespError>((
                        cursor_val
                            .into_string()?
                            .parse::<u64>()
                            .map_err(|e| RespError::Unexpected(Box::new(e)))?,
                        data_val
                            .into_vec()?
                            .into_iter()
                            .map(|v| v.into_string())
                            .collect::<Result<Vec<String>, RespError>>()?,
                    ))
                }
                Err(e) => Err(RespError::Internal(format!("{}", e))),
            })
    })
}

impl<Fetch, FetchFut> Stream for ScanStream<Fetch, FetchFut>
where
    Fetch: Fn(u64) -> FetchFut,
    FetchFut: Future<Item = (u64, Vec<String>), Error = RespError>,
{
    type Item = Vec<String>;
    type Error = RespError;

    fn poll(&mut self) -> Result<Async<Option<<Self as Stream>::Item>>, <Self as Stream>::Error> {
        if let Some(mut fut) = self.fut.take() {
            match fut.poll() {
                Ok(Async::NotReady) => {
                    self.fut = Some(fut);
                    return Ok(Async::NotReady);
                }
                Ok(Async::Ready((cursor, data))) => {
                    if cursor != 0 {
                        self.fut = Some((self.poll_fn)(cursor));
                    }
                    return Ok(Async::Ready(Some(data)));
                }
                Err(e) => return Err(e),
            }
        } else {
            return Ok(Async::Ready(None));
        }
    }
}

impl<Fetch, FetchFut> ScanStream<Fetch, FetchFut>
where
    Fetch: Fn(u64) -> FetchFut,
    FetchFut: Future<Item = (u64, Vec<String>), Error = RespError>,
{
    fn new(poll_fn: Fetch) -> Self {
        let fut = Some(poll_fn(0));
        ScanStream { fut, poll_fn }
    }
}

#[cfg(test)]
#[cfg(feature = "local-test")]
mod tests {
    use super::*;
    use actix::fut;
    use futures::prelude::*;

    #[test]
    fn test_scan_set() {
        let mut sys = System::new("test");

        let actor = RedisActor::start("127.0.0.1:6379");

        eprintln!("starting");
        let _ = sys.run_until_complete(futures::future::lazy(|| {
            actor
                .as_redis_handle()
                .scan_set("active_nodes".into(), 20)
                .map(move |data| {
                    let ref2 = actor.clone();
                    futures::future::join_all(
                        data.into_iter()
                            .map(move |node_id| {
                                ref2.as_redis_handle()
                                    .get_hash(format!("nodeinfo.{}", node_id))
                            })
                            .collect::<Vec<_>>(),
                    )
                    .into_stream()
                })
                .flatten()
                .fold(0, |p, nodes| {
                    eprintln!("start chunk");
                    let n = p + nodes.len();

                    for key in nodes {
                        println!("\tk={:?}", key)
                    }
                    eprintln!("end chunk");
                    Ok::<_, RespError>(n)
                })
                .and_then(|n| Ok(eprintln!("total={}", n)))
                .map_err(|_| ())
        }));
    }

}
