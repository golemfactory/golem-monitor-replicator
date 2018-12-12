use actix::prelude::*;
use actix_redis::{Command, RedisActor, RespError, RespValue};
use futures::prelude::*;

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
        RedisHandle {
            actor: self
        }
    }
}

impl<'a> RedisHandle<'a> {
    pub fn scan(
        &self,
        pattern: String,
        count: usize,
    ) -> impl Stream<Item = Vec<String>, Error = RespError> {
        let actor = self.actor.clone();
        ScanStream::new(move |cursor| {
            actor
                .send(Command(resp_array![
                    "SCAN",
                    cursor.to_string(),
                    "MATCH",
                    pattern.clone(),
                    "COUNT",
                    count.to_string()
                ]))
                .map_err(|e| RespError::Internal("mailbox".into()))
                .and_then(|result| match result {
                    Ok(val) => {
                        let (cursor_val, data_val) = val.into_pair()?;

                        Ok((
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
}

struct ScanStream<Fetch, FetchFut> {
    fut: Option<FetchFut>,
    poll_fn: Fetch,
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
mod test {
    use futures::prelude::*;
    use actix::fut;
    use super::*;


    //#[test]
    fn test() {
        let mut sys = System::new("test");

        let actor = RedisActor::start("127.0.0.1:6379");

        eprintln!("starting");
        let _ = sys.run_until_complete(futures::future::lazy(|| {
            actor.as_redis_handle().scan("nodeinfo.*".into(), 100).fold((), |_, it| {
                eprintln!("start chunk");
                for key in it {
                    println!("\tk={}", key)
                }
                eprintln!("end chunk");
                Ok::<(), RespError>(())
            }).map_err(|_| ())
        }));
    }

}