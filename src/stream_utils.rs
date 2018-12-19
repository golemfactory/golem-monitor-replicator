use bytes::{Bytes};
use futures::prelude::*;
use serde::{Serialize};
use serde_json;
use std::io::{Write};
use std::mem;

#[derive(Clone, Copy)]
enum State {
    Start,
    InArray,
    AtEnd,
}

struct JsonStream<Upstream, MapErr> {
    state: State,
    buffer: Option<Vec<u8>>,
    upstream: Upstream,
    map_json_err: MapErr,
    min_chunk: usize,
    max_chunk: usize,
}

impl<Upstream: Stream, MapErr> JsonStream<Upstream, MapErr>
where
    Upstream::Item: Serialize,
    MapErr: Fn(serde_json::Error) -> Upstream::Error,
{
    #[inline]
    fn append(&mut self, item: &Upstream::Item) -> Result<(), Upstream::Error> {
        let w = self.buffer.as_mut().unwrap();

        serde_json::to_writer(w, item).map_err(&self.map_json_err)?;

        //w.extend(serde_json::to_vec(item).unwrap());
        Ok(())
    }

    fn consume_buf(&mut self) -> Bytes {
        let buffer = mem::replace(&mut self.buffer, Some(Vec::with_capacity(self.max_chunk)));

        match buffer {
            Some(v) => Bytes::from(v),
            None => Bytes::new(),
        }
    }

    fn sep(&mut self, next_state: State) {
        //
        let sep_arr: &[u8] = match (self.state, next_state) {
            (State::Start, _) => &[b'['],
            (State::InArray, State::InArray) => &[b',', b'\n'],
            _ => &[],
        };
        self.buffer.as_mut().unwrap().write(sep_arr).unwrap();
        self.state = next_state;
    }
}

impl<Upstream: Stream, MapErr> Stream for JsonStream<Upstream, MapErr>
where
    Upstream::Item: Serialize,
    MapErr: Fn(serde_json::Error) -> Upstream::Error,
{
    type Item = Bytes;
    type Error = Upstream::Error;

    fn poll(&mut self) -> Result<Async<Option<<Self as Stream>::Item>>, <Self as Stream>::Error> {
        match self.state {
            State::AtEnd => return Ok(Async::Ready(None)),
            _ => (),
        }

        loop {
            if self.buffer.as_ref().unwrap().len() > self.min_chunk {
                let buf = self.consume_buf();
                return Ok(Async::Ready(Some(buf)));
            }

            let item = match self.upstream.poll() {
                Err(e) => return Err(e),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(Some(v))) => v,
                Ok(Async::Ready(None)) => {
                    self.sep(State::AtEnd);
                    self.buffer.as_mut().unwrap().write(&[b']']).unwrap();

                    let buf = self.consume_buf();
                    self.state = State::AtEnd;
                    return Ok(Async::Ready(Some(buf)));
                }
            };
            self.sep(State::InArray);
            self.append(&item)?;
        }
    }
}

pub fn stream_json_array<Upstream: Stream, MapErr>(
    min_chunk: usize,
    max_chunk: usize,
    map_err: MapErr,
    stream: Upstream,
) -> impl Stream<Item = Bytes, Error = Upstream::Error>
where
    Upstream::Item: Serialize,
    MapErr: Fn(serde_json::Error) -> Upstream::Error,
{
    JsonStream {
        state: State::Start,
        buffer: Some(Vec::with_capacity(max_chunk)),
        upstream: stream,
        map_json_err: map_err,
        min_chunk,
        max_chunk,
    }
}
