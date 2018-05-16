use futures::prelude::*;
use futures::future;
use actix::prelude::*;
use actix::fut;
use actix_redis::{RedisActor, RespValue, Command, self};

const UPDATE_SCRIPT: &str = r#"
local t1 = cjson.decode(redis.call('get', KEYS[1]) or '{}');
local t2 = cjson.decode(ARGV[1]); for k,v in pairs(t2) do t1[k] = v end;
return redis.call('set', KEYS[1], cjson.encode(t1))
"#;


pub struct Updater {
    redis_actor: Addr<Unsync, RedisActor>,
    script_hash: Option<RespValue>,
}

impl Updater {
    pub fn start(redis_actor: Addr<Unsync, RedisActor>) -> Addr<Unsync, Updater> {
        Supervisor::start(|_| Updater {
            redis_actor,
            script_hash: None
        })
    }
}


pub struct UpdateKey {
    pub key: String,
    pub value: String,
}

impl Message for UpdateKey {
    type Result = Result<(), Error>;
}

impl Actor for Updater {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut <Self as Actor>::Context) {
        self.redis_actor
            .send(Command(resp_array!["SCRIPT","LOAD", UPDATE_SCRIPT]))
            .into_actor(self)
            .and_then(|res: Result<RespValue, actix_redis::Error>, act: &mut Updater, ctx: &mut <Updater as Actor>::Context| {
                debug!("script load result {:?}", &res);
                match res {
                    Ok(h) => { act.script_hash = Some(h); }
                    Err(ref err) => error!("script load error: {:?}", err)
                };
                ctx.spawn(
                    act.redis_actor.send(Command(resp_array!["CLIENT","SETNAME", format!("monitor-thread-{:?}", ::std::thread::current().id())]))
                        .map(|_r| ())
                        .map_err(|e| warn!("CLIENT SETNAME error {:?}", e))
                        .into_actor(act)
                );
                fut::ok(())
            })
            .map_err(|_e, _act, _ctx| ())
            .wait(ctx);

        //ctx.spawn(r);
    }
}

impl Supervised for Updater {
    fn restarting(&mut self, _: &mut Self::Context) {
        warn!("restarting!");
    }
}

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "Redis error {}", _0)]
    Redis(actix_redis::Error),

    #[fail(display = "MailboxError {}", _0)]
    MailboxError(actix::MailboxError),

    #[fail(display = "ScriptError {}", _0)]
    ScriptError(String),

    #[fail(display = "NoScript")]
    NoScript
}


impl From<MailboxError> for Error {
    fn from(err: MailboxError) -> Self {
        Error::MailboxError(err)
    }
}

fn eval_if_hash(redis : &Addr<Unsync, RedisActor>, hash : &Option<RespValue>, k : String, v : String)
    -> Box<Future<Item=RespValue, Error=Error>> {

    match hash.as_ref() {
        Some(hash) => Box::new(
            redis.send(Command(resp_array!["EVALSHA", hash.clone(), "1", &k, &v]))
                .map_err(|e| Error::MailboxError(e))
                .and_then(|r| {
                    match r {
                        Ok(RespValue::Error(err)) =>
                            if err.starts_with("NOSCRIPT") {
                                future::err(Error::NoScript)
                            } else {
                                future::err(Error::ScriptError(err))
                            },
                        Err(err) => future::err(Error::Redis(err)),
                        Ok(other) => future::ok(other)
                    }
                })),
        None => Box::new(redis.send(Command(resp_array!["EVAL", UPDATE_SCRIPT, "1", &k, &v]))
            .map_err(|e| Error::MailboxError(e))
            .and_then(|r| {
                match r {
                    Ok(RespValue::Error(err)) => future::err(Error::ScriptError(err)),
                    Err(err) => future::err(Error::Redis(err)),
                    Ok(other) => future::ok(other)
                }
            }))
    }
}


impl Handler<UpdateKey> for Updater {
    type Result = ActorResponse<Updater, (), Error>;

    fn handle(&mut self, msg: UpdateKey, _: &mut Self::Context) -> <Self as Handler<UpdateKey>>::Result {
        let redis_actor = &self.redis_actor;

        let f =
            eval_if_hash(redis_actor, &self.script_hash, msg.key, msg.value)
                .into_actor(self)
                .map_err(|e, _act : &mut Updater, ctx : &mut <Updater as Actor>::Context|{
                    error!("update key error {:?}", &e);
                    match e {
                        Error::NoScript => {
                            ctx.stop();
                            e
                        }
                        _ => e
                    }
                })
                .map(|_,_,_| ());

        ActorResponse::async(f )
    }
}