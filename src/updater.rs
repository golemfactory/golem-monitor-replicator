use actix::prelude::*;
use actix_redis::{Command, RedisActor, RespValue};
use futures::prelude::*;
use std::collections::HashMap;

pub struct Updater {
    redis_actor: Addr<Unsync, RedisActor>,
}

impl Updater {
    pub fn start(redis_actor: Addr<Unsync, RedisActor>) -> Addr<Unsync, Updater> {
        Supervisor::start(|_| Updater { redis_actor })
    }
}

#[derive(Debug)]
pub struct UpdateKey {
    pub key: String,
    pub value: HashMap<String, String>,
}

impl Message for UpdateKey {
    type Result = Result<(), Error>;
}

impl Actor for Updater {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut <Self as Actor>::Context) {
        self.redis_actor
            .send(Command(resp_array![
                "CLIENT",
                "SETNAME",
                format!("monitor-thread-{:?}", ::std::thread::current().id())
            ]))
            .map(|_r| ())
            .map_err(|e| warn!("CLIENT SETNAME error {:?}", e))
            .into_actor(self)
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
    #[fail(display = "MailboxError {}", _0)]
    MailboxError(actix::MailboxError),
}

impl From<MailboxError> for Error {
    fn from(err: MailboxError) -> Self {
        Error::MailboxError(err)
    }
}

fn to_hmset_command(msg: UpdateKey) -> Command {
    debug!("preparing command for {:?}", msg);

    let mut msg_vec: Vec<RespValue> = Vec::with_capacity(2 + msg.value.len() * 2);
    msg_vec.push("HMSET".into());
    msg_vec.push(format!("nodeinfo.{}", msg.key).into());

    for (key, value) in msg.value {
        msg_vec.push(key.into());
        msg_vec.push(value.into());
    }

    Command(RespValue::Array(msg_vec))
}

impl Handler<UpdateKey> for Updater {
    type Result = ActorResponse<Updater, (), Error>;

    fn handle(
        &mut self,
        msg: UpdateKey,
        _: &mut Self::Context,
    ) -> <Self as Handler<UpdateKey>>::Result {
        let redis_actor = &self.redis_actor;

        redis_actor.do_send(Command(resp_array!["SADD", "active_nodes", &msg.key]));

        let f = redis_actor
            .send(to_hmset_command(msg))
            .into_actor(self)
            .map_err(|e, _, _| {
                error!("update key error {:?}", &e);
                e.into()
            })
            .map(|r, _, _| debug!("resp={:?}", r));

        ActorResponse::async(f)
    }
}
