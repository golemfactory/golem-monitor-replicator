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
pub struct UpdateMap {
    pub collection: String,
    pub key: String,
    pub value: HashMap<String, String>,
}

#[derive(Debug)]
pub struct UpdateVal {
    pub collection: String,
    pub key: String,
    pub value: String,
}


#[derive(Debug)]
pub enum UpdateRedis {
    UpdateRedisMap(UpdateMap),
    UpdateRedisVal(UpdateVal)
}


impl Message for UpdateRedis {
    type Result = Result<(), Error>;
}

//impl Message for UpdateMap {
//    type Result = Result<(), Error>;
//}
//
//impl Message for UpdateVal {
//    type Result = Result<(), Error>;
//}


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


fn to_hmset_command(msg: UpdateMap) -> Command {
    debug!("preparing command for {:?}", msg);

    let mut msg_vec: Vec<RespValue> = Vec::with_capacity(2 + msg.value.len() * 2);
    msg_vec.push("HMSET".into());
    msg_vec.push(format!("{}.{}", msg.collection, msg.key).into());

    for (key, value) in msg.value {
        msg_vec.push(key.into());
        msg_vec.push(value.into());
    }

    Command(RespValue::Array(msg_vec))
}

fn to_set_command(msg: UpdateVal) -> Command {
    debug!("preparing command for {:?}", msg);
    let key = format!["{}.{}", msg.collection, msg.key];
    Command(resp_array!["SET", key, msg.value])
}

impl Handler<UpdateRedis> for Updater {
    type Result = ActorResponse<Updater, (), Error>;

    fn handle(
        &mut self,
        msg: UpdateRedis,
        _: &mut Self::Context,
    ) -> <Self as Handler<UpdateRedis>>::Result {

        let redis_actor = &self.redis_actor;

        if let UpdateRedis::UpdateRedisMap(ref msg) = msg {
            redis_actor.do_send(Command(resp_array!["SADD", "active_nodes", &msg.key]));
        }

        let f = redis_actor
            .send(match msg {
                UpdateRedis::UpdateRedisMap(u) => to_hmset_command(u),
                UpdateRedis::UpdateRedisVal(u) => to_set_command(u)
            }).into_actor(self)
            .map_err(|e, _, _| {
                error!("update key error {:?}", &e);
                e.into()
            })
            .map(|r, _, _| debug!("resp={:?}", r));




        ActorResponse::async(f)
    }
}
