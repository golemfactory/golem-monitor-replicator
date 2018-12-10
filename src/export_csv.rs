use actix::prelude::*;
use actix_redis::{Command, RedisActor, RespValue};
use actix_web::dev::Handler;
use actix_web::{self, AsyncResponder, HttpMessage, HttpRequest, HttpResponse, Responder};
use futures::future;
use futures::prelude::*;

pub struct ExportCSVHandler {
    exporter: Addr<Unsync, RedisActor>,
}

impl ExportCSVHandler {
    pub fn new(redis_actor: Addr<Unsync, RedisActor>) -> ExportCSVHandler {
        ExportCSVHandler {
            exporter: redis_actor,
        }
    }
}

impl Handler<()> for ExportCSVHandler {
    type Result = Result<&'static str, actix_web::Error>;

    fn handle(&mut self, req: HttpRequest<()>) -> <Self as Handler<()>>::Result {
        Ok("OK")
    }
}

pub fn export_csv(r: HttpRequest) -> impl Responder /*Future<Item = HttpResponse, Error = actix_web::Error>*/
{
    //future::ok(HttpResponse::Ok().finish()) //.responder()
    "OK"
}
