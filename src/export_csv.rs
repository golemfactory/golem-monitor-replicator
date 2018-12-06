use actix_web::{self, AsyncResponder, HttpMessage, HttpRequest, HttpResponse, Responder};
use futures::future;
use futures::prelude::*;

pub fn export_csv(r: HttpRequest) -> impl Responder /*Future<Item = HttpResponse, Error = actix_web::Error>*/ {
    //future::ok(HttpResponse::Ok().finish()) //.responder()
    "OK"
}
