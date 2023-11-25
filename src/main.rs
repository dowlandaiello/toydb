use actix_web::{guard, web, App, HttpServer};
use jsonrpc_v2::{Data, Error, Params, Server};
use std::io::Result as IoResult;

#[actix_rt::main]
async fn main() -> IoResult<()> {
    let rpc = Server::new().finish();

    HttpServer::new(move || {
        let rpc = rpc.clone();
        App::new().service(
            web::service("/api")
                .guard(guard::Post())
                .finish(rpc.into_web_service()),
        )
    })
    .bind("0.0.0.0:3000")?
    .run()
    .await
}
