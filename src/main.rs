use actix_web::{guard, web, App, HttpServer};
use jsonrpc_v2::{Data, Server};
use std::io::Result as IoResult;
use toydb::{
    engine::Engine,
    rpc::ddl::{create_database, create_table},
};

#[actix_rt::main]
async fn main() -> IoResult<()> {
    let engine = Engine::start().await;
    let rpc = Server::new()
        .with_data(Data::new(engine))
        .with_method("create_database", create_database)
        .with_method("create_table", create_table)
        .finish();

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
