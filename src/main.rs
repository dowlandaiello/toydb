use actix_cors::Cors;
use actix_web::{guard, http, web, App, HttpServer};
use jsonrpc_v2::{Data, Server};
use std::io::Result as IoResult;
use toydb::{
    engine::Engine,
    rpc::{
        ddl::{create_database, create_table},
        dml::{group_by, insert, join, project, rename, select},
        execute_query,
    },
};

#[actix_rt::main]
async fn main() -> IoResult<()> {
    tracing_subscriber::fmt::init();

    let engine = Engine::start().await.expect("Engine to start");
    let rpc = Server::new()
        .with_data(Data::new(engine))
        .with_method("create_database", create_database)
        .with_method("create_table", create_table)
        .with_method("insert", insert)
        .with_method("select", select)
        .with_method("project", project)
        .with_method("join", join)
        .with_method("rename", rename)
        .with_method("group_by", group_by)
        .with_method("execute_query", execute_query)
        .finish();

    HttpServer::new(move || {
        let rpc = rpc.clone();
        let cors = Cors::default()
            .allowed_origin("http://localhost:8000")
            .allowed_methods(vec!["GET", "POST"])
            .allowed_header(http::header::CONTENT_TYPE);

        App::new().wrap(cors).service(
            web::service("/api")
                .guard(guard::Post())
                .finish(rpc.into_web_service()),
        )
    })
    .bind("0.0.0.0:3000")?
    .run()
    .await
}
