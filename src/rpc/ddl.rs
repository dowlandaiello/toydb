use super::super::{
    engine::{
        cmd::ddl::{CreateDatabase, CreateTable},
        Engine,
    },
    error::Error,
    types::table::Ty,
};
use actix::Addr;
use jsonrpc_v2::{Data, Params};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct CreateTableReq {
    db_name: String,
    table_name: String,
    columns: Vec<(String, Ty)>,
}

pub async fn create_table(
    data: Data<Addr<Engine>>,
    params: Params<CreateTableReq>,
) -> Result<(), Error> {
    data.send(CreateTable(
        params.0.db_name,
        params.0.table_name,
        params.0.columns,
    ))
    .await
    .map_err(|e| Error::MailboxError(e))??;

    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct CreateDatabaseReq {
    db_name: String,
}

pub async fn create_database(
    data: Data<Addr<Engine>>,
    params: Params<CreateDatabaseReq>,
) -> Result<(), Error> {
    data.send(CreateDatabase(params.0.db_name))
        .await
        .map_err(|e| Error::MailboxError(e))??;

    Ok(())
}
