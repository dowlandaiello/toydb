pub mod ddl;
pub mod dml;

use super::{
    engine::{cmd::dml::ExecuteQuery, Engine},
    error::Error,
    types::table::LabeledTypedTuple,
};
use actix::Addr;
use jsonrpc_v2::{Data, Params};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ExecuteQueryReq {
    pub db_name: String,
    pub query: String,
}

/// Executes an arbitrary SQL query.
pub async fn execute_query(
    data: Data<Addr<Engine>>,
    params: Params<ExecuteQueryReq>,
) -> Result<Vec<Vec<LabeledTypedTuple>>, Error> {
    data.send(ExecuteQuery {
        db_name: params.0.db_name,
        query: params.0.query,
    })
    .await
    .map_err(|e| Error::MailboxError(e))?
}
