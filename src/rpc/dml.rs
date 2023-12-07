use super::super::{
    engine::{
        cmd::dml::{Cmp, Insert, Project, Select},
        Engine,
    },
    error::Error,
    types::table::LabeledTypedTuple,
};
use actix::Addr;
use jsonrpc_v2::{Data, Params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub struct InsertReq {
    db_name: String,
    table_name: String,
    values: Vec<Value>,
}

pub async fn insert(data: Data<Addr<Engine>>, params: Params<InsertReq>) -> Result<(), Error> {
    let InsertReq {
        db_name,
        table_name,
        values: json_values,
    } = params.0;

    // Convert each argument to bytes using little-endian encoding
    let values = json_values
        .into_iter()
        .map(|v| match v {
            Value::String(s) => Ok(s.into_bytes()),
            Value::Number(n) => {
                if let Some(n) = n.as_i64().map(|n| n.to_le_bytes()) {
                    Ok(n.to_vec())
                } else {
                    Err(Error::MiscDecodeError)
                }
            }
            _ => Err(Error::MiscDecodeError),
        })
        .collect::<Result<Vec<Vec<u8>>, Error>>()?;

    data.send(Insert {
        db_name,
        table_name,
        values,
    })
    .await
    .map_err(|e| Error::MailboxError(e))??;

    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct SelectReq {
    db_name: String,
    table_name: String,
    filter: Option<Cmp>,
}

pub async fn select(
    data: Data<Addr<Engine>>,
    params: Params<SelectReq>,
) -> Result<Vec<LabeledTypedTuple>, Error> {
    let SelectReq {
        db_name,
        table_name,
        filter,
    } = params.0;

    data.send(Select {
        db_name,
        table_name,
        filter,
    })
    .await
    .map_err(|e| Error::MailboxError(e))?
}

#[derive(Serialize, Deserialize)]
pub struct ProjectReq {
    input: Vec<LabeledTypedTuple>,
    columns: Vec<String>,
}

pub async fn project(
    data: Data<Addr<Engine>>,
    params: Params<ProjectReq>,
) -> Result<Vec<LabeledTypedTuple>, Error> {
    let ProjectReq { input, columns } = params.0;

    data.send(Project { input, columns })
        .await
        .map_err(|e| Error::MailboxError(e))?
}
