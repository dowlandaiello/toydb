use super::super::{
    engine::{
        cmd::dml::{Aggregate, Cmp, GroupBy, Insert, Join, Project, Rename, Select},
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

#[derive(Serialize, Deserialize)]
pub struct JoinReq {
    pub(crate) input_1: Vec<LabeledTypedTuple>,
    pub(crate) input_2: Vec<LabeledTypedTuple>,
    pub(crate) cond: Cmp,
}

pub async fn join(
    data: Data<Addr<Engine>>,
    params: Params<JoinReq>,
) -> Result<Vec<LabeledTypedTuple>, Error> {
    let JoinReq {
        input_1,
        input_2,
        cond,
    } = params.0;

    data.send(Join {
        input_1,
        input_2,
        cond,
    })
    .await
    .map_err(|e| Error::MailboxError(e))?
}

#[derive(Serialize, Deserialize)]
pub struct RenameReq {
    pub(crate) input: Vec<LabeledTypedTuple>,
    pub(crate) target: String,
    pub(crate) new_name: String,
}

pub async fn rename(
    data: Data<Addr<Engine>>,
    params: Params<RenameReq>,
) -> Result<Vec<LabeledTypedTuple>, Error> {
    let RenameReq {
        input,
        target,
        new_name,
    } = params.0;

    data.send(Rename {
        input,
        target,
        new_name,
    })
    .await
    .map_err(|e| Error::MailboxError(e))?
}

#[derive(Serialize, Deserialize)]
pub struct GroupByReq {
    pub(crate) input: Vec<LabeledTypedTuple>,
    pub(crate) group_by: Vec<String>,
    pub(crate) aggregate: Vec<Aggregate>,
}

pub async fn group_by(
    data: Data<Addr<Engine>>,
    params: Params<GroupByReq>,
) -> Result<Vec<LabeledTypedTuple>, Error> {
    let GroupByReq {
        input,
        group_by,
        aggregate,
    } = params.0;

    data.send(GroupBy {
        input,
        group_by,
        aggregate,
    })
    .await
    .map_err(|e| Error::MailboxError(e))?
}
