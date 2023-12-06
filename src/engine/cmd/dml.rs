use super::super::super::{
    error::Error,
    types::{
        db::DbName,
        table::{TableName, TypedTuple},
    },
};
use actix::Message;

/// A message issued to the engine requesting that a tuple be inserted into the table.
#[derive(Message, Debug)]
#[rtype(result = "Result<(), Error>")]
pub struct Insert {
    pub(crate) db_name: DbName,
    pub(crate) table_name: TableName,
    pub(crate) values: Vec<Vec<u8>>,
}

/// A message issued to the engine requesting that a table be retrieved.
#[derive(Message, Debug)]
#[rtype(result = "Result<Vec<TypedTuple>, Error>")]
pub struct Select {
    pub(crate) db_name: DbName,
    pub(crate) table_name: TableName,
}

/// A message issued to the engine requesting that specific columns in a table be retrieved.
#[derive(Message, Debug)]
#[rtype(result = "Result<Vec<TypedTuple>, Error>")]
pub struct Project {
    pub(crate) db_name: DbName,
    pub(crate) table_name: TableName,
    pub(crate) input: Vec<TypedTuple>,
    pub(crate) columns: Vec<String>,
}
