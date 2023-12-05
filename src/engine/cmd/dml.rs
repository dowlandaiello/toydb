use super::super::super::{
    error::Error,
    types::{db::DbName, table::TableName},
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
