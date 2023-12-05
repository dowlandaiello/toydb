use super::super::{
    super::{
        error::Error,
        types::{
            db::DbName,
            table::{Constraint, TableName, Ty},
        },
    },
    DbHandle,
};
use actix::{Addr, Message};

/// A message issued to the engine requesting that a new database be created.
/// Returns the handle to the database if the database already exists.
#[derive(Message, Debug)]
#[rtype(result = "Result<Addr<DbHandle>, Error>")]
pub struct CreateDatabase(pub(crate) DbName);

/// A message issued to the engine requesting that a new table be created in
/// a database. Returns an error if the operation failed.
#[derive(Message, Debug)]
#[rtype(result = "Result<(), Error>")]
pub struct CreateTable {
    pub(crate) db_name: DbName,
    pub(crate) table_name: TableName,
    pub(crate) columns: Vec<(String, Ty)>,
    pub(crate) constraints: Vec<Constraint>,
}
