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
use sqlparser::ast::{ColumnDef, ObjectName, TableConstraint};

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

impl CreateTable {
    pub fn from_sql(
        db_name: DbName,
        mut name: ObjectName,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    ) -> Self {
        let typed_cols = columns
            .into_iter()
            .map(|def| (def.name.value, def.data_type.into()))
            .collect::<Vec<(String, Ty)>>();
        let allowed_constraints = constraints
            .into_iter()
            .filter_map(|constr| constr.try_into().ok())
            .collect::<Vec<Constraint>>();

        Self {
            db_name,
            table_name: name.0.remove(0).value,
            columns: typed_cols,
            constraints: allowed_constraints,
        }
    }
}
