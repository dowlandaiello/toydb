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
use sqlparser::ast::{ColumnDef, ColumnOption, ObjectName, TableConstraint};

/// A message issued to the engine requesting that a new database be created.
/// Returns the handle to the database if the database already exists.
#[derive(Message, Debug)]
#[rtype(result = "Result<Addr<DbHandle>, Error>")]
pub struct CreateDatabase(pub(crate) DbName);

impl CreateDatabase {
    pub fn from_sql(mut db_name: ObjectName) -> Self {
        Self(db_name.0.remove(0).value)
    }
}

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
    pub fn try_from_sql(
        db_name: DbName,
        mut name: ObjectName,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    ) -> Result<Self, Error> {
        let mut allowed_constraints = columns
            .iter()
            .map(|col| {
                col.options
                    .iter()
                    .map(|opt| match &opt.option {
                        ColumnOption::Unique { is_primary: true } => {
                            Ok(Constraint::PrimaryKey(vec![col.name.value.clone()]))
                        }
                        o => Err(Error::Unimplemented(Some(format!("{:?}", o.clone())))),
                    })
                    .collect::<Result<Vec<Constraint>, Error>>()
            })
            .collect::<Result<Vec<Vec<Constraint>>, Error>>()?
            .concat();

        let typed_cols = columns
            .into_iter()
            .map(|def| (def.name.value, def.data_type.into()))
            .collect::<Vec<(String, Ty)>>();

        let mut constraint_clause_constraints = constraints
            .into_iter()
            .filter_map(|constr| constr.try_into().ok())
            .collect::<Vec<Constraint>>();
        allowed_constraints.append(&mut constraint_clause_constraints);

        Ok(Self {
            db_name,
            table_name: name.0.remove(0).value,
            columns: typed_cols,
            constraints: allowed_constraints,
        })
    }
}
