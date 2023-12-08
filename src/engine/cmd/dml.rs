use super::super::super::{
    error::Error,
    types::{
        db::DbName,
        table::{LabeledTypedTuple, TableName, Ty, Value},
    },
};
use actix::Message;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, ObjectName, Query, SetExpr, Value as SqlValue, Values,
};

/// A message issued to the engine requesting that a tuple be inserted into the table.
#[derive(Message, Debug)]
#[rtype(result = "Result<(), Error>")]
pub struct Insert {
    pub(crate) db_name: DbName,
    pub(crate) table_name: TableName,
    pub(crate) values: Vec<Vec<u8>>,
}

impl Insert {
    pub fn try_from_sql(
        db_name: DbName,
        mut table_name: ObjectName,
        all_columns: Vec<(String, Ty)>,
        columns: Vec<Ident>,
        source: Option<Box<Query>>,
    ) -> Result<Self, Error> {
        let mut values = all_columns
            .into_iter()
            .map(|(name, col)| match col {
                Ty::String => (name, Value::String(String::default())),
                Ty::Integer => (name, Value::Integer(i64::default())),
            })
            .collect::<Vec<(String, Value)>>();

        match source.map(|b| *b) {
            Some(Query { body, .. }) => match *body {
                SetExpr::Values(Values { mut rows, .. }) => {
                    for (v, c) in rows.remove(0).into_iter().zip(columns) {
                        let col_name = c.value;
                        let safe_v = v.try_into()?;

                        values
                            .iter_mut()
                            .find(|(col, _)| col.as_str() == col_name.as_str())
                            .map(|tup| {
                                tup.1 = safe_v;
                            });
                    }
                }
                o => {
                    return Err(Error::Unimplemented(Some(format!("{:?}", o))));
                }
            },
            o => {
                return Err(Error::Unimplemented(Some(format!("{:?}", o))));
            }
        };

        Ok(Self {
            db_name,
            table_name: table_name.0.remove(0).value,
            values: values
                .into_iter()
                .map(|(_, val)| <Value as Into<Vec<u8>>>::into(val))
                .collect::<Vec<Vec<u8>>>(),
        })
    }
}

/// A comparison between two comparators
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Cmp {
    Eq(Comparator, Comparator),
    Lt(Comparator, Comparator),
    Gt(Comparator, Comparator),
    Ne(Comparator, Comparator),
}

impl Cmp {
    pub fn try_from_sql(e: Expr) -> Result<Self, Error> {
        match e {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::Eq => Ok(Self::Eq(
                    Comparator::try_from_sql(*left)?,
                    Comparator::try_from_sql(*right)?,
                )),
                BinaryOperator::Lt => Ok(Self::Lt(
                    Comparator::try_from_sql(*left)?,
                    Comparator::try_from_sql(*right)?,
                )),
                BinaryOperator::Gt => Ok(Self::Gt(
                    Comparator::try_from_sql(*left)?,
                    Comparator::try_from_sql(*right)?,
                )),
                BinaryOperator::NotEq => Ok(Self::Ne(
                    Comparator::try_from_sql(*left)?,
                    Comparator::try_from_sql(*right)?,
                )),
                o => Err(Error::Unimplemented(Some(format!("{:?}", o)))),
            },
            o => Err(Error::Unimplemented(Some(format!("{:?}", o)))),
        }
    }

    /// Returns whether or not the value falls under the comparison clause.
    pub fn has_value(&self, tup: &LabeledTypedTuple, attr_names: impl AsRef<[String]>) -> bool {
        self.helper(tup, attr_names).unwrap_or_default()
    }

    fn helper(&self, tup: &LabeledTypedTuple, attr_names: impl AsRef<[String]>) -> Option<bool> {
        match self {
            Self::Eq(a, b) => {
                let a_val = a.to_value_for(tup, attr_names.as_ref())?;
                let b_val = b.to_value_for(tup, attr_names.as_ref())?;

                Some(a_val == b_val)
            }
            Self::Lt(a, b) => {
                let a_val = a.to_value_for(tup, attr_names.as_ref())?;
                let b_val = b.to_value_for(tup, attr_names.as_ref())?;

                Some(a_val < b_val)
            }
            Self::Gt(a, b) => {
                let a_val = a.to_value_for(tup, attr_names.as_ref())?;
                let b_val = b.to_value_for(tup, attr_names.as_ref())?;

                Some(a_val > b_val)
            }
            Self::Ne(a, b) => {
                let a_val = a.to_value_for(tup, attr_names.as_ref())?;
                let b_val = b.to_value_for(tup, attr_names.as_ref())?;

                Some(a_val != b_val)
            }
        }
    }
}

/// Items that can be compared.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Comparator {
    Col(String),
    Val(Value),
}

impl Comparator {
    fn try_from_sql(e: Expr) -> Result<Self, Error> {
        match e {
            Expr::Identifier(ident) => Ok(Self::Col(ident.value)),
            Expr::Value(v) => Ok(Self::Val(v.try_into()?)),
            o => Err(Error::Unimplemented(Some(format!("{:?}", o)))),
        }
    }

    fn to_value_for(
        &self,
        tup: &LabeledTypedTuple,
        attr_names: impl AsRef<[String]>,
    ) -> Option<Value> {
        match self {
            Self::Col(c) => tup
                .0
                .clone()
                .into_iter()
                .zip(attr_names.as_ref().iter())
                .find(|(_, name)| name.as_str() == c.as_str())
                .map(|(val, _)| val.1),
            Self::Val(v) => Some(v.clone()),
        }
    }
}

/// A message issued to the engine requesting that a table be retrieved.
#[derive(Message, Debug)]
#[rtype(result = "Result<Vec<LabeledTypedTuple>, Error>")]
pub struct Select {
    pub(crate) db_name: DbName,
    pub(crate) table_name: TableName,
    pub(crate) filter: Option<Cmp>,
}

/// A message issued to the engine requesting that specific columns in a table be retrieved.
#[derive(Message, Debug)]
#[rtype(result = "Result<Vec<LabeledTypedTuple>, Error>")]
pub struct Project {
    pub(crate) input: Vec<LabeledTypedTuple>,
    pub(crate) columns: Vec<String>,
}

/// A message issued to the engine requesting that combines two tables on their matching column names.
#[derive(Message, Debug)]
#[rtype(result = "Result<Vec<LabeledTypedTuple>, Error>")]
pub struct Join {
    pub(crate) input_1: Vec<LabeledTypedTuple>,
    pub(crate) input_2: Vec<LabeledTypedTuple>,
    pub(crate) cond: Cmp,
}

/// A message issued to the engine requesting that a column be renamed in a relation.
#[derive(Message, Debug)]
#[rtype(result = "Result<Vec<LabeledTypedTuple>, Error>")]
pub struct Rename {
    pub(crate) input: Vec<LabeledTypedTuple>,
    pub(crate) target: String,
    pub(crate) new_name: String,
}

/// A message issued to the engine requesting that a column be grouped and aggregated.
#[derive(Message, Debug)]
#[rtype(result = "Result<Vec<LabeledTypedTuple>, Error>")]
pub struct GroupBy {
    pub(crate) input: Vec<LabeledTypedTuple>,

    // The name of the column to group by
    pub(crate) group_by: Vec<String>,

    // The type of aggregate to use and the name of the column to apply it to
    pub(crate) aggregate: Vec<Aggregate>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Aggregate {
    Count(Option<String>),
    Sum(String),
    Min(String),
    Max(String),
    Avg(String),
}

/// A message issued to the engine requesting that a query be executed.
#[derive(Message, Debug)]
#[rtype(result = "Result<Vec<Vec<LabeledTypedTuple>>, Error>")]
pub struct ExecuteQuery {
    pub(crate) query: String,
    pub(crate) db_name: String,
}
