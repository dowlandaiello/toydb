use super::super::{
    error::Error,
    items::{Element, Tuple},
    util::fs,
};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{DataType, Expr, Ident, TableConstraint, Value as AstValue};
use std::{fmt::Display, mem};

/// A name associated with a table.
pub type TableName = String;

/// Just primary key for now
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Constraint {
    PrimaryKey(Vec<String>),
}

impl TryFrom<TableConstraint> for Constraint {
    type Error = Error;

    fn try_from(constr: TableConstraint) -> Result<Self, Self::Error> {
        match constr {
            TableConstraint::Unique {
                columns,
                is_primary: true,
                ..
            } => Ok(Self::PrimaryKey(
                columns
                    .into_iter()
                    .map(|col| col.value)
                    .collect::<Vec<String>>(),
            )),
            o => Err(Error::Unimplemented(Some(format!("{:?}", o)))),
        }
    }
}

/// Gets the primary key attributes from a list of constraints.
pub fn get_primary_key(
    constraints: impl AsRef<[Constraint]>,
) -> Result<Option<Vec<String>>, Error> {
    let mut pks = constraints
        .as_ref()
        .iter()
        .filter_map(|x| match x {
            Constraint::PrimaryKey(pk) => Some(pk.clone()),
        })
        .collect::<Vec<Vec<String>>>();

    if pks.len() > 1 {
        return Err(Error::MultiplePrimaryKeyClauses);
    }

    if pks.len() < 1 {
        return Ok(None);
    }

    Ok(Some(pks.remove(0)))
}

/// Gets the primary key attributes from a list of constraints.
pub fn into_primary_key(
    constraints: impl IntoIterator<Item = Constraint>,
) -> Result<Option<Vec<String>>, Error> {
    let mut pks = constraints
        .into_iter()
        .filter_map(|x| match x {
            Constraint::PrimaryKey(pk) => Some(pk),
        })
        .collect::<Vec<Vec<String>>>();

    if pks.len() > 1 {
        return Err(Error::MultiplePrimaryKeyClauses);
    }

    if pks.len() < 1 {
        return Ok(None);
    }

    Ok(Some(pks.remove(0)))
}

/// A typed tuple.
#[derive(Serialize, Deserialize, Debug)]
pub struct TypedTuple(pub Vec<Value>);

/// A tuple with column names.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LabeledTypedTuple(pub Vec<(String, Value)>);

impl LabeledTypedTuple {
    pub fn flatten_display(&self) -> Vec<&dyn Display> {
        self.0
            .iter()
            .map(|v| -> &dyn Display {
                match v {
                    (_, Value::String(s)) => s,
                    (_, Value::Integer(i)) => i,
                }
            })
            .collect::<Vec<&dyn Display>>()
    }
}

/// A typed value.
#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Clone, Hash, Eq)]
pub enum Value {
    String(String),
    Integer(i64),
}

impl From<Value> for Vec<u8> {
    fn from(v: Value) -> Self {
        match v {
            Value::String(s) => s.into_bytes(),
            Value::Integer(i) => i.to_le_bytes().to_vec(),
        }
    }
}

impl TryFrom<Expr> for Value {
    type Error = Error;

    fn try_from(e: Expr) -> Result<Self, Self::Error> {
        match e {
            Expr::Value(AstValue::SingleQuotedString(s))
            | Expr::Value(AstValue::DoubleQuotedString(s)) => Ok(Self::String(s)),
            Expr::Value(AstValue::Number(n_s, _)) => Ok(Self::Integer(
                n_s.parse().map_err(|_| Error::MiscDecodeError)?,
            )),
            Expr::Identifier(Ident { value, .. }) => Ok(Self::String(value)),
            o => Err(Error::Unimplemented(Some(format!("{:?}", o)))),
        }
    }
}

/// A type of a value in a column in a table.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Ty {
    String,
    Integer,
}

impl From<DataType> for Ty {
    fn from(d: DataType) -> Self {
        match d {
            DataType::Integer(_) | DataType::Int(_) => Self::Integer,
            _ => Self::String,
        }
    }
}

impl TryFrom<Vec<u8>> for Ty {
    type Error = Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Error> {
        let v = bytes[0];

        match v {
            0 => Ok(Self::String),
            1 => Ok(Self::Integer),
            _ => Err(Error::MiscDecodeError),
        }
    }
}

impl From<Ty> for u8 {
    fn from(t: Ty) -> Self {
        match t {
            Ty::String => 0,
            Ty::Integer => 1,
        }
    }
}

impl From<Ty> for Vec<u8> {
    fn from(t: Ty) -> Self {
        vec![t.into()]
    }
}

/// A logical representation of a tuple as a catalogue.
#[derive(Debug, Clone)]
pub struct CatalogueEntry {
    pub table_name: String,
    pub file_name: String,
    pub index_name: Option<String>,
    pub attr_name: String,
    pub attr_index: usize,
    pub ty: Ty,
    pub primary_key: bool,
}

impl TryFrom<Tuple> for CatalogueEntry {
    type Error = Error;

    #[tracing::instrument]
    fn try_from(mut tup: Tuple) -> Result<Self, Error> {
        if tup.elements.len() < 7 {
            return Err(Error::MiscDecodeError);
        }

        let table_name =
            String::from_utf8(tup.elements.remove(0).data).map_err(|_| Error::MiscDecodeError)?;
        let file_name =
            String::from_utf8(tup.elements.remove(0).data).map_err(|_| Error::MiscDecodeError)?;
        let index_name = {
            let data = tup.elements.remove(0).data;
            if data.len() == 0 {
                None
            } else {
                Some(String::from_utf8(data).map_err(|_| Error::MiscDecodeError)?)
            }
        };
        let attr_name =
            String::from_utf8(tup.elements.remove(0).data).map_err(|_| Error::MiscDecodeError)?;

        let attr_index_bytes: [u8; mem::size_of::<usize>()] = tup
            .elements
            .remove(0)
            .data
            .try_into()
            .map_err(|_| Error::MiscDecodeError)?;
        let attr_index: usize = usize::from_le_bytes(attr_index_bytes);

        let ty = tup.elements.remove(0).data.try_into()?;
        let primary_key: bool = tup.elements.remove(0).data[0] != 0;

        Ok(Self {
            table_name,
            file_name,
            index_name,
            attr_name,
            attr_index,
            ty,
            primary_key,
        })
    }
}

impl From<CatalogueEntry> for Tuple {
    fn from(cat: CatalogueEntry) -> Self {
        let table_name = cat.table_name.into_bytes();
        let file_name = cat.file_name.into_bytes();
        let index_name = cat
            .index_name
            .map(|name| name.into_bytes())
            .unwrap_or_default();
        let attr_name = cat.attr_name.into_bytes();
        let attr_index = cat.attr_index.to_le_bytes().to_vec();
        let ty = cat.ty.into();
        let primary_key: u8 = cat.primary_key.into();

        Tuple {
            rel_name: fs::CATALOGUE_TABLE_NAME.to_owned(),
            elements: vec![
                Element { data: table_name },
                Element { data: file_name },
                Element { data: index_name },
                Element { data: attr_name },
                Element { data: attr_index },
                Element { data: ty },
                Element {
                    data: vec![primary_key],
                },
            ],
        }
    }
}
