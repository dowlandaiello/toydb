use super::super::super::types::{
    db::DbName,
    table::{TableName, Ty},
};

/// A message issued to the engine requesting that a tuple be inserted into the database.
pub struct Insert(
    pub(crate) DbName,
    pub(crate) TableName,
    pub(crate) Vec<(Vec<u8>, Ty)>,
);
