use super::super::{
    error::Error,
    items::{Element, Tuple},
};
use serde::{Deserialize, Serialize};

/// A name associated with a table.
pub type TableName = String;

/// A type of a value in a column in a table.
#[derive(Serialize, Deserialize)]
pub enum Ty {
    String,
    Integer,
    Float,
}

impl TryFrom<Vec<u8>> for Ty {
    type Error = Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Error> {
        let v = bytes[0];

        match v {
            0 => Ok(Self::String),
            1 => Ok(Self::Integer),
            2 => Ok(Self::Float),
            _ => Err(Error::MiscDecodeError),
        }
    }
}

impl From<Ty> for u8 {
    fn from(t: Ty) -> Self {
        match t {
            Ty::String => 0,
            Ty::Integer => 1,
            Ty::Float => 2,
        }
    }
}

impl From<Ty> for Vec<u8> {
    fn from(t: Ty) -> Self {
        vec![t.into()]
    }
}

/// A logical representation of a tuple as a catalogue.
pub struct CatalogueEntry {
    pub table_name: String,
    pub file_name: String,
    pub attr_name: String,
    pub ty: Ty,
}

impl TryFrom<Tuple> for CatalogueEntry {
    type Error = Error;

    fn try_from(mut tup: Tuple) -> Result<Self, Error> {
        if tup.elements.len() < 4 {
            return Err(Error::MiscDecodeError);
        }

        let table_name =
            String::from_utf8(tup.elements.remove(0).data).map_err(|_| Error::MiscDecodeError)?;
        let file_name =
            String::from_utf8(tup.elements.remove(0).data).map_err(|_| Error::MiscDecodeError)?;
        let attr_name =
            String::from_utf8(tup.elements.remove(0).data).map_err(|_| Error::MiscDecodeError)?;
        let ty = tup.elements.remove(0).data.try_into()?;

        Ok(Self {
            table_name,
            file_name,
            attr_name,
            ty,
        })
    }
}

impl From<CatalogueEntry> for Tuple {
    fn from(cat: CatalogueEntry) -> Self {
        let table_name = cat.table_name.into_bytes();
        let file_name = cat.file_name.into_bytes();
        let attr_name = cat.attr_name.into_bytes();
        let ty = cat.ty.into();

        Tuple {
            elements: vec![
                Element { data: table_name },
                Element { data: file_name },
                Element { data: attr_name },
                Element { data: ty },
            ],
        }
    }
}
