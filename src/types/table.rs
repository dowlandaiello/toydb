use super::super::{error::Error, items::Tuple};

/// A name associated with a table.
pub type TableName = String;

/// A type of a value in a column in a table.
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

/// A logical representation of a tuple as a catalogue.
pub struct CatalogueEntry {
    table_name: String,
    file_name: String,
    attr_name: String,
    ty: Ty,
}

impl TryFrom<Tuple> for CatalogueEntry {
    type Error = Error;

    fn try_from(tup: Tuple) -> Result<Self, Error> {
        if tup.elements.len() < 4 {
            return Err(Error::MiscDecodeError);
        }

        let table_name =
            String::from_utf8(tup.elements[0].data).map_err(|_| Error::MiscDecodeError)?;
        let file_name =
            String::from_utf8(tup.elements[1].data).map_err(|_| Error::MiscDecodeError)?;
        let attr_name =
            String::from_utf8(tup.elements[2].data).map_err(|_| Error::MiscDecodeError)?;
        let ty = tup.elements[3].data.try_into()?;

        Ok(Self {
            table_name,
            file_name,
            attr_name,
            ty,
        })
    }
}
