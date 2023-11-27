use std::{convert::Into, mem};

/// A name associated with a table.
pub type TableName = String;

/// A type of a value in a column in a table.
pub enum Ty {
    String,
    Integer,
    Float,
}

/// A typed data item.
pub struct Item(Ty, Vec<u8>);

impl From<Item> for Vec<u8> {
    fn from(mut item: Item) -> Vec<u8> {
        let size = item.1.len();
        let mut buff = Vec::with_capacity(size + mem::size_of::<usize>());
        let mut size_bytes: Vec<u8> = size.to_le_bytes().to_vec();
        buff.append(&mut size_bytes);
        buff.append(&mut item.1);

        buff
    }
}

/// A row in a table.
pub struct Tuple(Vec<Item>);

impl From<Tuple> for Vec<u8> {
    fn from(tup: Tuple) -> Self {
        tup.0
            .into_iter()
            .map(Into::into)
            .collect::<Vec<Vec<u8>>>()
            .concat()
    }
}
