use std::mem;

/// A name associated with a database.
pub type DbName = String;

/// An index in a heap file identifying a page.
pub type PageId = usize;

/// A unique identifier in a heap file identifying a record.
#[derive(Clone)]
pub struct RecordId {
    pub(crate) page: PageId,
    pub(crate) page_idx: usize,
}

/// The size of a record ID in memory
pub const RECORD_ID_SIZE: usize = mem::size_of::<PageId>() + mem::size_of::<usize>();

impl From<RecordId> for [u8; RECORD_ID_SIZE] {
    fn from(rid: RecordId) -> Self {
        let mut buff: Self = [0; RECORD_ID_SIZE];
        let page_bytes = rid.page.to_le_bytes();
        let page_idx_bytes = rid.page_idx.to_le_bytes();

        for i in 0..page_bytes.len() {
            buff[i] = page_bytes[i];
        }

        for i in 0..page_idx_bytes.len() {
            buff[i + page_bytes.len()] = page_idx_bytes[i];
        }

        buff
    }
}

impl From<[u8; RECORD_ID_SIZE]> for RecordId {
    fn from(bytes: [u8; RECORD_ID_SIZE]) -> Self {
        let mut page_bytes: [u8; mem::size_of::<PageId>()] = [0; mem::size_of::<PageId>()];
        let mut page_idx_bytes: [u8; mem::size_of::<usize>()] = [0; mem::size_of::<usize>()];

        for i in 0..page_bytes.len() {
            page_bytes[i] = bytes[i];
        }

        for i in 0..page_idx_bytes.len() {
            page_idx_bytes[i] = bytes[i + page_bytes.len()];
        }

        RecordId {
            page: PageId::from_le_bytes(page_bytes),
            page_idx: usize::from_le_bytes(page_idx_bytes),
        }
    }
}
