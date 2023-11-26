/// A name associated with a database.
pub type DbName = String;

/// An index in a heap file identifying a page.
pub type PageId = usize;

/// A unique identifier in a heap file identifying a record.
pub struct RecordId {
    pub(crate) page: PageId,
    pub(crate) page_idx: usize,
}
