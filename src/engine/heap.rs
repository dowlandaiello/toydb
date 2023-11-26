use super::{
    super::error::Error,
    buffer_pool::{DbHandle, PAGE_SIZE},
};
use actix::{Actor, Addr, Context, Handler, Message, ResponseActFuture};

/// Inserts a record at the end of the heap file.
#[derive(Message)]
#[rtype(result = "Result<(), Error>")]
pub struct InsertRecord(Vec<u8>);

/// An open heap abstraction.
/// Allows:
/// - Insertion of records by RID
/// - Deletion of records by RID
/// - Retrieval of records by RID
pub struct HeapHandle {
    buffer: Addr<DbHandle>,
}

impl Actor for HeapHandle {
    type Context = Context<Self>;
}

impl Handler<InsertRecord> for HeapHandle {
    type Result = ResponseActFuture<Self, Result<(), Error>>;

    fn handle(&mut self, msg: InsertRecord, _ctx: &mut Context<Self>) -> Self::Result {
        // The record must not be larger than the size of a page
        if msg.0.len() > PAGE_SIZE {
            return Box::pin(future::ready(Err(Error::PageOutOfBounds)).into_actor(self));
        }

        // Allocate a new page if the page cannot fit the record
    }
}
