use super::{
    super::error::Error,
    buffer_pool::{DbHandle, LoadHead, PAGE_SIZE},
};
use actix::{Actor, Addr, Context, Handler, Message, ResponseFuture};
use std::future;

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
    type Result = ResponseFuture<Result<(), Error>>;

    fn handle(&mut self, msg: InsertRecord, _ctx: &mut Context<Self>) -> Self::Result {
        // The record must not be larger than the size of a page
        if msg.0.len() > PAGE_SIZE {
            return Box::pin(future::ready(Err(Error::PageOutOfBounds)));
        }

        // Allocate a new page if the page cannot fit the record
        Box::pin(async move {
            // Load the last page in the file
            let page = self
                .buffer
                .send(LoadHead)
                .await
                .map_err(|e| Error::MailboxError(e))??;

            Ok(())
        })
    }
}
