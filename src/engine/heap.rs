use super::{
    super::error::Error,
    buffer_pool::{DbHandle, LoadHead, NewPage, PAGE_SIZE},
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

        // Load the last page in the file, or create a new one
        let req = self.buffer.send(LoadHead);

        let req_new_page = self.buffer.send(NewPage);
        let req_bigger_page = self.buffer.send(NewPage);

        // Allocate a new page if the page cannot fit the record
        Box::pin(async move {
            let mut page = if let Some(page) = req.await.map_err(|e| Error::MailboxError(e))?? {
                page
            } else {
                req_new_page.await.map_err(|e| Error::MailboxError(e))??
            };

            // Check if there is space in the page. If not, allocate a new buffer
            if msg.0.len() > PAGE_SIZE - page.space_used().unwrap_or(0) {
                page = req_bigger_page
                    .await
                    .map_err(|e| Error::MailboxError(e))??;
            }

            // Insert the record

            Ok(())
        })
    }
}
