use super::{
    super::{error::Error, types::db::RecordId},
    buffer_pool::{DbHandle, LoadHead, LoadPage, NewPage, WritePage, PAGE_SIZE},
};
use actix::{Actor, Addr, Context, Handler, Message, ResponseFuture};
use std::future;

/// Inserts a record at the end of the heap file.
#[derive(Message)]
#[rtype(result = "Result<RecordId, Error>")]
pub struct InsertRecord(Vec<u8>);

/// Loads a record from the heap file.
#[derive(Message)]
#[rtype(result = "Result<Vec<u8>, Error>")]
pub struct LoadRecord(RecordId);

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
    type Result = ResponseFuture<Result<RecordId, Error>>;

    fn handle(&mut self, msg: InsertRecord, _ctx: &mut Context<Self>) -> Self::Result {
        // The record must not be larger than the size of a page
        if msg.0.len() > PAGE_SIZE {
            return Box::pin(future::ready(Err(Error::PageOutOfBounds)));
        }

        let buff = self.buffer.clone();

        // Allocate a new page if the page cannot fit the record
        Box::pin(async move {
            // Load the last page in the file, or create a new one
            let (mut page, mut page_index) = if let Some(page) = buff
                .send(LoadHead)
                .await
                .map_err(|e| Error::MailboxError(e))??
            {
                page
            } else {
                buff.send(NewPage)
                    .await
                    .map_err(|e| Error::MailboxError(e))??
            };

            // Check if there is space in the page. If not, allocate a new buffer
            if msg.0.len() > PAGE_SIZE - page.space_used().unwrap_or_default() {
                (page, page_index) = buff
                    .send(NewPage)
                    .await
                    .map_err(|e| Error::MailboxError(e))??;
            }

            // Insert the record
            let idx = page.append(msg.0.as_slice())?;
            buff.send(WritePage(page_index, page))
                .await
                .map_err(|e| Error::MailboxError(e))??;

            Ok(RecordId {
                page: page_index,
                page_idx: idx,
            })
        })
    }
}

impl Handler<LoadRecord> for HeapHandle {
    type Result = ResponseFuture<Result<Vec<u8>, Error>>;

    fn handle(&mut self, msg: LoadRecord, _ctx: &mut Context<Self>) -> Self::Result {
        let RecordId { page, page_idx } = msg.0;
        let buff = self.buffer.clone();

        Box::pin(async move {
            // Read the page that the record is in
            let page = buff
                .send(LoadPage(page))
                .await
                .map_err(|e| Error::MailboxError(e))??;

            // Load the record from the page
            let val = page.get(page_idx).ok_or(Error::RecordNotFound)?;

            Ok(val)
        })
    }
}
