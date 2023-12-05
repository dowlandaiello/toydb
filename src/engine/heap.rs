use super::{
    super::{
        error::Error,
        items::{Record, RecordId, Tuple},
        types::db::DbName,
    },
    buffer_pool::{DbHandle, LoadHead, LoadPage, NewPage, WritePage, PAGE_SIZE},
    iterator::Next,
};
use actix::{
    Actor, ActorTryFutureExt, Addr, AsyncContext, Context, Handler, Message, ResponseActFuture,
    ResponseFuture, WrapFuture,
};
use prost::Message as ProstMessage;
use std::{collections::HashMap, future, sync::Arc};
use tokio::sync::Mutex;

/// Inserts a record at the end of the heap file.
#[derive(Message)]
#[rtype(result = "Result<RecordId, Error>")]
pub struct InsertRecord(pub Record);

/// Loads a record from the heap file.
#[derive(Message, Debug)]
#[rtype(result = "Result<Record, Error>")]
pub struct LoadRecord(RecordId);

/// Gets a heap handle for the database.
#[derive(Message, Debug)]
#[rtype(result = "Result<Addr<HeapHandle>, Error>")]
pub struct GetHeap(pub DbName, Addr<DbHandle>);

/// A message sent to a heaphandle that generates a new iterator.
#[derive(Message)]
#[rtype(result = "Addr<HeapHandleIterator>")]
pub struct Iter;

#[derive(Default, Debug)]
pub struct HeapPool {
    heaps: HashMap<DbName, Addr<HeapHandle>>,
}

impl Actor for HeapPool {
    type Context = Context<Self>;
}

impl Handler<GetHeap> for HeapPool {
    type Result = ResponseActFuture<Self, Result<Addr<HeapHandle>, Error>>;

    #[tracing::instrument]
    fn handle(&mut self, msg: GetHeap, _ctx: &mut Context<Self>) -> Self::Result {
        tracing::info!("requesting heap from heap pool");

        if let Some(handle) = self.heaps.get(&msg.0) {
            return Box::pin(future::ready(Ok(handle.clone())).into_actor(self));
        }

        Box::pin(
            async move { Ok(HeapHandle { buffer: msg.1 }.start()) }
                .into_actor(self)
                .map_ok(|addr, slf, _ctx| {
                    slf.heaps.insert(msg.0, addr);

                    addr
                }),
        )
    }
}

/// An open heap abstraction.
/// Allows:
/// - Insertion of records by RID
/// - Deletion of records by RID
/// - Retrieval of records by RID
pub struct HeapHandle {
    pub buffer: Addr<DbHandle>,
}

impl Actor for HeapHandle {
    type Context = Context<Self>;
}

impl Handler<InsertRecord> for HeapHandle {
    type Result = ResponseFuture<Result<RecordId, Error>>;

    fn handle(&mut self, msg: InsertRecord, _ctx: &mut Context<Self>) -> Self::Result {
        // The record must not be larger than the size of a page
        if msg.0.data.len() > PAGE_SIZE {
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
            if msg.0.data.len() > PAGE_SIZE - page.space_used as usize {
                (page, page_index) = buff
                    .send(NewPage)
                    .await
                    .map_err(|e| Error::MailboxError(e))??;
            }

            // Insert the record
            let idx = page.data.len();
            page.data.push(msg.0);
            buff.send(WritePage(page_index, page))
                .await
                .map_err(|e| Error::MailboxError(e))??;

            Ok(RecordId {
                page: page_index as u64,
                page_idx: idx as u64,
            })
        })
    }
}

impl Handler<LoadRecord> for HeapHandle {
    type Result = ResponseFuture<Result<Record, Error>>;

    fn handle(&mut self, msg: LoadRecord, _ctx: &mut Context<Self>) -> Self::Result {
        let RecordId { page, page_idx } = msg.0;
        let buff = self.buffer.clone();

        Box::pin(async move {
            // Read the page that the record is in
            let page = buff
                .send(LoadPage(page as usize))
                .await
                .map_err(|e| Error::MailboxError(e))??;

            // Load the record from the page
            let val = page
                .data
                .get(page_idx as usize)
                .ok_or(Error::RecordNotFound)
                .cloned()?;

            Ok(val)
        })
    }
}

impl Handler<Iter> for HeapHandle {
    type Result = Addr<HeapHandleIterator>;

    fn handle(&mut self, _msg: Iter, ctx: &mut Context<Self>) -> Self::Result {
        let addr = ctx.address();
        HeapHandleIterator {
            handle: addr,
            curr_rid: Arc::new(Mutex::new(RecordId::default())),
        }
        .start()
    }
}

/// A seeker that can iterate through the tuples in a heap file.
pub struct HeapHandleIterator {
    handle: Addr<HeapHandle>,
    curr_rid: Arc<Mutex<RecordId>>,
}

impl Actor for HeapHandleIterator {
    type Context = Context<Self>;
}

impl Handler<Next> for HeapHandleIterator {
    type Result = ResponseFuture<Option<Tuple>>;

    fn handle(&mut self, _msg: Next, _context: &mut Context<Self>) -> Self::Result {
        let curr_rid_handle = self.curr_rid.clone();
        let handle = self.handle.clone();

        Box::pin(async move {
            let mut curr_rid = curr_rid_handle.lock().await;

            let val = match handle
                .send(LoadRecord((*curr_rid).clone()))
                .await
                .ok()?
                .ok()
            {
                Some(v) => v,
                None => {
                    *curr_rid = RecordId {
                        page: curr_rid.page + 1,
                        page_idx: 0,
                    };

                    handle
                        .send(LoadRecord((*curr_rid).clone()))
                        .await
                        .ok()?
                        .ok()?
                }
            };

            // Update the current RID
            curr_rid.page_idx += 1;

            Some(Tuple::decode_length_delimited(val.data.as_slice()).ok()?)
        })
    }
}
