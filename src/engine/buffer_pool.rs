use super::super::{
    error::Error,
    items::Page,
    types::db::{DbName, PageId},
    util::fs,
};
use actix::{
    fut::ActorTryFutureExt, Actor, Addr, AsyncContext, Context, Handler, Message,
    ResponseActFuture, ResponseFuture, WrapFuture,
};
use futures::future::TryFutureExt;
use prost::Message as ProstMessage;
use std::{collections::HashMap, future, sync::Arc};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom},
    sync::Mutex,
};

/// The size of pages loaded from heap files in bytes.
pub const PAGE_SIZE: usize = 8_000;

/// A request to open or fetch an existing buffer for a database.
#[derive(Message)]
#[rtype(result = "Result<Addr<DbHandle>, Error>")]
pub struct GetBuffer(pub DbName);

/// A request to load a page from the heap file.
#[derive(Message)]
#[rtype(result = "Result<Page, Error>")]
pub struct LoadPage(pub PageId);

/// A request to load the last page in the heap file.
#[derive(Message)]
#[rtype(result = "Result<Option<(Page, PageId)>, Error>")]
pub struct LoadHead;

/// A request to create a new page in the heap file.
#[derive(Message)]
#[rtype(result = "Result<(Page, PageId), Error>")]
pub struct NewPage;

/// A request to write a page to a heap file.
#[derive(Message)]
#[rtype(result = "Result<(), Error>")]
pub struct WritePage(pub PageId, pub Page);

/// A pool of buffers for databases.
#[derive(Default)]
pub struct BufferPool {
    pools: HashMap<DbName, Addr<DbHandle>>,
}

impl Actor for BufferPool {
    type Context = Context<Self>;
}

impl Handler<GetBuffer> for BufferPool {
    type Result = ResponseActFuture<Self, Result<Addr<DbHandle>, Error>>;

    fn handle(&mut self, msg: GetBuffer, _ctx: &mut Context<Self>) -> Self::Result {
        // If the buffer pool is already open, return it
        if let Some(handle) = self.pools.get(&msg.0) {
            return Box::pin(future::ready(Ok(handle.clone())).into_actor(self));
        }

        // Obtain the path for the database to use
        let db_path = match fs::db_file_path_with_name(msg.0.as_str()) {
            Ok(p) => p,
            Err(e) => {
                return Box::pin(future::ready(Err(e)).into_actor(self));
            }
        };

        // Open the database file, or create it if it doesn't exist
        let open_fut = async move {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(db_path)
                .await?;
            let meta = f.metadata().await?;

            Ok((f, meta))
        }
        .into_actor(self)
        .map_ok(|(f, meta), slf, _ctx| {
            // Create enough empty page slots to store the entire file's contents
            let act = DbHandle {
                handle: Arc::new(Mutex::new(f)),
                pages: vec![None; f64::floor(meta.len() as f64 / (PAGE_SIZE as f64)) as usize],
            }
            .start();
            slf.pools.insert(msg.0, act.clone());

            act
        })
        .map_err(|e, _, _| Error::IoError(e));

        Box::pin(open_fut)
    }
}

/// An open instance of a database (i.e., a buffer pool).
pub struct DbHandle {
    handle: Arc<Mutex<File>>,
    pages: Vec<Option<Page>>,
}

impl Actor for DbHandle {
    type Context = Context<Self>;
}

impl Handler<LoadPage> for DbHandle {
    type Result = ResponseActFuture<Self, Result<Page, Error>>;

    fn handle(&mut self, msg: LoadPage, _ctx: &mut Context<Self>) -> Self::Result {
        // If the page already exists, return it
        if let Some(Some(page)) = self.pages.get(msg.0) {
            return Box::pin(future::ready(Ok(page.clone())).into_actor(self));
        };

        let handle_lock = self.handle.clone();

        // Seek to the position in the file that the page is located at
        let read_fut = async move {
            let mut handle = handle_lock.lock().await;

            handle
                .seek(SeekFrom::Start((msg.0 as usize * PAGE_SIZE) as u64))
                .await
                .map_err(|e| Error::IoError(e))?;

            // Load the page
            let mut buff = [0; PAGE_SIZE];
            handle
                .read_exact(&mut buff)
                .await
                .map_err(|e| Error::IoError(e))?;

            // Decode the page (size delimited)
            let page = Page::decode_length_delimited(buff.as_slice())
                .map_err(|e| Error::DecodeError(e))?;
            Ok(page)
        }
        .into_actor(self)
        .map_ok(move |page, slf, _ctx| {
            slf.pages[msg.0] = Some(page.clone());

            page
        });

        Box::pin(read_fut)
    }
}

impl Handler<LoadHead> for DbHandle {
    type Result = ResponseFuture<Result<Option<(Page, PageId)>, Error>>;

    fn handle(&mut self, _msg: LoadHead, ctx: &mut Context<Self>) -> Self::Result {
        if self.pages.is_empty() {
            return Box::pin(future::ready(Ok(None)));
        }

        let head_idx = self.pages.len() - 1;
        let addr = ctx.address();

        // Load the last page currently open
        Box::pin(async move {
            addr.send(LoadPage(head_idx))
                .await
                .map_err(|e| Error::MailboxError(e))?
                .map(|page| Some((page, head_idx)))
        })
    }
}

impl Handler<NewPage> for DbHandle {
    type Result = ResponseFuture<Result<(Page, PageId), Error>>;

    fn handle(&mut self, _msg: NewPage, ctx: &mut Context<Self>) -> Self::Result {
        // Create the page in memory
        let page = Page::default();

        // Write the new page to memory
        self.pages.push(Some(page.clone()));
        let head_idx = self.pages.len() - 1;

        let addr = ctx.address();

        // Write the new page to the disk
        Box::pin(async move {
            addr.send(WritePage(head_idx, page.clone()))
                .map_err(|e| Error::MailboxError(e))
                .await??;

            Ok((page, head_idx))
        })
    }
}

impl Handler<WritePage> for DbHandle {
    type Result = ResponseFuture<Result<(), Error>>;

    fn handle(&mut self, msg: WritePage, _ctx: &mut Context<Self>) -> Self::Result {
        // Write the page to memory
        self.pages[msg.0] = Some(msg.1.clone());

        // TODO: Make this happen in an interval not readily commit
        let handle_lock = self.handle.clone();

        // Commit the page to disk
        let write_fut = async move {
            let mut handle = handle_lock.lock().await;
            handle
                .seek(SeekFrom::Start((msg.0 as usize * PAGE_SIZE) as u64))
                .await
                .map_err(|e| Error::IoError(e))?;

            let encoded = msg.1.encode_length_delimited_to_vec();

            // Write the page
            handle
                .write_all(encoded.as_slice())
                .await
                .map_err(|e| Error::IoError(e))?;

            Ok(())
        };

        Box::pin(write_fut)
    }
}
