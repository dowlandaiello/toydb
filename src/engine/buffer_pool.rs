use super::super::{
    error::Error,
    types::db::{DbName, PageIndex},
    util::fs,
};
use actix::{
    fut::ActorTryFutureExt, Actor, Addr, Context, Handler, Message, ResponseActFuture,
    ResponseFuture, WrapFuture,
};
use futures::future::TryFutureExt;
use std::{collections::HashMap, future, mem, sync::Arc};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom},
};

/// The size of pages loaded from heap files in bytes.
pub const PAGE_SIZE: usize = 8_000;

/// A fixed-size page of 8kB.
pub struct Page(Arc<[u8; PAGE_SIZE]>);

impl Page {
    /// Calculates the number of bytes used in the page.
    pub fn space_used(&self) -> Option<usize> {
        let bytes: [u8; mem::size_of::<usize>()] = (&self.0[PAGE_SIZE - mem::size_of::<usize>()..])
            .try_into()
            .ok()?;

        Some(usize::from_le_bytes(bytes))
    }

    // Determines the absolute index in the page's bytes corresponding to an index of records
    fn follow_to_index(&self, raw_pos: usize, curr: usize, index: usize) -> Option<(usize, usize)> {
        let record_size_bytes: [u8; mem::size_of::<usize>()] = (&self.0
            [raw_pos..raw_pos + mem::size_of::<usize>()])
            .try_into()
            .ok()?;
        let record_size = usize::from_le_bytes(record_size_bytes);

        if curr == index {
            Some((raw_pos, record_size))
        } else {
            self.follow_to_index(raw_pos + record_size, curr + 1, index)
        }
    }

    pub fn get(&self, i: usize) -> Option<&[u8]> {
        let (pos, size) = self.follow_to_index(0, 0, i)?;
        Some(&self.0[pos..pos + size])
    }
}

/// A request to open or fetch an existing buffer for a database.
#[derive(Message)]
#[rtype(result = "Result<Addr<DbHandle>, Error>")]
pub struct GetBuffer(pub DbName);

/// A request to load a page from the heap file.
#[derive(Message)]
#[rtype(result = "Result<Page, Error>")]
pub struct LoadPage(pub PageIndex);

/// A request to write a page to a heap file.
#[derive(Message)]
#[rtype(result = "Result<(), Error>")]
pub struct WritePage(pub PageIndex, pub Page);

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
                handle: f,
                pages: vec![None; meta.len() as usize / PAGE_SIZE + 1],
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
    handle: File,
    pages: Vec<Option<Page>>,
}

impl Actor for DbHandle {
    type Context = Context<Self>;
}

impl Handler<LoadPage> for DbHandle {
    type Result = ResponseActFuture<Self, Result<Page, Error>>;

    fn handle(&mut self, msg: LoadPage, ctx: &mut Context<Self>) -> Self::Result {
        // If the page already exists, return it
        if let Some(Some(page)) = self.pages.get(msg.0) {
            return Box::pin(future::ready(Ok(*page)).into_actor(self));
        };

        // Seek to the position in the file that the page is located at
        let read_fut = async move {
            let handle = self.handle;

            handle
                .seek(SeekFrom::Start((msg.0 as usize * PAGE_SIZE) as u64))
                .await?;

            // Load the page
            let mut buff = [0; PAGE_SIZE];
            handle.read_exact(&mut buff).await?;

            Ok(Arc::new(buff))
        }
        .into_actor(self)
        .map_err(|e, _, _| Error::IoError(e))
        .map_ok(|page, slf, _ctx| {
            slf.pages[msg.0] = Some(page.clone());

            page
        });

        Box::pin(read_fut)
    }
}

impl Handler<WritePage> for DbHandle {
    type Result = ResponseFuture<Result<(), Error>>;

    fn handle(&mut self, msg: WritePage, ctx: &mut Context<Self>) -> Self::Result {
        // Write the page to memory
        self.pages[msg.0] = Some(msg.1);

        // Commit the page to disk
        let write_fut = async move {
            let handle = self.handle;

            handle
                .seek(SeekFrom::Start((msg.0 as usize * PAGE_SIZE) as u64))
                .await?;

            // Write the page
            handle.write_all(msg.1 .0.as_slice()).await?;

            Ok(())
        }
        .map_err(|e| Error::IoError(e));

        Box::pin(write_fut)
    }
}
