pub mod tree;

use super::{
    super::{error::Error, owned_items::RecordId, types::table::TableName, util::fs},
    buffer_pool::{DbHandle, PAGE_SIZE},
};
use actix::{
    Actor, ActorTryFutureExt, Addr, Context, Handler, Message, ResponseActFuture, ResponseFuture,
    WrapFuture,
};
use std::{collections::HashMap, future, sync::Arc};
use tokio::{fs::OpenOptions, sync::Mutex};
use tree::{TreeHandle, TreeHandleIterator};

/// 4 pages can fit in an index cache
const MAX_TENANTS: usize = PAGE_SIZE * 4;

/// A request to open or fetch an existing index for a database.
#[derive(Message, Debug)]
#[rtype(result = "Result<Addr<IndexHandle>, Error>")]
pub struct GetIndex(pub TableName);

/// Gets a record pointer from a key.
#[derive(Message, Debug)]
#[rtype(result = "Result<RecordId, Error>")]
pub struct GetKey(pub u64);

/// Inserts a record pointer at a key.
#[derive(Message, Debug)]
#[rtype(result = "Result<(), Error>")]
pub struct InsertKey(pub u64, pub RecordId);

/// A message requesting that an actor create a new iterator.
#[derive(Message, Debug)]
#[rtype(result = "Result<Addr<TreeHandleIterator>, Error>")]
pub struct Iter(pub Addr<DbHandle>);

/// An open abstraction representing a cached index for a database.
#[derive(Debug)]
pub struct IndexHandle {
    // Physical file representation
    tree_handle: Addr<TreeHandle>,

    // Cached entries for keys. LRU eviction policy determined by max_tenants
    record_cache: HashMap<u64, RecordId>,
    last_used: Vec<u64>,
}

impl Actor for IndexHandle {
    type Context = Context<Self>;
}

impl Handler<GetKey> for IndexHandle {
    type Result = ResponseActFuture<Self, Result<RecordId, Error>>;

    #[tracing::instrument]
    fn handle(&mut self, msg: GetKey, _ctx: &mut Context<Self>) -> Self::Result {
        tracing::info!("probing key in index");

        // If the record is stored in cache, use that instead of resorting to disk
        if let Some(rid) = self.record_cache.get(&msg.0) {
            return Box::pin(future::ready(Ok(rid.clone())).into_actor(self));
        }

        let tree_handle = self.tree_handle.clone();

        Box::pin(
            async move {
                tree_handle
                    .send(GetKey(msg.0))
                    .await
                    .map_err(|e| Error::MailboxError(e))?
            }
            .into_actor(self)
            .map_ok(move |rid, slf, _ctx| {
                slf.record_cache.insert(msg.0, rid.clone());
                slf.last_used.insert(0, msg.0);

                // Evict any stale entries
                if slf.last_used.len() > MAX_TENANTS {
                    let to_remove = slf.last_used.remove(MAX_TENANTS);
                    slf.record_cache.remove(&to_remove);
                }

                rid
            }),
        )
    }
}

impl Handler<InsertKey> for IndexHandle {
    type Result = ResponseActFuture<Self, Result<(), Error>>;

    #[tracing::instrument]
    fn handle(&mut self, msg: InsertKey, _ctx: &mut Context<Self>) -> Self::Result {
        tracing::info!("inserting key into index");

        self.record_cache.insert(msg.0, msg.1.clone());
        self.last_used.insert(0, msg.0);

        // Evict any stale entries
        if self.last_used.len() > MAX_TENANTS {
            let to_remove = self.last_used.remove(MAX_TENANTS);
            self.record_cache.remove(&to_remove);
        }

        let tree_handle = self.tree_handle.clone();

        Box::pin(
            async move {
                tree_handle
                    .send(msg)
                    .await
                    .map_err(|e| Error::MailboxError(e))?
            }
            .into_actor(self),
        )
    }
}

impl Handler<Iter> for IndexHandle {
    type Result = ResponseActFuture<Self, Result<Addr<TreeHandleIterator>, Error>>;

    #[tracing::instrument]
    fn handle(&mut self, msg: Iter, _ctx: &mut Context<Self>) -> Self::Result {
        let tree_handle = self.tree_handle.clone();

        Box::pin(
            async move {
                tracing::debug!("obtaining tree handle iterator");

                tree_handle
                    .send(msg)
                    .await
                    .map_err(|e| Error::MailboxError(e))?
            }
            .into_actor(self),
        )
    }
}

#[derive(Default, Debug)]
pub struct IndexPool {
    indexes: HashMap<TableName, Addr<IndexHandle>>,
}

impl Actor for IndexPool {
    type Context = Context<Self>;
}

impl Handler<GetIndex> for IndexPool {
    type Result = ResponseActFuture<Self, Result<Addr<IndexHandle>, Error>>;

    #[tracing::instrument]
    fn handle(&mut self, msg: GetIndex, _ctx: &mut Context<Self>) -> Self::Result {
        tracing::info!("requesting index from index pool");

        if let Some(handle) = self.indexes.get(&msg.0) {
            return Box::pin(future::ready(Ok(handle.clone())).into_actor(self));
        }

        // Obtain the path for the database to use
        let db_path = match fs::index_file_path_with_name(msg.0.as_str()) {
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
        .map_ok(|(f, _), slf, _ctx| {
            let tree_handle = TreeHandle {
                handle: Arc::new(Mutex::new(f)),
                seekers: Arc::new(Mutex::new(HashMap::new())),
            }
            .start();

            // Create enough empty page slots to store the entire file's contents
            let act = IndexHandle {
                tree_handle,
                record_cache: HashMap::new(),
                last_used: Vec::new(),
            }
            .start();
            slf.indexes.insert(msg.0, act.clone());

            act
        })
        .map_err(|e, _, _| Error::IoError(e));

        Box::pin(open_fut)
    }
}
