pub mod tree;

use super::{
    super::{error::Error, types::db::RecordId},
    buffer_pool::PAGE_SIZE,
};
use actix::{
    Actor, ActorTryFutureExt, Addr, Context, Handler, Message, ResponseActFuture, ResponseFuture,
    WrapFuture,
};
use std::{collections::HashMap, future};
use tree::TreeHandle;

/// 4 pages can fit in an index cache
const MAX_TENANTS: usize = PAGE_SIZE * 4;

/// Gets a record pointer from a key.
#[derive(Message)]
#[rtype(result = "Result<RecordId, Error>")]
pub struct GetKey(u64);

/// Inserts a record pointer at a key.
#[derive(Message)]
#[rtype(result = "Result<(), Error>")]
pub struct InsertKey(u64, RecordId);

/// An open abstraction representing a cached index for a database.
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

    fn handle(&mut self, msg: GetKey, _ctx: &mut Context<Self>) -> Self::Result {
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
    type Result = ResponseFuture<Result<(), Error>>;

    fn handle(&mut self, msg: InsertKey, _ctx: &mut Context<Self>) -> Self::Result {
        self.record_cache.insert(msg.0, msg.1.clone());
        self.last_used.insert(0, msg.0);

        // Evict any stale entries
        if self.last_used.len() > MAX_TENANTS {
            let to_remove = self.last_used.remove(MAX_TENANTS);
            self.record_cache.remove(&to_remove);
        }

        let tree_handle = self.tree_handle.clone();

        Box::pin(async move {
            tree_handle
                .send(msg)
                .await
                .map_err(|e| Error::MailboxError(e))?
        })
    }
}
