use super::{
    super::{
        super::{
            error::Error,
            items::{Page, RecordId, Tuple},
            types::table::TableName,
            util::fs,
            ORDER,
        },
        buffer_pool::{DbHandle, LoadPage},
        iterator::{Iterator as TupleIterator, Next},
    },
    mem_tree::RecordIdPointer,
    GetKey, InsertKey, Iter,
};
use actix::{
    Actor, Addr, AsyncContext, Context, Handler, Message, ResponseActFuture, ResponseFuture,
    WrapFuture,
};
use futures::future::BoxFuture;
use prost::Message as ProstMessage;
use std::{collections::BTreeMap, future, future::Future, pin::Pin, sync::Arc};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, OwnedMutexGuard},
};

/// An abstraction representing a handle to a B+ tree index file for a database.
#[derive(Debug)]
pub struct TreeHandle {
    pub table_name: TableName,
    pub tree: BTreeMap<u64, RecordIdPointer>,
}

impl Actor for TreeHandle {
    type Context = Context<Self>;

    fn stopped(&mut self, ctx: &mut Context<Self>) {
        let ser = bincode::serialize(&self.tree)
            .expect("failed to flush index tree: failed to serialize tree");

        let db_path = fs::index_file_path_with_name(self.table_name.clone())
            .expect("failed to flush index tree: couldn't formulate db path");

        ctx.spawn(
            async move {
                let mut f = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(db_path)
                    .await
                    .map_err(|e| Error::IoError(e))
                    .expect("failed to flush index tree: IO error");

                f.write_all(ser.as_slice())
                    .await
                    .expect("failed to flush index tree: IO error while writing tree");
            }
            .into_actor(self),
        );
    }
}

impl Handler<GetKey> for TreeHandle {
    type Result = Result<RecordId, Error>;

    fn handle(&mut self, msg: GetKey, _ctx: &mut Context<Self>) -> Self::Result {
        let res = self.tree.get(&msg.0);

        if let Some(v) = res {
            Ok(v.clone().into())
        } else {
            Err(Error::RecordNotFound)
        }
    }
}

impl Handler<InsertKey> for TreeHandle {
    type Result = Result<(), Error>;

    #[tracing::instrument]
    fn handle(&mut self, msg: InsertKey, ctx: &mut Context<Self>) -> Self::Result {
        tracing::info!("inserting key into indexing tree");

        self.tree.insert(msg.0, msg.1.into());
        Ok(())
    }
}

impl Handler<Iter> for TreeHandle {
    type Result = Result<TreeHandleIterator, Error>;

    fn handle(&mut self, msg: Iter, context: &mut Context<Self>) -> Self::Result {
        let addr = context.address();

        Ok(TreeHandleIterator {
            handle_data: msg.0,
            vals: self
                .tree
                .values()
                .cloned()
                .collect::<Vec<RecordIdPointer>>(),
        })
    }
}

#[derive(Debug)]
pub struct TreeHandleIterator {
    handle_data: Addr<DbHandle>,
    vals: Vec<RecordIdPointer>,
}

impl TupleIterator for TreeHandleIterator {
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Option<Tuple>>>> {
        tracing::debug!("obtaining next entry in index");

        let handle_data = self.handle_data.clone();

        if self.vals.is_empty() {
            return Box::pin(future::ready(None));
        }

        // Load the next RID we have available
        let mut rid = self.vals.remove(0);

        Box::pin(async move {
            tracing::debug!("got iteration RID: {:?}", rid);

            // Load from the heap file
            let res: Result<Page, Error> = handle_data
                .send(LoadPage(rid.page as usize))
                .await
                .map_err(|e| Error::MailboxError(e))
                .ok()?;
            let page = res.ok()?;

            let tup_bytes = page.data.get(rid.page_idx as usize)?;
            let tup = Tuple::decode_length_delimited(tup_bytes.data.as_slice()).ok()?;

            Some(tup)
        })
    }
}
