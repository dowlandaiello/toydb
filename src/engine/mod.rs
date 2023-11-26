use super::error::Error;
use actix::{Actor, Addr, Context, Handler, ResponseFuture};
use buffer_pool::{BufferPool, DbHandle, GetBuffer};
use cmd::ddl::{CreateDatabase, CreateTable};
use std::default::Default;

pub mod buffer_pool;
pub mod cmd;

/// The command processor for ToyDB.
pub struct Engine {
    buffer_pool: Addr<BufferPool>,
}

impl Default for Engine {
    fn default() -> Self {
        Self {
            buffer_pool: BufferPool::start_default(),
        }
    }
}

impl Actor for Engine {
    type Context = Context<Self>;
}

impl Handler<CreateDatabase> for Engine {
    type Result = ResponseFuture<Result<Addr<DbHandle>, Error>>;

    fn handle(&mut self, msg: CreateDatabase, _ctx: &mut Context<Self>) -> Self::Result {
        let req = self.buffer_pool.send(GetBuffer(msg.0));
        Box::pin(async move { req.await.map_err(|e| Error::MailboxError(e))? })
    }
}

impl Handler<CreateTable> for Engine {
    type Result = ResponseFuture<Result<(), Error>>;

    fn handle(&mut self, msg: CreateTable, _ctx: &mut Context<Self>) -> Self::Result {
        // Obtain a buffer from the buffer pool
        let req_buff = self.buffer_pool.send(GetBuffer(msg.0));
        Box::pin(async move {
            let buff = req_buff.await.map_err(|e| Error::MailboxError(e))??;

            // Ask the buffer to create a table
            buff.send(msg).await.map_err(|e| Error::MailboxError(e))?
        })
    }
}
