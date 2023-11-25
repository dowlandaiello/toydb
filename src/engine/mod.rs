use super::{error::Error, types::table::TableName, util::fs};
use actix::{
    fut::{self, ActorFutureExt, ActorTryFutureExt},
    Actor, Addr, AsyncContext, Context, Handler, ResponseActFuture, WrapFuture,
};
use cmd::ddl::CreateDatabase;
use std::{collections::HashMap, future};
use tokio::fs::{File, OpenOptions};

pub mod cmd;

/// The command processor for ToyDB.
pub struct Engine {
    buffer_pools: HashMap<TableName, Addr<DbHandle>>,
}

impl Actor for Engine {
    type Context = Context<Self>;
}

impl Handler<CreateDatabase> for Engine {
    type Result = ResponseActFuture<Self, Result<Addr<DbHandle>, Error>>;

    fn handle(&mut self, msg: CreateDatabase, _ctx: &mut Context<Self>) -> Self::Result {
        // If the buffer pool is already open, return it
        if let Some(handle) = self.buffer_pools.get(&msg.0) {
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
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(db_path)
                .await
        }
        .into_actor(self)
        .map_ok(|f, slf, _ctx| {
            let act = DbHandle { handle: f }.start();
            slf.buffer_pools.insert(msg.0, act.clone());

            act
        })
        .map_err(|e, _, _| Error::IoError(e));

        Box::pin(open_fut)
    }
}

/// An open instance of a database (i.e., a buffer pool).
pub struct DbHandle {
    handle: File,
}

impl Actor for DbHandle {
    type Context = Context<Self>;
}
