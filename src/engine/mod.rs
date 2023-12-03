use super::{
    error::Error,
    items::Tuple,
    types::table::{CatalogueEntry, Ty},
    util::fs,
};
use buffer_pool::{BufferPool, DbHandle, GetBuffer};
use catalogue::{Catalogue, InsertEntry};
use cmd::ddl::{CreateDatabase, CreateTable};
use heap::HeapHandle;
use index::{GetIndex, IndexPool};

use actix::{Actor, Addr, Context, Handler, ResponseFuture};
use tokio::fs::OpenOptions;

pub mod buffer_pool;
pub mod catalogue;
pub mod cmd;
pub mod heap;
pub mod index;
pub mod iterator;

/// The command processor for ToyDB.
pub struct Engine {
    buffer_pool: Addr<BufferPool>,
    index_pool: Addr<IndexPool>,
    catalogue: Addr<Catalogue>,
}

impl Engine {
    pub async fn start() -> Result<Addr<Self>, Error> {
        // Obtain the path for the database to use
        let db_path = fs::db_file_path_with_name(fs::CATALOGUE_TABLE_NAME)?;

        // Open the database file, or create it if it doesn't exist
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_path)
            .await
            .map_err(|e| Error::IoError(e))?;
        let meta = f.metadata().await.map_err(|e| Error::IoError(e))?;

        let buffer_pool = BufferPool::start_default();
        let index_pool = IndexPool::start_default();

        let db_handle = buffer_pool
            .send(GetBuffer(fs::CATALOGUE_TABLE_NAME.to_owned()))
            .await
            .map_err(|e| Error::MailboxError(e))??;
        let heap_handle = HeapHandle { buffer: db_handle }.start();

        let index_handle = index_pool
            .send(GetIndex(fs::CATALOGUE_TABLE_NAME.to_owned()))
            .await
            .map_err(|e| Error::MailboxError(e))??;

        let catalogue = Catalogue {
            db_handle: heap_handle,
            index_db_rel_name_handle: index_handle,
        }
        .start();

        // Write the catalogue entries to the catalogue
        if meta.len() == 0 {
            let entries = ["table_name", "file_name", "attr_name", "ty"]
                .into_iter()
                .filter_map(|attr| {
                    Some(CatalogueEntry {
                        table_name: fs::CATALOGUE_TABLE_NAME.to_owned(),
                        file_name: fs::db_file_path_with_name(fs::CATALOGUE_TABLE_NAME)
                            .ok()?
                            .to_str()?
                            .to_owned(),
                        attr_name: attr.to_owned(),
                        ty: Ty::String,
                    })
                });

            for ent in entries {
                catalogue
                    .send(InsertEntry(ent))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;
            }
        }

        Ok(Self {
            buffer_pool,
            index_pool,
            catalogue,
        }
        .start())
    }
}

impl Actor for Engine {
    type Context = Context<Self>;
}

impl Handler<CreateTable> for Engine {
    type Result = ResponseFuture<Result<(), Error>>;

    fn handle(&mut self, msg: CreateTable, _ctx: &mut Context<Self>) -> Self::Result {
        let CreateTable(db_name, table_name, elements) = msg;
        let catalogue = self.catalogue.clone();

        Box::pin(async move {
            // Get catalogue entries for all columns in the database
            let entries = elements.into_iter().filter_map(|(attr_name, ty)| {
                Some(CatalogueEntry {
                    table_name: table_name.clone(),
                    file_name: fs::db_file_path_with_name(db_name.as_str())
                        .ok()?
                        .to_str()?
                        .to_owned(),
                    attr_name,
                    ty,
                })
            });

            // Write all rel_name descriptor tuples
            for ent in entries {
                catalogue
                    .send(InsertEntry(ent))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;
            }

            Ok(())
        })
    }
}

impl Handler<CreateDatabase> for Engine {
    type Result = ResponseFuture<Result<Addr<DbHandle>, Error>>;

    fn handle(&mut self, msg: CreateDatabase, _ctx: &mut Context<Self>) -> Self::Result {
        let buffer_pool = self.buffer_pool.clone();

        Box::pin(async move {
            buffer_pool
                .send(GetBuffer(msg.0))
                .await
                .map_err(|e| Error::MailboxError(e))?
        })
    }
}
