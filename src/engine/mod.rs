use super::{
    error::Error,
    items::{Element, Tuple},
    types::table::{self, CatalogueEntry, Ty},
    util::fs,
};
use buffer_pool::{BufferPool, DbHandle, GetBuffer};
use catalogue::{Catalogue, InsertEntry};
use cmd::{
    ddl::{CreateDatabase, CreateTable},
    dml::Insert,
};
use heap::{HeapHandle, HeapPool};
use index::{GetIndex, IndexPool};

use actix::{Actor, Addr, Context, Handler, ResponseActFuture, ResponseFuture, WrapFuture};
use tokio::fs::OpenOptions;

pub mod buffer_pool;
pub mod catalogue;
pub mod cmd;
pub mod heap;
pub mod index;
pub mod iterator;

/// The command processor for ToyDB.
#[derive(Debug)]
pub struct Engine {
    buffer_pool: Addr<BufferPool>,
    heap_pool: Addr<HeapPool>,
    index_pool: Addr<IndexPool>,
    catalogue: Addr<Catalogue>,
}

impl Engine {
    #[tracing::instrument]
    pub async fn start() -> Result<Addr<Self>, Error> {
        // Obtain the path for the database to use
        tracing::info!("opening catalogue data file");

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

        tracing::info!("spawning buffer pool");
        let buffer_pool = BufferPool::start_default();

        tracing::info!("spawning heap pool");
        let heap_pool = HeapPool::start_default();

        tracing::info!("spawning index pool");
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
            tracing::info!("creating system catalogue");

            let entries = [
                "table_name",
                "file_name",
                "index_name",
                "attr_name",
                "ty",
                "primary_key",
            ]
            .into_iter()
            .filter_map(|attr| {
                Some(CatalogueEntry {
                    table_name: fs::CATALOGUE_TABLE_NAME.to_owned(),
                    file_name: fs::db_file_path_with_name(fs::CATALOGUE_TABLE_NAME)
                        .ok()?
                        .to_str()?
                        .to_owned(),
                    index_name: Some(
                        fs::index_file_path_with_name(fs::CATALOGUE_TABLE_NAME)
                            .ok()?
                            .to_str()?
                            .to_owned(),
                    ),
                    attr_name: attr.to_owned(),
                    ty: Ty::String,
                    primary_key: attr == "table_name" || attr == "attr_name",
                })
            });

            for ent in entries {
                catalogue
                    .send(InsertEntry(ent))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;
            }
        }

        tracing::info!("started");

        Ok(Self {
            buffer_pool,
            heap_pool,
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
    type Result = ResponseActFuture<Self, Result<(), Error>>;

    #[tracing::instrument]
    fn handle(&mut self, msg: CreateTable, _ctx: &mut Context<Self>) -> Self::Result {
        tracing::info!("creating table");

        let CreateTable {
            db_name,
            table_name,
            columns: elements,
            constraints,
        } = msg;
        let catalogue = self.catalogue.clone();
        let index_pool = self.index_pool.clone();

        Box::pin(
            async move {
                let pks = table::into_primary_key(constraints)?;

                // Get catalogue entries for all columns in the database
                let entries = elements.into_iter().filter_map(|(attr_name, ty)| {
                    // There might not be any primary key
                    let is_pk = pks
                        .as_ref()
                        .map(|pks| pks.iter().any(|pks| pks.contains(&attr_name)))
                        .unwrap_or(false);
                    let index_file = pks
                        .as_ref()
                        .map(|pks| pks.join("-"))
                        .and_then(|attr| {
                            fs::index_file_path_with_name_attr(db_name.as_str(), attr.as_str()).ok()
                        })
                        .and_then(|path| Some(path.to_str()?.to_owned()));

                    Some(CatalogueEntry {
                        table_name: table_name.clone(),
                        file_name: fs::db_file_path_with_name(db_name.as_str())
                            .ok()?
                            .to_str()?
                            .to_owned(),
                        index_name: index_file,
                        attr_name,
                        ty,
                        primary_key: is_pk,
                    })
                });

                // Get an index for the primary key
                if let Some(pks) = pks {
                    let _ = index_pool
                        .send(GetIndex(pks.join("-")))
                        .await
                        .map_err(|e| Error::MailboxError(e))?;
                }

                // Write all rel_name descriptor tuples
                for ent in entries {
                    catalogue
                        .send(InsertEntry(ent))
                        .await
                        .map_err(|e| Error::MailboxError(e))??;
                }

                Ok(())
            }
            .into_actor(self),
        )
    }
}

impl Handler<Insert> for Engine {
    type Result = ResponseActFuture<Self, Result<(), Error>>;

    #[tracing::instrument]
    fn handle(&mut self, msg: Insert, _ctx: &mut Context<Self>) -> Self::Result {
        let tuple = Tuple {
            elements: msg
                .values
                .into_iter()
                .map(|elem| Element { data: elem })
                .collect::<Vec<Element>>(),
        };

        tracing::info!(
            "inserting tuple {:?} into table {} in database {}",
            tuple,
            msg.table_name,
            msg.db_name
        );

        Box::pin(
            async move {
                // Get the heap handle for the file
                Ok(())
            }
            .into_actor(self),
        )
    }
}

impl Handler<CreateDatabase> for Engine {
    type Result = ResponseActFuture<Self, Result<Addr<DbHandle>, Error>>;

    #[tracing::instrument]
    fn handle(&mut self, msg: CreateDatabase, _ctx: &mut Context<Self>) -> Self::Result {
        tracing::info!("creating database");

        let buffer_pool = self.buffer_pool.clone();

        Box::pin(
            async move {
                buffer_pool
                    .send(GetBuffer(msg.0))
                    .await
                    .map_err(|e| Error::MailboxError(e))?
            }
            .into_actor(self),
        )
    }
}
