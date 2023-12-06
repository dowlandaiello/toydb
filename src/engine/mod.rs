use crate::util::rid;

use super::{
    error::Error,
    items::{Element, Record, Tuple},
    types::table::{self, CatalogueEntry, Ty, TypedTuple, Value},
    util::fs,
};
use buffer_pool::{BufferPool, DbHandle, GetBuffer};
use catalogue::{Catalogue, GetEntries, InsertEntry};
use cmd::{
    ddl::{CreateDatabase, CreateTable},
    dml::{Insert, Select},
};
use heap::{GetHeap, HeapHandle, HeapPool, InsertRecord, Iter as HeapIter};
use index::{GetIndex, IndexPool, InsertKey, Iter};
use iterator::Next;

use actix::{Actor, Addr, Context, Handler, ResponseActFuture, WrapFuture};
use prost::Message;
use std::mem;
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
                "attr_index",
                "ty",
                "primary_key",
            ]
            .into_iter()
            .enumerate()
            .filter_map(|(i, attr)| {
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
                    attr_index: i,
                    ty: Ty::String,
                    primary_key: attr == "file_name" || attr == "table_name" || attr == "attr_name",
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
                let entries =
                    elements
                        .into_iter()
                        .enumerate()
                        .filter_map(|(i, (attr_name, ty))| {
                            // There might not be any primary key
                            let is_pk = pks
                                .as_ref()
                                .map(|pks| pks.iter().any(|pks| pks.contains(&attr_name)))
                                .unwrap_or(false);
                            let index_file = pks
                                .as_ref()
                                .map(|pks| pks.join("-"))
                                .and_then(|attr| {
                                    fs::index_file_path_with_name_attr(
                                        table_name.as_str(),
                                        attr.as_str(),
                                    )
                                    .ok()
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
                                attr_index: i,
                                ty,
                                primary_key: is_pk,
                            })
                        });

                // Get an index for the primary key
                if let Some(pks) = &pks {
                    let _ = index_pool
                        .send(GetIndex(fs::index_name_with_name_attr(
                            table_name.clone(),
                            pks.join("-"),
                        )))
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
        let buffer_pool = self.buffer_pool.clone();
        let heap_pool = self.heap_pool.clone();
        let catalogue = self.catalogue.clone();
        let index_pool = self.index_pool.clone();

        Box::pin(
            async move {
                let db_handle = buffer_pool
                    .send(GetBuffer(msg.db_name.clone()))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;
                let heap_handle = heap_pool
                    .send(GetHeap(msg.db_name.clone(), db_handle))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;

                // Check the catalogue to see if there is a primary key for this table
                let cat = catalogue
                    .send(GetEntries(msg.db_name.clone(), msg.table_name.clone()))
                    .await
                    .map_err(|e| Error::MailboxError(e))?
                    .map_err(|_| Error::MissingCatalogueEntry)?;

                let tuple = Tuple {
                    rel_name: msg.table_name.clone(),
                    elements: msg
                        .values
                        .iter()
                        .map(|elem| Element { data: elem.clone() })
                        .collect::<Vec<Element>>(),
                };
                let tuple_enc = tuple.encode_length_delimited_to_vec();

                tracing::info!(
                    "inserting tuple {:?} into table {} in database {}",
                    tuple,
                    msg.table_name,
                    msg.db_name
                );

                // Insert the tuple into the heap
                let rid = heap_handle
                    .send(InsertRecord(Record {
                        size: tuple_enc.len() as u64,
                        data: tuple_enc,
                    }))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;

                tracing::debug!("successfully inserted tuple into the heap: {:?}", rid);

                // There are primary keys, so let's find them and make a composite key
                // of them
                let primary_keys = cat
                    .iter()
                    .enumerate()
                    .filter(|(_, cat)| cat.primary_key)
                    .map(|(i, _)| i)
                    .collect::<Vec<usize>>();
                let primary_keys_names = cat
                    .iter()
                    .enumerate()
                    .filter(|(_, cat)| cat.primary_key)
                    .map(|(_, pk)| pk.attr_name.clone())
                    .collect::<Vec<String>>();
                let k_values = msg
                    .values
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| primary_keys.contains(i))
                    .map(|(_, value)| value.clone())
                    .collect::<Vec<Vec<u8>>>();

                let composite_key = rid::key_for_composite_key(k_values);

                // Insert into the index
                let idx = index_pool
                    .send(GetIndex(fs::index_name_with_name_attr(
                        msg.table_name,
                        primary_keys_names.join("-"),
                    )))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;
                idx.send(InsertKey(composite_key, rid))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;

                Ok(())
            }
            .into_actor(self),
        )
    }
}

impl Handler<Select> for Engine {
    type Result = ResponseActFuture<Self, Result<Vec<TypedTuple>, Error>>;

    fn handle(&mut self, msg: Select, _ctx: &mut Context<Self>) -> Self::Result {
        let heap_pool = self.heap_pool.clone();
        let buffer_pool = self.buffer_pool.clone();
        let catalogue = self.catalogue.clone();
        let index_pool = self.index_pool.clone();

        Box::pin(
            async move {
                let db_handle = buffer_pool
                    .send(GetBuffer(msg.db_name.clone()))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;
                let heap_handle = heap_pool
                    .send(GetHeap(msg.db_name.clone(), db_handle.clone()))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;

                tracing::debug!(
                    "looking up catalogue entries for table {} in db {}",
                    &msg.table_name,
                    &msg.db_name
                );

                // Look up the table to see if there's an index we can scan over to be more selective
                let mut cat = catalogue
                    .send(GetEntries(msg.db_name.clone(), msg.table_name.clone()))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;

                tracing::debug!("got catalogue entries: {:?}", cat);

                let mut untyped_results: Vec<Tuple> = Vec::new();

                if let Some(index_name) = cat
                    .get(0)
                    .and_then(|cat| cat.index_name.as_ref())
                    .and_then(|index_path| fs::index_name_from_file_path(index_path))
                {
                    tracing::debug!("obtaining an index to search through database selectively");

                    let idx = index_pool
                        .send(GetIndex(index_name))
                        .await
                        .map_err(|e| Error::MailboxError(e))??;

                    tracing::debug!(
                        "got an index for table {} in db {}",
                        &msg.table_name,
                        &msg.db_name
                    );

                    // Get an iterator over the index
                    let iter = idx
                        .send(Iter(db_handle))
                        .await
                        .map_err(|e| Error::MailboxError(e))??;

                    // Keep polling the iterator for results until none are left
                    let mut next: Option<Tuple> =
                        iter.send(Next).await.map_err(|e| Error::MailboxError(e))?;

                    loop {
                        let n = if let Some(next) = next {
                            next
                        } else {
                            break;
                        };

                        tracing::debug!("got index entry {:?}", n);

                        untyped_results.push(n);

                        next = iter.send(Next).await.map_err(|e| Error::MailboxError(e))?;
                    }
                } else {
                    // Get an iterator over the heap
                    let iter = heap_handle
                        .send(HeapIter)
                        .await
                        .map_err(|e| Error::MailboxError(e))?;

                    // Keep polling the iterator for results until none are left
                    let mut next: Option<Tuple> =
                        iter.send(Next).await.map_err(|e| Error::MailboxError(e))?;

                    loop {
                        let n = if let Some(next) = next {
                            next
                        } else {
                            break;
                        };

                        if n.rel_name == msg.table_name {
                            untyped_results.push(n);
                        }

                        next = iter.send(Next).await.map_err(|e| Error::MailboxError(e))?;
                    }
                }

                // Convert all the untyped results into typed results
                let mut typed_results: Vec<TypedTuple> = Vec::new();

                // Generate functions for converting each column
                cat.sort_by_key(|x| x.attr_index);

                // Converts an element to the typed value of the column at its index
                let conv = |elem: Element, index: usize| -> Result<Value, Error> {
                    let ent = &cat[index];

                    match ent.ty {
                        Ty::String => Ok(Value::String(
                            String::from_utf8(elem.data).map_err(|_| Error::MiscDecodeError)?,
                        )),
                        Ty::Integer => {
                            let int_bytes: [u8; mem::size_of::<i64>()] =
                                elem.data.try_into().map_err(|_| Error::MiscDecodeError)?;
                            Ok(Value::Integer(i64::from_le_bytes(int_bytes)))
                        }
                    }
                };

                for tup in untyped_results {
                    let mut row: Vec<Value> = Vec::new();

                    for (i, elem) in tup.elements.into_iter().enumerate() {
                        row.push(conv(elem, i)?);
                    }

                    typed_results.push(TypedTuple(row));
                }

                tracing::info!(
                    "selected {} results from {} in {}",
                    typed_results.len(),
                    msg.table_name,
                    msg.db_name
                );

                Ok(typed_results)
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
