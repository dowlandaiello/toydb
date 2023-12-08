use crate::util::rid;

use super::{
    error::Error,
    items::{Element, Record, Tuple},
    types::table::{self, CatalogueEntry, LabeledTypedTuple, Ty, TypedTuple, Value},
    util::fs,
};
use buffer_pool::{BufferPool, DbHandle, GetBuffer};
use catalogue::{Catalogue, GetEntries, GetEntry, InsertEntry};
use cmd::{
    ddl::{CreateDatabase, CreateTable},
    dml::{
        Aggregate, Cmp, Comparator, ExecuteQuery, GroupBy, Insert, Join, Project, Rename, Select,
    },
};
use heap::{GetHeap, HeapHandle, HeapPool, InsertRecord, Iter as HeapIter};
use index::{GetIndex, IndexPool, InsertKey, Iter};
use iterator::Next;

use actix::{Actor, Addr, AsyncContext, Context, Handler, Response, ResponseActFuture, WrapFuture};
use prost::Message;
use sqlparser::{
    ast::{SetExpr, Statement, TableFactor},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use std::{collections::HashMap, mem};
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
    type Result = ResponseActFuture<Self, Result<Vec<LabeledTypedTuple>, Error>>;

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
                let mut typed_results: Vec<LabeledTypedTuple> = Vec::new();

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

                let cat_attrs = cat
                    .clone()
                    .into_iter()
                    .map(|ent| ent.attr_name)
                    .collect::<Vec<String>>();

                for (i, tup) in untyped_results.into_iter().enumerate() {
                    let mut row: Vec<(String, Value)> = Vec::new();

                    for (j, elem) in tup.elements.into_iter().enumerate() {
                        row.push((cat_attrs[j].clone(), conv(elem, j)?));
                    }

                    if let Some(cmp) = &msg.filter {
                        let tup = LabeledTypedTuple(row);

                        if cmp.has_value(&tup, &cat_attrs) {
                            typed_results.push(tup);
                        }
                    } else {
                        typed_results.push(LabeledTypedTuple(row));
                    }
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

impl Handler<Project> for Engine {
    type Result = ResponseActFuture<Self, Result<Vec<LabeledTypedTuple>, Error>>;

    #[tracing::instrument]
    fn handle(&mut self, msg: Project, _ctx: &mut Context<Self>) -> Self::Result {
        tracing::info!("projecting {} tuples", msg.input.len());

        let catalogue = self.catalogue.clone();

        Box::pin(
            async move {
                Ok(msg
                    .input
                    .into_iter()
                    .map(|row| {
                        LabeledTypedTuple(
                            row.0
                                .into_iter()
                                .filter(|(name, _)| msg.columns.contains(name))
                                .collect::<Vec<(String, Value)>>(),
                        )
                    })
                    .collect::<Vec<LabeledTypedTuple>>())
            }
            .into_actor(self),
        )
    }
}

impl Handler<Join> for Engine {
    type Result = Result<Vec<LabeledTypedTuple>, Error>;

    fn handle(&mut self, msg: Join, _ctx: &mut Context<Self>) -> Self::Result {
        let Join {
            input_1,
            mut input_2,
            cond,
        } = msg;

        let mut results: HashMap<Value, LabeledTypedTuple> = HashMap::new();

        // Get the columns to join on
        let (a, b) = match cond {
            Cmp::Eq(Comparator::Col(a), Comparator::Col(b)) => (a, b),
            _ => {
                return Err(Error::InvalidCondition);
            }
        };

        for tup in input_1 {
            let join_v = tup
                .0
                .iter()
                .find(|(col_name, _)| col_name.as_str() == a.as_str())
                .map(|(_, val)| val.clone())
                .ok_or(Error::JoinColumnNotFound)?;
            results.insert(join_v, tup);
        }

        for tup in input_2.iter_mut() {
            let join_v = tup
                .0
                .iter()
                .find(|(col_name, _)| col_name.as_str() == b.as_str())
                .map(|(_, val)| val.clone())
                .ok_or(Error::JoinColumnNotFound)?;
            results
                .get_mut(&join_v)
                .ok_or(Error::JoinColumnNotFound)?
                .0
                .append(&mut tup.0);
        }

        Ok(results
            .drain()
            .map(|(_, v)| v)
            .collect::<Vec<LabeledTypedTuple>>())
    }
}

impl Handler<Rename> for Engine {
    type Result = Result<Vec<LabeledTypedTuple>, Error>;

    fn handle(&mut self, msg: Rename, _ctx: &mut Context<Self>) -> Self::Result {
        let Rename {
            input,
            target,
            new_name,
        } = msg;

        Ok(input
            .into_iter()
            .map(|tup| {
                LabeledTypedTuple(
                    tup.0
                        .into_iter()
                        .map(|(col_name, val)| {
                            if col_name == target {
                                (new_name.clone(), val)
                            } else {
                                (col_name, val)
                            }
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>())
    }
}

impl Handler<GroupBy> for Engine {
    type Result = Result<Vec<LabeledTypedTuple>, Error>;

    fn handle(&mut self, msg: GroupBy, _ctx: &mut Context<Self>) -> Self::Result {
        let mut results: HashMap<Vec<Value>, Vec<LabeledTypedTuple>> = HashMap::new();

        // Collect all tuples with matching values of all group by columns
        for tup in msg.input {
            let group_by_values = tup
                .0
                .iter()
                .filter(|(col_name, _)| msg.group_by.contains(col_name))
                .map(|(_, val)| val.clone())
                .collect::<Vec<Value>>();

            if !results.contains_key(&group_by_values) {
                results.insert(group_by_values.clone(), Vec::new());
            }

            results.get_mut(&group_by_values).map(|ent| ent.push(tup));
        }

        // Random selected row from group alongside aggregate columns
        let mut final_results: HashMap<Vec<Value>, LabeledTypedTuple> = HashMap::new();

        // Perform all aggregation functions on all groups
        for (k, v) in results.into_iter() {
            for agg in &msg.aggregate {
                match agg {
                    Aggregate::Avg(col) => {
                        let avg_items = v
                            .iter()
                            .filter_map(|tup| {
                                tup.0
                                    .iter()
                                    .filter(|(col_name, _)| col.as_str() == col_name.as_str())
                                    .map(|(_, val)| match val {
                                        Value::Integer(n) => Some(n.clone()),
                                        Value::String(_) => None,
                                    })
                                    .flatten()
                                    .next()
                            })
                            .collect::<Vec<i64>>();
                        let avg = avg_items.iter().sum::<i64>() / avg_items.len() as i64;

                        // Add it as a column
                        let representative = final_results
                            .entry(k.clone())
                            .or_insert_with(|| v[0].clone());
                        representative
                            .0
                            .push((format!("AVG({})", col.clone()), Value::Integer(avg)));
                    }
                    Aggregate::Sum(col) => {
                        let sum_items = v
                            .iter()
                            .filter_map(|tup| {
                                tup.0
                                    .iter()
                                    .filter(|(col_name, _)| col.as_str() == col_name.as_str())
                                    .map(|(_, val)| match val {
                                        Value::Integer(n) => Some(n.clone()),
                                        Value::String(_) => None,
                                    })
                                    .flatten()
                                    .next()
                            })
                            .collect::<Vec<i64>>();
                        let sum = sum_items.iter().sum::<i64>();

                        // Add it as a column
                        let representative = final_results
                            .entry(k.clone())
                            .or_insert_with(|| v[0].clone());
                        representative
                            .0
                            .push((format!("SUM({})", col), Value::Integer(sum)));
                    }
                    Aggregate::Count(col) => {
                        let count_items = v
                            .iter()
                            .filter_map(|tup| {
                                tup.0
                                    .iter()
                                    .filter(|(col_name, _)| {
                                        col.as_ref()
                                            .map(|col| col.as_str() == col_name.as_str())
                                            .unwrap_or(true)
                                    })
                                    .map(|(_, val)| val.clone())
                                    .next()
                            })
                            .collect::<Vec<Value>>();
                        let count = count_items.len();

                        // Add it as a column
                        let representative = final_results
                            .entry(k.clone())
                            .or_insert_with(|| v[0].clone());
                        representative.0.push((
                            format!("COUNT({})", col.clone().unwrap_or("*".to_owned())),
                            Value::Integer(count as i64),
                        ));
                    }
                    Aggregate::Max(col) => {
                        let mut max_items = v
                            .iter()
                            .filter_map(|tup| {
                                tup.0
                                    .iter()
                                    .filter(|(col_name, _)| col.as_str() == col_name.as_str())
                                    .map(|(_, val)| match val {
                                        Value::Integer(n) => Some(n.clone()),
                                        Value::String(_) => None,
                                    })
                                    .flatten()
                                    .next()
                            })
                            .collect::<Vec<i64>>();
                        max_items.sort();
                        let max = max_items[max_items.len() - 1];

                        // Add it as a column
                        let representative = final_results
                            .entry(k.clone())
                            .or_insert_with(|| v[0].clone());
                        representative
                            .0
                            .push((format!("MAX({})", col), Value::Integer(max)));
                    }
                    Aggregate::Min(col) => {
                        let mut min_items = v
                            .iter()
                            .filter_map(|tup| {
                                tup.0
                                    .iter()
                                    .filter(|(col_name, _)| col.as_str() == col_name.as_str())
                                    .map(|(_, val)| match val {
                                        Value::Integer(n) => Some(n.clone()),
                                        Value::String(_) => None,
                                    })
                                    .flatten()
                                    .next()
                            })
                            .collect::<Vec<i64>>();
                        min_items.sort();
                        let min = min_items[0];

                        // Add it as a column
                        let representative = final_results
                            .entry(k.clone())
                            .or_insert_with(|| v[0].clone());
                        representative
                            .0
                            .push((format!("MAX({})", col), Value::Integer(min)));
                    }
                }
            }
        }

        todo!()
    }
}

impl Handler<ExecuteQuery> for Engine {
    type Result = ResponseActFuture<Self, Result<Vec<Vec<LabeledTypedTuple>>, Error>>;

    #[tracing::instrument]
    fn handle(&mut self, msg: ExecuteQuery, ctx: &mut Context<Self>) -> Self::Result {
        tracing::info!("executing query: {:?}", msg);

        let addr = ctx.address();
        let catalogue = self.catalogue.clone();

        Box::pin(
            async move {
                let dialect = PostgreSqlDialect {};
                let ast = Parser::parse_sql(&dialect, msg.query.as_str())
                    .map_err(|e| Error::SqlParserError(e))?;

                let mut results: Vec<Vec<LabeledTypedTuple>> = Vec::new();

                for stmt in ast.into_iter() {
                    match stmt {
                        Statement::CreateTable {
                            name,
                            columns,
                            constraints,
                            ..
                        } => {
                            let q = CreateTable::try_from_sql(
                                msg.db_name.clone(),
                                name,
                                columns,
                                constraints,
                            )?;

                            tracing::debug!("executing query: {:?}", q);

                            addr.send(q).await.map_err(|e| Error::MailboxError(e))??;

                            results.push(Vec::<LabeledTypedTuple>::new());
                        }
                        Statement::CreateDatabase { db_name, .. } => {
                            addr.send(CreateDatabase::from_sql(db_name))
                                .await
                                .map_err(|e| Error::MailboxError(e))??;

                            results.push(Vec::<LabeledTypedTuple>::new());
                        }
                        Statement::Insert {
                            table_name,
                            columns,
                            source,
                            ..
                        } => {
                            let table_name_disp = table_name.0[0].clone().value;

                            // Get the expected list of columns
                            let all_columns = catalogue
                                .send(GetEntries(msg.db_name.clone(), table_name_disp.clone()))
                                .await
                                .map_err(|e| Error::MailboxError(e))??;
                            let col_names = all_columns
                                .into_iter()
                                .map(|ent| (ent.attr_name, ent.ty))
                                .collect::<Vec<(String, Ty)>>();

                            let q = Insert::try_from_sql(
                                msg.db_name.clone(),
                                table_name,
                                col_names,
                                columns,
                                source,
                            )?;

                            tracing::debug!("executing query: {:?}", q);

                            addr.send(q).await.map_err(|e| Error::MailboxError(e))??;

                            results.push(Vec::<LabeledTypedTuple>::new());
                        }
                        // TODO: There is absolutely zero query optimization here.
                        Statement::Query(q) => match *q.body {
                            SetExpr::Select(s) => {
                                let mut in_tuples: Vec<Vec<LabeledTypedTuple>> = Vec::new();
                                let selection =
                                    s.selection.map(|s| Cmp::try_from_sql(s)).transpose()?;

                                for table in s.from {
                                    match table.relation {
                                        TableFactor::Table { mut name, .. } => {
                                            in_tuples.push(
                                                addr.send(Select {
                                                    db_name: msg.db_name.clone(),
                                                    table_name: name.0.remove(0).value,
                                                    filter: selection.clone(),
                                                })
                                                .await
                                                .map_err(|e| Error::MailboxError(e))??,
                                            );
                                        }
                                        o => {
                                            return Err(Error::Unimplemented(Some(format!(
                                                "{:?}",
                                                o
                                            ))));
                                        }
                                    }
                                }

                                results.append(&mut in_tuples);
                            }
                            o => {
                                return Err(Error::Unimplemented(Some(format!("{:?}", o))));
                            }
                        },
                        o => {
                            return Err(Error::Unimplemented(Some(format!("{:?}", o))));
                        }
                    }
                }

                tracing::debug!("query executed with {} results", results.len());

                Ok(results)
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
