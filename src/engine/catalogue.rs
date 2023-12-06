use super::{
    super::{
        error::Error,
        items::{Record, Tuple},
        types::{
            db::DbName,
            table::{CatalogueEntry, TableName},
        },
        util::{fs, rid},
    },
    heap::{HeapHandle, InsertRecord, Iter, LoadRecord},
    index::{GetKey, IndexHandle, InsertKey},
    iterator::Next,
};
use actix::{Actor, Addr, Context, Handler, Message, ResponseActFuture, WrapFuture};
use prost::Message as ProstMessage;

/// Inserts the catalogue entry into the catalogue.
#[derive(Message, Debug)]
#[rtype(result = "Result<(), Error>")]
pub struct InsertEntry(pub CatalogueEntry);

/// Gets the catalogue entry for the attribute in the table in the specified database.
#[derive(Message, Debug)]
#[rtype(result = "Result<CatalogueEntry, Error>")]
pub struct GetEntry(pub DbName, pub TableName, pub String);

/// Gets all of the catalogue entries for the table in the specified database.
#[derive(Message, Debug)]
#[rtype(result = "Result<Vec<CatalogueEntry>, Error>")]
pub struct GetEntries(pub DbName, pub TableName);

#[derive(Debug)]
pub struct Catalogue {
    // An index on the values of dbname and relname
    pub index_db_rel_name_handle: Addr<IndexHandle>,
    pub db_handle: Addr<HeapHandle>,
}

impl Actor for Catalogue {
    type Context = Context<Self>;
}

impl Handler<InsertEntry> for Catalogue {
    type Result = ResponseActFuture<Self, Result<(), Error>>;

    #[tracing::instrument]
    fn handle(&mut self, msg: InsertEntry, _ctx: &mut Context<Self>) -> Self::Result {
        tracing::debug!("inserting catalogue entry {:?}", msg);

        let db_handle = self.db_handle.clone();
        let index_db_rel_name_handle = self.index_db_rel_name_handle.clone();

        Box::pin(
            async move {
                // For inserting into the index
                let k = rid::key_for_rel_db_attr(
                    msg.0.file_name.as_str(),
                    msg.0.table_name.as_str(),
                    msg.0.attr_name.as_str(),
                );

                let tuple: Tuple = msg.0.into();
                let enc = tuple.encode_length_delimited_to_vec();

                // Add the entry to the page
                tracing::debug!(
                    "inserting tuple {:?}: {:?} into catalogue at key {}",
                    tuple,
                    enc,
                    k
                );
                tracing::info!("inserting entry into data file");
                let rid = db_handle
                    .send(InsertRecord(Record {
                        size: enc.len() as u64,
                        data: enc,
                    }))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;

                // Add it to the index, too
                tracing::info!("inserting entry into index file");
                index_db_rel_name_handle
                    .send(InsertKey(k, rid))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;

                tracing::info!("successfully inserted entry into index file");

                Ok(())
            }
            .into_actor(self),
        )
    }
}

impl Handler<GetEntry> for Catalogue {
    type Result = ResponseActFuture<Self, Result<CatalogueEntry, Error>>;

    #[tracing::instrument]
    fn handle(&mut self, msg: GetEntry, _ctx: &mut Context<Self>) -> Self::Result {
        // Since we are searching by the primary key
        tracing::info!(
            "looking up primary key ({}, {}, {}) in the catalogue",
            msg.0,
            msg.1,
            msg.2
        );

        let index_handle = self.index_db_rel_name_handle.clone();
        let db_handle = self.db_handle.clone();

        Box::pin(
            async move {
                // For looking up in the index
                let file_name = fs::db_file_path_with_name(msg.0)?;
                let k = rid::key_for_rel_db_attr(
                    file_name.as_path().to_str().ok_or(Error::MiscDecodeError)?,
                    msg.1,
                    msg.2,
                );

                // Lookup RID in the index then use that RID to look up in heap
                let rid = index_handle
                    .send(GetKey(k))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;
                let record = db_handle
                    .send(LoadRecord(rid))
                    .await
                    .map_err(|e| Error::MailboxError(e))??;

                // Decode the tuple into a catalogue entry, then return
                let tup = Tuple::decode_length_delimited(record.data.as_slice())
                    .map_err(|e| Error::DecodeError(e))?;
                tup.try_into()
            }
            .into_actor(self),
        )
    }
}

impl Handler<GetEntries> for Catalogue {
    type Result = ResponseActFuture<Self, Result<Vec<CatalogueEntry>, Error>>;

    #[tracing::instrument]
    fn handle(&mut self, msg: GetEntries, _ctx: &mut Context<Self>) -> Self::Result {
        let db_handle = self.db_handle.clone();

        Box::pin(
            async move {
                // Use the iterator interface to find matching values
                let iterator = db_handle
                    .send(Iter)
                    .await
                    .map_err(|e| Error::MailboxError(e))?;
                let mut next = iterator
                    .send(Next)
                    .await
                    .map_err(|e| Error::MailboxError(e))?;

                let mut results = Vec::new();

                loop {
                    // Once there are no more tuples, stop
                    let tup = if let Some(tup) = next {
                        tup
                    } else {
                        break;
                    };

                    tracing::debug!("got tuple in catalogue scan: {:?}", tup);

                    // Check if this value has the same table and database
                    let cat = CatalogueEntry::try_from(tup)?;

                    tracing::debug!("decoded tuple into: {:?}", cat);

                    if cat.table_name == msg.1
                        && fs::db_name_from_file_path(&cat.file_name)
                            .map(|dbname| msg.0 == dbname)
                            .unwrap_or_default()
                    {
                        results.push(cat);
                    }

                    next = iterator
                        .send(Next)
                        .await
                        .map_err(|e| Error::MailboxError(e))?;
                }

                tracing::debug!(
                    "got catalogue entries for table {} in db {}",
                    &msg.1,
                    &msg.0
                );

                Ok(results)
            }
            .into_actor(self),
        )
    }
}
