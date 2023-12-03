use super::{
    super::{
        error::Error,
        items::{Record, Tuple},
        types::table::CatalogueEntry,
        util::rid,
    },
    heap::{HeapHandle, InsertRecord},
    index::{IndexHandle, InsertKey},
};
use actix::{Actor, Addr, Context, Handler, Message, ResponseActFuture, WrapFuture};
use prost::Message as ProstMessage;

/// Inserts the catalogue entry into the catalogue.
#[derive(Message, Debug)]
#[rtype(result = "Result<(), Error>")]
pub struct InsertEntry(pub CatalogueEntry);

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
        let db_handle = self.db_handle.clone();
        let index_db_rel_name_handle = self.index_db_rel_name_handle.clone();

        Box::pin(
            async move {
                // For inserting into the index
                let k = rid::key_for_rel_db(msg.0.file_name.as_str(), msg.0.table_name.as_str());

                let tuple: Tuple = msg.0.into();
                let enc = tuple.encode_length_delimited_to_vec();

                // Add the entry to the page
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
                    .map_err(|e| Error::MailboxError(e))?
            }
            .into_actor(self),
        )
    }
}
