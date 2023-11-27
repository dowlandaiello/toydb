use super::super::{
    error::Error,
    types::db::{RecordId, RECORD_ID_SIZE},
};
use actix::{Actor, Context, Handler, Message, ResponseFuture};
use futures::future::BoxFuture;
use std::{io::SeekFrom, mem, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    sync::Mutex,
};

/// The number of children per node in the B+ tree
pub const ORDER: usize = 4;

/// The size of a node in the tree with children.
pub const INTERNAL_NODE_SIZE: usize =
    // Size of leaf flag:
    mem::size_of::<bool>() +
	// Size of keys:
	ORDER * mem::size_of::<u64>() +
		// Size of child pointers
		(ORDER + 1) * mem::size_of::<u64>();

/// The size of a node in the tree without children.
pub const LEAF_NODE_SIZE: usize =
    // Size of leaf flag:
    mem::size_of::<bool>() +
	// Size of keys:
	ORDER * mem::size_of::<u64>() +
		// Size of record pointers
		ORDER * RECORD_ID_SIZE;

/// A wrapp for a disk segment containing data in the format of an internal node.
pub struct InternalNode([u8; INTERNAL_NODE_SIZE]);

impl InternalNode {
    fn is_leaf_node(&self) -> bool {
        bool::from(self.0[0] != 0)
    }

    // Fetches the file pointer at a specified key in the internal node
    fn get(&self, key: u64) -> Result<u64, Error> {
        let mut rem = &self.0[1 + mem::size_of::<u64>()..];

        for i in 0..ORDER {
            let ptr_bytes: [u8; mem::size_of::<u64>()] = (&rem[0..mem::size_of::<u64>()])
                .try_into()
                .map_err(|_| Error::ConversionError)?;
            let key_bytes: [u8; mem::size_of::<u64>()] = (&rem
                [mem::size_of::<u64>()..2 * mem::size_of::<u64>()])
                .try_into()
                .map_err(|_| Error::ConversionError)?;

            let curr_key: u64 = u64::from_le_bytes(key_bytes);
            let ptr: u64 = u64::from_le_bytes(ptr_bytes);

            if curr_key > key {
                return Ok(ptr);
            }
        }

        Err(Error::RecordNotFound)
    }
}

/// A wrapper for a disk segment containing data in the format of a leaf node.
pub struct LeafNode([u8; LEAF_NODE_SIZE]);

impl LeafNode {
    fn is_leaf_node(&self) -> bool {
        bool::from(self.0[0] != 0)
    }

    // Fetches the record at a specified key in the leaf node.
    fn get(&self, key: u64) -> Result<RecordId, Error> {
        let mut rem = &self.0[1..];

        for i in 0..ORDER {
            let key_bytes: [u8; mem::size_of::<u64>()] = (&rem[0..mem::size_of::<u64>()])
                .try_into()
                .map_err(|_| Error::ConversionError)?;
            let val_bytes: [u8; RECORD_ID_SIZE] = (&rem
                [mem::size_of::<u64>()..(mem::size_of::<u64>() + RECORD_ID_SIZE)])
                .try_into()
                .map_err(|_| Error::ConversionError)?;

            let curr_key: u64 = u64::from_le_bytes(key_bytes);
            let val: RecordId = RecordId::from(val_bytes);

            // Check if the key is the key we're looking for
            if key == curr_key {
                return Ok(val);
            }

            rem = &rem[mem::size_of::<u64>() + RECORD_ID_SIZE..];
        }

        Err(Error::RecordNotFound)
    }
}

/// Gets a record pointer from a key.
#[derive(Message)]
#[rtype(result = "Result<RecordId, Error>")]
pub struct GetKey(u64);

/// An abstraction representing a handle to a B+ tree index file for a database.
pub struct IndexHandle {
    handle: Arc<Mutex<File>>,
}

// Traverse nodes until a key is found.
fn follow_node(
    handle: Arc<Mutex<File>>,
    pos: u64,
    key: u64,
) -> BoxFuture<'static, Result<RecordId, Error>> {
    Box::pin(async move {
        let node_buff = {
            let mut handle = handle.lock().await;

            // Get the current node
            handle
                .seek(SeekFrom::Current(pos as i64))
                .await
                .map_err(|e| Error::IoError(e))?;

            let mut is_leaf_flag_buff: [u8; mem::size_of::<bool>()] = [0; mem::size_of::<bool>()];
            handle
                .read_exact(&mut is_leaf_flag_buff)
                .await
                .map_err(|e| Error::IoError(e))?;

            // If the current node is a leaf, load the node and return the record pointer
            if is_leaf_flag_buff[0] != 0 {
                let mut node_buff: LeafNode = LeafNode([0; LEAF_NODE_SIZE]);
                handle
                    .read_exact(&mut node_buff.0)
                    .await
                    .map_err(|e| Error::IoError(e))?;

                // Get the record pointer
                return node_buff.get(key);
            }

            // Get the internal node, and get the next position to go to
            let mut node_buff: InternalNode = InternalNode([0; INTERNAL_NODE_SIZE]);
            handle
                .read_exact(&mut node_buff.0)
                .await
                .map_err(|e| Error::IoError(e))?;

            node_buff
        };

        follow_node(handle, node_buff.get(key)?, key).await
    })
}

impl Actor for IndexHandle {
    type Context = Context<Self>;
}

impl Handler<GetKey> for IndexHandle {
    type Result = ResponseFuture<Result<RecordId, Error>>;

    fn handle(&mut self, msg: GetKey, _ctx: &mut Context<Self>) -> Self::Result {
        let handle = self.handle.clone();

        Box::pin(async move { follow_node(handle, 0, msg.0).await })
    }
}
