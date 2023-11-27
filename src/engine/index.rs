use super::super::{
    error::Error,
    types::db::{RecordId, RECORD_ID_SIZE},
    util::fs,
};
use actix::{Actor, Context, Handler, Message, ResponseFuture};
use futures::future::BoxFuture;
use std::{io::SeekFrom, mem, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
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
	// Size of pointer to previous node
	mem::size_of::<u64>() +
	// Size of pointer to next node
	mem::size_of::<u64>() +
	// Size of keys:
	ORDER * mem::size_of::<u64>() +
		// Size of record pointers
		ORDER * RECORD_ID_SIZE;

/// A wrapp for a disk segment containing data in the format of an internal node.
pub struct InternalNode([u8; INTERNAL_NODE_SIZE]);

impl Default for InternalNode {
    fn default() -> Self {
        Self([0; INTERNAL_NODE_SIZE])
    }
}

impl InternalNode {
    // Inserts a pointer in the internal node
    fn insert(&mut self, k: u64, v: u64) -> Result<(), Error> {
        let mut rem = &self.0[1 + mem::size_of::<u64>()..];
        let mut insert_idx = 0;

        // Find a free spot in the list of nodes
        for i in 0..ORDER {
            let key_bytes: [u8; mem::size_of::<u64>()] = (&rem[0..mem::size_of::<u64>()])
                .try_into()
                .map_err(|_| Error::ConversionError)?;

            if key_bytes.iter().all(|x| x == &0) {
                insert_idx = i;
                break;
            }

            rem = &rem[mem::size_of::<u64>() * 2..];
        }

        // Insert the pointer followed by the key (left biased)
        let rem = &mut self.0[1..];
        let abs_idx = insert_idx * (mem::size_of::<u64>() * 2);

        let key_bytes: [u8; mem::size_of::<u64>()] = k.to_le_bytes();
        let ptr_bytes: [u8; mem::size_of::<u64>()] = v.to_le_bytes();

        for i in 0..ptr_bytes.len() {
            rem[i + abs_idx] = ptr_bytes[i];
        }

        for i in 0..key_bytes.len() {
            rem[i + abs_idx + ptr_bytes.len()] = key_bytes[i];
        }

        Ok(())
    }

    // Fetches the file pointer at a specified key in the internal node
    fn get(&self, key: u64) -> Result<u64, Error> {
        let mut rem = &self.0[1..];

        for _ in 0..ORDER {
            let ptr_bytes: [u8; mem::size_of::<u64>()] = (&rem[0..mem::size_of::<u64>()])
                .try_into()
                .map_err(|_| Error::ConversionError)?;
            let key_bytes: [u8; mem::size_of::<u64>()] = (&rem
                [mem::size_of::<u64>()..2 * mem::size_of::<u64>()])
                .try_into()
                .map_err(|_| Error::ConversionError)?;

            let curr_key: u64 = u64::from_le_bytes(key_bytes);
            let ptr: u64 = u64::from_le_bytes(ptr_bytes);

            if curr_key >= key && ptr != 0 {
                return Ok(ptr);
            }

            rem = &rem[mem::size_of::<u64>() * 2..];
        }

        Err(Error::RecordNotFound)
    }
}

/// A wrapper for a disk segment containing data in the format of a leaf node.
pub struct LeafNode([u8; LEAF_NODE_SIZE]);

impl Default for LeafNode {
    fn default() -> Self {
        let mut buff = [0; LEAF_NODE_SIZE];
        buff[0] = 1;

        LeafNode(buff)
    }
}

impl LeafNode {
    /// Returns whether or not there are any slots left in the leaf node
    fn is_full(&self) -> bool {
        fn is_full_helper(slf: &LeafNode) -> Option<bool> {
            let mut rem = &slf.0[1 + (2 * mem::size_of::<u64>())..];

            for _ in 0..ORDER {
                // Look for a key-value pair that is zero that is not in the zeroth position
                let key_bytes: [u8; mem::size_of::<u64>()] =
                    (&rem[0..mem::size_of::<u64>()]).try_into().ok()?;
                let val_bytes: [u8; RECORD_ID_SIZE] = (&rem
                    [mem::size_of::<u64>()..(mem::size_of::<u64>() + RECORD_ID_SIZE)])
                    .try_into()
                    .ok()?;

                // The position is empty
                if key_bytes.iter().all(|x| x == &0) && val_bytes.iter().all(|x| x == &0) {
                    return Some(false);
                }

                rem = &rem[mem::size_of::<u64>() + RECORD_ID_SIZE..];
            }

            Some(true)
        }

        is_full_helper(self).unwrap_or(true)
    }

    /// Inserts a record at a specified key in the leaf node.
    /// Replaces the last record in the leaf node if no space is available (this should not happen because you
    /// should already be rebalancing before running this).
    fn insert(&self, key: u64, val: RecordId) -> Result<(), Error> {
        let mut rem = &self.0[1 + (2 * mem::size_of::<u64>())..];
        let mut insert_idx = 0;

        for i in 0..ORDER {
            // Look for a key-value pair that is zero that is not in the zeroth position
            let key_bytes: [u8; mem::size_of::<u64>()] = (&rem[0..mem::size_of::<u64>()])
                .try_into()
                .map_err(|_| Error::ConversionError)?;
            let val_bytes: [u8; RECORD_ID_SIZE] = (&rem
                [mem::size_of::<u64>()..(mem::size_of::<u64>() + RECORD_ID_SIZE)])
                .try_into()
                .map_err(|_| Error::ConversionError)?;

            // The position is empty
            if key_bytes.iter().all(|x| x == &0) && val_bytes.iter().all(|x| x == &0) {
                insert_idx = i;
            }

            rem = &rem[mem::size_of::<u64>() + RECORD_ID_SIZE..];
        }

        // Insert the key followed by the value
        let rem = &mut self.0[1 + (2 * mem::size_of::<u64>())..];
        let abs_idx = insert_idx * (mem::size_of::<u64>() + RECORD_ID_SIZE);

        let key_bytes: [u8; mem::size_of::<u64>()] = k.to_le_bytes();
        let record_bytes: [u8; RECORD_ID_SIZE] = val.into();

        for i in 0..key_bytes.len() {
            rem[i + abs_idx] = key_bytes[i];
        }

        for i in 0..record_bytes.len() {
            rem[i + abs_idx + key_bytes.len()] = record_bytes[i];
        }

        Err(Error::RecordNotFound)
    }

    // Fetches the record at a specified key in the leaf node.
    fn get(&self, key: u64) -> Result<RecordId, Error> {
        let mut rem = &self.0[1 + (2 * mem::size_of::<u64>())..];

        for _ in 0..ORDER {
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

// Traverse nodes until a key is found.
fn follow_node(
    handle: Arc<Mutex<File>>,
    pos: u64,
    key: u64,
) -> BoxFuture<'static, Result<LeafNode, Error>> {
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
                return Ok(node_buff);
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

/// Gets a record pointer from a key.
#[derive(Message)]
#[rtype(result = "Result<RecordId, Error>")]
pub struct GetKey(u64);

/// Inserts a record pointer at a key.
#[derive(Message)]
#[rtype(result = "Result<(), Error>")]
pub struct InsertKey(u64, RecordId);

/// An abstraction representing a handle to a B+ tree index file for a database.
pub struct IndexHandle {
    handle: Arc<Mutex<File>>,
}

impl Actor for IndexHandle {
    type Context = Context<Self>;
}

impl Handler<GetKey> for IndexHandle {
    type Result = ResponseFuture<Result<RecordId, Error>>;

    fn handle(&mut self, msg: GetKey, _ctx: &mut Context<Self>) -> Self::Result {
        let handle = self.handle.clone();

        Box::pin(async move { Ok(follow_node(handle, 0, msg.0).await?.get(msg.0)?) })
    }
}

impl Handler<InsertKey> for IndexHandle {
    type Result = ResponseFuture<Result<(), Error>>;

    fn handle(&mut self, msg: InsertKey, _ctx: &mut Context<Self>) -> Self::Result {
        Box::pin(async move {
            {
                let f = self.handle.lock().await;

                // If the file length is zero, this is the root node. Insert it
                if fs::file_is_empty(&f).await {
                    let mut root_node = InternalNode::default();
                    root_node.insert(msg.0, INTERNAL_NODE_SIZE as u64);

                    let mut leaf_node = LeafNode::default();
                    leaf_node.insert(msg.0, msg.1);

                    // Write the two blocks to the file
                    f.write_all(root_node.0.as_slice())
                        .await
                        .map_err(|e| Error::IoError(e))?;
                    f.write_all(leaf_node.0.as_slice())
                        .await
                        .map_err(|e| Error::IoError(e))?;
                }
            }

            // Find the parent node that this belongs under that has space and insert
            let mut candidate_leaf_node = follow_node(self.handle, 0, msg.0).await?;
            if !candidate_leaf_node.is_full() {
                candidate_leaf_node.insert(msg.0, msg.1)?;
            }

            // Need to split the parent node
            todo!()
        })
    }
}
