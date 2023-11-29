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

/// Specifies whether the user is looking for a leaf node to insert into or a suitable parent node to insert a leaf node as a child of.
#[derive(PartialEq)]
pub enum SearchTarget {
    LeafNode,
    InternalNode,
}

pub enum SearchResult {
    LeafNode(LeafNode),
    InternalNode(InternalNode),
}

/// A wrapp for a disk segment containing data in the format of an internal node.
pub struct InternalNode([u8; INTERNAL_NODE_SIZE]);

impl Default for InternalNode {
    fn default() -> Self {
        Self([0; INTERNAL_NODE_SIZE])
    }
}

impl InternalNode {
    // Inserts a pointer in the internal node
    fn insert(&mut self, k: Option<u64>, v: u64) -> Result<(), Error> {
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

        let ptr_bytes: [u8; mem::size_of::<u64>()] = v.to_le_bytes();

        for i in 0..ptr_bytes.len() {
            rem[i + abs_idx] = ptr_bytes[i];
        }

        if let Some(key) = k {
            let key_bytes: [u8; mem::size_of::<u64>()] = k.to_le_bytes();
            for i in 0..key_bytes.len() {
                rem[i + abs_idx + ptr_bytes.len()] = key_bytes[i];
            }
        }

        Ok(())
    }

    // Fetches the file pointer at a specified key in the internal node
    fn get(&self, key: u64) -> Result<u64, Error> {
        let mut rem = &self.0[1..];

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

            if curr_key > key && ptr != 0 {
                return Ok(ptr);
            }

            // The last key also has a pointer space to the right of it
            if i == ORDER - 1 && curr_key < key {
                let last_ptr_bytes: [u8; mem::size_of::<u64>()] = (&rem
                    [(2 * mem::size_of::<u64>())..(3 * mem::size_of::<u64>())])
                    .try_into()
                    .map_err(|_| Error::ConversionError)?;
                let last_ptr: u64 = u64::from_le_bytes(last_ptr_bytes);

                // The space is occupied
                if last_ptr != 0 {
                    return Ok(last_ptr);
                }
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
    /// Determines the median key in the leaf node.
    fn median(&self) -> Result<u64, Error> {
        let mut rem = &self.0[1 + (2 * mem::size_of::<u64>())..];
        let mut median_key = 0;

        for _ in 0..(ORDER / 2) {
            let key_bytes: [u8; mem::size_of::<u64>()] = (&rem[0..mem::size_of::<u64>()])
                .try_into()
                .map_err(|_| Error::ConversionError)?;

            median_key = u64::from_le_bytes(key_bytes);
            rem = &rem[mem::size_of::<u64>() + RECORD_ID_SIZE..];
        }

        Ok(median_key)
    }

    /// Retrieves a list of the values less than the median value in the node's keys.
    fn left_values(&self) -> Result<Vec<(u64, RecordId)>, Error> {
        let mut rem = &self.0[1 + (2 * mem::size_of::<u64>())..];
        let mut vals = Vec::new();
        let med = self.median()?;

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

            if curr_key == med {
                break;
            }

            // This value is less than the median
            vals.push((curr_key, val));

            rem = &rem[mem::size_of::<u64>() + RECORD_ID_SIZE..];
        }

        Ok(vals)
    }

    /// Retrieves a list of the values less than the median value in the node's keys.
    fn right_values(&self) -> Result<Vec<(u64, RecordId)>, Error> {
        let mut rem = &self.0[1 + (2 * mem::size_of::<u64>())..];
        let mut vals = Vec::new();
        let med = self.median()?;

        // Seek to the part of children that begins with the key
        for _ in 0..ORDER {
            let key_bytes: [u8; mem::size_of::<u64>()] = (&rem[0..mem::size_of::<u64>()])
                .try_into()
                .map_err(|_| Error::ConversionError)?;
            let curr_key: u64 = u64::from_le_bytes(key_bytes);
            let val_bytes: [u8; RECORD_ID_SIZE] = (&rem
                [mem::size_of::<u64>()..(mem::size_of::<u64>() + RECORD_ID_SIZE)])
                .try_into()
                .map_err(|_| Error::ConversionError)?;
            let val: RecordId = RecordId::from(val_bytes);

            if curr_key >= med {
                vals.push((curr_key, val));
            }

            rem = &rem[mem::size_of::<u64>() + RECORD_ID_SIZE..];
        }

        Ok(vals)
    }

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
    fn insert(&mut self, key: u64, val: RecordId) -> Result<(), Error> {
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

        let key_bytes: [u8; mem::size_of::<u64>()] = key.to_le_bytes();
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
    target: SearchTarget,
) -> BoxFuture<'static, Result<(SearchResult, u64), Error>> {
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

            // If the current node is a leaf AND we are looking for a leaf load the node and return the record pointer
            if is_leaf_flag_buff[0] != 0 && target == SearchTarget::LeafNode {
                let mut node_buff: LeafNode = LeafNode([0; LEAF_NODE_SIZE]);
                handle
                    .read_exact(&mut node_buff.0)
                    .await
                    .map_err(|e| Error::IoError(e))?;

                return Ok((SearchResult::LeafNode(node_buff), pos));
            }

            // Get the internal node, and get the next position to go to
            let mut node_buff: InternalNode = InternalNode([0; INTERNAL_NODE_SIZE]);
            handle
                .read_exact(&mut node_buff.0)
                .await
                .map_err(|e| Error::IoError(e))?;

            node_buff
        };

        if target == SearchTarget::LeafNode {
            follow_node(handle, node_buff.get(key)?, key, target).await
        } else {
            let res = follow_node(handle, node_buff.get(key)?, key, target).await;
            Ok(res.unwrap_or((SearchResult::InternalNode(node_buff), pos)))
        }
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

        Box::pin(async move {
            let res = follow_node(handle, 0, msg.0, SearchTarget::LeafNode)
                .await?
                .0;

            if let SearchResult::LeafNode(n) = res {
                Ok(n.get(msg.0)?)
            } else {
                Err(Error::RecordNotFound)
            }
        })
    }
}

impl Handler<InsertKey> for IndexHandle {
    type Result = ResponseFuture<Result<(), Error>>;

    fn handle(&mut self, msg: InsertKey, _ctx: &mut Context<Self>) -> Self::Result {
        let handle = self.handle.clone();

        Box::pin(async move {
            {
                let mut f = handle.lock().await;

                // If the file length is zero, this is the root node. Insert it
                if fs::file_is_empty(&f).await {
                    let mut root_node = InternalNode::default();
                    root_node.insert(Some(msg.0), INTERNAL_NODE_SIZE as u64)?;

                    let mut leaf_node = LeafNode::default();
                    leaf_node.insert(msg.0, msg.1.clone())?;

                    // Write the two blocks to the file
                    f.write_all(root_node.0.as_slice())
                        .await
                        .map_err(|e| Error::IoError(e))?;
                    f.write_all(leaf_node.0.as_slice())
                        .await
                        .map_err(|e| Error::IoError(e))?;

                    return Ok(());
                }
            }

            // Find the leaf node that this belongs in that has space and insert
            let (candidate_leaf_node, pos) =
                follow_node(handle.clone(), 0, msg.0, SearchTarget::LeafNode).await?;

            if let SearchResult::LeafNode(mut n) = candidate_leaf_node {
                if !n.is_full() {
                    n.insert(msg.0, msg.1)?;

                    // Write the candidate node
                    let mut f = handle.lock().await;

                    f.seek(SeekFrom::Start(pos))
                        .await
                        .map_err(|e| Error::IoError(e))?;
                    f.write_all(n.0.as_slice())
                        .await
                        .map_err(|e| Error::IoError(e))?;

                    return Ok(());
                }

                // The leaf node is full. Split it by creating an internal node
                // with a value of the halfway point of the leaf node and inserting
                // a leaf node with half of the values to the left of it
                // and a leaf node with the other half of the values to the right of it
                let left_vals = n.left_values()?;
                let right_vals = n.right_values()?;
                let median = n.median()?;

                let mut new_parent = InternalNode::default();
                let left = LeafNode::default();
                let right = LeafNode::default();

                // Copy all nodes less than median into left node
                for (k, v) in left_vals {
                    left.insert(k, v)?;
                }

                for (k, v) in right_vals {
                    right.insert(k, v)?;
                }

                if msg.0 < median {
                    left.insert(msg.0, msg.1)?;
                } else {
                    right.insert(msg.0, msg.1)?;
                }

                // Go to the end of the file to insert the new nodes
                let mut f = handle.lock().await;
                let new_nodes_pos = f
                    .seek(SeekFrom::End(0))
                    .await
                    .map_err(|e| Error::IoError(e))?;
                f.write_all(left.0.as_slice())
                    .await
                    .map_err(|e| Error::IoError(e))?;
                f.write_all(right.0.as_slice())
                    .await
                    .map_err(|e| Error::IoError(e))?;

                // Go back to the old node and overwrite it with the new nodes inserted
                new_parent.insert(Some(median), new_nodes_pos)?;
                new_parent.insert(None, (new_nodes_pos as usize + LEAF_NODE_SIZE) as u64)?;

                f.seek(SeekFrom::Start(pos))
                    .await
                    .map_err(|e| Error::IoError(e))?;
                f.write_all(new_parent.0.as_slice())
                    .await
                    .map_err(|e| Error::IoError(e))?;

                return Ok(());
            }

            todo!()
        })
    }
}
