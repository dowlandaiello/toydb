// TODO: Make this less god awful

use super::{
    super::super::{
        error::Error,
        items::{BtreeInternalNode, BtreeLeafNode, Record, RecordId},
        util::fs,
    },
    GetKey, InsertKey,
};
use actix::{Actor, AsyncContext, Context, Handler, Message, ResponseFuture};
use futures::future::BoxFuture;
use std::{io::SeekFrom, mem, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, OwnedMutexGuard},
};

/// The number of children per node in the B+ tree
pub const ORDER: usize = 4;

/// Specifies whether the user is looking for a leaf node to insert into or a suitable parent node to insert a leaf node as a child of.
#[derive(PartialEq)]
enum SearchTarget {
    LeafNode,
    InternalNode,
}

enum SearchResult {
    LeafNode(LeafNode),
    InternalNode(InternalNode),
}

enum Node {
    InternalNode(BtreeInternalNode),
    LeafNode(BtreeLeafNode),
}

/// Determines the median key in the internal node.
fn median(n: Node) -> u64 {
    let keys = match n {
        Node::InternalNode(n) => n.keys.as_slice(),
        Node::LeafNode(n) => n.keys.as_slice(),
    };

    keys[keys.len() / 2]
}

fn left_values_internal(node: BtreeInternalNode) -> Vec<(u64, u64)> {
    let med = median(Node::InternalNode(node));

    node.keys
        .iter()
        .zip(node.child_pointers.iter())
        .filter(|(k, v)| k < &&med)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<(u64, u64)>>()
}

fn right_values_internal(node: BtreeInternalNode) -> Vec<(u64, u64)> {
    let med = median(Node::InternalNode(node));

    node.keys
        .iter()
        .zip(node.child_pointers.iter())
        .filter(|(k, _)| k >= &&med)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<(u64, u64)>>()
}

fn get_internal(node: BtreeInternalNode, k: u64) -> Option<u64> {
    node.keys
        .iter()
        .zip(node.child_pointers.iter())
        .filter(|(k_curr, _)| k_curr > &&k)
        .map(|(_, v)| v.clone())
        .next()
}

/// Retrieves a list of the values less than the median value in the node's keys.
fn left_values_leaf(node: BtreeLeafNode) -> Vec<(u64, RecordId)> {
    let med = median(Node::LeafNode(node));

    node.keys
        .iter()
        .zip(node.disk_pointers.iter())
        .filter(|(k, v)| k < &&med)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<(u64, RecordId)>>()
}

/// Retrieves a list of the values greater than or equal to the median value in the node's keys.
fn right_values(node: BtreeLeafNode) -> Vec<(u64, RecordId)> {
    let med = median(Node::LeafNode(node));

    node.keys
        .iter()
        .zip(node.disk_pointers.iter())
        .filter(|(k, v)| k >= &&med)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<(u64, RecordId)>>()
}

// Fetches the record at a specified key in the leaf node.
fn get_leaf(node: BtreeLeafNode, k: u64) -> Option<RecordId> {
    node.keys
        .iter()
        .zip(node.disk_pointers.iter())
        .find(|(k_curr, v)| k_curr == &&k)
        .map(|(_, v)| v.clone())
}

// TODO: CONTINUE MIGRATING BELOW HERE
// Traverse nodes until a key is found.
fn follow_node(
    mut handle: OwnedMutexGuard<File>,
    pos: u64,
    key: u64,
    target: SearchTarget,
) -> BoxFuture<'static, Result<(Option<SearchResult>, u64, OwnedMutexGuard<File>), Error>> {
    Box::pin(async move {
        let node_buff = {
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

                return Ok((Some(SearchResult::LeafNode(node_buff)), pos, handle));
            }

            // Get the internal node, and get the next position to go to
            let mut node_buff: InternalNode = InternalNode([0; INTERNAL_NODE_SIZE]);
            handle
                .read_exact(&mut node_buff.0)
                .await
                .map_err(|e| Error::IoError(e))?;

            node_buff
        };

        let next = if let Ok(n) = node_buff.get(key) {
            n
        } else if target == SearchTarget::LeafNode {
            return Ok((None, pos, handle));
        } else {
            return Ok((Some(SearchResult::InternalNode(node_buff)), pos, handle));
        };

        if target == SearchTarget::LeafNode {
            return follow_node(handle, next, key, target).await;
        }

        Ok((None, pos, handle))
    })
}

/// An abstraction representing a handle to a B+ tree index file for a database.
pub struct TreeHandle {
    handle: Arc<Mutex<File>>,
}

impl Actor for TreeHandle {
    type Context = Context<Self>;
}

impl Handler<GetKey> for TreeHandle {
    type Result = ResponseFuture<Result<RecordId, Error>>;

    fn handle(&mut self, msg: GetKey, _ctx: &mut Context<Self>) -> Self::Result {
        let handle = self.handle.clone();

        Box::pin(async move {
            let f = handle.lock_owned().await;
            let res = follow_node(f, 0, msg.0, SearchTarget::LeafNode).await?.0;

            if let Some(SearchResult::LeafNode(n)) = res {
                Ok(n.get(msg.0)?)
            } else {
                Err(Error::RecordNotFound)
            }
        })
    }
}

impl Handler<InsertKey> for TreeHandle {
    type Result = ResponseFuture<Result<(), Error>>;

    fn handle(&mut self, msg: InsertKey, ctx: &mut Context<Self>) -> Self::Result {
        let handle = self.handle.clone();
        let addr = ctx.address();

        Box::pin(async move {
            {
                let mut f = handle.lock_owned().await;

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

                // Find the leaf node that this belongs in that has space and insert
                let (candidate_leaf_node, pos, mut f) =
                    follow_node(f, 0, msg.0, SearchTarget::LeafNode).await?;

                if let SearchResult::LeafNode(mut n) =
                    candidate_leaf_node.ok_or(Error::RecordNotFound)?
                {
                    if !n.is_full() {
                        n.insert(msg.0, msg.1)?;

                        // Write the candidate node
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
                    let mut left = LeafNode::default();
                    let mut right = LeafNode::default();

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

                // An internal node has overflown. Need to split and then re-run insertion
                let (candidate_parent_node, pos, mut f) =
                    follow_node(f, 0, msg.0, SearchTarget::InternalNode).await?;

                if let SearchResult::InternalNode(n) =
                    candidate_parent_node.ok_or(Error::RecordNotFound)?
                {
                    // This shouldn't happen because it would indicate that there is a leaf node already there
                    // even if the leaf node is full
                    if !n.is_full() {
                        return Err(Error::TraversalError);
                    }

                    // Split the internal node up and then re-run the insertion
                    let left_vals = n.left_values()?;
                    let right_vals = n.right_values()?;
                    let median = n.median()?;

                    let mut new_parent = InternalNode::default();
                    let mut left = InternalNode::default();
                    let mut right = InternalNode::default();

                    // Copy all nodes less than median into left node
                    for (k, v) in left_vals {
                        left.insert(Some(k), v)?;
                    }

                    for (_k, v) in right_vals {
                        right.insert(None, v)?;
                    }

                    // Go to the end of the file to insert the new nodes
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
                }
            }

            addr.send(msg).await.map_err(|e| Error::MailboxError(e))?
        })
    }
}
