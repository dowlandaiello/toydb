use super::{
    super::{
        super::{
            error::Error,
            items::{BTreeInternalNode, BTreeLeafNode, Page, RecordId, RecordIdPointer, Tuple},
            util::fs,
        },
        buffer_pool::{DbHandle, LoadPage},
        iterator::Next,
    },
    GetKey, InsertKey,
};
use actix::{
    Actor, Addr, AsyncContext, Context, Handler, Message, ResponseActFuture, ResponseFuture,
    WrapFuture,
};
use futures::future::BoxFuture;
use prost::Message as ProstMessage;
use std::{collections::HashMap, io::SeekFrom, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, OwnedMutexGuard},
};

/// The number of children per node in the B+ tree
pub const ORDER: usize = 4;

/// A message requesting that an actor create a new iterator.
#[derive(Message)]
#[rtype(result = "Result<Addr<TreeHandleIterator>, Error>")]
pub struct Iter(Addr<DbHandle>);

/// Gets the next leaf node from the seeker identified by the ID.
#[derive(Message)]
#[rtype(result = "Result<BTreeLeafNode, Error>")]
struct NextLeaf(Addr<TreeHandleIterator>);

/// Specifies whether the user is looking for a leaf node to insert into or a suitable parent node to insert a leaf node as a child of.
#[derive(PartialEq)]
enum SearchTarget {
    LeafNode,
    InternalNode,
}

/// Specifies whether we are looking for values greater than or less than the key.
#[derive(Debug, PartialEq)]
enum SearchKey {
    Lt(u64),
    Gt(u64),
}

#[derive(Debug)]
enum SearchResult {
    LeafNode(BTreeLeafNode),
    InternalNode(BTreeInternalNode),
}

enum Node<'a> {
    InternalNode(&'a BTreeInternalNode),
    LeafNode(&'a BTreeLeafNode),
}

impl From<RecordId> for RecordIdPointer {
    fn from(rid: RecordId) -> Self {
        Self {
            is_empty: false,
            page: rid.page,
            page_idx: rid.page_idx,
        }
    }
}

impl From<RecordIdPointer> for RecordId {
    fn from(ridp: RecordIdPointer) -> Self {
        Self {
            page: ridp.page,
            page_idx: ridp.page_idx,
        }
    }
}

fn create_internal_node() -> BTreeInternalNode {
    let mut node = BTreeInternalNode::default();
    node.keys_pointers = vec![0; ORDER * 2 + 1];

    node
}

fn create_leaf_node() -> BTreeLeafNode {
    let mut node = BTreeLeafNode::default();
    node.is_leaf_node = true;
    node.keys = vec![0; ORDER];
    node.disk_pointers = vec![
        {
            let mut r = RecordIdPointer::default();
            r.is_empty = true;
            r
        };
        ORDER
    ];

    node
}

fn len_internal_keys(node: &BTreeInternalNode) -> usize {
    node.keys_pointers
        .iter()
        .enumerate()
        .filter(|(i, x)| i % 2 != 0 && x != &&0)
        .map(|(_, x)| x)
        .collect::<Vec<&u64>>()
        .len()
}

fn len_leaf_node_keys(node: &BTreeLeafNode) -> usize {
    node.keys
        .iter()
        .filter(|x| x != &&0)
        .collect::<Vec<&u64>>()
        .len()
}

fn len_internal_child_pointers(node: &BTreeInternalNode) -> usize {
    node.keys_pointers
        .iter()
        .enumerate()
        .filter(|(i, x)| i % 2 == 0 && x != &&0)
        .map(|(_, x)| x)
        .collect::<Vec<&u64>>()
        .len()
}

fn len_leaf_disk_pointers(node: &BTreeLeafNode) -> usize {
    node.disk_pointers
        .iter()
        .filter(|x| !x.is_empty)
        .collect::<Vec<&RecordIdPointer>>()
        .len()
}

/// Inserts the key and value into the btree node, if space exists.
fn insert_internal(node: &mut BTreeInternalNode, k: SearchKey, v: u64) -> Result<(), Error> {
    if let SearchKey::Lt(0) = k {
        return Err(Error::InvalidKey);
    }

    if let SearchKey::Gt(0) = k {
        return Err(Error::InvalidKey);
    }

    let len_keys = len_internal_keys(node);
    let len_children = len_internal_child_pointers(node);

    if len_children > ORDER {
        return Err(Error::TraversalError);
    };

    let mut pos_insert = None;

    for (i, key) in node.keys_pointers.iter().enumerate() {
        if i % 2 == 0 {
            continue;
        }

        match k {
            SearchKey::Lt(x) => {
                if x == key.clone() {
                    pos_insert = Some(i - 1);

                    break;
                }
            }
            SearchKey::Gt(x) => {
                if x == key.clone() {
                    pos_insert = Some(i + 1);

                    break;
                }
            }
        }
    }

    match pos_insert {
        Some(pos_insert) => {
            node.keys_pointers[pos_insert] = v;

            Ok(())
        }
        None => {
            if len_keys + len_children == node.keys_pointers.len() - 2 {
                // Insert the key in Gt position and then insert the value
                let k = if let SearchKey::Gt(k) = k {
                    k
                } else {
                    return Err(Error::TraversalError);
                };

                node.keys_pointers[len_keys + len_children] = k;
                node.keys_pointers[len_keys + len_children + 1] = v;

                Ok(())
            } else if len_keys + len_children < node.keys_pointers.len() - 2 {
                // Insert in Lt pos
                let k = if let SearchKey::Lt(k) = k {
                    k
                } else {
                    return Err(Error::TraversalError);
                };

                node.keys_pointers[len_keys + len_children + 1] = k;
                node.keys_pointers[len_keys + len_children] = v;

                Ok(())
            } else {
                Err(Error::TraversalError)
            }
        }
    }
}

/// Inserts the  key and record pointer into the leaf, if space exists.
fn insert_leaf(node: &mut BTreeLeafNode, k: u64, v: RecordIdPointer) -> Result<(), Error> {
    if k == 0 {
        return Err(Error::InvalidKey);
    }

    let len_keys = len_leaf_node_keys(node);
    let len_children = len_leaf_disk_pointers(node);

    if len_children >= ORDER {
        return Err(Error::TraversalError);
    }

    node.keys[len_keys] = k;
    node.disk_pointers[len_children] = v;

    Ok(())
}

/// Determines the median key in the internal node.
fn median(n: Node) -> u64 {
    match n {
        Node::InternalNode(n) => {
            let keys = n
                .keys_pointers
                .iter()
                .enumerate()
                .filter(|(i, _)| i % 2 != 0)
                .map(|(_, x)| x.clone())
                .collect::<Vec<u64>>();
            keys[keys.len() / 2]
        }
        Node::LeafNode(n) => n.keys.as_slice()[n.keys.len() / 2],
    }
}

fn left_values_internal(node: &BTreeInternalNode) -> Vec<(u64, u64)> {
    let med = median(Node::InternalNode(node));

    let mut vals = Vec::new();
    let mut k_v = Vec::new();

    for (i, key) in node.keys_pointers.iter().enumerate() {
        if i % 2 == 0 {
            continue;
        }

        if *key >= med {
            break;
        }

        vals.push((i, i - 1));
    }

    for (k_i, v_i) in vals {
        k_v.push((node.keys_pointers[k_i], node.keys_pointers[v_i]));
    }

    k_v
}

fn right_values_internal(node: &BTreeInternalNode) -> Vec<(SearchKey, u64)> {
    let med = median(Node::InternalNode(node));

    let mut vals = Vec::new();
    let mut k_v = Vec::new();

    for (i, key) in node.keys_pointers.iter().enumerate() {
        if i % 2 == 0 {
            continue;
        }

        if *key < med {
            continue;
        }

        vals.push((i, i - 1));
    }

    for (k_i, v_i) in &vals {
        k_v.push((
            SearchKey::Lt(node.keys_pointers[k_i.clone()]),
            node.keys_pointers[v_i.clone()],
        ));
    }

    // Include last straggle node
    k_v.push((
        SearchKey::Gt(node.keys_pointers[vals[vals.len() - 1].0]),
        node.keys_pointers[node.keys_pointers.len() - 1],
    ));

    k_v
}

fn get_internal(node: &BTreeInternalNode, k: SearchKey) -> Option<u64> {
    let mut pos = None;

    for (i, key) in node.keys_pointers.iter().enumerate() {
        if i % 2 == 0 {
            continue;
        }

        match k {
            SearchKey::Gt(x) => {
                if x == key.clone() {
                    pos = Some(i + 1);
                }
            }
            SearchKey::Lt(x) => {
                if x == key.clone() {
                    pos = Some(i - 1);
                }
            }
        }
    }

    pos.map(|pos| node.keys_pointers[pos])
}

/// Retrieves a list of the values less than the median value in the node's keys.
fn left_values_leaf(node: &BTreeLeafNode) -> Vec<(u64, RecordIdPointer)> {
    let med = median(Node::LeafNode(node));

    node.keys
        .iter()
        .zip(node.disk_pointers.iter())
        .filter(|(k, _v)| k < &&med)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<(u64, RecordIdPointer)>>()
}

/// Retrieves a list of the values greater than or equal to the median value in the node's keys.
fn right_values_leaf(node: &BTreeLeafNode) -> Vec<(u64, RecordIdPointer)> {
    let med = median(Node::LeafNode(node));

    node.keys
        .iter()
        .zip(node.disk_pointers.iter())
        .filter(|(k, _v)| k >= &&med)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<(u64, RecordIdPointer)>>()
}

// Fetches the record at a specified key in the leaf node.
fn get_leaf(node: BTreeLeafNode, k: u64) -> Option<RecordIdPointer> {
    node.keys
        .iter()
        .zip(node.disk_pointers.iter())
        .find(|(k_curr, _v)| k_curr == &&k)
        .map(|(_, v)| v.clone())
}

// Traverse nodes until a key is found.
fn follow_node(
    mut handle: OwnedMutexGuard<File>,
    pos: u64,
    key: u64,
    target: SearchTarget,
) -> BoxFuture<'static, Result<(Option<SearchResult>, u64, OwnedMutexGuard<File>), Error>> {
    Box::pin(async move {
        let node = {
            // Get the current node
            handle
                .seek(SeekFrom::Start(pos as u64))
                .await
                .map_err(|e| Error::IoError(e))?;

            // Peek 10 bytes for the length prefix
            let mut length_delim_buff: [u8; 10] = [0; 10];
            handle
                .read_exact(&mut length_delim_buff)
                .await
                .map_err(|e| Error::IoError(e))?;
            let length_delim = prost::decode_length_delimiter(&mut length_delim_buff.as_slice())
                .map_err(|e| Error::DecodeError(e))?;
            let delim_len = prost::length_delimiter_len(length_delim);

            // Make a buffer for the size of the node
            let mut node_buff: Vec<u8> = vec![0; length_delim + delim_len];

            // Read into the buff
            handle
                .seek(SeekFrom::Start(pos as u64))
                .await
                .map_err(|e| Error::IoError(e))?;
            handle
                .read_exact(&mut node_buff)
                .await
                .map_err(|e| Error::IoError(e))?;

            // Read a node from the buff
            let node = BTreeInternalNode::decode_length_delimited(node_buff.as_slice())
                .map_err(|e| Error::DecodeError(e))?;

            println!("{} {}", pos, node.is_leaf_node);

            // If the current node is a leaf AND we are looking for a leaf load the node and return the record pointer
            if node.is_leaf_node && target == SearchTarget::LeafNode {
                tracing::debug!("found candidate leaf node: {:?}", node);

                let node = BTreeLeafNode::decode_length_delimited(node_buff.as_slice())
                    .map_err(|e| Error::DecodeError(e))?;
                return Ok((Some(SearchResult::LeafNode(node)), pos, handle));
            }

            node
        };

        tracing::debug!(
            "candidate node {:?} is not leaf node, or we are not looking for a leaf node",
            node
        );

        let next = if let Some(n) = get_internal(&node, SearchKey::Lt(key)) {
            tracing::debug!("found next node to probe for key {}: {:?}", key, n);

            n
        } else if target == SearchTarget::LeafNode {
            tracing::debug!(
                "we were looking for a next node to probe, but no leaf nodes are available"
            );

            return Ok((None, pos, handle));
        } else {
            tracing::debug!(
                "no next node is found, and this is an internal node: {:?}",
                node
            );

            return Ok((Some(SearchResult::InternalNode(node)), pos, handle));
        };

        if target == SearchTarget::LeafNode {
            tracing::debug!("continue probing for leaf node");

            return follow_node(handle, next, key, target).await;
        }

        tracing::debug!("no candidate node found. giving up");

        Ok((None, pos, handle))
    })
}

/// An abstraction representing a handle to a B+ tree index file for a database.
#[derive(Debug)]
pub struct TreeHandle {
    pub handle: Arc<Mutex<File>>,
    pub seekers: Arc<Mutex<HashMap<Addr<TreeHandleIterator>, Vec<BTreeLeafNode>>>>,
}

impl TreeHandle {
    /// Constructs a plan of leaf nodes to iterate through
    fn collect_nodes(
        mut handle: OwnedMutexGuard<File>,
        curr_pos: u64,
    ) -> BoxFuture<
        'static,
        Result<(Vec<BTreeLeafNode>, OwnedMutexGuard<File>), (Error, OwnedMutexGuard<File>)>,
    > {
        Box::pin(async move {
            if let Err(e) = handle.seek(SeekFrom::Start(curr_pos)).await {
                return Err((Error::IoError(e), handle));
            }

            // Peek 10 bytes for the length prefix
            let mut length_delim_buff: [u8; 10] = [0; 10];
            if let Err(e) = handle.read_exact(&mut length_delim_buff).await {
                return Err((Error::IoError(e), handle));
            }
            let length_delim =
                match prost::decode_length_delimiter(&mut length_delim_buff.as_slice()) {
                    Ok(v) => v,
                    Err(e) => {
                        return Err((Error::DecodeError(e), handle));
                    }
                };
            let length_delim_len = prost::length_delimiter_len(length_delim);

            // Make a buffer for the size of the node
            let mut node_buff: Vec<u8> = vec![0; length_delim + length_delim_len];

            // Read into the buff
            if let Err(e) = handle.seek(SeekFrom::Start(curr_pos as u64)).await {
                return Err((Error::IoError(e), handle));
            }
            if let Err(e) = handle.read_exact(&mut node_buff).await {
                return Err((Error::IoError(e), handle));
            }

            // Read a node from the buff
            let node = match BTreeInternalNode::decode_length_delimited(node_buff.as_slice()) {
                Ok(v) => v,
                Err(e) => {
                    return Err((Error::DecodeError(e), handle));
                }
            };
            if node.is_leaf_node {
                let node = match BTreeLeafNode::decode_length_delimited(node_buff.as_slice()) {
                    Ok(v) => v,
                    Err(e) => {
                        return Err((Error::DecodeError(e), handle));
                    }
                };
                return Ok((vec![node], handle));
            }

            let mut leaves = Vec::new();

            // Get all child nodes and join
            for c_pointer in node
                .keys_pointers
                .iter()
                .enumerate()
                .filter(|(i, _)| i % 2 == 0)
                .map(|(i, x)| x.clone())
            {
                let (mut c_pointer_children, got_handle) =
                    match Self::collect_nodes(handle, c_pointer).await {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(e);
                        }
                    };
                leaves.append(&mut c_pointer_children);
                handle = got_handle;
            }

            Ok((leaves, handle))
        })
    }
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
                Ok(get_leaf(n, msg.0).ok_or(Error::TraversalError)?.into())
            } else {
                Err(Error::RecordNotFound)
            }
        })
    }
}

impl Handler<InsertKey> for TreeHandle {
    type Result = ResponseActFuture<Self, Result<(), Error>>;

    #[tracing::instrument]
    fn handle(&mut self, msg: InsertKey, ctx: &mut Context<Self>) -> Self::Result {
        tracing::info!("inserting key into indexing tree");

        let handle = self.handle.clone();
        let addr = ctx.address();

        Box::pin(
            async move {
                {
                    let mut f = handle.lock_owned().await;

                    // If the file length is zero, this is the root node. Insert it
                    if fs::file_is_empty(&f).await {
                        let mut root_node = create_internal_node();
                        let body_len = root_node.encoded_len();
                        let len_len = prost::length_delimiter_len(body_len);
                        insert_internal(
                            &mut root_node,
                            SearchKey::Lt(msg.0),
                            (body_len + len_len) as u64,
                        )?;

                        let mut leaf_node = create_leaf_node();
                        insert_leaf(&mut leaf_node, msg.0, msg.1.into())?;

                        // Write the two blocks to the file
                        f.write_all(root_node.encode_length_delimited_to_vec().as_slice())
                            .await
                            .map_err(|e| Error::IoError(e))?;
                        f.write_all(leaf_node.encode_length_delimited_to_vec().as_slice())
                            .await
                            .map_err(|e| Error::IoError(e))?;

                        println!(
                            "{} {} {} {} {} BRUH",
                            root_node.encode_length_delimited_to_vec().as_slice().len(),
                            leaf_node.encode_length_delimited_to_vec().as_slice().len(),
                            prost::length_delimiter_len(
                                leaf_node.encode_length_delimited_to_vec().as_slice().len()
                            ),
                            leaf_node.is_leaf_node,
                            body_len + len_len
                        );

                        return Ok(());
                    }

                    tracing::debug!("index not empty: searching for candidate node");

                    // Find the leaf node that this belongs in that has space and insert
                    let (candidate_leaf_node, pos, mut f) =
                        follow_node(f, 0, msg.0, SearchTarget::LeafNode).await?;

                    if let SearchResult::LeafNode(mut n) =
                        candidate_leaf_node.ok_or(Error::RecordNotFound)?
                    {
                        tracing::debug!("found candidate node: {:?}", &n);

                        // Attempt an insert

                        if let Ok(_) = insert_leaf(&mut n, msg.0, msg.1.clone().into()) {
                            // Write the candidate node
                            f.seek(SeekFrom::Start(pos))
                                .await
                                .map_err(|e| Error::IoError(e))?;
                            f.write_all(n.encode_length_delimited_to_vec().as_slice())
                                .await
                                .map_err(|e| Error::IoError(e))?;

                            return Ok(());
                        }

                        // The leaf node is full. Split it by creating an internal node
                        // with a value of the halfway point of the leaf node and inserting
                        // a leaf node with half of the values to the left of it
                        // and a leaf node with the other half of the values to the right of it
                        let left_vals = left_values_leaf(&n);
                        let right_vals = right_values_leaf(&n);
                        let median = median(Node::LeafNode(&n));

                        let mut new_parent = create_internal_node();
                        let mut left = create_leaf_node();
                        let mut right = create_leaf_node();

                        // Copy all nodes less than median into left node
                        for (k, v) in left_vals {
                            insert_leaf(&mut left, k, v)?;
                        }

                        for (k, v) in right_vals {
                            insert_leaf(&mut right, k, v)?;
                        }

                        if msg.0 < median {
                            insert_leaf(&mut left, msg.0, msg.1.into())?;
                        } else {
                            insert_leaf(&mut right, msg.0, msg.1.into())?;
                        }

                        let left_size = left.encoded_len();
                        let len_len = prost::length_delimiter_len(left_size);

                        // Go to the end of the file to insert the new nodes
                        let new_nodes_pos = f
                            .seek(SeekFrom::End(0))
                            .await
                            .map_err(|e| Error::IoError(e))?;
                        f.write_all(left.encode_length_delimited_to_vec().as_slice())
                            .await
                            .map_err(|e| Error::IoError(e))?;
                        f.write_all(right.encode_length_delimited_to_vec().as_slice())
                            .await
                            .map_err(|e| Error::IoError(e))?;

                        // Go back to the old node and overwrite it with the new nodes inserted
                        insert_internal(&mut new_parent, SearchKey::Lt(median), new_nodes_pos)?;
                        insert_internal(
                            &mut new_parent,
                            SearchKey::Gt(median),
                            (new_nodes_pos as usize + left_size + len_len) as u64,
                        )?;

                        f.seek(SeekFrom::Start(pos))
                            .await
                            .map_err(|e| Error::IoError(e))?;
                        f.write_all(new_parent.encode_length_delimited_to_vec().as_slice())
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
                        if len_internal_child_pointers(&n) > ORDER {
                            return Err(Error::TraversalError);
                        }

                        // Split the internal node up and then re-run the insertion
                        let left_vals = left_values_internal(&n);
                        let right_vals = right_values_internal(&n);
                        let median = median(Node::InternalNode(&n));

                        let mut new_parent = create_internal_node();
                        let mut left = create_internal_node();
                        let mut right = create_internal_node();

                        // Copy all nodes less than median into left node
                        for (k, v) in left_vals {
                            insert_internal(&mut left, SearchKey::Lt(k), v)?;
                        }

                        for (k, v) in right_vals {
                            insert_internal(&mut right, k, v)?;
                        }

                        // Go to the end of the file to insert the new nodes
                        let new_nodes_pos = f
                            .seek(SeekFrom::End(0))
                            .await
                            .map_err(|e| Error::IoError(e))?;
                        f.write_all(left.encode_length_delimited_to_vec().as_slice())
                            .await
                            .map_err(|e| Error::IoError(e))?;
                        f.write_all(right.encode_length_delimited_to_vec().as_slice())
                            .await
                            .map_err(|e| Error::IoError(e))?;

                        let parent_size = new_parent.encoded_len();
                        let len_len = prost::length_delimiter_len(parent_size);

                        // Go back to the old node and overwrite it with the new nodes inserted
                        insert_internal(&mut new_parent, SearchKey::Lt(median), new_nodes_pos)?;
                        insert_internal(
                            &mut new_parent,
                            SearchKey::Gt(median),
                            (new_nodes_pos as usize + len_len + parent_size) as u64,
                        )?;

                        f.seek(SeekFrom::Start(pos))
                            .await
                            .map_err(|e| Error::IoError(e))?;
                        f.write_all(new_parent.encode_length_delimited_to_vec().as_slice())
                            .await
                            .map_err(|e| Error::IoError(e))?;
                    }
                }

                addr.send(msg).await.map_err(|e| Error::MailboxError(e))?
            }
            .into_actor(self),
        )
    }
}

impl Handler<NextLeaf> for TreeHandle {
    type Result = ResponseFuture<Result<BTreeLeafNode, Error>>;

    fn handle(&mut self, msg: NextLeaf, _context: &mut Context<Self>) -> Self::Result {
        let seekers = self.seekers.clone();

        Box::pin(async move {
            let mut seekers = seekers.lock().await;

            // If the entry does not exist, make it
            if let None = seekers.get(&msg.0) {
                seekers.insert(msg.0.clone(), Vec::new());
            }

            // If there are no locations left to go back to, we are done
            let seeker_progress = seekers.get_mut(&msg.0).ok_or(Error::TraversalError)?;
            let next_node = seeker_progress.remove(0);

            Ok(next_node)
        })
    }
}

impl Handler<Iter> for TreeHandle {
    type Result = ResponseFuture<Result<Addr<TreeHandleIterator>, Error>>;

    fn handle(&mut self, msg: Iter, context: &mut Context<Self>) -> Self::Result {
        let seekers = self.seekers.clone();
        let handle = self.handle.clone();
        let addr = context.address();

        Box::pin(async move {
            let handle = handle.lock_owned().await;

            let iter = TreeHandleIterator {
                curr_leaf_idx: Arc::new(Mutex::new(None)),
                curr_leaf_node: Arc::new(Mutex::new(None)),
                handle: addr,
                handle_data: msg.0,
            }
            .start();
            let mut seekers = seekers.lock().await;

            let (res, _) = match Self::collect_nodes(handle, 0).await {
                Ok(x) => x,
                Err((e, _)) => {
                    return Err(e);
                }
            };

            seekers.insert(iter.clone(), res);

            Ok(iter)
        })
    }
}

pub struct TreeHandleIterator {
    handle: Addr<TreeHandle>,
    handle_data: Addr<DbHandle>,
    curr_leaf_node: Arc<Mutex<Option<BTreeLeafNode>>>,
    curr_leaf_idx: Arc<Mutex<Option<usize>>>,
}

impl Actor for TreeHandleIterator {
    type Context = Context<Self>;
}

impl Handler<Next> for TreeHandleIterator {
    type Result = ResponseFuture<Option<Tuple>>;

    fn handle(&mut self, _msg: Next, context: &mut Context<Self>) -> Self::Result {
        let handle = self.handle.clone();
        let handle_data = self.handle_data.clone();
        let curr_leaf_node_handle = self.curr_leaf_node.clone();
        let curr_leaf_idx = self.curr_leaf_idx.clone();

        let addr = context.address();

        Box::pin(async move {
            let mut curr_leaf_node = curr_leaf_node_handle.lock().await;
            let mut curr_leaf_idx = curr_leaf_idx.lock().await;

            // Load the next leaf node if we have no leaf node
            let (mut curr_leaf, mut curr_idx): (BTreeLeafNode, usize) =
                if let Some(curr) = (curr_leaf_node.clone()).zip(curr_leaf_idx.clone()) {
                    curr
                } else {
                    let node = handle.send(NextLeaf(addr.clone())).await.ok()?.ok()?;
                    curr_leaf_node.replace(node.clone());
                    (node, 0)
                };

            // If we are out of bounds, load a new leaf
            if curr_idx >= curr_leaf.keys.len() {
                (curr_leaf, curr_idx) = (handle.send(NextLeaf(addr)).await.ok()?.ok()?, 0);
            }

            // Get the next item
            let rid = &curr_leaf.disk_pointers[curr_idx];
            curr_leaf_idx.replace(curr_idx + 1);

            // Load from the heap file
            let res: Result<Page, Error> = handle_data
                .send(LoadPage(rid.page as usize))
                .await
                .map_err(|e| Error::MailboxError(e))
                .ok()?;
            let page = res.ok()?;

            let tup_bytes = page.data.get(rid.page_idx as usize)?;
            let tup = Tuple::decode_length_delimited(tup_bytes.data.as_slice()).ok()?;

            Some(tup)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_test::traced_test;

    #[traced_test]
    #[test]
    fn test_create_internal_node() {
        let node = create_internal_node();

        assert_eq!(node.is_leaf_node, false);
        assert_eq!(node.keys_pointers[0], 0);
    }

    #[traced_test]
    #[test]
    fn test_create_leaf_node() {
        let node = create_leaf_node();

        assert_eq!(node.is_leaf_node, true);
        assert_eq!(node.disk_pointers[0].is_empty, true);
    }

    #[traced_test]
    #[test]
    fn test_len_internal() {
        let mut node = create_internal_node();

        assert_eq!(len_internal_keys(&node), 0);
        assert_eq!(len_internal_child_pointers(&node), 0);

        insert_internal(&mut node, SearchKey::Lt(1), 2).unwrap();

        assert_eq!(len_internal_keys(&node), 1);
        assert_eq!(len_internal_child_pointers(&node), 1);
    }

    #[traced_test]
    #[test]
    fn test_len_leaf_node() {
        let mut node = create_leaf_node();

        assert_eq!(len_leaf_node_keys(&node), 0);
        assert_eq!(len_leaf_disk_pointers(&node), 0);

        insert_leaf(
            &mut node,
            1,
            RecordIdPointer {
                is_empty: false,
                page: 1,
                page_idx: 10,
            },
        )
        .unwrap();

        assert_eq!(len_leaf_node_keys(&node), 1);
        assert_eq!(len_leaf_disk_pointers(&node), 1);
    }

    #[traced_test]
    #[test]
    fn test_get_internal() {
        let mut node = create_internal_node();

        // Create a node-key space that looks like:
        // [pointer: (1, 1), key: 2, pointer: (3, 3), key: 4, pointer: (5, 5)]
        tracing::info!("inserting 5 test values");

        insert_internal(&mut node, SearchKey::Lt(2), 1).unwrap();
        tracing::info!("wrote 1 test value");

        insert_internal(&mut node, SearchKey::Lt(4), 3).unwrap();
        tracing::info!("wrote 2nd test value");

        insert_internal(&mut node, SearchKey::Lt(6), 5).unwrap();
        tracing::info!("wrote 3rd test value");

        insert_internal(&mut node, SearchKey::Lt(8), 7).unwrap();
        tracing::info!("wrote 4th test value");

        insert_internal(&mut node, SearchKey::Gt(8), 9).unwrap();
        tracing::info!("successfuly inserted test keys: {:?}", node.keys_pointers);

        assert_eq!(get_internal(&node, SearchKey::Lt(2)), Some(1));
        assert_eq!(get_internal(&node, SearchKey::Lt(4)), Some(3));
        assert_eq!(get_internal(&node, SearchKey::Lt(6)), Some(5));
        assert_eq!(get_internal(&node, SearchKey::Lt(8)), Some(7));
        assert_eq!(get_internal(&node, SearchKey::Gt(8)), Some(9));
    }

    #[traced_test]
    #[test]
    fn test_left_internal() {
        // Create a node-key space that looks like:
        // [pointer: (1, 1), key: 2, pointer: (3, 3), key: 4, pointer: (5, 5)]
        tracing::info!("inserting 5 test values");

        let mut node = create_internal_node();

        insert_internal(&mut node, SearchKey::Lt(2), 1).unwrap();
        insert_internal(&mut node, SearchKey::Lt(4), 3).unwrap();
        insert_internal(&mut node, SearchKey::Lt(6), 5).unwrap();
        insert_internal(&mut node, SearchKey::Lt(8), 7).unwrap();
        insert_internal(&mut node, SearchKey::Gt(8), 9).unwrap();

        let left = left_values_internal(&node);
        assert_eq!(left.len(), 2);
        assert_eq!(left[0], (2, 1));
        assert_eq!(left[1], (4, 3));
    }

    #[traced_test]
    #[test]
    fn test_median_internal() {
        // Create a node-key space that looks like:
        // [pointer: (1, 1), key: 2, pointer: (3, 3), key: 4, pointer: (5, 5)]
        tracing::info!("inserting 5 test values");

        let mut node = create_internal_node();

        insert_internal(&mut node, SearchKey::Lt(2), 1).unwrap();
        insert_internal(&mut node, SearchKey::Lt(4), 3).unwrap();
        insert_internal(&mut node, SearchKey::Lt(6), 5).unwrap();
        insert_internal(&mut node, SearchKey::Lt(8), 7).unwrap();
        insert_internal(&mut node, SearchKey::Gt(8), 9).unwrap();

        let median = median(Node::InternalNode(&node));
        assert_eq!(median, 6);
    }

    #[traced_test]
    #[test]
    fn test_right_internal() {
        // Create a node-key space that looks like:
        // [pointer: (1, 1), key: 2, pointer: (3, 3), key: 4, pointer: (5, 5)]
        tracing::info!("inserting 5 test values");

        let mut node = create_internal_node();

        insert_internal(&mut node, SearchKey::Lt(2), 1).unwrap();
        insert_internal(&mut node, SearchKey::Lt(4), 3).unwrap();
        insert_internal(&mut node, SearchKey::Lt(6), 5).unwrap();
        insert_internal(&mut node, SearchKey::Lt(8), 7).unwrap();
        insert_internal(&mut node, SearchKey::Gt(8), 9).unwrap();

        let right = right_values_internal(&node);
        assert_eq!(right.len(), 3);
        assert_eq!(right[0], (SearchKey::Lt(6), 5));
        assert_eq!(right[1], (SearchKey::Lt(8), 7));
        assert_eq!(right[2], (SearchKey::Gt(8), 9));
    }

    #[traced_test]
    #[test]
    fn test_insert_leaf() {
        use rand::Rng;

        let mut node = create_leaf_node();

        assert_eq!(len_leaf_node_keys(&node), 0);
        assert_eq!(len_leaf_disk_pointers(&node), 0);

        let mut rng = rand::thread_rng();

        for i in 0..ORDER {
            println!("inserted {}", i);

            let k: u64 = rng.gen();

            insert_leaf(
                &mut node,
                k,
                RecordIdPointer {
                    is_empty: false,
                    page: 1,
                    page_idx: 7,
                },
            )
            .expect("to succeed");
        }
    }

    #[traced_test]
    #[actix::test]
    async fn test_insert_key() {
        use rand::Rng;
        use tokio::fs::OpenOptions;

        async fn insert_key() -> Option<()> {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open("/tmp/test.idx")
                .await
                .map_err(|e| Error::IoError(e))
                .ok()?;

            let tree_handle = TreeHandle {
                handle: Arc::new(Mutex::new(f)),
                seekers: Arc::new(Mutex::new(HashMap::new())),
            }
            .start();

            let mut rng = rand::thread_rng();

            // Insert a shitton of random keys
            for i in 0..1_000 {
                let mut k: u64 = rng.gen();

                while k == 0 {
                    k = rng.gen();
                }

                println!("inserting {}: {}", i, k);

                tree_handle
                    .send(InsertKey(
                        k,
                        RecordId {
                            page: 10,
                            page_idx: 52,
                        },
                    ))
                    .await
                    .ok()?
                    .ok()?;
            }

            Some(())
        }

        let res = insert_key().await;
        std::fs::remove_file("/tmp/test.idx").unwrap();
        assert_eq!(res, Some(()));
    }
}
