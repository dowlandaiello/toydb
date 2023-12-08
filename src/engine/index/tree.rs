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
    GetKey, InsertKey, Iter,
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

fn pos_last_entry(node: &BTreeInternalNode) -> Option<usize> {
    if len_internal_keys(&node) == 0 {
        return Some(0);
    }

    node.keys_pointers
        .iter()
        .enumerate()
        .filter(|(_, x)| **x != 0)
        .map(|(i, _)| i)
        .last()
}

/// Inserts the key and value into the btree node, if space exists.
#[tracing::instrument]
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
            tracing::debug!("key already exists; inserting in position {}", pos_insert);

            node.keys_pointers[pos_insert] = v;

            Ok(())
        }
        None => {
            let pos_insert = pos_last_entry(&node).ok_or(Error::TraversalError)?;

            if len_keys < ORDER && len_children <= ORDER {
                match k {
                    SearchKey::Lt(k) => {
                        tracing::debug!(
                            "inserting in less than position in {:?} at {}",
                            node,
                            len_keys + len_children + 1
                        );

                        if len_keys == 0 {
                            node.keys_pointers[pos_insert] = v;
                            node.keys_pointers[pos_insert + 1] = k;

                            return Ok(());
                        }

                        // Insert in Lt pos
                        node.keys_pointers[pos_insert + 2] = k;
                        node.keys_pointers[pos_insert + 1] = v;
                    }
                    SearchKey::Gt(k) => {
                        tracing::debug!(
                            "inserting in GYATT position in {:?} at {}",
                            node,
                            pos_insert + 1
                        );

                        // Insert with a lt before if there are no keys
                        if len_keys == 0 {
                            node.keys_pointers[pos_insert + 1] = k;
                            node.keys_pointers[pos_insert + 2] = v;

                            return Ok(());
                        }

                        // Insert in gt pos
                        node.keys_pointers[pos_insert + 1] = k;
                        node.keys_pointers[pos_insert + 2] = v;
                    }
                }

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
) -> BoxFuture<
    'static,
    Result<(Option<SearchResult>, u64, OwnedMutexGuard<File>), (Error, OwnedMutexGuard<File>)>,
> {
    Box::pin(async move {
        let node = {
            // Get the current node
            if let Err(e) = handle
                .seek(SeekFrom::Start(pos as u64))
                .await
                .map_err(|e| Error::IoError(e))
            {
                return Err((e, handle));
            }

            // Peek 10 bytes for the length prefix
            let mut length_delim_buff: [u8; 10] = [0; 10];
            if let Err(e) = handle
                .read_exact(&mut length_delim_buff)
                .await
                .map_err(|e| Error::IoError(e))
            {
                return Err((e, handle));
            }
            let length_delim =
                match prost::decode_length_delimiter(&mut length_delim_buff.as_slice())
                    .map_err(|e| Error::DecodeError(e))
                {
                    Ok(v) => v,
                    Err(e) => {
                        return Err((e, handle));
                    }
                };
            tracing::debug!("got record length {} at pos {}", length_delim, pos);
            let delim_len = prost::length_delimiter_len(length_delim);

            // Make a buffer for the size of the node
            let mut node_buff: Vec<u8> = vec![0; length_delim + delim_len];

            // Read into the buff
            if let Err(e) = handle
                .seek(SeekFrom::Start(pos as u64))
                .await
                .map_err(|e| Error::IoError(e))
            {
                return Err((e, handle));
            }
            if let Err(e) = handle
                .read_exact(&mut node_buff)
                .await
                .map_err(|e| Error::IoError(e))
            {
                return Err((e, handle));
            }

            tracing::debug!("read node bytes: {:?}", node_buff);

            // Read a node from the buff
            let node = match BTreeInternalNode::decode_length_delimited(node_buff.as_slice())
                .map_err(|e| Error::DecodeError(e))
            {
                Ok(v) => v,
                Err(e) => {
                    return Err((e, handle));
                }
            };

            tracing::debug!("decoded node");

            // If the current node is a leaf AND we are looking for a leaf load the node and return the record pointer
            if node.is_leaf_node && target == SearchTarget::LeafNode {
                let node = match BTreeLeafNode::decode_length_delimited(node_buff.as_slice())
                    .map_err(|e| Error::DecodeError(e))
                {
                    Ok(v) => v,
                    Err(e) => {
                        return Err((e, handle));
                    }
                };

                tracing::debug!("found candidate leaf node: {:?}", node);

                return Ok((Some(SearchResult::LeafNode(node)), pos, handle));
            } else if node.is_leaf_node && target == SearchTarget::InternalNode {
                tracing::debug!("no candidate node found. giving up");

                return Ok((None, pos, handle));
            } else if target == SearchTarget::InternalNode {
                tracing::debug!("found candidate internal node: {:?}", node);

                return Ok((Some(SearchResult::InternalNode(node)), pos, handle));
            }

            node
        };

        tracing::debug!(
            "candidate node {:?} is not leaf node, or we are not looking for a leaf node",
            node
        );

        let next = if let Some(n) = get_internal(&node, SearchKey::Gt(key)) {
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

        tracing::debug!("continue probing for node");

        follow_node(handle, next, key, target).await
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
    #[tracing::instrument]
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

                tracing::debug!("finished path with leaf node: {:?}", node);

                return Ok((vec![node], handle));
            }

            tracing::debug!("collecting children for internal node: {:?}", node);

            let mut leaves = Vec::new();

            // Get all child nodes and join
            for c_pointer in node
                .keys_pointers
                .iter()
                .enumerate()
                .filter(|(i, _)| i % 2 == 0)
                .filter(|(_, ptr)| **ptr != 0)
                .map(|(_, x)| x.clone())
            {
                tracing::debug!("hopping to child node {}", c_pointer);

                let (mut c_pointer_children, got_handle) =
                    match Self::collect_nodes(handle, c_pointer).await {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(e);
                        }
                    };

                tracing::debug!("got child nodes {:?}", c_pointer_children);

                leaves.append(&mut c_pointer_children);
                handle = got_handle;
            }

            Ok((leaves, handle))
        })
    }

    async fn insert_empty_tree(
        mut f: OwnedMutexGuard<File>,
        msg: &InsertKey,
    ) -> Result<((), OwnedMutexGuard<File>), (Error, OwnedMutexGuard<File>)> {
        // If the file length is zero, this is the root node. Insert it
        if !fs::file_is_empty(&f).await {
            return Err((Error::TraversalError, f));
        }

        tracing::debug!("inserting leaf node and internal node");

        let mut root_node = create_internal_node();
        let body_len = root_node.encoded_len();
        let len_len = prost::length_delimiter_len(body_len);
        if let Err(e) = insert_internal(
            &mut root_node,
            SearchKey::Gt(msg.0),
            (body_len + len_len) as u64,
        ) {
            return Err((e, f));
        };

        let mut leaf_node = create_leaf_node();
        if let Err(e) = insert_leaf(&mut leaf_node, msg.0, msg.1.clone().into()) {
            return Err((e, f));
        }

        // Write the two blocks to the file
        if let Err(e) = f
            .seek(SeekFrom::Start(0))
            .await
            .map_err(|e| Error::IoError(e))
        {
            return Err((e, f));
        }

        tracing::debug!(
            "inserting root node {:?} at pos {} with length {}",
            root_node,
            0,
            body_len + len_len
        );

        if let Err(e) = f
            .write_all(root_node.encode_length_delimited_to_vec().as_slice())
            .await
            .map_err(|e| Error::IoError(e))
        {
            return Err((e, f));
        }
        if let Err(e) = f
            .write_all(leaf_node.encode_length_delimited_to_vec().as_slice())
            .await
            .map_err(|e| Error::IoError(e))
        {
            return Err((e, f));
        }

        tracing::debug!("wrote leaf node and internal node to disk at pos 0");

        Ok(((), f))
    }

    async fn insert_candidate_leaf_node(
        f: OwnedMutexGuard<File>,
        msg: &InsertKey,
    ) -> Result<((), OwnedMutexGuard<File>), (Error, OwnedMutexGuard<File>)> {
        let (candidate_leaf_node, candidate_pos, mut f) =
            match follow_node(f, 0, msg.0, SearchTarget::LeafNode).await {
                Ok(x) => x,
                Err(e) => {
                    return Err(e);
                }
            };

        let mut n = match candidate_leaf_node {
            Some(SearchResult::LeafNode(n)) => n,
            _ => {
                return Err((Error::TraversalError, f));
            }
        };

        if let Err(e) = insert_leaf(
            &mut n,
            msg.0,
            RecordIdPointer {
                is_empty: false,
                page: msg.1.page,
                page_idx: msg.1.page_idx,
            },
        ) {
            return Err((e, f));
        }

        tracing::debug!(
            "writing updated parent node to disk at pos {}",
            candidate_pos
        );

        // Write the node
        if let Err(e) = f.seek(SeekFrom::Start(candidate_pos)).await {
            return Err((Error::IoError(e), f));
        }

        if let Err(e) = f
            .write_all(n.encode_length_delimited_to_vec().as_slice())
            .await
        {
            return Err((Error::IoError(e), f));
        }

        Ok(((), f))
    }

    async fn insert_candidate_internal_node(
        f: OwnedMutexGuard<File>,
        msg: &InsertKey,
    ) -> Result<((), OwnedMutexGuard<File>), (Error, OwnedMutexGuard<File>)> {
        let (candidate_internal_node, candidate_pos, mut f) =
            match follow_node(f, 0, msg.0, SearchTarget::InternalNode).await {
                Ok(x) => x,
                Err(e) => {
                    return Err(e);
                }
            };

        tracing::debug!(
            "inserting leaf node into internal node {:?} with key {} and value {:?}",
            candidate_internal_node,
            msg.0,
            msg.1.clone(),
        );

        let mut n = match candidate_internal_node {
            Some(SearchResult::InternalNode(n)) => n,
            _ => {
                return Err((Error::TraversalError, f));
            }
        };

        let n_size = n.encoded_len();
        let len_len = prost::length_delimiter_len(n_size);
        let total_n_size = n_size + len_len;

        // Make a new leaf node
        let mut leaf = create_leaf_node();
        if let Err(e) = insert_leaf(
            &mut leaf,
            msg.0,
            RecordIdPointer {
                is_empty: false,
                page: msg.1.page,
                page_idx: msg.1.page_idx,
            },
        ) {
            return Err((e, f));
        }

        let insert_pos = match f.seek(SeekFrom::End(0)).await {
            Err(e) => {
                return Err((Error::IoError(e), f));
            }
            Ok(v) => v,
        };

        tracing::debug!("writing leaf node {:?} at disk pos {}", leaf, insert_pos,);

        // Write the leaf node
        if let Err(e) = f
            .write_all(leaf.encode_length_delimited_to_vec().as_slice())
            .await
        {
            return Err((Error::IoError(e), f));
        }

        if let Err(e) = insert_internal(&mut n, SearchKey::Gt(msg.0), insert_pos) {
            return Err((e, f));
        }

        tracing::debug!(
            "writing node {:?} at disk pos {} with length {}",
            n,
            candidate_pos,
            total_n_size
        );

        // Write the node
        if let Err(e) = f.seek(SeekFrom::Start(candidate_pos)).await {
            return Err((Error::IoError(e), f));
        }

        if let Err(e) = f
            .write_all(n.encode_length_delimited_to_vec().as_slice())
            .await
        {
            return Err((Error::IoError(e), f));
        }

        tracing::debug!("inserted successfuly: {:?}", n);

        Ok(((), f))
    }

    async fn split_internal_node(
        f: OwnedMutexGuard<File>,
        msg: &InsertKey,
    ) -> Result<((), OwnedMutexGuard<File>), (Error, OwnedMutexGuard<File>)> {
        tracing::debug!("splitting internal node with {:?}", msg);

        let (candidate_internal_node, candidate_pos, mut f) =
            match follow_node(f, 0, msg.0, SearchTarget::InternalNode).await {
                Ok(x) => x,
                Err(e) => {
                    return Err(e);
                }
            };

        let n = match candidate_internal_node {
            Some(SearchResult::InternalNode(n)) => n,
            _ => {
                return Err((Error::TraversalError, f));
            }
        };

        // Split the node up into left and right nodes
        let left = left_values_internal(&n);
        let right = left_values_internal(&n);
        let med = median(Node::InternalNode(&n));

        // Insert all values in appropriate nodes
        let mut left_n = create_internal_node();
        let mut right_n = create_internal_node();

        let left_len = left_n.encoded_len();
        let left_len_len = prost::length_delimiter_len(left_len);
        let total_left_len = left_len + left_len_len;

        for (k, v) in left {
            if let Err(e) = insert_internal(&mut left_n, SearchKey::Gt(k), v) {
                return Err((e, f));
            }
        }

        for (k, v) in right {
            if let Err(e) = insert_internal(&mut right_n, SearchKey::Gt(k), v) {
                return Err((e, f));
            }
        }

        // Create a new parent node
        let mut parent = create_internal_node();
        let len = parent.encoded_len();
        let len_len = prost::length_delimiter_len(len);
        let total_parent_len = len + len_len;

        if let Err(e) = insert_internal(
            &mut parent,
            SearchKey::Lt(med),
            candidate_pos as u64 + total_parent_len as u64,
        ) {
            return Err((e, f));
        }
        if let Err(e) = insert_internal(
            &mut parent,
            SearchKey::Gt(med),
            candidate_pos as u64 + total_parent_len as u64 + total_left_len as u64,
        ) {
            return Err((e, f));
        }

        // Insert the parent in its original spot
        if let Err(e) = f.seek(SeekFrom::Start(candidate_pos)).await {
            return Err((Error::IoError(e), f));
        }
        if let Err(e) = f
            .write_all(parent.encode_length_delimited_to_vec().as_slice())
            .await
        {
            return Err((Error::IoError(e), f));
        }

        // Insert the child nodes at the end of the file
        if let Err(e) = f.seek(SeekFrom::End(0)).await {
            return Err((Error::IoError(e), f));
        }

        if let Err(e) = f
            .write_all(left_n.encode_length_delimited_to_vec().as_slice())
            .await
        {
            return Err((Error::IoError(e), f));
        }

        if let Err(e) = f
            .write_all(right_n.encode_length_delimited_to_vec().as_slice())
            .await
        {
            return Err((Error::IoError(e), f));
        }

        Ok(((), f))
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
            let res = match follow_node(f, 0, msg.0, SearchTarget::LeafNode).await {
                Ok(x) => x.0,
                Err(e) => return Err(e.0),
            };

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

                    // If we can insert this as the root, do so
                    match Self::insert_empty_tree(f, &msg).await {
                        Ok((x, _)) => {
                            tracing::debug!("inserted in empty tree");

                            return Ok(x);
                        }
                        Err((Error::TraversalError, fi)) => {
                            f = fi;
                        }
                        Err((e, _)) => {
                            return Err(e);
                        }
                    };

                    tracing::debug!("index not empty: searching for candidate node");

                    // Insertion procedure priority:
                    //
                    // 1. Find the leaf node that this belongs in that has space and
                    // insert in that
                    match Self::insert_candidate_leaf_node(f, &msg).await {
                        Ok((x, _)) => {
                            tracing::debug!("inserted in candidate leaf node");

                            return Ok(x);
                        }
                        Err((Error::TraversalError, fi)) => {
                            f = fi;
                        }
                        Err((e, _)) => {
                            tracing::error!("could not find a candidate node: {:?}", e);

                            return Err(e);
                        }
                    };

                    // 2. Insert a new leaf node in the internal node
                    match Self::insert_candidate_internal_node(f, &msg).await {
                        Ok((x, _)) => {
                            tracing::debug!("inserted in candidate internal node");

                            return Ok(x);
                        }
                        Err((Error::TraversalError, fi)) => {
                            f = fi;
                        }
                        Err((e, _)) => {
                            return Err(e);
                        }
                    };

                    // 3. Split the internal node, and try again
                    match Self::split_internal_node(f, &msg).await {
                        Ok(_) => {}
                        Err((e, _)) => {
                            return Err(e);
                        }
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

            if seeker_progress.len() == 0 {
                return Err(Error::TraversalError);
            }

            tracing::debug!("making progress on {:?}", seeker_progress);

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

            tracing::debug!("collected nodes for iteration: {:?}", res);

            seekers.insert(iter.clone(), res);

            Ok(iter)
        })
    }
}

#[derive(Debug)]
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
    type Result = ResponseActFuture<Self, Option<Tuple>>;

    #[tracing::instrument]
    fn handle(&mut self, _msg: Next, context: &mut Context<Self>) -> Self::Result {
        tracing::debug!("obtaining next entry in index");

        let handle = self.handle.clone();
        let handle_data = self.handle_data.clone();
        let curr_leaf_node_handle = self.curr_leaf_node.clone();
        let curr_leaf_idx = self.curr_leaf_idx.clone();

        let addr = context.address();

        Box::pin(
            async move {
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
                    (curr_leaf, curr_idx) =
                        (handle.send(NextLeaf(addr.clone())).await.ok()?.ok()?, 0);
                }

                // Get the next item
                let mut rid = &curr_leaf.disk_pointers[curr_idx];

                curr_leaf_idx.replace(curr_idx + 1);

                // We need to keep advancing if the RID is empty
                if rid.is_empty {
                    (curr_leaf, curr_idx) = (handle.send(NextLeaf(addr)).await.ok()?.ok()?, 0);
                    curr_leaf_idx.replace(curr_idx + 1);
                    rid = &curr_leaf.disk_pointers[curr_idx];
                }

                tracing::debug!("got iteration RID: {:?}", rid);

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
            }
            .into_actor(self),
        )
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

        insert_internal(&mut node, SearchKey::Gt(1), 2).unwrap();

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

        insert_internal(&mut node, SearchKey::Gt(2), 3).unwrap();
        tracing::info!("wrote 1 test value");

        insert_internal(&mut node, SearchKey::Lt(2), 1).unwrap();
        tracing::info!("wrote 2nd test value");

        insert_internal(&mut node, SearchKey::Gt(4), 5).unwrap();
        tracing::info!("wrote 3rd test value");

        insert_internal(&mut node, SearchKey::Gt(6), 7).unwrap();
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

        for _ in 0..ORDER {
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
    async fn test_insert_empty_tree() {
        async fn insert_key() -> Option<()> {
            use tokio::fs::OpenOptions;

            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open("/tmp/test_empty_tree.idx")
                .await
                .map_err(|e| Error::IoError(e))
                .ok()?;

            let f_mutex = Arc::new(Mutex::new(f));
            let f = f_mutex.lock_owned().await;

            // Random insertion request
            let msg = InsertKey(
                1,
                RecordId {
                    page: 12,
                    page_idx: 5,
                },
            );

            // Insert a key
            if let Err(_) = TreeHandle::insert_empty_tree(f, &msg).await {
                return None;
            }

            Some(())
        }

        let res = insert_key().await;
        std::fs::remove_file("/tmp/test_empty_tree.idx").unwrap();
        assert_eq!(res, Some(()));
    }

    // Create a few different trees:
    // - One that has a root to a leaf node
    // a
    // |
    // b
    //
    // - One that has a root to two leaf nodes, of which we are trying
    // to find the right node
    //   a
    //  _|_
    // |   |
    // b   c
    //
    // - One that ha an intermediate internal node
    // a
    // |
    // b
    // |
    // c

    #[traced_test]
    #[actix::test]
    async fn test_follow_node_direct_descendant() {
        async fn test_follow_node() -> Result<(), Error> {
            use tokio::fs::OpenOptions;

            tracing::info!("testing follow_node with direct descendant");

            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open("/tmp/test_follow_node_1.idx")
                .await
                .map_err(|e| Error::IoError(e))?;
            let f = Arc::new(Mutex::new(f));
            let mut f = f.lock_owned().await;

            // Insert root node
            let mut root = create_internal_node();
            let root_len = root.encoded_len();
            let root_len_len = prost::length_delimiter_len(root_len);

            let total_root_len = root_len + root_len_len;

            // Insert leaf
            let mut b = create_leaf_node();
            insert_leaf(
                &mut b,
                1,
                RecordIdPointer {
                    is_empty: false,
                    page: 1,
                    page_idx: 2,
                },
            )?;

            // Insert the leaf into the root node
            tracing::info!("inserting leaf node into root node at position Gt(1)");
            insert_internal(&mut root, SearchKey::Gt(1), total_root_len as u64)?;

            tracing::info!("writing root node and leaf node to disk");
            f.write_all(root.encode_length_delimited_to_vec().as_slice())
                .await?;
            f.write_all(b.encode_length_delimited_to_vec().as_slice())
                .await?;

            // This should give us node b
            let res = match follow_node(f, 0, 1, SearchTarget::LeafNode).await {
                Ok((Some(SearchResult::LeafNode(res)), _, _)) => res,
                Err(e) => return Err(e.0),
                _ => {
                    return Err(Error::TraversalError);
                }
            };

            assert_eq!(res.is_leaf_node, true);
            assert_eq!(res.keys[0], 1);
            assert_eq!(res.disk_pointers[0].is_empty, false);
            assert_eq!(res.disk_pointers[0].page, 1);
            assert_eq!(res.disk_pointers[0].page_idx, 2);

            Ok(())
        }

        let res = test_follow_node().await;
        std::fs::remove_file("/tmp/test_follow_node_1.idx").unwrap();
        res.unwrap();
    }

    #[traced_test]
    #[actix::test]
    async fn test_follow_node_right_descendant() {
        async fn test_follow_node() -> Result<(), Error> {
            use tokio::fs::OpenOptions;

            tracing::info!("testing follow_node with direct descendant");

            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open("/tmp/test_follow_node_2.idx")
                .await
                .map_err(|e| Error::IoError(e))?;
            let f = Arc::new(Mutex::new(f));
            let mut f = f.lock_owned().await;

            // Insert root node
            let mut root = create_internal_node();
            let root_len = root.encoded_len();
            let root_len_len = prost::length_delimiter_len(root_len);

            let total_root_len = root_len + root_len_len;

            // Insert leaf
            let mut b = create_leaf_node();
            insert_leaf(
                &mut b,
                1,
                RecordIdPointer {
                    is_empty: false,
                    page: 1,
                    page_idx: 2,
                },
            )?;
            let leaf_len = b.encoded_len();
            let leaf_len_len = prost::length_delimiter_len(leaf_len);

            let total_leaf_len = leaf_len + leaf_len_len;

            let mut c = create_leaf_node();
            insert_leaf(
                &mut c,
                2,
                RecordIdPointer {
                    is_empty: false,
                    page: 3,
                    page_idx: 4,
                },
            )?;

            // Insert the leaf into the root node
            tracing::info!("inserting leaf node into root node at position Gt(1)");
            insert_internal(&mut root, SearchKey::Gt(1), total_root_len as u64)?;

            tracing::info!("inserting leaf node into root node at position Gt(2)");
            insert_internal(
                &mut root,
                SearchKey::Gt(2),
                total_leaf_len as u64 + total_root_len as u64,
            )?;

            tracing::info!("writing root node and leaf node to disk");
            f.write_all(root.encode_length_delimited_to_vec().as_slice())
                .await?;
            f.write_all(b.encode_length_delimited_to_vec().as_slice())
                .await?;
            f.write_all(c.encode_length_delimited_to_vec().as_slice())
                .await?;

            // This should give us node b
            let res = match follow_node(f, 0, 2, SearchTarget::LeafNode).await {
                Ok((Some(SearchResult::LeafNode(res)), _, _)) => res,
                Err(e) => return Err(e.0),
                _ => {
                    return Err(Error::TraversalError);
                }
            };

            assert_eq!(res.is_leaf_node, true);
            assert_eq!(res.keys[0], 2);
            assert_eq!(res.disk_pointers[0].is_empty, false);
            assert_eq!(res.disk_pointers[0].page, 3);
            assert_eq!(res.disk_pointers[0].page_idx, 4);

            Ok(())
        }

        let res = test_follow_node().await;
        std::fs::remove_file("/tmp/test_follow_node_2.idx").unwrap();
        res.unwrap();
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
            for i in 0..1000 {
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
