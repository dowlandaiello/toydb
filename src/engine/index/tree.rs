use super::{
    super::{
        super::{
            error::Error,
            items_capnp::{b_tree_internal_node, b_tree_leaf_node, record_id_pointer, tuple},
            owned_items::{
                BTreeInternalNode, BTreeLeafNode, Page, RecordId, RecordIdPointer, Tuple,
            },
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
use capnp::{
    message::{Builder, ReaderOptions},
    serialize,
};
use futures::future::BoxFuture;
use std::{collections::HashMap, io::SeekFrom, ops::DerefMut, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, OwnedMutexGuard},
};
use tokio_util::compat::TokioAsyncReadCompatExt;

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

#[derive(Debug)]
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
        .filter(|(i, x)| i % 2 != 0 && **x != 0)
        .map(|(_, x)| x)
        .collect::<Vec<&u64>>()
        .len()
}

fn len_leaf_node_keys(node: &BTreeLeafNode) -> usize {
    node.keys
        .iter()
        .filter(|x| **x != 0)
        .collect::<Vec<&u64>>()
        .len()
}

fn len_internal_child_pointers(node: &BTreeInternalNode) -> usize {
    node.keys_pointers
        .iter()
        .enumerate()
        .filter(|(i, x)| i % 2 == 0 && **x != 0)
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

fn pos_last_entry_lt(node: &BTreeInternalNode, key: u64) -> Option<usize> {
    if len_internal_keys(&node) == 0 {
        return Some(0);
    }

    node.keys_pointers
        .iter()
        .enumerate()
        .filter(|(i, x)| **x != 0 && i % 2 != 0 && **x < key)
        .map(|(i, _)| i)
        .last()
        .map(|pos| pos + 1)
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
            // This value already exists.
            if key.clone() == v {
                match k {
                    SearchKey::Lt(x) => {
                        pos_insert = Some(i);

                        break;
                    }
                    SearchKey::Gt(x) => {
                        pos_insert = Some(i);

                        break;
                    }
                }
            }

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
            if len_keys < ORDER && len_children <= ORDER {
                match k {
                    SearchKey::Lt(k) => {
                        let pos_insert = pos_last_entry_lt(&node, k).unwrap_or(0);

                        tracing::debug!(
                            "inserting in less than position in {:?} at {}",
                            node,
                            pos_insert + 1,
                        );

                        if len_keys == 0 {
                            node.keys_pointers[pos_insert] = v;
                            node.keys_pointers[pos_insert + 1] = k;

                            return Ok(());
                        }

                        // Insert in Lt pos
                        node.keys_pointers.insert(pos_insert + 2, k);
                        node.keys_pointers.insert(pos_insert + 1, v);
                        node.keys_pointers
                            .resize_with(ORDER * 2 + 1, Default::default);
                    }
                    SearchKey::Gt(k) => {
                        let pos_insert = pos_last_entry_lt(&node, k).unwrap_or(0);

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
                        node.keys_pointers.insert(pos_insert + 1, k);
                        node.keys_pointers.insert(pos_insert + 2, v);
                        node.keys_pointers
                            .resize_with(ORDER * 2 + 1, Default::default);
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
#[tracing::instrument]
fn median(n: Node) -> u64 {
    match n {
        Node::InternalNode(n) => {
            let keys = n
                .keys_pointers
                .iter()
                .enumerate()
                .filter(|(i, x)| i % 2 != 0 && **x != 0)
                .map(|(_, x)| x.clone())
                .collect::<Vec<u64>>();
            keys[keys.len() / 2]
        }
        Node::LeafNode(n) => n.keys.as_slice()[n.keys.len() / 2],
    }
}

#[tracing::instrument]
fn left_values_internal(node: &BTreeInternalNode) -> Vec<(SearchKey, u64)> {
    let med = median(Node::InternalNode(node));

    tracing::debug!(
        "getting left values for node {:?} at median {:?}",
        node,
        med
    );

    let mut vals = Vec::new();
    let mut k_v = Vec::new();

    let len = node.keys_pointers.len();

    for (i, key) in node.keys_pointers.iter().enumerate() {
        if i % 2 == 0 {
            continue;
        }

        if *key >= med {
            break;
        }

        vals.push((SearchKey::Lt(i as u64), i - 1));

        if len / 2 - i > 1 {
            vals.push((SearchKey::Gt(i as u64), i + 1));
        }
    }

    for (k_i, v_i) in vals {
        match k_i {
            SearchKey::Gt(i) => {
                k_v.push((
                    SearchKey::Gt(node.keys_pointers[i as usize]),
                    node.keys_pointers[v_i],
                ));
            }
            SearchKey::Lt(i) => {
                k_v.push((
                    SearchKey::Lt(node.keys_pointers[i as usize]),
                    node.keys_pointers[v_i],
                ));
            }
        }
    }

    k_v
}

fn right_values_internal(node: &BTreeInternalNode) -> Vec<(SearchKey, u64)> {
    let med = median(Node::InternalNode(node));

    let mut vals = Vec::new();
    let mut k_v = Vec::new();

    let len = node.keys_pointers.len();

    for (i, key) in node.keys_pointers.iter().enumerate() {
        if i % 2 == 0 {
            continue;
        }

        if *key < med {
            continue;
        }

        vals.push((SearchKey::Lt(i as u64), i - 1));

        if len - i >= 1 {
            vals.push((SearchKey::Gt(i as u64), i + 1));
        }
    }

    for (k_i, v_i) in vals {
        match k_i {
            SearchKey::Gt(i) => {
                k_v.push((
                    SearchKey::Gt(node.keys_pointers[i as usize]),
                    node.keys_pointers[v_i],
                ));
            }
            SearchKey::Lt(i) => {
                k_v.push((
                    SearchKey::Lt(node.keys_pointers[i as usize]),
                    node.keys_pointers[v_i],
                ));
            }
        }
    }

    k_v
}

fn get_internal(node: &BTreeInternalNode, k: SearchKey) -> Option<u64> {
    let mut pos = None;

    let len = len_internal_child_pointers(node) + len_internal_keys(node);

    tracing::debug!("searching for key {:?} in haystack of length {}", k, len);

    for i in 0..len {
        if i % 2 == 0 {
            continue;
        }

        let key = &node.keys_pointers[i];
        let peek_f =
            node.keys_pointers
                .get(i + 2)
                .and_then(|peek| if peek == &0 { None } else { Some(peek) });
        let peek_b = if i >= 2 {
            node.keys_pointers
                .get(i - 2)
                .and_then(|peek| if peek == &0 { None } else { Some(peek) })
        } else {
            None
        };

        tracing::debug!(
            "at key {} with peek_f {:?} and peek_b {:?}",
            key,
            peek_f,
            peek_b
        );

        match k {
            SearchKey::Gt(x) => {
                // If this key is less than x and the next key is greater than it
                // we've found our spot

                // Alternatively, if this is the last key and it's less than us,
                // that means we've DEFINITELY found it
                match peek_f {
                    Some(peek) => {
                        if key <= &x && peek > &x {
                            pos = Some(i + 1);
                        }
                    }
                    None => {
                        if key <= &x {
                            pos = Some(i + 1);

                            break;
                        }
                    }
                }
            }
            SearchKey::Lt(x) => {
                // If this key is greater than x and the previous key is less than it
                // we've found our spot

                // Alternatively, if this is the first key and it's greater than us,
                // that means we've DEFINITELy found it
                match peek_b {
                    Some(peek) => {
                        if key > &x && peek <= &x {
                            pos = Some(i - 1);

                            break;
                        }
                    }
                    None => {
                        if key > &x {
                            pos = Some(i - 1);
                        }
                    }
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

            // Make a reader to read the contents of the node
            let reader = match capnp_futures::serialize::read_message(
                TokioAsyncReadCompatExt::compat(handle.deref_mut()),
                ReaderOptions::default(),
            )
            .await
            .map_err(|e| Error::DecodeError(e))
            {
                Ok(v) => v,
                Err(e) => {
                    return Err((e, handle));
                }
            };
            let n_r = reader
                .get_root::<b_tree_leaf_node::Reader>()
                .ok()
                .and_then(|n_r| {
                    let keys_r = n_r.get_keys().ok()?;
                    let keys = keys_r.as_slice()?.to_vec();

                    let disk_pointers_r = n_r.get_disk_pointers().ok()?;
                    let mut disk_pointers = Vec::new();
                    let mut curr = disk_pointers_r.try_get(0);

                    loop {
                        if let Some(c) = curr {
                            let disk_pointer_r = c;

                            let disk_pointer = RecordIdPointer {
                                is_empty: disk_pointer_r.get_is_empty(),
                                page: disk_pointer_r.get_page(),
                                page_idx: disk_pointer_r.get_page_idx(),
                            };
                            disk_pointers.push(disk_pointer);

                            curr = disk_pointers_r.try_get(disk_pointers.len() as u32);
                        } else {
                            break;
                        }
                    }

                    // Read a node from the buff
                    Some(BTreeLeafNode {
                        is_leaf_node: n_r.get_is_leaf_node(),
                        keys,
                        disk_pointers,
                    })
                });

            let n_internal = reader
                .get_root::<b_tree_internal_node::Reader>()
                .ok()
                .and_then(|n_r| {
                    let keys_pointers_r = n_r.get_keys_pointers().ok()?;

                    Some(BTreeInternalNode {
                        is_leaf_node: n_r.get_is_leaf_node(),
                        keys_pointers: keys_pointers_r.as_slice()?.to_vec(),
                    })
                });

            tracing::debug!("decoded node: {:?}", n_r);

            // If the current node is a leaf AND we are looking for a leaf load the node and return the record pointer
            if let Some(node) = n_r {
                if target == SearchTarget::LeafNode {
                    tracing::debug!("found candidate leaf node: {:?}", node);

                    return Ok((Some(SearchResult::LeafNode(node)), pos, handle));
                } else if target == SearchTarget::InternalNode {
                    tracing::debug!("no candidate node found. giving up");

                    return Ok((None, pos, handle));
                }
            } else if let Some(node) = n_internal.clone() {
                if target == SearchTarget::InternalNode {
                    return Ok((Some(SearchResult::InternalNode(node)), pos, handle));
                }
            } else {
                tracing::debug!("no candidate node found. giving up");

                return Ok((None, pos, handle));
            }

            n_internal
        };

        tracing::debug!(
            "candidate node {:?} is not leaf node, or we are not looking for a leaf node",
            node
        );

        let node = if let Some(node) = node {
            node
        } else {
            return Ok((None, pos, handle));
        };

        let next = if let Some(n) = get_internal(&node, SearchKey::Gt(key)) {
            tracing::debug!("found next node to probe for key {}: {:?}", key, n);

            n
        } else if target == SearchTarget::LeafNode {
            tracing::debug!(
                "we were looking for a next node to probe for key {}, but no leaf nodes are available", key
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

            let reader = match capnp_futures::serialize::read_message(
                TokioAsyncReadCompatExt::compat(handle.deref_mut()),
                ReaderOptions::default(),
            )
            .await
            .map_err(|e| Error::DecodeError(e))
            {
                Ok(v) => v,
                Err(e) => {
                    return Err((e, handle));
                }
            };

            let n_r = reader
                .get_root::<b_tree_leaf_node::Reader>()
                .ok()
                .and_then(|n_r| {
                    let keys_r = n_r.get_keys().ok()?;
                    let keys = keys_r.as_slice()?.to_vec();

                    let disk_pointers_r = n_r.get_disk_pointers().ok()?;
                    let mut disk_pointers = Vec::new();
                    let mut curr = disk_pointers_r.try_get(0);

                    loop {
                        if let Some(c) = curr {
                            let disk_pointer_r = c;

                            let disk_pointer = RecordIdPointer {
                                is_empty: disk_pointer_r.get_is_empty(),
                                page: disk_pointer_r.get_page(),
                                page_idx: disk_pointer_r.get_page_idx(),
                            };
                            disk_pointers.push(disk_pointer);

                            curr = disk_pointers_r.try_get(disk_pointers.len() as u32);
                        } else {
                            break;
                        }
                    }

                    // Read a node from the buff
                    Some(BTreeLeafNode {
                        is_leaf_node: n_r.get_is_leaf_node(),
                        keys,
                        disk_pointers,
                    })
                });

            let n_internal = reader
                .get_root::<b_tree_internal_node::Reader>()
                .ok()
                .and_then(|n_r| {
                    let keys_pointers_r = n_r.get_keys_pointers().ok()?;

                    Some(BTreeInternalNode {
                        is_leaf_node: n_r.get_is_leaf_node(),
                        keys_pointers: keys_pointers_r.as_slice()?.to_vec(),
                    })
                });

            if let Some(node) = n_r {
                tracing::debug!("finished path with leaf node: {:?}", node);

                return Ok((vec![node], handle));
            } else if let Some(node) = n_internal {
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
            } else {
                Err((Error::TraversalError, handle))
            }
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

        let mut builder = Builder::new_default();
        let mut node_msg = builder.init_root::<b_tree_internal_node::Builder>();
        {
            node_msg
                .reborrow()
                .init_keys_pointers((ORDER * 2 + 1) as u32);
        }

        let body_len = match node_msg.total_size().map_err(|e| Error::EncodeError(e)) {
            Ok(v) => v,
            Err(e) => {
                return Err((e, f));
            }
        }
        .word_count
            * 2;

        if let Err(e) = insert_internal(&mut root_node, SearchKey::Gt(msg.0), body_len) {
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
            body_len,
        );

        let mut keys_pointers = node_msg
            .reborrow()
            .init_keys_pointers((ORDER * 2 + 1) as u32);

        // Set the keys_pointers in the encoded message to our owned ones
        for (i, k) in root_node.keys_pointers.iter().enumerate() {
            keys_pointers.set(i as u32, k.clone());
        }

        if let Err(e) = f
            .write_all(serialize::write_message_to_words(&builder).as_slice())
            .await
            .map_err(|e| Error::IoError(e))
        {
            return Err((e, f));
        }

        let mut builder = Builder::new_default();
        let mut node_msg = builder.init_root::<b_tree_leaf_node::Builder>();

        {
            let mut keys = node_msg.reborrow().init_keys(ORDER as u32);

            for (i, k) in leaf_node.keys.into_iter().enumerate() {
                keys.set(i as u32, k);
            }
        }

        {
            let mut disk_pointers = node_msg.reborrow().init_disk_pointers(ORDER as u32);

            for (i, k) in leaf_node.disk_pointers.into_iter().enumerate() {
                let mut rid = disk_pointers.reborrow().get(i as u32);
                rid.set_is_empty(k.is_empty);
                rid.set_page(k.page);
                rid.set_page_idx(k.page_idx);
            }
        }

        if let Err(e) = f
            .write_all(serialize::write_message_to_words(&builder).as_slice())
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

        let mut builder = Builder::new_default();
        let mut node_msg = builder.init_root::<b_tree_leaf_node::Builder>();

        {
            let mut keys = node_msg.reborrow().init_keys(ORDER as u32);

            for (i, k) in n.keys.iter().enumerate() {
                keys.set(i as u32, k.clone());
            }
        }

        {
            let mut disk_pointers = node_msg.reborrow().init_disk_pointers(ORDER as u32);

            for (i, k) in n.disk_pointers.iter().enumerate() {
                let mut rid = disk_pointers.reborrow().get(i as u32);
                rid.set_is_empty(k.is_empty);
                rid.set_page(k.page);
                rid.set_page_idx(k.page_idx);
            }
        }

        let encoded = serialize::write_message_to_words(&builder);

        tracing::debug!(
            "writing updated parent node {:?} to disk at pos {} with length {}",
            n,
            candidate_pos,
            encoded.len()
        );

        // Write the node
        if let Err(e) = f.seek(SeekFrom::Start(candidate_pos)).await {
            return Err((Error::IoError(e), f));
        }

        if let Err(e) = f.write_all(encoded.as_slice()).await {
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

        let mut builder_internal = Builder::new_default();
        let mut node_msg = builder_internal.init_root::<b_tree_internal_node::Builder>();
        {
            node_msg
                .reborrow()
                .init_keys_pointers((ORDER * 2 + 1) as u32);
        }

        let body_len = match node_msg.total_size().map_err(|e| Error::EncodeError(e)) {
            Ok(v) => v,
            Err(e) => {
                return Err((e, f));
            }
        }
        .word_count
            * 2;

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

        let mut builder = Builder::new_default();
        let mut node_msg_leaf = builder.init_root::<b_tree_leaf_node::Builder>();

        {
            let mut keys = node_msg_leaf.reborrow().init_keys(ORDER as u32);

            for (i, k) in leaf.keys.iter().enumerate() {
                keys.set(i as u32, k.clone());
            }
        }

        {
            let mut disk_pointers = node_msg_leaf.reborrow().init_disk_pointers(ORDER as u32);

            for (i, k) in leaf.disk_pointers.iter().enumerate() {
                let mut rid = disk_pointers.reborrow().get(i as u32);
                rid.set_is_empty(k.is_empty);
                rid.set_page(k.page);
                rid.set_page_idx(k.page_idx);
            }
        }

        let insert_pos = match f.seek(SeekFrom::End(0)).await {
            Err(e) => {
                return Err((Error::IoError(e), f));
            }
            Ok(v) => v,
        };

        tracing::debug!("writing leaf node {:?} at disk pos {}", leaf, insert_pos);

        // Write the leaf node
        if let Err(e) = f
            .write_all(serialize::write_message_to_words(&builder).as_slice())
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
            body_len,
        );

        // Write the node
        if let Err(e) = f.seek(SeekFrom::Start(candidate_pos)).await {
            return Err((Error::IoError(e), f));
        }

        node_msg
            .reborrow()
            .init_keys_pointers((ORDER * 2 + 1) as u32);

        if let Err(e) = f
            .write_all(serialize::write_message_to_words(&builder_internal).as_slice())
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
        let right = right_values_internal(&n);
        let med = median(Node::InternalNode(&n));

        tracing::debug!("splitting by {} into {:?}, {:?}", med, left, right);

        // Insert all values in appropriate nodes
        let mut left_n = create_internal_node();
        let mut right_n = create_internal_node();

        let mut left_builder = Builder::new_default();
        let mut node_msg_left = left_builder.init_root::<b_tree_internal_node::Builder>();

        let mut right_builder = Builder::new_default();
        let mut node_msg_right = right_builder.init_root::<b_tree_internal_node::Builder>();

        {
            node_msg_left
                .reborrow()
                .init_keys_pointers((ORDER * 2 + 1) as u32);
        }

        {
            node_msg_right
                .reborrow()
                .init_keys_pointers((ORDER * 2 + 1) as u32);
        }

        let total_left_len = match node_msg_left
            .total_size()
            .map_err(|e| Error::EncodeError(e))
        {
            Ok(v) => v,
            Err(e) => {
                return Err((e, f));
            }
        }
        .word_count
            * 2;

        for (k, v) in left {
            if let Err(e) = insert_internal(&mut left_n, k, v) {
                return Err((e, f));
            }
        }

        for (k, v) in right {
            if let Err(e) = insert_internal(&mut right_n, k, v) {
                return Err((e, f));
            }
        }

        tracing::debug!("created left node: {:?}", left_n);
        tracing::debug!("created right node: {:?}", right_n);

        // Create a new parent node
        let mut parent = create_internal_node();

        // Insert the child nodes at the end of the file
        let insertion_pos = match f.seek(SeekFrom::End(0)).await {
            Ok(v) => v,
            Err(e) => {
                return Err((Error::IoError(e), f));
            }
        };

        let mut keys_pointers = node_msg_left
            .reborrow()
            .init_keys_pointers((ORDER * 2 + 1) as u32);

        // Set the keys_pointers in the encoded message to our owned ones
        for (i, k) in left_n.keys_pointers.iter().enumerate() {
            keys_pointers.set(i as u32, k.clone());
        }

        let mut keys_pointers = node_msg_right
            .reborrow()
            .init_keys_pointers((ORDER * 2 + 1) as u32);

        // Set the keys_pointers in the encoded message to our owned ones
        for (i, k) in right_n.keys_pointers.iter().enumerate() {
            keys_pointers.set(i as u32, k.clone());
        }

        if let Err(e) = f
            .write_all(serialize::write_message_to_words(&left_builder).as_slice())
            .await
        {
            return Err((Error::IoError(e), f));
        }

        if let Err(e) = f
            .write_all(serialize::write_message_to_words(&right_builder).as_slice())
            .await
        {
            return Err((Error::IoError(e), f));
        }

        if let Err(e) = insert_internal(&mut parent, SearchKey::Lt(med), insertion_pos as u64) {
            return Err((e, f));
        }
        if let Err(e) = insert_internal(
            &mut parent,
            SearchKey::Gt(med),
            insertion_pos as u64 + total_left_len as u64,
        ) {
            return Err((e, f));
        }

        // Insert the parent in its original spot
        if let Err(e) = f.seek(SeekFrom::Start(candidate_pos)).await {
            return Err((Error::IoError(e), f));
        }

        let mut builder = Builder::new_default();
        let mut node_msg = builder.init_root::<b_tree_internal_node::Builder>();
        {
            let mut keys_pointers = node_msg
                .reborrow()
                .init_keys_pointers((ORDER * 2 + 1) as u32);

            // Set the keys_pointers in the encoded message to our owned ones
            for (i, k) in parent.keys_pointers.iter().enumerate() {
                keys_pointers.set(i as u32, k.clone());
            }
        }

        if let Err(e) = f
            .write_all(serialize::write_message_to_words(&builder).as_slice())
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
                            tracing::debug!("could not insert into empty tree");

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
                            tracing::debug!("could not insert into leaf node");

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
                            tracing::debug!("could not insert into internal node");

                            f = fi;
                        }
                        Err((e, _)) => {
                            return Err(e);
                        }
                    };

                    // 3. Split the internal node, and try again
                    match Self::split_internal_node(f, &msg).await {
                        Ok(_) => {
                            tracing::debug!("split node successfully");
                        }
                        Err((e, _)) => {
                            return Err(e);
                        }
                    }
                }

                tracing::debug!("retrying insert");

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

                // Decode the tuple into a catalogue entry, then return
                let reader = serialize::read_message_from_flat_slice(
                    &mut tup_bytes.data.as_slice(),
                    ReaderOptions::default(),
                )
                .ok()?;
                let tup_r = reader.get_root::<tuple::Reader>().ok()?;

                let rel_name = tup_r.get_rel_name().ok()?.to_string().ok()?;

                let elements_r = tup_r.get_elements().ok()?;
                let mut elements = Vec::new();
                let mut current = elements_r.try_get(0);

                loop {
                    if let Some(Ok(c)) = current {
                        elements.push(c.to_vec());
                        current = elements_r.try_get(elements.len() as u32);
                    } else {
                        break;
                    }
                }

                let tup = Tuple { rel_name, elements };

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

        insert_internal(&mut node, SearchKey::Lt(2), 1).unwrap();
        tracing::info!("wrote 1 test value");

        insert_internal(&mut node, SearchKey::Lt(4), 3).unwrap();
        tracing::info!("wrote 2nd test value");

        insert_internal(&mut node, SearchKey::Gt(4), 5).unwrap();
        tracing::info!("wrote 3rd test value");

        insert_internal(&mut node, SearchKey::Gt(6), 7).unwrap();
        tracing::info!("wrote 4th test value");

        insert_internal(&mut node, SearchKey::Lt(9), 8).unwrap();
        tracing::info!("successfuly inserted test keys: {:?}", node.keys_pointers);

        assert_eq!(get_internal(&node, SearchKey::Lt(2)), Some(1));
        assert_eq!(get_internal(&node, SearchKey::Lt(4)), Some(3));
        assert_eq!(get_internal(&node, SearchKey::Gt(4)), Some(5));
        assert_eq!(get_internal(&node, SearchKey::Lt(6)), Some(5));
        assert_eq!(get_internal(&node, SearchKey::Lt(8)), Some(7));
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

        tracing::debug!("got left values: {:?}", left);

        assert_eq!(left.len(), 3);
        assert_eq!(left[0], (SearchKey::Lt(2), 1));
        assert_eq!(left[1], (SearchKey::Gt(2), 3));
        assert_eq!(left[2], (SearchKey::Lt(4), 3));

        tracing::info!("inserting 1 test value");

        let mut node = create_internal_node();

        insert_internal(&mut node, SearchKey::Lt(2), 1).unwrap();
        insert_internal(&mut node, SearchKey::Gt(2), 3).unwrap();

        let left = left_values_internal(&node);
        assert_eq!(left.len(), 0);
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

        tracing::debug!("got right values: {:?}", right);

        assert_eq!(right.len(), 4);
        assert_eq!(right[0], (SearchKey::Lt(6), 5));
        assert_eq!(right[1], (SearchKey::Gt(6), 7));
        assert_eq!(right[2], (SearchKey::Lt(8), 7));
        assert_eq!(right[3], (SearchKey::Gt(8), 9));

        let mut node = create_internal_node();

        insert_internal(&mut node, SearchKey::Lt(2), 1).unwrap();
        insert_internal(&mut node, SearchKey::Gt(2), 3).unwrap();

        let right = right_values_internal(&node);
        assert_eq!(right.len(), 2);
        assert_eq!(right[0], (SearchKey::Lt(2), 1));
        assert_eq!(right[1], (SearchKey::Gt(2), 3));
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
                .open("/tmp/test_insert_1.idx")
                .await
                .map_err(|e| Error::IoError(e))
                .ok()?;

            let tree_handle = TreeHandle {
                handle: Arc::new(Mutex::new(f)),
                seekers: Arc::new(Mutex::new(HashMap::new())),
            }
            .start();

            let mut rng = rand::thread_rng();

            // Values to look for for each inserted key
            let mut k_v: Vec<(u64, RecordId)> = Vec::new();

            // Insert a shitton of random keys
            for i in 0..5 {
                let mut k: u64 = rng.gen();

                while k == 0 {
                    k = rng.gen();
                }

                tracing::info!("\n\n\ninserting {}", i);

                let page: u64 = rng.gen();
                let page_idx: u64 = rng.gen();

                let v = RecordId { page, page_idx };
                k_v.push((k, v.clone()));

                tree_handle.send(InsertKey(k, v)).await.ok()?.ok()?;

                tracing::info!("\n\n\nchecking consistency after write {}", i);

                for (k, v) in k_v.iter() {
                    let record = tree_handle.send(GetKey(k.clone())).await.ok()?.ok()?;
                    assert_eq!(record.page, v.page);
                    assert_eq!(record.page_idx, v.page_idx);
                }
            }

            Some(())
        }

        let res = insert_key().await;
        std::fs::remove_file("/tmp/test_insert_1.idx").unwrap();
        assert_eq!(res, Some(()));
    }
}
