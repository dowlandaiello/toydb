use super::super::super::{error::Error, items::RecordId, ORDER};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RecordIdPointer {
    pub is_empty: bool,
    pub page: u64,
    pub page_idx: u64,
}

impl Default for RecordIdPointer {
    fn default() -> Self {
        Self {
            is_empty: true,
            page: 0,
            page_idx: 0,
        }
    }
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

/// Specifies whether we are looking for values greater than or less than the key.
#[derive(Debug, PartialEq, Clone)]
pub enum SearchKey {
    Lt(u64),
    Gt(u64),
}

#[derive(Debug, PartialEq)]
pub enum SearchTarget {
    LeafNode,
    InternalNode,
}

#[derive(Debug)]
pub enum SearchResult {
    LeafNode(Box<BTreeLeafNode>),
    InternalNode(Box<BTreeInternalNode>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Node {
    InternalNode(Box<BTreeInternalNode>),
    LeafNode(Box<BTreeLeafNode>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KeyPointer {
    Key(u64),
    Pointer(Node),
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct BTreeInternalNode {
    pub keys_pointers: [Option<KeyPointer>; ORDER * 2 + 1],
}

impl BTreeInternalNode {
    pub fn len_keys(&self) -> usize {
        self.keys_pointers
            .iter()
            .filter_map(|kp| {
                kp.as_ref().and_then(|kp| match kp {
                    KeyPointer::Key(_) => Some(true),
                    _ => None,
                })
            })
            .collect::<Vec<bool>>()
            .len()
    }

    pub fn len_child_pointers(&self) -> usize {
        self.keys_pointers
            .iter()
            .filter_map(|kp| {
                kp.as_ref().and_then(|kp| match kp {
                    KeyPointer::Pointer(_) => Some(true),
                    _ => None,
                })
            })
            .collect::<Vec<bool>>()
            .len()
    }

    fn insertion_pos(&self, k: SearchKey) -> Option<usize> {
        let mut pos = None;

        let len = self.len_child_pointers() + self.len_keys();

        // Note: this should be replaced with some kind of binary search

        for i in 0..len {
            if i % 2 == 0 {
                continue;
            }

            let key = if let Some(KeyPointer::Key(k)) = &self.keys_pointers[i] {
                k
            } else {
                continue;
            };

            let peek_f = self.keys_pointers.get(i + 2).and_then(|s| s.as_ref());
            let peek_b = if i >= 2 {
                self.keys_pointers.get(i - 2).and_then(|s| s.as_ref())
            } else {
                None
            };

            tracing::debug!(
                "at key {:?} with peek_f {:?} and peek_b {:?}",
                key,
                peek_f,
                peek_b
            );

            let k_x = match k {
                SearchKey::Gt(x) => x,
                SearchKey::Lt(x) => x,
            };

            match peek_f {
                Some(&KeyPointer::Key(peek)) => {
                    if key <= &k_x && peek > k_x {
                        pos = Some(i);

                        break;
                    }
                }
                Some(&KeyPointer::Pointer(_)) => {}
                None => {
                    if key <= &k_x {
                        pos = Some(i);

                        break;
                    }
                }
            };

            match peek_b {
                Some(&KeyPointer::Key(peek)) => {
                    if key > &k_x && peek <= k_x {
                        pos = Some(i - 1);

                        break;
                    }
                }
                Some(&KeyPointer::Pointer(_)) => {}
                None => {
                    if key >= &k_x {
                        pos = Some(i - 1);

                        break;
                    }
                }
            };
        }

        pos
    }

    fn get(&self, k: SearchKey) -> Option<usize> {
        let mut pos = None;

        let len = self.len_child_pointers() + self.len_keys();

        tracing::debug!("searching for key {:?} in haystack of length {}", k, len);

        // Note: this should be replaced with some kind of binary search

        for i in 0..len {
            if i % 2 == 0 {
                continue;
            }

            let key = if let Some(KeyPointer::Key(k)) = &self.keys_pointers[i] {
                k
            } else {
                continue;
            };

            let peek_f = self.keys_pointers.get(i + 2).and_then(|s| s.as_ref());
            let peek_b = if i >= 2 {
                self.keys_pointers.get(i - 2).and_then(|s| s.as_ref())
            } else {
                None
            };

            tracing::debug!(
                "at key {:?} with peek_f {:?} and peek_b {:?}",
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
                        Some(&KeyPointer::Key(peek)) => {
                            if key <= &x && peek > x {
                                pos = Some(i + 1);
                            }
                        }
                        Some(&KeyPointer::Pointer(_)) => {}
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
                        Some(&KeyPointer::Key(peek)) => {
                            if key > &x && peek <= x {
                                pos = Some(i - 1);

                                break;
                            }
                        }
                        Some(&KeyPointer::Pointer(_)) => {}
                        None => {
                            if key >= &x {
                                pos = Some(i - 1);

                                break;
                            }
                        }
                    }
                }
            }
        }

        pos
    }

    /// Inserts the value at a key such that the key to the left of it is less than the key
    /// and the key to the right of it is greater than the key
    pub fn insert(&mut self, k: SearchKey, v: KeyPointer) -> Result<(), Error> {
        if let SearchKey::Lt(0) = k {
            return Err(Error::InvalidKey);
        }

        if let SearchKey::Gt(0) = k {
            return Err(Error::InvalidKey);
        }

        // If there are no items in the node, we can be inserted anywhere
        if self.len_keys() == 0 {
            match &k {
                SearchKey::Gt(x) => {
                    self.keys_pointers[1] = Some(KeyPointer::Key(x.clone()));
                    self.keys_pointers[2] = Some(v);
                }
                SearchKey::Lt(x) => {
                    self.keys_pointers[0] = Some(v);
                    self.keys_pointers[1] = Some(KeyPointer::Key(x.clone()));
                }
            }

            return Ok(());
        }

        // There are nodes. We should be able to find one we can insert in
        let pos_insert = {
            if self.len_keys() == ORDER {
                return Err(Error::TraversalError);
            }

            self.insertion_pos(k.clone()).ok_or(Error::TraversalError)?
        };
        let mut keys_pointers = self.keys_pointers.to_vec();

        match k {
            SearchKey::Gt(x) => {
                // If we would be inserting outside the end of the keys, we are out
                // of capacity
                if pos_insert + 3 >= keys_pointers.len() {
                    return Err(Error::TraversalError);
                }

                // Leave a spot to the right of the previous key empty
                keys_pointers.insert(pos_insert + 1, Some(v));
                keys_pointers.insert(pos_insert + 1, Some(KeyPointer::Key(x)));
                keys_pointers.resize_with(ORDER * 2 + 1, Default::default);

                (*self).keys_pointers = keys_pointers
                    .try_into()
                    .map_err(|_| Error::TraversalError)?;

                Ok(())
            }
            SearchKey::Lt(x) => {
                // If there is a node already occupying that spot,
                // we are out of capacity
                if keys_pointers[pos_insert + 1].is_some() {
                    return Err(Error::TraversalError);
                }

                keys_pointers.insert(pos_insert, Some(KeyPointer::Key(x)));
                keys_pointers.insert(pos_insert, Some(v));
                keys_pointers.resize_with(ORDER * 2 + 1, Default::default);

                (*self).keys_pointers = keys_pointers
                    .try_into()
                    .map_err(|_| Error::TraversalError)?;

                Ok(())
            }
        }
    }

    pub fn median(&self) -> u64 {
        let keys = self
            .keys_pointers
            .iter()
            .filter_map(|kp| {
                kp.as_ref().and_then(|kp| match kp {
                    KeyPointer::Key(x) => Some(x),
                    _ => None,
                })
            })
            .map(|x| x.clone())
            .collect::<Vec<u64>>();
        keys[keys.len() / 2]
    }

    pub fn left_values(&self) -> Vec<(SearchKey, KeyPointer)> {
        let med = self.median();

        let mut vals = Vec::new();
        let mut k_v = Vec::new();

        let len = self.keys_pointers.len();

        for (i, key) in self.keys_pointers.iter().enumerate() {
            let key = if let Some(KeyPointer::Key(key)) = key {
                key
            } else {
                continue;
            };

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
                    match self.keys_pointers[i as usize]
                        .as_ref()
                        .zip(self.keys_pointers[v_i].as_ref())
                    {
                        Some((KeyPointer::Key(key), val)) => {
                            k_v.push((SearchKey::Gt(key.clone()), val.clone()));
                        }
                        _ => {}
                    }
                }
                SearchKey::Lt(i) => {
                    match self.keys_pointers[i as usize]
                        .as_ref()
                        .zip(self.keys_pointers[v_i].as_ref())
                    {
                        Some((KeyPointer::Key(key), val)) => {
                            k_v.push((SearchKey::Lt(key.clone()), val.clone()));
                        }
                        _ => {}
                    }
                }
            }
        }

        k_v
    }

    pub fn right_values(&self) -> Vec<(SearchKey, KeyPointer)> {
        let med = self.median();

        let mut vals = Vec::new();
        let mut k_v = Vec::new();

        let len = self.keys_pointers.len();

        for (i, key) in self.keys_pointers.iter().enumerate() {
            let key = if let Some(KeyPointer::Key(key)) = key {
                key
            } else {
                continue;
            };

            if *key < med {
                break;
            }

            vals.push((SearchKey::Gt(i as u64), i + 1));
        }

        for (k_i, v_i) in vals {
            match k_i {
                SearchKey::Gt(i) => {
                    match self.keys_pointers[i as usize]
                        .as_ref()
                        .zip(self.keys_pointers[v_i].as_ref())
                    {
                        Some((KeyPointer::Key(key), val)) => {
                            k_v.push((SearchKey::Gt(key.clone()), val.clone()));
                        }
                        _ => {}
                    }
                }
                SearchKey::Lt(i) => {
                    match self.keys_pointers[i as usize]
                        .as_ref()
                        .zip(self.keys_pointers[v_i].as_ref())
                    {
                        Some((KeyPointer::Key(key), val)) => {
                            k_v.push((SearchKey::Lt(key.clone()), val.clone()));
                        }
                        _ => {}
                    }
                }
            }
        }

        k_v
    }

    #[tracing::instrument]
    pub fn follow_node(
        slf: &Box<BTreeInternalNode>,
        key: u64,
        target: SearchTarget,
    ) -> Result<SearchResult, Error> {
        let candidate_pos = if let Some(pos) = slf.get(SearchKey::Gt(key)) {
            pos as usize
        } else {
            if target == SearchTarget::InternalNode {
                return Ok(SearchResult::InternalNode(slf.clone()));
            } else {
                return Err(Error::TraversalError);
            }
        };

        // We are not a leaf node, so look for a leaf node or an internal node that will get us closer
        let node = if let Some(KeyPointer::Pointer(p)) = slf.keys_pointers[candidate_pos].as_ref() {
            p
        } else {
            return Err(Error::TraversalError);
        };

        tracing::debug!("candidate node found {:?}", node);

        match target {
            SearchTarget::LeafNode => match node {
                Node::LeafNode(n) => Ok(SearchResult::LeafNode(n.clone())),
                Node::InternalNode(n) => Self::follow_node(n, key, target),
            },
            SearchTarget::InternalNode => match node {
                Node::LeafNode(_) => Err(Error::TraversalError),
                Node::InternalNode(n) => {
                    Self::follow_node(n, key, target).or(Ok(SearchResult::InternalNode(n.clone())))
                }
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeLeafNode {
    pub keys: [u64; ORDER],
    pub disk_pointers: [RecordIdPointer; ORDER],
}

impl Default for BTreeLeafNode {
    fn default() -> Self {
        Self {
            keys: [0; ORDER],
            disk_pointers: [RecordIdPointer::default(); ORDER],
        }
    }
}

impl BTreeLeafNode {
    pub fn len_disk_pointers(&self) -> usize {
        self.disk_pointers
            .iter()
            .filter(|dp| !dp.is_empty)
            .collect::<Vec<&RecordIdPointer>>()
            .len()
    }

    pub fn len_keys(&self) -> usize {
        self.keys
            .iter()
            .filter(|key| **key != 0)
            .collect::<Vec<&u64>>()
            .len()
    }

    pub fn insert(&mut self, k: u64, v: RecordIdPointer) -> Result<(), Error> {
        if k == 0 {
            return Err(Error::InvalidKey);
        }

        let len_children = self.len_disk_pointers();
        let len_keys = self.len_keys();

        if len_children >= ORDER {
            return Err(Error::TraversalError);
        }

        self.keys[len_keys] = k;
        self.disk_pointers[len_keys] = v;

        Ok(())
    }

    pub fn get(&self, k: u64) -> Option<RecordIdPointer> {
        self.keys
            .iter()
            .zip(self.disk_pointers.iter())
            .find(|(k_curr, _v)| k_curr == &&k)
            .map(|(_, v)| v.clone())
    }
}
