#[derive(Default, Debug, Clone)]
pub struct RecordId {
    pub page: u64,
    pub page_idx: u64,
}

#[derive(Default, Debug, Clone)]
pub struct RecordIdPointer {
    pub is_empty: bool,
    pub page: u64,
    pub page_idx: u64,
}

#[derive(Default, Debug, Clone)]
pub struct BTreeInternalNode {
    pub is_leaf_node: bool,
    pub keys_pointers: Vec<u64>,
}

#[derive(Default, Debug, Clone)]
pub struct BTreeLeafNode {
    pub is_leaf_node: bool,
    pub keys: Vec<u64>,
    pub disk_pointers: Vec<RecordIdPointer>,
}

#[derive(Default, Debug, Clone)]
pub struct Record {
    pub size: u64,
    pub data: Vec<u8>,
}

#[derive(Default, Debug, Clone)]
pub struct Tuple {
    pub rel_name: String,
    pub elements: Vec<Vec<u8>>,
}

#[derive(Default, Debug, Clone)]
pub struct Page {
    pub space_used: u64,
    pub data: Vec<Record>,
}
