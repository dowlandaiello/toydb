use super::{error::Error, ORDER};
use std::mem;

#[derive(Default, Debug, Clone, Copy)]
pub struct RecordIdPointer {
    pub is_empty: bool,
    pub page: u64,
    pub page_idx: u64,
}

impl RecordIdPointer {
    pub const fn encoded_len() -> usize {
        mem::size_of::<bool>() + mem::size_of::<u64>() + mem::size_of::<u64>()
    }

    pub fn encode_length_delimited_to_vec(&self) -> Vec<u8> {
        let is_empty_bytes = [self.is_empty as u8];
        let page_bytes = self.page.to_le_bytes();
        let page_idx_bytes = self.page_idx.to_le_bytes();

        let mut buff = Vec::new();
        buff.extend_from_slice(&is_empty_bytes);
        buff.extend_from_slice(&page_bytes);
        buff.extend_from_slice(&page_idx_bytes);

        buff
    }

    pub fn decode_length_delimited(b: &[u8]) -> Result<Self, Error> {
        if b.len() < Self::encoded_len() {
            return Err(Error::DecodeError);
        }

        let is_empty = b[0] != 0;
        let page_bytes: [u8; mem::size_of::<u64>()] = (&b[1..(mem::size_of::<u64>() + 1)])
            .try_into()
            .map_err(|_| Error::DecodeError)?;
        let page_idx_bytes: [u8; mem::size_of::<u64>()] = (&b
            [(mem::size_of::<u64>() + 1)..(mem::size_of::<u64>() + mem::size_of::<u64>() + 1)])
            .try_into()
            .map_err(|_| Error::DecodeError)?;

        let page = u64::from_le_bytes(page_bytes);
        let page_idx = u64::from_le_bytes(page_idx_bytes);

        Ok(Self {
            is_empty,
            page,
            page_idx,
        })
    }
}

#[derive(Default, Debug, Clone)]
pub struct BTreeInternalNode {
    pub is_leaf_node: bool,
    pub keys_pointers: [u64; ORDER * 2 + 1],
}

impl BTreeInternalNode {
    pub const fn encoded_len() -> usize {
        mem::size_of::<bool>() + (mem::size_of::<u64>() * (ORDER * 2 + 1))
    }

    pub fn encode_length_delimited_to_vec(&self) -> Vec<u8> {
        let is_leaf_node_bytes = [self.is_leaf_node as u8];
        let mut keys_pointers_bytes = Vec::new();

        for k_p in self.keys_pointers.iter() {
            keys_pointers_bytes.extend_from_slice(&k_p.to_le_bytes());
        }

        let mut buff = Vec::new();
        buff.extend_from_slice(&is_leaf_node_bytes);
        buff.append(&mut keys_pointers_bytes);

        buff
    }

    pub fn decode_length_delimited(b: &[u8]) -> Result<Self, Error> {
        let is_leaf_node = b[0] != 0;
        let mut keys_pointers: [u64; ORDER * 2 + 1] = [0; ORDER * 2 + 1];

        for (j, i) in (1..(mem::size_of::<u64>() * (ORDER * 2 + 1) + 1))
            .step_by(mem::size_of::<u64>())
            .enumerate()
        {
            let k_p_bytes: [u8; mem::size_of::<u64>()] = (&b[i..(i + mem::size_of::<u64>())])
                .try_into()
                .map_err(|_| Error::DecodeError)?;
            let k_p = u64::from_le_bytes(k_p_bytes);

            keys_pointers[j] = k_p;
        }

        Ok(Self {
            is_leaf_node,
            keys_pointers,
        })
    }
}

#[derive(Default, Debug, Clone)]
pub struct BTreeLeafNode {
    pub is_leaf_node: bool,
    pub keys: [u64; ORDER],
    pub disk_pointers: [RecordIdPointer; ORDER],
}

impl BTreeLeafNode {
    pub const fn encoded_len() -> usize {
        mem::size_of::<bool>()
            + mem::size_of::<u64>() * ORDER
            + RecordIdPointer::encoded_len() * ORDER
    }

    pub fn encode_length_delimited_to_vec(&self) -> Vec<u8> {
        let is_leaf_node_bytes = [self.is_leaf_node as u8];
        let mut keys_bytes = Vec::new();

        for k in self.keys.iter() {
            keys_bytes.extend_from_slice(&k.to_le_bytes());
        }

        let mut disk_pointers_bytes = Vec::new();

        for d_p in self.disk_pointers.iter() {
            disk_pointers_bytes.append(&mut d_p.encode_length_delimited_to_vec());
        }

        let mut buff = Vec::new();
        buff.extend_from_slice(&is_leaf_node_bytes);
        buff.append(&mut keys_bytes);
        buff.append(&mut disk_pointers_bytes);

        buff
    }

    pub fn decode_length_delimited(b: &[u8]) -> Result<Self, Error> {
        let is_leaf_node = b[0] != 0;
        let mut keys: [u64; ORDER] = [0; ORDER];

        for (j, i) in (1..(mem::size_of::<u64>() * ORDER + 1))
            .step_by(mem::size_of::<u64>())
            .enumerate()
        {
            let k_bytes: [u8; mem::size_of::<u64>()] = (&b[i..(i + mem::size_of::<u64>())])
                .try_into()
                .map_err(|_| Error::DecodeError)?;
            let k = u64::from_le_bytes(k_bytes);

            keys[j] = k;
        }

        let mut disk_pointers: [RecordIdPointer; ORDER] = [RecordIdPointer::default(); ORDER];

        for (j, i) in ((mem::size_of::<u64>() * ORDER + 1)..b.len())
            .step_by(RecordIdPointer::encoded_len())
            .enumerate()
        {
            let ridp_bytes: [u8; RecordIdPointer::encoded_len()] = (&b
                [i..(i + RecordIdPointer::encoded_len())])
                .try_into()
                .map_err(|_| Error::DecodeError)?;
            let ridp = RecordIdPointer::decode_length_delimited(&ridp_bytes)?;
            disk_pointers[i] = ridp;
        }

        Ok(Self {
            is_leaf_node,
            keys,
            disk_pointers,
        })
    }
}
