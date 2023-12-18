pub mod engine;
pub mod error;
pub mod result;
pub mod rpc;
pub mod types;
pub mod util;

pub mod items {
    include!(concat!(env!("OUT_DIR"), "/toydb.items.rs"));
}

pub mod btree_capnp {
    include!(concat!(env!("OUT_DIR"), "/btree_capnp.rs"));
}

/// The prefix name used to resolve XDG paths for configs and data.
pub const XDG_APP_PREFIX: &str = "toydb";

/// The number of children per node in the B+ tree
pub const ORDER: usize = 4;
