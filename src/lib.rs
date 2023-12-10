pub mod engine;
pub mod error;
pub mod owned_items;
pub mod result;
pub mod rpc;
pub mod types;
pub mod util;

pub mod items_capnp {
    include!(concat!(env!("OUT_DIR"), "/items_capnp.rs"));
}

/// The prefix name used to resolve XDG paths for configs and data.
pub const XDG_APP_PREFIX: &str = "toydb";
