pub mod engine;
pub mod error;
pub mod result;
pub mod rpc;
pub mod types;
pub mod util;

/// The prefix name used to resolve XDG paths for configs and data.
pub const XDG_APP_PREFIX: &str = "toydb";
