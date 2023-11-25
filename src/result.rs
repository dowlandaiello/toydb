use super::error::Error;
use std::result::Result as StdResult;

/// A result using a ToyDB error.
pub type Result<T> = StdResult<T, Error>;
