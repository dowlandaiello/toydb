use std::{
    collections::hash_map::DefaultHasher,
    fmt::Display,
    hash::{Hash, Hasher},
};

/// Gets the unique hash key for the database name and relation name.
pub fn key_for_rel_db_attr(
    db_name: impl Display,
    rel_name: impl Display,
    attr_name: impl Display,
) -> u64 {
    let mut hasher = DefaultHasher::default();
    format!("{}:{}:{}", db_name, rel_name, attr_name).hash(&mut hasher);
    hasher.finish()
}

/// Gets the unique hash key for any composite key
pub fn key_for_composite_key(vals: impl AsRef<[Vec<u8>]>) -> u64 {
    let mut hasher = DefaultHasher::default();
    let buff = vals.as_ref().concat();
    buff.hash(&mut hasher);
    hasher.finish()
}
