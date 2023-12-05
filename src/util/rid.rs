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
