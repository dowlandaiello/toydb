use super::super::{error::Error, XDG_APP_PREFIX};
use std::{
    fmt::Display,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};
use tokio::fs::File;
use xdg::BaseDirectories;

/// The name of the system catalogue
pub const CATALOGUE_TABLE_NAME: &str = "system_catalogue";

/// Obtains an absolute path from a relative path inside the XDG data dir for toydb.
pub fn path_in_data_dir(relative_path: impl AsRef<Path>) -> Result<PathBuf, Error> {
    BaseDirectories::with_prefix(XDG_APP_PREFIX)
        .map_err(|e| Error::XdgError(e))?
        .place_data_file(relative_path)
        .map_err(|e| Error::IoError(e))
}

/// Obtains a relative path to a database file with the database name `name`.
pub fn db_file_path_with_name(name: impl Display) -> Result<PathBuf, Error> {
    path_in_data_dir(format!("{}.db", name))
}

/// Obtains a database name from a fully qualified path.
pub fn db_name_from_file_path(path: impl AsRef<Path>) -> Option<String> {
    path.as_ref()
        .file_stem()
        .and_then(|s| s.to_str().map(|s| s.to_owned()))
}

/// Obtains an index name from a fully qualified path
pub fn index_name_from_file_path(path: impl AsRef<Path>) -> Option<String> {
    db_name_from_file_path(path)
}

/// Obtains a relative path to an index file with the index name `name`.
pub fn index_file_path_with_name(name: impl Display) -> Result<PathBuf, Error> {
    path_in_data_dir(format!("{}.idx", name))
}

pub fn index_file_path_with_name_attr(
    name: impl Display,
    attr: impl Display,
) -> Result<PathBuf, Error> {
    path_in_data_dir(format!("{}.idx", index_name_with_name_attr(name, attr)))
}

pub fn index_name_with_name_attr(name: impl Display, attr: impl Display) -> String {
    format!("{}_{}", name, attr)
}

/// Checks whether or not the file is empty.
pub async fn file_is_empty(f: &File) -> bool {
    f.metadata()
        .await
        .map(|meta| meta.size() == 0)
        .unwrap_or(true)
}
