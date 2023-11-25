use std::{error::Error as StdError, fmt};
use tokio::io::Error as IoError;
use xdg::BaseDirectoriesError;

#[derive(Debug)]
pub enum Error {
    XdgError(BaseDirectoriesError),
    IoError(IoError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::XdgError(e) => write!(
                f,
                "encountered an error while resolving base XDG paths: {:?}",
                e
            ),
            Self::IoError(e) => write!(f, "encountered an IO error: {:?}", e),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::XdgError(e) => Some(e),
            Self::IoError(e) => Some(e),
        }
    }
}
