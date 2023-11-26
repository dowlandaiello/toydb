use actix::MailboxError;
use std::{error::Error as StdError, fmt};
use tokio::io::Error as IoError;
use xdg::BaseDirectoriesError;

#[derive(Debug)]
pub enum Error {
    XdgError(BaseDirectoriesError),
    IoError(IoError),
    MailboxError(MailboxError),
    PageOutOfBounds,
    MutexError,
    ConversionError,
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
            Self::MailboxError(e) => write!(f, "encountered an actor mailbox error: {:?}", e),
            Self::PageOutOfBounds => write!(f, "the record is larger than a page"),
            Self::MutexError => write!(f, "encountered a mutex error"),
            Self::ConversionError => write!(f, "encountered a type conversion error"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::XdgError(e) => Some(e),
            Self::IoError(e) => Some(e),
            Self::MailboxError(e) => Some(e),
            Self::PageOutOfBounds | Self::MutexError => None,
            Self::ConversionError => None,
        }
    }
}
