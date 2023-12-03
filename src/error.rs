use actix::MailboxError;
use prost::{DecodeError, EncodeError};
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
    RecordNotFound,
    TraversalError,
    DecodeError(DecodeError),
    EncodeError(EncodeError),
    MiscDecodeError,
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
            Self::RecordNotFound => write!(f, "the record was not found in the page"),
            Self::TraversalError => write!(f, "failed to correctly traverse a tree structure"),
            Self::DecodeError(e) => write!(f, "failed to decode a message with protobuf: {:?}", e),
            Self::EncodeError(e) => write!(f, "failed to encode a message with protobuf: {:?}", e),
            Self::MiscDecodeError => write!(f, "failed to parse the message"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::XdgError(e) => Some(e),
            Self::IoError(e) => Some(e),
            Self::MailboxError(e) => Some(e),
            Self::DecodeError(e) => Some(e),
            Self::EncodeError(e) => Some(e),
            Self::PageOutOfBounds
            | Self::MutexError
            | Self::ConversionError
            | Self::RecordNotFound
            | Self::TraversalError
            | Self::MiscDecodeError => None,
        }
    }
}
