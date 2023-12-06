use super::super::items::Tuple;
use actix::Message;

/// A message requesting the next tuple from the iterator.
#[derive(Message, Debug)]
#[rtype(result = "Option<Tuple>")]
pub struct Next;
