use super::super::items::Tuple;
use actix::Message;
use std::{future::Future, pin::Pin};

/// A message requesting the next tuple from the iterator.
#[derive(Message, Debug)]
#[rtype(result = "Option<Tuple>")]
pub struct Next;

pub trait Iterator {
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Option<Tuple>>>>;
}
