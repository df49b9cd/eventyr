//! Upcasting: transforming historical event shapes into the current one.

use alloc::string::String;
use alloc::vec::Vec;

use crate::error::UpcastError;

/// A stored event before upcasting: its type name and raw payload.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RawEvent {
    /// The stored event type name.
    pub event_type: String,
    /// The stored payload, as raw bytes.
    pub payload: Vec<u8>,
}

/// Transforms one historical event shape into the current one.
///
/// The chain (0.2, with the Postgres store) selects upcasters by event
/// type; a selected upcaster that cannot parse its payload is an
/// [`Err`](UpcastError) — never a silent drop.
pub trait Upcaster<E>: Send + Sync {
    /// Transform `raw` into the current event shape.
    fn upcast(&self, raw: RawEvent) -> Result<E, UpcastError>;
}
