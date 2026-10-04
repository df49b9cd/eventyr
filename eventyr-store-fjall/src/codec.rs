//! The fjall codec: persist and re-read a
//! [`Persistable`](eventyr_core::schema::Persistable) event as a keyspace
//! row.
//!
//! The fjall `streams` partition stores one JSON blob per event. This
//! codec is where that blob's two halves live: [`SchemaCodec::encode`]
//! writes the event, [`SchemaCodec::identify`] reads the `(type,
//! version, bytes)` the registry ladder needs from it, and
//! [`DecodeEvent::decode`] gives the typed event back at its stored
//! version — the read path that is already current skips the ladder.

use eventyr_core::error::StoreError;
use eventyr_core::schema::Persistable;
use eventyr_core::upcast::RawEvent;
use eventyr_core::version_registry::EventSchemaVersion;
use eventyr_store::schema::{DecodeEvent, SchemaCodec};

use crate::FjallStoreError;

/// The fjall codec: one row per serialized event.
pub struct FjallCodec<E> {
    _event: std::marker::PhantomData<fn() -> E>,
}

impl<E> FjallCodec<E> {
    /// A codec over the event type `E`.
    pub fn new() -> Self {
        Self {
            _event: std::marker::PhantomData,
        }
    }
}

impl<E> Default for FjallCodec<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> SchemaCodec for FjallCodec<E>
where
    E: Persistable + serde::Serialize,
{
    /// The persisted record: the event's JSON bytes, exactly what a
    /// `streams` keyspace value carries.
    type Record = Vec<u8>;
    type Event = E;

    fn encode(event: &Self::Event) -> Result<Self::Record, StoreError> {
        serde_json::to_vec(event).map_err(|error| {
            StoreError::Other(std::sync::Arc::new(FjallStoreError::CorruptRow(format!(
                "the event does not serialize: {error}"
            ))))
        })
    }

    fn identify(record: &Self::Record) -> RawEvent {
        // The blob is the row in full: deserialize just to learn its
        // declared identity, which is what the registry walks by.
        // The fjall store predates the 0.5.1 registry, so the stored
        // version is always V1 until the schema gains it.
        let value: serde_json::Value = serde_json::from_slice(record).expect(
            "a fjall record the codec wrote decodes back; rows it did not write are the caller's \
             error path, not this one",
        );
        RawEvent {
            event_type: value
                .get("event_type")
                .and_then(|v| v.as_str().map(String::from))
                .unwrap_or_default(),
            schema_version: EventSchemaVersion::V1,
            payload: record.clone(),
        }
    }
}

impl<E> DecodeEvent for FjallCodec<E>
where
    E: Persistable + serde::Serialize + serde::de::DeserializeOwned,
{
    /// Decode the row bytes back to the typed event at its *stored*
    /// version. A row that does not decode is a corrupt row, surfaced
    /// through the store's own error type — never unwrapped.
    fn decode(record: &Self::Record) -> Result<Self::Event, StoreError> {
        serde_json::from_slice(record).map_err(|error| {
            StoreError::Other(std::sync::Arc::new(FjallStoreError::CorruptRow(format!(
                "the stored payload does not decode: {error}"
            ))))
        })
    }
}
