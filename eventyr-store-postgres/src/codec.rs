//! The Postgres codec: render a [`Persistable`](eventyr_core::schema::Persistable)
//! event into the row the events table stores.
//!
//! The persistence seam from roadmap 0.6.2: an event type declares
//! *what it is* to the store via [`Persistable`]; this codec is the
//! store side of the bargain, turning that identity into the
//! `(event_type, payload)` JSONB columns `append_events` writes. A
//! read goes the other way: the row's columns become the
//! [`RawEvent`] the [`UpcasterRegistry`](eventyr_projection::registry::UpcasterRegistry)
//! walks, or — through [`DecodeEvent`] — straight back to the typed
//! event for a current-shape read.

use eventyr_core::error::StoreError;
use eventyr_core::schema::Persistable;
use eventyr_core::upcast::RawEvent;
use eventyr_core::version_registry::EventSchemaVersion;
use eventyr_store::schema::{DecodeEvent, SchemaCodec};

use crate::PgStoreError;

/// The row shape the Postgres events table holds: the stored type name,
/// the schema version the payload was written at, and the JSONB body.
/// The codec is generic over the event type — one impl per codec shape,
/// instantiated per `Persistable` in a store's read/write paths.
pub struct JsonCodec<E> {
    _event: std::marker::PhantomData<fn() -> E>,
}

impl<E> JsonCodec<E> {
    /// A codec over the event type `E`.
    pub fn new() -> Self {
        Self {
            _event: std::marker::PhantomData,
        }
    }
}

impl<E> Default for JsonCodec<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> SchemaCodec for JsonCodec<E>
where
    E: Persistable + serde::Serialize,
{
    /// The stored record: name and version in the clear, the payload
    /// as JSONB. The codec decides nothing beyond the serializer —
    /// `Persistable` owns the identity.
    type Record = (String, EventSchemaVersion, serde_json::Value);
    type Event = E;

    fn encode(event: &Self::Event) -> Result<Self::Record, StoreError> {
        // The event says what it is; the codec renders it. A writer
        // with a custom serializer swaps in its own `SchemaCodec`,
        // never a new `Persistable`.
        let payload = serde_json::to_value(event).map_err(|error| {
            StoreError::other(format!("the event does not serialize: {error}"))
        })?;
        Ok((event.event_type(), event.schema_version(), payload))
    }

    fn identify(record: &Self::Record) -> RawEvent {
        let (event_type, schema_version, payload) = record;
        RawEvent {
            event_type: event_type.clone(),
            schema_version: *schema_version,
            payload: serde_json::to_vec(payload)
                .expect("a stored JSONB payload re-encodes to JSON"),
        }
    }
}

impl<E> DecodeEvent for JsonCodec<E>
where
    E: Persistable + serde::Serialize + serde::de::DeserializeOwned,
{
    /// Decode the record back to the typed event at its *stored*
    /// version. A payload that does not decode is a corrupt row,
    /// surfaced through the store's own error type — never unwrapped,
    /// never silently skipped.
    fn decode(record: &Self::Record) -> Result<Self::Event, StoreError> {
        let (_event_type, _schema_version, payload) = record;
        serde_json::from_value(payload.clone()).map_err(|error| {
            StoreError::from(PgStoreError::CorruptRow(format!(
                "the stored payload does not decode: {error}"
            )))
        })
    }
}
