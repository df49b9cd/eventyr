//! The SQLite codec: persist and re-read a
//! [`Persistable`](eventyr_core::schema::Persistable) event as an
//! `events`-table row.
//!
//! SQLite keeps the same columns the Postgres schema does (`event_type`,
//! `payload` as JSON text); the codec makes the mapping explicit rather
//! than embedded in [`append_within`] and [`stream`] — the
//! `(type, version, payload)` identity the registry walks and the typed
//! event the read path needs.

use eventyr_core::error::StoreError;
use eventyr_core::schema::Persistable;
use eventyr_core::upcast::RawEvent;
use eventyr_core::version_registry::EventSchemaVersion;
use eventyr_store::schema::{DecodeEvent, SchemaCodec};

use crate::SqliteStoreError;

/// The SQLite codec: one `events` row per event, `payload` as JSON text.
pub struct SqliteCodec<E> {
    _event: std::marker::PhantomData<fn() -> E>,
}

impl<E> SqliteCodec<E> {
    /// A codec over the event type `E`.
    pub fn new() -> Self {
        Self {
            _event: std::marker::PhantomData,
        }
    }
}

impl<E> Default for SqliteCodec<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> SchemaCodec for SqliteCodec<E>
where
    E: Persistable + serde::Serialize,
{
    /// The persisted record: `(event_type, schema_version, payload)` in
    /// the clear. The schema keeps these columns beside the row's own
    /// stream/version/sequence columns.
    type Record = (String, EventSchemaVersion, String);
    type Event = E;

    fn encode(event: &Self::Event) -> Result<Self::Record, StoreError> {
        let payload = serde_json::to_string(event).map_err(|error| {
            StoreError::from(SqliteStoreError::CorruptRow(format!(
                "the event does not serialize: {error}"
            )))
        })?;
        Ok((event.event_type(), event.schema_version(), payload))
    }

    fn identify(record: &Self::Record) -> RawEvent {
        let (event_type, schema_version, payload) = record;
        RawEvent {
            event_type: event_type.clone(),
            schema_version: *schema_version,
            payload: payload.clone().into_bytes(),
        }
    }
}

impl<E> DecodeEvent for SqliteCodec<E>
where
    E: Persistable + serde::Serialize + serde::de::DeserializeOwned,
{
    /// Decode the payload column back to the typed event at its *stored*
    /// version. A payload that does not decode is a corrupt row,
    /// surfaced through the store's own error type — never unwrapped.
    fn decode(record: &Self::Record) -> Result<Self::Event, StoreError> {
        let (_event_type, _schema_version, payload) = record;
        serde_json::from_str(payload).map_err(|error| {
            StoreError::from(SqliteStoreError::CorruptRow(format!(
                "the stored payload does not decode: {error}"
            )))
        })
    }
}
