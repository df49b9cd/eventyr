//! The store-side codec a [`Persistable`](eventyr_core::schema::Persistable)
//! event is written and read through.
//!
//! Core's `Persistable` names the persisted *identity* of an event —
//! `event_type`, `schema_version`, `payload` — without a serializer.
//! This trait is where serialization happens: each store implements
//! [`SchemaCodec`] for the row shape its backend writes (Postgres's
//! JSONB columns, fjall's blob). One codec per store, never one per
//! event: the point of §3's placement rule is that "render this event
//! into a store record" is the store's answer, not the domain's.
//!
//! The two consumers a store shares: the write path encodes an event
//! into a record before append, and the read path decodes it into a
//! [`RawEvent`]/`UpcastingSource` feed or, with a matching
//! [`DecodeEvent`], straight back into the typed event for a projection.

use eventyr_core::error::StoreError;
use eventyr_core::schema::Persistable;
use eventyr_core::upcast::RawEvent;

/// How one store renders `Persistable` events to and from its rows.
pub trait SchemaCodec {
    /// The persisted record this store writes and reads.
    type Record;
    /// The event type this codec handles.
    type Event: Persistable;

    /// Render `event` into the store's record form. Pure, total within
    /// the serializer's rules: a failure is unserializable data, and
    /// the store surfaces it as a [`StoreError`] — never silence.
    fn encode(event: &Self::Event) -> Result<Self::Record, StoreError>;

    /// The identity a stored record carries: the `(type, version,
    /// bytes)` the upcast registry's ladder walks. Pure — facts about
    /// the record, before any decode.
    fn identify(record: &Self::Record) -> RawEvent;
}

/// A codec that decodes its own records back into the typed event.
///
/// Implemented on the codec, not the event: the trait extends
/// [`SchemaCodec`] with the return leg. Implemented by codecs whose
/// record is self-describing (Postgres's JSONB columns — type, version,
/// and payload in one row — or fjall's serialized blob); a raw byte
/// stream without framing cannot decode on its own.
///
/// The decode answers "the event as *stored*" — at the record's stored
/// version. Upcasting a renamed or older shape is the registry's job,
/// not this trait's: a read path that might upcast calls
/// [`identify`](SchemaCodec::identify) and walks the ladder, a read
/// path that knows the record is current calls `decode` and skips it.
pub trait DecodeEvent: SchemaCodec {
    /// Decode a persisted record back into the typed event. A payload
    /// that fails to decode is a corrupt row, surfaced as a
    /// [`StoreError`] — never unwrapped, never guessed.
    fn decode(record: &Self::Record) -> Result<Self::Event, StoreError>;
}
