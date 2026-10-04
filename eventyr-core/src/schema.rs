//! The persistence seam for an event: what an [`Aggregate`]-typed event
//! *is* to a store — its name, its schema version, and its serialized
//! payload, before the registry's upcaster ever sees it (roadmap 0.6.2).
//!
//! Up to 0.5 the answer was implicit in the Postgres driver: the event
//! enum serialized itself to JSON, the variant's
//! [`EventName`](crate::event_name::EventName) went in the `event_type`
//! column, and `schema_version` was pinned at 1 by
//! [`RawEvent::v1`](crate::upcast::RawEvent::v1). That works — but it
//! makes "this event, as stored" a side effect of the event type, and
//! leaves no place to say *this* variant is stored as `AmountV2`
//! because it was renamed, or *this* payload has a column computed from
//! two fields. This module names that seam.
//!
//! The split is the one §3's placement rule mandates: sequence /
//! identity / version / error are core vocabulary (what a machine
//! transitions on), while *how a payload becomes bytes* is the store's
//! — so the codec that does the serialization lives in
//! `eventyr-store`, and only the shape it fills is here. A type
//! implements [`Persistable`] to declare its stored identity; a store
//! holds a [`SchemaCodec`](eventyr_store::schema::SchemaCodec) to render
//! it into its rows.

use alloc::string::String;
use alloc::vec::Vec;


use crate::version_registry::EventSchemaVersion;

/// An event that declares how it is stored.
///
/// One `Persistable` impl per event type — not per variant, not per
/// field — is the load-bearing answer to the rename/event-version
/// problem every ES library solves differently (`SerdeReduce` in
/// `cqrs-es`, the `events![]` macro in `sourcery`, `Schema` in `esrs`).
/// In Eventyr the name and version are the *storage key*, the payload
/// bytes the *storage column*: the upcast registry's ladder (0.5.1)
/// and every store's read path select by `(event_type, schema_version)`
/// and feed `payload` to the decoder.
///
/// Interaction with upcasting: a store *reads* through a codec to
/// produce either the typed event for a projection, or the raw row
/// ([`RawEvent`]) for an upcasting source — the same bytes looked at
/// twice, never twice-stored.
pub trait Persistable {
    /// The event type's stored name — stable across renames.
    fn event_type(&self) -> String;

    /// The schema version this write is stored at. Right side of
    /// the ladder: every rung registered for the type starts below
    /// this version, so the read path walking up from it reaches
    /// the current shape.
    fn schema_version(&self) -> EventSchemaVersion {
        EventSchemaVersion::V1
    }

    /// The serialized payload: the bytes the store keeps as the
    /// event's body. Serialization is the store's job — a
    /// `Persistable` fills the value the codec encodes.
    fn payload(&self) -> Result<Vec<u8>, crate::error::StoreError>;
}
