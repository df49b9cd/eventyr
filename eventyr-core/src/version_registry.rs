//! The upcasting vocabulary: [`EventSchemaVersion`] (the storage-level
//! schema version carried by [`RawEvent`](crate::upcast::RawEvent)) and
//! [`VersionedRaw`] (a raw event tagged with it).
//!
//! The ladder machinery — `VersionUpcaster`, `UpcasterRegistry`,
//! `VersionRung`, and the `RegistryError` shape validation — lives in
//! `eventyr_projection::registry`, where the read path composes it
//! (DESIGN §3: read-side glue is store-side). The umbrella crate
//! re-exports it under `eventyr::projection`, so the pre-0.5.1
//! `eventyr::prelude` surface resolves there too.

use alloc::string::String;
use alloc::vec::Vec;

/// The version of a stored event's schema: 1-based, where the current
/// shape is one past the highest registered version.
///
/// Upcasters form a per-type ladder: each registered rung lifts one
/// event type one step, from `version` to `version + 1`. A payload
/// already at the current version needs no rung.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct EventSchemaVersion(u32);

impl EventSchemaVersion {
    /// The first version of an event's schema.
    pub const V1: Self = Self(1);

    /// Construct a version from its raw number.
    pub const fn new(version: u32) -> Self {
        Self(version)
    }

    /// The raw version number.
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl core::fmt::Display for EventSchemaVersion {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A raw event tagged with the schema version it was stored at.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct VersionedRaw {
    /// The stored event type name.
    pub event_type: String,
    /// The schema version `payload` was stored at.
    pub version: EventSchemaVersion,
    /// The stored payload, as raw bytes.
    pub payload: Vec<u8>,
}
