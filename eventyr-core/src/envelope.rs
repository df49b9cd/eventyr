//! The event envelope and the metadata that travels with it.

use alloc::string::String;

use crate::vocabulary::{Sequence, StreamId, Version};

/// A domain event together with its storage position and metadata.
///
/// The envelope separates storage concerns (sequence, stream, version,
/// metadata) from the domain event itself, keeping domain enums clean and
/// serde payloads stable.
#[derive(Clone, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EventEnvelope<E> {
    /// The global, store-assigned, monotonic position.
    pub sequence: Sequence,
    /// The stream this event belongs to.
    pub stream_id: StreamId,
    /// The 1-based position within the stream.
    pub version: Version,
    /// The domain event.
    pub event: E,
    /// Correlation and causation metadata.
    pub metadata: Metadata,
}

/// Correlation and causation metadata.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Metadata {
    /// The id of the command (or event) that caused this event.
    pub causation_id: Option<String>,
    /// The id grouping one interaction's events across aggregates.
    pub correlation_id: Option<String>,
    /// When the event was committed. Only present behind the `time`
    /// feature.
    #[cfg(feature = "time")]
    pub timestamp: Option<time::OffsetDateTime>,
}

/// An event the machine asks the driver to append.
///
/// The store assigns sequence, stream id and version; the committed
/// [`EventEnvelope`]s come back in
/// [`Appended`](crate::write::WriteInput::Appended).
#[derive(Clone, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NewEvent<E> {
    /// The domain event.
    pub event: E,
    /// Correlation and causation metadata.
    pub metadata: Metadata,
}

impl<E> NewEvent<E> {
    /// A new event with empty metadata.
    pub fn new(event: E) -> Self {
        Self {
            event,
            metadata: Metadata::default(),
        }
    }
}
