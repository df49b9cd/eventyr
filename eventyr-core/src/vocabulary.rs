//! The protocol vocabulary: newtypes for the three position concepts and
//! the optimistic-concurrency expectation.
//!
//! One representation per concept, everywhere — trait signatures, machine
//! actions, and store schemas all speak these types.

use alloc::format;
use alloc::string::String;
use core::fmt;

use crate::aggregate::Aggregate;

/// A stream's identity: `"{Aggregate::NAME}-{id}"`.
///
/// The single place the id→stream mapping exists is
/// [`for_aggregate`](StreamId::for_aggregate).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct StreamId(String);

impl StreamId {
    /// The stream id for one instance of aggregate `A`: `"{NAME}-{id}"`.
    pub fn for_aggregate<A: Aggregate>(id: &A::Id) -> Self {
        Self(format!("{}-{}", A::NAME, id))
    }

    /// The stream id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for StreamId {
    fn from(s: &str) -> Self {
        Self(String::from(s))
    }
}

impl From<String> for StreamId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// The position of an event within its stream. 1-based: the first event of
/// a stream is at `Version(1)`.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Version(u64);

impl Version {
    /// The version of an empty (or absent) stream: before any event.
    ///
    /// As an exclusive lower bound it means "from the beginning" — the
    /// stream-side twin of [`Sequence::START`].
    pub const EMPTY: Self = Self(0);

    /// Construct a version from its raw position.
    pub const fn new(position: u64) -> Self {
        Self(position)
    }

    /// The raw position.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A position in the global, store-assigned, monotonic event sequence.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Sequence(u64);

impl Sequence {
    /// Before the first event; as an exclusive lower bound, "read
    /// everything" — the global-side twin of [`Version::EMPTY`].
    pub const START: Self = Self(0);

    /// Construct a sequence from its raw position.
    pub const fn new(position: u64) -> Self {
        Self(position)
    }

    /// The raw position.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Sequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The optimistic-concurrency expectation for an append.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum ExpectedVersion {
    /// No check: append regardless of the stream's current version.
    Any,
    /// The stream must currently be at this version.
    Exact(Version),
    /// The stream must not exist yet.
    Empty,
}

impl ExpectedVersion {
    /// The expectation for an append on top of a fold that reached
    /// `version`: [`Empty`](ExpectedVersion::Empty) when it folded
    /// nothing ([`Version::EMPTY`]), [`Exact`](ExpectedVersion::Exact)
    /// otherwise. The one place the write, batch, and boundary paths
    /// turn a folded version into a guard.
    pub const fn after(version: Version) -> Self {
        if version.as_u64() == Version::EMPTY.as_u64() {
            Self::Empty
        } else {
            Self::Exact(version)
        }
    }
}
