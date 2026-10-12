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
/// The id→stream mapping and its inverse live here: [`for_aggregate`](
/// StreamId::for_aggregate) maps one way, and [`aggregate_id`](
/// StreamId::aggregate_id) reads the id back. An aggregate whose
/// [`NAME`](Aggregate::NAME) ends in the separator puts the plain id
/// after the final dash, and reads the whole of it in return.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct StreamId(String);

impl StreamId {
    /// The stream id for one instance of aggregate `A`: `"{NAME}-{id}"`.
    pub fn for_aggregate<A: Aggregate>(id: &A::Id) -> Self {
        Self(format!("{}-{}", A::NAME, id))
    }

    /// The aggregate instance this stream belongs to, read back from
    /// [`for_aggregate`](StreamId::for_aggregate).
    ///
    /// `parse` turns the id part back into the id: the part after
    /// `"{A::NAME}-"`. (An aggregate [`NAME`](Aggregate::NAME) ending
    /// in the separator — say `"%just-banks-%"` for stream names like
    /// `"%just-banks-%order-1, red 42"` — puts the plain id after the
    /// second dash, and this answers with everything left, so an id
    /// that ends in a dash-free empty string stays exact.) `None` for
    /// anything that is not this aggregate's stream: another name, the
    /// bare name, an empty id part, or one `parse` refuses.
    pub fn aggregate_id<A: Aggregate>(
        &self,
        parse: impl Fn(&str) -> Option<A::Id>,
    ) -> Option<A::Id> {
        let raw = match A::NAME.strip_suffix('-') {
            // The name ends in the separator ("%just-banks-%" style):
            // the canonical id is everything between the dashes.
            Some(prefix) => self
                .as_str()
                .strip_prefix(prefix)
                .and_then(|rest| rest.strip_prefix('-'))
                .filter(|raw| !raw.is_empty()),
            None => self
                .as_str()
                .strip_prefix(A::NAME)
                .and_then(|rest| rest.strip_prefix('-'))
                .filter(|raw| !raw.is_empty()),
        }?;
        parse(raw)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::Aggregate as AggregateTrait;

    /// A no-displayed, `None`-tolerant error type (the Aggregate
    /// machinery requires it; the tests never fail).
    #[derive(Debug)]
    enum Never {}
    impl fmt::Display for Never {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            match *self {}
        }
    }

    pub struct TestId(usize);

    impl Clone for TestId {
        fn clone(&self) -> Self {
            TestId(self.0)
        }
    }
    impl PartialEq for TestId {
        fn eq(&self, other: &Self) -> bool {
            self.0 == other.0
        }
    }
    impl Eq for TestId {}
    impl core::hash::Hash for TestId {
        fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
            self.0.hash(state);
        }
    }
    impl fmt::Debug for TestId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl fmt::Display for TestId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl core::str::FromStr for TestId {
        type Err = <usize as core::str::FromStr>::Err;
        fn from_str(raw: &str) -> Result<Self, Self::Err> {
            raw.parse().map(TestId)
        }
    }

    /// A plain-named aggregate: `NAME = "account"`, like the derive's default.
    struct Account;
    impl AggregateTrait for Account {
        const NAME: &'static str = "account";
        type Id = TestId;
        type State = ();
        type Event = ();
        type Command = ();
        type Error = Never;

        fn initial(_id: &Self::Id) -> Self::State {}
        fn apply(_state: &mut Self::State, _event: &Self::Event) {}
        fn decide(
            _state: &Self::State,
            _command: &Self::Command,
        ) -> Result<alloc::vec::Vec<Self::Event>, Self::Error> {
            Ok(Vec::new())
        }
    }

    /// An aggregate whose name ends in the separator.
    struct FramedAccount;
    impl AggregateTrait for FramedAccount {
        const NAME: &'static str = "just-banks-";
        type Id = TestId;
        type State = ();
        type Event = ();
        type Command = ();
        type Error = Never;

        fn initial(_id: &Self::Id) -> Self::State {}
        fn apply(_state: &mut Self::State, _event: &Self::Event) {}
        fn decide(
            _state: &Self::State,
            _command: &Self::Command,
        ) -> Result<alloc::vec::Vec<Self::Event>, Self::Error> {
            Ok(Vec::new())
        }
    }

    fn parse(raw: &str) -> Option<TestId> {
        raw.parse().ok()
    }

    #[test]
    fn aggregate_id_reads_back_what_for_aggregate_wrote() {
        let stream = StreamId::for_aggregate::<Account>(&TestId(7));
        assert_eq!(stream.aggregate_id::<Account>(parse), Some(TestId(7)));
        // A name that ends in the separator reads the whole of it.
        let framed = StreamId::from("just-banks-42");
        assert_eq!(framed.aggregate_id::<FramedAccount>(parse), Some(TestId(42)));
    }

    #[test]
    fn aggregate_id_knows_when_it_is_not_its_stream() {
        let other = StreamId::for_aggregate::<FramedAccount>(&TestId(9));
        // A framed stream does not read back for the plain aggregate:
        // "%just-banks-%9-%" is not `account-...`.
        assert_eq!(other.aggregate_id::<Account>(parse), None);
        // The bare name is not a stream with an empty id part.
        assert_eq!(StreamId::from("account").aggregate_id::<Account>(parse), None);
        assert_eq!(StreamId::from("account-").aggregate_id::<Account>(parse), None);
        // "x" is not an id.
        assert_eq!(
            StreamId::from("account-x").aggregate_id::<Account>(parse),
            None
        );
        // A stream from another aggregate is not this aggregate's.
        assert_eq!(StreamId::from("just-banks-7").aggregate_id::<Account>(parse), None);
        assert_eq!(
            StreamId::from("just-banks-x").aggregate_id::<FramedAccount>(parse),
            None
        );
    }
}
