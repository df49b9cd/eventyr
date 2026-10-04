//! Snapshot vocabulary for the write machine: [`Snapshot`],
//! [`SnapshotPolicy`], and [`WritePolicy`].
//!
//! Snapshots are an opt-in fast path for state rebuild: instead of
//! folding the whole stream, the machine loads the newest snapshot,
//! checks its version against the stream it then reads, and folds only
//! the delta. The policy — *how often* to snapshot — and the snapshot
//! itself are data in core; serializing the state to bytes is the
//! store's problem (core stays serde-free).
//!
//! Snapshot persistence is fire-and-forget: a committed interaction
//! carries the offer on [`WriteOutcome::Committed`][crate::write::WriteOutcome]
//! — the driver may persist it, but the write is already committed, so a
//! failed or skipped snapshot save never turns into a store failure.
//! Snapshots are read-side shortcuts, never the source of truth.

use core::num::NonZeroU64;

use crate::aggregate::Aggregate;
use crate::vocabulary::{StreamId, Version};
use crate::write::RetryPolicy;

/// A materialized aggregate state at a known stream version.
///
/// The machine builds one when the [`SnapshotPolicy`] fires; the store
/// may persist it (state serialized out of band — core never sees the
/// bytes) and hand it back as the answer to
/// [`LoadSnapshot`](crate::write::WriteAction::LoadSnapshot).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(bound(
        serialize = "S: serde::Serialize",
        deserialize = "S: serde::de::DeserializeOwned"
    ))
)]
pub struct Snapshot<S> {
    /// The stream this snapshot belongs to.
    pub stream_id: StreamId,
    /// The stream version the state was folded up to (inclusive).
    pub version: Version,
    /// The materialized state at `version`.
    pub state: S,
}

/// How often to snapshot: every `every` versions the stream advances.
///
/// Deliberately the *only* policy knob — a snapshot is taken when the
/// number of events folded since the last snapshot crosses `every`. The
/// store decides what "since" means (snapshot version vs. freshly
/// committed version); the machine just offers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotPolicy {
    /// Take a snapshot every `every` versions of stream progress.
    pub every: NonZeroU64,
}

impl SnapshotPolicy {
    /// A policy that snapshots every `every` versions.
    pub const fn new(every: NonZeroU64) -> Self {
        Self { every }
    }

    /// Whether the policy fires for a stream that moved from
    /// `base_version` (the snapshot's version; [`Version::EMPTY`] when
    /// none) to `committed_version`.
    ///
    /// The cadence is tracked per stream: fires when the versions
    /// crossed a multiple of `every` on the way from `base` to
    /// `committed`.
    pub fn is_due(&self, base_version: Version, committed_version: Version) -> bool {
        committed_version
            .as_u64()
            .saturating_sub(base_version.as_u64())
            >= self.every.get()
    }
}

/// The write-side tuning for one repository or driver: the conflict
/// retry budget, plus an optional snapshot policy.
///
/// `Default`: retries as the [`RetryPolicy`] default, snapshots off —
/// identical to the pre-snapshot behavior.
///
/// This is the combined-knob vocabulary: a driver or repository that
/// wants one value for "how the write path behaves" (a config file, a
/// builder's tuning setter) takes a `WritePolicy`; the machine
/// constructors keep their narrow signatures and the repository splits
/// it straight back out:
///
/// ```ignore
/// let machine = WriteMachine::with_snapshots(
///     id, command, policy.retry, policy.snapshot.expect("checked"),
/// );
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WritePolicy {
    /// How many times the machine may reload and re-decide after a
    /// conflict.
    pub retry: RetryPolicy,
    /// Snapshots are opt-in: `None` means "behave exactly as before".
    pub snapshot: Option<SnapshotPolicy>,
}

impl WritePolicy {
    /// Retries at `retry`, snapshots off.
    pub const fn retries(retry: RetryPolicy) -> Self {
        Self {
            retry,
            snapshot: None,
        }
    }

    /// Turn snapshots on with `every` versions between them.
    pub const fn with_snapshots(mut self, every: NonZeroU64) -> Self {
        self.snapshot = Some(SnapshotPolicy::new(every));
        self
    }
}

/// Opt-in marker: the aggregate's state is snapshot-able (`Clone`).
///
/// Snapshot support on the machine
/// ([`with_snapshots`](crate::write::WriteMachine::with_snapshots)) and
/// the store-side repository is bound on this trait, not on
/// [`Aggregate`] itself, so aggregate definitions never pay for or
/// name snapshots until they opt in. The blanket impl covers every
/// aggregate whose state can be cloned; aggregate authors do not write
/// anything to "implement" it.
pub trait HasSnapshotState: Aggregate {}

impl<A> HasSnapshotState for A
where
    A: Aggregate,
    A::State: Clone,
{
}

/// A snapshot candidate offered to the driver after a commit, when the
/// policy fired. Wrapper around [`Snapshot`] so the outcome's payload
/// names what it's for.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(bound(
        serialize = "S: serde::Serialize",
        deserialize = "S: serde::de::DeserializeOwned"
    ))
)]
pub struct OfferSnapshot<S>(pub Snapshot<S>);

impl<S> OfferSnapshot<S> {
    /// The snapshot being offered.
    pub fn into_inner(self) -> Snapshot<S> {
        self.0
    }
}
