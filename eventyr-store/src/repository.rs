//! The repository: the ergonomic entry point that hides the machine and
//! driver behind one method.
//!
//! [`AggregateRepository::execute`] is load → fold → decide → append with
//! optimistic-concurrency retry — a thin wrapper that constructs the
//! [`WriteMachine`], drives it against the store, and maps the outcome
//! into a [`Result`].

use eventyr_core::aggregate::Aggregate;
use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::snapshot::{HasSnapshotState, OfferSnapshot, WritePolicy};
use eventyr_core::write::{RetryPolicy, WriteMachine, WriteOutcome};
use futures::TryStreamExt;

use crate::driver::{drive_write, drive_write_with_snapshots};
use crate::snapshot_store::SnapshotStore;
use crate::store::EventStore;

/// Why a command execution failed: the domain rejected it, or the store
/// did.
pub enum ExecutionError<A: Aggregate> {
    /// [`decide`](Aggregate::decide) rejected the command against the
    /// current state.
    Domain(A::Error),
    /// The store failed: a conflict that exhausted the retry budget, a
    /// transient error, or a fatal one.
    Store(StoreError),
}

impl<A: Aggregate> core::fmt::Debug for ExecutionError<A> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Domain(error) => f.debug_tuple("Domain").field(error).finish(),
            Self::Store(error) => f.debug_tuple("Store").field(error).finish(),
        }
    }
}

impl<A: Aggregate> core::fmt::Display for ExecutionError<A> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Domain(error) => write!(f, "command rejected: {error}"),
            Self::Store(error) => write!(f, "store failure: {error}"),
        }
    }
}

impl<A: Aggregate> core::error::Error for ExecutionError<A> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::Domain(_) => None,
        }
    }
}

/// The result of a successful command execution.
#[derive(Debug)]
pub enum ExecutionOutcome<E, S = ()> {
    /// The events were committed; the envelopes are as the store
    /// recorded them.
    ///
    /// `snapshot` is the machine's fire-and-forget post-commit offer:
    /// `Some` when a snapshots-on repository's policy fired and the
    /// driver persisted it (or accepted it for persistence), `None`
    /// otherwise. Never affects the commit itself.
    Committed {
        /// The committed events as the store recorded them.
        committed: Vec<EventEnvelope<E>>,
        /// The snapshot the machine offered at commit, if any.
        snapshot: Option<OfferSnapshot<S>>,
    },
    /// The command decided no events; nothing was appended.
    Noop,
}

/// Executes commands against an aggregate's stream: load → fold →
/// decide → append, with conflict retry.
///
/// The repository owns no state beyond the store, the retry policy, and
/// the optional snapshot policy; every interaction gets a fresh machine,
/// so a repository is freely shareable (`Arc`, concurrent `execute`
/// calls) as long as the stores are. `SS` is the snapshot-store channel:
/// `()` (the default) on a plain repository, a [`SnapshotStore`]
/// implementation on one built with
/// [`with_snapshots`](AggregateRepository::with_snapshots).
pub struct AggregateRepository<A, S, SS = ()> {
    store: S,
    snapshots: SS,
    policy: WritePolicy,
    _aggregate: core::marker::PhantomData<fn(A)>,
}

impl<A, S> AggregateRepository<A, S, ()>
where
    A: Aggregate,
    S: EventStore<Event = A::Event>,
{
    /// A repository over `store`, retrying conflicts per `retry_policy`.
    /// Snapshots are off — the write protocol is exactly the
    /// pre-snapshot one.
    pub fn new(store: S, retry_policy: RetryPolicy) -> Self {
        Self {
            store,
            snapshots: (),
            policy: WritePolicy::retries(retry_policy),
            _aggregate: core::marker::PhantomData,
        }
    }

    /// A repository over `store` whose whole write path — retry budget
    /// and (like [`new`](AggregateRepository::new), absent) snapshot
    /// cadence — comes from the combined
    /// [`WritePolicy`](eventyr_core::snapshot::WritePolicy).
    ///
    /// Snapshot cadence on a snapshots-off repository is a no-op (`with_snapshots`
    /// reads the same field); build from a policy already carrying one
    /// only if the repository is about to be turned on.
    pub fn from_policy(store: S, policy: WritePolicy) -> Self {
        Self {
            store,
            snapshots: (),
            policy,
            _aggregate: core::marker::PhantomData,
        }
    }
}

/// The half of the API every repository has, snapshots or not: the
/// store accessor and, on the snapshot-off half, plain `execute`.
impl<A, S, SS> AggregateRepository<A, S, SS>
where
    A: Aggregate,
    S: EventStore<Event = A::Event>,
{
    /// The store this repository reads and writes — for projections and
    /// queries that share the same event log.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// The snapshot store this repository reads and persists through
    /// — `()` on a snapshots-off repository.
    pub fn snapshots(&self) -> &SS {
        &self.snapshots
    }

    /// Execute `command` against the aggregate instance `id`.
    ///
    /// Loads the stream, folds the state, decides the events, and
    /// appends them under the expected version. On a conflict the
    /// interaction is retried (reload the delta, re-fold, re-decide)
    /// until the retry budget is spent.
    ///
    /// Events carry empty metadata; to stamp the request's correlation
    /// (or the event's causation) on every event of the interaction,
    /// call [`execute_with_metadata`](Self::execute_with_metadata) —
    /// the 0.5.2 boundary seam.
    ///
    /// This is the snapshot-off path: on a snapshots-on repository,
    /// `execute_with_snapshots` runs the full snapshot protocol. The
    /// behavior here is identical whether or not the repository was
    /// built with [`with_snapshots`](AggregateRepository::with_snapshots)
    /// — snapshots are a read-side fast path, never a change to the
    /// write contract.
    pub async fn execute(
        &self,
        id: A::Id,
        command: A::Command,
    ) -> Result<ExecutionOutcome<A::Event>, ExecutionError<A>> {
        self.execute_with_metadata(id, command, eventyr_core::envelope::Metadata::default())
            .await
    }

    /// [`execute`](Self::execute) with the interaction's metadata stamped
    /// on every emitted event (0.5.2).
    ///
    /// The repository drivers stamp `metadata` onto the machine's
    /// `Append` events, so a caller can trace one request's events
    /// (and their causes) without threading ids through the domain.
    /// `decide` never sees them — causation/correlation are boundary
    /// concerns, not the domain's.
    pub async fn execute_with_metadata(
        &self,
        id: A::Id,
        command: A::Command,
        metadata: eventyr_core::envelope::Metadata,
    ) -> Result<ExecutionOutcome<A::Event>, ExecutionError<A>> {
        let mut machine = WriteMachine::<A>::new(id, command, self.policy.retry)
            .with_metadata(metadata);
        match drive_write(&mut machine, &self.store).await {
            WriteOutcome::Committed { committed, .. } => Ok(ExecutionOutcome::Committed {
                committed,
                snapshot: None,
            }),
            WriteOutcome::Noop => Ok(ExecutionOutcome::Noop),
            WriteOutcome::Rejected(error) => Err(ExecutionError::Domain(error)),
            WriteOutcome::Failed(error) => Err(ExecutionError::Store(error)),
        }
    }
}

impl<A, S> AggregateRepository<A, S, ()>
where
    A: HasSnapshotState,
    A::State: Clone + Send,
    S: EventStore<Event = A::Event>,
{
    /// Turn snapshots on: the same repository, plus the snapshot store
    /// (to read the newest snapshot from at load time, and to persist
    /// the post-commit offer into) and the cadence policy for it.
    ///
    /// The policy is the whole tuning surface: the machine offers a
    /// snapshot when a commit moves the stream at least `every`
    /// versions past the version the fold started from. Persistence is
    /// fire-and-forget — a failed save logs nothing and changes
    /// nothing; the next load simply folds a longer delta.
    ///
    /// Takes a [`SnapshotPolicy`] (not a [`WritePolicy`]) so the
    /// "snapshots on, but no cadence" shape is unrepresentable at the
    /// type level — no `assert!` for what the signature rules out.
    pub fn with_snapshots<SS>(
        self,
        snapshots: SS,
        policy: eventyr_core::snapshot::SnapshotPolicy,
    ) -> AggregateRepository<A, S, SS>
    where
        SS: SnapshotStore<State = A::State>,
    {
        AggregateRepository {
            store: self.store,
            snapshots,
            policy: WritePolicy {
                retry: self.policy.retry,
                snapshot: Some(policy),
            },
            _aggregate: core::marker::PhantomData,
        }
    }
}

impl<A, S, SS> AggregateRepository<A, S, SS>
where
    A: HasSnapshotState,
    A::State: Clone + Send,
    S: EventStore<Event = A::Event>,
    SS: SnapshotStore<State = A::State>,
{
    /// Execute `command` against the aggregate instance `id` under the
    /// snapshot protocol.
    ///
    /// The load starts from the newest persisted snapshot when one
    /// exists (folding only the delta after it); a policy-firing
    /// commit's offer is persisted through the [`SnapshotStore`]
    /// before the outcome returns. The fire-and-forget contract
    /// stands: a failed save is dropped, and the committed outcome the
    /// caller sees is never affected by it.
    ///
    /// Bonded on `SS: SnapshotStore` so the type system rules out a
    /// snapshots-off repository calling it.
    pub async fn execute_with_snapshots(
        &self,
        id: A::Id,
        command: A::Command,
    ) -> Result<ExecutionOutcome<A::Event, A::State>, ExecutionError<A>> {
        self.execute_with_snapshots_and_metadata(
            id,
            command,
            eventyr_core::envelope::Metadata::default(),
        )
        .await
    }

    /// [`execute_with_snapshots`](Self::execute_with_snapshots) with the
    /// interaction's metadata stamped on every emitted event (0.5.2).
    pub async fn execute_with_snapshots_and_metadata(
        &self,
        id: A::Id,
        command: A::Command,
        metadata: eventyr_core::envelope::Metadata,
    ) -> Result<ExecutionOutcome<A::Event, A::State>, ExecutionError<A>> {
        let policy = self
            .policy
            .snapshot
            .expect("with_snapshots sets the policy before this method is reachable");
        let mut machine = WriteMachine::<A, A::State>::with_snapshots(
            id,
            command,
            self.policy.retry,
            policy,
        )
        .with_metadata(metadata);
        match drive_write_with_snapshots(&mut machine, &self.store, &self.snapshots).await {
            WriteOutcome::Committed {
                committed,
                snapshot,
            } => Ok(ExecutionOutcome::Committed {
                committed,
                snapshot,
            }),
            WriteOutcome::Noop => Ok(ExecutionOutcome::Noop),
            WriteOutcome::Rejected(error) => Err(ExecutionError::Domain(error)),
            WriteOutcome::Failed(error) => Err(ExecutionError::Store(error)),
        }
    }
}
use eventyr_core::snapshot::{Snapshot, SnapshotCache};

/// A repository that holds the last-committed snapshot of each stream
/// in-process (roadmap 0.6.5): next command starts from the cached
/// state instead of re-folding the stream.
///
/// The cache is a snapshot the repository promotes: a miss runs the
/// ordinary write protocol; a hit's state seeds the fold, and the
/// append's optimistic-concurrency expectation still guards the write
/// — a stale cache lands me as a conflict, never as a corrupt fold.
/// Every commit's snapshot re-primes the cache, so the next interaction
/// over this stream starts from it.
pub struct CachedRepository<A, S, C>
where
    A: Aggregate,
{
    store: S,
    cache: C,
    retry_policy: RetryPolicy,
    _aggregate: core::marker::PhantomData<fn(A)>,
}

impl<A, S, C> CachedRepository<A, S, C>
where
    A: HasSnapshotState,
    A::State: Clone + Send,
    S: EventStore<Event = A::Event>,
    C: SnapshotCache<A::State>,
{
    /// A cached repository over `store`, sharing `cache` across
    /// interactions.
    pub fn new(store: S, cache: C, retry_policy: RetryPolicy) -> Self {
        Self {
            store,
            cache,
            retry_policy,
            _aggregate: core::marker::PhantomData,
        }
    }

    /// Execute `command` against `id`, priming the fold from the cache.
    pub async fn execute_cached(
        &mut self,
        id: A::Id,
        command: A::Command,
    ) -> Result<ExecutionOutcome<A::Event, A::State>, ExecutionError<A>> {
        let mut machine = WriteMachine::<A, A::State>::with_cache(id, command, self.retry_policy);
        let action = machine.start();
        let eventyr_core::write::WriteAction::Primed { stream_id } = action else {
            unreachable!("a cache-primed machine opens on Primed");
        };
        let stream_id = stream_id.clone();
        // Seed: the cache's latest prime, or the state the machine
        // starts from (a cache miss is a miss, not a guess about `Id`).
        let seed = match self.cache.lookup(&stream_id) {
            Some(snapshot) => snapshot,
            None => Snapshot {
                stream_id: stream_id.clone(),
                version: eventyr_core::vocabulary::Version::EMPTY,
                state: machine.initial_state().clone(),
            },
        };
        let action = machine.handle(eventyr_core::write::WriteInput::Cached { snapshot: seed });
        let outcome = self.drive_from(action, &mut machine).await;
        match outcome {
            WriteOutcome::Committed { committed, snapshot } => {
                if let Some(offer) = &snapshot {
                    self.cache.prime(offer.clone().into_inner());
                }
                Ok(ExecutionOutcome::Committed { committed, snapshot })
            }
            // A command that decided nothing still moves the cache: the
            // fold the decision ran against is itself the freshest state.
            WriteOutcome::Noop => {
                self.cache.prime(Snapshot {
                    stream_id,
                    version: machine.version(),
                    state: machine.initial_state().clone(),
                });
                Ok(ExecutionOutcome::Noop)
            }
            WriteOutcome::Rejected(error) => Err(ExecutionError::Domain(error)),
            WriteOutcome::Failed(error) => Err(ExecutionError::Store(error)),
        }
    }

    /// Finish the interaction after its priming answer: the async
    /// driver over the store.
    async fn drive_from(
        &self,
        action: eventyr_core::write::WriteAction<A::Event, A::Error, A::State>,
        machine: &mut WriteMachine<A, A::State>,
    ) -> WriteOutcome<A::Event, A::Error, A::State> {
        let mut action = action;
        loop {
            use eventyr_core::write::WriteInput;
            action = match action {
                eventyr_core::write::WriteAction::LoadStream { stream_id, from } => {
                    let loaded: Result<Vec<EventEnvelope<A::Event>>, _> =
                        self.store.stream(&stream_id, from).try_collect().await;
                    machine.handle(match loaded {
                        Ok(events) => WriteInput::Loaded { events },
                        Err(error) => WriteInput::Failed(error),
                    })
                }
                eventyr_core::write::WriteAction::Append {
                    stream_id,
                    expected,
                    events,
                } => match self.store.append(&stream_id, expected, events).await {
                    Ok(committed) => machine.handle(WriteInput::Appended { committed }),
                    Err(error) => machine.handle(error.into()),
                },
                eventyr_core::write::WriteAction::LoadSnapshot { .. }
                | eventyr_core::write::WriteAction::Primed { .. } => machine.handle(
                    WriteInput::Failed(StoreError::other(
                        "the cached path only loads and appends; it never re-reads a snapshot",
                    )),
                ),
                eventyr_core::write::WriteAction::Done(outcome) => return outcome,
            };
        }
    }
}
