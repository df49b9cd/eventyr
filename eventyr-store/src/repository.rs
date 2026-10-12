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
use eventyr_core::snapshot::{HasSnapshotState, OfferSnapshot, Snapshot, WritePolicy};
use eventyr_core::vocabulary::{StreamId, Version};
use eventyr_core::write::{RetryPolicy, WriteMachine, WriteOutcome, envelope_continues};

use core::future::Future;

use eventyr_core::envelope::Metadata;

use crate::driver::{drive_write, drive_write_with_snapshots};
use crate::snapshot_store::SnapshotStore;
use crate::store::EventStore;

/// A state folded from one stream, and the version it reached (roadmap 0.7.8).
///
/// `version` is the last folded event's position — `Version::EMPTY`
/// on an empty or unknown stream, and below the bound `load_at` asked
/// for when the stream never reached it. It is what makes "as of a
/// timestamp" answerable on the write side: `load_until` reports the
/// version the fold stopped at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Loaded<St> {
    /// The folded state (`Aggregate::initial` with each event applied).
    pub state: St,
    /// The position of the last folded event; `EMPTY` when none applied.
    pub version: Version,
}

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
    /// The command carried an idempotency key (roadmap 0.7.5) whose earlier
    /// commit is already in the stream; nothing was decided or appended.
    /// `committed` is that earlier commit, as stored.
    AlreadyCommitted {
        /// The events the earlier commit with this key appended.
        committed: Vec<EventEnvelope<E>>,
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

    /// Load the instance's state by folding its whole stream (roadmap 0.7.8).
    ///
    /// No command, no append, no retry: one fold from
    /// [`Aggregate::initial`]. Snapshots are not consulted — see
    /// [`load_with_snapshots`](AggregateRepository::load_with_snapshots)
    /// on a snapshots-on repository. An unknown stream loads its
    /// `initial` and reports `Version::EMPTY`.
    ///
    /// # Errors
    ///
    /// The store's stream read failed, or the stream it read was
    /// protocol-violating: events of another stream, a non-contiguous
    /// version run, or a snapshot store answering for another stream.
    pub async fn load(&self, id: A::Id) -> Result<Loaded<A::State>, StoreError> {
        self.fold_while(&id, None, |_| Ok(true)).await
    }

    /// [`load`](Self::load) stopping at version `version` (inclusive).
    ///
    /// `Version::EMPTY` returns the initial state with no I/O. A version
    /// past the stream's head returns the head state and reports the
    /// head — `Loaded.version` is what was actually folded.
    ///
    /// # Errors
    ///
    /// As [`load`](Self::load): the stream read failed or the events
    /// read back violated the fold's invariants.
    pub async fn load_at(
        &self,
        id: A::Id,
        version: Version,
    ) -> Result<Loaded<A::State>, StoreError> {
        if version == Version::EMPTY {
            return Ok(Loaded {
                state: A::initial(&id),
                version: Version::EMPTY,
            });
        }
        self.fold_at_most(&id, None, version).await
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
    ///
    /// # Errors
    ///
    /// [`ExecutionError::Domain`] when `decide` rejected the command.
    /// [`ExecutionError::Store`] when the store failed: a conflict that
    /// exhausted the retry budget, a transient unavailability, a
    /// protocol-violating read, or a fatal store error.
    ///
    /// # Examples
    ///
    /// Open then deposit — the second `execute` folds the first's
    /// event, so the deposit decides against an open account. The
    /// committed envelopes are as the store recorded them:
    ///
    /// ```
    /// # fn main() {
    /// use eventyr_core::testing::account::*;
    /// use eventyr_core::write::RetryPolicy;
    /// use eventyr_store::prelude::*;
    /// use futures::executor::block_on;
    ///
    /// let repository = AggregateRepository::<Account, _>::new(
    ///     InMemoryStore::new(),
    ///     RetryPolicy::default(),
    /// );
    ///
    /// block_on(repository.execute(
    ///     AccountId(1),
    ///     AccountCommand::Open { owner: "me".into() },
    /// )).expect("open");
    ///
    /// match block_on(repository.execute(
    ///     AccountId(1),
    ///     AccountCommand::Deposit { amount: 50 },
    /// )) {
    ///     Ok(ExecutionOutcome::Committed { committed, .. }) => {
    ///         let events: Vec<_> = committed.iter().map(|e| &e.event).collect();
    ///         assert!(matches!(
    ///             events.as_slice(),
    ///             [AccountEvent::Deposited { amount: 50 }],
    ///         ));
    ///     }
    ///     _ => panic!("a deposit on an open account commits"),
    /// }
    /// # }
    /// ```
    pub async fn execute(
        &self,
        id: A::Id,
        command: A::Command,
    ) -> Result<ExecutionOutcome<A::Event>, ExecutionError<A>> {
        self.execute_with_metadata(id, command, eventyr_core::envelope::Metadata::default())
            .await
    }

    /// [`execute`](Self::execute) with the interaction's metadata stamped
    /// on every emitted event (roadmap 0.5.2).
    ///
    /// A metadata [`idempotency_key`](eventyr_core::envelope::Metadata::idempotency_key)
    /// makes the call idempotent (roadmap 0.7.5): if the stream already holds
    /// events stamped with the key, the command is not decided again
    /// and the outcome is [`AlreadyCommitted`](ExecutionOutcome::AlreadyCommitted).
    /// The check reads the stream the command targets, so it holds
    /// across processes and restarts with nothing but the event log.
    ///
    /// The repository drivers stamp `metadata` onto the machine's
    /// `Append` events, so a caller can trace one request's events
    /// (and their causes) without threading ids through the domain.
    /// `decide` never sees them — causation/correlation are boundary
    /// concerns, not the domain's.
    ///
    /// # Errors
    ///
    /// As [`execute`](Self::execute): a domain rejection, or a store
    /// failure — a conflict after retries, a transient error, or a
    /// fatal one.
    pub async fn execute_with_metadata(
        &self,
        id: A::Id,
        command: A::Command,
        metadata: eventyr_core::envelope::Metadata,
    ) -> Result<ExecutionOutcome<A::Event>, ExecutionError<A>> {
        let mut machine =
            WriteMachine::<A>::new(id, command, self.policy.retry).with_metadata(metadata);
        match drive_write(&mut machine, &self.store).await {
            WriteOutcome::Committed { committed, .. } => Ok(ExecutionOutcome::Committed {
                committed,
                snapshot: None,
            }),
            WriteOutcome::AlreadyCommitted { committed } => {
                Ok(ExecutionOutcome::AlreadyCommitted { committed })
            }
            WriteOutcome::Noop => Ok(ExecutionOutcome::Noop),
            WriteOutcome::Rejected(error) => Err(ExecutionError::Domain(error)),
            WriteOutcome::Failed(error) => Err(ExecutionError::Store(error)),
        }
    }

    /// [`fold_while`](Self::fold_while) with the one `keep` both
    /// `load_at` paths use: fold through `version`, inclusive. The
    /// `Version::EMPTY` early return is each public method's — the
    /// seed read is snapshots' to skip.
    async fn fold_at_most(
        &self,
        id: &A::Id,
        seed: Option<Snapshot<A::State>>,
        version: Version,
    ) -> Result<Loaded<A::State>, StoreError> {
        self.fold_while(id, seed, |envelope| Ok(envelope.version <= version))
            .await
    }

    /// The shared read fold behind `load` / `load_at` /
    /// `load_until` (roadmap 0.7.8): seeds from `seed` (a snapshot, when one is
    /// used) or from [`Aggregate::initial`], then folds the stream from
    /// the seed's version onward, applying each event while `keep` says
    /// continue. `keep` runs *before* the fold so `load_until` can stop
    /// at the first event past its bound without folding it.
    ///
    /// The two machine invariants are re-checked here — the events are
    /// for the expected stream, and versions are contiguous from the
    /// seed — so a misbehaving store fails the fold loudly instead of
    /// silently producing a wrong state. [`envelope_continues`] is the
    /// rule itself, shared with `WriteMachine`'s load fold: one
    /// predicate, one pair of messages. It runs before `keep` (the
    /// check reads the envelope, it does not fold it), and the version
    /// it advances is committed only when the event is folded — so
    /// `load_until` still reports the version it stopped at.
    async fn fold_while(
        &self,
        id: &A::Id,
        seed: Option<Snapshot<A::State>>,
        mut keep: impl FnMut(&EventEnvelope<A::Event>) -> Result<bool, StoreError>,
    ) -> Result<Loaded<A::State>, StoreError> {
        let stream_id = StreamId::for_aggregate::<A>(id);
        let (mut state, mut version) = match seed {
            Some(snapshot) if snapshot.stream_id != stream_id => {
                return Err(StoreError::protocol(
                    "the snapshot store answered for another stream",
                ));
            }
            Some(snapshot) => (snapshot.state, snapshot.version),
            None => (A::initial(id), Version::EMPTY),
        };
        let events = self.store.stream(&stream_id, version);
        futures::pin_mut!(events);
        while let Some(envelope) = futures::StreamExt::next(&mut events).await {
            let envelope = envelope?;
            // The predicate advances a scratch of the reached version;
            // it becomes the fold's version only if the event is kept.
            let mut reached = version;
            envelope_continues(&stream_id, &mut reached, &envelope)?;
            if !keep(&envelope)? {
                break;
            }
            A::apply(&mut state, &envelope.event);
            version = reached;
        }
        Ok(Loaded { state, version })
    }

    /// Fold every event whose metadata timestamp is at or before
    /// `timestamp`, load-only (roadmap 0.7.8, `time` feature).
    ///
    /// The fold stops at the *first* event after the instant — a stream
    /// appends once per version and timestamps can reorder across
    /// concurrent commits, so skipping and continuing would fold a hole.
    /// An event carrying no timestamp is a `StoreError` naming the
    /// stream: only stores that persist one (the in-memory and Postgres
    /// stores) can answer a load by time. Never snapshot-seeded — a
    /// snapshot records a version, not an instant.
    ///
    /// # Errors
    ///
    /// As [`load`](Self::load), or an event in the folded stretch
    /// carries no timestamp — a store that does not persist one cannot
    /// answer a load by time.
    #[cfg(feature = "time")]
    pub async fn load_until(
        &self,
        id: A::Id,
        timestamp: time::OffsetDateTime,
    ) -> Result<Loaded<A::State>, StoreError> {
        let keep = |envelope: &EventEnvelope<A::Event>| match envelope.metadata.timestamp {
            Some(stamped) => Ok(stamped <= timestamp),
            None => Err(StoreError::other(format!(
                "load_until needs a timestamp; {} has none at version {}",
                envelope.stream_id,
                envelope.version.as_u64(),
            ))),
        };
        self.fold_while(&id, None, keep).await
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
    /// Takes a [`SnapshotPolicy`](eventyr_core::snapshot::SnapshotPolicy) (not a [`WritePolicy`]) so the
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
            policy: self.policy.with_snapshots(policy),
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
    ///
    /// # Errors
    ///
    /// [`ExecutionError::Domain`] when `decide` rejected the command.
    /// [`ExecutionError::Store`] when the store failed: the snapshot
    /// read, a conflict that exhausted the retry budget, a
    /// protocol-violating stream, or a fatal store error. A failed
    /// post-commit snapshot *save* is not one — it is
    /// fire-and-forget.
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

    /// Load the instance's state from the newest persisted snapshot,
    /// folding only the delta after it (roadmap 0.7.8).
    ///
    /// The snapshot is a seed, never the whole answer: a failed snapshot
    /// read fails the load (the write path's rule — a seed is not a
    /// cache to fall back past). Ignoring the cadence, a load does not
    /// offer a snapshot. The unseeded fold is
    /// [`load`](AggregateRepository::load), which exists only on a
    /// snapshots-off repository; a full replay of a snapshots-on one goes
    /// through a plain repository over the same store. The distinct names
    /// mean no call silently pays for the wrong fold.
    ///
    /// # Errors
    ///
    /// The snapshot read failed (the write path's rule: a seed is not a
    /// cache to fall back past), or the delta fold's stream read failed
    /// or violated the fold's invariants.
    pub async fn load_with_snapshots(&self, id: A::Id) -> Result<Loaded<A::State>, StoreError> {
        let stream_id = StreamId::for_aggregate::<A>(&id);
        let seed = self.snapshots.load(&stream_id).await?;
        self.fold_while(&id, seed, |_| Ok(true)).await
    }

    /// [`load_with_snapshots`](Self::load_with_snapshots) stopping at
    /// version `version` (inclusive).
    ///
    /// A snapshot *past* `version` is not a seed for it (the fold can
    /// only move forward), so this seeds from one at or below the bound
    /// and otherwise replays. `Version::EMPTY` returns the initial state
    /// with no I/O.
    ///
    /// # Errors
    ///
    /// As [`load_with_snapshots`](Self::load_with_snapshots): the
    /// snapshot read failed, or the delta fold did.
    pub async fn load_at_with_snapshots(
        &self,
        id: A::Id,
        version: Version,
    ) -> Result<Loaded<A::State>, StoreError> {
        if version == Version::EMPTY {
            return Ok(Loaded {
                state: A::initial(&id),
                version: Version::EMPTY,
            });
        }
        let stream_id = StreamId::for_aggregate::<A>(&id);
        let seed = self
            .snapshots
            .load(&stream_id)
            .await?
            .filter(|snapshot| snapshot.version <= version);
        self.fold_at_most(&id, seed, version).await
    }
    /// [`execute_with_snapshots`](Self::execute_with_snapshots) with the
    /// interaction's metadata stamped on every emitted event (roadmap 0.5.2).
    ///
    /// # Errors
    ///
    /// As [`execute_with_snapshots`](Self::execute_with_snapshots): a
    /// domain rejection, or a store failure — the snapshot read, a
    /// conflict after retries, or a fatal error.
    ///
    /// # Panics
    ///
    /// When the repository carries no snapshot cadence policy — the
    /// constructor [`with_snapshots`](Self::with_snapshots) sets one
    /// before this method is reachable (the `SS: SnapshotStore` bound
    /// rules a plain repository out at the type level).
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
        let mut machine =
            WriteMachine::<A, A::State>::with_snapshots(id, command, self.policy.retry, policy)
                .with_metadata(metadata);
        match drive_write_with_snapshots(&mut machine, &self.store, &self.snapshots).await {
            WriteOutcome::Committed {
                committed,
                snapshot,
            } => Ok(ExecutionOutcome::Committed {
                committed,
                snapshot,
            }),
            WriteOutcome::AlreadyCommitted { committed } => {
                Ok(ExecutionOutcome::AlreadyCommitted { committed })
            }
            WriteOutcome::Noop => Ok(ExecutionOutcome::Noop),
            WriteOutcome::Rejected(error) => Err(ExecutionError::Domain(error)),
            WriteOutcome::Failed(error) => Err(ExecutionError::Store(error)),
        }
    }
}

/// The write and read operations every [`AggregateRepository`] offers,
/// whichever snapshot channel it carries — the seam for code generic over
/// "a repository of `A`".
///
/// A plain repository (`SS = ()`) executes and loads by folding the
/// whole stream; one built with
/// [`with_snapshots`](AggregateRepository::with_snapshots) starts from
/// the newest snapshot and persists the post-commit offer. Through this
/// trait both answer the same calls, so the snapshot decision stays
/// where the repository is built. The snapshot offer a commit may carry
/// is dropped here: it was already persisted (or deliberately skipped)
/// by the time the outcome returns.
pub trait Repository<A: Aggregate>: Send + Sync {
    /// [`execute_with_metadata`](AggregateRepository::execute_with_metadata),
    /// or its snapshot counterpart.
    ///
    /// # Errors
    ///
    /// A domain rejection, or a store failure — as the inherent methods.
    fn execute_with_metadata(
        &self,
        id: A::Id,
        command: A::Command,
        metadata: Metadata,
    ) -> impl Future<Output = Result<ExecutionOutcome<A::Event>, ExecutionError<A>>> + Send;

    /// [`load`](AggregateRepository::load), or its snapshot counterpart.
    ///
    /// # Errors
    ///
    /// As the inherent methods: the store's read failed, or the stream
    /// violated the fold's invariants.
    fn load(&self, id: A::Id) -> impl Future<Output = Result<Loaded<A::State>, StoreError>> + Send;

    /// [`load_at`](AggregateRepository::load_at), or its snapshot
    /// counterpart.
    ///
    /// # Errors
    ///
    /// As [`load`](Repository::load).
    fn load_at(
        &self,
        id: A::Id,
        version: Version,
    ) -> impl Future<Output = Result<Loaded<A::State>, StoreError>> + Send;
}

impl<A, S> Repository<A> for AggregateRepository<A, S, ()>
where
    A: Aggregate,
    A::Id: Send + Sync,
    A::Command: Send,
    A::State: Send,
    A::Event: Send + Sync,
    A::Error: Send,
    S: EventStore<Event = A::Event> + Send + Sync,
{
    fn execute_with_metadata(
        &self,
        id: A::Id,
        command: A::Command,
        metadata: Metadata,
    ) -> impl Future<Output = Result<ExecutionOutcome<A::Event>, ExecutionError<A>>> + Send {
        AggregateRepository::execute_with_metadata(self, id, command, metadata)
    }

    fn load(&self, id: A::Id) -> impl Future<Output = Result<Loaded<A::State>, StoreError>> + Send {
        AggregateRepository::load(self, id)
    }

    fn load_at(
        &self,
        id: A::Id,
        version: Version,
    ) -> impl Future<Output = Result<Loaded<A::State>, StoreError>> + Send {
        AggregateRepository::load_at(self, id, version)
    }
}

impl<A, S, SS> Repository<A> for AggregateRepository<A, S, SS>
where
    A: HasSnapshotState,
    A::Id: Send + Sync,
    A::Command: Send,
    A::State: Clone + Send + Sync,
    A::Event: Send + Sync,
    A::Error: Send,
    S: EventStore<Event = A::Event> + Send + Sync,
    SS: SnapshotStore<State = A::State> + Send + Sync,
{
    async fn execute_with_metadata(
        &self,
        id: A::Id,
        command: A::Command,
        metadata: Metadata,
    ) -> Result<ExecutionOutcome<A::Event>, ExecutionError<A>> {
        Ok(
            match self
                .execute_with_snapshots_and_metadata(id, command, metadata)
                .await?
            {
                ExecutionOutcome::Committed { committed, .. } => ExecutionOutcome::Committed {
                    committed,
                    snapshot: None,
                },
                ExecutionOutcome::AlreadyCommitted { committed } => {
                    ExecutionOutcome::AlreadyCommitted { committed }
                }
                ExecutionOutcome::Noop => ExecutionOutcome::Noop,
            },
        )
    }

    fn load(&self, id: A::Id) -> impl Future<Output = Result<Loaded<A::State>, StoreError>> + Send {
        self.load_with_snapshots(id)
    }

    fn load_at(
        &self,
        id: A::Id,
        version: Version,
    ) -> impl Future<Output = Result<Loaded<A::State>, StoreError>> + Send {
        self.load_at_with_snapshots(id, version)
    }
}

impl<A: Aggregate, R: Repository<A> + ?Sized> Repository<A> for &R {
    fn execute_with_metadata(
        &self,
        id: A::Id,
        command: A::Command,
        metadata: Metadata,
    ) -> impl Future<Output = Result<ExecutionOutcome<A::Event>, ExecutionError<A>>> + Send {
        (**self).execute_with_metadata(id, command, metadata)
    }

    fn load(&self, id: A::Id) -> impl Future<Output = Result<Loaded<A::State>, StoreError>> + Send {
        (**self).load(id)
    }

    fn load_at(
        &self,
        id: A::Id,
        version: Version,
    ) -> impl Future<Output = Result<Loaded<A::State>, StoreError>> + Send {
        (**self).load_at(id, version)
    }
}

impl<A: Aggregate, R: Repository<A> + ?Sized> Repository<A> for std::sync::Arc<R> {
    fn execute_with_metadata(
        &self,
        id: A::Id,
        command: A::Command,
        metadata: Metadata,
    ) -> impl Future<Output = Result<ExecutionOutcome<A::Event>, ExecutionError<A>>> + Send {
        (**self).execute_with_metadata(id, command, metadata)
    }

    fn load(&self, id: A::Id) -> impl Future<Output = Result<Loaded<A::State>, StoreError>> + Send {
        (**self).load(id)
    }

    fn load_at(
        &self,
        id: A::Id,
        version: Version,
    ) -> impl Future<Output = Result<Loaded<A::State>, StoreError>> + Send {
        (**self).load_at(id, version)
    }
}
