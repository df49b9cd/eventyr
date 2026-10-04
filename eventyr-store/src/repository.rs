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
use eventyr_core::write::{RetryPolicy, WriteMachine, WriteOutcome};

use crate::driver::drive_write;
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
pub enum ExecutionOutcome<E> {
    /// The events were committed; the envelopes are as the store
    /// recorded them.
    Committed(Vec<EventEnvelope<E>>),
    /// The command decided no events; nothing was appended.
    Noop,
}

/// Executes commands against an aggregate's stream: load → fold →
/// decide → append, with conflict retry.
///
/// The repository owns no state beyond the store and the retry policy;
/// every interaction gets a fresh machine, so a repository is freely
/// shareable (`Arc`, concurrent `execute` calls) as long as the store
/// is.
pub struct AggregateRepository<A, S> {
    store: S,
    retry_policy: RetryPolicy,
    _aggregate: core::marker::PhantomData<fn(A)>,
}

impl<A, S> AggregateRepository<A, S>
where
    A: Aggregate,
    S: EventStore<Event = A::Event>,
{
    /// A repository over `store`, retrying conflicts per `retry_policy`.
    pub fn new(store: S, retry_policy: RetryPolicy) -> Self {
        Self {
            store,
            retry_policy,
            _aggregate: core::marker::PhantomData,
        }
    }

    /// The store this repository reads and writes — for projections and
    /// queries that share the same event log.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Execute `command` against the aggregate instance `id`.
    ///
    /// Loads the stream, folds the state, decides the events, and
    /// appends them under the expected version. On a conflict the
    /// interaction is retried (reload the delta, re-fold, re-decide)
    /// until the retry budget is spent.
    ///
    /// Events are appended with empty metadata. Correlation and
    /// causation ids are not yet reachable through this path — a
    /// metadata-aware variant arrives with 0.2; until then, hand-drive
    /// the [`WriteMachine`] to
    /// enrich events.
    pub async fn execute(
        &self,
        id: A::Id,
        command: A::Command,
    ) -> Result<ExecutionOutcome<A::Event>, ExecutionError<A>> {
        let mut machine = WriteMachine::<A>::new(id, command, self.retry_policy);
        match drive_write(&mut machine, &self.store).await {
            WriteOutcome::Committed(events) => Ok(ExecutionOutcome::Committed(events)),
            WriteOutcome::Noop => Ok(ExecutionOutcome::Noop),
            WriteOutcome::Rejected(error) => Err(ExecutionError::Domain(error)),
            WriteOutcome::Failed(error) => Err(ExecutionError::Store(error)),
        }
    }
}
