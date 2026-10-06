//! The embedded store against the shared contract, plus end-to-end
//! coverage through the blocking driver — the two halves of this
//! crate's thesis made testable.

use std::future::Future;

use eventyr_core::batch::{CommittedStream, StreamAppend};
use eventyr_core::boundary::{AppendCondition, Query, Tagged};
use eventyr_core::envelope::{EventEnvelope, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
use eventyr_store::notify::CommitSignal;
use eventyr_store::store::{
    EventFilter, EventStore, FilteredRead, QueryAppend, StreamLifecycle, StreamsAll,
};
use eventyr_store_fjall::FjallStore;
use eventyr_store_testing::{Counted, ParityEvent, PayloadEvent};
use futures::Stream;
use serde::{Deserialize, Serialize};

/// A store together with the temporary directory its database lives in.
///
/// The contract suites take a fresh store per check and drop it at the
/// check's end; owning the directory here means it is removed then too,
/// after the store (fields drop in declaration order), instead of being
/// leaked for the OS to reap.
struct InTempDir<S> {
    store: S,
    _dir: tempfile::TempDir,
}

/// A fresh fjall store in its own temporary directory.
fn fresh<E>() -> InTempDir<FjallStore<E>> {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FjallStore::open(dir.path()).expect("open");
    InTempDir { store, _dir: dir }
}

impl<S: EventStore> EventStore for InTempDir<S> {
    type Event = S::Event;

    fn append(
        &self,
        stream_id: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent<Self::Event>>,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<Self::Event>>, StoreError>> + Send {
        self.store.append(stream_id, expected, events)
    }

    fn append_batch(
        &self,
        appends: Vec<StreamAppend<Self::Event>>,
    ) -> impl Future<Output = Result<Vec<CommittedStream<Self::Event>>, StoreError>> + Send {
        self.store.append_batch(appends)
    }

    fn stream(
        &self,
        stream_id: &StreamId,
        from: Version,
    ) -> impl Stream<Item = Result<EventEnvelope<Self::Event>, StoreError>> + Send {
        self.store.stream(stream_id, from)
    }
}

impl<S: StreamsAll> StreamsAll for InTempDir<S> {
    fn stream_all(
        &self,
        from: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<Self::Event>, StoreError>> + Send {
        self.store.stream_all(from)
    }

    fn stream_all_filtered(
        &self,
        from: Sequence,
        filter: &EventFilter,
        max: usize,
        scan_limit: usize,
    ) -> impl Future<Output = Result<FilteredRead<Self::Event>, StoreError>> + Send
    where
        Self::Event: EventName,
    {
        self.store
            .stream_all_filtered(from, filter, max, scan_limit)
    }
}

impl<S> QueryAppend for InTempDir<S>
where
    S: QueryAppend,
    S::Event: EventName + Tagged,
{
    fn read(
        &self,
        query: &Query,
        after: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<Self::Event>, StoreError>> + Send {
        self.store.read(query, after)
    }

    fn append_if(
        &self,
        appends: Vec<StreamAppend<Self::Event>>,
        condition: AppendCondition,
    ) -> impl Future<Output = Result<Vec<CommittedStream<Self::Event>>, StoreError>> + Send {
        self.store.append_if(appends, condition)
    }
}

impl<S: StreamLifecycle> StreamLifecycle for InTempDir<S> {
    fn close_stream(
        &self,
        stream_id: &StreamId,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.store.close_stream(stream_id)
    }

    fn truncate_before(
        &self,
        stream_id: &StreamId,
        version: Version,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.store.truncate_before(stream_id, version)
    }
}

impl<S: CommitSignal> CommitSignal for InTempDir<S> {
    type Listener = S::Listener;

    fn subscribe(&self) -> impl Future<Output = Result<Self::Listener, StoreError>> + Send {
        self.store.subscribe()
    }
}

#[test]
fn fjall_store_passes_the_event_store_contract() {
    eventyr_store_testing::event_store_contract::<PayloadEvent, _>(fresh);
}

#[test]
fn fjall_store_passes_the_streams_all_contract() {
    eventyr_store_testing::streams_all_contract::<PayloadEvent, _>(fresh);
}

#[test]
fn fjall_store_passes_the_append_batch_contract() {
    eventyr_store_testing::event_store_batch_contract::<PayloadEvent, _>(fresh);
}

#[test]
fn blocking_driver_commits_through_the_embedded_store() {
    use std::fmt;

    use eventyr_core::prelude::*;
    use eventyr_store::driver;

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    struct Id(u64);
    impl fmt::Display for Id {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    #[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
    enum Event {
        Opened { owner: String },
    }
    impl EventName for Event {
        fn event_name(&self) -> &'static str {
            match self {
                Event::Opened { .. } => "Opened",
            }
        }
    }

    #[derive(Clone, Debug)]
    enum Command {
        Open { owner: String },
    }

    #[derive(Debug, PartialEq)]
    enum Error {
        AlreadyOpen,
    }
    impl fmt::Display for Error {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                Error::AlreadyOpen => "already open",
            })
        }
    }

    #[derive(Debug, PartialEq)]
    struct State {
        open: bool,
    }

    struct Account;
    impl Aggregate for Account {
        const NAME: &'static str = "account";
        type Id = Id;
        type State = State;
        type Event = Event;
        type Command = Command;
        type Error = Error;

        fn initial(_id: &Self::Id) -> Self::State {
            State { open: false }
        }
        fn apply(state: &mut Self::State, event: &Self::Event) {
            match event {
                Event::Opened { .. } => state.open = true,
            }
        }
        fn decide(
            state: &Self::State,
            command: &Self::Command,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            match command {
                Command::Open { .. } if state.open => Err(Error::AlreadyOpen),
                Command::Open { owner } => Ok(vec![Event::Opened {
                    owner: owner.clone(),
                }]),
            }
        }
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let store = FjallStore::<Event>::open(dir.path()).expect("open");
    let id = Id(1);

    let mut machine = WriteMachine::<Account>::new(
        id.clone(),
        Command::Open { owner: "me".into() },
        RetryPolicy::default(),
    );
    let outcome = driver::drive_write_blocking(&mut machine, &store);
    assert!(matches!(outcome, WriteOutcome::Committed { .. }));

    // A second open against the folded stream rejects — the embedded
    // store honours the protocol the machine drives.
    let mut machine = WriteMachine::<Account>::new(
        id,
        Command::Open { owner: "me".into() },
        RetryPolicy::default(),
    );
    let outcome = driver::drive_write_blocking(&mut machine, &store);
    assert!(matches!(outcome, WriteOutcome::Rejected(_)));
}

#[test]
fn fjall_store_passes_the_query_append_contract() {
    eventyr_store_testing::query_append_contract(fresh);
}

#[test]
fn fjall_store_passes_the_commit_signal_contract() {
    eventyr_store_testing::commit_signal_contract::<PayloadEvent, _>(fresh);
}

#[test]
fn fjall_store_passes_the_filtered_read_contract() {
    eventyr_store_testing::filtered_read_contract::<ParityEvent, _>(fresh);
}

#[test]
fn fjall_store_passes_the_lifecycle_contract() {
    eventyr_store_testing::lifecycle_contract::<PayloadEvent, _>(fresh);
    eventyr_store_testing::lifecycle_query_append_contract(fresh);
}

#[cfg(feature = "snapshots")]
#[test]
fn fjall_snapshot_store_passes_the_snapshot_contract() {
    use eventyr_core::snapshot::Snapshot;
    use eventyr_store::snapshot_store::SnapshotStore;
    use eventyr_store_fjall::snapshots::FjallSnapshotStore;

    /// The snapshot store and the directory its database lives in.
    struct Snapshots {
        store: FjallSnapshotStore<u64>,
        _dir: tempfile::TempDir,
    }

    impl SnapshotStore for Snapshots {
        type State = u64;

        fn load(
            &self,
            stream_id: &StreamId,
        ) -> impl Future<Output = Result<Option<Snapshot<u64>>, StoreError>> + Send {
            self.store.load(stream_id)
        }

        fn save(
            &self,
            snapshot: Snapshot<u64>,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            self.store.save(snapshot)
        }
    }

    eventyr_store_testing::snapshot_contract::<u64, _>(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let database = fjall::SingleWriterTxDatabase::builder(dir.path())
            .open()
            .expect("open");
        Snapshots {
            store: FjallSnapshotStore::open(&database).expect("snapshots"),
            _dir: dir,
        }
    });
}

/// Reading a few events from the global stream must not walk the rest
/// of the log: a subscriber polls `stream_all(checkpoint).take(batch)`
/// every round, so a read that decoded the whole tail would make
/// catch-up quadratic.
#[test]
fn a_short_global_read_decodes_only_what_it_takes() {
    use futures::StreamExt;
    use futures::executor::block_on;

    const EVENTS: u64 = 600;
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FjallStore::<Counted>::open(dir.path()).expect("open");
    block_on(
        store.append(
            &StreamId::from("lazy-1"),
            ExpectedVersion::Empty,
            (1..=EVENTS)
                .map(|value| NewEvent::new(Counted(value)))
                .collect(),
        ),
    )
    .expect("append");

    Counted::reset_decodes();
    let first: Vec<_> = block_on(store.stream_all(Sequence::START).take(3).collect());
    assert_eq!(first.len(), 3);
    assert_eq!(
        Counted::decodes(),
        3,
        "a read of three events decodes three rows"
    );
}
