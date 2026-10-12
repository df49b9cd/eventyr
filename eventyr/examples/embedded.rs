//! # Embedded — fjall, the blocking drivers, and no runtime, §5.2 / 0.6.4.
//!
//! A desktop app, a CLI, an edge device: event sourcing does not need a
//! server or an async runtime. This example runs a savings pot on the
//! embedded fjall store with nothing else:
//!
//! - **One directory holds everything.** The log and the snapshots live
//!   in one fjall database: [`FjallStore::from_keyspace`] and
//!   [`FjallSnapshotStore::open`] on the same handle (the `fjall` crate
//!   is a direct dependency for that handle).
//! - **The blocking drivers.** Commands run through
//!   `drive_write_with_snapshots_blocking`, the read model through
//!   `drive_projector_blocking` — the same machines the async drivers
//!   run, with no `tokio` anywhere (fjall's futures resolve
//!   immediately; the one direct port call below is a plain `block_on`).
//! - **A pot that does not exist yet.** `State = Option<PotState>`,
//!   folded through the [`Optional`] helper: a deposit before the pot
//!   is opened is the domain's `NotOpen`, not a missing-row special case.
//! - **A filtered read model.** `EventFilter::event_types(["Deposited"])`
//!   hands the projector deposits only.
//! - **Reopen the directory.** The log and the snapshot are still there,
//!   and the next write continues the stream.
//!
//! Run with:
//! ```sh
//! cargo run -p eventyr --example embedded --features fjall_snapshots,subscription
//! ```

#![allow(dead_code)] // A demo: the point is the focused feature.

use std::convert::Infallible;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use eventyr::fjall::{FjallSnapshotStore, FjallStore};
use eventyr::prelude::*;
use eventyr::store::prelude::*;
use eventyr::subscription::prelude::{
    DriverPorts, FilteredSubscription, InMemoryCheckpointStore, Projection,
    drive_projector_blocking,
};

// -- the domain ---------------------------------------------------------

/// A savings pot's identifier.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct PotId(u64);
impl std::fmt::Display for PotId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Opened {
    goal: u64,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Deposited {
    amount: u64,
}
/// The pot is spent: its balance goes to the purchase.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Emptied {
    spent: u64,
}

#[derive(Clone, Debug)]
enum PotCommand {
    Open { goal: u64 },
    Deposit { amount: u64 },
    Empty,
}

#[derive(Debug, PartialEq)]
enum PotError {
    AlreadyOpen,
    NotOpen,
    OverGoal,
}
impl std::fmt::Display for PotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::AlreadyOpen => "the pot is already open",
            Self::NotOpen => "the pot is not open",
            Self::OverGoal => "the deposit would pass the goal",
        })
    }
}

/// The pot once it exists. The aggregate's state is `Option<PotState>`:
/// `None` until `Opened`.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct PotState {
    goal: u64,
    balance: u64,
}

/// The fold for an existing pot; `Optional::apply_state` lifts it over
/// the `None` before the first event.
impl Optional for PotState {
    type Event = PotEvent;
    fn apply(state: &mut Self, event: &PotEvent) {
        match event {
            PotEvent::Opened(Opened { goal }) => state.goal = *goal,
            PotEvent::Deposited(Deposited { amount }) => state.balance += amount,
            PotEvent::Emptied(Emptied { spent }) => state.balance -= spent,
        }
    }
}

// `crate = "eventyr"`: this example lives in the `eventyr` package
// itself, so the derive cannot resolve the target from the manifest —
// a user's crate would not need the attribute.
#[derive(Aggregate)]
#[eventyr(
    crate = "eventyr",
    id = PotId,
    state = Option<PotState>,
    error = PotError,
    command = PotCommand,
    initial = None,
    events(Opened, Deposited, Emptied),
    event_derive(serde::Serialize, serde::Deserialize)
)]
struct Pot;

fn apply(state: &mut Option<PotState>, event: &PotEvent) {
    Optional::apply_state(state, event);
}

fn decide(state: &Option<PotState>, command: &PotCommand) -> Result<Vec<PotEvent>, PotError> {
    match (state, command) {
        (Some(_), PotCommand::Open { .. }) => Err(PotError::AlreadyOpen),
        (None, PotCommand::Open { goal }) => Ok(vec![Opened { goal: *goal }.into()]),
        // Whether a command may run against a missing pot is the
        // domain's question: here, only `Open` may.
        (None, _) => Err(PotError::NotOpen),
        (Some(pot), PotCommand::Deposit { amount }) if pot.balance + amount > pot.goal => {
            Err(PotError::OverGoal)
        }
        (Some(_), PotCommand::Deposit { amount }) => Ok(vec![Deposited { amount: *amount }.into()]),
        (Some(pot), PotCommand::Empty) => Ok(vec![Emptied { spent: pot.balance }.into()]),
    }
}

// -- the drivers ----------------------------------------------------------

type Snapshots = FjallSnapshotStore<Option<PotState>>;

/// One command: the write machine, snapshot-seeded, on the blocking
/// driver. A snapshot is offered (and saved) every two versions.
fn execute(
    store: &FjallStore<PotEvent>,
    snapshots: &Snapshots,
    pot: u64,
    command: PotCommand,
) -> WriteOutcome<PotEvent, PotError, Option<PotState>> {
    let mut machine = WriteMachine::<Pot, _>::with_snapshots(
        PotId(pot),
        command,
        RetryPolicy::default(),
        SnapshotPolicy::new(NonZeroU64::new(2).expect("> 0")),
    );
    drive_write_with_snapshots_blocking(&mut machine, store, snapshots)
}

/// The read model: deposits only, folded into a total.
struct Deposits {
    total: Arc<AtomicU64>,
    seen: Arc<AtomicU64>,
}
impl Projection for Deposits {
    type Event = PotEvent;
    type Error = Infallible;
    async fn apply(&mut self, envelope: &EventEnvelope<PotEvent>) -> Result<(), Self::Error> {
        self.seen.fetch_add(1, Ordering::Relaxed);
        if let PotEvent::Deposited(Deposited { amount }) = &envelope.event {
            self.total.fetch_add(*amount, Ordering::Relaxed);
        }
        Ok(())
    }
}

/// Rebuild the deposit total from the origin on the blocking projector
/// driver, through a filtered source. Returns `(total, events seen)`.
///
/// fjall ships no checkpoint store, so this read model rebuilds on each
/// start — cheap here; a long log would keep a checkpoint store of its
/// own.
fn deposited(store: &FjallStore<PotEvent>) -> (u64, u64) {
    let total = Arc::new(AtomicU64::new(0));
    let seen = Arc::new(AtomicU64::new(0));
    let mut machine = SubscriptionMachine::new(
        SubscriptionPolicy::default().stop_at_catch_up(),
        Checkpoint::ORIGIN,
    );
    let outcome = drive_projector_blocking(
        &mut machine,
        "deposits",
        &FilteredSubscription::new(store, EventFilter::all().event_types(["Deposited"])),
        &InMemoryCheckpointStore::new(),
        Deposits {
            total: total.clone(),
            seen: seen.clone(),
        },
        // Never called: the run stops at catch-up and nothing fails.
        |_| std::future::ready(()),
        DriverPorts::new(),
    );
    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    (total.load(Ordering::Relaxed), seen.load(Ordering::Relaxed))
}

/// Open the log and the snapshots on one fjall database in `dir`.
fn open(dir: &std::path::Path) -> (FjallStore<PotEvent>, Snapshots) {
    let database = fjall::SingleWriterTxDatabase::builder(dir)
        .open()
        .expect("open the database");
    let snapshots = Snapshots::open(&database).expect("open the snapshots");
    let store = FjallStore::from_keyspace(database).expect("open the log");
    (store, snapshots)
}

// -- the run ------------------------------------------------------------

fn main() {
    let dir = tempfile::tempdir().expect("a temporary directory");

    // -- the first run ---------------------------------------------------
    {
        let (store, snapshots) = open(dir.path());

        // No pot yet: the state is `None`, and the domain refuses.
        assert!(matches!(
            execute(&store, &snapshots, 1, PotCommand::Deposit { amount: 5 }),
            WriteOutcome::Rejected(PotError::NotOpen)
        ));

        for command in [
            PotCommand::Open { goal: 100 },
            PotCommand::Deposit { amount: 40 },
            PotCommand::Deposit { amount: 40 },
        ] {
            match execute(&store, &snapshots, 1, command) {
                WriteOutcome::Committed {
                    snapshot: Some(offer),
                    ..
                } => println!("snapshot saved at version {}", offer.into_inner().version),
                WriteOutcome::Committed { .. } => {}
                other => panic!("the pot accepts it: {other:?}"),
            }
        }
        assert!(matches!(
            execute(&store, &snapshots, 1, PotCommand::Deposit { amount: 30 }),
            WriteOutcome::Rejected(PotError::OverGoal)
        ));
        let WriteOutcome::Committed { snapshot, .. } =
            execute(&store, &snapshots, 1, PotCommand::Empty)
        else {
            panic!("an open pot can be emptied")
        };
        let at = snapshot
            .expect("the cadence fires again")
            .into_inner()
            .version;
        println!("snapshot saved at version {at}");

        assert_eq!(deposited(&store), (80, 2), "two deposits, nothing else");
        // Every handle drops here: the database closes.
    }

    // -- the second run: reopen the directory -----------------------------
    {
        let (store, snapshots) = open(dir.path());

        // The snapshot survived — a direct port call, one `block_on`.
        let snapshot =
            futures::executor::block_on(snapshots.load(&StreamId::for_aggregate::<Pot>(&PotId(1))))
                .expect("read the snapshot")
                .expect("the first run's snapshot");
        assert_eq!(snapshot.version, Version::new(4));
        assert_eq!(
            snapshot.state,
            Some(PotState {
                goal: 100,
                balance: 0
            })
        );

        // The next write continues the stream from the snapshot.
        let WriteOutcome::Committed { committed, .. } =
            execute(&store, &snapshots, 1, PotCommand::Deposit { amount: 25 })
        else {
            panic!("the emptied pot takes a deposit")
        };
        assert_eq!(committed[0].version, Version::new(5));

        assert_eq!(deposited(&store), (105, 3), "the log survived the reopen");
    }

    println!("embedded: no runtime; the reopened log and snapshot carried on at version 5");
}
