//! # Operations — the side stores a restart must not lose, §6 / 0.7.6,
//! 0.7.7, 0.5.3.
//!
//! The event log is the history, but a running service keeps more than
//! the log: subject keys (erasure), parked events (poison the
//! projection refused), snapshots, and checkpoints. On SQLite all four
//! live beside the log, in the same file, on the same connection — and
//! this example proves they survive a restart: run one "process",
//! drop every handle, open the file again, and carry on.
//!
//! Along the way:
//!
//! - **Metrics** — the write path runs `drive_write_with_metrics` and
//!   the projector `Projector::with_metrics`, both reporting to
//!   [`TracingMetrics`] (the `metrics` feature): plain `tracing`
//!   events under the `eventyr.metrics` target. This example installs no
//!   subscriber, so they go nowhere here; an application routes them
//!   through its own.
//! - **The XChaCha20-Poly1305 cipher** seals the owner's name, in
//!   place of the bank's AES-GCM — any `Cipher` drops in.
//! - **Parking** — a projection refuses an order over its risk limit;
//!   the order parks (sealed, through `ShreddingParkedStore`) and the
//!   projection carries on.
//! - **Erasure, and the write it refuses** — after the subject's key is
//!   deleted, its sealed fields read back shredded, and a *new* write
//!   carrying the subject's personal data is refused: nothing is ever
//!   sealed for an erased subject again.
//!
//! Run with:
//! ```sh
//! cargo run -p eventyr --example operations --features sqlite_snapshots,sqlite_checkpoints,sqlite_shred,sqlite_parked,shred_chacha,shred_parked,metrics
//! ```

#![allow(dead_code)] // A demo: the point is the focused feature.

use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use eventyr::prelude::*;
use eventyr::shred::{Sensitive, ShredError, Shredder, ShreddingParkedStore, ShreddingStore};
use eventyr::shred_chacha::XChaCha20Poly1305Cipher;
use eventyr::sqlite::{
    SqliteCheckpointStore, SqliteKeyStore, SqliteParkedStore, SqliteSnapshotStore, SqliteStore,
};
use eventyr::store::metrics::TracingMetrics;
use eventyr::store::prelude::*;
use eventyr::subscription::prelude::{
    CheckpointStore, ParkedStore, Projection, Projector, StoreSubscription,
};

// -- the domain -----------------------------------------------------------

/// A customer's identifier — and, prefixed, the data subject their
/// personal fields are sealed under.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
struct CustomerId(u64);
impl std::fmt::Display for CustomerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The customer signed up. The name is personal data.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Registered {
    name: Sensitive<String>,
}
/// The customer changed their name — fresh personal data.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Renamed {
    name: Sensitive<String>,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Ordered {
    total: u64,
}

#[derive(Clone, Debug)]
enum CustomerCommand {
    Register { name: String },
    Rename { name: String },
    Order { total: u64 },
}

#[derive(Debug, PartialEq)]
enum CustomerError {
    AlreadyRegistered,
    NotRegistered,
}
impl std::fmt::Display for CustomerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::AlreadyRegistered => "the customer is already registered",
            Self::NotRegistered => "the customer is not registered",
        })
    }
}

/// The state carries the id so `decide` can name the data subject.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct CustomerState {
    id: u64,
    registered: bool,
    orders: u64,
}

// `crate = "eventyr"`: this example lives in the `eventyr` package
// itself, so the derive cannot resolve the target from the manifest —
// a user's crate would not need the attribute.
#[derive(Aggregate)]
#[eventyr(
    crate = "eventyr",
    id = CustomerId,
    state = CustomerState,
    error = CustomerError,
    command = CustomerCommand,
    initial = CustomerState { id: id.0, ..CustomerState::default() },
    events(Registered, Renamed, Ordered),
    event_derive(serde::Serialize, serde::Deserialize)
)]
struct Customer;

fn apply(state: &mut CustomerState, event: &CustomerEvent) {
    match event {
        CustomerEvent::Registered(_) => state.registered = true,
        CustomerEvent::Renamed(_) => {}
        CustomerEvent::Ordered(_) => state.orders += 1,
    }
}

fn decide(
    state: &CustomerState,
    command: &CustomerCommand,
) -> Result<Vec<CustomerEvent>, CustomerError> {
    // The customer is the data subject of their own name.
    let subject = format!("customer-{}", state.id);
    match command {
        CustomerCommand::Register { .. } if state.registered => {
            Err(CustomerError::AlreadyRegistered)
        }
        CustomerCommand::Register { name } => Ok(vec![
            Registered {
                name: Sensitive::new(subject, name.clone()),
            }
            .into(),
        ]),
        _ if !state.registered => Err(CustomerError::NotRegistered),
        CustomerCommand::Rename { name } => Ok(vec![
            Renamed {
                name: Sensitive::new(subject, name.clone()),
            }
            .into(),
        ]),
        CustomerCommand::Order { total } => Ok(vec![Ordered { total: *total }.into()]),
    }
}

// -- the operational stores ------------------------------------------------

type Keys = SqliteKeyStore;
type Cipher = XChaCha20Poly1305Cipher;
type Log = ShreddingStore<SqliteStore<CustomerEvent>, Cipher, Keys>;

/// Everything one "process" opens: the sealed log and its four side
/// stores, all in the one SQLite file, on the store's connection.
struct Service {
    log: Log,
    shredder: Arc<Shredder<Cipher, Keys>>,
    snapshots: SqliteSnapshotStore<CustomerState>,
    checkpoints: SqliteCheckpointStore,
    parked: ShreddingParkedStore<SqliteParkedStore<CustomerEvent>, Cipher, Keys>,
}

impl Service {
    fn open(path: &Path) -> Self {
        let store = SqliteStore::<CustomerEvent>::open(path).expect("open the log");
        let keys = SqliteKeyStore::beside(&store).expect("the key store");
        let snapshots = SqliteSnapshotStore::new(&store).expect("the snapshot store");
        let checkpoints = SqliteCheckpointStore::beside(&store).expect("the checkpoint store");
        let parked = SqliteParkedStore::beside(&store).expect("the parked store");
        // One shredder seals the log *and* the parked store, so a
        // subject erased in one is erased in both.
        let shredder = Arc::new(Shredder::new(XChaCha20Poly1305Cipher, keys));
        Self {
            log: ShreddingStore::new(store, shredder.clone()),
            parked: ShreddingParkedStore::new(parked, shredder.clone()),
            shredder,
            snapshots,
            checkpoints,
        }
    }

    /// One command on the write machine, its I/O reported to
    /// `TracingMetrics`: append latency, appends, conflicts.
    async fn execute(
        &self,
        id: u64,
        command: CustomerCommand,
    ) -> WriteOutcome<CustomerEvent, CustomerError> {
        let mut machine =
            WriteMachine::<Customer>::new(CustomerId(id), command, RetryPolicy::default());
        drive_write_with_metrics(&mut machine, &self.log, &TracingMetrics).await
    }

    /// The snapshot-seeded write path: a snapshot every two versions.
    fn repository(
        &self,
    ) -> AggregateRepository<Customer, &Log, &SqliteSnapshotStore<CustomerState>> {
        AggregateRepository::new(&self.log, RetryPolicy::default()).with_snapshots(
            &self.snapshots,
            SnapshotPolicy::new(NonZeroU64::new(2).expect("> 0")),
        )
    }

    /// The risk read model, from its durable checkpoint: an order over
    /// the limit parks instead of stalling the projection.
    async fn project_risk(&self) -> SubscriptionOutcome {
        Projector::new(
            "risk",
            StoreSubscription::new(&self.log),
            &self.checkpoints,
            RiskLimit,
        )
        .with_policy(SubscriptionPolicy::new(64, Duration::ZERO, Duration::ZERO).stop_at_catch_up())
        .park_into(&self.parked, FailurePolicy::Park { retries: 0 })
        .with_metrics(TracingMetrics)
        .run(|_| std::future::ready(()))
        .await
        .expect("the projection runs")
    }
}

/// Refuses any order over 1_000 — the poison event to park.
struct RiskLimit;
impl Projection for RiskLimit {
    type Event = CustomerEvent;
    type Error = String;
    async fn apply(&mut self, envelope: &EventEnvelope<CustomerEvent>) -> Result<(), Self::Error> {
        match &envelope.event {
            CustomerEvent::Ordered(Ordered { total }) if *total > 1_000 => {
                Err(format!("an order of {total} needs review"))
            }
            _ => Ok(()),
        }
    }
}

// -- the run ----------------------------------------------------------------

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("operations.db");

    // -- the first process ----------------------------------------------
    {
        let service = Service::open(&path);
        let ada = StreamId::for_aggregate::<Customer>(&CustomerId(1));

        for command in [
            CustomerCommand::Register { name: "Ada".into() },
            CustomerCommand::Order { total: 120 },
            CustomerCommand::Order { total: 5_000 },
        ] {
            assert!(matches!(
                service.execute(1, command).await,
                WriteOutcome::Committed { .. }
            ));
        }
        // A snapshot-seeded command: the cadence offer at version 4 is
        // saved beside the log.
        let ExecutionOutcome::Committed { snapshot, .. } = service
            .repository()
            .execute_with_snapshots(CustomerId(1), CustomerCommand::Order { total: 80 })
            .await
            .expect("an order")
        else {
            panic!("an order commits")
        };
        assert_eq!(
            snapshot.expect("the cadence fires").into_inner().version,
            Version::new(4)
        );

        // The risk projection parks the 5_000 order and carries on.
        assert!(matches!(
            service.project_risk().await,
            SubscriptionOutcome::CaughtUp { .. }
        ));
        let parked = service.parked.list("risk").await.expect("list parked");
        assert_eq!(parked.len(), 1, "exactly the 5_000 order parked");
        assert_eq!(parked[0].envelope.stream_id, ada);
        println!("first run: four events, one snapshot, one parked order");
        // Every handle drops here: the "process" exits.
    }

    // -- the second process: the same file ---------------------------------
    {
        let service = Service::open(&path);
        let ada = StreamId::for_aggregate::<Customer>(&CustomerId(1));

        // The checkpoint resumes past everything the first run saw —
        // the parked order included.
        let resumed = service.checkpoints.load("risk").await.expect("load");
        assert_eq!(resumed.as_sequence(), Sequence::new(4));
        // The parked order is still waiting for review, its name-bearing
        // event sealed at rest and opened on the way out.
        assert_eq!(service.parked.list("risk").await.expect("list").len(), 1);
        // The snapshot is still there; the next load seeds from it.
        let snapshot = service
            .snapshots
            .load(&ada)
            .await
            .expect("load")
            .expect("the first run's snapshot");
        assert_eq!(snapshot.version, Version::new(4));
        let loaded = service
            .repository()
            .load_with_snapshots(CustomerId(1))
            .await
            .expect("load");
        assert_eq!(loaded.state.orders, 3);
        // And the subject's key survived: the name still opens.
        let history: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(EventStore::stream(
            &service.log,
            &ada,
            Version::EMPTY,
        ))
        .await
        .expect("read Ada");
        let CustomerEvent::Registered(Registered { name }) = &history[0].event else {
            panic!("the first event registered the customer")
        };
        assert_eq!(name.get().map(String::as_str), Some("Ada"));

        // New events after the restart move the projection on from
        // its checkpoint, not from the origin.
        assert!(matches!(
            service
                .execute(1, CustomerCommand::Order { total: 40 })
                .await,
            WriteOutcome::Committed { .. }
        ));
        assert!(matches!(
            service.project_risk().await,
            SubscriptionOutcome::CaughtUp { checkpoint } if checkpoint.as_sequence() == Sequence::new(5)
        ));

        // -- erasure ------------------------------------------------------
        service
            .shredder
            .erase("customer-1")
            .await
            .expect("erase Ada");
        let history: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(EventStore::stream(
            &service.log,
            &ada,
            Version::EMPTY,
        ))
        .await
        .expect("read Ada");
        let CustomerEvent::Registered(Registered { name }) = &history[0].event else {
            panic!("the first event registered the customer")
        };
        assert!(name.is_shredded(), "Ada's name is gone; her history is not");

        // The write that refuses: a rename seals fresh personal data,
        // and an erased subject never gets a key again. The domain
        // accepted the command — the store refused the seal.
        let WriteOutcome::Failed(error) = service
            .execute(
                1,
                CustomerCommand::Rename {
                    name: "Ada L.".into(),
                },
            )
            .await
        else {
            panic!("a write sealing an erased subject's data is refused")
        };
        let StoreError::Other(source) = &error else {
            panic!("the shredder's refusal is a store failure: {error}")
        };
        assert!(
            matches!(
                source.downcast_ref::<ShredError>(),
                Some(ShredError::SubjectErased(subject)) if subject == "customer-1"
            ),
            "the refusal names the erased subject: {error}"
        );
        // Events with no personal data still append for the customer.
        assert!(matches!(
            service
                .execute(1, CustomerCommand::Order { total: 10 })
                .await,
            WriteOutcome::Committed { .. }
        ));
        println!(
            "second run: checkpoint, parked order, snapshot, and key survived; \
             Ada erased, her rename refused"
        );
    }
}
