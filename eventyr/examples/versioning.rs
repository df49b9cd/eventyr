//! # Versioning — reading old payloads, and rebuilding when the fold
//! changes, §4.4 / 0.3 + 0.5.1.
//!
//! The log keeps every payload in the shape it was written in; the code
//! knows only the current shape. A [`RawEvent`] is the stored form — the
//! event type, the payload bytes, and the schema version it was written
//! at — and upcasting turns it into the current shape on read:
//!
//! - An [`UpcasterRegistry`] holds a per-type *ladder*: one rung per
//!   `(event_type, from_version)`, each lifting a payload one version.
//!   [`build`](UpcasterRegistry::build) refuses a broken ladder at
//!   startup, so a gap is a configuration error, not the first reader's
//!   failure. A V1 payload climbs every rung; a current one passes
//!   through.
//! - An [`UpcasterChain`] maps an event type to the typed event — here,
//!   the decode of the ladder's current-version bytes.
//!
//! The source adapters run both inside the subscription's own `Fetch`:
//! [`VersionedSource`] walks the ladder, [`UpcastingSource`] applies the
//! chain. A payload no rung can lift becomes a store error on that
//! fetch, and the projection stops with its checkpoint before the
//! poison event — loudly, by design: skipping a stored fact is data
//! loss.
//!
//! A change to the *fold* — the projection's own code, not the payloads
//! — is a different job: [`RebuildPlan`] re-folds from the origin under
//! a versioned checkpoint key (`"balance@v2"`), stops at catch-up so
//! the caller can swap the rebuilt model in, and leaves `"balance@v1"`
//! in place for a rollback.
//!
//! No shipped store keeps a schema version per row (DESIGN 0.6.2), so
//! the raw log here is a small in-memory `SubscriptionSource`; the read
//! path over it is the one a raw-payload store would plug into.
//!
//! Run with:
//! ```sh
//! cargo run -p eventyr --example versioning --features projection
//! ```

#![allow(dead_code)] // A demo: the point is the focused feature.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use eventyr::prelude::*;
use eventyr::projection::prelude::*;
use eventyr::subscription::prelude::{
    CheckpointStore, InMemoryCheckpointStore, Projection, Projector, SubscriptionSource,
};

// -- the stored history ----------------------------------------------

/// One stored `Amount` row of account 7's log.
fn row(sequence: u64, schema_version: u32, payload: &[u8]) -> EventEnvelope<RawEvent> {
    EventEnvelope {
        sequence: Sequence::new(sequence),
        stream_id: StreamId::from("account-7"),
        version: Version::new(sequence),
        event: RawEvent {
            event_type: "Amount".into(),
            schema_version: EventSchemaVersion::new(schema_version),
            payload: payload.to_vec(),
        },
        metadata: Metadata::default(),
    }
}

/// Three deposits written over three schema versions: V1 was a bare
/// number of cents, V2 a decimal string, V3 (current) account-tagged
/// JSON.
fn history() -> Vec<EventEnvelope<RawEvent>> {
    vec![
        row(1, 1, b"5000"),
        row(2, 2, b"20.00"),
        row(3, 3, br#"{"account":7,"cents":3000}"#),
    ]
}

/// The raw log, as a subscription source: up to `max` rows after the
/// checkpoint, in sequence order.
struct RawLog(Vec<EventEnvelope<RawEvent>>);

impl SubscriptionSource for RawLog {
    type Event = RawEvent;

    async fn fetch(&self, from: Checkpoint, max: usize) -> Result<Batch<RawEvent>, StoreError> {
        let rows = self
            .0
            .iter()
            .filter(|row| row.sequence > from.as_sequence())
            .take(max)
            .cloned()
            .collect();
        Ok(Batch::of(rows))
    }
}

// -- the current shape and the ladder ----------------------------------

/// The deposit as the code knows it today.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Amount {
    account: u64,
    cents: u64,
}

fn invalid(message: &str) -> UpcastError {
    UpcastError {
        event_type: "Amount".into(),
        message: message.into(),
    }
}

/// V1 → V2: cents became a decimal string.
fn v1_to_v2(payload: Vec<u8>) -> Result<Vec<u8>, UpcastError> {
    let cents: u64 = std::str::from_utf8(&payload)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| invalid("the V1 payload is not a number of cents"))?;
    Ok(format!("{}.{:02}", cents / 100, cents % 100).into_bytes())
}

/// V2 → V3: the account id arrived — every row before it was account
/// 7's — and the payload became JSON.
fn v2_to_v3(payload: Vec<u8>) -> Result<Vec<u8>, UpcastError> {
    let text = std::str::from_utf8(&payload).map_err(|_| invalid("the V2 payload is not text"))?;
    let (units, hundredths) = text
        .split_once('.')
        .ok_or_else(|| invalid("the V2 payload is not a decimal"))?;
    let cents = units
        .parse::<u64>()
        .ok()
        .zip(hundredths.parse::<u64>().ok())
        .map(|(units, hundredths)| units * 100 + hundredths)
        .ok_or_else(|| invalid("the V2 payload is not a decimal"))?;
    serde_json::to_vec(&Amount { account: 7, cents })
        .map_err(|_| invalid("cannot encode the current shape"))
}

/// The two rungs that lift any stored `Amount` to V3.
fn ladder() -> UpcasterRegistry {
    let (registry, _) =
        UpcasterRegistry::new().with("Amount", EventSchemaVersion::V1, VersionRung::new(v1_to_v2));
    let (registry, _) = registry.with(
        "Amount",
        EventSchemaVersion::new(2),
        VersionRung::new(v2_to_v3),
    );
    registry
}

/// The terminal step: the ladder's V3 bytes, decoded.
fn decode() -> UpcasterChain<Amount> {
    UpcasterChain::new().with(
        "Amount",
        ClosureUpcaster::new(|raw: RawEvent| {
            serde_json::from_slice(&raw.payload).map_err(|_| invalid("not the V3 shape"))
        }),
    )
}

/// The full read path: raw rows in, current `Amount`s out.
fn typed(rows: Vec<EventEnvelope<RawEvent>>) -> UpcastingSource<VersionedSource<RawLog>, Amount> {
    UpcastingSource::new(VersionedSource::new(RawLog(rows), ladder()), decode())
}

// -- the read model -----------------------------------------------------

/// Account balances in cents. Version 2 of the fold ignores deposits
/// under 25.00 — a business rule changed, not the payloads.
struct Balance {
    cents: Arc<Mutex<u64>>,
    minimum: u64,
}
impl Projection for Balance {
    type Event = Amount;
    type Error = Infallible;
    async fn apply(&mut self, envelope: &EventEnvelope<Amount>) -> Result<(), Self::Error> {
        if envelope.event.cents >= self.minimum {
            *self.cents.lock().expect("the lock") += envelope.event.cents;
        }
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // -- startup: validate the ladder --------------------------------
    // A ladder missing its V1 rung cannot lift V1 rows: `build` says so
    // before any read happens.
    let (broken, _) = UpcasterRegistry::new().with(
        "Amount",
        EventSchemaVersion::new(2),
        VersionRung::new(v2_to_v3),
    );
    assert_eq!(
        broken.build(),
        Err(RegistryError::MissingBase {
            event_type: "Amount".into(),
            found: EventSchemaVersion::new(2),
        })
    );
    ladder()
        .build()
        .expect("the two-rung ladder is well-formed");

    // One V1 row, walked up both rungs by hand.
    let current = ladder()
        .upcast(RawEvent::v1("Amount", b"5000".to_vec()))
        .expect("V1 lifts to V3");
    assert_eq!(current.schema_version, EventSchemaVersion::new(3));
    assert_eq!(current.payload, br#"{"account":7,"cents":5000}"#);

    // -- the projection, fold v1 --------------------------------------
    let checkpoints = InMemoryCheckpointStore::new();
    let v1 = Arc::new(Mutex::new(0));
    let outcome = RebuildPlan::new("balance", SchemaVersion(1), decode())
        .projector(
            VersionedSource::new(RawLog(history()), ladder()),
            &checkpoints,
            Balance {
                cents: v1.clone(),
                minimum: 0,
            },
        )
        .run(|_| std::future::ready(()))
        .await
        .expect("the v1 rebuild runs");
    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    // 50.00 + 20.00 + 30.00, each row read at its own schema version.
    assert_eq!(*v1.lock().expect("the lock"), 10_000);

    // -- the fold changes: rebuild as v2 -------------------------------
    // Same payloads, same ladder; a new key. The rebuild starts from
    // the origin because "balance@v2" was never written.
    let plan = RebuildPlan::new("balance", SchemaVersion(2), decode());
    assert_eq!(plan.checkpoint_key(), "balance@v2");
    let v2 = Arc::new(Mutex::new(0));
    let outcome = plan
        .projector(
            VersionedSource::new(RawLog(history()), ladder()),
            &checkpoints,
            Balance {
                cents: v2.clone(),
                minimum: 2_500,
            },
        )
        .run(|_| std::future::ready(()))
        .await
        .expect("the v2 rebuild runs");
    assert!(matches!(outcome, SubscriptionOutcome::CaughtUp { .. }));
    assert_eq!(
        *v2.lock().expect("the lock"),
        8_000,
        "the 20.00 deposit no longer counts"
    );

    // Both keys sit side by side: rolling back to the v1 fold resumes
    // from its own checkpoint.
    for version in [SchemaVersion(1), SchemaVersion(2)] {
        assert_eq!(
            checkpoints
                .load_versioned("balance", version)
                .await
                .expect("load"),
            Checkpoint::new(Sequence::new(3))
        );
    }

    // -- a poison payload stops the projection -------------------------
    // A fourth row claims V1 but is not a number. The fetch fails, the
    // projection stops, and its checkpoint stays before the poison row.
    let mut poisoned = history();
    poisoned.push(row(4, 1, b"not-a-number"));
    let ignored = Arc::new(Mutex::new(0));
    let outcome = Projector::new(
        "audit",
        typed(poisoned),
        &checkpoints,
        Balance {
            cents: ignored,
            minimum: 0,
        },
    )
    .with_policy(SubscriptionPolicy::default().stop_at_catch_up())
    .run(|_| std::future::ready(()))
    .await
    .expect("the checkpoint loads");
    let SubscriptionOutcome::Failed(error) = outcome else {
        panic!("a poison payload stops the projection: {outcome:?}")
    };
    assert!(
        error.to_string().contains("upcasting sequence 4"),
        "the failure names the row: {error}"
    );
    assert_eq!(
        checkpoints.load("audit").await.expect("load"),
        Checkpoint::ORIGIN,
        "the batch holding the poison row was never acked"
    );

    println!(
        "versioning: V1/V2/V3 rows read as one shape; fold v1 = 100.00, \
         v2 = 80.00 under its own key; a poison row stops the projection"
    );
}
