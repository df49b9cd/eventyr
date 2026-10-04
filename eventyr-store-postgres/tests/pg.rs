//! End-to-end against a real Postgres: append, conflict, read-back.
//!
//! Requires a database; `EVENTYR_TEST_PG_URL` points at it (CI runs a
//! disposable container). Without the env var the test is skipped.

use futures::TryStreamExt;
use uuid::Uuid;

use eventyr_core::envelope::{EventEnvelope, Metadata, NewEvent};
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, StreamId, Version};
use eventyr_store::store::{EventStore, StreamsAll};

use eventyr_store_postgres::PgStore;

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
enum BankEvent {
    Opened { owner: String },
    Deposited { amount: u64 },
}

impl EventName for BankEvent {
    fn event_name(&self) -> &'static str {
        match self {
            BankEvent::Opened { .. } => "Opened",
            BankEvent::Deposited { .. } => "Deposited",
        }
    }
}

fn new_stream() -> StreamId {
    StreamId::from(format!("account-{}", Uuid::new_v4()))
}

#[tokio::test(flavor = "current_thread")]
async fn append_read_conflict_stream_all() {
    // The integration test needs a real Postgres; the workspace gate
    // without `EVENTYR_TEST_PG_URL` skips it (CI runs the container).
    let Ok(url) = std::env::var("EVENTYR_TEST_PG_URL") else {
        eprintln!("eventyr-store-postgres test skipped: EVENTYR_TEST_PG_URL is not set");
        return;
    };
    let store = PgStore::connect(&url).await.expect("connect and migrate");
    let stream = new_stream();

    // 1. ExpectEmpty on a brand-new stream.
    let committed = store
        .append(
            &stream,
            ExpectedVersion::Empty,
            vec![NewEvent::new(BankEvent::Opened { owner: "me".into() })],
        )
        .await
        .expect("the empty expectation matches a new stream");
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].version, Version::new(1));

    // 2. Retry the empty expectation: the advisory lock + live-max
    //    check surface a conflict against the row we just wrote.
    let conflict = store
        .append(
            &stream,
            ExpectedVersion::Empty,
            vec![NewEvent::new(BankEvent::Deposited { amount: 1 })],
        )
        .await
        .expect_err("the stream is no longer empty");
    assert!(matches!(
        conflict,
        eventyr_core::error::StoreError::Conflict { current } if current == Version::new(1)
    ));

    // 3. Exact matches the live max; the wrong exact value conflicts.
    let conflict = store
        .append(
            &stream,
            ExpectedVersion::Exact(Version::new(5)),
            vec![NewEvent::new(BankEvent::Deposited { amount: 1 })],
        )
        .await
        .expect_err("the stream is at 1, not 5");
    assert!(matches!(
        conflict,
        eventyr_core::error::StoreError::Conflict { current } if current == Version::new(1)
    ));

    store
        .append(
            &stream,
            ExpectedVersion::Exact(Version::new(1)),
            vec![
                NewEvent::new(BankEvent::Deposited { amount: 10 }),
                NewEvent::new(BankEvent::Deposited { amount: 20 }),
            ],
        )
        .await
        .expect("the exact expectation commits");

    // Any always appends.
    let committed = store
        .append(
            &stream,
            ExpectedVersion::Any,
            vec![NewEvent::new(BankEvent::Deposited { amount: 30 })],
        )
        .await
        .expect("`Any` appends unconditionally");
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].version, Version::new(4));

    // 4. Read the stream: only events after the exclusive bound come
    //    back, in stream order, with versions and sequences intact.
    let tail = store
        .stream(&stream, Version::new(2))
        .try_collect::<Vec<EventEnvelope<BankEvent>>>()
        .await
        .expect("stream read");
    assert_eq!(tail.len(), 2);
    assert_eq!(tail[0].version, Version::new(3));
    assert_eq!(tail[1].event, BankEvent::Deposited { amount: 30 });
    assert!(tail[1].metadata.causation_id.is_none());

    // 5. The whole history, from the start.
    let all = store
        .stream(&stream, Version::EMPTY)
        .try_collect::<Vec<EventEnvelope<BankEvent>>>()
        .await
        .expect("full stream");
    assert_eq!(
        all.iter().map(|event| event.version).collect::<Vec<_>>(),
        vec![
            Version::new(1),
            Version::new(2),
            Version::new(3),
            Version::new(4)
        ]
    );
    assert_eq!(
        all.iter().map(|event| &event.event).collect::<Vec<_>>(),
        vec![
            &BankEvent::Opened { owner: "me".into() },
            &BankEvent::Deposited { amount: 10 },
            &BankEvent::Deposited { amount: 20 },
            &BankEvent::Deposited { amount: 30 }
        ]
    );

    // 6. A second stream proves the global sequence orders across
    //    streams and the advisory locks don't hold them back. The
    //    sequence numbers are monotonic across runs, so we assert on
    //    relative order, not absolute values.
    let other = new_stream();
    store
        .append(
            &other,
            ExpectedVersion::Empty,
            vec![NewEvent::new(BankEvent::Opened {
                owner: "them".into(),
            })],
        )
        .await
        .expect("other stream opens");

    let base = store
        .stream(&stream, Version::EMPTY)
        .try_collect::<Vec<EventEnvelope<BankEvent>>>()
        .await
        .expect("base")[0]
        .sequence;
    let events = store
        .stream_all(base)
        .try_collect::<Vec<EventEnvelope<BankEvent>>>()
        .await
        .expect("global stream");
    // After our first stream's first event: three deposits and the
    // other open (the other stream's first event comes after ours).
    assert_eq!(events.len(), 4, "three deposits + the other open");
    assert_eq!(events[0].event, BankEvent::Deposited { amount: 10 });
    assert_eq!(events[1].event, BankEvent::Deposited { amount: 20 });
    assert_eq!(events[2].event, BankEvent::Deposited { amount: 30 });
    assert_eq!(
        events[3].event,
        BankEvent::Opened {
            owner: "them".into()
        }
    );
    assert!(events[0].sequence < events[1].sequence);
    assert!(events[1].sequence < events[2].sequence);
    assert!(events[2].sequence < events[3].sequence);

    // 7. Correlation/causation ids ride the metadata through and back.
    let stream = new_stream();
    store
        .append(
            &stream,
            ExpectedVersion::Empty,
            vec![NewEvent::new(BankEvent::Opened { owner: "id".into() })],
        )
        .await
        .expect("opened");
    let meta = Metadata {
        causation_id: Some("cmd-42".into()),
        correlation_id: Some("corr-7".into()),
        #[cfg(feature = "time")]
        timestamp: None,
    };
    let evt = NewEvent {
        event: BankEvent::Deposited { amount: 5 },
        metadata: meta,
    };
    store
        .append(&stream, ExpectedVersion::Exact(Version::new(1)), vec![evt])
        .await
        .expect("with metadata");
    let back = store
        .stream(&stream, Version::new(1))
        .try_collect::<Vec<EventEnvelope<BankEvent>>>()
        .await
        .expect("read back");
    assert_eq!(back[0].metadata.causation_id, Some("cmd-42".into()));
    assert_eq!(back[0].metadata.correlation_id, Some("corr-7".into()));
    #[cfg(feature = "time")]
    assert!(back[0].metadata.timestamp.is_some());
}
