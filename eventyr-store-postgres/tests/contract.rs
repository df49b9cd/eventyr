//! The store contract against a real Postgres: the proof that `PgStore`
//! honours the protocol the in-memory store is compared to.
//!
//! `#[ignore]`d like every Postgres test here — run with
//! `cargo test -p eventyr-store-postgres -- --ignored` and
//! `EVENTYR_TEST_PG_URL` pointing at a real Postgres.

use serde::{Deserialize, Serialize};

use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::StreamId;
use eventyr_store_postgres::PgStore;

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
enum ContractEvent {
    Payload { value: u64 },
}

impl EventName for ContractEvent {
    fn event_name(&self) -> &'static str {
        match self {
            ContractEvent::Payload { .. } => "Payload",
        }
    }
}

impl From<u64> for ContractEvent {
    fn from(value: u64) -> Self {
        ContractEvent::Payload { value }
    }
}

/// A store over a fresh, empty schema.
///
/// The suite calls `make_store()` once per check and expects an empty
/// store each time — `stream_all` reads the whole log, so a shared
/// database would hand later checks every earlier check's events. Each
/// call creates its own schema, pins a one-connection pool's
/// `search_path` to it, and migrates into it.
fn fresh_store<E>(runtime: &tokio::runtime::Runtime, url: &str) -> PgStore<E> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let schema = format!(
        "contract_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    runtime.block_on(async {
        let admin = sqlx::PgPool::connect(url).await.expect("connect");
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .expect("create schema");
        admin.close().await;
        let options: sqlx::postgres::PgConnectOptions = url.parse().expect("url");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options.options([("search_path", schema.as_str())]))
            .await
            .expect("connect to schema");
        eventyr_store_postgres::store::migrate(&pool)
            .await
            .expect("migrate");
        PgStore::new(pool)
    })
}

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_store_passes_the_contract() {
    let url = std::env::var("EVENTYR_TEST_PG_URL")
        .expect("EVENTYR_TEST_PG_URL must point at a real Postgres");
    // Multi-thread, not current-thread: the suite drives each future
    // with `futures::executor::block_on`, which parks this thread, so
    // the I/O reactor has to run on the runtime's own worker threads.
    // A current-thread runtime only drives its reactor inside its own
    // `block_on`, and sqlx's first socket wait would never wake.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let make_store = || fresh_store(&runtime, &url);
    // Entered so the sqlx futures the suite polls find this runtime.
    let _guard = runtime.enter();
    eventyr_store_testing::event_store_contract::<ContractEvent, _>(make_store);
    eventyr_store_testing::streams_all_contract::<ContractEvent, _>(make_store);
    eventyr_store_testing::event_store_batch_contract::<ContractEvent, _>(make_store);
}

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_store_passes_the_query_append_contract() {
    let url = std::env::var("EVENTYR_TEST_PG_URL")
        .expect("EVENTYR_TEST_PG_URL must point at a real Postgres");
    // Multi-thread for the same reason as the main contract above.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let _guard = runtime.enter();
    eventyr_store_testing::query_append_contract(|| fresh_store(&runtime, &url));
}

/// A smoke test under real concurrency: many boundary decisions race
/// for the seats of one course over a multi-connection pool, and
/// exactly `seats` commit. Timing-dependent — it rarely catches a
/// broken condition on its own; the deterministic proof is
/// [`a_conditional_append_waits_out_an_in_flight_writer`].
///
/// Each racer writes its *own* stream (the student's), so no stream
/// lock is shared: only the condition's serialization can stop an
/// oversell. (Routing to the course's stream would let its advisory
/// lock hide a broken condition.)
#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_store_serializes_racing_boundary_decisions() {
    use eventyr_core::boundary::BoundaryOutcome;
    use eventyr_core::boundary::enrollment::{Enroll, EnrollmentEvent};
    use eventyr_core::boundary::{BoundaryMachine, Decision};
    use eventyr_core::envelope::NewEvent;
    use eventyr_core::vocabulary::ExpectedVersion;
    use eventyr_core::write::RetryPolicy;
    use eventyr_store::store::{EventStore, QueryAppend};

    const SEATS: u32 = 3;
    const RIVALS: usize = 12;

    let url = std::env::var("EVENTYR_TEST_PG_URL")
        .expect("EVENTYR_TEST_PG_URL must point at a real Postgres");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let one: PgStore<EnrollmentEvent> = fresh_store_async(&url, RIVALS as u32).await;
        one.append(
            &StreamId::from("course-c1"),
            ExpectedVersion::Empty,
            vec![NewEvent::new(EnrollmentEvent::CourseDefined {
                course: "c1".into(),
                seats: SEATS,
            })],
        )
        .await
        .expect("define the course");

        let tasks: Vec<_> = (0..RIVALS)
            .map(|rival| {
                let store = one.clone();
                tokio::spawn(async move {
                    let mut machine = BoundaryMachine::new(
                        IntoStudentStream(Enroll::new("c1", &format!("s{rival}"))),
                        RetryPolicy::new(RIVALS as u32),
                    );
                    eventyr_store::driver::drive_boundary(&mut machine, &store).await
                })
            })
            .collect();
        let mut committed = 0;
        for task in tasks {
            match task.await.expect("the task completes") {
                BoundaryOutcome::Committed { .. } => committed += 1,
                BoundaryOutcome::Rejected(_) => {}
                other => panic!("every racer commits or is turned away, got {other:?}"),
            }
        }
        assert_eq!(committed, SEATS as usize);

        let enrolled = futures::TryStreamExt::try_collect::<Vec<_>>(QueryAppend::read(
            &one,
            &Enroll::new("c1", "nobody").query(),
            eventyr_core::vocabulary::Sequence::START,
        ))
        .await
        .expect("read back");
        let enrolled = enrolled
            .iter()
            .filter(|e| matches!(e.event, EnrollmentEvent::Enrolled { .. }))
            .count();
        assert_eq!(enrolled, SEATS as usize, "no seat was oversold");
    });
}

/// The fixture's enrollment, routed to the student's stream instead of
/// the course's — see the race test above.
struct IntoStudentStream(eventyr_core::boundary::enrollment::Enroll);

impl eventyr_core::boundary::Decision for IntoStudentStream {
    type Event = eventyr_core::boundary::enrollment::EnrollmentEvent;
    type State = eventyr_core::boundary::enrollment::EnrollmentState;
    type Error = eventyr_core::boundary::enrollment::EnrollmentError;

    fn query(&self) -> eventyr_core::boundary::Query {
        self.0.query()
    }

    fn initial(&self) -> Self::State {
        self.0.initial()
    }

    fn apply(&self, state: &mut Self::State, event: &Self::Event) {
        self.0.apply(state, event);
    }

    fn decide(
        &self,
        state: &Self::State,
    ) -> eventyr_core::boundary::BoundaryDecision<Self::Event, Self::Error> {
        use eventyr_core::boundary::BoundaryDecision;
        use eventyr_core::boundary::enrollment::{EnrollmentError, EnrollmentEvent, MAX_COURSES};
        match state.seats {
            None => BoundaryDecision::reject(EnrollmentError::NoSuchCourse),
            Some(_) if state.already => BoundaryDecision::reject(EnrollmentError::AlreadyEnrolled),
            Some(seats) if state.enrolled >= seats => {
                BoundaryDecision::reject(EnrollmentError::CourseFull)
            }
            Some(_) if state.student_courses >= MAX_COURSES => {
                BoundaryDecision::reject(EnrollmentError::StudentAtLimit)
            }
            Some(_) => BoundaryDecision::to(
                StreamId::from(format!("student-{}", self.0.student)),
                vec![EnrollmentEvent::Enrolled {
                    course: self.0.course.clone(),
                    student: self.0.student.clone(),
                }],
            ),
        }
    }
}

/// [`fresh_store`] for callers already inside a runtime, with a pool
/// wide enough for real concurrency.
async fn fresh_store_async<E>(url: &str, connections: u32) -> PgStore<E> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let schema = format!(
        "race_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let admin = sqlx::PgPool::connect(url).await.expect("connect");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create schema");
    admin.close().await;
    let options: sqlx::postgres::PgConnectOptions = url.parse().expect("url");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(connections)
        .connect_with(options.options([("search_path", schema.as_str())]))
        .await
        .expect("connect to schema");
    eventyr_store_postgres::store::migrate(&pool)
        .await
        .expect("migrate");
    PgStore::new(pool)
}

/// The deterministic proof that the condition check cannot miss an
/// in-flight write: a raw transaction inserts a matching event and
/// holds it uncommitted; `append_if` must block behind it rather than
/// read past it, and — once it commits — fail with the conflict.
///
/// Under plain READ COMMITTED, without the store's table lock, the
/// check would not see the uncommitted row, the append would commit,
/// and the course would be oversold.
#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn a_conditional_append_waits_out_an_in_flight_writer() {
    use eventyr_core::batch::StreamAppend;
    use eventyr_core::boundary::enrollment::{Enroll, EnrollmentEvent};
    use eventyr_core::boundary::{AppendCondition, Decision};
    use eventyr_core::envelope::NewEvent;
    use eventyr_core::error::StoreError;
    use eventyr_core::vocabulary::{ExpectedVersion, Sequence};
    use eventyr_store::store::{EventStore, QueryAppend};

    let url = std::env::var("EVENTYR_TEST_PG_URL")
        .expect("EVENTYR_TEST_PG_URL must point at a real Postgres");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let store: PgStore<EnrollmentEvent> = fresh_store_async(&url, 4).await;
        let defined = store
            .append(
                &StreamId::from("course-c1"),
                ExpectedVersion::Empty,
                vec![NewEvent::new(EnrollmentEvent::CourseDefined {
                    course: "c1".into(),
                    seats: 1,
                })],
            )
            .await
            .expect("define the course");
        let read_position: Sequence = defined[0].sequence;

        // A rival enrollment, inserted and held open.
        let mut rival = store.pool().begin().await.expect("begin");
        sqlx::query(
            "SELECT * FROM append_events(0::smallint, 0, 'student-s9', ARRAY['Enrolled'], \
             ARRAY['{\"Enrolled\": {\"course\": \"c1\", \"student\": \"s9\"}}'::jsonb], \
             ARRAY[NULL]::text[], ARRAY[NULL]::text[], ARRAY[NULL]::text[])",
        )
        .execute(&mut *rival)
        .await
        .expect("the rival inserts");

        let ours = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .append_if(
                        vec![StreamAppend {
                            stream_id: StreamId::from("student-s1"),
                            expected: ExpectedVersion::Any,
                            events: vec![NewEvent::new(EnrollmentEvent::Enrolled {
                                course: "c1".into(),
                                student: "s1".into(),
                            })],
                        }],
                        AppendCondition {
                            query: Enroll::new("c1", "s1").query(),
                            after: read_position,
                        },
                    )
                    .await
            })
        };

        // Ours must be waiting on the rival, not finished past it.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !ours.is_finished(),
            "the conditional append read past an uncommitted matching write"
        );

        rival.commit().await.expect("the rival commits");
        match ours.await.expect("the task completes") {
            Err(StoreError::QueryConflict { sequence }) => assert!(sequence > read_position),
            other => panic!("expected a query conflict after the rival commits, got {other:?}"),
        }
    });
}

/// `StreamsAll`'s visibility rule against a real Postgres: a later
/// sequence must not become visible before an earlier one that will
/// still commit.
///
/// A raw transaction appends (drawing the next sequence) and stays
/// open; a second append to another stream must wait for it rather
/// than commit the later sequence first. If it committed first, a
/// projector polling in between would checkpoint past the open
/// transaction's event and never deliver it.
#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn a_later_sequence_never_commits_before_an_earlier_one() {
    use eventyr_core::envelope::NewEvent;
    use eventyr_core::vocabulary::{ExpectedVersion, Sequence};
    use eventyr_store::store::{EventStore, StreamsAll};
    use futures::TryStreamExt;

    let url = std::env::var("EVENTYR_TEST_PG_URL")
        .expect("EVENTYR_TEST_PG_URL must point at a real Postgres");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let store: PgStore<ContractEvent> = fresh_store_async(&url, 4).await;

        let mut first = store.pool().begin().await.expect("begin");
        sqlx::query(
            "SELECT * FROM append_events(0::smallint, 0, 'stream-a', ARRAY['Payload'], \
             ARRAY['{\"Payload\": {\"value\": 1}}'::jsonb], \
             ARRAY[NULL]::text[], ARRAY[NULL]::text[], ARRAY[NULL]::text[])",
        )
        .execute(&mut *first)
        .await
        .expect("the first append draws its sequence");

        let second = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .append(
                        &StreamId::from("stream-b"),
                        ExpectedVersion::Any,
                        vec![NewEvent::new(ContractEvent::from(2))],
                    )
                    .await
            })
        };

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let visible: Vec<_> = store
            .stream_all(Sequence::START)
            .try_collect()
            .await
            .expect("read");
        assert!(
            visible.is_empty(),
            "a later sequence became visible while an earlier one was in flight: {:?}",
            visible.iter().map(|e| e.sequence).collect::<Vec<_>>()
        );
        assert!(!second.is_finished(), "the second append did not wait");

        first.commit().await.expect("commit");
        second
            .await
            .expect("task")
            .expect("the second append commits");
        let sequences: Vec<_> = store
            .stream_all(Sequence::START)
            .try_collect::<Vec<_>>()
            .await
            .expect("read")
            .into_iter()
            .map(|e| (e.stream_id.as_str().to_owned(), e.sequence.as_u64()))
            .collect();
        assert_eq!(sequences[0].0, "stream-a");
        assert_eq!(sequences[1].0, "stream-b");
        assert!(sequences[0].1 < sequences[1].1);
    });
}

/// The filtered-read contract needs a stored name that depends on the
/// payload: even values are `"Even"`, odd ones `"Odd"`.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
struct ParityEvent(u64);

impl EventName for ParityEvent {
    fn event_name(&self) -> &'static str {
        if self.0.is_multiple_of(2) {
            "Even"
        } else {
            "Odd"
        }
    }
}

impl From<u64> for ParityEvent {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_store_passes_the_filtered_read_contract() {
    let url = std::env::var("EVENTYR_TEST_PG_URL")
        .expect("EVENTYR_TEST_PG_URL must point at a real Postgres");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let _guard = runtime.enter();
    eventyr_store_testing::filtered_read_contract::<ParityEvent, _>(|| fresh_store(&runtime, &url));
}
