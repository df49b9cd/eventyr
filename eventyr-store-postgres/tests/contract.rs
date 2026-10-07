//! The store contract against a real Postgres: the proof that `PgStore`
//! honours the protocol the in-memory store is compared to.
//!
//! `#[ignore]`d like every Postgres test here — run with
//! `cargo test -p eventyr-store-postgres -- --ignored` and
//! `EVENTYR_TEST_PG_URL` pointing at a real Postgres.

mod common;

use eventyr_core::vocabulary::StreamId;
use eventyr_store_postgres::PgStore;
use eventyr_store_testing::{ParityEvent, PayloadEvent};

static HEAVY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_store_passes_the_contract() {
    let url = common::url();
    let runtime = common::runtime();
    let make_store = || common::fresh_store(&runtime, &url, "contract");
    // Entered for the sync suite so the sqlx futures it polls find this
    // runtime's reactor; dropped before `block_on`, which an entered
    // runtime refuses.
    {
        let _guard = runtime.enter();
        eventyr_store_testing::event_store_contract::<PayloadEvent, _>(make_store);
        eventyr_store_testing::streams_all_contract::<PayloadEvent, _>(make_store);
        eventyr_store_testing::event_store_batch_contract::<PayloadEvent, _>(make_store);
    }
    runtime.block_on(common::cleanup_schemas());
}

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_store_passes_the_query_append_contract() {
    let url = common::url();
    let runtime = common::runtime();
    {
        let _guard = runtime.enter();
        eventyr_store_testing::query_append_contract(|| {
            common::fresh_store(&runtime, &url, "contract")
        });
    }
    runtime.block_on(common::cleanup_schemas());
}

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_store_passes_the_lifecycle_contracts() {
    let url = common::url();
    let runtime = common::runtime();
    {
        let _guard = runtime.enter();
        eventyr_store_testing::lifecycle_contract::<PayloadEvent, _>(|| {
            common::fresh_store(&runtime, &url, "contract")
        });
        eventyr_store_testing::lifecycle_query_append_contract(|| {
            common::fresh_store(&runtime, &url, "contract")
        });
    }
    runtime.block_on(common::cleanup_schemas());
}

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_store_passes_the_filtered_read_contract() {
    let url = common::url();
    let runtime = common::runtime();
    {
        let _guard = runtime.enter();
        eventyr_store_testing::filtered_read_contract::<ParityEvent, _>(|| {
            common::fresh_store(&runtime, &url, "contract")
        });
    }
    runtime.block_on(common::cleanup_schemas());
}

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_snapshot_store_passes_the_snapshot_contract() {
    let url = common::url();
    let runtime = common::runtime();
    {
        let _guard = runtime.enter();
        eventyr_store_testing::snapshot_contract::<u64, _>(|| {
            let store: PgStore<PayloadEvent> = common::fresh_store(&runtime, &url, "contract");
            eventyr_store_postgres::snapshots::PgSnapshotStore::new(&store)
        });
    }
    runtime.block_on(common::cleanup_schemas());
}

#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn pg_checkpoint_store_passes_the_checkpoint_store_contract() {
    let url = common::url();
    let runtime = common::runtime();
    {
        let _guard = runtime.enter();
        eventyr_subscription::checkpoint::checkpoint_store_contract(|| {
            let store: PgStore<PayloadEvent> = common::fresh_store(&runtime, &url, "contract");
            eventyr_store_postgres::PgCheckpointStore::new(&store)
        });
    }
    runtime.block_on(common::cleanup_schemas());
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
    use eventyr_core::boundary::enrollment::{Enroll, EnrollmentEvent};
    use eventyr_core::boundary::{BoundaryMachine, BoundaryOutcome, Decision};
    use eventyr_core::envelope::NewEvent;
    use eventyr_core::vocabulary::ExpectedVersion;
    use eventyr_core::write::RetryPolicy;
    use eventyr_store::store::{EventStore, QueryAppend};

    const SEATS: u32 = 3;
    const RIVALS: usize = 12;

    let url = common::url();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        // The suite serializes its own connection-heavy tests, so the
        // default 100-connection server always leaves room. Siblings
        // taking 4-connection pools and a listener connection apiece
        // could otherwise starve this many-writer roster.
        let _heavy = HEAVY.lock().await;
        let one: PgStore<EnrollmentEvent> =
            common::fresh_store_async(&url, "contract", RIVALS as u32).await;
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
        common::cleanup_schemas().await;
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

/// Poll `pg_locks` until an advisory lock is *waiting* (not granted):
/// the deterministic form of "the other task is blocked on the lock",
/// replacing a hoped-long-enough sleep. Panics after five seconds.
async fn wait_for_a_waiting_lock(pool: &sqlx::PgPool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
        )
        .fetch_one(pool)
        .await
        .expect("read pg_locks");
        if waiting > 0 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no writer ever queued on a lock"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// The deterministic proof that the condition check cannot miss an
/// in-flight write: a raw transaction inserts a matching event and
/// holds it uncommitted; `append_if` must block behind it rather than
/// read past it, and — once it commits — fail with the conflict.
///
/// Under plain READ COMMITTED, without the store's commit-order lock,
/// the check would not see the uncommitted row, the append would commit,
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

    let url = common::url();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let _heavy = HEAVY.lock().await;
        let store: PgStore<EnrollmentEvent> = common::fresh_store_async(&url, "contract", 4).await;
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

        // A rival enrollment, inserted and held open. It takes the
        // commit-order lock (migration 0006/0010) and keeps it until the
        // transaction ends.
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

        // Ours is queued on the rival's commit-order lock, not finished
        // past it — poll the lock table instead of hoping a sleep was
        // long enough.
        wait_for_a_waiting_lock(store.pool()).await;
        assert!(
            !ours.is_finished(),
            "the conditional append read past an uncommitted matching write"
        );

        rival.commit().await.expect("the rival commits");
        match ours.await.expect("the task completes") {
            Err(StoreError::QueryConflict { sequence }) => assert!(sequence > read_position),
            other => panic!("expected a query conflict after the rival commits, got {other:?}"),
        }
        common::cleanup_schemas().await;
    });
}

/// `StreamsAll`'s visibility rule against a real Postgres: a later
/// sequence must not become visible before an earlier one that will
/// still commit.
///
/// A raw transaction appends (drawing the next sequence, and taking the
/// commit-order lock) and stays open; a second append to another stream
/// must wait for it rather than commit the later sequence first. If it
/// committed first, a projector polling in between would checkpoint past
/// the open transaction's event and never deliver it.
#[test]
#[ignore = "needs EVENTYR_TEST_PG_URL pointing at a real Postgres"]
fn a_later_sequence_never_commits_before_an_earlier_one() {
    use eventyr_core::envelope::NewEvent;
    use eventyr_core::vocabulary::{ExpectedVersion, Sequence};
    use eventyr_store::store::{EventStore, StreamsAll};
    use futures::TryStreamExt;

    let url = common::url();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let _heavy = HEAVY.lock().await;
        let store: PgStore<PayloadEvent> = common::fresh_store_async(&url, "contract", 4).await;

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
                        vec![NewEvent::new(PayloadEvent::from(2))],
                    )
                    .await
            })
        };

        // The second append queues on the first's commit-order lock
        // before it can draw its sequence: nothing is visible, and the
        // wait is observed in the lock table, not guessed from a sleep.
        wait_for_a_waiting_lock(store.pool()).await;
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
        common::cleanup_schemas().await;
    });
}
