//! The [`QueryAppend`] contract checks (roadmap 0.7.1).
//!
//! A store that serves dynamic consistency boundaries proves it here:
//! reads select by type and tags in global order, a condition fails on
//! exactly the matching events committed after its position, a failed
//! condition writes nothing, and the boundary machine driven against
//! the store reaches the outcomes the protocol promises.
//!
//! The suite speaks the enrollment fixture from
//! [`eventyr_core::boundary::enrollment`]: stores implementing this
//! port need a typed event that is [`EventName`] + [`Tagged`], so the
//! suite fixes one rather than abstracting a payload family.
//!
//! [`EventName`]: eventyr_core::event_name::EventName
//! [`Tagged`]: eventyr_core::boundary::Tagged

use eventyr_core::batch::{CommittedStream, StreamAppend};
use eventyr_core::boundary::enrollment::{Enroll, EnrollmentError, EnrollmentEvent};
use eventyr_core::boundary::{
    AppendCondition, BoundaryMachine, BoundaryOutcome, Query, QueryItem, Tag,
};
use eventyr_core::envelope::{EventEnvelope, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId};
use eventyr_core::write::RetryPolicy;
use eventyr_store::driver::drive_boundary;
use eventyr_store::store::{EventStore, QueryAppend, StreamsAll};
use futures::TryStreamExt;
use futures::executor::block_on;

/// Run the [`QueryAppend`] contract against `make_store`'s fresh stores.
pub fn query_append_contract<S>(make_store: impl Fn() -> S)
where
    S: QueryAppend<Event = EnrollmentEvent>,
{
    reads_select_by_type_and_tags_in_global_order(&make_store());
    reads_start_after_the_exclusive_bound(&make_store());
    an_empty_query_reads_nothing(&make_store());
    a_condition_without_matches_appends(&make_store());
    a_condition_fails_on_a_later_match_and_writes_nothing(&make_store());
    a_condition_ignores_matches_at_or_before_its_position(&make_store());
    a_condition_ignores_events_its_query_does_not_select(&make_store());
    stream_expectations_are_still_checked(&make_store());
    a_conditional_batch_naming_a_stream_twice_is_refused(&make_store());
    the_boundary_machine_commits_and_rejects(&make_store());
    the_boundary_machine_retries_past_a_concurrent_write(&make_store());
}

fn defined(course: &str, seats: u32) -> EnrollmentEvent {
    EnrollmentEvent::CourseDefined {
        course: course.into(),
        seats,
    }
}

fn enrolled(course: &str, student: &str) -> EnrollmentEvent {
    EnrollmentEvent::Enrolled {
        course: course.into(),
        student: student.into(),
    }
}

fn to(stream: &str, events: Vec<EnrollmentEvent>) -> StreamAppend<EnrollmentEvent> {
    StreamAppend {
        stream_id: StreamId::from(stream),
        expected: ExpectedVersion::Any,
        events: events.into_iter().map(NewEvent::new).collect(),
    }
}

fn seed<S: EventStore<Event = EnrollmentEvent>>(
    store: &S,
    stream: &str,
    events: Vec<EnrollmentEvent>,
) {
    block_on(store.append(
        &StreamId::from(stream),
        ExpectedVersion::Any,
        events.into_iter().map(NewEvent::new).collect(),
    ))
    .expect("seeding appends");
}

fn read<S: QueryAppend<Event = EnrollmentEvent>>(
    store: &S,
    query: &Query,
    after: Sequence,
) -> Vec<EventEnvelope<EnrollmentEvent>> {
    block_on(store.read(query, after).try_collect()).expect("the read succeeds")
}

fn append_if<S: QueryAppend<Event = EnrollmentEvent>>(
    store: &S,
    appends: Vec<StreamAppend<EnrollmentEvent>>,
    query: Query,
    after: Sequence,
) -> Result<Vec<CommittedStream<EnrollmentEvent>>, StoreError> {
    block_on(store.append_if(appends, AppendCondition { query, after }))
}

fn course(course: &str) -> Query {
    Query::of(QueryItem::any().tags([Tag::of("course", course)]))
}

fn head<S: StreamsAll<Event = EnrollmentEvent>>(store: &S) -> Sequence {
    block_on(store.stream_all(Sequence::START).try_collect::<Vec<_>>())
        .expect("the global read succeeds")
        .last()
        .map_or(Sequence::START, |envelope| envelope.sequence)
}

fn reads_select_by_type_and_tags_in_global_order<S: QueryAppend<Event = EnrollmentEvent>>(
    store: &S,
) {
    seed(store, "course-c1", vec![defined("c1", 3)]);
    seed(store, "course-c2", vec![defined("c2", 3)]);
    seed(store, "course-c1", vec![enrolled("c1", "s1")]);
    seed(
        store,
        "student-s1",
        vec![EnrollmentEvent::AddressChanged {
            student: "s1".into(),
        }],
    );
    seed(store, "course-c2", vec![enrolled("c2", "s1")]);

    // Tag-only: everything about c1.
    let events = read(store, &course("c1"), Sequence::START);
    let got: Vec<_> = events.iter().map(|e| e.event.clone()).collect();
    assert_eq!(got, vec![defined("c1", 3), enrolled("c1", "s1")]);

    // Type + tag across streams, in global order.
    let query = Query::of(
        QueryItem::any()
            .types(["Enrolled"])
            .tags([Tag::of("student", "s1")]),
    );
    let events = read(store, &query, Sequence::START);
    let got: Vec<_> = events.iter().map(|e| e.event.clone()).collect();
    assert_eq!(got, vec![enrolled("c1", "s1"), enrolled("c2", "s1")]);
    assert!(events.windows(2).all(|w| w[0].sequence < w[1].sequence));

    // Every tag of an item is required.
    let both =
        Query::of(QueryItem::any().tags([Tag::of("course", "c2"), Tag::of("student", "s1")]));
    let got: Vec<_> = read(store, &both, Sequence::START)
        .into_iter()
        .map(|e| e.event)
        .collect();
    assert_eq!(got, vec![enrolled("c2", "s1")]);

    // Items are a union; envelopes keep their stream positions.
    let union = course("c1").or(QueryItem::any().types(["AddressChanged"]));
    let events = read(store, &union, Sequence::START);
    assert_eq!(events.len(), 3);
    assert_eq!(events[2].stream_id.as_str(), "student-s1");
    assert_eq!(events[2].version.as_u64(), 1);
}

fn reads_start_after_the_exclusive_bound<S: QueryAppend<Event = EnrollmentEvent>>(store: &S) {
    seed(
        store,
        "course-c1",
        vec![defined("c1", 3), enrolled("c1", "s1"), enrolled("c1", "s2")],
    );
    let all = read(store, &course("c1"), Sequence::START);
    let after_first = read(store, &course("c1"), all[0].sequence);
    assert_eq!(after_first, all[1..].to_vec());
    assert!(read(store, &course("c1"), all[2].sequence).is_empty());
}

fn an_empty_query_reads_nothing<S: QueryAppend<Event = EnrollmentEvent>>(store: &S) {
    seed(store, "course-c1", vec![defined("c1", 3)]);
    assert!(read(store, &Query::none(), Sequence::START).is_empty());
    assert_eq!(read(store, &Query::all(), Sequence::START).len(), 1);
}

fn a_condition_without_matches_appends<S: QueryAppend<Event = EnrollmentEvent>>(store: &S) {
    seed(store, "course-c2", vec![defined("c2", 3)]);
    let committed = append_if(
        store,
        vec![to("course-c1", vec![defined("c1", 3)])],
        course("c1"),
        Sequence::START,
    )
    .expect("nothing matches c1 yet");
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].events.len(), 1);
    assert_eq!(committed[0].events[0].version.as_u64(), 1);
    assert_eq!(read(store, &course("c1"), Sequence::START).len(), 1);
}

fn a_condition_fails_on_a_later_match_and_writes_nothing<
    S: QueryAppend<Event = EnrollmentEvent>,
>(
    store: &S,
) {
    seed(store, "course-c1", vec![defined("c1", 3)]);
    seed(store, "course-c1", vec![enrolled("c1", "s1")]);
    let latest = head(store);

    let error = append_if(
        store,
        vec![
            to("course-c1", vec![enrolled("c1", "s2")]),
            to("audit", vec![enrolled("c1", "s2")]),
        ],
        course("c1"),
        Sequence::START,
    )
    .expect_err("c1 has events after START");
    match error {
        StoreError::QueryConflict { sequence } => assert_eq!(sequence, latest),
        other => panic!("expected a query conflict, got {other:?}"),
    }
    // Atomic: neither stream was written.
    assert_eq!(head(store), latest);
}

fn a_condition_ignores_matches_at_or_before_its_position<
    S: QueryAppend<Event = EnrollmentEvent>,
>(
    store: &S,
) {
    seed(store, "course-c1", vec![defined("c1", 3)]);
    let position = head(store);
    append_if(
        store,
        vec![to("course-c1", vec![enrolled("c1", "s1")])],
        course("c1"),
        position,
    )
    .expect("the only match is at the condition's position");
}

fn a_condition_ignores_events_its_query_does_not_select<S: QueryAppend<Event = EnrollmentEvent>>(
    store: &S,
) {
    seed(
        store,
        "course-c2",
        vec![defined("c2", 3), enrolled("c2", "s1")],
    );
    append_if(
        store,
        vec![to("course-c1", vec![defined("c1", 3)])],
        course("c1"),
        Sequence::START,
    )
    .expect("c2's events are outside c1's boundary");
}

fn stream_expectations_are_still_checked<S: QueryAppend<Event = EnrollmentEvent>>(store: &S) {
    seed(store, "course-c1", vec![defined("c1", 3)]);
    let before = head(store);
    let error = append_if(
        store,
        vec![StreamAppend {
            stream_id: StreamId::from("course-c1"),
            expected: ExpectedVersion::Empty,
            events: vec![NewEvent::new(enrolled("c1", "s1"))],
        }],
        Query::none(),
        Sequence::START,
    )
    .expect_err("the stream is not empty");
    assert!(matches!(error, StoreError::Conflict { .. }));
    assert_eq!(head(store), before);
}

fn a_conditional_batch_naming_a_stream_twice_is_refused<S: QueryAppend<Event = EnrollmentEvent>>(
    store: &S,
) {
    seed(store, "course-c1", vec![defined("c1", 3)]);
    let before = head(store);
    let error = append_if(
        store,
        vec![
            to("course-c1", vec![enrolled("c1", "s1")]),
            to("course-c1", vec![enrolled("c1", "s2")]),
        ],
        Query::none(),
        Sequence::START,
    )
    .expect_err("a stream may appear once per batch");
    assert!(matches!(error, StoreError::Other(_)), "{error:?}");
    assert_eq!(head(store), before, "nothing was written");
}

fn the_boundary_machine_commits_and_rejects<S: QueryAppend<Event = EnrollmentEvent>>(store: &S) {
    seed(store, "course-c1", vec![defined("c1", 1)]);

    let mut machine = BoundaryMachine::new(Enroll::new("c1", "s1"), RetryPolicy::default());
    match block_on(drive_boundary(&mut machine, store)) {
        BoundaryOutcome::Committed { committed } => {
            assert_eq!(committed[0].stream_id.as_str(), "course-c1");
            assert_eq!(committed[0].events[0].event, enrolled("c1", "s1"));
        }
        other => panic!("the first enrollment commits, got {other:?}"),
    }

    // One seat: the second student is turned away by the folded state.
    let mut machine = BoundaryMachine::new(Enroll::new("c1", "s2"), RetryPolicy::default());
    assert!(matches!(
        block_on(drive_boundary(&mut machine, store)),
        BoundaryOutcome::Rejected(EnrollmentError::CourseFull)
    ));
}

fn the_boundary_machine_retries_past_a_concurrent_write<S: QueryAppend<Event = EnrollmentEvent>>(
    store: &S,
) {
    use eventyr_core::boundary::{BoundaryAction, BoundaryInput};

    seed(store, "course-c1", vec![defined("c1", 1)]);
    let mut machine = BoundaryMachine::new(Enroll::new("c1", "s1"), RetryPolicy::new(1));

    // Drive by hand so a rival can commit between the read and the
    // append — the race the condition exists for.
    let BoundaryAction::Read { query, after } = machine.start() else {
        panic!("the machine reads first");
    };
    let events = read(store, &query, after);
    let BoundaryAction::Append { appends, condition } =
        machine.handle(BoundaryInput::Read { events })
    else {
        panic!("one free seat: the machine appends");
    };

    // The rival takes the last seat.
    seed(store, "course-c1", vec![enrolled("c1", "s9")]);

    let error = block_on(store.append_if(appends, condition)).expect_err("the rival moved c1");
    let BoundaryAction::Read { query, after } = machine.handle(BoundaryInput::from(error)) else {
        panic!("a query conflict re-reads");
    };
    let events = read(store, &query, after);
    assert_eq!(events.len(), 1, "the delta is exactly the rival's event");
    assert!(matches!(
        machine.handle(BoundaryInput::Read { events }),
        BoundaryAction::Done(BoundaryOutcome::Rejected(EnrollmentError::CourseFull))
    ));
}

#[cfg(test)]
mod tests {
    use super::query_append_contract;
    use eventyr_store::memory::InMemoryStore;

    #[test]
    fn the_in_memory_store_passes_the_query_append_contract() {
        query_append_contract(InMemoryStore::new);
    }
}
