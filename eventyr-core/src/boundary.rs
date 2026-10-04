//! Dynamic consistency boundaries: the sans-IO machine behind a decision
//! whose boundary is a *query*, not a stream (roadmap 0.7.1).
//!
//! An [`Aggregate`](crate::aggregate::Aggregate) fixes its boundary to
//! one stream; a [`BatchMachine`](crate::batch::BatchMachine) to a fixed
//! set of streams the caller names. Both guard the append by stream
//! version. The Dynamic Consistency Boundary (DCB) model replaces "these
//! streams" with "the events this query selects": events carry
//! [`Tag`]s, a [`Query`] selects events by type and tags, a decision
//! folds what the query selects into whatever state it needs, and the
//! append is guarded by an [`AppendCondition`] — *nothing matching this
//! query was committed after the position I read*. The boundary is
//! chosen per decision and may span what aggregates would have
//! separated, without a saga.
//!
//! What stays the caller's: the query. The machine never computes a
//! boundary (§12's rule against `StreamResolver` stands); the
//! [`Decision`] states its query, the machine reads exactly that.
//!
//! The protocol, mirroring the write machine:
//!
//! 1. [`Read`](BoundaryAction::Read) the decision's [`query`](Decision::query)
//!    after [`Sequence::START`]; the driver answers once with every
//!    matching event in sequence order.
//! 2. Fold them, [`decide`](Decision::decide), and emit one
//!    [`Append`](BoundaryAction::Append) carrying the routed events and
//!    the condition: the decision's [`validation`](Decision::validation)
//!    query (the fold query unless narrowed), after the highest position
//!    the machine has accounted for.
//! 3. On [`Conflict`](BoundaryInput::Conflict) — an event matching the
//!    condition was committed — re-read only the delta, fold it, and
//!    re-decide, within the [`RetryPolicy`] budget.
//!
//! Tags are a pure function of the event ([`Tagged`]), like its stored
//! name ([`EventName`]): a store indexes them from the payload at append
//! time, so the envelope carries nothing new and a tag can never drift
//! from the event it describes.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;

use crate::batch::{CommittedStream, StreamAppend};
use crate::envelope::{EventEnvelope, Metadata, NewEvent};
use crate::error::{ProtocolError, StoreError};
use crate::event_name::EventName;
use crate::vocabulary::{ExpectedVersion, Sequence, StreamId};
use crate::write::RetryPolicy;

/// A domain identifier attached to an event: `"course:c-1"`,
/// `"student:42"`. Queries select events by tag; the consistency
/// boundary of a decision is the set of tags (and types) it queries.
///
/// The string is opaque to the protocol — `kind:value` via
/// [`Tag::of`] is a convention, not a rule.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Tag(String);

impl Tag {
    /// The conventional `"{kind}:{value}"` tag.
    pub fn of(kind: &str, value: impl fmt::Display) -> Self {
        Self(alloc::format!("{kind}:{value}"))
    }

    /// The tag as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for Tag {
    fn from(s: &str) -> Self {
        Self(String::from(s))
    }
}

impl From<String> for Tag {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// The tags an event carries: a pure function of the event, like
/// [`EventName::event_name`].
///
/// Tags are part of the storage schema the way stored names are: a
/// store indexes them at append time, so changing what an existing
/// event variant returns changes which historical events a query
/// selects. Add tags to new variants freely; treat existing ones as
/// pinned.
pub trait Tagged {
    /// The tags this event carries. Order and duplicates are
    /// irrelevant.
    fn tags(&self) -> Vec<Tag>;
}

/// One clause of a [`Query`]: events of one of `types` (any type when
/// empty) carrying *every* tag in `tags` (no tag constraint when
/// empty).
///
/// An item with neither types nor tags selects every event.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct QueryItem {
    /// Accepted stored event names ([`EventName`]); empty accepts any.
    pub types: Vec<&'static str>,
    /// Tags the event must all carry; empty requires none.
    pub tags: Vec<Tag>,
}

impl QueryItem {
    /// An item accepting any type and requiring no tag — every event.
    pub fn any() -> Self {
        Self::default()
    }

    /// Builder-style: accept events of these stored names.
    pub fn types(mut self, types: impl IntoIterator<Item = &'static str>) -> Self {
        self.types.extend(types);
        self
    }

    /// Builder-style: require these tags, all of them.
    pub fn tags(mut self, tags: impl IntoIterator<Item = Tag>) -> Self {
        self.tags.extend(tags);
        self
    }

    /// Whether an event of `event_type` carrying `tags` satisfies this
    /// item.
    pub fn matches(&self, event_type: &str, tags: &[Tag]) -> bool {
        (self.types.is_empty() || self.types.contains(&event_type))
            && self.tags.iter().all(|tag| tags.contains(tag))
    }
}

/// The events a decision reads and guards: the union of its
/// [`QueryItem`]s.
///
/// A query with no items selects nothing — reading it returns no
/// events and a condition over it never fails. [`Query::all`] selects
/// everything.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Query {
    /// The clauses; an event matches the query when it matches any.
    pub items: Vec<QueryItem>,
}

impl Query {
    /// The query selecting no events.
    pub fn none() -> Self {
        Self::default()
    }

    /// The query selecting every event.
    pub fn all() -> Self {
        Self {
            items: alloc::vec![QueryItem::any()],
        }
    }

    /// The query of one item.
    pub fn of(item: QueryItem) -> Self {
        Self {
            items: alloc::vec![item],
        }
    }

    /// Builder-style: add an item (widening the query).
    pub fn or(mut self, item: QueryItem) -> Self {
        self.items.push(item);
        self
    }

    /// Whether an event of `event_type` carrying `tags` is selected.
    pub fn matches(&self, event_type: &str, tags: &[Tag]) -> bool {
        self.items.iter().any(|item| item.matches(event_type, tags))
    }

    /// Whether `event` is selected — [`matches`](Self::matches) over
    /// the event's own name and tags.
    pub fn selects<E: EventName + Tagged>(&self, event: &E) -> bool {
        self.matches(event.event_name(), &event.tags())
    }
}

/// The guard on a boundary append: fail if any event matching `query`
/// was committed after `after`.
///
/// `after` is exclusive, like every lower bound in the protocol:
/// [`Sequence::START`] means "no matching event may exist at all".
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AppendCondition {
    /// The events whose arrival invalidates the decision.
    pub query: Query,
    /// The highest position the decision accounted for.
    pub after: Sequence,
}

/// A decision taken against a dynamic boundary: what to read, how to
/// fold it, and what to decide.
///
/// The implementing value *is* the command — it carries the
/// identifiers its query names (`EnrollStudent { course, student }`),
/// so the query depends on the command without a resolver. Every
/// method is pure.
pub trait Decision {
    /// The domain event type.
    type Event: EventName + Tagged;
    /// The folded state the decision reads.
    type State;
    /// The domain rejection.
    type Error;

    /// The events to fold.
    fn query(&self) -> Query;

    /// The events whose later arrival invalidates the decision —
    /// [`query`](Self::query) unless narrowed. Narrow it when the fold
    /// reads events that cannot change the outcome: a withdrawal folds
    /// deposits to compute the balance, but a concurrent deposit only
    /// raises it and need not force a retry (disintegrate's
    /// *validation query*).
    ///
    /// Events selected here but not by `query` are never folded; their
    /// arrival forces a re-read and a re-decide against unchanged
    /// state, and the condition then moves past them.
    fn validation(&self) -> Query {
        self.query()
    }

    /// The state before any event.
    fn initial(&self) -> Self::State;

    /// Fold one selected event into `state`. Total, like
    /// [`Aggregate::apply`](crate::aggregate::Aggregate::apply).
    fn apply(&self, state: &mut Self::State, event: &Self::Event);

    /// Decide against the folded state: events routed to streams
    /// ([`BoundaryDecision::of`], [`BoundaryDecision::to`]), a
    /// rejection, or no change.
    fn decide(&self, state: &Self::State) -> BoundaryDecision<Self::Event, Self::Error>;
}

/// The result of [`Decision::decide`]: events routed to streams,
/// a rejection, or no change.
///
/// Events still land in streams — the store is stream-shaped and every
/// committed event has a stream and a version — but the streams carry
/// no guard: consistency comes from the [`AppendCondition`], so each
/// stream is appended with [`ExpectedVersion::Any`].
pub struct BoundaryDecision<E, Err> {
    outcome: Result<Vec<(NewEvent<E>, StreamId)>, Err>,
}

impl<E, Err> BoundaryDecision<E, Err> {
    /// Accept: append `events`, each routed to the stream at the same
    /// index of `targets` (the two vecs are the same length).
    pub fn of(events: Vec<E>, targets: Vec<StreamId>) -> Self {
        assert_eq!(
            events.len(),
            targets.len(),
            "every event needs a target stream",
        );
        Self {
            outcome: Ok(events.into_iter().map(NewEvent::new).zip(targets).collect()),
        }
    }

    /// Accept: append every event to one stream.
    pub fn to(stream: StreamId, events: Vec<E>) -> Self {
        Self {
            outcome: Ok(events
                .into_iter()
                .map(|event| (NewEvent::new(event), stream.clone()))
                .collect()),
        }
    }

    /// Reject the decision: a domain outcome, not a failure.
    pub fn reject(error: Err) -> Self {
        Self {
            outcome: Err(error),
        }
    }

    /// No change: nothing is appended.
    pub fn noop() -> Self {
        Self {
            outcome: Ok(Vec::new()),
        }
    }
}

/// What the machine wants the driver to do.
#[derive(Clone, Debug)]
pub enum BoundaryAction<E, Err> {
    /// Read every event matching `query` committed after `after`
    /// (exclusive), in sequence order, and answer with one
    /// [`Read`](BoundaryInput::Read).
    Read {
        /// The events to read.
        query: Query,
        /// The exclusive lower bound.
        after: Sequence,
    },
    /// Append the per-stream events atomically, guarded by
    /// `condition`. Every [`StreamAppend`] carries
    /// [`ExpectedVersion::Any`]; the condition is the guard.
    Append {
        /// The routed events, grouped per stream (sorted by stream).
        appends: Vec<StreamAppend<E>>,
        /// The guard: nothing matching its query after its position.
        condition: AppendCondition,
    },
    /// Terminal: the interaction's outcome.
    Done(BoundaryOutcome<E, Err>),
}

/// What the driver reports back to the machine.
#[derive(Clone, Debug)]
pub enum BoundaryInput<E> {
    /// The read completed: every matching event after the requested
    /// bound, in strictly increasing sequence order.
    Read {
        /// The events read.
        events: Vec<EventEnvelope<E>>,
    },
    /// The guarded append committed.
    Appended {
        /// One entry per requested append, as the store recorded it.
        committed: Vec<CommittedStream<E>>,
    },
    /// The condition failed: an event matching its query was committed
    /// at `sequence`, after the condition's position.
    Conflict {
        /// The highest matching position the store saw.
        sequence: Sequence,
    },
    /// A store operation failed — the read or the append.
    Failed(StoreError),
}

impl<E> From<StoreError> for BoundaryInput<E> {
    /// A [`QueryConflict`](StoreError::QueryConflict) is the boundary
    /// protocol's retry path; every other store failure — including a
    /// per-stream version [`Conflict`](StoreError::Conflict), which a
    /// boundary append (all [`Any`](ExpectedVersion::Any)) cannot
    /// legitimately produce — is [`Failed`](BoundaryInput::Failed).
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::QueryConflict { sequence } => BoundaryInput::Conflict { sequence },
            other => BoundaryInput::Failed(other),
        }
    }
}

/// The terminal outcome of a driven boundary machine.
#[derive(Clone, Debug)]
pub enum BoundaryOutcome<E, Err> {
    /// The events committed; one entry per requested append.
    Committed {
        /// One entry per requested append, as the store recorded it.
        committed: Vec<CommittedStream<E>>,
    },
    /// The decision carried an idempotency key (0.7.5) and the events
    /// its query reads include an earlier commit with that key: nothing
    /// was decided or appended. `committed` is the earlier commit's
    /// events the query selected — only those: a boundary decision sees
    /// nothing outside its query, so the key is honoured only when the
    /// decision's events fall inside it (decisions that read what they
    /// write, the usual DCB shape, always do).
    AlreadyCommitted {
        /// The earlier commit's events the query selected.
        committed: Vec<EventEnvelope<E>>,
    },
    /// The decision produced no events; nothing was appended.
    Noop,
    /// The domain rejected the decision.
    Rejected(Err),
    /// The store failed — a conflict that exhausted the retry budget, a
    /// transient error, a fatal one, or a driver protocol violation
    /// (see [`ProtocolError`]).
    Failed(StoreError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Reading,
    Appending,
    Done,
}

/// The sans-IO machine behind a [`Decision`]: read the query → fold →
/// decide → append under a query condition, with conflict retry.
///
/// Like every machine here it never does I/O and never panics on bad
/// input: a driver that answers out of phase, delivers events out of
/// order or outside the query, or drives a finished machine gets
/// [`Done`](BoundaryAction::Done) with
/// [`Failed(StoreError::Other(ProtocolError))`](BoundaryOutcome::Failed).
pub struct BoundaryMachine<D: Decision> {
    decision: D,
    query: Query,
    retry_policy: RetryPolicy,
    retries_used: u32,
    phase: Phase,
    state: D::State,
    /// The highest sequence the fold has consumed: the next read's
    /// exclusive bound.
    read_position: Sequence,
    /// The highest conflicting sequence a store reported: events up to
    /// it that the fold query does not select are accounted for by the
    /// re-decide that follows.
    acknowledged: Sequence,
    metadata: Metadata,
    /// Read events carrying the interaction's idempotency key (0.7.5).
    earlier: Vec<EventEnvelope<D::Event>>,
}

impl<D: Decision> BoundaryMachine<D> {
    /// Begin an interaction for `decision`.
    pub fn new(decision: D, retry_policy: RetryPolicy) -> Self {
        let query = decision.query();
        let state = decision.initial();
        Self {
            decision,
            query,
            retry_policy,
            retries_used: 0,
            phase: Phase::Reading,
            state,
            read_position: Sequence::START,
            acknowledged: Sequence::START,
            metadata: Metadata::default(),
            earlier: Vec::new(),
        }
    }

    /// Builder-style: the metadata stamped on every event this
    /// interaction appends (0.5.2). Set before [`start`](Self::start).
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// The first action: read the decision's query from the beginning.
    /// Idempotent until the first [`handle`](Self::handle).
    pub fn start(&mut self) -> BoundaryAction<D::Event, D::Error> {
        if self.phase == Phase::Reading && self.read_position == Sequence::START {
            self.read_action()
        } else {
            self.violation("start() on a machine that already progressed")
        }
    }

    /// Consume a driver result, transition, and emit the next action.
    pub fn handle(&mut self, input: BoundaryInput<D::Event>) -> BoundaryAction<D::Event, D::Error> {
        match input {
            BoundaryInput::Read { events } => self.on_read(events),
            BoundaryInput::Appended { committed } => self.on_appended(committed),
            BoundaryInput::Conflict { sequence } => self.on_conflict(sequence),
            BoundaryInput::Failed(error) => self.on_failed(error),
        }
    }

    /// The decision this machine runs.
    pub fn decision(&self) -> &D {
        &self.decision
    }

    /// Whether the machine reached its terminal outcome.
    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    fn read_action(&self) -> BoundaryAction<D::Event, D::Error> {
        BoundaryAction::Read {
            query: self.query.clone(),
            after: self.read_position,
        }
    }

    fn on_read(
        &mut self,
        events: Vec<EventEnvelope<D::Event>>,
    ) -> BoundaryAction<D::Event, D::Error> {
        if self.phase != Phase::Reading {
            return self.violation("`Read` outside the reading phase");
        }
        // Validate before folding anything: a bad answer must not leave
        // a half-folded state behind (the machine terminates either
        // way, but the rule keeps the fold a pure function of a valid
        // read).
        let mut position = self.read_position;
        for envelope in &events {
            if envelope.sequence <= position {
                return self.violation("`Read` delivered events out of sequence order");
            }
            if !self.query.selects(&envelope.event) {
                return self.violation("`Read` delivered an event the query does not select");
            }
            position = envelope.sequence;
        }
        for envelope in &events {
            self.decision.apply(&mut self.state, &envelope.event);
        }
        self.read_position = position;
        if self.metadata.idempotency_key.is_some() {
            let key = &self.metadata.idempotency_key;
            self.earlier.extend(
                events
                    .into_iter()
                    .filter(|envelope| &envelope.metadata.idempotency_key == key),
            );
            if !self.earlier.is_empty() {
                self.phase = Phase::Done;
                return BoundaryAction::Done(BoundaryOutcome::AlreadyCommitted {
                    committed: core::mem::take(&mut self.earlier),
                });
            }
        }
        self.decide_and_emit()
    }

    fn on_appended(
        &mut self,
        committed: Vec<CommittedStream<D::Event>>,
    ) -> BoundaryAction<D::Event, D::Error> {
        if self.phase != Phase::Appending {
            return self.violation("`Appended` outside the appending phase");
        }
        self.phase = Phase::Done;
        BoundaryAction::Done(BoundaryOutcome::Committed { committed })
    }

    fn on_conflict(&mut self, sequence: Sequence) -> BoundaryAction<D::Event, D::Error> {
        if self.phase != Phase::Appending {
            return self.violation("`Conflict` outside the appending phase");
        }
        if sequence <= self.condition_position() {
            return self.violation("conflict reported a position at or before the condition's");
        }
        if self.retries_used < self.retry_policy.max_retries {
            self.retries_used += 1;
            self.phase = Phase::Reading;
            // Re-read the fold query's delta from where the fold
            // stopped — never from `sequence`: selected events between
            // the two must be folded. The conflicting position only
            // moves the next condition past what the re-decide has now
            // accounted for.
            self.acknowledged = self.acknowledged.max(sequence);
            self.read_action()
        } else {
            self.phase = Phase::Done;
            BoundaryAction::Done(BoundaryOutcome::Failed(StoreError::QueryConflict {
                sequence,
            }))
        }
    }

    fn on_failed(&mut self, error: StoreError) -> BoundaryAction<D::Event, D::Error> {
        if self.phase == Phase::Done {
            return self.violation("`Failed` on a finished machine");
        }
        self.phase = Phase::Done;
        BoundaryAction::Done(BoundaryOutcome::Failed(error))
    }

    /// The condition's exclusive bound: everything the decision has
    /// accounted for — folded, or acknowledged by a re-decide.
    fn condition_position(&self) -> Sequence {
        self.read_position.max(self.acknowledged)
    }

    fn decide_and_emit(&mut self) -> BoundaryAction<D::Event, D::Error> {
        match self.decision.decide(&self.state).outcome {
            Ok(routed) if routed.is_empty() => {
                self.phase = Phase::Done;
                BoundaryAction::Done(BoundaryOutcome::Noop)
            }
            Ok(routed) => {
                // Group per stream, preserving the decision's order
                // within each stream.
                let mut by_stream: alloc::collections::BTreeMap<StreamId, Vec<NewEvent<D::Event>>> =
                    alloc::collections::BTreeMap::new();
                for (event, stream) in routed {
                    by_stream.entry(stream).or_default().push(NewEvent {
                        event: event.event,
                        metadata: self.metadata.clone(),
                    });
                }
                self.phase = Phase::Appending;
                BoundaryAction::Append {
                    appends: by_stream
                        .into_iter()
                        .map(|(stream_id, events)| StreamAppend {
                            stream_id,
                            expected: ExpectedVersion::Any,
                            events,
                        })
                        .collect(),
                    condition: AppendCondition {
                        query: self.decision.validation(),
                        after: self.condition_position(),
                    },
                }
            }
            Err(error) => {
                self.phase = Phase::Done;
                BoundaryAction::Done(BoundaryOutcome::Rejected(error))
            }
        }
    }

    fn violation(&mut self, message: &'static str) -> BoundaryAction<D::Event, D::Error> {
        self.phase = Phase::Done;
        BoundaryAction::Done(BoundaryOutcome::Failed(StoreError::Other(Arc::new(
            ProtocolError::new(message),
        ))))
    }
}

/// Reusable fixture: course enrollment — the canonical DCB example. A
/// student may enroll in a course while the course has seats and the
/// student holds fewer than [`MAX_COURSES`](enrollment::MAX_COURSES)
/// courses: one invariant per tag, two tags per decision, and no
/// aggregate owning both.
pub mod enrollment {
    use alloc::string::String;
    use alloc::vec;
    use alloc::vec::Vec;
    use core::fmt;

    use super::{BoundaryDecision, Decision, Query, QueryItem, Tag, Tagged};
    use crate::event_name::EventName;
    use crate::vocabulary::StreamId;

    /// How many courses one student may hold.
    pub const MAX_COURSES: usize = 2;

    /// The enrollment events.
    #[derive(Clone, PartialEq, Eq, Debug)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    pub enum EnrollmentEvent {
        /// A course opened with `seats` places.
        CourseDefined {
            /// The course.
            course: String,
            /// Its capacity.
            seats: u32,
        },
        /// A student enrolled in a course.
        Enrolled {
            /// The course.
            course: String,
            /// The student.
            student: String,
        },
        /// A student's address changed — selected by nothing an
        /// enrollment decision reads.
        AddressChanged {
            /// The student.
            student: String,
        },
    }

    impl EventName for EnrollmentEvent {
        fn event_name(&self) -> &'static str {
            match self {
                Self::CourseDefined { .. } => "CourseDefined",
                Self::Enrolled { .. } => "Enrolled",
                Self::AddressChanged { .. } => "AddressChanged",
            }
        }
    }

    impl Tagged for EnrollmentEvent {
        fn tags(&self) -> Vec<Tag> {
            match self {
                Self::CourseDefined { course, .. } => vec![Tag::of("course", course)],
                Self::Enrolled { course, student } => {
                    vec![Tag::of("course", course), Tag::of("student", student)]
                }
                Self::AddressChanged { student } => vec![Tag::of("student", student)],
            }
        }
    }

    /// Why an enrollment was refused.
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum EnrollmentError {
        /// The course was never defined.
        NoSuchCourse,
        /// The course has no seats left.
        CourseFull,
        /// The student already holds the maximum number of courses.
        StudentAtLimit,
        /// The student is already enrolled in this course.
        AlreadyEnrolled,
    }

    impl fmt::Display for EnrollmentError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                Self::NoSuchCourse => "no such course",
                Self::CourseFull => "course is full",
                Self::StudentAtLimit => "student is at the course limit",
                Self::AlreadyEnrolled => "already enrolled",
            })
        }
    }

    /// What the decision folds: the course's capacity and enrolment
    /// count, and the student's course count.
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct EnrollmentState {
        /// The course's capacity, once defined.
        pub seats: Option<u32>,
        /// Students enrolled in the course.
        pub enrolled: u32,
        /// Courses the student holds.
        pub student_courses: usize,
        /// Whether this student already holds this course.
        pub already: bool,
    }

    /// The command and its decision: enroll `student` in `course`.
    #[derive(Clone, Debug)]
    pub struct Enroll {
        /// The course.
        pub course: String,
        /// The student.
        pub student: String,
    }

    impl Enroll {
        /// The decision for `student` in `course`.
        pub fn new(course: &str, student: &str) -> Self {
            Self {
                course: String::from(course),
                student: String::from(student),
            }
        }

        /// The stream enrollment events land in: the course's.
        pub fn stream(&self) -> StreamId {
            StreamId::from(alloc::format!("course-{}", self.course))
        }
    }

    impl Decision for Enroll {
        type Event = EnrollmentEvent;
        type State = EnrollmentState;
        type Error = EnrollmentError;

        fn query(&self) -> Query {
            Query::of(
                QueryItem::any()
                    .types(["CourseDefined", "Enrolled"])
                    .tags([Tag::of("course", &self.course)]),
            )
            .or(QueryItem::any()
                .types(["Enrolled"])
                .tags([Tag::of("student", &self.student)]))
        }

        fn initial(&self) -> EnrollmentState {
            EnrollmentState::default()
        }

        fn apply(&self, state: &mut EnrollmentState, event: &EnrollmentEvent) {
            match event {
                EnrollmentEvent::CourseDefined { seats, .. } => state.seats = Some(*seats),
                EnrollmentEvent::Enrolled { course, student } => {
                    let this_course = *course == self.course;
                    let this_student = *student == self.student;
                    if this_course {
                        state.enrolled += 1;
                    }
                    if this_student {
                        state.student_courses += 1;
                    }
                    if this_course && this_student {
                        state.already = true;
                    }
                }
                EnrollmentEvent::AddressChanged { .. } => {}
            }
        }

        fn decide(
            &self,
            state: &EnrollmentState,
        ) -> BoundaryDecision<EnrollmentEvent, EnrollmentError> {
            let Some(seats) = state.seats else {
                return BoundaryDecision::reject(EnrollmentError::NoSuchCourse);
            };
            if state.already {
                return BoundaryDecision::reject(EnrollmentError::AlreadyEnrolled);
            }
            if state.enrolled >= seats {
                return BoundaryDecision::reject(EnrollmentError::CourseFull);
            }
            if state.student_courses >= MAX_COURSES {
                return BoundaryDecision::reject(EnrollmentError::StudentAtLimit);
            }
            BoundaryDecision::to(
                self.stream(),
                vec![EnrollmentEvent::Enrolled {
                    course: self.course.clone(),
                    student: self.student.clone(),
                }],
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::enrollment::*;
    use super::*;
    use alloc::vec;
    use proptest::prelude::*;

    fn envelope(sequence: u64, event: EnrollmentEvent) -> EventEnvelope<EnrollmentEvent> {
        EventEnvelope {
            sequence: Sequence::new(sequence),
            stream_id: StreamId::from("course-c1"),
            version: crate::vocabulary::Version::new(sequence),
            event,
            metadata: Metadata::default(),
        }
    }

    fn defined(seats: u32) -> EnrollmentEvent {
        EnrollmentEvent::CourseDefined {
            course: "c1".into(),
            seats,
        }
    }

    fn enrolled(course: &str, student: &str) -> EnrollmentEvent {
        EnrollmentEvent::Enrolled {
            course: course.into(),
            student: student.into(),
        }
    }

    fn machine() -> BoundaryMachine<Enroll> {
        BoundaryMachine::new(Enroll::new("c1", "s1"), RetryPolicy::new(1))
    }

    fn is_violation<E, Err>(action: &BoundaryAction<E, Err>) -> bool {
        matches!(
            action,
            BoundaryAction::Done(BoundaryOutcome::Failed(StoreError::Other(_)))
        )
    }

    #[test]
    fn queries_select_by_type_and_all_tags() {
        let query = Enroll::new("c1", "s1").query();
        assert!(query.selects(&defined(3)));
        assert!(query.selects(&enrolled("c1", "s9")));
        assert!(query.selects(&enrolled("c7", "s1")));
        assert!(!query.selects(&enrolled("c7", "s9")));
        assert!(!query.selects(&EnrollmentEvent::AddressChanged {
            student: "s1".into()
        }));
        assert!(Query::all().selects(&enrolled("c7", "s9")));
        assert!(!Query::none().selects(&defined(1)));
    }

    #[test]
    fn start_reads_the_decision_query_from_the_beginning() {
        let mut m = machine();
        let BoundaryAction::Read { query, after } = m.start() else {
            panic!("expected a read");
        };
        assert_eq!(query, Enroll::new("c1", "s1").query());
        assert_eq!(after, Sequence::START);
        // Idempotent until handled.
        assert!(matches!(m.start(), BoundaryAction::Read { .. }));
    }

    #[test]
    fn an_accepted_decision_appends_under_the_read_position() {
        let mut m = machine();
        m.start();
        let action = m.handle(BoundaryInput::Read {
            events: vec![envelope(1, defined(2)), envelope(4, enrolled("c1", "s2"))],
        });
        let BoundaryAction::Append { appends, condition } = action else {
            panic!("expected an append, got {action:?}");
        };
        assert_eq!(appends.len(), 1);
        assert_eq!(appends[0].stream_id.as_str(), "course-c1");
        assert_eq!(appends[0].expected, ExpectedVersion::Any);
        assert_eq!(appends[0].events[0].event, enrolled("c1", "s1"));
        assert_eq!(condition.query, Enroll::new("c1", "s1").validation());
        assert_eq!(condition.after, Sequence::new(4));

        let done = m.handle(BoundaryInput::Appended { committed: vec![] });
        assert!(matches!(
            done,
            BoundaryAction::Done(BoundaryOutcome::Committed { .. })
        ));
        assert!(m.is_done());
    }

    #[test]
    fn the_domain_rejects_against_the_folded_state() {
        let mut m = machine();
        m.start();
        let action = m.handle(BoundaryInput::Read {
            events: vec![envelope(1, defined(1)), envelope(2, enrolled("c1", "s2"))],
        });
        assert!(matches!(
            action,
            BoundaryAction::Done(BoundaryOutcome::Rejected(EnrollmentError::CourseFull))
        ));

        let mut m = machine();
        m.start();
        let action = m.handle(BoundaryInput::Read { events: vec![] });
        assert!(matches!(
            action,
            BoundaryAction::Done(BoundaryOutcome::Rejected(EnrollmentError::NoSuchCourse))
        ));
    }

    #[test]
    fn a_noop_decision_appends_nothing() {
        struct Nothing;
        impl Decision for Nothing {
            type Event = EnrollmentEvent;
            type State = ();
            type Error = ();
            fn query(&self) -> Query {
                Query::all()
            }
            fn initial(&self) {}
            fn apply(&self, _: &mut (), _: &EnrollmentEvent) {}
            fn decide(&self, _: &()) -> BoundaryDecision<EnrollmentEvent, ()> {
                BoundaryDecision::noop()
            }
        }
        let mut m = BoundaryMachine::new(Nothing, RetryPolicy::default());
        m.start();
        let action = m.handle(BoundaryInput::Read { events: vec![] });
        assert!(matches!(
            action,
            BoundaryAction::Done(BoundaryOutcome::Noop)
        ));
    }

    #[test]
    fn a_conflict_rereads_the_delta_and_redecides() {
        let mut m = machine();
        m.start();
        m.handle(BoundaryInput::Read {
            events: vec![envelope(1, defined(2))],
        });
        // Someone else took a seat at 5.
        let action = m.handle(BoundaryInput::Conflict {
            sequence: Sequence::new(5),
        });
        let BoundaryAction::Read { after, .. } = action else {
            panic!("expected a delta read, got {action:?}");
        };
        // From where the fold stopped, not from the conflict.
        assert_eq!(after, Sequence::new(1));
        // The delta fills the course: the re-decide rejects.
        let action = m.handle(BoundaryInput::Read {
            events: vec![
                envelope(3, enrolled("c1", "s3")),
                envelope(5, enrolled("c1", "s4")),
            ],
        });
        assert!(matches!(
            action,
            BoundaryAction::Done(BoundaryOutcome::Rejected(EnrollmentError::CourseFull))
        ));
    }

    #[test]
    fn a_conflict_outside_the_fold_moves_the_condition_past_it() {
        // A validation query wider than the fold query: the conflicting
        // event is never folded, so the re-read finds nothing new — the
        // condition must still move past the conflict or the retry
        // could never succeed.
        struct Wide;
        impl Decision for Wide {
            type Event = EnrollmentEvent;
            type State = ();
            type Error = ();
            fn query(&self) -> Query {
                Query::none()
            }
            fn validation(&self) -> Query {
                Query::all()
            }
            fn initial(&self) {}
            fn apply(&self, _: &mut (), _: &EnrollmentEvent) {}
            fn decide(&self, _: &()) -> BoundaryDecision<EnrollmentEvent, ()> {
                BoundaryDecision::to(StreamId::from("s"), vec![defined(1)])
            }
        }
        let mut m = BoundaryMachine::new(Wide, RetryPolicy::new(1));
        m.start();
        m.handle(BoundaryInput::Read { events: vec![] });
        m.handle(BoundaryInput::Conflict {
            sequence: Sequence::new(9),
        });
        let action = m.handle(BoundaryInput::Read { events: vec![] });
        let BoundaryAction::Append { condition, .. } = action else {
            panic!("expected a re-append, got {action:?}");
        };
        assert_eq!(condition.after, Sequence::new(9));
    }

    #[test]
    fn conflicts_beyond_the_budget_fail() {
        let mut m = BoundaryMachine::new(Enroll::new("c1", "s1"), RetryPolicy::NEVER);
        m.start();
        m.handle(BoundaryInput::Read {
            events: vec![envelope(1, defined(2))],
        });
        let action = m.handle(BoundaryInput::Conflict {
            sequence: Sequence::new(2),
        });
        assert!(matches!(
            action,
            BoundaryAction::Done(BoundaryOutcome::Failed(StoreError::QueryConflict { sequence }))
                if sequence == Sequence::new(2)
        ));
    }

    #[test]
    fn metadata_is_stamped_on_every_appended_event() {
        let metadata = Metadata::of_ids(Some("cause".into()), Some("corr".into()));
        let mut m = machine().with_metadata(metadata.clone());
        m.start();
        let BoundaryAction::Append { appends, .. } = m.handle(BoundaryInput::Read {
            events: vec![envelope(1, defined(2))],
        }) else {
            panic!("expected an append");
        };
        assert_eq!(appends[0].events[0].metadata, metadata);
    }

    #[test]
    fn a_replayed_key_inside_the_query_returns_the_earlier_commit() {
        let mut m = machine().with_metadata(Metadata::default().with_idempotency_key("enroll-1"));
        m.start();
        let mut earlier = envelope(3, enrolled("c1", "s1"));
        earlier.metadata = Metadata::default().with_idempotency_key("enroll-1");
        let action = m.handle(BoundaryInput::Read {
            events: vec![envelope(1, defined(2)), earlier],
        });
        let BoundaryAction::Done(BoundaryOutcome::AlreadyCommitted { committed }) = action else {
            panic!("expected AlreadyCommitted, got {action:?}");
        };
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].sequence, Sequence::new(3));
    }

    #[test]
    fn a_keyed_decision_with_no_earlier_commit_decides() {
        let mut m = machine().with_metadata(Metadata::default().with_idempotency_key("enroll-1"));
        m.start();
        let BoundaryAction::Append { appends, .. } = m.handle(BoundaryInput::Read {
            events: vec![envelope(1, defined(2))],
        }) else {
            panic!("append");
        };
        assert_eq!(
            appends[0].events[0].metadata.idempotency_key.as_deref(),
            Some("enroll-1")
        );
    }

    #[test]
    fn protocol_violations_terminate_the_machine() {
        // Out of order.
        let mut m = machine();
        m.start();
        let action = m.handle(BoundaryInput::Read {
            events: vec![envelope(2, defined(2)), envelope(2, enrolled("c1", "s2"))],
        });
        assert!(is_violation(&action));

        // An event the query does not select.
        let mut m = machine();
        m.start();
        let action = m.handle(BoundaryInput::Read {
            events: vec![envelope(1, enrolled("c9", "s9"))],
        });
        assert!(is_violation(&action));

        // A conflict at or before the condition's position.
        let mut m = machine();
        m.start();
        m.handle(BoundaryInput::Read {
            events: vec![envelope(3, defined(2))],
        });
        let action = m.handle(BoundaryInput::Conflict {
            sequence: Sequence::new(3),
        });
        assert!(is_violation(&action));

        // Appended while reading.
        let mut m = machine();
        m.start();
        assert!(is_violation(
            &m.handle(BoundaryInput::Appended { committed: vec![] })
        ));

        // Driving a finished machine.
        assert!(is_violation(
            &m.handle(BoundaryInput::Failed(StoreError::Unavailable))
        ));
        assert!(is_violation(&m.start()));
    }

    #[test]
    fn store_errors_map_to_inputs() {
        assert!(matches!(
            BoundaryInput::<EnrollmentEvent>::from(StoreError::QueryConflict {
                sequence: Sequence::new(4)
            }),
            BoundaryInput::Conflict { sequence } if sequence == Sequence::new(4)
        ));
        assert!(matches!(
            BoundaryInput::<EnrollmentEvent>::from(StoreError::Unavailable),
            BoundaryInput::Failed(StoreError::Unavailable)
        ));
    }

    fn any_input() -> impl Strategy<Value = BoundaryInput<EnrollmentEvent>> {
        prop_oneof![
            (1u64..6).prop_map(|s| BoundaryInput::Read {
                events: vec![envelope(s, defined(2))],
            }),
            Just(BoundaryInput::Read { events: vec![] }),
            Just(BoundaryInput::Appended { committed: vec![] }),
            (0u64..8).prop_map(|s| BoundaryInput::Conflict {
                sequence: Sequence::new(s)
            }),
            Just(BoundaryInput::Failed(StoreError::Unavailable)),
        ]
    }

    proptest! {
        /// Any input sequence: the machine never panics, terminates at
        /// most once, and stays done.
        #[test]
        fn never_panics_and_terminates_once(inputs in proptest::collection::vec(any_input(), 0..12)) {
            let mut m = machine();
            let mut done = matches!(m.start(), BoundaryAction::Done(_));
            for input in inputs {
                let was_done = done;
                let action = m.handle(input);
                if was_done {
                    prop_assert!(is_violation(&action));
                }
                done |= matches!(action, BoundaryAction::Done(_));
                prop_assert_eq!(done, m.is_done());
            }
        }
    }
}
