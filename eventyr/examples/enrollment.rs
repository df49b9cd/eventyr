//! # Course enrollment — a dynamic consistency boundary, §14 / 0.7.1.
//!
//! Two invariants that no single aggregate owns: a course has a fixed
//! number of seats, and a student may hold at most two courses. Each
//! enrollment decision queries the events tagged with *its* course and
//! *its* student, folds them, decides, and appends under the condition
//! that nothing matching that query arrived since the read. Decisions
//! about other courses and other students never conflict with it.
//!
//! Dynamic consistency boundaries are not a special aggregate—they are
//! a `Decision` over a `Query`: the boundary is whatever the decision
//! reads. The store needs to answer two things: *read the events the
//! query selects since a point* and *append these events if nothing the
//! query selects arrived since that point*. That is `QueryAppend` (the
//! port behind `drive_boundary`), and it runs the same against the
//! in-memory store, SQLite, fjall, or Postgres.
//!
//! Run with:
//! ```sh
//! cargo run -p eventyr --example enrollment
//! ```

#![allow(dead_code)] // A demo: the point is the focused feature.

use eventyr::prelude::*;
use eventyr::store::prelude::*;

/// A course's identifier.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct CourseId(String);
impl std::fmt::Display for CourseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A student's identifier.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct StudentId(String);
impl std::fmt::Display for StudentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The events an enrollment decision reads.
// `crate = "eventyr"`: this example lives in the `eventyr` package
// itself, so the derive cannot resolve the target from the manifest —
// a user's crate would not need the attribute.
#[derive(Clone, Debug, PartialEq, EventName)]
#[eventyr(crate = "eventyr")]
enum EnrollmentEvent {
    CourseDefined {
        course: CourseId,
        seats: u32,
    },
    Enrolled {
        course: CourseId,
        student: StudentId,
    },
}

/// Tags are a pure function of the payload (0.7.1): a course tag for
/// the course's history, a student tag for the student's — so one
/// event can count for two invariants at once.
impl Tagged for EnrollmentEvent {
    fn tags(&self) -> Vec<Tag> {
        match self {
            Self::CourseDefined { course, .. } => {
                vec![Tag::of("course", course)]
            }
            Self::Enrolled { course, student } => {
                vec![Tag::of("course", course), Tag::of("student", student)]
            }
        }
    }
}

/// What the decision folds over: the course's occupancy and the
/// student's load.
#[derive(Default)]
struct EnrollmentState {
    capacity: Option<u32>,
    enrolled_in_course: u32,
    courses_taken: u32,
    already_enrolled: bool,
}

/// Why an enrollment is refused.
#[derive(Debug, PartialEq)]
enum EnrollmentError {
    NoSuchCourse,
    CourseFull,
    StudentAtLimit,
    AlreadyEnrolled,
}
impl std::fmt::Display for EnrollmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NoSuchCourse => "no such course",
            Self::CourseFull => "course is full",
            Self::StudentAtLimit => "student is at the course limit",
            Self::AlreadyEnrolled => "already enrolled",
        })
    }
}

/// The decision: enroll `student` in `course`.
struct Enroll {
    course: CourseId,
    student: StudentId,
}

impl Enroll {
    const MAX_COURSES: u32 = 2;
}

impl Decision for Enroll {
    type Event = EnrollmentEvent;
    type State = EnrollmentState;
    type Error = EnrollmentError;

    /// Everything the fold needs: the course's seats (to fill), the
    /// student's enrollments elsewhere (their load), and this course's
    /// prior enrollment of this student (idempotency).
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
            EnrollmentEvent::CourseDefined { seats, .. } => {
                state.capacity = Some(*seats);
            }
            EnrollmentEvent::Enrolled { course, student } => {
                if *course == self.course {
                    state.enrolled_in_course += 1;
                }
                if *student == self.student {
                    state.courses_taken += 1;
                }
                if *course == self.course && *student == self.student {
                    state.already_enrolled = true;
                }
            }
        }
    }

    fn decide(
        &self,
        state: &EnrollmentState,
    ) -> BoundaryDecision<EnrollmentEvent, EnrollmentError> {
        let Some(capacity) = state.capacity else {
            return BoundaryDecision::reject(EnrollmentError::NoSuchCourse);
        };
        if state.already_enrolled {
            return BoundaryDecision::reject(EnrollmentError::AlreadyEnrolled);
        }
        if state.enrolled_in_course >= capacity {
            return BoundaryDecision::reject(EnrollmentError::CourseFull);
        }
        if state.courses_taken >= Self::MAX_COURSES {
            return BoundaryDecision::reject(EnrollmentError::StudentAtLimit);
        }
        BoundaryDecision::to(
            StreamId::from(format!("course-{}", self.course)),
            [EnrollmentEvent::Enrolled {
                course: self.course.clone(),
                student: self.student.clone(),
            }],
        )
    }
}

async fn enroll(
    store: &InMemoryStore<EnrollmentEvent>,
    course: &str,
    student: &str,
) -> Result<(), EnrollmentError> {
    let mut machine = BoundaryMachine::new(
        Enroll {
            course: CourseId(course.to_owned()),
            student: StudentId(student.to_owned()),
        },
        RetryPolicy::default(),
    );
    // The async driver, as a server calls it; `loan_eligibility.rs`
    // shows its blocking twin.
    match drive_boundary(&mut machine, store).await {
        BoundaryOutcome::Committed { .. } | BoundaryOutcome::AlreadyCommitted { .. } => Ok(()),
        BoundaryOutcome::Rejected(error) => Err(error),
        BoundaryOutcome::Noop => unreachable!("an enrollment always decides an event"),
        BoundaryOutcome::Failed(error) => panic!("the in-memory store does not fail: {error}"),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let store = InMemoryStore::new();
    for (course, seats) in [("rust", 2), ("sql", 5), ("math", 5)] {
        store
            .append(
                &StreamId::from(format!("course-{course}")),
                ExpectedVersion::Empty,
                vec![NewEvent::new(EnrollmentEvent::CourseDefined {
                    course: CourseId(course.into()),
                    seats,
                })],
            )
            .await
            .expect("define the course");
    }

    let attempts = [
        ("rust", "ada"),
        ("rust", "bob"),
        ("rust", "cy"), // the course is full
        ("sql", "ada"),
        ("math", "ada"), // ada is at the limit
        ("rust", "ada"), // already enrolled
        ("math", "bob"),
    ];
    for (course, student) in attempts {
        match enroll(&store, course, student).await {
            Ok(()) => println!("{student:>4} enrolled in {course}"),
            Err(error) => println!("{student:>4} refused {course}: {error}"),
        }
    }

    assert_eq!(
        enroll(&store, "rust", "dee").await,
        Err(EnrollmentError::CourseFull)
    );
    assert_eq!(
        enroll(&store, "math", "ada").await,
        Err(EnrollmentError::StudentAtLimit)
    );
}
