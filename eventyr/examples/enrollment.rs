//! # Course enrollment — a dynamic consistency boundary, §14 / 0.7.1.
//!
//! Two invariants that no single aggregate owns: a course has a fixed
//! number of seats, and a student may hold at most two courses. Each
//! enrollment decision queries the events tagged with *its* course and
//! *its* student, folds them, decides, and appends under the condition
//! that nothing matching that query arrived since the read. Decisions
//! about other courses and other students never conflict with it.
//!
//! Run with:
//! ```sh
//! cargo run -p eventyr --example enrollment
//! ```

use eventyr::boundary::enrollment::{Enroll, EnrollmentError, EnrollmentEvent};
use eventyr::prelude::*;
use eventyr::store::prelude::*;

fn enroll(
    store: &InMemoryStore<EnrollmentEvent>,
    course: &str,
    student: &str,
) -> Result<(), EnrollmentError> {
    let mut machine = BoundaryMachine::new(Enroll::new(course, student), RetryPolicy::default());
    match drive_boundary_blocking(&mut machine, store) {
        BoundaryOutcome::Committed { .. } | BoundaryOutcome::AlreadyCommitted { .. } => Ok(()),
        BoundaryOutcome::Rejected(error) => Err(error),
        BoundaryOutcome::Noop => unreachable!("an enrollment always decides an event"),
        BoundaryOutcome::Failed(error) => panic!("the in-memory store does not fail: {error}"),
    }
}

fn main() {
    let store = InMemoryStore::new();
    for (course, seats) in [("rust", 2), ("sql", 5), ("math", 5)] {
        futures::executor::block_on(store.append(
            &StreamId::from(format!("course-{course}")),
            ExpectedVersion::Empty,
            vec![NewEvent::new(EnrollmentEvent::CourseDefined {
                course: course.into(),
                seats,
            })],
        ))
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
        match enroll(&store, course, student) {
            Ok(()) => println!("{student:>4} enrolled in {course}"),
            Err(error) => println!("{student:>4} refused {course}: {error}"),
        }
    }

    assert_eq!(
        enroll(&store, "rust", "dee"),
        Err(EnrollmentError::CourseFull)
    );
    assert_eq!(
        enroll(&store, "math", "ada"),
        Err(EnrollmentError::StudentAtLimit)
    );
}
