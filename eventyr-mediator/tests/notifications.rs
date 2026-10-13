//! Notifications: fan-out, both publisher strategies, handler errors,
//! and an event nobody handles.

mod common;

use std::convert::Infallible;

use eventyr_mediator::prelude::*;
use futures::executor::block_on;

use common::Trace;

/// A notification whose handlers can fail.
struct Shipped(u32);
impl Notification for Shipped {
    type Error = String;
}

/// A notification nobody handles yet.
struct Archived;
impl Notification for Archived {
    type Error = Infallible;
}

/// Records, then fails on odd orders.
struct Mailer {
    trace: Trace,
}

impl<M: Sync> NotificationHandler<Shipped, M> for Mailer {
    async fn handle(&self, _: &M, notification: &Shipped) -> Result<(), String> {
        self.trace.push(format!("mail {}", notification.0));
        if notification.0 % 2 == 1 {
            Err(format!("mail failed {}", notification.0))
        } else {
            Ok(())
        }
    }
}

/// Always records, never fails.
struct Ledger {
    trace: Trace,
}

impl<M: Sync> NotificationHandler<Shipped, M> for Ledger {
    async fn handle(&self, _: &M, notification: &Shipped) -> Result<(), String> {
        self.trace.push(format!("ledger {}", notification.0));
        Ok(())
    }
}

eventyr_mediator::mediator! {
    struct Sequential {
        mailer: Mailer,
        ledger: Ledger,
    }

    event Shipped => self.mailer, self.ledger;
    event Archived;
}

eventyr_mediator::mediator! {
    struct Concurrent {
        mailer: Mailer,
        ledger: Ledger,
    }

    event Shipped => self.mailer, self.ledger;

    publish concurrent;
}

#[test]
fn sequential_publish_stops_at_the_first_error() {
    let trace = Trace::default();
    let app = Sequential::new(
        Mailer {
            trace: trace.clone(),
        },
        Ledger {
            trace: trace.clone(),
        },
    );
    assert_eq!(block_on(app.publish(&Shipped(2))), Ok(()));
    assert_eq!(trace.take(), ["mail 2", "ledger 2"]);

    assert_eq!(
        block_on(app.publish(&Shipped(3))),
        Err("mail failed 3".to_string())
    );
    // The ledger never ran: ForeachAwait semantics.
    assert_eq!(trace.take(), ["mail 3"]);
}

#[test]
fn concurrent_publish_runs_every_handler() {
    let trace = Trace::default();
    let app = Concurrent::new(
        Mailer {
            trace: trace.clone(),
        },
        Ledger {
            trace: trace.clone(),
        },
    );
    assert_eq!(
        block_on(app.publish(&Shipped(3))),
        Err("mail failed 3".to_string())
    );
    // Every handler ran, and the first error in declared order wins:
    // TaskWhenAll semantics.
    let mut ran = trace.take();
    ran.sort();
    assert_eq!(ran, ["ledger 3", "mail 3"]);
}

#[test]
fn an_unhandled_notification_is_a_no_op() {
    let app = Sequential::new(
        Mailer {
            trace: Trace::default(),
        },
        Ledger {
            trace: Trace::default(),
        },
    );
    let Ok(()) = block_on(app.publish(&Archived));
}

/// A publisher of the application's own — MediatR's
/// `INotificationPublisher` — wrapping the built-in strategy: it records
/// every publish, then delivers to every handler even when one fails.
struct Recording {
    trace: Trace,
}

impl<N: Notification> NotificationPublisher<N> for Recording {
    async fn publish<D: Deliveries<N>>(&self, _: &N, handlers: D) -> Result<(), N::Error> {
        self.trace.push("publish");
        handlers.sequential_all().await
    }
}

eventyr_mediator::mediator! {
    struct Custom {
        mailer: Mailer,
        ledger: Ledger,
        publisher: Recording,
    }

    event Shipped => self.mailer, self.ledger;
    event Archived;

    publish with self.publisher;
}

#[test]
fn a_custom_publisher_decides_delivery() {
    let trace = Trace::default();
    let app = Custom::new(
        Mailer {
            trace: trace.clone(),
        },
        Ledger {
            trace: trace.clone(),
        },
        Recording {
            trace: trace.clone(),
        },
    );
    assert_eq!(
        block_on(app.publish(&Shipped(5))),
        Err("mail failed 5".to_string())
    );
    // The publisher ran first, and kept delivering past the failure.
    assert_eq!(trace.take(), ["publish", "mail 5", "ledger 5"]);
    // A notification nobody handles still goes through the publisher.
    let Ok(()) = block_on(app.publish(&Archived));
    assert_eq!(trace.take(), ["publish"]);
}
