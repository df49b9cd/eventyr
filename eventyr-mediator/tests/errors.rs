//! Error handlers and error actions: recovery, observation, selection by
//! the request and the error type, and both action strategies.

use std::sync::{Arc, Mutex};

use eventyr_mediator::prelude::*;
use futures::executor::block_on;

#[derive(Clone, Default)]
struct Trace(Arc<Mutex<Vec<String>>>);

impl Trace {
    fn push(&self, step: impl Into<String>) {
        self.0.lock().unwrap().push(step.into());
    }

    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

/// A fallible query.
#[derive(Clone)]
struct Divide(u32, u32);
impl Request for Divide {
    type Response = Result<u32, Fault>;
}

/// A fallible query with another error type.
#[derive(Clone)]
struct Parse(&'static str);
impl Request for Parse {
    type Response = Result<u32, String>;
}

/// An infallible query: no error stage applies, so it needs no `Clone`.
struct Constant;
impl Request for Constant {
    type Response = u32;
}

#[derive(Debug, PartialEq)]
enum Fault {
    ByZero,
    Overflow,
}

#[derive(Default)]
struct Math;

impl<M: Sync> RequestHandler<Divide, M> for Math {
    async fn handle(&self, _: &M, Divide(a, b): Divide) -> Result<u32, Fault> {
        match (a, b) {
            (_, 0) => Err(Fault::ByZero),
            (a, _) if a == u32::MAX => Err(Fault::Overflow),
            (a, b) => Ok(a / b),
        }
    }
}

impl<M: Sync> RequestHandler<Parse, M> for Math {
    async fn handle(&self, _: &M, Parse(text): Parse) -> Result<u32, String> {
        text.parse().map_err(|_| format!("not a number: {text}"))
    }
}

impl<M: Sync> RequestHandler<Constant, M> for Math {
    async fn handle(&self, _: &M, _: Constant) -> u32 {
        1
    }
}

/// Recovers division by zero only — a domain-specific handler, like
/// MediatR's handler for one exception type.
struct ZeroIsZero {
    trace: Trace,
}

impl<M: Sync> RequestErrorHandler<Divide, M> for ZeroIsZero {
    async fn handle(&self, _: &M, _: &Divide, error: Fault) -> Result<u32, Fault> {
        self.trace.push(format!("recover {error:?}"));
        match error {
            Fault::ByZero => Ok(0),
            other => Err(other),
        }
    }
}

/// Observes every failure of every fallible route whose error is
/// `Debug`.
struct Alert {
    trace: Trace,
}

impl<Req, M> RequestErrorAction<Req, M> for Alert
where
    // The `async fn` holds `&Req` across an await: a generic action
    // needs `Req: Sync` for its future to be `Send`.
    Req: Request + Sync,
    Req::Response: Fallible,
    <Req::Response as Fallible>::Error: std::fmt::Debug + Sync,
    M: Sync,
{
    async fn execute(&self, _: &M, _: &Req, error: &<Req::Response as Fallible>::Error) {
        self.trace.push(format!("alert {error:?}"));
    }
}

eventyr_mediator::mediator! {
    struct Unhandled {
        recover: ZeroIsZero,
        alert: Alert,
    }

    query Divide => Math;
    query Parse => Math;
    query Constant => Math;

    error_handlers { self.recover }
    error_actions { self.alert }
}

eventyr_mediator::mediator! {
    struct All {
        recover: ZeroIsZero,
        alert: Alert,
    }

    query Divide => Math;

    error_handlers { self.recover }
    error_actions { self.alert }
    error_action_strategy all;
}

#[test]
fn a_handler_recovers_and_the_action_sees_only_what_is_left() {
    let trace = Trace::default();
    let app = Unhandled::new(
        ZeroIsZero {
            trace: trace.clone(),
        },
        Alert {
            trace: trace.clone(),
        },
    );

    assert_eq!(block_on(app.send(Divide(9, 3))), Ok(3));
    assert!(trace.take().is_empty());

    // Recovered: the action never hears of it.
    assert_eq!(block_on(app.send(Divide(9, 0))), Ok(0));
    assert_eq!(trace.take(), ["recover ByZero"]);

    // Not recovered: the action observes it, and it propagates.
    assert_eq!(
        block_on(app.send(Divide(u32::MAX, 1))),
        Err(Fault::Overflow)
    );
    assert_eq!(trace.take(), ["recover Overflow", "alert Overflow"]);
}

#[test]
fn stages_apply_by_the_request_and_error_types() {
    let trace = Trace::default();
    let app = Unhandled::new(
        ZeroIsZero {
            trace: trace.clone(),
        },
        Alert {
            trace: trace.clone(),
        },
    );
    // `ZeroIsZero` handles `Divide` only; `Alert` any `Debug` error.
    assert_eq!(
        block_on(app.send(Parse("x"))),
        Err("not a number: x".to_string())
    );
    assert_eq!(trace.take(), [r#"alert "not a number: x""#]);
    // An infallible route has no error stages at all.
    assert_eq!(block_on(app.send(Constant)), 1);
    assert!(trace.take().is_empty());
}

#[test]
fn the_all_strategy_shows_actions_every_error() {
    let trace = Trace::default();
    let app = All::new(
        ZeroIsZero {
            trace: trace.clone(),
        },
        Alert {
            trace: trace.clone(),
        },
    );
    assert_eq!(block_on(app.send(Divide(9, 0))), Ok(0));
    assert_eq!(trace.take(), ["alert ByZero", "recover ByZero"]);
}
