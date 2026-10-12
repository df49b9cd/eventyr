//! The request pipeline: behaviors in declared order, selection by impl
//! bounds, short-circuits, pre- and post-processors, and the order of
//! every stage around the handler.

use std::sync::{Arc, Mutex};

use eventyr_mediator::prelude::*;
use futures::executor::block_on;

/// The order stages ran in, shared by every stage of one mediator.
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

/// Requests a guard checks.
trait Guarded {
    fn allowed(&self) -> bool;
}

/// A guarded command: the guard and both processors apply.
#[derive(Clone)]
struct Withdraw {
    amount: u32,
}
impl Request for Withdraw {
    type Response = Result<u32, String>;
}
impl Guarded for Withdraw {
    fn allowed(&self) -> bool {
        self.amount <= 100
    }
}

/// An unguarded query: not `Clone`, so post-processing it would not
/// compile — and nothing that needs a copy applies to it.
struct Balance;
impl Request for Balance {
    type Response = u32;
}

/// The handlers record themselves.
struct Teller {
    trace: Trace,
}

impl<M: Sync> RequestHandler<Withdraw, M> for Teller {
    async fn handle(&self, _: &M, request: Withdraw) -> Result<u32, String> {
        self.trace.push("handler");
        Ok(request.amount)
    }
}

impl<M: Sync> RequestHandler<Balance, M> for Teller {
    async fn handle(&self, _: &M, _: Balance) -> u32 {
        self.trace.push("handler");
        7
    }
}

/// Wraps every route.
struct Outer {
    trace: Trace,
}

impl<Req: Request, M: Sync> PipelineBehavior<Req, M> for Outer {
    async fn handle<N: Next<Req>>(&self, _: &M, request: Req, next: N) -> Req::Response {
        self.trace.push("outer in");
        let response = next.run(request).await;
        self.trace.push("outer out");
        response
    }
}

/// Wraps only `Guarded` routes, and short-circuits a refused request.
struct Guard {
    trace: Trace,
}

impl<Req, M> PipelineBehavior<Req, M> for Guard
where
    Req: Request<Response = Result<u32, String>> + Guarded,
    M: Sync,
{
    async fn handle<N: Next<Req>>(&self, _: &M, request: Req, next: N) -> Req::Response {
        self.trace.push("guard");
        if request.allowed() {
            next.run(request).await
        } else {
            Err("refused".into())
        }
    }
}

/// Applies to one concrete route.
struct OnlyBalance {
    trace: Trace,
}

impl<M: Sync> PipelineBehavior<Balance, M> for OnlyBalance {
    async fn handle<N: Next<Balance>>(&self, _: &M, request: Balance, next: N) -> u32 {
        self.trace.push("only balance");
        next.run(request).await + 1
    }
}

/// Pre-processes `Guarded` routes.
struct Validate {
    trace: Trace,
}

impl<Req: Request + Guarded + Sync, M: Sync> RequestPreProcessor<Req, M> for Validate {
    async fn process(&self, _: &M, _: &Req) {
        self.trace.push("pre");
    }
}

/// Post-processes `Guarded` routes; sees the response.
struct Audit {
    trace: Trace,
}

impl<M: Sync> RequestPostProcessor<Withdraw, M> for Audit {
    async fn process(&self, _: &M, request: &Withdraw, response: &Result<u32, String>) {
        self.trace
            .push(format!("post {} {}", request.amount, response.is_ok()));
    }
}

eventyr_mediator::mediator! {
    struct Bank {
        teller: Teller,
        outer: Outer,
        guard: Guard,
        only_balance: OnlyBalance,
        validate: Validate,
        audit: Audit,
    }

    command Withdraw => self.teller;
    query Balance => self.teller;

    behaviors { self.outer, self.guard, self.only_balance }
    pre_processors { self.validate }
    post_processors { self.audit }
}

fn bank() -> (Bank, Trace) {
    let trace = Trace::default();
    let bank = Bank::new(
        Teller {
            trace: trace.clone(),
        },
        Outer {
            trace: trace.clone(),
        },
        Guard {
            trace: trace.clone(),
        },
        OnlyBalance {
            trace: trace.clone(),
        },
        Validate {
            trace: trace.clone(),
        },
        Audit {
            trace: trace.clone(),
        },
    );
    (bank, trace)
}

#[test]
fn stages_run_in_mediatr_order() {
    let (bank, trace) = bank();
    assert_eq!(block_on(bank.send(Withdraw { amount: 40 })), Ok(40));
    // Pre-processors first, post-processors around the behaviors, the
    // behaviors in declared order around the handler.
    assert_eq!(
        trace.take(),
        [
            "pre",
            "outer in",
            "guard",
            "handler",
            "outer out",
            "post 40 true"
        ]
    );
}

#[test]
fn a_behavior_applies_where_its_bounds_hold() {
    let (bank, trace) = bank();
    // `Balance` is not `Guarded`: the guard, the pre-processor, and the
    // post-processor skip it; `OnlyBalance` applies to it alone.
    assert_eq!(block_on(bank.send(Balance)), 8);
    assert_eq!(
        trace.take(),
        ["outer in", "only balance", "handler", "outer out"]
    );
}

#[test]
fn a_behavior_can_short_circuit() {
    let (bank, trace) = bank();
    assert_eq!(
        block_on(bank.send(Withdraw { amount: 500 })),
        Err("refused".into())
    );
    // The handler never ran; the post-processor saw the refusal.
    assert_eq!(
        trace.take(),
        ["pre", "outer in", "guard", "outer out", "post 500 false"]
    );
}
