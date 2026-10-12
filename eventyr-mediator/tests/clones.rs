//! Regression: error stages clone the request once, as a group.
//!
//! The review found one copy per (retaining) stage — one post-processor,
//! one error handler and one error action copied the request 4 times on
//! a successful send.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use eventyr_mediator::prelude::*;
use futures::executor::block_on;

/// The clone count, shared by the request and the test.
#[derive(Default)]
struct Clones(Arc<AtomicU32>);

impl Clone for Clones {
    fn clone(&self) -> Self {
        Clones(self.0.clone())
    }
}

impl Clones {
    fn bump(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// A request whose clones are counted.
struct Transfer {
    keeper: Clones,
}
impl Clone for Transfer {
    fn clone(&self) -> Self {
        self.keeper.bump();
        Transfer {
            keeper: self.keeper.clone(),
        }
    }
}
impl Request for Transfer {
    type Response = Result<u32, String>;
}

#[derive(Clone, Copy, Default)]
struct Handler;
impl<M: Sync> RequestHandler<Transfer, M> for Handler {
    async fn handle(&self, _: &M, _: Transfer) -> Result<u32, String> {
        Ok(1)
    }
}

#[derive(Clone, Default)]
struct Backup;
impl<M: Sync> RequestErrorHandler<Transfer, M> for Backup {
    async fn handle(&self, _: &M, _: &Transfer, _: String) -> Result<u32, String> {
        Ok(0)
    }
}
impl<M: Sync> RequestErrorHandler<Transfer, M> for Backup2 {
    async fn handle(&self, _: &M, _: &Transfer, _: String) -> Result<u32, String> {
        Ok(0)
    }
}
#[derive(Clone, Default)]
struct Backup2;
#[derive(Clone, Default)]
struct Seen;
impl<M: Sync> RequestErrorAction<Transfer, M> for Seen {
    async fn execute(&self, _: &M, _: &Transfer, _: &String) {}
}
#[derive(Clone, Default)]
struct Seen2;
impl<M: Sync> RequestErrorAction<Transfer, M> for Seen2 {
    async fn execute(&self, _: &M, _: &Transfer, _: &String) {}
}
#[derive(Clone, Default)]
struct Audit;
impl<M: Sync> RequestPostProcessor<Transfer, M> for Audit {
    async fn process(&self, _: &M, _: &Transfer, _: &Result<u32, String>) {}
}
#[derive(Clone, Default)]
struct Check;
impl<M: Sync> RequestPreProcessor<Transfer, M> for Check {
    async fn process(&self, _: &M, _: &Transfer) {}
}

eventyr_mediator::mediator! {
    struct App;
    command Transfer => Handler;
    pre_processors { Check }
    post_processors { Audit }
    error_handlers { Backup, Backup2 }
    error_actions { Seen, Seen2 }
}

#[test]
fn one_retaining_layer_clones_one_time() {
    let app = App::new();
    let clones = Clones::default();
    assert_eq!(
        block_on(app.send(Transfer {
            keeper: clones.clone(),
        })),
        Ok(1)
    );
    // See the doc comment: before, 4 clones (one per stage).
    assert_eq!(clones.0.load(Ordering::SeqCst), 1);
}
