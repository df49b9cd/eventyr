//! Regression: the retaining stages keep one copy of the request
//! between them.
//!
//! A post-processor and the error stages see the request after the
//! handler consumed it, so the outermost retaining layer keeps one copy
//! the inner retaining layers borrow (the review measured one copy per
//! retaining stage — a post-processor, an error handler, and an error
//! action copied the request three times on a successful send).

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
    fn count(&self) -> u32 {
        self.0.load(Ordering::SeqCst)
    }
}

/// A `Sync` request whose clones are counted.
struct Transfer {
    keeper: Clones,
}
impl Clone for Transfer {
    fn clone(&self) -> Self {
        self.keeper.0.fetch_add(1, Ordering::SeqCst);
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

/// A post-processor: sees the kept copy on the success path.
#[derive(Clone, Default)]
struct Audit;
impl<M: Sync> RequestPostProcessor<Transfer, M> for Audit {
    async fn process(&self, _: &M, _: &Transfer, _: &Result<u32, String>) {}
}

/// An error handler: sees the kept copy on a failure.
#[derive(Clone, Default)]
struct Backup;
impl<M: Sync> RequestErrorHandler<Transfer, M> for Backup {
    async fn handle(&self, _: &M, _: &Transfer, _: String) -> Result<u32, String> {
        Ok(0)
    }
}

/// An error action: observes the kept copy on a failure.
#[derive(Clone, Default)]
struct Seen;
impl<M: Sync> RequestErrorAction<Transfer, M> for Seen {
    async fn execute(&self, _: &M, _: &Transfer, _: &String) {}
}

eventyr_mediator::mediator! {
    struct App;
    command Transfer => Handler;
    post_processors { Audit }
    error_handlers { Backup }
    error_actions { Seen }
}

#[test]
fn the_retaining_stages_share_one_copy() {
    let app = App::new();
    let clones = Clones::default();
    assert_eq!(
        block_on(app.send(Transfer {
            keeper: clones.clone(),
        })),
        Ok(1)
    );
    // One copy for all three retaining stages, not one per stage.
    assert_eq!(clones.count(), 1);
}
