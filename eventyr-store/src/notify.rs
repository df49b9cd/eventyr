//! The commit signal (roadmap 0.7.2): a store's "something committed"
//! hint, so an idle projector can poll at once instead of waiting out
//! its idle sleep.
//!
//! A hint, never a delivery: nothing here carries events, and nothing
//! depends on a wake-up arriving. The subscription's checkpoint poll
//! stays authoritative — a lost or spurious wake-up costs latency or
//! one empty poll, never an event.
//!
//! [`CommitSignal`] is the store side: [`subscribe`](CommitSignal::subscribe)
//! arms a [`CommitListener`] *before* the subscriber's first poll, so a
//! commit that lands between a poll and the wait is never missed — the
//! listener remembers it and its next [`committed`](CommitListener::committed)
//! returns at once. [`LocalCommitSignal`] is the in-process
//! implementation the embedded stores share.
//!
//! This is not [`EventBus`](https://docs.rs/eventyr-subscription): that
//! trait pushes envelopes *out* to a transport the caller owns. A
//! commit signal is narrower and store-owned — Postgres raises it from
//! inside the commit (a `NOTIFY`), not through a call.

use core::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use event_listener::Event;

use eventyr_core::error::StoreError;

/// A store that can tell subscribers "a commit happened".
pub trait CommitSignal: Send + Sync {
    /// The armed listener [`subscribe`](Self::subscribe) returns.
    type Listener: CommitListener;

    /// Arm a listener. Every commit after this call resolves the
    /// listener's next [`committed`](CommitListener::committed).
    fn subscribe(&self) -> impl Future<Output = Result<Self::Listener, StoreError>> + Send;
}

/// An armed subscription to a store's commits.
pub trait CommitListener: Send {
    /// Resolve once a commit has happened since the listener was armed
    /// or since this method last returned — at once if one already has.
    /// Coalescing is allowed: many commits may resolve one call.
    ///
    /// Must be cancel-safe: a driver races it against a timer and drops
    /// it when the timer wins, and a commit observed by the dropped
    /// future must still resolve the next call (or have been consumed
    /// by nobody). An `Err` means the signal is broken for now; the
    /// driver falls back to its timer.
    fn committed(&mut self) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// The listener that never fires: a projector without a commit signal
/// waits out its idle sleep, as before 0.7.2.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoSignal;

impl CommitListener for NoSignal {
    fn committed(&mut self) -> impl Future<Output = Result<(), StoreError>> + Send {
        core::future::pending()
    }
}

/// The in-process commit signal: a generation counter and an
/// [`Event`]. A store bumps it after every commit; listeners compare
/// generations, so a commit is never lost between a check and a wait.
///
/// Cloning shares the signal. It covers commits made through the store
/// handles holding it — another process writing the same file is not
/// seen (its commits still arrive at the next timed poll).
#[derive(Clone, Default)]
pub struct LocalCommitSignal {
    inner: Arc<Shared>,
}

#[derive(Default)]
struct Shared {
    generation: AtomicU64,
    event: Event,
}

impl LocalCommitSignal {
    /// A fresh signal.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a commit and wake every listener. Call after the commit is
    /// durable and visible to reads.
    pub fn notify(&self) {
        self.inner.generation.fetch_add(1, Ordering::Release);
        self.inner.event.notify(usize::MAX);
    }

    /// A listener armed at the current generation.
    pub fn listener(&self) -> LocalCommitListener {
        LocalCommitListener {
            seen: self.inner.generation.load(Ordering::Acquire),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl CommitSignal for LocalCommitSignal {
    type Listener = LocalCommitListener;

    fn subscribe(&self) -> impl Future<Output = Result<Self::Listener, StoreError>> + Send {
        core::future::ready(Ok(self.listener()))
    }
}

/// A [`LocalCommitSignal`] listener.
pub struct LocalCommitListener {
    inner: Arc<Shared>,
    /// The generation this listener has already reported.
    seen: u64,
}

impl CommitListener for LocalCommitListener {
    /// Cancel-safe: `seen` moves only when the call returns, so a
    /// dropped call leaves the commit for the next one.
    async fn committed(&mut self) -> Result<(), StoreError> {
        loop {
            let current = self.inner.generation.load(Ordering::Acquire);
            if current != self.seen {
                self.seen = current;
                return Ok(());
            }
            // Register, then re-check: a commit between the check above
            // and `listen` is caught by the re-check, one after it by
            // the listener.
            let listener = self.inner.event.listen();
            if self.inner.generation.load(Ordering::Acquire) != self.seen {
                continue;
            }
            listener.await;
        }
    }
}

// Shared stores implement the port through the pointer the caller
// chose, as the other ports do.
impl<S: CommitSignal + ?Sized> CommitSignal for &S {
    type Listener = S::Listener;

    fn subscribe(&self) -> impl Future<Output = Result<Self::Listener, StoreError>> + Send {
        (**self).subscribe()
    }
}

impl<S: CommitSignal + ?Sized> CommitSignal for Arc<S> {
    type Listener = S::Listener;

    fn subscribe(&self) -> impl Future<Output = Result<Self::Listener, StoreError>> + Send {
        (**self).subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    #[test]
    fn a_commit_before_the_wait_is_not_lost() {
        let signal = LocalCommitSignal::new();
        let mut listener = signal.listener();
        signal.notify();
        assert!(matches!(listener.committed().now_or_never(), Some(Ok(()))));
        // Consumed: nothing pending now.
        assert!(listener.committed().now_or_never().is_none());
    }

    #[test]
    fn commits_coalesce_into_one_wake() {
        let signal = LocalCommitSignal::new();
        let mut listener = signal.listener();
        signal.notify();
        signal.notify();
        signal.notify();
        assert!(listener.committed().now_or_never().is_some());
        assert!(listener.committed().now_or_never().is_none());
    }

    #[test]
    fn a_dropped_wait_leaves_the_commit_for_the_next_call() {
        let signal = LocalCommitSignal::new();
        let mut listener = signal.listener();
        // Start a wait, poll it once (pending), drop it.
        assert!(listener.committed().now_or_never().is_none());
        signal.notify();
        assert!(listener.committed().now_or_never().is_some());
    }

    #[test]
    fn a_listener_armed_after_a_commit_does_not_see_it() {
        let signal = LocalCommitSignal::new();
        signal.notify();
        let mut listener = signal.listener();
        assert!(listener.committed().now_or_never().is_none());
    }

    #[tokio::test]
    async fn a_waiting_listener_wakes_on_commit() {
        let signal = LocalCommitSignal::new();
        let mut listener = signal.listener();
        let waiter = tokio::spawn(async move { listener.committed().await });
        tokio::task::yield_now().await;
        signal.notify();
        waiter.await.expect("task").expect("woken");
    }

    #[test]
    fn no_signal_never_fires() {
        assert!(NoSignal.committed().now_or_never().is_none());
    }
}
