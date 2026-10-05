//! Parked events (0.7.7): where a subscription records an event its
//! projection kept rejecting, so it can carry on past it.
//!
//! A parked event is skipped, not lost: the record keeps the whole
//! envelope and the last rejection, so an operator can see what the
//! projection refused and, once the projection is fixed, replay it.

use std::sync::Mutex;

use core::future::Future;

use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::vocabulary::Sequence;

/// One parked event.
#[derive(Clone, Debug)]
pub struct ParkedEvent<E> {
    /// The subscription that parked it — the checkpoint name.
    pub subscription: String,
    /// The event, as delivered.
    pub envelope: EventEnvelope<E>,
    /// How many times the projection rejected it.
    pub attempts: u32,
    /// The last rejection, rendered.
    pub error: String,
}

/// Where parked events are recorded.
///
/// [`park`](Self::park) must be durable before it returns: the
/// subscription acks past the event right after, so a park that is lost
/// is an event that is silently skipped.
pub trait ParkedStore<E>: Send + Sync {
    /// Whether [`park`](Self::park) can ever succeed. Only [`NoParking`]
    /// says no; a [`Projector`](crate::runner::Projector) whose policy
    /// parks refuses to run over a store that says no, rather than
    /// retry the event forever.
    const RECORDS: bool = true;

    /// Record `event`. Parking the same `(subscription, sequence)` again
    /// — a redelivery after a crash between park and ack — replaces the
    /// record, never duplicates it.
    fn park(&self, event: ParkedEvent<E>) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// The events `subscription` has parked, oldest first.
    fn list(
        &self,
        subscription: &str,
    ) -> impl Future<Output = Result<Vec<ParkedEvent<E>>, StoreError>> + Send;

    /// Remove a parked event — after replaying it, or after deciding it
    /// is not needed. Removing one that is not there is not an error.
    fn remove(
        &self,
        subscription: &str,
        sequence: Sequence,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// Parked events in process memory: tests and examples.
pub struct InMemoryParkedStore<E> {
    parked: Mutex<Vec<ParkedEvent<E>>>,
}

impl<E> Default for InMemoryParkedStore<E> {
    fn default() -> Self {
        Self {
            parked: Mutex::new(Vec::new()),
        }
    }
}

impl<E> InMemoryParkedStore<E> {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<ParkedEvent<E>>> {
        self.parked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<E: Clone + Send> ParkedStore<E> for InMemoryParkedStore<E> {
    async fn park(&self, event: ParkedEvent<E>) -> Result<(), StoreError> {
        let mut parked = self.lock();
        parked.retain(|p| {
            p.subscription != event.subscription || p.envelope.sequence != event.envelope.sequence
        });
        parked.push(event);
        parked.sort_by_key(|p| p.envelope.sequence);
        Ok(())
    }

    async fn list(&self, subscription: &str) -> Result<Vec<ParkedEvent<E>>, StoreError> {
        Ok(self
            .lock()
            .iter()
            .filter(|p| p.subscription == subscription)
            .cloned()
            .collect())
    }

    async fn remove(&self, subscription: &str, sequence: Sequence) -> Result<(), StoreError> {
        self.lock()
            .retain(|p| p.subscription != subscription || p.envelope.sequence != sequence);
        Ok(())
    }
}

impl<E, P: ParkedStore<E> + ?Sized> ParkedStore<E> for std::sync::Arc<P> {
    const RECORDS: bool = P::RECORDS;

    fn park(&self, event: ParkedEvent<E>) -> impl Future<Output = Result<(), StoreError>> + Send {
        (**self).park(event)
    }

    fn list(
        &self,
        subscription: &str,
    ) -> impl Future<Output = Result<Vec<ParkedEvent<E>>, StoreError>> + Send {
        (**self).list(subscription)
    }

    fn remove(
        &self,
        subscription: &str,
        sequence: Sequence,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        (**self).remove(subscription, sequence)
    }
}

impl<E, P: ParkedStore<E> + ?Sized> ParkedStore<E> for &P {
    const RECORDS: bool = P::RECORDS;

    fn park(&self, event: ParkedEvent<E>) -> impl Future<Output = Result<(), StoreError>> + Send {
        (**self).park(event)
    }

    fn list(
        &self,
        subscription: &str,
    ) -> impl Future<Output = Result<Vec<ParkedEvent<E>>, StoreError>> + Send {
        (**self).list(subscription)
    }

    fn remove(
        &self,
        subscription: &str,
        sequence: Sequence,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        (**self).remove(subscription, sequence)
    }
}

/// A parked store for subscriptions that never park ([`FailurePolicy::Halt`](eventyr_core::subscription::FailurePolicy::Halt)):
/// a `Park` reaching it is refused, so the event is redelivered rather
/// than skipped.
///
/// A [`Projector`](crate::runner::Projector) with a
/// [`FailurePolicy::Park`](eventyr_core::subscription::FailurePolicy::Park)
/// policy over `NoParking` refuses to run. Driven directly, every
/// refusal counts on
/// [`PARK_FAILURES`](eventyr_store::metrics::names::PARK_FAILURES).
#[derive(Clone, Copy, Debug, Default)]
pub struct NoParking;

impl<E: Send> ParkedStore<E> for NoParking {
    const RECORDS: bool = false;

    async fn park(&self, _: ParkedEvent<E>) -> Result<(), StoreError> {
        Err(StoreError::other(
            "this subscription has no parked store; give the projector one to park events",
        ))
    }

    async fn list(&self, _: &str) -> Result<Vec<ParkedEvent<E>>, StoreError> {
        Ok(Vec::new())
    }

    async fn remove(&self, _: &str, _: Sequence) -> Result<(), StoreError> {
        Ok(())
    }
}

/// Run the [`ParkedStore`] contract against `make_store`'s fresh stores.
/// Every implementation runs it, behind this crate's `testing` feature.
#[cfg(any(test, feature = "testing"))]
pub fn parked_store_contract<P: ParkedStore<u64>>(make_store: impl Fn() -> P) {
    use eventyr_core::envelope::{EventEnvelope, Metadata};
    use eventyr_core::vocabulary::{StreamId, Version};
    use futures::executor::block_on;

    let event = |subscription: &str, sequence: u64, attempts: u32| ParkedEvent {
        subscription: subscription.to_owned(),
        envelope: EventEnvelope {
            sequence: Sequence::new(sequence),
            stream_id: StreamId::from("s-1"),
            version: Version::new(sequence),
            event: sequence * 10,
            metadata: Metadata::of_ids(Some("cause".into()), None),
        },
        attempts,
        error: format!("failed {attempts} times"),
    };

    let store = make_store();
    assert!(block_on(store.list("a")).expect("list").is_empty());
    block_on(store.park(event("a", 7, 3))).expect("park");
    block_on(store.park(event("a", 2, 1))).expect("park");
    block_on(store.park(event("b", 5, 1))).expect("park");

    let listed = block_on(store.list("a")).expect("list");
    assert_eq!(
        listed
            .iter()
            .map(|p| p.envelope.sequence.as_u64())
            .collect::<Vec<_>>(),
        vec![2, 7],
        "per subscription, oldest first"
    );
    assert_eq!(listed[1].envelope.event, 70, "the event round-trips");
    assert_eq!(listed[1].attempts, 3);
    assert_eq!(listed[1].error, "failed 3 times");
    assert_eq!(
        listed[1].envelope.metadata.causation_id.as_deref(),
        Some("cause")
    );

    // Parking again (a redelivery after a crash) replaces, never duplicates.
    block_on(store.park(event("a", 7, 4))).expect("park again");
    let listed = block_on(store.list("a")).expect("list");
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[1].attempts, 4);

    block_on(store.remove("a", Sequence::new(7))).expect("remove");
    block_on(store.remove("a", Sequence::new(99))).expect("removing a missing one is fine");
    assert_eq!(block_on(store.list("a")).expect("list").len(), 1);
    assert_eq!(
        block_on(store.list("b")).expect("list").len(),
        1,
        "other subscriptions untouched"
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_in_memory_parked_store_passes_the_contract() {
        super::parked_store_contract(super::InMemoryParkedStore::<u64>::new);
    }
}
