//! # Live push — an `EventBus` transport waking a projector, §6 / 0.3 +
//! 0.7.2.
//!
//! A projector polls. Once it has caught up it sleeps until its next
//! poll, so a commit made during that sleep waits for it. Two seams cut
//! the wait short:
//!
//! - [`EventBus`] publishes committed envelopes to subscribers. No
//!   transport ships in the workspace — the trait is the seam a broker,
//!   a websocket, or a Postgres `NOTIFY` relay implements. Here the
//!   transport is a channel and a receive thread.
//! - A [`CommitSignal`] wakes a projector out of its idle sleep
//!   (`Projector::wake_on`). The subscriber side turns each push it
//!   receives into a wake-up on a [`LocalCommitSignal`].
//!
//! Push is only a hint. The projector still reads the log from its
//! checkpoint, so a publish that never arrives costs latency, never an
//! event. The run shows both halves: a commit made while the projector
//! sleeps for an hour is applied as soon as the push lands, and a
//! commit published to nobody is picked up by the next run from its
//! checkpoint.
//!
//! Run with:
//! ```sh
//! cargo run -p eventyr --example bus --features bus
//! ```

#![allow(dead_code)] // A demo: the point is the seam.

use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use eventyr::prelude::*;
use eventyr::store::prelude::*;
use eventyr::subscription::prelude::{
    Catch, EventBus, InMemoryCheckpointStore, Projection, Projector, StoreSubscription,
};

/// The event: a numbered ping.
#[derive(Clone, Debug, PartialEq)]
struct Ping(u64);

// -- the transport -------------------------------------------------------

/// A channel transport: one sender per subscriber.
#[derive(Default)]
struct ChannelBus {
    subscribers: Mutex<Vec<Sender<EventEnvelope<Ping>>>>,
}

impl ChannelBus {
    fn subscribe(&self) -> Receiver<EventEnvelope<Ping>> {
        let (sender, receiver) = mpsc::channel();
        self.subscribers.lock().expect("the lock").push(sender);
        receiver
    }

    /// Unplug every subscriber: later publishes reach no one.
    fn disconnect(&self) {
        self.subscribers.lock().expect("the lock").clear();
    }
}

impl EventBus for ChannelBus {
    type Event = Ping;

    async fn publish(&self, envelope: &EventEnvelope<Ping>) -> Result<(), StoreError> {
        for subscriber in self.subscribers.lock().expect("the lock").iter() {
            // Best-effort: a subscriber that went away loses a hint, and
            // its projector still polls.
            let _ = subscriber.send(envelope.clone());
        }
        Ok(())
    }
}

// -- the publishing store ------------------------------------------------

/// Any store, publishing each committed envelope through a bus — after
/// the commit, so a subscriber woken by the push finds the event
/// readable.
struct PublishingStore<B> {
    inner: InMemoryStore<Ping>,
    bus: B,
}

impl<B: EventBus<Event = Ping>> PublishingStore<B> {
    async fn publish(&self, committed: &[EventEnvelope<Ping>]) {
        for envelope in committed {
            // A failed publish costs subscribers a wake-up, not the
            // event, and the commit has already happened: it is not the
            // writer's error.
            let _ = self.bus.publish(envelope).await;
        }
    }
}

impl<B: EventBus<Event = Ping>> EventStore for PublishingStore<B> {
    type Event = Ping;

    async fn append(
        &self,
        stream_id: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent<Ping>>,
    ) -> Result<Vec<EventEnvelope<Ping>>, StoreError> {
        let committed = self.inner.append(stream_id, expected, events).await?;
        self.publish(&committed).await;
        Ok(committed)
    }

    async fn append_batch(
        &self,
        appends: Vec<StreamAppend<Ping>>,
    ) -> Result<Vec<CommittedStream<Ping>>, StoreError> {
        let committed = self.inner.append_batch(appends).await?;
        for stream in &committed {
            self.publish(&stream.events).await;
        }
        Ok(committed)
    }

    fn stream(
        &self,
        stream_id: &StreamId,
        from: Version,
    ) -> impl futures::Stream<Item = Result<EventEnvelope<Ping>, StoreError>> + Send {
        self.inner.stream(stream_id, from)
    }
}

impl<B: EventBus<Event = Ping>> StreamsAll for PublishingStore<B> {
    fn stream_all(
        &self,
        from: Sequence,
    ) -> impl futures::Stream<Item = Result<EventEnvelope<Ping>, StoreError>> + Send {
        self.inner.stream_all(from)
    }
}

// -- the subscriber side -------------------------------------------------

/// The receive loop: every push becomes a wake-up. Returns the
/// sequences it saw once the bus disconnects.
fn bridge(
    pushes: Receiver<EventEnvelope<Ping>>,
    wake: LocalCommitSignal,
    seen: Arc<Mutex<Vec<u64>>>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        for envelope in pushes {
            wake.notify();
            seen.lock()
                .expect("the lock")
                .push(envelope.sequence.as_u64());
        }
    })
}

/// The read model: how many pings were applied.
struct Counter(Arc<AtomicU64>);
impl Projection for Counter {
    type Event = Ping;
    type Error = Infallible;
    async fn apply(&mut self, _envelope: &EventEnvelope<Ping>) -> Result<(), Self::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Counts the projector's idle sleeps — each one means "caught up".
#[derive(Default)]
struct Idle(AtomicU64);
impl Catch for Idle {
    fn caught_up(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Wait (up to five seconds) for `done`.
async fn until(what: &str, done: impl Fn() -> bool) {
    let wait = async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

async fn ping(store: &PublishingStore<ChannelBus>, n: u64) {
    store
        .append(
            &StreamId::from("pings"),
            ExpectedVersion::Any,
            vec![NewEvent::new(Ping(n))],
        )
        .await
        .expect("the ping commits");
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let bus = ChannelBus::default();
    let pushes = bus.subscribe();
    let store = PublishingStore {
        inner: InMemoryStore::new(),
        bus,
    };
    let wake = LocalCommitSignal::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let receiver = bridge(pushes, wake.clone(), seen.clone());

    // One ping before the projector starts; its push is handled before
    // the projector arms its listener.
    ping(&store, 1).await;
    until("the first push", || {
        seen.lock().expect("the lock").len() == 1
    })
    .await;

    // -- a commit during an hour-long idle sleep -------------------------
    let checkpoints = InMemoryCheckpointStore::new();
    let applied = Arc::new(AtomicU64::new(0));
    let idle = Idle::default();
    let run = Projector::new(
        "pings",
        StoreSubscription::new(&store),
        &checkpoints,
        Counter(applied.clone()),
    )
    .with_policy(SubscriptionPolicy::new(
        16,
        Duration::from_secs(3_600),
        Duration::ZERO,
    ))
    .wake_on(wake.clone())
    .caught_up_on(&idle)
    .run_woken(tokio::time::sleep);
    let script = async {
        // Caught up on the first ping; now asleep for an hour.
        until("the projector to idle", || {
            idle.0.load(Ordering::SeqCst) >= 1
        })
        .await;
        assert_eq!(applied.load(Ordering::SeqCst), 1);

        // The second ping's push wakes it: applied in moments, not in
        // an hour.
        ping(&store, 2).await;
        until("the pushed ping to apply", || {
            applied.load(Ordering::SeqCst) == 2
        })
        .await;
    };
    tokio::select! {
        outcome = run => panic!("the projector runs until stopped: {outcome:?}"),
        () = script => {}
    }
    // Leaving `select!` dropped the run mid-sleep — an interrupted
    // projector, resumable from its last ack.

    // -- a publish that reaches no one ----------------------------------
    store.bus.disconnect();
    receiver.join().expect("the receive loop ends with the bus");
    ping(&store, 3).await;
    assert_eq!(
        *seen.lock().expect("the lock"),
        vec![1, 2],
        "the third ping was published to no one"
    );

    // The next run resumes from the checkpoint and applies it anyway.
    let outcome = Projector::new(
        "pings",
        StoreSubscription::new(&store),
        &checkpoints,
        Counter(applied.clone()),
    )
    .with_policy(SubscriptionPolicy::default().stop_at_catch_up())
    .run(|_| std::future::ready(()))
    .await
    .expect("the catch-up run");
    assert!(matches!(
        outcome,
        SubscriptionOutcome::CaughtUp { checkpoint } if checkpoint == Checkpoint::new(Sequence::new(3))
    ));
    assert_eq!(applied.load(Ordering::SeqCst), 3);

    println!(
        "bus: a pushed commit woke the sleeping projector; \
         the unpublished one was caught up from the checkpoint"
    );
}
