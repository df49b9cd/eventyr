//! The store proper: partitions, keys, append and read paths.

use std::sync::Mutex;

use fjall::{KeyspaceCreateOptions, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace};
use futures::Stream;
use futures::stream::iter;
use serde::{Deserialize, Serialize};

use eventyr_core::batch::{CommittedStream, StreamAppend};
use eventyr_core::boundary::{AppendCondition, Query, Tagged};
use eventyr_core::envelope::{EventEnvelope, Metadata, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
use eventyr_store::notify::{CommitSignal, LocalCommitListener, LocalCommitSignal};
use eventyr_store::store::{EventStore, QueryAppend, StreamLifecycle, StreamsAll};

use crate::FjallStoreError;

/// The partitions, and the keys inside them.
///
/// - `streams`: `"{stream_id}\0{version:016}"` → a JSON
///   [`StoredRow`]. The zero-padded version keeps one stream's keys in
///   version order, so `stream(from)` is a range scan from
///   `"{stream_id}\0{from+1:016}"` up to `"{stream_id}\0\xff"`.
/// - `heads`: `stream_id` → `"{version:016}"`, the stream's current
///   version; read inside the append transaction to enforce the
///   [`ExpectedVersion`] compaction.
/// - `global`: `"{sequence:016}"` → `"{stream_id}\0{version:016}"`,
///   the pointer resolving [`StreamsAll::stream_all`]'s sequence scan.
/// - `meta`: `"next"` → `u64` big-endian, the global sequence counter
///   (started at 0; the first event is sequence 1).
const PARTITION_STREAMS: &str = "streams";
const PARTITION_HEADS: &str = "heads";
const PARTITION_GLOBAL: &str = "global";
const PARTITION_META: &str = "meta";
/// `stream_id` → `[closed: u8, first_kept: u64 BE]`, only for streams
/// that were closed or truncated (0.7.6). `heads` already keeps a
/// truncated stream's version.
const PARTITION_LIFECYCLE: &str = "lifecycle";
const KEY_NEXT_SEQUENCE: &[u8] = b"next";

/// The serialized row in `streams`: everything an
/// [`EventEnvelope`] needs, once.
#[derive(Serialize, Deserialize)]
struct StoredRow<E> {
    sequence: u64,
    version: u64,
    event_type: String,
    payload: E,
    causation_id: Option<String>,
    correlation_id: Option<String>,
    /// 0.7.5; rows written before it decode with none.
    #[serde(default)]
    idempotency_key: Option<String>,
}

/// An embedded [`EventStore`] and [`StreamsAll`] over fjall — and, for
/// [`Tagged`] events, [`QueryAppend`] (0.7.1), answered by scanning the
/// global log. Its [`CommitSignal`] (0.7.2) wakes subscribers on every
/// commit made through this store or its clones.
///
/// Shareable and cloneable (it wraps a [`fjall::SingleWriterTxDatabase`]):
/// clones point at the same keyspaces. All writes go through one
/// `WriteTransaction`, serialized by a mutex — fjall's single-writer
/// database accepts one writer at a time, and the lock is
/// held only across synchronous fjall calls.
///
/// Construct via [`open`](FjallStore::open) (defaults) or
/// [`from_keyspace`](FjallStore::from_keyspace) (a configured
/// database). Clone it to share; the keyspaces open once, at
/// construction.
pub struct FjallStore<E> {
    keyspace: SingleWriterTxDatabase,
    streams: SingleWriterTxKeyspace,
    heads: SingleWriterTxKeyspace,
    global: SingleWriterTxKeyspace,
    meta: SingleWriterTxKeyspace,
    lifecycle: SingleWriterTxKeyspace,
    /// The write-side serialization point (see the type's docs).
    write_lock: Mutex<()>,
    /// Raised after every commit (0.7.2); shared by clones.
    signal: LocalCommitSignal,
    _event: std::marker::PhantomData<fn() -> E>,
}

impl<E> Clone for FjallStore<E> {
    fn clone(&self) -> Self {
        Self {
            keyspace: self.keyspace.clone(),
            streams: self.streams.clone(),
            heads: self.heads.clone(),
            global: self.global.clone(),
            meta: self.meta.clone(),
            lifecycle: self.lifecycle.clone(),
            write_lock: Mutex::new(()),
            signal: self.signal.clone(),
            _event: std::marker::PhantomData,
        }
    }
}

type WrittenStream<E> = (StreamId, Vec<EventEnvelope<E>>);

impl<E> FjallStore<E> {
    /// Open (or create) a store at `path` with fjall's defaults.
    ///
    /// Defaults mean: durable, snappy-compressed, journal fsynced on
    /// every commit — the append transaction's atomicity survives a
    /// crash; a fsync failure surfaces as `Other(fjall::Error::...)`.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, FjallStoreError> {
        let keyspace = SingleWriterTxDatabase::builder(path).open()?;
        Self::from_keyspace(keyspace)
    }

    /// Open the partitions on an already-configured transactional
    /// database — the caller owns cache size, compaction policy, blob
    /// thresholds, everything.
    pub fn from_keyspace(keyspace: SingleWriterTxDatabase) -> Result<Self, FjallStoreError> {
        Ok(Self {
            streams: keyspace.keyspace(PARTITION_STREAMS, KeyspaceCreateOptions::default)?,
            heads: keyspace.keyspace(PARTITION_HEADS, KeyspaceCreateOptions::default)?,
            global: keyspace.keyspace(PARTITION_GLOBAL, KeyspaceCreateOptions::default)?,
            meta: keyspace.keyspace(PARTITION_META, KeyspaceCreateOptions::default)?,
            lifecycle: keyspace.keyspace(PARTITION_LIFECYCLE, KeyspaceCreateOptions::default)?,
            write_lock: Mutex::new(()),
            signal: LocalCommitSignal::new(),
            keyspace,
            _event: std::marker::PhantomData,
        })
    }

    /// `"{stream_id}\0{version:016}"` — the `streams` key.
    fn stream_key(stream_id: &StreamId, version: Version) -> Vec<u8> {
        let mut key = Vec::with_capacity(stream_id.as_str().len() + 1 + 16);
        key.extend_from_slice(stream_id.as_str().as_bytes());
        key.push(0);
        key.extend_from_slice(format!("{:016}", version.as_u64()).as_bytes());
        key
    }

    /// The range covering one stream from `from` (exclusive) on.
    fn stream_range(stream_id: &StreamId, from: Version) -> (Vec<u8>, Vec<u8>) {
        let lower = Self::stream_key(stream_id, Version::new(from.as_u64() + 1));
        let mut upper = stream_id.as_str().as_bytes().to_vec();
        upper.push(0);
        upper.push(0xff);
        (lower, upper)
    }

    /// `(closed, first_kept)` for `stream_id`: `(false, 1)` unless the
    /// stream was closed or truncated.
    fn life(&self, tx: &impl Readable, stream_id: &StreamId) -> Result<(bool, u64), StoreError> {
        match tx
            .get(&self.lifecycle, stream_id.as_str())
            .map_err(engine)?
        {
            None => Ok((false, 1)),
            Some(value) => {
                let (closed, first) = value
                    .split_first()
                    .ok_or_else(|| corrupt("an empty lifecycle record"))?;
                let first = first
                    .first_chunk::<8>()
                    .ok_or_else(|| corrupt("a lifecycle record without its cut"))?;
                Ok((*closed != 0, u64::from_be_bytes(*first)))
            }
        }
    }

    fn head(&self, tx: &impl Readable, stream_id: &StreamId) -> Result<u64, StoreError> {
        match tx.get(&self.heads, stream_id.as_str()).map_err(engine)? {
            Some(head) => Ok(u64::from_be_bytes(
                *head
                    .first_chunk::<8>()
                    .ok_or_else(|| corrupt("the head is not 8 bytes"))?,
            )),
            None => Ok(0),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        // No code under this lock can panic meaningfully (fjall's
        // Result error path is a return, not a panic), and a panicking
        // holder leaves the keyspace itself consistent — take the lock
        // rather than propagate.
        self.write_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn read_next_sequence(&self, head: &fjall::Slice) -> Result<u64, StoreError> {
        // Called with the counter's current value.
        head.first_chunk::<8>()
            .map(|bytes| u64::from_be_bytes(*bytes))
            .ok_or_else(|| corrupt("the sequence counter is not 8 bytes"))
    }

    /// The append path, single- or multi-stream: one write transaction
    /// checks every head, writes every event, advances the heads and
    /// the sequence counter, and commits — all-or-nothing across the
    /// whole batch, under the single-writer lock.
    ///
    /// `appends` yields `(stream_id, expected, events)`; the returned
    /// vec pairs each input's stream with its committed envelopes, in
    /// input order. The global sequence is assigned in input order.
    fn write_batch(
        &self,
        appends: impl IntoIterator<Item = (StreamId, ExpectedVersion, Vec<NewEvent<E>>)>,
    ) -> Result<Vec<WrittenStream<E>>, StoreError>
    where
        E: EventName + serde::Serialize,
    {
        // The whole interaction is one write transaction (single
        // writer): check every head, then write everything, or write
        // nothing.
        let _guard = self.lock();
        let mut tx = self.keyspace.write_tx();
        let committed = self.write_in(&mut tx, appends)?;
        tx.commit().map_err(engine)?;
        self.notify_if_written(&committed);
        Ok(committed)
    }

    /// Raise the commit signal when the commit wrote any event.
    fn notify_if_written(&self, committed: &[WrittenStream<E>]) {
        if committed.iter().any(|(_, events)| !events.is_empty()) {
            self.signal.notify();
        }
    }

    /// The body of [`write_batch`](Self::write_batch), inside a write
    /// transaction the caller holds (and commits) under the write lock
    /// — so a conditional append can check its condition in the same
    /// transaction it writes in.
    fn write_in(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        appends: impl IntoIterator<Item = (StreamId, ExpectedVersion, Vec<NewEvent<E>>)>,
    ) -> Result<Vec<WrittenStream<E>>, StoreError>
    where
        E: EventName + serde::Serialize,
    {
        let appends: Vec<(StreamId, ExpectedVersion, Vec<NewEvent<E>>)> =
            appends.into_iter().collect();

        // Pass 1: every head, before anything is written.
        let mut currents = Vec::with_capacity(appends.len());
        for (stream_id, expected, _) in &appends {
            if self.life(tx, stream_id)?.0 {
                return Err(StoreError::StreamClosed {
                    stream_id: stream_id.clone(),
                });
            }
            let current = match tx.get(&self.heads, stream_id.as_str()).map_err(engine)? {
                Some(head) => u64::from_be_bytes(
                    *head
                        .first_chunk::<8>()
                        .ok_or_else(|| corrupt("the head is not 8 bytes"))?,
                ),
                None => 0,
            };
            let matches = eventyr_store::store::expected_version_matches(*expected, current);
            if !matches {
                return Err(StoreError::Conflict {
                    stream_id: Some(stream_id.clone()),
                    current: Version::new(current),
                });
            }
            currents.push(current);
        }

        let mut total_events = 0usize;
        for (_, _, events) in &appends {
            total_events += events.len();
        }
        let next = if total_events > 0 {
            match tx.get(&self.meta, KEY_NEXT_SEQUENCE).map_err(engine)? {
                Some(value) => self.read_next_sequence(&value)?,
                None => 0,
            }
        } else {
            // No events to write: the counter stays, the batch is a
            // commit of nothing.
            0
        };

        // Pass 2: write, knowing every expectation held.
        let mut sequence_offset = 0u64;
        let mut committed = Vec::with_capacity(appends.len());
        for ((stream_id, _, events), current) in appends.into_iter().zip(currents) {
            let mut envelopes = Vec::with_capacity(events.len());
            for (index, event) in events.into_iter().enumerate() {
                let version = Version::new(current + index as u64 + 1);
                sequence_offset += 1;
                let sequence = Sequence::new(next + sequence_offset);
                let row = StoredRow {
                    sequence: sequence.as_u64(),
                    version: version.as_u64(),
                    event_type: event.event.event_name().to_string(),
                    payload: event.event,
                    causation_id: event.metadata.causation_id.clone(),
                    correlation_id: event.metadata.correlation_id.clone(),
                    idempotency_key: event.metadata.idempotency_key.clone(),
                };
                let bytes = serde_json::to_vec(&row).map_err(corrupt)?;
                let key = Self::stream_key(&stream_id, version);
                tx.insert(&self.streams, key, bytes);
                tx.insert(
                    &self.global,
                    format!("{:016}", sequence.as_u64()),
                    Self::stream_key(&stream_id, version),
                );
                envelopes.push(EventEnvelope {
                    sequence,
                    stream_id: stream_id.clone(),
                    version,
                    event: row.payload,
                    metadata: row_metadata(
                        row.causation_id,
                        row.correlation_id,
                        row.idempotency_key,
                    ),
                });
            }
            tx.insert(
                &self.heads,
                stream_id.as_str(),
                (current + envelopes.len() as u64).to_be_bytes(),
            );
            committed.push((stream_id, envelopes));
        }

        // Advance the counter once, by the whole batch — and only when
        // anything was written, so an empty batch leaves no trace.
        if total_events > 0 {
            tx.insert(
                &self.meta,
                KEY_NEXT_SEQUENCE,
                (next + sequence_offset).to_be_bytes(),
            );
        }

        Ok(committed)
    }

    fn decode(&self, bytes: &[u8], stream_id: &StreamId) -> Result<EventEnvelope<E>, StoreError>
    where
        E: serde::de::DeserializeOwned,
    {
        let row: StoredRow<E> = serde_json::from_slice(bytes).map_err(corrupt)?;
        // The event_type was the upcast selector at write time; the row
        // keeps it for inspection, and re-decoding through `payload` is
        // the single source of truth the contract checks compare.
        let _ = &row.event_type;
        Ok(EventEnvelope {
            sequence: Sequence::new(row.sequence),
            stream_id: stream_id.clone(),
            version: Version::new(row.version),
            event: row.payload,
            metadata: row_metadata(row.causation_id, row.correlation_id, row.idempotency_key),
        })
    }
}

impl<E> FjallStore<E>
where
    E: serde::de::DeserializeOwned,
{
    /// Every envelope after `from` (exclusive), in global order, read
    /// through `tx` — a snapshot for reads, the write transaction for a
    /// conditional append's check.
    fn scan_global(
        &self,
        tx: &impl Readable,
        from: Sequence,
    ) -> Vec<Result<EventEnvelope<E>, StoreError>> {
        let lower = format!("{:016}", from.as_u64() + 1).into_bytes();
        let mut upper = format!("{:016}", u64::MAX).into_bytes();
        upper.push(0xff);
        tx.range(&self.global, lower..upper)
            .map(|guard| {
                let pointer = guard.value().map_err(engine)?;
                // The pointer is the `streams` key
                // `"{stream_id}\0{version:016}"`: the row is at that
                // key, the stream id before its separator.
                let stream_id = pointer
                    .iter()
                    .position(|&byte| byte == 0)
                    .map(|at| StreamId::from(String::from_utf8_lossy(&pointer[..at]).into_owned()));
                match stream_id {
                    Some(stream_id) => match tx.get(&self.streams, &pointer) {
                        Ok(Some(bytes)) => self.decode(&bytes, &stream_id),
                        Ok(None) => Err(corrupt("a global-sequence pointer resolved to no row")),
                        Err(error) => Err(engine(error)),
                    },
                    None => Err(corrupt("a global-sequence key had no stream-id separator")),
                }
            })
            .collect()
    }

    /// The envelopes after `from` that `query` selects — a scan of the
    /// global log, matched on the decoded event. No tag index: tags are
    /// a pure function of the payload, so an index built at append time
    /// would miss every event written before it existed; scanning is
    /// correct on any history.
    fn scan_query(
        &self,
        tx: &impl Readable,
        query: &Query,
        from: Sequence,
    ) -> Vec<Result<EventEnvelope<E>, StoreError>>
    where
        E: EventName + Tagged,
    {
        if query.items.is_empty() {
            return Vec::new();
        }
        self.scan_global(tx, from)
            .into_iter()
            .filter(|result| match result {
                Ok(envelope) => query.selects(&envelope.event),
                Err(_) => true,
            })
            .collect()
    }
}

fn row_metadata(
    causation_id: Option<String>,
    correlation_id: Option<String>,
    idempotency_key: Option<String>,
) -> Metadata {
    Metadata {
        idempotency_key,
        ..Metadata::of_ids(causation_id, correlation_id)
    }
}

fn corrupt(error: impl std::fmt::Display) -> StoreError {
    StoreError::Other(std::sync::Arc::new(FjallStoreError::CorruptRow(format!(
        "{error}"
    ))))
}

/// Engine errors: an I/O or lock failure is transient — the protocol's
/// `Unavailable` (read: caller may retry) — while a corrupt journal, a
/// version it can't read, or a failed commit is fatal-by-construction
/// (`Other`). The write machine never retries I/O; the distinction is
/// for the caller's retry policy.
fn engine(error: fjall::Error) -> StoreError {
    match error {
        fjall::Error::Io(_) | fjall::Error::Locked => StoreError::Unavailable,
        other => StoreError::Other(std::sync::Arc::new(FjallStoreError::Engine(other))),
    }
}

impl<E> EventStore for FjallStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName + Clone + Send + Sync,
{
    type Event = E;

    fn append(
        &self,
        stream_id: &StreamId,
        expected: ExpectedVersion,
        events: Vec<NewEvent<E>>,
    ) -> impl std::future::Future<Output = Result<Vec<EventEnvelope<E>>, StoreError>> + Send {
        let this = self.clone();
        let stream_id = stream_id.clone();
        async move {
            let mut committed = this.write_batch(std::iter::once((stream_id, expected, events)))?;
            // One append in the batch: the write already committed; the
            // caller asked for the single stream's envelopes.
            debug_assert_eq!(committed.len(), 1);
            Ok(committed.remove(0).1)
        }
    }

    /// Append the whole batch in one write transaction: every head
    /// checked before any write, then every event written, the heads
    /// and the sequence counter advanced, one commit — so the batch is
    /// all-or-nothing exactly as
    /// [`append_batch`](EventStore::append_batch) promises, and
    /// `stream_all` never observes it partially.
    fn append_batch(
        &self,
        appends: Vec<StreamAppend<E>>,
    ) -> impl std::future::Future<Output = Result<Vec<CommittedStream<E>>, StoreError>> + Send {
        let this = self.clone();
        async move {
            this.write_batch(
                appends
                    .into_iter()
                    .map(|a| (a.stream_id, a.expected, a.events)),
            )
            .map(|appends| {
                appends
                    .into_iter()
                    .map(|(stream_id, events)| CommittedStream { stream_id, events })
                    .collect()
            })
        }
    }

    fn stream(
        &self,
        stream_id: &StreamId,
        from: Version,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send
    where
        E: serde::de::DeserializeOwned,
    {
        let tx = self.keyspace.read_tx();
        match self.life(&tx, stream_id) {
            Ok((_, first)) if from.as_u64() + 1 < first => {
                return iter(vec![Err(StoreError::Truncated {
                    stream_id: stream_id.clone(),
                    first: Version::new(first),
                })]);
            }
            Ok(_) => {}
            Err(error) => return iter(vec![Err(error)]),
        }
        let (lower, upper) = Self::stream_range(stream_id, from);
        let events: Vec<Result<EventEnvelope<E>, StoreError>> = tx
            .range(&self.streams, lower..upper)
            .map(|guard| self.decode(&guard.value().map_err(engine)?, stream_id))
            .collect();
        iter(events)
    }
}

impl<E> StreamsAll for FjallStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName + Clone + Send + Sync,
{
    fn stream_all(
        &self,
        from: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send
    where
        E: serde::de::DeserializeOwned,
    {
        iter(self.scan_global(&self.keyspace.read_tx(), from))
    }
}

impl<E> QueryAppend for FjallStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName + Tagged + Clone + Send + Sync,
{
    fn read(
        &self,
        query: &Query,
        after: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send {
        iter(self.scan_query(&self.keyspace.read_tx(), query, after))
    }

    /// Check the condition and write in one write transaction. fjall's
    /// single-writer database hands out one write transaction at a time
    /// (database-wide, across every clone of this store), and reads in
    /// it see every committed write — so no append, conditional or not,
    /// can commit between the check and the write, and overlapping
    /// conditions serialize.
    fn append_if(
        &self,
        appends: Vec<StreamAppend<E>>,
        condition: AppendCondition,
    ) -> impl std::future::Future<Output = Result<Vec<CommittedStream<E>>, StoreError>> + Send {
        let this = self.clone();
        async move {
            let _guard = this.lock();
            let mut tx = this.keyspace.write_tx();
            let mut latest = None;
            for envelope in this.scan_query(&tx, &condition.query, condition.after) {
                latest = Some(envelope?.sequence);
            }
            if let Some(sequence) = latest {
                return Err(StoreError::QueryConflict { sequence });
            }
            let committed = this.write_in(
                &mut tx,
                appends
                    .into_iter()
                    .map(|a| (a.stream_id, a.expected, a.events)),
            )?;
            tx.commit().map_err(engine)?;
            this.notify_if_written(&committed);
            Ok(committed
                .into_iter()
                .map(|(stream_id, events)| CommittedStream { stream_id, events })
                .collect())
        }
    }
}

impl<E> CommitSignal for FjallStore<E>
where
    E: Send,
{
    type Listener = LocalCommitListener;

    fn subscribe(
        &self,
    ) -> impl std::future::Future<Output = Result<Self::Listener, StoreError>> + Send {
        self.signal.subscribe()
    }
}

impl<E> StreamLifecycle for FjallStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName + Clone + Send + Sync,
{
    fn close_stream(
        &self,
        stream_id: &StreamId,
    ) -> impl std::future::Future<Output = Result<(), StoreError>> + Send {
        let this = self.clone();
        let stream_id = stream_id.clone();
        async move {
            let _guard = this.lock();
            let mut tx = this.keyspace.write_tx();
            let (_, first) = this.life(&tx, &stream_id)?;
            let mut record = vec![1u8];
            record.extend_from_slice(&first.to_be_bytes());
            tx.insert(&this.lifecycle, stream_id.as_str(), record);
            tx.commit().map_err(engine)
        }
    }

    /// One write transaction: record the cut, then remove the stream's
    /// rows below it and their global pointers. The stream's head stays
    /// in `heads`, so its version survives.
    fn truncate_before(
        &self,
        stream_id: &StreamId,
        version: Version,
    ) -> impl std::future::Future<Output = Result<(), StoreError>> + Send {
        let this = self.clone();
        let stream_id = stream_id.clone();
        async move {
            let _guard = this.lock();
            let mut tx = this.keyspace.write_tx();
            let (closed, first) = this.life(&tx, &stream_id)?;
            let head = this.head(&tx, &stream_id)?;
            let cut = version.as_u64();
            if cut > head + 1 {
                return Err(StoreError::other(format!(
                    "cannot truncate {stream_id} before {version}: it ends at {head}"
                )));
            }
            if cut <= first {
                return Ok(());
            }
            let (lower, _) = Self::stream_range(&stream_id, Version::new(first - 1));
            let upper = Self::stream_key(&stream_id, version);
            let doomed: Vec<(Vec<u8>, u64)> = tx
                .range(&this.streams, lower..upper)
                .map(|guard| {
                    let (key, value) = guard.into_inner().map_err(engine)?;
                    let row: StoredRow<serde::de::IgnoredAny> =
                        serde_json::from_slice(&value).map_err(corrupt)?;
                    Ok((key.to_vec(), row.sequence))
                })
                .collect::<Result<_, StoreError>>()?;
            for (key, sequence) in doomed {
                tx.remove(&this.streams, key);
                tx.remove(&this.global, format!("{sequence:016}"));
            }
            let mut record = vec![u8::from(closed)];
            record.extend_from_slice(&cut.to_be_bytes());
            tx.insert(&this.lifecycle, stream_id.as_str(), record);
            tx.commit().map_err(engine)
        }
    }
}
