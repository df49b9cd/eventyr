//! The store proper: partitions, keys, append and read paths.

use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
};
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
use eventyr_store::store::{
    EventStore, QueryAppend, StreamLifecycle, StreamsAll, TruncatePlan, plan_truncate,
    read_starts_before_cut, selected, validate_batch,
};

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
/// clones point at the same keyspaces. Every write is one fjall write
/// transaction, and fjall serializes them: `write_tx` takes a mutex the
/// database shares with all its clones and holds it until the
/// transaction commits or drops. Every clone of this store, and every
/// other handle on the same database (a
/// [`FjallSnapshotStore`](crate::snapshots::FjallSnapshotStore)),
/// queues on that one lock, held only across synchronous fjall calls.
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
            signal: self.signal.clone(),
            _event: std::marker::PhantomData,
        }
    }
}

/// Open a write transaction that fsyncs the journal on commit.
///
/// fjall's own default ([`PersistMode::Buffer`]) only hands the journal
/// to the OS, so a power loss could drop a commit the caller was told
/// about. An event store's append must be durable when it returns, so
/// every write transaction in this crate asks for
/// [`PersistMode::SyncAll`], whatever the database was configured with.
pub(crate) fn durable_write_tx(
    keyspace: &SingleWriterTxDatabase,
) -> fjall::SingleWriterWriteTx<'_> {
    keyspace.write_tx().durability(Some(PersistMode::SyncAll))
}

impl<E> FjallStore<E> {
    /// Open (or create) a store at `path` with fjall's defaults.
    ///
    /// Every commit fsyncs fjall's journal before it returns (see
    /// [`from_keyspace`](Self::from_keyspace)), so an acknowledged
    /// append survives a crash or power loss, and the transaction's
    /// atomicity means a crash never leaves half of one; a failed
    /// fsync surfaces as `Other(fjall::Error::...)`.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, FjallStoreError> {
        let keyspace = SingleWriterTxDatabase::builder(path).open()?;
        Self::from_keyspace(keyspace)
    }

    /// Open the partitions on an already-configured transactional
    /// database — the caller owns cache size, compaction policy, blob
    /// thresholds, everything but durability: this store's write
    /// transactions fsync the journal on every commit
    /// ([`PersistMode::SyncAll`]), even on a database opened with
    /// `manual_journal_persist`.
    pub fn from_keyspace(keyspace: SingleWriterTxDatabase) -> Result<Self, FjallStoreError> {
        Ok(Self {
            streams: keyspace.keyspace(PARTITION_STREAMS, KeyspaceCreateOptions::default)?,
            heads: keyspace.keyspace(PARTITION_HEADS, KeyspaceCreateOptions::default)?,
            global: keyspace.keyspace(PARTITION_GLOBAL, KeyspaceCreateOptions::default)?,
            meta: keyspace.keyspace(PARTITION_META, KeyspaceCreateOptions::default)?,
            lifecycle: keyspace.keyspace(PARTITION_LIFECYCLE, KeyspaceCreateOptions::default)?,
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
        let lower = Self::stream_key(stream_id, Version::new(from.as_u64().saturating_add(1)));
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

    fn read_next_sequence(&self, head: &fjall::Slice) -> Result<u64, StoreError> {
        // Called with the counter's current value.
        head.first_chunk::<8>()
            .map(|bytes| u64::from_be_bytes(*bytes))
            .ok_or_else(|| corrupt("the sequence counter is not 8 bytes"))
    }

    /// The append path, single- or multi-stream: one write transaction
    /// checks every head, writes every event, advances the heads and
    /// the sequence counter, and commits — all-or-nothing across the
    /// whole batch, under fjall's single-writer lock.
    ///
    /// Returns each input's stream with its committed envelopes, in
    /// input order. The global sequence is assigned in input order.
    fn write_batch(
        &self,
        appends: Vec<StreamAppend<E>>,
    ) -> Result<Vec<CommittedStream<E>>, StoreError>
    where
        E: EventName + serde::Serialize,
    {
        let mut tx = durable_write_tx(&self.keyspace);
        let committed = self.write_in(&mut tx, appends)?;
        tx.commit().map_err(engine)?;
        self.notify_if_written(&committed);
        Ok(committed)
    }

    /// Raise the commit signal when the commit wrote any event.
    fn notify_if_written(&self, committed: &[CommittedStream<E>]) {
        if committed.iter().any(|stream| !stream.events.is_empty()) {
            self.signal.notify();
        }
    }

    /// The body of [`write_batch`](Self::write_batch), inside a write
    /// transaction the caller holds (and commits) — so a conditional
    /// append can check its condition in the same transaction it writes
    /// in. The caller has already refused a batch naming a stream twice:
    /// pass 1 reads every head before pass 2 writes, so a repeated stream
    /// would be written twice at the same versions.
    fn write_in(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        appends: Vec<StreamAppend<E>>,
    ) -> Result<Vec<CommittedStream<E>>, StoreError>
    where
        E: EventName + serde::Serialize,
    {
        // Pass 1: every head, before anything is written.
        let mut currents = Vec::with_capacity(appends.len());
        for append in &appends {
            if self.life(tx, &append.stream_id)?.0 {
                return Err(StoreError::StreamClosed {
                    stream_id: append.stream_id.clone(),
                });
            }
            let current = self.head(tx, &append.stream_id)?;
            if !eventyr_store::store::expected_version_matches(append.expected, current) {
                return Err(StoreError::Conflict {
                    stream_id: Some(append.stream_id.clone()),
                    current: Version::new(current),
                });
            }
            currents.push(current);
        }

        let total_events: usize = appends.iter().map(|append| append.events.len()).sum();
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
        for (append, current) in appends.into_iter().zip(currents) {
            let stream_id = append.stream_id;
            let mut envelopes = Vec::with_capacity(append.events.len());
            for (index, event) in append.events.into_iter().enumerate() {
                let version = Version::new(current + index as u64 + 1);
                sequence_offset += 1;
                let sequence = Sequence::new(next + sequence_offset);
                let row = StoredRow {
                    sequence: sequence.as_u64(),
                    version: version.as_u64(),
                    event_type: event.event.event_name().to_string(),
                    payload: event.event,
                    causation_id: event.metadata.causation_id,
                    correlation_id: event.metadata.correlation_id,
                    idempotency_key: event.metadata.idempotency_key,
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
                    metadata: Metadata::stored(
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
            committed.push(CommittedStream {
                stream_id,
                events: envelopes,
            });
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
            metadata: Metadata::stored(row.causation_id, row.correlation_id, row.idempotency_key),
        })
    }
}

impl<E> FjallStore<E>
where
    E: serde::de::DeserializeOwned,
{
    /// Resolve one `global` entry: the value is the `streams` key
    /// `"{stream_id}\0{version:016}"`, so the row is at that key and the
    /// stream id is the part before the separator.
    fn resolve(
        &self,
        tx: &impl Readable,
        guard: fjall::Guard,
    ) -> Result<EventEnvelope<E>, StoreError> {
        let pointer = guard.value().map_err(engine)?;
        let Some(at) = pointer.iter().position(|&byte| byte == 0) else {
            return Err(corrupt("a global-sequence key had no stream-id separator"));
        };
        let stream_id = StreamId::from(String::from_utf8_lossy(&pointer[..at]).into_owned());
        match tx.get(&self.streams, &pointer).map_err(engine)? {
            Some(bytes) => self.decode(&bytes, &stream_id),
            None => Err(corrupt("a global-sequence pointer resolved to no row")),
        }
    }

    /// The `global` key range after `from` (exclusive).
    fn global_range(from: Sequence) -> core::ops::Range<Vec<u8>> {
        let lower = format!("{:016}", from.as_u64().saturating_add(1)).into_bytes();
        let mut upper = format!("{:016}", u64::MAX).into_bytes();
        upper.push(0xff);
        lower..upper
    }

    /// Every envelope after `from` (exclusive), in global order, read
    /// lazily through `tx` — the write transaction, for a conditional
    /// append's check.
    fn scan_global<'a>(
        &'a self,
        tx: &'a impl Readable,
        from: Sequence,
    ) -> impl Iterator<Item = Result<EventEnvelope<E>, StoreError>> + 'a {
        tx.range(&self.global, Self::global_range(from))
            .map(move |guard| self.resolve(tx, guard))
    }

    /// [`scan_global`](Self::scan_global) over a fresh snapshot the
    /// iterator owns: the read path of `stream_all` and query reads.
    ///
    /// Lazy: an entry is resolved and decoded only when the consumer
    /// asks for it, so a subscriber that takes a few events after its
    /// checkpoint never reads the rest of the log. The snapshot fixes
    /// what the whole read sees, however slowly it is consumed — commits
    /// after it began are not in it, and nothing in it can change.
    fn snapshot_global(
        &self,
        from: Sequence,
    ) -> impl Iterator<Item = Result<EventEnvelope<E>, StoreError>> + Send {
        let snapshot = self.keyspace.read_tx();
        let range = snapshot.range(&self.global, Self::global_range(from));
        let this = self.clone();
        range.map(move |guard| this.resolve(&snapshot, guard))
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
        let append = StreamAppend {
            stream_id: stream_id.clone(),
            expected,
            events,
        };
        async move {
            let mut committed = this.write_batch(vec![append])?;
            // One append in the batch: the write already committed; the
            // caller asked for the single stream's envelopes.
            debug_assert_eq!(committed.len(), 1);
            Ok(committed.remove(0).events)
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
            validate_batch(&appends)?;
            this.write_batch(appends)
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
        let checked = self
            .life(&tx, stream_id)
            .and_then(|(_, first)| read_starts_before_cut(stream_id, from, first));
        if let Err(error) = checked {
            return iter(vec![Err(error)]);
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
    /// Lazy over one snapshot: entries are read as the stream is polled,
    /// so `stream_all(from).take(n)` costs `n` reads, not the log's tail.
    fn stream_all(
        &self,
        from: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send
    where
        E: serde::de::DeserializeOwned,
    {
        iter(self.snapshot_global(from))
    }
}

impl<E> QueryAppend for FjallStore<E>
where
    E: serde::Serialize + serde::de::DeserializeOwned + EventName + Tagged + Clone + Send + Sync,
{
    /// A lazy scan of the global log over one snapshot, matched on the
    /// decoded event (`selected`, private below).
    fn read(
        &self,
        query: &Query,
        after: Sequence,
    ) -> impl Stream<Item = Result<EventEnvelope<E>, StoreError>> + Send {
        let query = query.clone();
        let scan = (!query.items.is_empty()).then(|| self.snapshot_global(after));
        iter(
            scan.into_iter()
                .flatten()
                .filter(move |result| selected(&query, result)),
        )
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
            validate_batch(&appends)?;
            let mut tx = durable_write_tx(&this.keyspace);
            if !condition.query.items.is_empty() {
                let mut latest = None;
                for envelope in this
                    .scan_global(&tx, condition.after)
                    .filter(|result| selected(&condition.query, result))
                {
                    latest = Some(envelope?.sequence);
                }
                if let Some(sequence) = latest {
                    return Err(StoreError::QueryConflict { sequence });
                }
            }
            let committed = this.write_in(&mut tx, appends)?;
            tx.commit().map_err(engine)?;
            this.notify_if_written(&committed);
            Ok(committed)
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
            let mut tx = durable_write_tx(&this.keyspace);
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
            let mut tx = durable_write_tx(&this.keyspace);
            let (closed, first) = this.life(&tx, &stream_id)?;
            let head = this.head(&tx, &stream_id)?;
            let TruncatePlan::Cut(cut) = plan_truncate(&stream_id, head, first, version)? else {
                return Ok(());
            };
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
