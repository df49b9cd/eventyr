//! The store proper: partitions, keys, append and read paths.

use std::sync::Mutex;

use fjall::{KeyspaceCreateOptions, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace};
use futures::Stream;
use futures::stream::iter;
use serde::{Deserialize, Serialize};

use eventyr_core::envelope::{EventEnvelope, Metadata, NewEvent};
use eventyr_core::error::StoreError;
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
use eventyr_store::store::{EventStore, StreamsAll};

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
}

/// An embedded [`EventStore`] and [`StreamsAll`] over fjall.
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
    /// The write-side serialization point (see the type's docs).
    write_lock: Mutex<()>,
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
            write_lock: Mutex::new(()),
            _event: std::marker::PhantomData,
        }
    }
}

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
            write_lock: Mutex::new(()),
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
            metadata: Metadata {
                causation_id: row.causation_id,
                correlation_id: row.correlation_id,
                ..Metadata::default()
            },
        })
    }
}

fn corrupt(error: impl std::fmt::Display) -> StoreError {
    StoreError::Other(std::sync::Arc::new(FjallStoreError::CorruptRow(format!(
        "{error}"
    ))))
}

fn engine(error: fjall::Error) -> StoreError {
    StoreError::Other(std::sync::Arc::new(FjallStoreError::Engine(error)))
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
            if events.is_empty() {
                return Ok(Vec::new());
            }

            // The whole interaction is one write transaction (single
            // writer): check the head, then write everything, or write
            // nothing.
            let _guard = this.lock();
            let mut tx = this.keyspace.write_tx();

            let current = match tx.get(&this.heads, stream_id.as_str()).map_err(engine)? {
                Some(head) => u64::from_be_bytes(
                    *head
                        .first_chunk::<8>()
                        .ok_or_else(|| corrupt("the head is not 8 bytes"))?,
                ),
                None => 0,
            };
            let matches = match expected {
                ExpectedVersion::Any => true,
                ExpectedVersion::Empty => current == 0,
                ExpectedVersion::Exact(version) => current == version.as_u64(),
            };
            if !matches {
                return Err(StoreError::Conflict {
                    current: Version::new(current),
                });
            }

            let next = match tx.get(&this.meta, KEY_NEXT_SEQUENCE).map_err(engine)? {
                Some(value) => this.read_next_sequence(&value)?,
                None => 0,
            };

            let mut committed = Vec::with_capacity(events.len());
            for (index, event) in events.into_iter().enumerate() {
                let version = Version::new(current + index as u64 + 1);
                let sequence = Sequence::new(next + index as u64 + 1);
                let row = StoredRow {
                    sequence: sequence.as_u64(),
                    version: version.as_u64(),
                    event_type: event.event.event_name().to_string(),
                    payload: event.event,
                    causation_id: event.metadata.causation_id.clone(),
                    correlation_id: event.metadata.correlation_id.clone(),
                };
                let bytes = serde_json::to_vec(&row).map_err(corrupt)?;
                let key = Self::stream_key(&stream_id, version);
                tx.insert(&this.streams, key, bytes);
                tx.insert(
                    &this.global,
                    format!("{:016}", sequence.as_u64()),
                    Self::stream_key(&stream_id, version),
                );
                committed.push(EventEnvelope {
                    sequence,
                    stream_id: stream_id.clone(),
                    version,
                    event: row.payload,
                    metadata: Metadata {
                        causation_id: row.causation_id,
                        correlation_id: row.correlation_id,
                        ..Metadata::default()
                    },
                });
            }

            tx.insert(
                &this.heads,
                stream_id.as_str(),
                (current + committed.len() as u64).to_be_bytes(),
            );
            tx.insert(
                &this.meta,
                KEY_NEXT_SEQUENCE,
                (next + committed.len() as u64).to_be_bytes(),
            );
            tx.commit().map_err(engine)?;

            Ok(committed)
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
        let (lower, upper) = Self::stream_range(stream_id, from);
        let tx = self.keyspace.read_tx();
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
        let lower = format!("{:016}", from.as_u64() + 1).into_bytes();
        let mut upper = format!("{:016}", u64::MAX).into_bytes();
        upper.push(0xff);
        let tx = self.keyspace.read_tx();
        let events: Vec<Result<EventEnvelope<E>, StoreError>> = tx
            .range(&self.global, lower..upper)
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
            .collect();
        iter(events)
    }
}
