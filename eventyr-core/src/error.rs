//! Error vocabulary shared by the machine protocols and the store port.

use alloc::string::String;
use alloc::sync::Arc;
use core::fmt;

use crate::vocabulary::{StreamId, Version};

/// A store operation failed — a stream read, an append, or a
/// snapshot read. (A snapshot read failing fails the interaction: the
/// store just told the machine its reads are broken.)
#[derive(Clone, Debug)]
pub enum StoreError {
    /// Optimistic-concurrency violation: the append's target is at
    /// `current`, not at the expected version. The write machine may
    /// retry. `stream_id` names the conflicting stream when the store
    /// knows it (a batch names it); a single-stream append leaves it
    /// `None` — the caller already knows its stream.
    Conflict {
        /// The conflicting stream, when the store knows it.
        stream_id: Option<StreamId>,
        /// The conflicting stream's actual version at append time.
        current: Version,
    },
    /// A boundary append's [`AppendCondition`](crate::boundary::AppendCondition)
    /// failed: an event matching its query was committed at `sequence`,
    /// after the condition's position. The boundary machine may retry.
    QueryConflict {
        /// The highest matching position the store saw.
        sequence: crate::vocabulary::Sequence,
    },
    /// The stream is closed (roadmap 0.7.6): it accepts no more appends. Its
    /// history stays readable unless it was also truncated.
    StreamClosed {
        /// The closed stream.
        stream_id: StreamId,
    },
    /// The read asked for events the store no longer has (roadmap 0.7.6): the
    /// stream was truncated, and its first remaining event is at
    /// `first`. Never folded around — state rebuilt from a partial
    /// history would be wrong without saying so. A reader that starts
    /// at or after `first - 1` (a snapshot at or past the cut) is
    /// unaffected.
    Truncated {
        /// The truncated stream.
        stream_id: StreamId,
        /// The first version the store still has.
        first: Version,
    },
    /// Transient failure (connection lost, timeout). Retrying the whole
    /// interaction is the caller's business; the machine does not retry
    /// I/O.
    Unavailable,
    /// Anything else: abort. Also how the machine reports driver
    /// protocol violations (see [`ProtocolError`]).
    Other(Arc<dyn core::error::Error + Send + Sync>),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict { stream_id, current } => match stream_id {
                Some(stream_id) => {
                    write!(f, "version conflict: {stream_id} is at {current}")
                }
                None => write!(f, "version conflict: stream is at {current}"),
            },
            Self::QueryConflict { sequence } => {
                write!(
                    f,
                    "append condition failed: a matching event exists at {sequence}"
                )
            }
            Self::StreamClosed { stream_id } => write!(f, "stream {stream_id} is closed"),
            Self::Truncated { stream_id, first } => {
                write!(f, "stream {stream_id} was truncated: it starts at {first}")
            }
            Self::Unavailable => f.write_str("store unavailable"),
            Self::Other(source) => write!(f, "store failure: {source}"),
        }
    }
}

impl core::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Other(source) => {
                let source: &(dyn core::error::Error + 'static) = &**source;
                Some(source)
            }
            _ => None,
        }
    }
}

impl From<ProtocolError> for StoreError {
    fn from(error: ProtocolError) -> Self {
        Self::Other(Arc::new(error))
    }
}

impl From<String> for StoreError {
    fn from(message: String) -> Self {
        struct StringError(String);

        impl fmt::Display for StringError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl fmt::Debug for StringError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl core::error::Error for StringError {}

        Self::Other(Arc::new(StringError(message)))
    }
}

impl StoreError {
    /// Wrap a message as [`Other`](StoreError::Other).
    pub fn other(message: impl Into<String>) -> Self {
        Self::from(message.into())
    }

    /// A driver protocol violation: a [`ProtocolError`] carrying
    /// `message`, wrapped as [`Other`](StoreError::Other). The one way
    /// every machine reports a driver that broke its protocol.
    pub fn protocol(message: &'static str) -> Self {
        Self::from(ProtocolError::new(message))
    }

    /// Whether this is a driver protocol violation — an
    /// [`Other`](StoreError::Other) carrying a [`ProtocolError`], as
    /// [`protocol`](StoreError::protocol) builds it.
    pub fn is_protocol_violation(&self) -> bool {
        matches!(self, Self::Other(source) if source.downcast_ref::<ProtocolError>().is_some())
    }
}

/// A driver violated the machine protocol: fed an input the current phase
/// does not accept, or drove a finished machine.
///
/// Machines never panic on bad input — they terminate with this error
/// wrapped in [`StoreError::Other`].
#[derive(Debug)]
pub struct ProtocolError(&'static str);

impl ProtocolError {
    /// Wrap a static description of the violation.
    pub fn new(message: &'static str) -> Self {
        Self(message)
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl core::error::Error for ProtocolError {}

/// An upcaster could not transform a historical event it selected.
///
/// A silently dropped historical event is data loss, so this is a loud
/// failure, never a `None`.
#[derive(Debug)]
pub struct UpcastError {
    /// The stored event type name.
    pub event_type: String,
    /// What went wrong (e.g. a payload that failed to parse).
    pub message: String,
}

impl fmt::Display for UpcastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cannot upcast event `{}`: {}",
            self.event_type, self.message
        )
    }
}

impl core::error::Error for UpcastError {}
