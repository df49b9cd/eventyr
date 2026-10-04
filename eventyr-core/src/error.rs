//! Error vocabulary shared by the machine protocols and the store port.

use alloc::string::String;
use alloc::sync::Arc;
use core::fmt;

use crate::vocabulary::Version;

/// The failure kinds the write protocol distinguishes.
///
/// Store implementations map their concrete errors into these at the port
/// boundary; [`Other`](StoreError::Other) carries the source along for
/// diagnostics. [`Clone`]-able, so drivers can record every action and
/// input they see.
#[derive(Clone, Debug)]
pub enum StoreError {
    /// Optimistic-concurrency violation: the stream is at `current`, not
    /// at the expected version. The write machine may retry.
    Conflict {
        /// The stream's actual version at append time.
        current: Version,
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
            Self::Conflict { current } => {
                write!(f, "version conflict: stream is at {current}")
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
