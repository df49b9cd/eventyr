//! The personal-field wrapper.

use core::fmt;

use serde::{Deserialize, Serialize};

/// One personal field of an event: plain while the domain holds it,
/// sealed in the log, or shredded once its subject's key is erased.
///
/// The field names its own data subject, so one event can carry the
/// personal data of several subjects, each erased on its own.
///
/// Serialization is the *stored* form, tagged `$sensitive` so the
/// [`ShreddingStore`](crate::ShreddingStore) can find it inside any
/// event's JSON. A `Plain` value serializes in the clear: append events
/// through a `ShreddingStore`, which seals every one first. `Debug`
/// output never shows the value.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "$sensitive", rename_all = "snake_case")]
pub enum Sensitive<T> {
    /// The value, in memory.
    Plain {
        /// The data subject the value belongs to.
        subject: String,
        /// The value.
        value: T,
    },
    /// The value, encrypted.
    Sealed(Sealed),
    /// The value is gone: its subject's key was erased.
    Shredded,
}

/// A sealed field as stored: which cipher, which subject, and the
/// ciphertext (base64, with the cipher's nonce inside).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sealed {
    /// [`Cipher::algorithm`](crate::Cipher::algorithm) at sealing time.
    pub algorithm: String,
    /// The data subject whose key sealed it.
    pub subject: String,
    /// The ciphertext, base64.
    pub ciphertext: String,
}

impl<T> Sensitive<T> {
    /// A plain value of `subject`'s, to be sealed before the event is
    /// appended.
    pub fn new(subject: impl Into<String>, value: T) -> Self {
        Self::Plain {
            subject: subject.into(),
            value,
        }
    }

    /// The value, or `None` when it was shredded (or not yet opened).
    pub fn get(&self) -> Option<&T> {
        match self {
            Self::Plain { value, .. } => Some(value),
            Self::Sealed(_) | Self::Shredded => None,
        }
    }

    /// The value, or `replacement` when it is gone — the declared
    /// stand-in a projection shows for erased data.
    pub fn or<'a>(&'a self, replacement: &'a T) -> &'a T {
        self.get().unwrap_or(replacement)
    }

    /// Whether the subject's key was erased.
    pub fn is_shredded(&self) -> bool {
        matches!(self, Self::Shredded)
    }
}

impl<T> fmt::Debug for Sensitive<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Plain { .. } => "Sensitive(<plain>)",
            Self::Sealed(_) => "Sensitive(<sealed>)",
            Self::Shredded => "Sensitive(<shredded>)",
        })
    }
}

impl fmt::Debug for Sealed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sealed")
            .field("algorithm", &self.algorithm)
            .field("subject", &self.subject)
            .finish_non_exhaustive()
    }
}
