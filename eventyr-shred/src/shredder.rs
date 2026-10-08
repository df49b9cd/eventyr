//! Sealing and opening personal fields.
//!
//! The shredder works on an event's JSON form, which is what makes it
//! need nothing from the event type: it walks the value for objects
//! tagged `"$sensitive"` and replaces them. A `Plain` field becomes
//! `Sealed` on the way in; a `Sealed` field becomes `Plain` — or
//! `Shredded`, when its subject's key is gone — on the way out.

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use eventyr_core::error::StoreError;
use serde_json::{Map, Value};

use crate::cipher::{Cipher, CipherError, SubjectKey};
use crate::keys::KeyStore;

/// The serde tag [`Sensitive`](crate::Sensitive) serializes under.
const TAG: &str = "$sensitive";

/// Sealing or opening failed.
#[derive(Debug)]
pub enum ShredError {
    /// The serde leaf of an error: the category and the position, never
    /// the message — serde echoes the offending input in it, and the
    /// input here is being decrypted: leaving it in the error would put
    /// the personal data into logs and `StoreError::Other`.
    Json(JsonKind),
    /// The cipher failed. On open this is a sealed field that failed
    /// authentication under a key that *does* exist — tampering or
    /// corruption, never erasure.
    Cipher(CipherError),
    /// A sealed field names a cipher other than this shredder's.
    WrongAlgorithm {
        /// The algorithm the field was sealed with.
        sealed_with: String,
        /// This shredder's.
        expected: &'static str,
    },
    /// A `Plain` field's subject was already erased: data for an erased
    /// subject must not enter the log again.
    SubjectErased(String),
    /// A field tagged `$sensitive` that is not a valid `Sensitive`.
    Malformed(String),
    /// The key store failed.
    Keys(StoreError),
}

/// The serde leaf of a [`ShredError`]: what failed and where, without
/// the field's content.
#[derive(Debug)]
pub struct JsonKind {
    category: serde_json::error::Category,
    line: usize,
    column: usize,
}

/// `result` or a redacted [`JsonKind`]: serde's messages can quote the
/// input, and the input is personal data on its way through the pipe.
fn redacted<T>(result: Result<T, serde_json::Error>) -> Result<T, ShredError> {
    result.map_err(|error| {
        ShredError::Json(JsonKind {
            category: error.classify(),
            line: error.line(),
            column: error.column(),
        })
    })
}

impl core::fmt::Display for ShredError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Json(kind) => write!(
                f,
                "shredding: a value failed to serialize at line {} column {} ({:?})",
                kind.line, kind.column, kind.category
            ),
            Self::Cipher(e) => write!(f, "shredding: {e}"),
            Self::WrongAlgorithm {
                sealed_with,
                expected,
            } => write!(
                f,
                "shredding: a field sealed with `{sealed_with}` cannot be opened with `{expected}`"
            ),
            Self::SubjectErased(subject) => {
                write!(f, "shredding: subject `{subject}` was erased")
            }
            Self::Malformed(message) => write!(f, "shredding: malformed field: {message}"),
            Self::Keys(e) => write!(f, "shredding: key store: {e}"),
        }
    }
}

impl core::error::Error for ShredError {}

impl From<ShredError> for StoreError {
    fn from(error: ShredError) -> Self {
        match error {
            // The key store's own failures keep their kind (a transient
            // outage stays retryable).
            ShredError::Keys(error) => error,
            other => StoreError::Other(Arc::new(other)),
        }
    }
}

/// A cipher and a key store: seals and opens the [`Sensitive`](crate::Sensitive)
/// fields of events, and erases subjects.
///
/// # Examples
///
/// Seal an event, open it back, then erase the subject — the sealed
/// field survives in the log but reads back shredded. A stand-in toy
/// cipher (XOR with the key, not real encryption) keeps the example
/// dependency-free; the real ciphers are `eventyr-shred-aes-gcm` and
/// `eventyr-shred-chacha`:
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use eventyr_shred::{Cipher, CipherError, InMemoryKeyStore, Sensitive, Shredder, SubjectKey};
/// use futures::executor::block_on;
/// use serde::{Deserialize, Serialize};
///
/// // A toy cipher: XOR with the key — not encryption, but the
/// // shredder only needs the Cipher contract.
/// struct Toy;
/// impl Cipher for Toy {
///     fn algorithm(&self) -> &'static str { "toy" }
///     fn generate_key(&self) -> Result<SubjectKey, CipherError> {
///         Ok(SubjectKey::from_bytes(vec![7; 32]))
///     }
///     fn encrypt(&self, key: &SubjectKey, plaintext: &[u8], aad: &[u8])
///         -> Result<Vec<u8>, CipherError> {
///         let mut out = plaintext.to_vec();
///         for (i, byte) in out.iter_mut().enumerate() {
///             *byte ^= key.as_bytes()[i % 32] ^ aad.first().copied().unwrap_or(0);
///         }
///         Ok(out)
///     }
///     fn decrypt(&self, key: &SubjectKey, ciphertext: &[u8], aad: &[u8])
///         -> Result<Vec<u8>, CipherError> {
///         self.encrypt(key, ciphertext, aad) // XOR is its own inverse
///     }
/// }
///
/// #[derive(Clone, Serialize, Deserialize)]
/// enum CustomerEvent {
///     Registered { email: Sensitive<String> },
/// }
///
/// let shredder = Shredder::new(Toy, InMemoryKeyStore::new());
/// let event = CustomerEvent::Registered {
///     email: Sensitive::new("customer-1", "ada@example.com".to_owned()),
/// };
///
/// // Seal: the email is encrypted before it reaches the log. The
/// // sealed form is what the store keeps — clone it as the log's copy.
/// let sealed = block_on(shredder.seal(event))?;
/// assert!(matches!(sealed, CustomerEvent::Registered { email: Sensitive::Sealed(_) }));
/// let stored = sealed.clone();
///
/// // Open: with the key, the email comes back.
/// let opened = block_on(shredder.open(sealed))?;
/// let CustomerEvent::Registered { email } = &opened;
/// assert_eq!(email.get().map(String::as_str), Some("ada@example.com"));
///
/// // Erase the subject, then read the log's copy again: the key is
/// // gone, so the field reads back shredded — the ciphertext stays in
/// // the log, unreadable.
/// block_on(shredder.erase("customer-1"))?;
/// let shredded = block_on(shredder.open(stored))?;
/// let CustomerEvent::Registered { email } = shredded;
/// assert!(email.is_shredded());
/// # Ok(())
/// # }
/// ```
pub struct Shredder<C, K> {
    cipher: C,
    keys: K,
}

impl<C: Cipher, K: KeyStore> Shredder<C, K> {
    /// A shredder over `cipher` and `keys`.
    pub fn new(cipher: C, keys: K) -> Self {
        Self { cipher, keys }
    }

    /// Erase `subject`: delete its key. Every field sealed under it
    /// reads back as [`Shredded`](crate::Sensitive::Shredded) from now on,
    /// and no new data for it can be sealed.
    ///
    /// # Errors
    ///
    /// The key store failed — the deletion itself; erasure is
    /// idempotent, so an already-erased subject is a success, never an
    /// error.
    pub async fn erase(&self, subject: &str) -> Result<(), StoreError> {
        self.keys.delete(subject).await
    }

    /// Encrypt every `Plain` field of `event`, creating a subject's key
    /// on first use. Already-sealed fields pass through. An event with
    /// no sensitive fields passes through untouched — no round trip.
    ///
    /// # Errors
    ///
    /// [`ShredError::Keys`] when the key store failed (an erased
    /// subject's key cannot come back: [`ShredError::SubjectErased`]);
    /// [`ShredError::Cipher`] when the cipher rejected the key or the
    /// encryption failed; a redacted [`ShredError::Json`] when the
    /// event's own serde round trip failed.
    pub async fn seal<E>(&self, event: E) -> Result<E, ShredError>
    where
        E: serde::Serialize + serde::de::DeserializeOwned,
    {
        let mut value = redacted(serde_json::to_value(&event))?;
        let mut fields = Vec::new();
        collect(&mut value, &mut fields);
        if fields.is_empty() {
            return Ok(event);
        }
        let mut keys: HashMap<String, SubjectKey> = HashMap::new();
        for field in fields {
            let Some(Value::String(state)) = field.get(TAG).cloned() else {
                return Err(ShredError::Malformed("no `$sensitive` state".into()));
            };
            if state != "plain" {
                continue;
            }
            let subject = string_at(field, "subject")?;
            let plain = field
                .get("value")
                .ok_or_else(|| ShredError::Malformed("a plain field without a value".into()))?;
            let plaintext = redacted(serde_json::to_vec(plain))?;
            let key = match keys.get(&subject) {
                Some(key) => key.clone(),
                None => {
                    let key = self.key_for_sealing(&subject).await?;
                    keys.insert(subject.clone(), key.clone());
                    key
                }
            };
            let ciphertext = self
                .cipher
                .encrypt(&key, &plaintext, subject.as_bytes())
                .map_err(ShredError::Cipher)?;
            *field = sealed(self.cipher.algorithm(), &subject, &ciphertext);
        }
        redacted(serde_json::from_value(value))
    }

    /// Decrypt every `Sealed` field of `event`. A field whose subject's
    /// key is gone becomes `Shredded`; a field that fails to decrypt
    /// under a key that exists is an error.
    ///
    /// # Errors
    ///
    /// [`ShredError::WrongAlgorithm`] when a sealed field names a
    /// cipher other than this shredder's; [`ShredError::Cipher`] when a
    /// field fails authentication under a key that exists — tampering
    /// or corruption, never erasure; [`ShredError::Keys`] when the key
    /// store failed; a redacted [`ShredError::Json`] when the event's
    /// own serde round trip failed.
    pub async fn open<E>(&self, event: E) -> Result<E, ShredError>
    where
        E: serde::Serialize + serde::de::DeserializeOwned,
    {
        let mut value = redacted(serde_json::to_value(&event))?;
        let mut fields = Vec::new();
        collect(&mut value, &mut fields);
        if fields.is_empty() {
            return Ok(event);
        }
        let mut keys: HashMap<String, Option<SubjectKey>> = HashMap::new();
        for field in fields {
            let Some(Value::String(state)) = field.get(TAG).cloned() else {
                return Err(ShredError::Malformed("no `$sensitive` state".into()));
            };
            if state != "sealed" {
                continue;
            }
            let algorithm = string_at(field, "algorithm")?;
            if algorithm != self.cipher.algorithm() {
                return Err(ShredError::WrongAlgorithm {
                    sealed_with: algorithm,
                    expected: self.cipher.algorithm(),
                });
            }
            let subject = string_at(field, "subject")?;
            let ciphertext = BASE64
                .decode(string_at(field, "ciphertext")?)
                .map_err(|e| ShredError::Malformed(format!("ciphertext is not base64: {e}")))?;
            let key = match keys.get(&subject) {
                Some(key) => key.clone(),
                None => {
                    let key = self.keys.load(&subject).await.map_err(ShredError::Keys)?;
                    keys.insert(subject.clone(), key.clone());
                    key
                }
            };
            *field = match key {
                None => shredded(),
                Some(key) => {
                    let plaintext = self
                        .cipher
                        .decrypt(&key, &ciphertext, subject.as_bytes())
                        .map_err(ShredError::Cipher)?;
                    let value: Value = redacted(serde_json::from_slice(&plaintext))?;
                    plain(&subject, value)
                }
            };
        }
        redacted(serde_json::from_value(value))
    }

    async fn key_for_sealing(&self, subject: &str) -> Result<SubjectKey, ShredError> {
        if let Some(key) = self.keys.load(subject).await.map_err(ShredError::Keys)? {
            return Ok(key);
        }
        let fresh = self.cipher.generate_key().map_err(ShredError::Cipher)?;
        self.keys
            .create(subject, fresh)
            .await
            .map_err(ShredError::Keys)?
            .ok_or_else(|| ShredError::SubjectErased(subject.to_owned()))
    }
}

/// Every `$sensitive` object in `value`, outermost first. A sensitive
/// field's own value is never searched: nesting is not supported, and a
/// value that happens to contain the tag is data, not a field.
fn collect<'a>(value: &'a mut Value, out: &mut Vec<&'a mut Value>) {
    match value {
        Value::Object(map) if map.contains_key(TAG) => out.push(value),
        Value::Object(map) => map.values_mut().for_each(|v| collect(v, out)),
        Value::Array(items) => items.iter_mut().for_each(|v| collect(v, out)),
        _ => {}
    }
}

fn string_at(field: &Value, key: &str) -> Result<String, ShredError> {
    match field.get(key) {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(ShredError::Malformed(format!("missing `{key}`"))),
    }
}

fn sealed(algorithm: &str, subject: &str, ciphertext: &[u8]) -> Value {
    let mut map = Map::new();
    map.insert(TAG.into(), Value::from("sealed"));
    map.insert("algorithm".into(), Value::from(algorithm));
    map.insert("subject".into(), Value::from(subject));
    map.insert("ciphertext".into(), Value::from(BASE64.encode(ciphertext)));
    Value::Object(map)
}

fn plain(subject: &str, value: Value) -> Value {
    let mut map = Map::new();
    map.insert(TAG.into(), Value::from("plain"));
    map.insert("subject".into(), Value::from(subject));
    map.insert("value".into(), value);
    Value::Object(map)
}

fn shredded() -> Value {
    let mut map = Map::new();
    map.insert(TAG.into(), Value::from("shredded"));
    Value::Object(map)
}
