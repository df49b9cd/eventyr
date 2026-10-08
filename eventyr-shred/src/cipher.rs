//! The bring-your-own cipher seam.

use core::fmt;

/// A subject's secret key, as the cipher made it. Opaque bytes: the
/// cipher decides their length and meaning, the key store keeps them.
///
/// Its `Debug` output never shows the bytes, and under the `zeroize`
/// feature the bytes are wiped from memory when the key is dropped
/// (recipients of [`as_bytes`](Self::as_bytes) — a key store writing to
/// disk, say — hold their own copy regardless).
#[derive(Clone)]
#[cfg_attr(feature = "zeroize", derive(zeroize::ZeroizeOnDrop))]
pub struct SubjectKey(Vec<u8>);

impl SubjectKey {
    /// Wrap key bytes a cipher produced or a key store loaded.
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// The key bytes, for a key store to persist.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Raw key content comparison, for the contracts' "two generated
    /// keys differ" check. Deliberately not `PartialEq`: if a key must
    /// be constant-time compared, this is not the seam that does it.
    #[cfg(any(test, feature = "testing"))]
    pub fn same_bytes(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl fmt::Debug for SubjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SubjectKey({} bytes)", self.0.len())
    }
}

/// An authenticated cipher: what [`Shredder`](crate::Shredder) encrypts
/// personal fields with.
///
/// Implementations must be *authenticated* (AEAD): `decrypt` must fail
/// on any ciphertext or `aad` that was tampered with, never return
/// garbage. `aad` binds a ciphertext to the subject it belongs to, so a
/// sealed field copied into another subject's event fails to open.
///
/// `encrypt` must use a fresh nonce for every call and carry it inside
/// the ciphertext it returns; `decrypt` reads it back from there.
pub trait Cipher: Send + Sync {
    /// The algorithm's stable name, stored with every sealed field, so a
    /// log written under one cipher is never decrypted with another.
    fn algorithm(&self) -> &'static str;

    /// A new random key.
    ///
    /// # Errors
    ///
    /// [`CipherError`] when the algorithm rejects its key size or the
    /// random source would not produce bytes.
    fn generate_key(&self) -> Result<SubjectKey, CipherError>;

    /// Encrypt `plaintext` under `key`, authenticating `aad`.
    ///
    /// # Errors
    ///
    /// [`CipherError`] when `key` is malformed for this algorithm, or
    /// the encryption itself failed (a spent random source, an
    /// allocation failure inside the backend).
    fn encrypt(
        &self,
        key: &SubjectKey,
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, CipherError>;

    /// Decrypt `ciphertext` under `key`, checking `aad`. Fails on any
    /// tampering.
    ///
    /// # Errors
    ///
    /// [`CipherError`] when `key` is malformed, or the ciphertext
    /// failed authentication under it — tampered bytes, or an `aad`
    /// other than the one it was sealed with.
    fn decrypt(
        &self,
        key: &SubjectKey,
        ciphertext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, CipherError>;
}

/// A cipher operation failed: a malformed key, a ciphertext that failed
/// authentication, or a random source that would not produce bytes.
#[derive(Debug)]
pub struct CipherError(pub String);

impl fmt::Display for CipherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cipher: {}", self.0)
    }
}

impl core::error::Error for CipherError {}
