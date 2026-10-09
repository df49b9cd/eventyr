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
///
/// # Examples
///
/// A stand-in cipher showing the `aad` binding the contract rests on:
/// the same bytes decrypt under the `aad` they were sealed with, and
/// fail under any other. The toy here is a shape demo, not encryption —
/// a real cipher must pass `cipher_contract`, and the shipped ones are
/// `eventyr-shred-aes-gcm` and `eventyr-shred-chacha`:
///
/// ```
/// # fn main() {
/// use eventyr_shred::{Cipher, CipherError, SubjectKey};
///
/// struct Toy;
/// impl Cipher for Toy {
///     fn algorithm(&self) -> &'static str { "toy" }
///     fn generate_key(&self) -> Result<SubjectKey, CipherError> {
///         Ok(SubjectKey::from_bytes(vec![7; 32]))
///     }
///     fn encrypt(&self, key: &SubjectKey, plaintext: &[u8], aad: &[u8])
///         -> Result<Vec<u8>, CipherError> {
///         // The whole `aad` folds into the mask, so two subjects'
///         // bindings genuinely differ.
///         let aad_mask = aad.iter().fold(0u8, |mask, byte| mask ^ byte);
///         let mut out = plaintext.to_vec();
///         for (i, byte) in out.iter_mut().enumerate() {
///             *byte ^= key.as_bytes()[i % 32] ^ aad_mask;
///         }
///         Ok(out)
///     }
///     fn decrypt(&self, key: &SubjectKey, ciphertext: &[u8], aad: &[u8])
///         -> Result<Vec<u8>, CipherError> {
///         self.encrypt(key, ciphertext, aad) // XOR is its own inverse
///     }
/// }
///
/// let cipher = Toy;
/// let key = cipher.generate_key().expect("generate");
/// let subject = b"customer-1".as_slice();
///
/// // Roundtrip under the sealing subject.
/// let sealed = cipher.encrypt(&key, b"ada@example.com", subject)
///     .expect("encrypt");
/// assert_eq!(cipher.decrypt(&key, &sealed, subject).expect("decrypt"),
///            b"ada@example.com");
///
/// // The binding: the same ciphertext under another subject's `aad`
/// // opens to garbage, not the plaintext — a sealed field moved
/// // between subjects is unreadable, which is the point of the `aad`.
/// // (An authenticated cipher fails outright here; a shape demo can
/// // only show the `aad` participating in the mask.)
/// let moved = cipher.decrypt(&key, &sealed, b"customer-2".as_slice())
///     .expect("the toy never fails — it is not authenticated");
/// assert_ne!(&moved[..], b"ada@example.com".as_slice());
/// # }
/// ```
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
