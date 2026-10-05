//! A generic AEAD [`Cipher`] over the `aead` crate's traits.
//!
//! Both cipher adapters were the same code with a different algorithm
//! name, key size and nonce width; this is that code, written once. An
//! algorithm adapter crate (`eventyr-shred-aes-gcm`,
//! `eventyr-shred-chacha`) reduces to a newtype over [`AeadCipher`]
//! plus its constants. Keeping two crates over one shared
//! implementation is deliberate: the adapter's algorithm is a pin
//! decision (auditable through its `algorithm()` string), not a library
//! surface.

use core::marker::PhantomData;

use aead::common::KeySizeUser;
use aead::common::typenum::Unsigned;
use aead::{Aead, AeadCore, Generate, KeyInit, Nonce, Payload};

use crate::cipher::{Cipher, CipherError, SubjectKey};

/// An AEAD cipher over `A`'s algorithm: a random nonce per encryption,
/// prepended to the sealed bytes (`nonce ‖ ciphertext ‖ tag`).
///
/// The subject is bound as associated data (the caller passes it on
/// each call), so a sealed field moved to another subject fails
/// authentication. Key and nonce widths come from `A`'s associated
/// types, checked at compile time.
/// The fields are constants; `A` arrives only through `PhantomData`, so
/// the cipher is `Copy` whatever `A` is.
pub struct AeadCipher<A> {
    algorithm: &'static str,
    key_hint: &'static str,
    _cipher: PhantomData<A>,
}

impl<A> Clone for AeadCipher<A> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<A> Copy for AeadCipher<A> {}

impl<A> core::fmt::Debug for AeadCipher<A> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AeadCipher")
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}

impl<A> Default for AeadCipher<A> {
    fn default() -> Self {
        Self::new("aead", "key-sized")
    }
}

impl<A> AeadCipher<A> {
    /// A cipher named `algorithm`, with `key_hint` naming the key
    /// length in error messages ("AES-256", "256-bit").
    pub const fn new(algorithm: &'static str, key_hint: &'static str) -> Self {
        Self {
            algorithm,
            key_hint,
            _cipher: PhantomData,
        }
    }
}

impl<A> AeadCipher<A>
where
    A: KeyInit,
{
    /// The key was not made for `A`: name the algorithm and both widths.
    fn bad_key(&self, key: &SubjectKey) -> CipherError {
        CipherError(format!(
            "{}: an {} key fits it, not {} bytes",
            self.algorithm,
            self.key_hint,
            key.as_bytes().len()
        ))
    }

    fn engine(&self, key: &SubjectKey) -> Result<A, CipherError> {
        A::new_from_slice(key.as_bytes()).map_err(|_| self.bad_key(key))
    }
}

impl<A> Cipher for AeadCipher<A>
where
    A: KeyInit + Aead + Send + Sync,
    A: KeySizeUser,
{
    fn algorithm(&self) -> &'static str {
        self.algorithm
    }

    fn generate_key(&self) -> Result<SubjectKey, CipherError> {
        let key = aead::Key::<A>::try_generate()
            .map_err(|e| CipherError(format!("random source: {e}")))?;
        // One copy: out of the `GenericArray`, into the `Vec` the
        // caller's `SubjectKey` owns — nothing else is left over.
        // (`SubjectKey` itself wipes on drop under `zeroize`.)
        Ok(SubjectKey::from_bytes(key.as_slice().to_vec()))
    }

    fn encrypt(
        &self,
        key: &SubjectKey,
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, CipherError> {
        let engine = self.engine(key)?;
        let nonce =
            Nonce::<A>::try_generate().map_err(|e| CipherError(format!("random source: {e}")))?;
        let sealed = engine
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|_| CipherError("encryption failed".into()))?;
        let mut out = Vec::with_capacity(nonce.len() + sealed.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&sealed);
        Ok(out)
    }

    fn decrypt(
        &self,
        key: &SubjectKey,
        ciphertext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, CipherError> {
        let (nonce, sealed) = ciphertext
            .split_at_checked(<<A as AeadCore>::NonceSize as Unsigned>::USIZE)
            .ok_or_else(|| CipherError("ciphertext shorter than its nonce".into()))?;
        let nonce = Nonce::<A>::try_from(nonce).map_err(|_| CipherError("bad nonce".into()))?;
        let engine = self.engine(key)?;
        engine
            .decrypt(&nonce, Payload { msg: sealed, aad })
            .map_err(|_| CipherError("authentication failed".into()))
    }
}
