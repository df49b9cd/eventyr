//! # eventyr-shred-aes-gcm
//!
//! AES-256-GCM for [`eventyr_shred`]: 256-bit subject keys, a random
//! 96-bit nonce per field, and the subject bound as associated data.
//!
//! A sealed field stores `nonce ‖ ciphertext ‖ tag`. Random 96-bit
//! nonces are safe for up to 2³² encryptions under one key (NIST SP
//! 800-38D) — one key per data subject keeps every key far below that.
//! For volumes where that bound could matter, use
//! `eventyr-shred-chacha`, whose 192-bit nonces have no practical limit.
//!
//! ```
//! use eventyr_shred::Cipher;
//! use eventyr_shred_aes_gcm::Aes256GcmCipher;
//!
//! let cipher = Aes256GcmCipher;
//! let key = cipher.generate_key()?;
//! let sealed = cipher.encrypt(&key, b"ada@example.com", b"customer-1")?;
//! assert_eq!(cipher.decrypt(&key, &sealed, b"customer-1")?, b"ada@example.com");
//! # Ok::<(), eventyr_shred::CipherError>(())
//! ```

use aes_gcm::aead::{Aead, Generate, Key, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use eventyr_shred::{Cipher, CipherError, SubjectKey};

/// The AES-256-GCM [`Cipher`].
#[derive(Clone, Copy, Debug, Default)]
pub struct Aes256GcmCipher;

/// Nonce length in bytes.
const NONCE: usize = 12;

fn engine(key: &SubjectKey) -> Result<Aes256Gcm, CipherError> {
    Aes256Gcm::new_from_slice(key.as_bytes()).map_err(|_| {
        CipherError(format!(
            "an AES-256 key is 32 bytes, not {}",
            key.as_bytes().len()
        ))
    })
}

impl Cipher for Aes256GcmCipher {
    fn algorithm(&self) -> &'static str {
        "aes-256-gcm"
    }

    fn generate_key(&self) -> Result<SubjectKey, CipherError> {
        let key = Key::<Aes256Gcm>::try_generate()
            .map_err(|e| CipherError(format!("random source: {e}")))?;
        Ok(SubjectKey::from_bytes(key.to_vec()))
    }

    fn encrypt(
        &self,
        key: &SubjectKey,
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, CipherError> {
        let engine = engine(key)?;
        let nonce =
            Nonce::try_generate().map_err(|e| CipherError(format!("random source: {e}")))?;
        let sealed = engine
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|_| CipherError("encryption failed".into()))?;
        let mut out = Vec::with_capacity(NONCE + sealed.len());
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
        let engine = engine(key)?;
        if ciphertext.len() < NONCE {
            return Err(CipherError("ciphertext shorter than its nonce".into()));
        }
        let (nonce, sealed) = ciphertext.split_at(NONCE);
        let nonce = Nonce::try_from(nonce).map_err(|_| CipherError("bad nonce".into()))?;
        engine
            .decrypt(&nonce, Payload { msg: sealed, aad })
            .map_err(|_| CipherError("authentication failed".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::Aes256GcmCipher;

    #[test]
    fn passes_the_cipher_contract() {
        eventyr_shred::cipher_contract(&Aes256GcmCipher);
    }
}
