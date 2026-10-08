//! # eventyr-shred-aes-gcm
//!
//! AES-256-GCM for [`eventyr_shred`]: 256-bit subject keys, a random
//! 96-bit nonce per field, and the subject bound as associated data —
//! [`AeadCipher`] pinned to this
//! algorithm.
//!
//! A sealed field stores `nonce ‖ ciphertext ‖ tag`. Random 96-bit
//! nonces are safe for up to 2³² encryptions under one key (NIST SP
//! 800-38D) — one key per data subject keeps every key far below that.
//! For volumes where that bound could matter, use
//! `eventyr-shred-chacha`, whose 192-bit nonces have no practical limit.
//!
//! There is no algorithm-generic API here on purpose: `eventyr-shred`'s
//! `aead` feature owns the shared code, and this crate is the auditable
//! pin.
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

#![cfg_attr(docsrs, feature(doc_cfg))]
use aes_gcm::Aes256Gcm;
use eventyr_shred::aead::AeadCipher;
use eventyr_shred::{Cipher, CipherError, SubjectKey};

/// The AES-256-GCM [`Cipher`].
#[derive(Clone, Copy, Debug, Default)]
pub struct Aes256GcmCipher;

/// The shared implementation: both algorithm adapters are this const,
/// over their algorithm.
const AES_256_GCM: AeadCipher<Aes256Gcm> = AeadCipher::new("aes-256-gcm", "AES-256");

impl Cipher for Aes256GcmCipher {
    fn algorithm(&self) -> &'static str {
        AES_256_GCM.algorithm()
    }

    fn generate_key(&self) -> Result<SubjectKey, CipherError> {
        AES_256_GCM.generate_key()
    }

    fn encrypt(
        &self,
        key: &SubjectKey,
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, CipherError> {
        AES_256_GCM.encrypt(key, plaintext, aad)
    }

    fn decrypt(
        &self,
        key: &SubjectKey,
        ciphertext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, CipherError> {
        AES_256_GCM.decrypt(key, ciphertext, aad)
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
