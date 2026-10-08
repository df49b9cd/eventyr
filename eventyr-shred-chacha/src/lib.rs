//! # eventyr-shred-chacha
//!
//! XChaCha20-Poly1305 for [`eventyr_shred`]: 256-bit subject keys, a
//! random 192-bit nonce per field, and the subject bound as associated
//! data — [`AeadCipher`] pinned to
//! this algorithm.
//!
//! A sealed field stores `nonce ‖ ciphertext ‖ tag`. The 192-bit nonce
//! makes random nonces safe at any realistic volume, and the cipher is
//! fast without AES hardware.
//!
//! There is no algorithm-generic API here on purpose: `eventyr-shred`'s
//! `aead` feature owns the shared code, and this crate is the auditable
//! pin.
//!
//! ```
//! use eventyr_shred::Cipher;
//! use eventyr_shred_chacha::XChaCha20Poly1305Cipher;
//!
//! let cipher = XChaCha20Poly1305Cipher;
//! let key = cipher.generate_key()?;
//! let sealed = cipher.encrypt(&key, b"ada@example.com", b"customer-1")?;
//! assert_eq!(cipher.decrypt(&key, &sealed, b"customer-1")?, b"ada@example.com");
//! # Ok::<(), eventyr_shred::CipherError>(())
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
use chacha20poly1305::XChaCha20Poly1305;
use eventyr_shred::aead::AeadCipher;
use eventyr_shred::{Cipher, CipherError, SubjectKey};

/// The XChaCha20-Poly1305 [`Cipher`].
#[derive(Clone, Copy, Debug, Default)]
pub struct XChaCha20Poly1305Cipher;

/// The shared implementation: both algorithm adapters are this const,
/// over their algorithm.
const XCHACHA20_POLY1305: AeadCipher<XChaCha20Poly1305> =
    AeadCipher::new("xchacha20-poly1305", "256-bit");

impl Cipher for XChaCha20Poly1305Cipher {
    fn algorithm(&self) -> &'static str {
        XCHACHA20_POLY1305.algorithm()
    }

    fn generate_key(&self) -> Result<SubjectKey, CipherError> {
        XCHACHA20_POLY1305.generate_key()
    }

    fn encrypt(
        &self,
        key: &SubjectKey,
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, CipherError> {
        XCHACHA20_POLY1305.encrypt(key, plaintext, aad)
    }

    fn decrypt(
        &self,
        key: &SubjectKey,
        ciphertext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, CipherError> {
        XCHACHA20_POLY1305.decrypt(key, ciphertext, aad)
    }
}

#[cfg(test)]
mod tests {
    use super::XChaCha20Poly1305Cipher;

    #[test]
    fn passes_the_cipher_contract() {
        eventyr_shred::cipher_contract(&XChaCha20Poly1305Cipher);
    }
}
