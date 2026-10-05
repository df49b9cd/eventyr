//! # eventyr-shred-chacha
//!
//! XChaCha20-Poly1305 for [`eventyr_shred`]: 256-bit subject keys, a
//! random 192-bit nonce per field, and the subject bound as associated
//! data.
//!
//! A sealed field stores `nonce ‖ ciphertext ‖ tag`. The 192-bit nonce
//! makes random nonces safe at any realistic volume, and the cipher is
//! fast without AES hardware.
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

use chacha20poly1305::aead::{Aead, Generate, Key, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce as Nonce};
use eventyr_shred::{Cipher, CipherError, SubjectKey};

/// The XChaCha20-Poly1305 [`Cipher`].
#[derive(Clone, Copy, Debug, Default)]
pub struct XChaCha20Poly1305Cipher;

/// Nonce length in bytes.
const NONCE: usize = 24;

fn engine(key: &SubjectKey) -> Result<XChaCha20Poly1305, CipherError> {
    XChaCha20Poly1305::new_from_slice(key.as_bytes()).map_err(|_| {
        CipherError(format!(
            "an XChaCha20 key is 32 bytes, not {}",
            key.as_bytes().len()
        ))
    })
}

impl Cipher for XChaCha20Poly1305Cipher {
    fn algorithm(&self) -> &'static str {
        "xchacha20-poly1305"
    }

    fn generate_key(&self) -> Result<SubjectKey, CipherError> {
        let key = Key::<XChaCha20Poly1305>::try_generate()
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
    use super::XChaCha20Poly1305Cipher;

    #[test]
    fn passes_the_cipher_contract() {
        eventyr_shred::cipher_contract(&XChaCha20Poly1305Cipher);
    }
}
