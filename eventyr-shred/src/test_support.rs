//! A shared *not-encryption* cipher, for the tests that need a
//! [`Cipher`] without depending on a real one.
//!
//! The toy XORs with the key and appends a checksum of key, plaintext
//! and `aad` — it is a shape demo, never a security boundary, but it
//! has the properties the shredder's tests rely on: a wrong key or a
//! wrong `aad` fails to decrypt, and the stored bytes differ from the
//! plaintext. Both the in-crate suite and the integration tests use it
//! through this module, behind the `testing` feature.

use crate::cipher::{Cipher, CipherError, SubjectKey};

/// The toy cipher: XOR-encrypt with a generated key and append a
/// checksum of key, plaintext, and `aad` — *not encryption*, but
/// authenticated in the one way the tests need (wrong key or wrong
/// `aad` fails).
pub struct Toy;

/// The folding checksum the toy appends and verifies.
fn checksum(parts: &[&[u8]]) -> u8 {
    parts
        .iter()
        .flat_map(|p| p.iter())
        .fold(0u8, |acc, b| acc.wrapping_mul(31).wrapping_add(*b))
}

impl Cipher for Toy {
    fn algorithm(&self) -> &'static str {
        "toy"
    }

    fn generate_key(&self) -> Result<SubjectKey, CipherError> {
        use std::sync::atomic::{AtomicU8, Ordering};
        static NEXT: AtomicU8 = AtomicU8::new(1);
        Ok(SubjectKey::from_bytes(vec![
            NEXT.fetch_add(
                37,
                Ordering::Relaxed
            ) | 1;
            4
        ]))
    }

    fn encrypt(
        &self,
        key: &SubjectKey,
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, CipherError> {
        let k = key.as_bytes();
        let mut out: Vec<u8> = plaintext
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ k[i % k.len()])
            .collect();
        out.push(checksum(&[k, plaintext, aad]));
        Ok(out)
    }

    fn decrypt(
        &self,
        key: &SubjectKey,
        ciphertext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, CipherError> {
        let (body, tag) = ciphertext.split_at(ciphertext.len() - 1);
        let k = key.as_bytes();
        let plain: Vec<u8> = body
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ k[i % k.len()])
            .collect();
        if checksum(&[k, &plain, aad]) != tag[0] {
            return Err(CipherError("authentication failed".into()));
        }
        Ok(plain)
    }
}
