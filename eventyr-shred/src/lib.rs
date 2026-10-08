//! # eventyr-shred
//!
//! Crypto-shredding (roadmap 0.7.6): personal data is stored encrypted
//! under a key per *data subject*, and erasing the subject deletes the
//! key. The events stay; the fields they carried become unreadable, and
//! read back as a replacement value the event declares. History is never
//! rewritten, every projection rebuild still works, and an erased field
//! cannot be recovered from any copy of the log.
//!
//! Encryption is field-level, in the domain event. A personal field is
//! a [`Sensitive<T>`] naming the data subject it belongs to. No trait to
//! implement: any serde event works, and the stores keep storing JSON.
//!
//! ```text
//! #[derive(Serialize, Deserialize)]
//! enum CustomerEvent {
//!     Registered { customer: String, email: Sensitive<String> },
//! }
//! // CustomerEvent::Registered {
//! //     customer: id.clone(),
//! //     email: Sensitive::new(id, "ada@example.com".into()),
//! // }
//! ```
//!
//! The pieces:
//!
//! - [`Cipher`]: the bring-your-own encryption seam. This crate ships no
//!   cipher; `eventyr-shred-aes-gcm` and `eventyr-shred-chacha` are the
//!   adapters, and anything else (a KMS, an HSM) implements the same
//!   four methods.
//! - [`KeyStore`]: where subject keys live, *outside* the event log.
//!   [`InMemoryKeyStore`] here.
//! - [`Shredder`]: a cipher and a key store. [`seal`](Shredder::seal)
//!   encrypts every [`Sensitive`] field of an event, [`open`](Shredder::open)
//!   decrypts them or marks them shredded when the key is gone, and
//!   [`erase`](Shredder::erase) deletes a subject's key.
//! - [`ShreddingStore`]: wraps any store so appends are sealed and reads
//!   are opened. Domain code, the write machine, and projections all see
//!   plain or shredded fields; only sealed ones reach the log.
//!
//! An erased field reads back as [`Sensitive::Shredded`]. Domain code
//! reads fields with [`Sensitive::get`] or [`Sensitive::or`], so erasure
//! is a value it handles, never an error that stops a rebuild.
//!
//! Two features shape the crate: `zeroize` wipes key bytes on drop (the
//! cipher adapters enable it), and `aead` shares the adapters' cipher
//! implementation ([`aead::AeadCipher`]); `testing` exports the
//! contracts for adapter and key store tests.
//!
//! ## Cargo features
//!
//! - `zeroize` — zeroize key bytes on drop (`SubjectKey` and the
//!   per-call caches); recipients (a key store on disk, a key
//!   management service) may keep their own copies regardless.
//! - `aead` — a generic AEAD `Cipher` over the `aead` traits: an
//!   algorithm adapter crate (eventyr-shred-aes-gcm / -chacha) is one
//!   newtype over it.
//! - `parked` — `ShreddingParkedStore`: a `ParkedStore` wrapper that
//!   seals an event's `Sensitive` fields before they are parked
//!   (roadmap 0.7.7).
//! - `testing` — exports `cipher_contract` and `key_store_contract`,
//!   for adapter-crate and key store tests; not for production builds.
//!
//! Erasure covers the event log. Anything that copied personal data out
//! of it — a snapshot of folded state, a view row, a *parked* event
//! (roadmap 0.7.7: the poison events a projection gave up on, envelope kept for
//! replay), a log line — holds it in the clear and must be cleared too.
//! Wrap the parked store in
//! [`ShreddingParkedStore`] (the `parked`
//! feature) so rejects reach it sealed, and note that a parked record's
//! *rejection text*
//! ([`ParkedEvent::error`](eventyr_subscription::parked::ParkedEvent::error),
//! what the projection said about the event) may itself quote personal
//! data. After [`Shredder::erase`], delete the subject's snapshots,
//! rebuild or delete their view rows, and mind those rejection logs.

#![cfg_attr(docsrs, feature(doc_cfg))]
#[cfg(feature = "aead")]
pub mod aead;
mod cipher;
#[cfg(any(test, feature = "testing"))]
mod contract;
mod keys;
#[cfg(feature = "parked")]
mod parked;
mod sensitive;
mod shredder;
mod store;

pub use cipher::{Cipher, CipherError, SubjectKey};
#[cfg(any(test, feature = "testing"))]
pub use contract::{cipher_contract, key_store_contract};
pub use keys::{InMemoryKeyStore, KeyStore};
#[cfg(feature = "parked")]
pub use parked::ShreddingParkedStore;
pub use sensitive::{Sealed, Sensitive};
pub use shredder::{ShredError, Shredder};
pub use store::ShreddingStore;
