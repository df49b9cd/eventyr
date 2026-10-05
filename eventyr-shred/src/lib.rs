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
//! Erasure covers the event log. Anything that copied personal data out
//! of it — a snapshot of folded state, a view row, a log line — holds it
//! in the clear and must be cleared too: delete the subject's snapshots
//! and rebuild or delete their view rows after [`Shredder::erase`].

mod cipher;
mod contract;
mod keys;
mod sensitive;
mod shredder;
mod store;

pub use cipher::{Cipher, CipherError, SubjectKey};
pub use contract::{cipher_contract, key_store_contract};
pub use keys::{InMemoryKeyStore, KeyStore};
pub use sensitive::{Sealed, Sensitive};
pub use shredder::{ShredError, Shredder};
pub use store::ShreddingStore;
