//! Where subject keys live.

use std::collections::HashMap;
use std::sync::Mutex;

use core::future::Future;

use eventyr_core::error::StoreError;

use crate::cipher::SubjectKey;

/// Subject keys, kept outside the event log.
///
/// The one property everything rests on: after [`delete`](Self::delete)
/// returns, the key is gone from this store for good. A backup or a
/// replica that still holds it still holds the personal data, so
/// erasure is only as complete as the key store's own retention.
pub trait KeyStore: Send + Sync {
    /// The subject's key, or `None` when it has none — never created,
    /// or erased.
    fn load(
        &self,
        subject: &str,
    ) -> impl Future<Output = Result<Option<SubjectKey>, StoreError>> + Send;

    /// Store `key` for `subject` unless it already has one or was
    /// erased, and return the key the subject ends up with — the
    /// existing one when two writers race. `None` means the subject was
    /// erased and must not get a new key: an erased subject stays
    /// erased.
    fn create(
        &self,
        subject: &str,
        key: SubjectKey,
    ) -> impl Future<Output = Result<Option<SubjectKey>, StoreError>> + Send;

    /// Erase the subject's key, and remember that it was erased.
    /// Idempotent.
    fn delete(&self, subject: &str) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// Subject keys in process memory: tests and examples.
#[derive(Default)]
pub struct InMemoryKeyStore {
    // `None` marks an erased subject.
    keys: Mutex<HashMap<String, Option<SubjectKey>>>,
}

impl InMemoryKeyStore {
    /// An empty key store.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Option<SubjectKey>>> {
        self.keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl KeyStore for InMemoryKeyStore {
    async fn load(&self, subject: &str) -> Result<Option<SubjectKey>, StoreError> {
        Ok(self.lock().get(subject).cloned().flatten())
    }

    async fn create(
        &self,
        subject: &str,
        key: SubjectKey,
    ) -> Result<Option<SubjectKey>, StoreError> {
        Ok(self
            .lock()
            .entry(subject.to_owned())
            .or_insert(Some(key))
            .clone())
    }

    async fn delete(&self, subject: &str) -> Result<(), StoreError> {
        self.lock().insert(subject.to_owned(), None);
        Ok(())
    }
}

impl<K: KeyStore + ?Sized> KeyStore for std::sync::Arc<K> {
    fn load(
        &self,
        subject: &str,
    ) -> impl Future<Output = Result<Option<SubjectKey>, StoreError>> + Send {
        (**self).load(subject)
    }

    fn create(
        &self,
        subject: &str,
        key: SubjectKey,
    ) -> impl Future<Output = Result<Option<SubjectKey>, StoreError>> + Send {
        (**self).create(subject, key)
    }

    fn delete(&self, subject: &str) -> impl Future<Output = Result<(), StoreError>> + Send {
        (**self).delete(subject)
    }
}
