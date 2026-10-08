//! The parked-store wrapper (roadmap 0.7.7): seal on park, open on list.
//!
//! A parked event is kept *durable and reviewable* so a fixed projection
//! can replay it — which is exactly why a park must not hold personal
//! data in the clear: [`ParkedStore::park`] receives the event as the
//! projection saw it, already opened, so a plain [`Sensitive`] value
//! would sit next to the event's rejection until someone replays or
//! removes it. This wrapper seals the envelope on the way in and opens
//! it on the way out, mirroring [`ShreddingStore`](crate::ShreddingStore)
//! for the log: erased subjects read back
//! [`Shredded`](crate::Sensitive::Shredded) from a parked record, the
//! same as everywhere else.
//!
//! The record's [`error`](ParkedEvent::error) — what the projection
//! said when it rejected the event — is free text, and may itself quote
//! personal data. The wrapper leaves it alone: it is the operator's
//! diagnostic. Mind it when clearing a subject's traces.

use std::sync::Arc;

use eventyr_core::envelope::EventEnvelope;
use eventyr_core::error::StoreError;
use eventyr_core::vocabulary::Sequence;
use eventyr_subscription::parked::{ParkedEvent, ParkedStore};

use crate::cipher::Cipher;
use crate::keys::KeyStore;
use crate::shredder::Shredder;

/// Any [`ParkedStore`], with every [`Sensitive`](crate::Sensitive)
/// field of the parked envelope sealed before it is recorded and opened
/// after it is listed — the parked counterpart of
/// [`ShreddingStore`](crate::ShreddingStore).
///
/// Removing a record passes straight through, and an erased subject's
/// fields list back [`Shredded`](crate::Sensitive::Shredded): the
/// record survives erasure as a pointer, never as the data. Wrap the
/// store the projector parks into so rejects never reach it in the
/// clear.
pub struct ShreddingParkedStore<P, C, K> {
    inner: P,
    shredder: Arc<Shredder<C, K>>,
}

impl<P: Clone, C, K> Clone for ShreddingParkedStore<P, C, K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            shredder: Arc::clone(&self.shredder),
        }
    }
}

impl<P, C: Cipher, K: KeyStore> ShreddingParkedStore<P, C, K> {
    /// Wrap `inner`, sealing and opening through `shredder`. Share the
    /// shredder of the [`ShreddingStore`](crate::ShreddingStore) the
    /// events pass through, so a subject erased there is erased here.
    pub fn new(inner: P, shredder: Arc<Shredder<C, K>>) -> Self {
        Self { inner, shredder }
    }

    /// The wrapped store. Reading from it directly yields sealed fields.
    pub fn inner(&self) -> &P {
        &self.inner
    }

    /// The shredder, for [`erase`](Shredder::erase).
    pub fn shredder(&self) -> &Shredder<C, K> {
        &self.shredder
    }
}

/// `record` with its envelope's event sealed (like
/// [`ShreddingStore`](crate::ShreddingStore), one event instead of a
/// batch): metadata, attempts and the rejection text pass through.
async fn seal_record<E, C, K>(
    shredder: &Shredder<C, K>,
    record: ParkedEvent<E>,
) -> Result<ParkedEvent<E>, StoreError>
where
    E: serde::Serialize + serde::de::DeserializeOwned + Sync,
    C: Cipher,
    K: KeyStore,
{
    let ParkedEvent {
        subscription,
        envelope,
        attempts,
        error,
    } = record;
    let envelope = seal_envelope(shredder, envelope).await?;
    Ok(ParkedEvent {
        subscription,
        envelope,
        attempts,
        error,
    })
}

async fn seal_envelope<E, C, K>(
    shredder: &Shredder<C, K>,
    envelope: EventEnvelope<E>,
) -> Result<EventEnvelope<E>, StoreError>
where
    E: serde::Serialize + serde::de::DeserializeOwned + Sync,
    C: Cipher,
    K: KeyStore,
{
    let EventEnvelope {
        sequence,
        stream_id,
        version,
        event,
        metadata,
    } = envelope;
    Ok(EventEnvelope {
        sequence,
        stream_id,
        version,
        event: shredder.seal(event).await?,
        metadata,
    })
}

async fn open_record<E, C, K>(
    shredder: &Shredder<C, K>,
    record: ParkedEvent<E>,
) -> Result<ParkedEvent<E>, StoreError>
where
    E: serde::Serialize + serde::de::DeserializeOwned,
    C: Cipher,
    K: KeyStore,
{
    let ParkedEvent {
        subscription,
        envelope,
        attempts,
        error,
    } = record;
    let EventEnvelope {
        sequence,
        stream_id,
        version,
        event,
        metadata,
    } = envelope;
    Ok(ParkedEvent {
        subscription,
        envelope: EventEnvelope {
            sequence,
            stream_id,
            version,
            event: shredder.open(event).await?,
            metadata,
        },
        attempts,
        error,
    })
}

impl<P, C, K, E> ParkedStore<E> for ShreddingParkedStore<P, C, K>
where
    P: ParkedStore<E>,
    E: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    C: Cipher + 'static,
    K: KeyStore + 'static,
{
    const RECORDS: bool = P::RECORDS;

    async fn park(&self, event: ParkedEvent<E>) -> Result<(), StoreError> {
        self.inner
            .park(seal_record(&self.shredder, event).await?)
            .await
    }

    async fn list(&self, subscription: &str) -> Result<Vec<ParkedEvent<E>>, StoreError> {
        let parked = self.inner.list(subscription).await?;
        let mut opened = Vec::with_capacity(parked.len());
        for record in parked {
            opened.push(open_record(&self.shredder, record).await?);
        }
        Ok(opened)
    }

    async fn remove(&self, subscription: &str, sequence: Sequence) -> Result<(), StoreError> {
        self.inner.remove(subscription, sequence).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use eventyr_core::envelope::{EventEnvelope, Metadata};
    use eventyr_core::vocabulary::{Sequence, StreamId, Version};
    use eventyr_subscription::parked::{InMemoryParkedStore, ParkedEvent, ParkedStore};
    use futures::executor::block_on;

    use crate::{
        Cipher, CipherError, InMemoryKeyStore, Sensitive, Shredder, ShreddingParkedStore,
        SubjectKey,
    };

    /// The same *not-encryption* toy the integration tests use (the
    /// shredder's guarantees tested here don't depend on the cipher),
    /// duplicated: cross-file test sharing is not worth a public seam.
    struct Toy;

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

    #[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    #[serde(transparent)]
    struct Event(Sensitive<u64>);

    fn shredder() -> Arc<Shredder<Toy, InMemoryKeyStore>> {
        Arc::new(Shredder::new(Toy, InMemoryKeyStore::new()))
    }

    fn record(subscription: &str, sequence: u64, subject: &str, email: u64) -> ParkedEvent<Event> {
        ParkedEvent {
            subscription: subscription.to_owned(),
            envelope: EventEnvelope {
                sequence: Sequence::new(sequence),
                stream_id: StreamId::from("c-1"),
                version: Version::new(1),
                event: Event(Sensitive::new(subject, email)),
                metadata: Metadata::default(),
            },
            attempts: 1,
            error: "no".into(),
        }
    }

    /// The reject reached the store sealed, and erasure shreds the
    /// parked copy like any other read.
    #[test]
    fn a_parked_event_is_stored_sealed_and_lists_back_shredded() {
        let shredder = shredder();
        let store = ShreddingParkedStore::new(InMemoryParkedStore::new(), shredder);
        block_on(store.park(record("s", 1, "c-1", 41))).expect("park");

        let raw = block_on(store.inner().list("s")).expect("list the inner store");
        let stored = serde_json::to_string(&raw[0].envelope.event).expect("json");
        assert!(stored.contains("\"$sensitive\":\"sealed\""), "{stored}");

        block_on(store.shredder().erase("c-1")).expect("erase");
        let listed = block_on(store.list("s")).expect("list");
        assert!(listed[0].envelope.event.0.is_shredded());
        assert_eq!(listed[0].envelope.sequence, Sequence::new(1));
        assert_eq!(listed[0].attempts, 1, "the record itself is untouched");
        assert_eq!(listed[0].error, "no");
    }

    /// A parked record lists back open, with its sensitive value intact.
    #[test]
    fn a_parked_event_lists_back_open() {
        let shredder = shredder();
        let store = ShreddingParkedStore::new(InMemoryParkedStore::new(), shredder);
        block_on(store.park(record("s", 1, "c-1", 41))).expect("park");
        let listed = block_on(store.list("s")).expect("list");
        assert_eq!(listed[0].envelope.event.0.get(), Some(&41));
    }

    /// The parked-store contract holds through the wrapper: sealing and
    /// opening do not change what the store promises. `u64` has no
    /// sensitive fields, so the wrapper is transparent on both paths.
    #[test]
    fn the_wrapper_passes_the_parked_store_contract() {
        let shredder = shredder();
        eventyr_subscription::parked::parked_store_contract(move || {
            super::ShreddingParkedStore::new(
                eventyr_subscription::parked::InMemoryParkedStore::new(),
                Arc::clone(&shredder),
            )
        });
    }
}
