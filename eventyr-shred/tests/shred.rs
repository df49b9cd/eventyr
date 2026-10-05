//! The shredder against a toy cipher: sealing, opening, erasure, and the
//! store wrapper end to end. The toy cipher is *not* encryption — it
//! XORs with the key and appends a checksum of key, plaintext and aad —
//! but it has the properties the shredder relies on: wrong key or wrong
//! aad fails, and the stored bytes differ from the plaintext.

use std::sync::Arc;

use eventyr_core::envelope::NewEvent;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId, Version};
use eventyr_shred::{
    Cipher, CipherError, InMemoryKeyStore, Sensitive, ShredError, Shredder, ShreddingStore,
    SubjectKey,
};
use eventyr_store::memory::InMemoryStore;
use eventyr_store::store::{EventStore, StreamsAll};
use futures::TryStreamExt;
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum CustomerEvent {
    Registered {
        customer: String,
        email: Sensitive<String>,
    },
    Married {
        a: String,
        b: String,
        a_name: Sensitive<String>,
        b_name: Sensitive<String>,
    },
    Ordered {
        customer: String,
        total: u64,
    },
}

impl eventyr_core::event_name::EventName for CustomerEvent {
    fn event_name(&self) -> &'static str {
        match self {
            Self::Registered { .. } => "Registered",
            Self::Married { .. } => "Married",
            Self::Ordered { .. } => "Ordered",
        }
    }
}

impl eventyr_core::boundary::Tagged for CustomerEvent {
    fn tags(&self) -> Vec<eventyr_core::boundary::Tag> {
        vec![]
    }
}

fn registered(customer: &str, email: &str) -> CustomerEvent {
    CustomerEvent::Registered {
        customer: customer.into(),
        email: Sensitive::new(customer, email.to_owned()),
    }
}

fn shredder() -> Arc<Shredder<Toy, InMemoryKeyStore>> {
    Arc::new(Shredder::new(Toy, InMemoryKeyStore::new()))
}

#[test]
fn sealing_hides_the_value_and_opening_restores_it() {
    let shredder = shredder();
    let event = registered("c-1", "ada@example.com");
    let sealed = block_on(shredder.seal(&event)).expect("seal");
    let stored = serde_json::to_string(&sealed).expect("json");
    assert!(!stored.contains("ada@example.com"), "{stored}");
    assert!(stored.contains("\"$sensitive\":\"sealed\""), "{stored}");
    assert_eq!(block_on(shredder.open(sealed)).expect("open"), event);
}

#[test]
fn erasing_a_subject_shreds_its_fields_and_only_its_fields() {
    let shredder = shredder();
    let married = CustomerEvent::Married {
        a: "c-1".into(),
        b: "c-2".into(),
        a_name: Sensitive::new("c-1", "Ada".to_owned()),
        b_name: Sensitive::new("c-2", "Bob".to_owned()),
    };
    let sealed = block_on(shredder.seal(&married)).expect("seal");
    block_on(shredder.erase("c-1")).expect("erase");
    let CustomerEvent::Married { a_name, b_name, .. } =
        block_on(shredder.open(sealed)).expect("an erased field is not an error")
    else {
        panic!("married");
    };
    assert!(a_name.is_shredded());
    assert_eq!(a_name.or(&"[erased]".to_owned()), "[erased]");
    assert_eq!(b_name.get().map(String::as_str), Some("Bob"));
}

#[test]
fn an_erased_subject_cannot_be_sealed_again() {
    let shredder = shredder();
    block_on(shredder.seal(&registered("c-1", "a@x"))).expect("seal");
    block_on(shredder.erase("c-1")).expect("erase");
    assert!(matches!(
        block_on(shredder.seal(&registered("c-1", "b@x"))),
        Err(ShredError::SubjectErased(subject)) if subject == "c-1"
    ));
}

#[test]
fn a_field_moved_to_another_subject_fails_to_open() {
    // The subject is authenticated (aad): rewriting the stored subject
    // to one whose key would decrypt is caught, not silently accepted.
    let shredder = shredder();
    block_on(shredder.seal(&registered("c-2", "bob@x"))).expect("give c-2 a key");
    let sealed = block_on(shredder.seal(&registered("c-1", "ada@x"))).expect("seal");
    let tampered = serde_json::to_string(&sealed)
        .expect("json")
        .replace("\"subject\":\"c-1\"", "\"subject\":\"c-2\"");
    let tampered: CustomerEvent = serde_json::from_str(&tampered).expect("parse");
    assert!(matches!(
        block_on(shredder.open(tampered)),
        Err(ShredError::Cipher(_))
    ));
}

#[test]
fn events_without_sensitive_fields_pass_through() {
    let shredder = shredder();
    let order = CustomerEvent::Ordered {
        customer: "c-1".into(),
        total: 5,
    };
    let sealed = block_on(shredder.seal(&order)).expect("seal");
    assert_eq!(sealed, order);
    assert_eq!(block_on(shredder.open(sealed)).expect("open"), order);
}

#[test]
fn the_store_wrapper_seals_in_the_log_and_opens_for_readers() {
    let inner = Arc::new(InMemoryStore::<CustomerEvent>::new());
    let store = ShreddingStore::new(Arc::clone(&inner), shredder());
    let stream = StreamId::from("customer-c-1");

    let committed = block_on(store.append(
        &stream,
        ExpectedVersion::Empty,
        vec![NewEvent::new(registered("c-1", "ada@example.com"))],
    ))
    .expect("append");
    assert_eq!(committed[0].event, registered("c-1", "ada@example.com"));

    // The log holds ciphertext.
    let raw: Vec<_> = block_on(inner.stream_all(Sequence::START).try_collect()).expect("raw");
    let stored = serde_json::to_string(&raw[0].event).expect("json");
    assert!(!stored.contains("ada@example.com"), "{stored}");

    // Readers through the wrapper see plain values...
    let read: Vec<_> = block_on(store.stream(&stream, Version::EMPTY).try_collect()).expect("read");
    assert_eq!(read[0].event, registered("c-1", "ada@example.com"));

    // ...until the subject is erased; then the history still reads.
    block_on(store.shredder().erase("c-1")).expect("erase");
    let all: Vec<_> = block_on(store.stream_all(Sequence::START).try_collect()).expect("rebuild");
    let CustomerEvent::Registered { email, .. } = &all[0].event else {
        panic!("registered");
    };
    assert!(email.is_shredded());
}

#[test]
fn a_sensitive_value_never_shows_in_debug_output() {
    let event = registered("c-1", "ada@example.com");
    assert!(!format!("{event:?}").contains("ada@example.com"));
}

#[test]
fn the_in_memory_key_store_passes_the_key_store_contract() {
    eventyr_shred::key_store_contract(InMemoryKeyStore::new);
}

/// The wrapper must be indistinguishable from the store it wraps for
/// events with no sensitive fields: the whole store contract, run
/// through it.
#[test]
fn a_shredding_store_passes_the_store_contracts() {
    let make = || ShreddingStore::new(InMemoryStore::<u64>::new(), shredder());
    eventyr_store_testing::event_store_contract::<u64, _>(make);
    eventyr_store_testing::streams_all_contract::<u64, _>(make);
    eventyr_store_testing::event_store_batch_contract::<u64, _>(make);
    eventyr_store_testing::lifecycle_contract::<u64, _>(make);
}

/// End to end through the write machine: commands decide against plain
/// fields, erasure shreds them, and the aggregate still folds — the
/// domain sees `Shredded`, not an error, and later commands still run.
#[test]
fn an_aggregate_keeps_working_after_its_subject_is_erased() {
    use eventyr_core::aggregate::Aggregate;
    use eventyr_core::write::RetryPolicy;
    use eventyr_store::repository::AggregateRepository;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    enum ProfileEvent {
        Created { email: Sensitive<String> },
        Renamed { name: Sensitive<String> },
    }

    #[derive(Default, Debug)]
    struct Profile {
        exists: bool,
        email: Option<String>,
        renames: u32,
    }

    #[derive(Debug)]
    enum ProfileCommand {
        Create { email: String },
        Rename { name: String },
    }

    #[derive(Debug, PartialEq)]
    enum ProfileError {
        Missing,
    }
    impl core::fmt::Display for ProfileError {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.write_str("no such profile")
        }
    }

    struct ProfileAggregate;
    impl Aggregate for ProfileAggregate {
        const NAME: &'static str = "profile";
        type Id = String;
        type State = Profile;
        type Event = ProfileEvent;
        type Command = ProfileCommand;
        type Error = ProfileError;

        fn initial(_: &String) -> Profile {
            Profile::default()
        }

        fn apply(state: &mut Profile, event: &ProfileEvent) {
            match event {
                ProfileEvent::Created { email } => {
                    state.exists = true;
                    state.email = email.get().cloned();
                }
                ProfileEvent::Renamed { .. } => state.renames += 1,
            }
        }

        fn decide(
            state: &Profile,
            command: &ProfileCommand,
        ) -> Result<Vec<ProfileEvent>, ProfileError> {
            match command {
                ProfileCommand::Create { email } => Ok(vec![ProfileEvent::Created {
                    email: Sensitive::new("p-1", email.clone()),
                }]),
                ProfileCommand::Rename { .. } if !state.exists => Err(ProfileError::Missing),
                ProfileCommand::Rename { name } => Ok(vec![ProfileEvent::Renamed {
                    name: Sensitive::new("p-1", name.clone()),
                }]),
            }
        }
    }

    let inner = Arc::new(InMemoryStore::<ProfileEvent>::new());
    let store = Arc::new(ShreddingStore::new(Arc::clone(&inner), shredder()));
    let repo =
        AggregateRepository::<ProfileAggregate, _>::new(Arc::clone(&store), RetryPolicy::default());

    block_on(repo.execute(
        "p-1".into(),
        ProfileCommand::Create {
            email: "ada@x".into(),
        },
    ))
    .expect("create");
    block_on(repo.execute("p-1".into(), ProfileCommand::Rename { name: "Ada".into() }))
        .expect("rename");

    block_on(store.shredder().erase("p-1")).expect("erase");

    // The history still folds — `exists` comes from the event's shape,
    // not its personal data — so the command is decided. But an erased
    // subject cannot be given new personal data: the seal fails and the
    // append is refused, never written in the clear.
    let outcome =
        block_on(repo.execute("p-1".into(), ProfileCommand::Rename { name: "A.".into() }));
    assert!(
        matches!(&outcome, Err(error) if error.to_string().contains("erased")),
        "{outcome:?}"
    );

    let raw: Vec<_> = block_on(inner.stream_all(Sequence::START).try_collect()).expect("raw");
    assert_eq!(raw.len(), 2, "nothing was appended for the erased subject");
    let opened: Vec<_> = block_on(store.stream_all(Sequence::START).try_collect()).expect("open");
    let mut state = Profile::default();
    for envelope in &opened {
        ProfileAggregate::apply(&mut state, &envelope.event);
    }
    assert!(state.exists);
    assert_eq!(state.email, None, "the email is gone");
    assert_eq!(state.renames, 1);
}

/// Every write path seals: single, batch, and conditional appends.
#[test]
fn no_write_path_lets_plaintext_through() {
    use eventyr_core::batch::StreamAppend;
    use eventyr_core::boundary::{AppendCondition, Query};
    use eventyr_store::store::QueryAppend;

    let inner = Arc::new(InMemoryStore::<CustomerEvent>::new());
    let store = ShreddingStore::new(Arc::clone(&inner), shredder());
    let batch = |stream: &str, email: &str| StreamAppend {
        stream_id: StreamId::from(stream),
        expected: ExpectedVersion::Any,
        events: vec![NewEvent::new(registered("c-1", email))],
    };
    block_on(store.append_batch(vec![batch("customer-1", "batch@x")])).expect("batch");
    block_on(store.append_if(
        vec![batch("customer-2", "conditional@x")],
        AppendCondition {
            query: Query::none(),
            after: Sequence::START,
        },
    ))
    .expect("append_if");

    let raw: Vec<_> = block_on(inner.stream_all(Sequence::START).try_collect()).expect("raw");
    let stored =
        serde_json::to_string(&raw.iter().map(|e| &e.event).collect::<Vec<_>>()).expect("json");
    assert_eq!(raw.len(), 2);
    assert!(!stored.contains("batch@x"), "{stored}");
    assert!(!stored.contains("conditional@x"), "{stored}");
}
