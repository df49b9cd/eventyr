//! Crypto-shredding over SQLite: the key store contract, and erasure end
//! to end through a shredding store with a real cipher.

use std::sync::Arc;

use eventyr_core::envelope::NewEvent;
use eventyr_core::event_name::EventName;
use eventyr_core::vocabulary::{ExpectedVersion, Sequence, StreamId};
use eventyr_shred::{Sensitive, Shredder, ShreddingStore};
use eventyr_shred_chacha::XChaCha20Poly1305Cipher;
use eventyr_store::store::{EventStore, StreamsAll};
use eventyr_store_sqlite::{SqliteKeyStore, SqliteStore};
use futures::TryStreamExt;
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

#[test]
fn the_sqlite_key_store_passes_the_key_store_contract() {
    eventyr_shred::key_store_contract(|| {
        SqliteKeyStore::from_connection(rusqlite::Connection::open_in_memory().expect("open"))
            .expect("schema")
    });
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Event {
    Registered {
        customer: String,
        email: Sensitive<String>,
    },
}

impl EventName for Event {
    fn event_name(&self) -> &'static str {
        "Registered"
    }
}

#[test]
fn an_erased_subjects_data_is_gone_from_the_database_file() {
    let dir = std::env::temp_dir().join(format!("eventyr-shred-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("dir");
    let events_path = dir.join("events.db");
    let keys_path = dir.join("keys.db");

    let events = SqliteStore::<Event>::open(&events_path).expect("events");
    let keys = SqliteKeyStore::open(&keys_path).expect("keys");
    let store = ShreddingStore::new(
        events.clone(),
        Arc::new(Shredder::new(XChaCha20Poly1305Cipher, keys)),
    );
    block_on(store.append(
        &StreamId::from("customer-1"),
        ExpectedVersion::Empty,
        vec![NewEvent::new(Event::Registered {
            customer: "c-1".into(),
            email: Sensitive::new("c-1", "ada@example.com".to_owned()),
        })],
    ))
    .expect("append");

    // The plaintext never reaches the events file.
    drop(events);
    let bytes = std::fs::read(&events_path).expect("read file");
    assert!(
        !bytes.windows(15).any(|w| w == b"ada@example.com"),
        "plaintext in the events file"
    );

    block_on(store.shredder().erase("c-1")).expect("erase");
    let all: Vec<_> = block_on(store.stream_all(Sequence::START).try_collect()).expect("read");
    let Event::Registered { email, .. } = &all[0].event;
    assert!(email.is_shredded(), "the field reads back shredded");

    // And the key is gone from the keys file, not just marked.
    drop(store);
    let conn = rusqlite::Connection::open(&keys_path).expect("open keys");
    let key: Option<Vec<u8>> = conn
        .query_row(
            "SELECT key FROM subject_keys WHERE subject = 'c-1'",
            [],
            |r| r.get(0),
        )
        .expect("row");
    assert_eq!(key, None);
    std::fs::remove_dir_all(dir).ok();
}
