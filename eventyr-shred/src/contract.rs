//! The [`Cipher`] and [`KeyStore`] contracts: what an adapter must
//! satisfy before the shredder can rely on it. Every adapter crate and
//! every key store runs the one that applies.

use crate::cipher::{Cipher, SubjectKey};
use crate::keys::KeyStore;

/// Run the cipher contract against `cipher`. Panics with the first
/// broken property.
pub fn cipher_contract<C: Cipher>(cipher: &C) {
    let key = cipher.generate_key().expect("generate a key");
    let other = cipher.generate_key().expect("generate another key");
    assert_ne!(key, other, "two generated keys are distinct");

    let plaintext = b"ada@example.com";
    let sealed = cipher.encrypt(&key, plaintext, b"c-1").expect("encrypt");
    assert_ne!(
        &sealed[..],
        &plaintext[..],
        "ciphertext is not the plaintext"
    );
    assert!(
        !sealed
            .windows(plaintext.len())
            .any(|window| window == plaintext),
        "the plaintext does not appear inside the ciphertext"
    );
    assert_eq!(
        cipher.decrypt(&key, &sealed, b"c-1").expect("round trip"),
        plaintext,
        "decrypt inverts encrypt"
    );

    let again = cipher
        .encrypt(&key, plaintext, b"c-1")
        .expect("encrypt again");
    assert_ne!(sealed, again, "every encryption uses a fresh nonce");

    assert!(
        cipher.decrypt(&other, &sealed, b"c-1").is_err(),
        "the wrong key fails"
    );
    assert!(
        cipher.decrypt(&key, &sealed, b"c-2").is_err(),
        "the wrong associated data fails"
    );
    let mut flipped = sealed.clone();
    let last = flipped.len() - 1;
    flipped[last] ^= 1;
    assert!(
        cipher.decrypt(&key, &flipped, b"c-1").is_err(),
        "a flipped bit fails"
    );
    assert!(
        cipher.decrypt(&key, &sealed[..4], b"c-1").is_err(),
        "a truncated ciphertext fails"
    );
    assert!(
        cipher.decrypt(&key, &[], b"c-1").is_err(),
        "an empty ciphertext fails"
    );

    let empty = cipher.encrypt(&key, b"", b"c-1").expect("encrypt nothing");
    assert_eq!(
        cipher
            .decrypt(&key, &empty, b"c-1")
            .expect("round trip nothing"),
        b""
    );

    let short_key = SubjectKey::from_bytes(vec![1, 2, 3]);
    assert!(
        cipher.encrypt(&short_key, plaintext, b"c-1").is_err(),
        "a key of the wrong length is refused"
    );
}

/// Run the key store contract against `make_store`'s fresh stores. The
/// property erasure rests on: an erased subject stays erased — its key
/// is gone, and it cannot be given a new one.
pub fn key_store_contract<K: KeyStore>(make_store: impl Fn() -> K) {
    use futures::executor::block_on;

    let key = |byte| SubjectKey::from_bytes(vec![byte; 32]);

    let store = make_store();
    assert_eq!(
        block_on(store.load("s-1")).expect("load"),
        None,
        "no key yet"
    );
    assert_eq!(
        block_on(store.create("s-1", key(1))).expect("create"),
        Some(key(1)),
        "create stores the key"
    );
    assert_eq!(block_on(store.load("s-1")).expect("load"), Some(key(1)));
    assert_eq!(
        block_on(store.create("s-1", key(2))).expect("create again"),
        Some(key(1)),
        "a second create keeps the first key (two racing writers agree)"
    );
    assert_eq!(
        block_on(store.load("s-2")).expect("load"),
        None,
        "keys are per subject"
    );

    block_on(store.delete("s-1")).expect("delete");
    assert_eq!(
        block_on(store.load("s-1")).expect("load"),
        None,
        "the key is gone"
    );
    assert_eq!(
        block_on(store.create("s-1", key(3))).expect("create after delete"),
        None,
        "an erased subject cannot get a new key"
    );
    assert_eq!(block_on(store.load("s-1")).expect("load"), None);
    block_on(store.delete("s-1")).expect("delete is idempotent");

    // Erasing a subject that never had a key still bars it.
    let store = make_store();
    block_on(store.delete("s-9")).expect("delete unknown");
    assert_eq!(
        block_on(store.create("s-9", key(4))).expect("create"),
        None,
        "erasure before first use still holds"
    );
}
