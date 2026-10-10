# eventyr-shred

Crypto-shredding for the log: personal data encrypted per **data subject**
at append time, erased by deleting the subject's key. `Sensitive<T>` wraps
a personal field; a `ShreddingStore` decorates any store, sealing every
`Sensitive` field on the way in and opening them on the way out; when a
subject's key is deleted, their fields become unreadable forever — the
log is intact, the personal data is gone.

**Position:** a decorator over eventyr-store (implements the same ports by
delegation — the [core placement rule](../ARCHITECTURE.md#2-crate-layering)
bends for no one, so the wrapper lives above the ports, not inside core).
Parent: [ARCHITECTURE.md](../ARCHITECTURE.md).

## Features

* `zeroize` — `SubjectKey` and per-call caches zero their bytes on drop.
* `aead` — the generic `AeadCipher<A>`: an algorithm adapter crate
  ([aes-gcm](eventyr-shred-aes-gcm.md), [chacha](eventyr-shred-chacha.md))
  is one newtype over it.
* `parked` (pulls subscription) — `ShreddingParkedStore`: a parked-event
  store wrapper that **seals before parking**, so poison events on disk
  carry no plaintext personal data either.
* `testing` — `cipher_contract`, `key_store_contract` for adapter/KMS
  implementations.

## The vocabulary (`sensitive.rs`)

```rust
enum Sensitive<T> { Plain{subject, value}, Sealed(Sealed), Shredded }
```

* Serialized tagged `"$sensitive"`, so the shredder can find personal
  fields inside any event's JSON, wherever they nest. One event can carry
  several subjects' data; each erases independently.
* `Debug` never shows the value — `Sensitive(<plain>)` etc.
* `or(replacement)` gives the projection's declared stand-in for erased
  data; `is_shredded()` asks directly.
* `Sealed{algorithm, subject, ciphertext}` is the stored form: which
  cipher, which subject, base64 ciphertext (nonce inside).

## The machinery

* **`SubjectKey`** — the symmetric key for one subject
  (`from_bytes`/`as_bytes`, `ZeroizeOnDrop` under the feature). Recipients
  (a KMS) may keep copies; this type's job is not to leak in-process.
* **`Cipher`** — `algorithm()`, `generate_key(subject)`,
  `encrypt(key, plaintext, aad)`, `decrypt(..)`. The **subject is bound as
  the AAD**: a sealed field cannot be re-attached to another subject, and
  an erased subject can never mint a fresh key that decrypts old
  ciphertexts — the erasure is irrevocable by construction, not by
  bookkeeping.
* **`KeyStore`** — `load(subject)`, `create(subject, key) ->
  Option<SubjectKey>` (**existing key wins; an erased subject answers
  `None`** — this is the whole erasure mechanism), `delete(subject)`.
  `InMemoryKeyStore` is the reference; [SQLite](eventyr-store-sqlite.md)
  ships the durable one.
* **`Shredder::new(cipher, keys)`** — `seal`/`open`/`erase`, walking the
  event's JSON for `$sensitive` nodes.
* **`ShreddingStore<S, C, K>`** — the port decorator: implements
  `EventStore`, `StreamsAll`, `QueryAppend`, `StreamLifecycle`,
  `CommitSignal` by delegation, sealing on every append path (including
  `append_batch` and `append_if`) and opening on every read path. Tags
  still match on **decoded** events (the [DCB](../ARCHITECTURE.md#4-the-five-machines)
  rule), so sealing is transparent to boundary queries.

## Failure model

`ShredError::{Json(JsonKind), Cipher, WrongAlgorithm, SubjectErased,
Malformed, Keys}` — and the design rule is **loud**:

* JSON errors are **redacted to category + position** (`JsonKind`, line/
  column): a shredded payload must never leak through an error message.
* `WrongAlgorithm` (the ciphertext names a cipher this shredder doesn't
  run) and `SubjectErased` end the read — a shredded field is not "empty
  data", it is *gone*, and pretending otherwise would corrupt a
  projection silently.
* There is no "open or default" escape hatch; `Sensitive::or` is the
  application's declared stand-in at the *value* level, after the event
  decoded.

## Erasure, end to end

1. Append events through the `ShreddingStore`: every `Plain` field is
   sealed with the subject's key (`KeyStore::create` mints one if
   none exists — existing wins).
2. To erase a subject: `KeyStore::delete(subject)`. The log still holds
   every sealed field; nothing can decrypt them, ever (AAD binds the
   subject; no new key can be valid for an erased subject).
3. Reads open `Sealed` → `Plain` for subjects with keys; a deleted
   subject's fields error `SubjectErased` — the application decides
   whether to skip the event or re-project with `or(replacement)`.

## Testing

The [store contract suites](eventyr-store-testing.md) run through
`ShreddingStore` — the decorator must be behaviorally transparent.
`cipher_contract` and `key_store_contract` (behind `testing`) gate every
cipher adapter and key-store implementation; both shipped ciphers run
them, and the SQLite key store runs the key-store one.

## Limits

* Shredding protects *personal fields*, not whole events: non-`Sensitive`
  payload stays in the clear (that's the point — the audit log survives).
* `KeyStore` is the trust boundary; a KMS that keeps deleted keys defeats
  the design — the SQLite store's docs are explicit about separate-file
  custody.
* Parked-event sealing needs the `parked` feature; a plain `ParkedStore`
  parks plaintext — check which wrapper you configured.
* The erasure is per-*subject* named in the event; subject-identity
  management (which user is which subject) is the application's job.
