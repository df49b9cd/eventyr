# eventyr-shred-chacha

The XChaCha20-Poly1305 adapter for [crypto-shredding](eventyr-shred.md): a
one-newtype `Cipher` implementation over `AeadCipher`, so a shredder can
name `"xchacha20-poly1305"` as its algorithm and seal/open with XChaCha20-Poly1305.

**Position:** a leaf adapter over eventyr-shred's `aead` feature; pulled
in through the umbrella as `shred_aes_gcm` / `shred_chacha`. Parent:
[ARCHITECTURE.md](../ARCHITECTURE.md).

## What it is

```rust
pub struct XChaCha20Poly1305Cipher(AeadCipher<...>);
// `Cipher::algorithm() == "xchacha20-poly1305"`
```

The whole crate is that newtype plus docs: the generic `AeadCipher<A>`
in [eventyr-shred](eventyr-shred.md) does the nonce/ciphertext/tag
assembly (`nonce ‖ ciphertext ‖ tag`, base64 in `Sealed`); the adapter
binds the concrete AEAD and its algorithm name. A new cipher crate is
the same shape — this pair exists partly to prove how small the seam is.

Nonce strategy: 192-bit nonces (random nonces are safe to the birthday horizon; the crate documents the bound).

The subject-as-AAD binding, key custody, and erasure semantics are all
in [eventyr-shred](eventyr-shred.md); nothing here is cipher-specific
beyond the construction.

## Testing

Runs `cipher_contract` (eventyr-shred's `testing` feature) against the
real cipher — encrypt/decrypt round-trip, wrong-algorithm rejection,
AAD mismatch, seal-through-a-shredder integration.

## Limits

* Follows the underlying crate's safety bounds; read the crate docs
  before high-volume use.
* One cipher per `Shredder` at a time: a shredder seals under its
  configured `Cipher`, and ciphertexts name their algorithm
  (`Sealed.algorithm`), so a rotation means a new shredder reading old
  seals until re-encryption — no cross-decode magic ships.
