# Security policy

## Supported versions

The workspace is pre-release: only the latest `main` receives fixes.
Once crates are published, the latest minor line will be supported and
the policy here will name the branches.

## Reporting a vulnerability

Report privately to the repository owner via GitHub's
*Report a vulnerability* action on the Security tab, or open a private
security advisory on `df49b9cd/eventyr`. Please do not open a public
issue for anything security-sensitive. You will get an acknowledgement
within a few days; a fix or mitigation plan follows before any public
disclosure, coordinated with you.

## Scope notes specific to this workspace

- **`eventyr-shred` is crypto-shredding, not access control.** It
  encrypts personal fields under per-subject keys so that deleting a
  key erases the data. It is not a secret store, an authentication
  system, or a defence against an attacker who holds both the database
  and the key store. The ciphers are AES-256-GCM and XChaCha20-Poly1305
  with random nonces; the keys are yours to generate, rotate, and
  protect. Erasure covers the event log only — snapshots, view rows,
  and logs that copied personal data out must be cleared separately
  (the crate docs say so in detail).
- **Stores execute SQL from their own migrations only** and expose no
  user-supplied SQL. Stream ids and event names are bound as
  parameters, never interpolated.
- **A store's security is the database's.** The Postgres store assumes
  a single writable primary and standard connection hygiene; see the
  crate docs' deployment contract before running it against
  user-facing traffic.
