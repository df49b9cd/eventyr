# eventyr-sample — a miniature bank, end to end

A deployable CQRS/event-sourcing sample on eventyr and eventyr-mediator:
commands through the mediator, events in the store, views from a
projection, failover via leases. One account aggregate, one read model,
one cross-aggregate rule (the transfer is a batch, not a saga), and one
edge that maps every command outcome to its HTTP status.

Run it two ways:

```sh
# In-memory, one process, out of the box.
cargo run -p eventyr-sample

# The compose-managed cluster: a Postgres primary behind the API replicas.
docker compose up --build
```

## What it is

The crate's axis is **a miniature bank**: open an account, deposit,
withdraw, transfer. The point is the architecture, not banking. One
aggregate (`Account`) holds the one rule a bank cannot break — an account
never goes below zero — and the shape around it is the CQRS/event-
sourcing one eventyr composes:

```text
            ┌──────────────────────────────────────────────┐
            │                the wiring                     │
            │  HTTP edge → mediator → Execute / batch        │
            │                 │            │                │
            │            projections ← store subscription   │
            └──────────────────────────────────────────────┘
                 everything the slices name is inside the box
```

- **The domain** ([`src/domain/`](src/domain/)) is one aggregate,
  decided and applied by `#[derive(Aggregate)]`. Its events are the
  store's schema; the read side is one projection over them.
- **The slices** ([`src/features/`](src/features/)) are vertical: one
  request, one handler, one outcome. They send `Execute` and
  `Transfer` through the mediator and never name a store.
- **The wiring** ([`src/wiring/`](src/wiring/)) is the composition
  root. The mediator, the projection, and the saga are the same type on
  every backend; only the store's name changes.
- **The edge** ([`src/main.rs`](src/main.rs)) serves HTTP: commands to
  the mediator, a projection's row to the read, the interaction's
  result to its status.

## Run it

**In memory** — `cargo run -p eventyr-sample`. `eventyr`'s in-memory
store, so no dependencies. The projection and the saga run as the
process's own replicas.

**The cluster** — `docker compose up --build`. A Postgres primary
serves every API replica through the event store; the replicas share a
checkpoint and a lease, so exactly one projects while the others stand
by. Scale the API — `docker compose up --build --scale api=3` — and the
projection fails over between them.

```sh
# Open an account, then deposit and read the balance back.
curl -X POST localhost:3000/accounts -d '{"account": 1, "owner": "ada"}' -H 'content-type: application/json'
curl -X POST localhost:3000/accounts/1/deposits -d '{"amount": 100}' -H 'content-type: application/json'
curl localhost:3000/accounts/1/balance
```

## How it's wired

The composition root is the only place that names a store.

| Piece             | What it is                                                  |
| ----------------- | ----------------------------------------------------------- |
| `Backend`         | `Memory` \| `Postgres` — which store composes.              |
| `compose()`       | Builds one backend; `DATABASE_URL` picks which.             |
| `App`             | The mediator; one type, composed per backend.                |
| `runners`         | The projection and the saga, one task each, on a replica.    |
| `main`            | The HTTP edge, over the mediator.                           |

Each backend builds the same `App` — the commands route to
`Execute<Account>`, the queries to `Load`, the transfer to its batch —
and the projection's replica runs `run_projection_once` + `run_saga`
against the store the backend names.

## The boundaries that hold

- The slices never name a store, a repository, or a retry policy.
- The mediator composes the write side; a domain rejection is a value
  the edge maps, not a panic.
- The read path serves the projection's row, without touching the event
  log.

## Layout

```text
eventyr-sample/
├── Cargo.toml
├── README.md
├── Dockerfile
├── docker-compose.yml
├── src/
│   ├── lib.rs
│   ├── main.rs
│   ├── domain/account.rs
│   ├── features/{open_account, deposit, withdraw, transfer, balance}.rs
│   └── wiring/{mod, compose, runners}.rs
└── tests/{api_contract, read_side, compose}.rs
```

The compose-managed test (`tests/compose.rs`) runs against a real
Postgres. Bring one up with `./scripts/dev-services.sh up` (a throwaway
container on `127.0.0.1:55432`, the same credentials as the compose
file), then run it with the backend's feature:

```sh
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/eventyr \
    cargo test -p eventyr-sample --features postgres --test compose -- --include-ignored
```

The umbrella's own examples cover what the sample does not — snapshots,
upcasting, crypto-shredding, `Decision` boundaries, and the store's
adapters (SQLite, Postgres, fjall) in full.
