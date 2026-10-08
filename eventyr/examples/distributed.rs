//! # A distributed deployment — many writers, standby projectors, one
//! codebase from an embedded device to a cluster (§15, §16).
//!
//! Every node runs the same code: it accepts commands and runs a replica
//! of the `balances` projector. Nothing routes commands; any node
//! executes any command, and the store's optimistic concurrency settles
//! the races. Nothing elects a leader either: the projector lease (roadmap 0.7.9)
//! lets exactly one replica project at a time, and the others stand by
//! to take over from the shared checkpoint.
//!
//! The run shows, on each adapter:
//!
//! - **Concurrent writers** on a few hot wallets. Conflicts are normal
//!   and retried; the domain's invariant (no overdraft) holds anyway.
//! - **A client retry on another node**, sent to two nodes at once
//!   under one idempotency key (roadmap 0.7.5), commits exactly once.
//! - **Projector failover.** The active replica freezes mid-batch (a GC
//!   pause, a network partition). Its lease expires, a standby resumes
//!   from the last checkpoint, and the frozen replica wakes to find its
//!   lease gone and stops without writing a checkpoint again. The events
//!   both of them applied are absorbed by the view store's newest-wins
//!   guard, so the read model stays exact.
//! - **Read-your-writes** without waiting for the projector to "catch
//!   up": the client waits until the row's own version passes the
//!   position its command committed at (the 0.8.3 token, written by
//!   hand).
//!
//! The same `run_cluster` function drives two deployments:
//!
//! - **Embedded**: one SQLite file, the nodes are tasks in one process
//!   sharing one connection, and the lease is in process memory — all
//!   an embedded device needs.
//! - **Clustered**: Postgres, each node with its own connection pool (a
//!   separate database session, as a separate process would have),
//!   Postgres leases, and commit notifications over `LISTEN`/`NOTIFY`.
//!   It runs when `EVENTYR_PG_URL` points at a Postgres; the run uses a
//!   throwaway schema and drops it afterwards.
//!
//! What this example does not claim (see DESIGN.md §15.4 and §16): every
//! commit to one store still serializes on its commit-order lock, so
//! writers scale out but each store has a write ceiling; and replicas of
//! one projector give failover, not parallelism, until slices (roadmap 0.9).
//!
//! Run with:
//! ```sh
//! cargo run -p eventyr --example distributed --features sqlite_views,sqlite_checkpoints,postgres_checkpoints,postgres_leases,postgres_views
//! EVENTYR_PG_URL=postgres://postgres:postgres@localhost:5432/eventyr \
//!     cargo run -p eventyr --example distributed --features sqlite_views,sqlite_checkpoints,postgres_checkpoints,postgres_leases,postgres_views
//! ```

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use eventyr::postgres::views::PgViewStore;
use eventyr::postgres::{PgCheckpointStore, PgCommitSignal, PgLeaseStore, PgStore};
use eventyr::prelude::*;
use eventyr::projection::prelude::{View, ViewProjection, ViewStore};
use eventyr::sqlite::{SqliteCheckpointStore, SqliteStore, SqliteViewStore};
use eventyr::store::driver::drive_write_with_metrics;
use eventyr::store::metrics::names;
use eventyr::store::prelude::*;
use eventyr::subscription::prelude::{
    CheckpointStore, InMemoryLeaseStore, LeasePolicy, Projection, Projector, ProjectorLease,
    RunError, StoreSubscription,
};
use futures::TryStreamExt;

// -- the domain -----------------------------------------------------

/// A wallet instance's identifier.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct WalletId(u64);
impl fmt::Display for WalletId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Credited {
    amount: u64,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Debited {
    amount: u64,
}

/// The wallet's commands.
#[derive(Clone, Debug)]
enum WalletCommand {
    Credit { amount: u64 },
    Debit { amount: u64 },
}

/// The wallet's one rule: no overdraft.
#[derive(Debug, PartialEq)]
enum WalletError {
    InsufficientFunds,
}
impl fmt::Display for WalletError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("insufficient funds")
    }
}

#[derive(Clone, Debug, Default)]
struct WalletState {
    balance: u64,
}

fn apply(state: &mut WalletState, event: &WalletEvent) {
    match event {
        WalletEvent::Credited(Credited { amount }) => state.balance += amount,
        WalletEvent::Debited(Debited { amount }) => state.balance -= amount,
    }
}

fn decide(state: &WalletState, command: &WalletCommand) -> Result<Vec<WalletEvent>, WalletError> {
    match command {
        WalletCommand::Credit { amount } => Ok(vec![Credited { amount: *amount }.into()]),
        WalletCommand::Debit { amount } if *amount > state.balance => {
            Err(WalletError::InsufficientFunds)
        }
        WalletCommand::Debit { amount } => Ok(vec![Debited { amount: *amount }.into()]),
    }
}

#[derive(Aggregate)]
// The umbrella's own examples live in the `eventyr` package itself,
// where `proc-macro-crate` cannot distinguish them from the target
// crate — a user's crate is not named `eventyr` and needs none of
// this. (Same story as serde's own examples.)
#[eventyr(
    crate = "eventyr",
    id = WalletId,
    state = WalletState,
    error = WalletError,
    command = WalletCommand,
    events(Credited, Debited),
    event_derive(serde::Serialize, serde::Deserialize)
)]
struct Wallet;

// -- the read model -------------------------------------------------

/// The `balances` view: one row per wallet stream, persisted in the
/// event store's own database, so a failover or restore rewinds it
/// together with the log (§16.2).
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct Balance {
    balance: u64,
}
impl View<WalletEvent> for Balance {
    fn initial() -> Self {
        Self::default()
    }
    fn apply(&mut self, event: &EventEnvelope<WalletEvent>) {
        let mut state = WalletState {
            balance: self.balance,
        };
        apply(&mut state, &event.event);
        self.balance = state.balance;
    }
}

const VIEW: &str = "balances";

/// The name every node's replica runs under: one name, one lease, one
/// checkpoint.
const PROJECTOR: &str = "balances";

fn stream_of(wallet: u64) -> StreamId {
    StreamId::for_aggregate::<Wallet>(&WalletId(wallet))
}

// -- the knobs ------------------------------------------------------

const NODES: usize = 3;
const COMMANDS_PER_NODE: u64 = 80;
const WALLETS: u64 = 6;

/// Short enough that the demo fails over in well under a second: renew
/// every 100 ms, presumed dead after 300 ms of silence.
const LEASE: LeasePolicy = LeasePolicy {
    ttl: Duration::from_millis(100),
    grace: 3,
    max_grace: 100,
};

/// The active replica freezes on this apply (counted across replicas)...
const FREEZE_AT: u64 = 40;
/// ...for four times as long as its lease survives without a renewal.
const FREEZE_FOR: Duration = Duration::from_millis(1200);

// -- one node -------------------------------------------------------

/// What one node holds: its handles on the event store, the checkpoint
/// store, the lease store, the view store, and the commit signal.
///
/// Embedded, these are clones of one SQLite connection and one
/// in-memory lease table. Clustered, each node has its own Postgres
/// pool. The code below never knows which.
#[derive(Clone)]
struct Node<S, C, L, V, W> {
    id: usize,
    store: S,
    checkpoints: C,
    leases: L,
    views: V,
    signal: W,
}

/// Counts what the write driver reports through the `Metrics` port.
#[derive(Default)]
struct WriteStats {
    appends: AtomicU64,
    conflicts: AtomicU64,
}
impl Metrics for WriteStats {
    fn counter(&self, name: &'static str, by: u64) {
        match name {
            names::APPENDS => self.appends.fetch_add(by, Ordering::Relaxed),
            names::CONFLICTS => self.conflicts.fetch_add(by, Ordering::Relaxed),
            _ => 0,
        };
    }
    fn gauge(&self, _: &'static str, _: u64) {}
    fn histogram(&self, _: &'static str, _: Duration) {}
}

/// Execute one command under its idempotency key, as a client would:
/// the write machine retries conflicts itself, and when its budget runs
/// out on a hot wallet the client tries again. Retrying is safe because
/// the key travels with every attempt.
async fn execute<S>(
    store: &S,
    stats: &WriteStats,
    wallet: u64,
    command: WalletCommand,
    key: String,
) -> WriteOutcome<WalletEvent, WalletError>
where
    S: EventStore<Event = WalletEvent>,
{
    let mut backoff = Duration::from_millis(2);
    for _ in 0..20 {
        let mut machine =
            WriteMachine::<Wallet>::new(WalletId(wallet), command.clone(), RetryPolicy::new(5))
                .with_metadata(Metadata::default().with_idempotency_key(key.clone()));
        match drive_write_with_metrics(&mut machine, store, stats).await {
            WriteOutcome::Failed(StoreError::Conflict { .. } | StoreError::Unavailable) => {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_millis(50));
            }
            outcome => return outcome,
        }
    }
    panic!("command {key} kept conflicting");
}

/// A deterministic workload per node: half the traffic hits wallets 0
/// and 1, so the nodes collide on them.
struct Workload(u64);
impl Workload {
    fn next(&mut self) -> u64 {
        // xorshift64: no dependency, the same sequence every run.
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn command(&mut self) -> (u64, WalletCommand) {
        let wallet = if self.next().is_multiple_of(2) {
            self.next() % 2
        } else {
            self.next() % WALLETS
        };
        let command = if self.next().is_multiple_of(3) {
            WalletCommand::Debit {
                amount: 1 + self.next() % 30,
            }
        } else {
            WalletCommand::Credit {
                amount: 1 + self.next() % 20,
            }
        };
        (wallet, command)
    }
}

/// What one node's writer did.
#[derive(Default)]
struct WriterReport {
    committed: u64,
    rejected: u64,
}

async fn writer<S>(node: usize, store: S, stats: Arc<WriteStats>) -> WriterReport
where
    S: EventStore<Event = WalletEvent>,
{
    let mut workload = Workload(0x9E37_79B9_7F4A_7C15 ^ (node as u64 + 1));
    let mut report = WriterReport::default();
    for i in 0..COMMANDS_PER_NODE {
        let (wallet, command) = workload.command();
        let key = format!("node-{node}:{i}");
        match execute(&store, &stats, wallet, command, key).await {
            WriteOutcome::Committed { .. } => report.committed += 1,
            WriteOutcome::Rejected(WalletError::InsufficientFunds) => report.rejected += 1,
            other => panic!("node {node}: unexpected outcome {other:?}"),
        }
    }
    report
}

// -- the projector replicas -----------------------------------------

/// What the replicas share, to narrate and to check the run: who is
/// projecting, how many events each replica applied, and the freeze.
struct ReplicaStats {
    applied: [AtomicU64; NODES],
    applied_total: AtomicU64,
    /// The node that applied last; `usize::MAX` before anyone has.
    active: AtomicUsize,
    takeovers: AtomicU64,
    lost: AtomicU64,
    /// The node frozen mid-batch, while it is; `usize::MAX` otherwise.
    frozen: AtomicUsize,
}
impl Default for ReplicaStats {
    fn default() -> Self {
        Self {
            applied: Default::default(),
            applied_total: AtomicU64::new(0),
            active: AtomicUsize::new(usize::MAX),
            takeovers: AtomicU64::new(0),
            lost: AtomicU64::new(0),
            frozen: AtomicUsize::new(usize::MAX),
        }
    }
}

/// One node's replica of the projection: the view projection, plus the
/// bookkeeping above and the freeze.
struct Replica<P> {
    node: usize,
    inner: P,
    stats: Arc<ReplicaStats>,
}
impl<P> Projection for Replica<P>
where
    P: Projection,
    P::Event: Sync,
{
    type Event = P::Event;
    type Error = P::Error;
    async fn apply(&mut self, event: &EventEnvelope<P::Event>) -> Result<(), P::Error> {
        let stats = &self.stats;
        let previous = stats.active.swap(self.node, Ordering::Relaxed);
        if stats.frozen.load(Ordering::Relaxed) == self.node {
            // Waking from the freeze: still mid-batch, still believing
            // it holds the lease. Not a takeover; a stale holder.
            if previous != self.node {
                println!(
                    "  node {} applies sequence {} from its stale batch; node {previous} has moved past it",
                    self.node, event.sequence
                );
            }
        } else if previous != self.node {
            if previous == usize::MAX {
                println!("  node {} is projecting", self.node);
            } else {
                stats.takeovers.fetch_add(1, Ordering::Relaxed);
                println!(
                    "  node {} took over the projector from node {previous}, resuming at sequence {}",
                    self.node, event.sequence
                );
            }
        }
        stats.applied[self.node].fetch_add(1, Ordering::Relaxed);
        if stats.applied_total.fetch_add(1, Ordering::Relaxed) + 1 == FREEZE_AT {
            println!(
                "  node {} freezes for {FREEZE_FOR:?} mid-batch (a GC pause, a partition)",
                self.node
            );
            stats.frozen.store(self.node, Ordering::Relaxed);
            tokio::time::sleep(FREEZE_FOR).await;
            println!("  node {} wakes and finishes its batch", self.node);
        }
        self.inner.apply(event).await
    }
}

/// One node's supervisor: keep trying to run the projector. Only the
/// lease holder runs; the others get `Taken` and stand by.
async fn replica<S, C, L, V, W>(node: Node<S, C, L, V, W>, stats: Arc<ReplicaStats>)
where
    S: StreamsAll<Event = WalletEvent> + Clone + Send + Sync + 'static,
    C: CheckpointStore + Clone + 'static,
    L: ProjectorLease + Clone + 'static,
    V: ViewStore<Balance> + Clone + 'static,
    W: CommitSignal + Clone + 'static,
{
    loop {
        let projection = Replica {
            node: node.id,
            inner: ViewProjection::new(
                VIEW,
                node.views.clone(),
                |e: &EventEnvelope<WalletEvent>| Some(e.stream_id.as_str().to_owned()),
            ),
            stats: stats.clone(),
        };
        let run = Projector::new(
            PROJECTOR,
            StoreSubscription::new(node.store.clone()),
            node.checkpoints.clone(),
            projection,
        )
        .with_policy(SubscriptionPolicy::new(
            16,
            Duration::from_millis(200),
            Duration::from_millis(50),
        ))
        .wake_on(node.signal.clone())
        .lease_with_woken(node.leases.clone())
        .with_policy(LEASE)
        .run_woken_leased(tokio::time::sleep)
        .await;
        match run {
            // Standing by: another node holds the lease.
            Err(RunError::Taken { .. }) => tokio::time::sleep(LEASE.ttl).await,
            Err(RunError::LeaseLost { checkpoint, .. }) => {
                stats.lost.fetch_add(1, Ordering::Relaxed);
                let _ = stats.frozen.compare_exchange(
                    node.id,
                    usize::MAX,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
                println!(
                    "  node {} lost its lease and stopped; the last checkpoint it wrote is {checkpoint}",
                    node.id
                );
            }
            Err(RunError::Store(error)) => {
                println!("  node {}: store failure {error}, retrying", node.id);
                tokio::time::sleep(LEASE.ttl).await;
            }
            Ok(outcome) => {
                println!(
                    "  node {}: projector ended {outcome:?}, restarting",
                    node.id
                );
                tokio::time::sleep(LEASE.ttl).await;
            }
        }
    }
}

// -- read-your-writes ------------------------------------------------

/// Wait until the view row for `stream` has folded the event at `at`.
///
/// The row's version is the sequence of the newest event folded into
/// it, so this needs no checkpoint read and works on whatever copy of
/// the view the client reaches. It applies only to rows the command's
/// events fold into. Bounded: a stale result is returned as `None`,
/// never waited on forever (§15, 0.8.3).
async fn read_your_write<V: ViewStore<Balance>>(
    views: &V,
    stream: &StreamId,
    at: Sequence,
    within: Duration,
) -> Option<Balance> {
    let deadline = Instant::now() + within;
    loop {
        if let Some(row) = views
            .load(VIEW, stream.as_str())
            .await
            .expect("the view store reads")
            && row.version >= at
        {
            return Some(row.value);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// -- the run --------------------------------------------------------

async fn run_cluster<S, C, L, V, W>(deployment: &str, nodes: Vec<Node<S, C, L, V, W>>)
where
    S: EventStore<Event = WalletEvent> + StreamsAll + Clone + Send + Sync + 'static,
    C: CheckpointStore + Clone + 'static,
    L: ProjectorLease + Clone + 'static,
    V: ViewStore<Balance> + Clone + 'static,
    W: CommitSignal + Clone + Send + Sync + 'static,
{
    println!("\n== {deployment}: {} nodes ==", nodes.len());
    let started = Instant::now();

    // Every node runs a replica; the lease lets one project.
    let replica_stats = Arc::new(ReplicaStats::default());
    let replicas: Vec<_> = nodes
        .iter()
        .map(|node| tokio::spawn(replica(node.clone(), replica_stats.clone())))
        .collect();

    // Every node writes, concurrently, to the same few wallets.
    let write_stats: Vec<Arc<WriteStats>> = (0..nodes.len()).map(|_| Arc::default()).collect();
    let writers: Vec<_> = nodes
        .iter()
        .map(|node| {
            tokio::spawn(writer(
                node.id,
                node.store.clone(),
                write_stats[node.id].clone(),
            ))
        })
        .collect();
    let mut committed = 0;
    for (node, handle) in writers.into_iter().enumerate() {
        let report = handle.await.expect("the writer runs");
        let stats = &write_stats[node];
        println!(
            "  node {node} wrote {} commands: {} committed, {} rejected (no overdraft), {} conflicts retried",
            COMMANDS_PER_NODE,
            report.committed,
            report.rejected,
            stats.conflicts.load(Ordering::Relaxed),
        );
        committed += report.committed;
    }

    // A client's request times out on node 0, and it retries on node 1
    // while the first attempt is still running. Same key, so exactly one
    // commit.
    let (first, second) = tokio::join!(
        execute(
            &nodes[0].store,
            &write_stats[0],
            3,
            WalletCommand::Credit { amount: 500 },
            "client-retry-1".into(),
        ),
        execute(
            &nodes[1 % nodes.len()].store,
            &write_stats[1 % nodes.len()],
            3,
            WalletCommand::Credit { amount: 500 },
            "client-retry-1".into(),
        ),
    );
    let outcomes = [&first, &second];
    let commits = outcomes
        .iter()
        .filter(|o| matches!(o, WriteOutcome::Committed { .. }))
        .count();
    let replays = outcomes
        .iter()
        .filter(|o| matches!(o, WriteOutcome::AlreadyCommitted { .. }))
        .count();
    assert_eq!(
        (commits, replays),
        (1, 1),
        "one attempt commits, the other returns it"
    );
    committed += 1;
    let keyed = nodes[0]
        .store
        .stream(&stream_of(3), Version::EMPTY)
        .try_filter(|e| {
            futures::future::ready(e.metadata.idempotency_key.as_deref() == Some("client-retry-1"))
        })
        .try_collect::<Vec<_>>()
        .await
        .expect("read wallet 3");
    assert_eq!(keyed.len(), 1, "the retried credit is in the log once");
    println!("  a client retry on two nodes under one key committed once");

    // Read-your-writes: commit on the last node, read through another
    // node's view handle, and wait only for this row to pass this write.
    let last = nodes.len() - 1;
    let WriteOutcome::Committed {
        committed: events, ..
    } = execute(
        &nodes[last].store,
        &write_stats[last],
        5,
        WalletCommand::Credit { amount: 7 },
        "client-ryw-1".into(),
    )
    .await
    else {
        panic!("a credit commits");
    };
    committed += 1;
    let at = events.last().expect("one event").sequence;
    let seen = read_your_write(&nodes[0].views, &stream_of(5), at, Duration::from_secs(10))
        .await
        .expect("the view reaches the write within the bound");
    println!(
        "  read-your-writes: wallet 5 committed at sequence {at}, the view shows balance {}",
        seen.balance
    );

    // Wait for the projector to reach the head, then stop the replicas.
    let log: Vec<_> = nodes[0]
        .store
        .stream_all(Sequence::START)
        .try_collect()
        .await
        .expect("read the log");
    assert_eq!(log.len() as u64, committed, "every commit is one event");
    let head = log.last().expect("events").sequence;
    let deadline = Instant::now() + Duration::from_secs(30);
    while nodes[0]
        .checkpoints
        .load(PROJECTOR)
        .await
        .expect("read the checkpoint")
        .as_sequence()
        < head
    {
        assert!(Instant::now() < deadline, "the projector reaches the head");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The frozen replica wakes after the standby has caught up, applies
    // the rest of its stale batch over a current view, and finds its
    // lease gone at the ack. Wait for that before checking the view, so
    // the check covers its late writes too.
    while replica_stats.lost.load(Ordering::Relaxed) == 0 {
        assert!(
            Instant::now() < deadline,
            "the frozen replica finds its lease gone"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for handle in replicas {
        handle.abort();
    }

    // The read model matches the write side, wallet by wallet, even
    // though two replicas applied some events twice.
    let repository: AggregateRepository<Wallet, _> =
        AggregateRepository::new(nodes[0].store.clone(), RetryPolicy::default());
    let mut total = 0;
    for wallet in 0..WALLETS {
        let written = repository
            .load(WalletId(wallet))
            .await
            .expect("load the wallet")
            .state
            .balance;
        let viewed = nodes[0]
            .views
            .load(VIEW, stream_of(wallet).as_str())
            .await
            .expect("read the view")
            .map_or(0, |row| row.value.balance);
        assert_eq!(viewed, written, "wallet {wallet}: the view matches the log");
        total += written;
    }

    let stats = &replica_stats;
    let applied = stats.applied_total.load(Ordering::Relaxed);
    assert!(
        stats.takeovers.load(Ordering::Relaxed) >= 1,
        "a standby took over"
    );
    assert!(
        applied > committed,
        "the takeover redelivered the unacked batch"
    );
    let per_node: Vec<String> = stats
        .applied
        .iter()
        .enumerate()
        .map(|(node, n)| format!("node {node}: {}", n.load(Ordering::Relaxed)))
        .collect();
    println!(
        "  {committed} events, {applied} applies ({}); {} redelivered and absorbed by the view",
        per_node.join(", "),
        applied - committed,
    );
    println!(
        "  every wallet's view equals its log; {total} units across {WALLETS} wallets; {:.1?}",
        started.elapsed()
    );
}

// -- the two deployments --------------------------------------------

/// Embedded: one SQLite file, one connection the node tasks share, and
/// the in-process lease table.
async fn embedded() {
    let path = std::env::temp_dir().join(format!("eventyr-distributed-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = SqliteStore::<WalletEvent>::open(&path).expect("open the database");
    let checkpoints = SqliteCheckpointStore::beside(&store).expect("checkpoint table");
    let views = SqliteViewStore::<Balance>::new(&store);
    let leases = Arc::new(InMemoryLeaseStore::new());
    let nodes = (0..NODES)
        .map(|id| Node {
            id,
            store: store.clone(),
            checkpoints: checkpoints.clone(),
            leases: leases.clone(),
            views: views.clone(),
            signal: store.clone(),
        })
        .collect();
    run_cluster("embedded (SQLite, one process)", nodes).await;
    let _ = std::fs::remove_file(&path);
}

/// Clustered: Postgres, one pool per node, in a throwaway schema.
async fn clustered(url: &str) {
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    let since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after 1970");
    let schema = format!(
        "distributed_{}_{}",
        std::process::id(),
        since_epoch.as_micros()
    );
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect to EVENTYR_PG_URL");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create the schema");
    let options: PgConnectOptions = url.parse().expect("a Postgres URL");
    let options = options.options([("search_path", schema.as_str())]);

    let mut pools = Vec::new();
    let mut nodes = Vec::new();
    for id in 0..NODES {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options.clone())
            .await
            .expect("connect a node");
        if id == 0 {
            eventyr::postgres::store::migrate(&pool)
                .await
                .expect("migrate");
        }
        let store = PgStore::<WalletEvent>::new(pool.clone());
        nodes.push(Node {
            id,
            checkpoints: PgCheckpointStore::new(&store),
            leases: PgLeaseStore::new(&store),
            views: PgViewStore::<Balance>::new(&store),
            signal: PgCommitSignal::new(&store),
            store,
        });
        pools.push(pool);
    }
    run_cluster("clustered (Postgres, one pool per node)", nodes).await;

    for pool in pools {
        pool.close().await;
    }
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE"
    )))
    .execute(&admin)
    .await
    .expect("drop the schema");
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    embedded().await;
    match std::env::var("EVENTYR_PG_URL") {
        Ok(url) => clustered(&url).await,
        Err(_) => println!("\n(set EVENTYR_PG_URL to run the same nodes on Postgres)"),
    }
}
