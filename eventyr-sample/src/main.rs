//! The miniature bank, served over HTTP.
//!
//! One process composes the sample on the backend `DATABASE_URL` picks —
//! the in-memory store out of the box, the compose-managed Postgres when
//! the variable is set — runs the projection and the saga as its own
//! replica, and serves the edge. Run it directly; the compose-managed
//! deployment in the README repeats it in replicas against the clustered
//! store.
//!
//! ```sh
//! cargo run -p eventyr-sample
//! ```

use anyhow::Context;
use axum::routing::{get, post};
use axum::{Json, Router, extract::Path, extract::State, response::Response};
use serde::Deserialize;
use tokio::signal;
use tokio::sync::watch;

use eventyr::prelude::Version;
use eventyr_sample::domain::{AccountEvent, AccountState, Cash};
use eventyr_sample::features::{
    Balance, Deposit, GetBalance, OpenAccount, OpenedAccount, Outcome, Transfer, Withdraw, respond,
};
use eventyr_sample::wiring::{App, Backend, Composed, Sample, compose, runners};

/// The edge's request body: JSON with the slice's field names. The
/// metadata stamp is empty at the edge; the mediator fills the ids.
#[derive(Debug, Deserialize)]
struct OpenRequestBody {
    account: u64,
    owner: String,
}

/// A money movement's body: the amount only, the account is the path.
#[derive(Debug, Deserialize)]
struct MoneyRequestBody {
    amount: Cash,
}

/// The transfer's body: the two accounts, the amount, and the caller's
/// idempotency key.
#[derive(Debug, Deserialize)]
struct TransferRequestBody {
    from: u64,
    to: u64,
    amount: Cash,
    #[serde(default)]
    idempotency_key: Option<String>,
}

/// The runtime's one config: read from the environment once at `main`,
/// validated before anything binds. Every field is a variable the
/// deployment sets; nothing is read deeper in the process.
#[derive(Debug)]
struct Config {
    /// The tracing filter (`RUST_LOG`), or the sample's own quiet
    /// default.
    log_filter: String,
    /// The edge's listen address (`EVENTYR_SAMPLE_ADDR`).
    listen_addr: std::net::SocketAddr,
    /// The backend to compose (`DATABASE_URL` picked, in-memory
    /// otherwise).
    backend: Backend,
}

impl Config {
    /// The default filter, when `RUST_LOG` is not set.
    const DEFAULT_LOG_FILTER: &'static str = "eventyr_sample=info,tower_http=warn";
    /// The default bind, when `EVENTYR_SAMPLE_ADDR` is not set.
    const DEFAULT_LISTEN_ADDR: &'static str = "0.0.0.0:3000";

    /// Read the environment once; fail fast on a mistyped variable
    /// instead of deep inside `serve`.
    fn from_env() -> anyhow::Result<Self> {
        let log_filter =
            std::env::var("RUST_LOG").unwrap_or_else(|_| Self::DEFAULT_LOG_FILTER.into());
        let listen_addr = std::env::var("EVENTYR_SAMPLE_ADDR")
            .unwrap_or_else(|_| Self::DEFAULT_LISTEN_ADDR.into())
            .parse()
            .context("EVENTYR_SAMPLE_ADDR must be a socket address")?;
        let backend = Backend::from_env();
        #[cfg(not(feature = "postgres"))]
        if matches!(backend, Backend::Memory) && std::env::var_os("DATABASE_URL").is_some() {
            tracing::warn!(
                "DATABASE_URL is set, but this binary was built without the \
                 `postgres` feature; the in-memory store will serve instead"
            );
        }
        Ok(Self {
            log_filter,
            listen_addr,
            backend,
        })
    }
}

/// How long `serve` waits for a runner to stand down after the edge
/// stops before it aborts the task.
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// `POST /accounts` — open an account.
async fn open<Store, Snapshots, Views>(
    State(app): State<App<Store, Snapshots, Views>>,
    Json(body): Json<OpenRequestBody>,
) -> Response
where
    Store: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync + 'static,
    Snapshots: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    Views: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    std::sync::Arc<Store>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<Snapshots>:
        eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
{
    respond(open_account(app, body.account, body.owner).await)
}

/// One account-opening send: the edge's shape for `open`.
async fn open_account<Store, Snapshots, Views>(
    app: App<Store, Snapshots, Views>,
    account: u64,
    owner: String,
) -> Outcome<OpenedAccount>
where
    Store: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync + 'static,
    Snapshots: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    Views: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    std::sync::Arc<Store>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<Snapshots>:
        eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
{
    app.send(OpenAccount {
        account,
        owner,
        stamped: Default::default(),
    })
    .await
}

/// `POST /accounts/{id}/deposits`.
async fn deposit<Store, Snapshots, Views>(
    State(app): State<App<Store, Snapshots, Views>>,
    Path(account): Path<u64>,
    Json(body): Json<MoneyRequestBody>,
) -> Response
where
    Store: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync + 'static,
    Snapshots: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    Views: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    std::sync::Arc<Store>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<Snapshots>:
        eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
{
    respond(move_money(app, account, body.amount, Movement::Deposit).await)
}

/// `POST /accounts/{id}/withdrawals`.
async fn withdraw<Store, Snapshots, Views>(
    State(app): State<App<Store, Snapshots, Views>>,
    Path(account): Path<u64>,
    Json(body): Json<MoneyRequestBody>,
) -> Response
where
    Store: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync + 'static,
    Snapshots: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    Views: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    std::sync::Arc<Store>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<Snapshots>:
        eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
{
    respond(move_money(app, account, body.amount, Movement::Withdraw).await)
}

/// Which way the money moves; the two `MoneyRequestBody` routes share
/// the handling.
#[derive(Clone, Copy)]
enum Movement {
    /// Credit the account.
    Deposit,
    /// Debit the account.
    Withdraw,
}

/// A deposit or a withdrawal, one send: the movement picks the command.
async fn move_money<Store, Snapshots, Views>(
    app: App<Store, Snapshots, Views>,
    account: u64,
    amount: Cash,
    movement: Movement,
) -> Outcome<Version>
where
    Store: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync + 'static,
    Snapshots: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    Views: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    std::sync::Arc<Store>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<Snapshots>:
        eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
{
    match movement {
        Movement::Deposit => {
            app.send(Deposit {
                account,
                amount,
                stamped: Default::default(),
            })
            .await
        }
        Movement::Withdraw => {
            app.send(Withdraw {
                account,
                amount,
                stamped: Default::default(),
            })
            .await
        }
    }
}

/// `POST /transfers`.
async fn transfer<Store, Snapshots, Views>(
    State(app): State<App<Store, Snapshots, Views>>,
    Json(body): Json<TransferRequestBody>,
) -> Response
where
    Store: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync + 'static,
    Snapshots: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    Views: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    std::sync::Arc<Store>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<Snapshots>:
        eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
{
    respond(
        app.send(Transfer {
            from: body.from,
            to: body.to,
            amount: body.amount,
            key: body.idempotency_key,
            stamped: Default::default(),
        })
        .await,
    )
}

/// `GET /accounts/{id}/balance` — the read-your-writes row the
/// projection maintains.
async fn balance<Store, Snapshots, Views>(
    State(app): State<App<Store, Snapshots, Views>>,
    Path(account): Path<u64>,
) -> Response
where
    Store: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync + 'static,
    Snapshots: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    Views: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    std::sync::Arc<Store>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<Snapshots>:
        eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
{
    respond(app.send(GetBalance { account }).await)
}

/// `GET /healthz` — the process is up.
async fn healthz() -> &'static str {
    "ok"
}

/// `GET /readyz` — the process is up; deeper checks are the
/// deployment's concern.
async fn readyz() -> &'static str {
    "ok"
}

/// The edge's routes: one per slice.
fn routes<Store, Snapshots, Views>() -> Router<App<Store, Snapshots, Views>>
where
    Store: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync + 'static,
    Snapshots: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    Views: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    std::sync::Arc<Store>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<Snapshots>:
        eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
{
    Router::new()
        .route("/accounts", post(open))
        .route("/accounts/{id}/deposits", post(deposit))
        .route("/accounts/{id}/withdrawals", post(withdraw))
        .route("/transfers", post(transfer))
        .route("/accounts/{id}/balance", get(balance))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    tracing_subscriber::fmt()
        .with_env_filter(config.log_filter.clone())
        .init();

    match compose(&config.backend).await? {
        Composed::Memory(sample) => serve(sample, config.listen_addr).await,
        #[cfg(feature = "postgres")]
        Composed::Postgres(sample) => serve(sample, config.listen_addr).await,
    }
}

/// Run one composed sample: the projection and the saga as this
/// process's own replicas, and the edge over the mediator, until
/// shutdown.
async fn serve<Store, Snapshots, Checkpoints, Leases, Views, Signal>(
    sample: Sample<Store, Snapshots, Checkpoints, Leases, Views, Signal>,
    listen_addr: std::net::SocketAddr,
) -> anyhow::Result<()>
where
    Store: eventyr::store::prelude::EventStore<Event = AccountEvent>
        + eventyr::store::prelude::StreamsAll<Event = AccountEvent>
        + Send
        + Sync
        + 'static,
    Snapshots: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    Checkpoints: eventyr::subscription::prelude::CheckpointStore + Send + Sync + 'static,
    Leases: eventyr::subscription::prelude::ProjectorLease + Send + Sync + 'static,
    Views: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    Signal: eventyr::subscription::prelude::CommitSignal + Send + Sync + 'static,
    std::sync::Arc<Store>: eventyr::store::prelude::StreamsAll<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<Store>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<Snapshots>:
        eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
    std::sync::Arc<Signal>: eventyr::subscription::prelude::CommitSignal + Send + Sync,
{
    let shutdown = sample.shutdown.clone();
    let app = sample.app.clone();

    let projection = tokio::spawn({
        let inputs = sample.inputs.clone();
        let shutdown = sample.shutdown.clone();
        async move { runners::run_projection(&inputs, &shutdown).await }
    });
    let saga = tokio::spawn({
        let inputs = sample.inputs.clone();
        let app = sample.app.clone();
        let shutdown = sample.shutdown.clone();
        async move { runners::run_saga(&inputs, &app, &shutdown).await }
    });

    let server = axum::serve(
        tokio::net::TcpListener::bind(listen_addr)
            .await
            .context("bind the edge")?,
        routes().with_state(app),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal(shutdown).await;
    });

    server.await.context("serve the edge")?;

    let _ = sample.shutdown.send(true);
    let _ = tokio::time::timeout(SHUTDOWN_GRACE, projection).await;
    let _ = tokio::time::timeout(SHUTDOWN_GRACE, saga).await;
    Ok(())
}

/// SIGINT or SIGTERM — the edge's graceful shutdown.
async fn shutdown_signal(shutdown: watch::Sender<bool>) {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("install Ctrl-C");
    };
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM")
            .recv()
            .await;
    };
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    let _ = shutdown.send(true);
}
