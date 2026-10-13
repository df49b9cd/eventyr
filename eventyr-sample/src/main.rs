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
use axum::{Json, Router, extract::Path, extract::State, http::StatusCode, response::IntoResponse};
use serde::Deserialize;
use tokio::signal;
use tokio::sync::watch;

use eventyr_sample::domain::{AccountEvent, AccountState, Cash};
use eventyr_sample::features::{
    ApiError, Balance, Deposit, GetBalance, OpenAccount, Outcome, Transfer, Withdraw,
};
use eventyr_sample::wiring::{App, Backend, Composed, Sample, compose, runners};

/// The edge's request body: JSON with the slice's field names. The
/// metadata stamp is empty at the edge; the mediator fills the ids.
#[derive(Debug, Deserialize)]
struct OpenRequestBody {
    account: u64,
    owner: String,
}

#[derive(Debug, Deserialize)]
struct MoneyRequestBody {
    amount: Cash,
}

#[derive(Debug, Deserialize)]
struct TransferRequestBody {
    from: u64,
    to: u64,
    amount: Cash,
    #[serde(default)]
    idempotency_key: Option<String>,
}

/// Wrap a succeeded command: 200 with the outcome.
#[derive(serde::Serialize)]
struct OkJson<T> {
    ok: T,
}

impl<T> OkJson<T> {
    fn new(value: T) -> Self {
        Self { ok: value }
    }
}

/// Wrap a failure the edge maps: the status, and the message.
#[derive(serde::Serialize)]
struct ErrJson {
    error: String,
}

/// Whatever a command answers: 200 with the outcome, or the edge-mapped
/// failure.
fn map_outcome<T: serde::Serialize>(result: Outcome<T>) -> impl IntoResponse {
    match result {
        Ok(ok) => (StatusCode::OK, Json(OkJson::new(ok))).into_response(),
        Err(ApiError::Domain(rejection)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ErrJson { error: rejection }),
        )
            .into_response(),
        Err(ApiError::Conflict) => (
            StatusCode::CONFLICT,
            Json(ErrJson {
                error: "conflict: re-issue the command".into(),
            }),
        )
            .into_response(),
        Err(ApiError::Unavailable) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrJson {
                error: "the store is unavailable: retry later".into(),
            }),
        )
            .into_response(),
    }
}

/// `POST /accounts` — open an account.
async fn open<S, SS, V>(
    State(app): State<App<S, SS, V>>,
    Json(body): Json<OpenRequestBody>,
) -> impl IntoResponse
where
    S: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync + 'static,
    SS: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    V: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    std::sync::Arc<S>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<SS>: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
{
    map_outcome(
        app.send(OpenAccount {
            account: body.account,
            owner: body.owner,
            stamped: Default::default(),
        })
        .await,
    )
}

/// `POST /accounts/{id}/deposits`.
async fn deposit<S, SS, V>(
    State(app): State<App<S, SS, V>>,
    Path(account): Path<u64>,
    Json(body): Json<MoneyRequestBody>,
) -> impl IntoResponse
where
    S: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync + 'static,
    SS: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    V: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    std::sync::Arc<S>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<SS>: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
{
    map_outcome(
        app.send(Deposit {
            account,
            amount: body.amount,
            stamped: Default::default(),
        })
        .await,
    )
}

/// `POST /accounts/{id}/withdrawals`.
async fn withdraw<S, SS, V>(
    State(app): State<App<S, SS, V>>,
    Path(account): Path<u64>,
    Json(body): Json<MoneyRequestBody>,
) -> impl IntoResponse
where
    S: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync + 'static,
    SS: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    V: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    std::sync::Arc<S>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<SS>: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
{
    map_outcome(
        app.send(Withdraw {
            account,
            amount: body.amount,
            stamped: Default::default(),
        })
        .await,
    )
}

/// `POST /transfers`.
async fn transfer<S, SS, V>(
    State(app): State<App<S, SS, V>>,
    Json(body): Json<TransferRequestBody>,
) -> impl IntoResponse
where
    S: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync + 'static,
    SS: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    V: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    std::sync::Arc<S>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<SS>: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
{
    map_outcome(
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
async fn balance<S, SS, V>(
    State(app): State<App<S, SS, V>>,
    Path(account): Path<u64>,
) -> impl IntoResponse
where
    S: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync + 'static,
    SS: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    V: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    std::sync::Arc<S>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<SS>: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
{
    map_outcome(app.send(GetBalance { account }).await)
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "eventyr_sample=info,tower_http=warn".into()),
        )
        .init();

    let backend = Backend::from_env();
    match compose(&backend).await? {
        Composed::Memory(sample) => serve(sample).await,
        #[cfg(feature = "postgres")]
        Composed::Postgres(sample) => serve(sample).await,
    }
}

/// Run one composed sample: the projection and the saga as this
/// process's own replicas, and the edge over the mediator, until
/// shutdown.
async fn serve<S, SS, C, L, V, W>(sample: Sample<S, SS, C, L, V, W>) -> anyhow::Result<()>
where
    S: eventyr::store::prelude::EventStore<Event = AccountEvent>
        + eventyr::store::prelude::StreamsAll<Event = AccountEvent>
        + Send
        + Sync
        + 'static,
    SS: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync + 'static,
    C: eventyr::subscription::prelude::CheckpointStore + Send + Sync + 'static,
    L: eventyr::subscription::prelude::ProjectorLease + Send + Sync + 'static,
    V: eventyr::projection::prelude::ViewStore<Balance> + Send + Sync + 'static,
    W: eventyr::subscription::prelude::CommitSignal + Send + Sync + 'static,
    std::sync::Arc<S>: eventyr::store::prelude::StreamsAll<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<S>: eventyr::store::prelude::EventStore<Event = AccountEvent> + Send + Sync,
    std::sync::Arc<SS>: eventyr::store::prelude::SnapshotStore<State = AccountState> + Send + Sync,
    std::sync::Arc<W>: eventyr::subscription::prelude::CommitSignal + Send + Sync,
{
    let shutdown = sample.shutdown.clone();
    let app = sample.app.clone();

    let projection = tokio::spawn({
        let inputs = sample.inputs.clone();
        let shutdown = sample.shutdown.clone();
        async move { runners::run_projection_once(&inputs, &shutdown).await }
    });
    let saga = tokio::spawn({
        let inputs = sample.inputs.clone();
        let app = sample.app.clone();
        let shutdown = sample.shutdown.clone();
        async move { runners::run_saga(&inputs, &app, &shutdown).await }
    });

    let server = axum::serve(
        tokio::net::TcpListener::bind(
            std::env::var("EVENTYR_SAMPLE_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".into()),
        )
        .await
        .context("bind the edge")?,
        Router::new()
            .route("/accounts", post(open))
            .route("/accounts/{id}/deposits", post(deposit))
            .route("/accounts/{id}/withdrawals", post(withdraw))
            .route("/transfers", post(transfer))
            .route("/accounts/{id}/balance", get(balance))
            .route("/healthz", get(healthz))
            .route("/readyz", get(readyz))
            .with_state(app),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal(shutdown).await;
    });

    server.await.context("serve the edge")?;

    projection.abort();
    saga.abort();
    let _ = projection.await;
    let _ = saga.await;
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
