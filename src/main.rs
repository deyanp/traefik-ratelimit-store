use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinSet;

use traefik_ratelimit_store::app_env::AppEnv;
use traefik_ratelimit_store::health::Health;
use traefik_ratelimit_store::script::ScriptRegistry;
use traefik_ratelimit_store::store::BucketStore;
use traefik_ratelimit_store::{log_events, logging, mesh, server};

/// Reclaims expired entries on a timer, so memory is returned whether or not traffic
/// keeps arriving. One task for the process, living as long as the store does.
async fn run_sweeper(store: Arc<BucketStore>) {
    let mut ticker = tokio::time::interval(store.config().sweep_interval);
    loop {
        ticker.tick().await;
        store.sweep_expired(Instant::now());
    }
}

/// Turns a termination signal into an orderly withdrawal.
///
/// Readiness fails first and the process keeps serving for `drain_period`, so the
/// orchestrator stops sending new connections while this replica finishes the ones it
/// holds. Only then does it exit. A replica that exits immediately drops requests that
/// were already in flight toward it. The period must be long enough for the withdrawal
/// to propagate — and shorter than the orchestrator's termination grace period.
async fn drain_on_termination(health: Arc<Health>, drain_period: Duration) {
    let mut terminate = match tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::terminate(),
    ) {
        Ok(signal) => signal,
        Err(error) => {
            tracing::warn!(error = %error, "cannot listen for termination; shutdown will be abrupt");
            return;
        }
    };

    // Either signal starts the drain. A handler that could not be installed resolves
    // immediately with an error, which must not be mistaken for the signal itself.
    tokio::select! {
        _ = terminate.recv() => {}
        Ok(()) = tokio::signal::ctrl_c() => {}
    }

    health.begin_draining();
    tracing::info!(
        drain_seconds = drain_period.as_secs_f64(),
        "termination requested; failing readiness and draining"
    );

    tokio::time::sleep(drain_period).await;
    tracing::info!("drained; exiting");
    std::process::exit(0);
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    logging::init();

    let env_vars: HashMap<String, String> = std::env::vars().collect();
    let app_env = AppEnv::create(&env_vars);

    let (event_id, event_name) = log_events::STORE_CAPACITY_DERIVED;
    tracing::info!(
        event_id,
        event_name,
        shards = app_env.store.shard_count,
        entries_per_shard = app_env.store.capacity_per_shard,
        total_entries = app_env.store.shard_count * app_env.store.capacity_per_shard,
        table_bytes = traefik_ratelimit_store::memory_budget::compute_table_bytes(
            app_env.store.capacity_per_shard,
            app_env.store.shard_count,
        ),
        "entry ceiling sized against the memory budget"
    );

    let store = Arc::new(BucketStore::new(app_env.store));
    let scripts = Arc::new(ScriptRegistry::new());
    let health = Arc::new(Health::new(app_env.listen_address.clone()));

    tokio::spawn(drain_on_termination(health.clone(), app_env.drain_period));

    // Every task here must run for the life of the process. They are supervised together:
    // a sweeper that has died leaves memory to grow, a publisher that has died leaves the
    // mesh silently counting alone, and neither shows in any probe — so the first one to
    // stop, for any reason, ends the process and lets the orchestrator replace it.
    let mut background = JoinSet::new();
    background.spawn(run_sweeper(store.clone()));
    background.spawn(mesh::run_publisher(store.clone(), app_env.clone()));
    background.spawn({
        let store = store.clone();
        let health = health.clone();
        let app_env = app_env.clone();
        async move {
            if let Err(error) = mesh::run_peer_endpoint(store, health, &app_env).await {
                tracing::error!(error = %error, "peer endpoint stopped");
            }
        }
    });

    // Serving the protocol is this process's whole job, so a failure here is fatal and
    // the orchestrator should replace the pod rather than leave it running dead.
    tokio::select! {
        served = server::run(
            store,
            scripts,
            &app_env.listen_address,
            app_env.max_connections,
            app_env.connection_idle_timeout,
        ) => {
            if let Err(error) = served {
                tracing::error!(error = %error, "{} stopped", app_env.app_name);
            }
        }
        stopped = background.join_next() => {
            let (event_id, event_name) = log_events::BACKGROUND_TASK_STOPPED;
            match stopped {
                Some(Err(error)) => tracing::error!(event_id, event_name, error = %error, "a background task panicked"),
                _ => tracing::error!(event_id, event_name, "a background task stopped"),
            }
        }
    }

    background.abort_all();
    std::process::exit(1);
}
