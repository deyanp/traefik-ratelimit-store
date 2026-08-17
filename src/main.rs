use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use traefik_ratelimit_store::app_env::AppEnv;
use traefik_ratelimit_store::health::Health;
use traefik_ratelimit_store::peers::PeerTable;
use traefik_ratelimit_store::script::ScriptRegistry;
use traefik_ratelimit_store::store::BucketStore;
use traefik_ratelimit_store::{logging, mesh, server};

/// How long to keep serving after readiness starts failing.
///
/// Long enough for the orchestrator to notice and withdraw this replica from its
/// endpoints. Exiting before that would drop connections the orchestrator is still
/// sending, which is the failure the grace period exists to avoid.
const DRAIN_PERIOD: Duration = Duration::from_secs(5);

/// Reclaims expired entries on a timer, so memory is returned whether or not traffic
/// keeps arriving. One task for the process, living as long as the store does.
fn spawn_sweeper(store: Arc<BucketStore>) {
    let interval = store.config().sweep_interval;

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            store.sweep_expired(Instant::now());
        }
    });
}

/// Turns a termination signal into an orderly withdrawal.
///
/// Readiness fails first and the process keeps serving, so the orchestrator stops sending
/// new connections while this replica finishes the ones it holds. Only then does it exit.
/// A replica that exits immediately drops requests that were already in flight toward it.
async fn drain_on_termination(health: Arc<Health>) {
    let mut terminate = match tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::terminate(),
    ) {
        Ok(signal) => signal,
        Err(error) => {
            tracing::warn!(error = %error, "cannot listen for termination; shutdown will be abrupt");
            return;
        }
    };

    tokio::select! {
        _ = terminate.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }

    health.begin_draining();
    tracing::info!(
        drain_seconds = DRAIN_PERIOD.as_secs(),
        "termination requested; failing readiness and draining"
    );

    tokio::time::sleep(DRAIN_PERIOD).await;
    tracing::info!("drained; exiting");
    std::process::exit(0);
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    logging::init();

    let env_vars: HashMap<String, String> = std::env::vars().collect();
    let app_env = AppEnv::create(&env_vars);

    let store = Arc::new(BucketStore::new(app_env.store));
    let peers = Arc::new(PeerTable::new(app_env.peer_staleness_limit));
    let scripts = Arc::new(ScriptRegistry::new());
    let health = Arc::new(Health::new(app_env.listen_address.clone()));

    spawn_sweeper(store.clone());
    // Independent of the publisher, so the ring rotates even when this replica has no
    // peers to publish to.
    tokio::spawn(mesh::run_consumption_ticker(
        store.clone(),
        app_env.peer_publish_interval,
    ));
    tokio::spawn(drain_on_termination(health.clone()));

    // The peer endpoint and the publisher are only useful together, and a replica with
    // neither is still correct — it simply counts alone.
    let peer_endpoint = tokio::spawn({
        let peers = peers.clone();
        let replica_id = app_env.replica_id.clone();
        let health = health.clone();
        let address = app_env.peer_listen_address.clone();
        async move {
            if let Err(error) = mesh::run_peer_endpoint(peers, replica_id, health, &address).await {
                tracing::error!(error = %error, "peer endpoint stopped");
            }
        }
    });

    tokio::spawn(mesh::run_publisher(
        store.clone(),
        peers.clone(),
        app_env.clone(),
    ));

    // Serving the protocol is this process's whole job, so a failure here is fatal and
    // the orchestrator should replace the pod rather than leave it running dead.
    if let Err(error) = server::run(store, peers, scripts, &app_env.listen_address).await {
        tracing::error!(error = %error, "{} stopped", app_env.app_name);
        peer_endpoint.abort();
        std::process::exit(1);
    }
}
