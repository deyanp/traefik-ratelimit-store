use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use traefik_ratelimit_store::app_env::AppEnv;
use traefik_ratelimit_store::peers::PeerTable;
use traefik_ratelimit_store::script::ScriptRegistry;
use traefik_ratelimit_store::store::BucketStore;
use traefik_ratelimit_store::{logging, mesh, server};

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

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    logging::init();

    let env_vars: HashMap<String, String> = std::env::vars().collect();
    let app_env = AppEnv::create(&env_vars);

    let store = Arc::new(BucketStore::new(app_env.store));
    let peers = Arc::new(PeerTable::new(app_env.peer_staleness_limit));
    let scripts = Arc::new(ScriptRegistry::new());

    spawn_sweeper(store.clone());

    // The peer endpoint and the publisher are only useful together, and a replica with
    // neither is still correct — it simply counts alone.
    let peer_endpoint = tokio::spawn({
        let peers = peers.clone();
        let replica_id = app_env.replica_id.clone();
        let address = app_env.peer_listen_address.clone();
        async move {
            if let Err(error) = mesh::run_peer_endpoint(peers, replica_id, &address).await {
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
