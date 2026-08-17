//! The peer mesh: a full broadcast, not a gossip protocol.
//!
//! Epidemic protocols contact a random subset of peers and rely on transitive spread over
//! several rounds, which exists because `N²` is prohibitive at large `N`. At a handful of
//! replicas a full mesh is both simpler and lower latency — one hop rather than several
//! rounds — so every replica sends its own report to every peer, every interval.
//!
//! Nothing here is specific to one orchestrator. Peers are named by a DNS name that
//! resolves to all of them, or by an explicit list.

use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::app_env::AppEnv;
use crate::errors::TechnicalError;
use crate::health::Health;
use crate::log_events;
use crate::peers::{PeerReport, PeerTable};
use crate::store::BucketStore;

/// Where a replica accepts its peers' reports.
const REPORT_PATH: &str = "/peer-report";

/// Turns the configured endpoint into the addresses to publish to.
///
/// A comma-separated value is taken as an explicit list; anything else is resolved, which
/// under an orchestrator means a headless service returning one address per replica.
fn resolve_peers(peer_endpoint: &str) -> Vec<String> {
    if peer_endpoint.trim().is_empty() {
        return Vec::new();
    }

    if peer_endpoint.contains(',') {
        return peer_endpoint
            .split(',')
            .map(|entry| entry.trim().to_string())
            .filter(|entry| !entry.is_empty())
            .collect();
    }

    match peer_endpoint.to_socket_addrs() {
        Ok(addresses) => addresses.map(|address| address.to_string()).collect(),
        Err(_) => Vec::new(),
    }
}

/// Publishes this replica's consumption to every peer, once.
///
/// Failures are dropped rather than retried: a retry would deliver a stale report after a
/// fresher one is already due, and a peer that hears nothing simply ages out of the window.
async fn publish_once(client: &reqwest::Client, store: &BucketStore, app_env: &AppEnv) -> usize {
    let peers = resolve_peers(&app_env.peer_endpoint);
    if peers.is_empty() {
        return 0;
    }

    let report = PeerReport::new(
        app_env.replica_id.clone(),
        &store.collect_consumption(app_env.peer_max_keys_per_report),
    );

    // Spawned rather than awaited in turn, so one slow peer cannot delay the others.
    // Each task owns its inputs, and the client's timeout bounds how long it can linger.
    let mut deliveries = tokio::task::JoinSet::new();
    for peer in peers {
        let client = client.clone();
        let report = report.clone();
        let url = format!("http://{peer}{REPORT_PATH}");
        let secret = app_env.peer_shared_secret.clone();

        deliveries.spawn(async move {
            let mut request = client.post(&url).json(&report);
            if !secret.is_empty() {
                request = request.bearer_auth(secret);
            }
            request.send().await.is_ok()
        });
    }

    let mut delivered = 0;
    while let Some(outcome) = deliveries.join_next().await {
        if outcome.unwrap_or(false) {
            delivered += 1;
        }
    }

    delivered
}

/// Advances the consumption ring on a timer, forever, whether or not peers exist.
///
/// Rotation used to be a side effect of publishing, which meant a replica running alone
/// never rotated: its per-key counters accumulated for the lifetime of each key. Nothing
/// read them, so nothing broke — but an unbounded counter that is only correct because
/// nobody looks at it is a defect waiting for its first reader.
pub async fn run_consumption_ticker(store: Arc<BucketStore>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        store.advance_tick();
    }
}

/// Runs the publish loop until the process ends.
pub async fn run_publisher(store: Arc<BucketStore>, peers: Arc<PeerTable>, app_env: AppEnv) {
    if app_env.peer_endpoint.trim().is_empty() {
        let (event_id, event_name) = log_events::PEER_DISCOVERY_EMPTY;
        tracing::info!(
            event_id,
            event_name,
            "no peer endpoint configured; counting alone"
        );
        return;
    }

    let client = reqwest::Client::builder()
        .timeout(app_env.peer_request_timeout)
        .build()
        .expect("failed to build the peer HTTP client");

    let mut ticker = tokio::time::interval(app_env.peer_publish_interval);
    loop {
        ticker.tick().await;

        let expected = resolve_peers(&app_env.peer_endpoint).len();
        let delivered = publish_once(&client, &store, &app_env).await;

        if expected > 0 && delivered < expected {
            let (event_id, event_name) = log_events::PEER_PUBLISH_FAILED;
            tracing::debug!(
                event_id,
                event_name,
                delivered,
                expected,
                "some peer reports were not delivered"
            );
        }

        peers.evict_stale(Instant::now());
    }
}

#[derive(Clone)]
struct PeerEndpointState {
    peers: Arc<PeerTable>,
    replica_id: String,
    health: Arc<Health>,
    /// Empty means unauthenticated, and the endpoint then relies entirely on network
    /// policy to keep strangers out.
    shared_secret: Arc<String>,
}

/// Compares two secrets without leaking their similarity through timing.
///
/// The exposure here is small — an attacker inside the cluster has better options — but a
/// comparison that returns early is a habit worth not forming.
fn secrets_match(presented: &str, expected: &str) -> bool {
    if presented.len() != expected.len() {
        return false;
    }

    presented
        .bytes()
        .zip(expected.bytes())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

/// Whether the caller may submit a report.
///
/// An unauthenticated endpoint accepts anything the network lets through. That is a
/// deliberate default rather than an oversight: inflated counts make this replica *more*
/// restrictive, so the worst a stranger achieves is throttling traffic — bad, but not a
/// bypass — and requiring a secret by default would leave the mesh silently broken for
/// anyone who deployed without one.
fn is_authorised(headers: &axum::http::HeaderMap, shared_secret: &str) -> bool {
    if shared_secret.is_empty() {
        return true;
    }

    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|presented| secrets_match(presented, shared_secret))
}

async fn receive_report(
    State(state): State<PeerEndpointState>,
    headers: axum::http::HeaderMap,
    Json(report): Json<PeerReport>,
) -> StatusCode {
    if !is_authorised(&headers, &state.shared_secret) {
        return StatusCode::UNAUTHORIZED;
    }

    state
        .peers
        .record(&state.replica_id, report, Instant::now());
    StatusCode::ACCEPTED
}

/// Whether this replica should be restarted. Deliberately trivial: anything that can
/// restart a pod should be as close to "the process exists" as possible, because a clever
/// liveness probe causes restart loops under exactly the load it was meant to survive.
async fn liveness() -> &'static str {
    "alive"
}

/// Whether this replica should be sent traffic.
///
/// Answers for the protocol port rather than for itself: a replica whose protocol listener
/// has stopped but whose peer endpoint still answers must leave the rotation, and a probe
/// that only proves it can answer its own probe proves nothing.
async fn readiness(State(state): State<PeerEndpointState>) -> Response {
    if state.health.is_draining() {
        // Said before the listener closes, so the orchestrator withdraws this replica
        // while it can still finish the connections it already holds.
        return (StatusCode::SERVICE_UNAVAILABLE, "draining").into_response();
    }

    if state.health.protocol_listener_answers().await {
        (StatusCode::OK, "ready").into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "protocol listener is not answering",
        )
            .into_response()
    }
}

/// Serves the peer endpoint until the process ends.
pub async fn run_peer_endpoint(
    peers: Arc<PeerTable>,
    replica_id: String,
    health: Arc<Health>,
    shared_secret: String,
    listen_address: &str,
) -> Result<(), TechnicalError> {
    if shared_secret.is_empty() {
        let (event_id, event_name) = log_events::PEER_ENDPOINT_UNAUTHENTICATED;
        tracing::warn!(
            event_id,
            event_name,
            "the peer endpoint accepts reports from anyone the network allows; \
             set PEER_SHARED_SECRET, or confine it with a network policy"
        );
    }

    let app = Router::new()
        .route(REPORT_PATH, post(receive_report))
        // A report is bounded by PEER_MAX_KEYS_PER_REPORT, so anything far larger is not
        // a peer. Rejecting it early keeps a stranger from making this replica allocate.
        .layer(axum::extract::DefaultBodyLimit::max(4 * 1024 * 1024))
        .route("/health", get(liveness))
        .route("/readiness", get(readiness))
        .with_state(PeerEndpointState {
            peers,
            replica_id,
            health,
            shared_secret: Arc::new(shared_secret),
        });

    let listener = tokio::net::TcpListener::bind(listen_address)
        .await
        .map_err(|e| {
            TechnicalError(format!(
                "failed to bind the peer endpoint {listen_address}: {e}"
            ))
        })?;

    axum::serve(listener, app)
        .await
        .map_err(|e| TechnicalError(format!("peer endpoint stopped: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};

    fn headers_with(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_str(value).unwrap());
        headers
    }

    #[test]
    fn an_unset_secret_accepts_anything() {
        assert!(is_authorised(&HeaderMap::new(), ""));
        assert!(is_authorised(&headers_with("Bearer whatever"), ""));
    }

    #[test]
    fn a_set_secret_is_required_and_must_match() {
        assert!(is_authorised(&headers_with("Bearer correct"), "correct"));
        assert!(!is_authorised(&headers_with("Bearer wrong"), "correct"));
        assert!(!is_authorised(&HeaderMap::new(), "correct"));
    }

    #[test]
    fn a_secret_without_the_bearer_scheme_is_refused() {
        assert!(!is_authorised(&headers_with("correct"), "correct"));
        assert!(!is_authorised(&headers_with("Basic correct"), "correct"));
    }

    #[test]
    fn secrets_of_different_lengths_never_match() {
        assert!(!secrets_match("short", "much-longer-secret"));
        assert!(!secrets_match("", "secret"));
        assert!(secrets_match("same", "same"));
    }

    #[test]
    fn an_empty_endpoint_yields_no_peers() {
        assert!(resolve_peers("").is_empty());
        assert!(resolve_peers("   ").is_empty());
    }

    #[test]
    fn a_comma_separated_value_is_taken_as_a_list() {
        let peers = resolve_peers("10.0.0.1:8080, 10.0.0.2:8080 ,10.0.0.3:8080");

        assert_eq!(
            peers,
            vec!["10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080"]
        );
    }

    #[test]
    fn a_resolvable_name_yields_its_addresses() {
        // localhost resolves everywhere, which is enough to prove the resolution path.
        let peers = resolve_peers("localhost:8080");

        assert!(!peers.is_empty());
        assert!(peers.iter().all(|peer| peer.ends_with(":8080")));
    }

    #[test]
    fn an_unresolvable_name_yields_no_peers_rather_than_failing() {
        // A replica that cannot find its peers must keep serving, counting alone.
        assert!(resolve_peers("no-such-host.invalid:8080").is_empty());
    }
}
