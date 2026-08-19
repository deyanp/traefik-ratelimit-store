//! The peer mesh: a full broadcast, not a gossip protocol.
//!
//! Epidemic protocols contact a random subset of peers and rely on transitive spread over
//! several rounds, which exists because `N²` is prohibitive at large `N`. At a handful of
//! replicas a full mesh is both simpler and lower latency — one hop rather than several
//! rounds — so every replica sends its own report to every peer, every interval.
//!
//! Nothing here is specific to one orchestrator. Peers are named by a DNS name that
//! resolves to all of them, or by an explicit list.

use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};

use crate::app_env::AppEnv;
use crate::errors::TechnicalError;
use crate::health::Health;
use crate::log_events;
use crate::peers::{self, PeerReport};
use crate::store::BucketStore;

/// Where a replica accepts its peers' reports.
const REPORT_PATH: &str = "/peer-report";

/// Generous upper bound on one serialised report line, used to size the body limit from
/// the configured key cap so a stranger cannot make this replica buffer more than a peer
/// could legitimately send.
const BYTES_PER_REPORT_LINE: usize = 160;

/// Turns the configured endpoint into the addresses to publish to.
///
/// A comma-separated value is taken as an explicit list; anything else is resolved, which
/// under an orchestrator means a headless service returning one address per replica.
/// Resolution is asynchronous: a slow resolver must never hold a runtime worker, because
/// the same workers serve the protocol connections.
async fn resolve_peers(peer_endpoint: &str) -> Vec<String> {
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

    match tokio::net::lookup_host(peer_endpoint).await {
        Ok(addresses) => addresses.map(|address| address.to_string()).collect(),
        Err(_) => Vec::new(),
    }
}

/// How one round of publishing went, per peer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Delivery {
    /// The peer took the report.
    accepted: usize,
    /// The peer answered, and refused: wrong secret, or a body it will not take.
    rejected: usize,
    /// No usable answer: unreachable, timed out, or the connection failed.
    failed: usize,
}

/// Publishes `report` to every peer, once.
///
/// Failures are dropped rather than retried: a retry would deliver a stale report after a
/// fresher one is already due, and a peer that hears nothing is simply behind by one
/// interval of this replica's admissions.
async fn publish_once(
    client: &reqwest::Client,
    peers: &[String],
    report: &PeerReport,
    shared_secret: &str,
) -> Delivery {
    // Serialised once; each delivery shares the bytes.
    let body = match serde_json::to_vec(report) {
        Ok(body) => Bytes::from(body),
        Err(_) => return Delivery::default(),
    };

    // Spawned rather than awaited in turn, so one slow peer cannot delay the others.
    // Each task owns its inputs, and the client's timeout bounds how long it can linger.
    let mut deliveries = tokio::task::JoinSet::new();
    for peer in peers {
        let client = client.clone();
        let body = body.clone();
        let url = format!("http://{peer}{REPORT_PATH}");
        let secret = shared_secret.to_string();

        deliveries.spawn(async move {
            let mut request = client
                .post(&url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body);
            if !secret.is_empty() {
                request = request.bearer_auth(secret);
            }
            request
                .send()
                .await
                .map(|response| response.status().is_success())
        });
    }

    let mut delivery = Delivery::default();
    while let Some(outcome) = deliveries.join_next().await {
        match outcome {
            Ok(Ok(true)) => delivery.accepted += 1,
            Ok(Ok(false)) => delivery.rejected += 1,
            _ => delivery.failed += 1,
        }
    }

    delivery
}

/// What the publisher last told the operator, so each condition is reported when it
/// starts and when it clears rather than on every interval.
#[derive(Debug, Default)]
struct PublishHealth {
    unresolved: bool,
    rejected: bool,
    unreachable: bool,
}

impl PublishHealth {
    fn note_resolution(&mut self, peer_count: usize) {
        if peer_count == 0 && !self.unresolved {
            let (event_id, event_name) = log_events::PEER_DISCOVERY_EMPTY;
            tracing::warn!(
                event_id,
                event_name,
                "peer endpoint resolved to no addresses; counting alone until it does"
            );
        } else if peer_count > 0 && self.unresolved {
            tracing::info!(peers = peer_count, "peer endpoint resolves again");
        }
        self.unresolved = peer_count == 0;
    }

    fn note_delivery(&mut self, expected: usize, delivery: Delivery) {
        if delivery.rejected > 0 && !self.rejected {
            let (event_id, event_name) = log_events::PEER_PUBLISH_REJECTED;
            tracing::warn!(
                event_id,
                event_name,
                rejected = delivery.rejected,
                expected,
                "peers are refusing this replica's reports; check PEER_SHARED_SECRET \
                 matches on every replica and PEER_MAX_KEYS_PER_REPORT is the same everywhere"
            );
        } else if delivery.rejected == 0 && self.rejected {
            tracing::info!("peers accept this replica's reports again");
        }
        self.rejected = delivery.rejected > 0;

        let unreachable = expected > 0 && delivery.accepted == 0 && delivery.rejected == 0;
        if unreachable && !self.unreachable {
            let (event_id, event_name) = log_events::PEER_PUBLISH_FAILED;
            tracing::warn!(
                event_id,
                event_name,
                expected,
                "no peer could be reached; counting alone until one answers"
            );
        } else if !unreachable && self.unreachable {
            tracing::info!(accepted = delivery.accepted, "peers reachable again");
        }
        self.unreachable = unreachable;
    }
}

/// Runs the publish loop until the process ends.
///
/// Never returns: a replica without a peer endpoint logs that it counts alone and then
/// idles, so the caller can treat any return as the failure it would be.
pub async fn run_publisher(store: Arc<BucketStore>, app_env: AppEnv) {
    if app_env.peer_endpoint.trim().is_empty() {
        let (event_id, event_name) = log_events::PEER_DISCOVERY_EMPTY;
        tracing::info!(
            event_id,
            event_name,
            "no peer endpoint configured; counting alone"
        );
        std::future::pending::<()>().await;
    }

    let client = reqwest::Client::builder()
        .timeout(app_env.peer_request_timeout)
        .build()
        .expect("failed to build the peer HTTP client");

    let mut health = PublishHealth::default();
    let mut ticker = tokio::time::interval(app_env.peer_publish_interval);
    loop {
        ticker.tick().await;

        // Resolved once per interval and used for the whole round.
        let peers = resolve_peers(&app_env.peer_endpoint).await;
        health.note_resolution(peers.len());
        if peers.is_empty() {
            continue;
        }

        let lines = store.collect_report(app_env.peer_max_keys_per_report, Instant::now());
        if lines.is_empty() {
            // Nothing admitted since the last report; peers have nothing to fold.
            continue;
        }

        let report = PeerReport::new(app_env.replica_id.clone(), &lines);
        let delivery = publish_once(&client, &peers, &report, &app_env.peer_shared_secret).await;
        health.note_delivery(peers.len(), delivery);
    }
}

#[derive(Clone)]
struct PeerEndpointState {
    store: Arc<BucketStore>,
    replica_id: String,
    health: Arc<Health>,
    /// Empty means unauthenticated, and the endpoint then relies entirely on network
    /// policy to keep strangers out.
    shared_secret: Arc<String>,
    /// Most keys one inbound report may carry; the same cap the publisher applies.
    max_keys_per_report: usize,
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
/// With no secret configured the endpoint accepts anything the network lets through. That
/// is not a safe default, and startup refuses it unless the operator opts in explicitly
/// (`PEER_ALLOW_UNAUTHENTICATED`): a stranger's report is folded into this replica's
/// buckets like any peer's, so inflated admissions throttle a key and a claimed far-future
/// timestamp refills a drained one — a bypass, not a nuisance. Network policy is the other
/// half of the defence either way.
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

/// Folds a peer's report into this replica's buckets.
///
/// Authorisation is checked before the body is parsed, so an unauthorised caller costs
/// a header lookup and nothing more.
async fn receive_report(
    State(state): State<PeerEndpointState>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> StatusCode {
    if !is_authorised(&headers, &state.shared_secret) {
        return StatusCode::UNAUTHORIZED;
    }

    let report: PeerReport = match serde_json::from_slice(&body) {
        Ok(report) => report,
        Err(error) => {
            let (event_id, event_name) = log_events::PEER_REPORT_REFUSED;
            tracing::debug!(event_id, event_name, error = %error, "malformed peer report");
            return StatusCode::BAD_REQUEST;
        }
    };

    let admissions =
        match peers::decode_report(report, &state.replica_id, state.max_keys_per_report) {
            Ok(admissions) => admissions,
            Err(error) => {
                let (event_id, event_name) = log_events::PEER_REPORT_REFUSED;
                tracing::debug!(event_id, event_name, error = %error, "peer report refused");
                return StatusCode::PAYLOAD_TOO_LARGE;
            }
        };

    let now = Instant::now();
    for (key, peer) in admissions {
        state.store.apply_peer_admissions(key, peer, now);
    }
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
    store: Arc<BucketStore>,
    health: Arc<Health>,
    app_env: &AppEnv,
) -> Result<(), TechnicalError> {
    if app_env.peer_shared_secret.is_empty() {
        let (event_id, event_name) = log_events::PEER_ENDPOINT_UNAUTHENTICATED;
        tracing::warn!(
            event_id,
            event_name,
            "the peer endpoint accepts reports from anyone the network allows; \
             set PEER_SHARED_SECRET, or confine it with a network policy"
        );
    }

    // A report is bounded by PEER_MAX_KEYS_PER_REPORT, so anything far larger is not a
    // peer. Rejecting it early keeps a stranger from making this replica allocate.
    let body_limit = 1024 + app_env.peer_max_keys_per_report * BYTES_PER_REPORT_LINE;

    let app = Router::new()
        .route(REPORT_PATH, post(receive_report))
        .layer(axum::extract::DefaultBodyLimit::max(body_limit))
        .route("/health", get(liveness))
        .route("/readiness", get(readiness))
        .with_state(PeerEndpointState {
            store,
            replica_id: app_env.replica_id.clone(),
            health,
            shared_secret: Arc::new(app_env.peer_shared_secret.clone()),
            max_keys_per_report: app_env.peer_max_keys_per_report,
        });

    let listener = tokio::net::TcpListener::bind(&app_env.peer_listen_address)
        .await
        .map_err(|e| {
            TechnicalError(format!(
                "failed to bind the peer endpoint {}: {e}",
                app_env.peer_listen_address
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

    #[tokio::test]
    async fn an_empty_endpoint_yields_no_peers() {
        assert!(resolve_peers("").await.is_empty());
        assert!(resolve_peers("   ").await.is_empty());
    }

    #[tokio::test]
    async fn a_comma_separated_value_is_taken_as_a_list() {
        let peers = resolve_peers("10.0.0.1:8080, 10.0.0.2:8080 ,10.0.0.3:8080").await;

        assert_eq!(
            peers,
            vec!["10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080"]
        );
    }

    #[tokio::test]
    async fn a_resolvable_name_yields_its_addresses() {
        // localhost resolves everywhere, which is enough to prove the resolution path.
        let peers = resolve_peers("localhost:8080").await;

        assert!(!peers.is_empty());
        assert!(peers.iter().all(|peer| peer.ends_with(":8080")));
    }

    #[tokio::test]
    async fn an_unresolvable_name_yields_no_peers_rather_than_failing() {
        // A replica that cannot find its peers must keep serving, counting alone.
        assert!(resolve_peers("no-such-host.invalid:8080").await.is_empty());
    }

    #[test]
    fn publish_health_reports_a_condition_when_it_starts_and_when_it_clears() {
        // Pure bookkeeping: the transitions are what decide whether a line is logged, so
        // they are what is pinned here.
        let mut health = PublishHealth::default();

        health.note_delivery(
            2,
            Delivery {
                accepted: 0,
                rejected: 2,
                failed: 0,
            },
        );
        assert!(health.rejected);
        health.note_delivery(
            2,
            Delivery {
                accepted: 2,
                rejected: 0,
                failed: 0,
            },
        );
        assert!(!health.rejected);

        health.note_delivery(
            2,
            Delivery {
                accepted: 0,
                rejected: 0,
                failed: 2,
            },
        );
        assert!(health.unreachable);
        // One peer out of two is the expected occasional miss, not an outage.
        health.note_delivery(
            2,
            Delivery {
                accepted: 1,
                rejected: 0,
                failed: 1,
            },
        );
        assert!(!health.unreachable);

        health.note_resolution(0);
        assert!(health.unresolved);
        health.note_resolution(3);
        assert!(!health.unresolved);
    }
}
