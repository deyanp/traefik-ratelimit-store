//! Startup configuration.
//!
//! Built once from a map the caller supplies, never by reading the process environment
//! directly — so tests construct the same shape by hand and exercise the same factory.

use std::collections::HashMap;
use std::time::Duration;

use crate::store::StoreConfig;

/// Everything the binary needs to run, resolved at startup.
#[derive(Clone, Debug)]
pub struct AppEnv {
    pub app_name: String,
    /// A stable identity for this replica, used to discard its own peer reports.
    pub replica_id: String,
    /// Address the rate-limit protocol listener binds to.
    pub listen_address: String,
    /// Address the peer endpoint binds to.
    pub peer_listen_address: String,
    /// Either a DNS name resolving to every peer, or a comma-separated list of addresses.
    /// Empty means this replica counts alone.
    pub peer_endpoint: String,
    /// How often this replica publishes its consumption to peers.
    pub peer_publish_interval: Duration,
    /// How long a peer's report stays usable after it arrives.
    pub peer_staleness_limit: Duration,
    /// How long a single delivery may take before it is abandoned.
    pub peer_request_timeout: Duration,
    /// Most keys a single report carries, busiest first.
    pub peer_max_keys_per_report: usize,
    /// Shared secret peers must present. Empty means the endpoint is unauthenticated and
    /// relies entirely on network policy.
    pub peer_shared_secret: String,
    pub store: StoreConfig,
}

fn read_optional(env_vars: &HashMap<String, String>, name: &str) -> Option<String> {
    env_vars
        .get(name)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_or_default(env_vars: &HashMap<String, String>, name: &str, fallback: &str) -> String {
    read_optional(env_vars, name).unwrap_or_else(|| fallback.to_string())
}

fn read_duration_millis(env_vars: &HashMap<String, String>, name: &str, fallback: u64) -> Duration {
    let millis = read_optional(env_vars, name)
        .map(|value| {
            value.parse::<u64>().unwrap_or_else(|_| {
                panic!("Environment variable {name} must be a whole number of milliseconds!")
            })
        })
        .unwrap_or(fallback);

    Duration::from_millis(millis)
}

fn read_usize(env_vars: &HashMap<String, String>, name: &str, fallback: usize) -> usize {
    read_optional(env_vars, name)
        .map(|value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("Environment variable {name} must be a whole number!"))
        })
        .unwrap_or(fallback)
}

/// Refuses to start a meshed replica whose peer endpoint anyone can write to.
///
/// An unauthenticated endpoint is a rate-limit bypass rather than a nuisance: reports are
/// keyed by replica id and overwrite, so a stranger can send an empty report in a peer's
/// name and erase that peer's consumption from this replica's view. Do that to each
/// replica and every one believes it is alone.
///
/// Requiring a secret by default would be no safer — an unset secret would reject every
/// report, age every peer out, and reach the same over-admission by a different route. So
/// neither default is safe, and the only safe thing is to make the operator choose.
fn require_a_decision_about_peer_authentication(
    peer_endpoint: &str,
    peer_shared_secret: &str,
    allow_unauthenticated: bool,
) {
    if peer_endpoint.trim().is_empty() || !peer_shared_secret.is_empty() || allow_unauthenticated {
        return;
    }

    panic!(
        "PEER_ENDPOINT is set but PEER_SHARED_SECRET is not. An unauthenticated peer \
         endpoint lets anyone erase a peer's consumption and lift the rate limit. Set \
         PEER_SHARED_SECRET to the same value on every replica, or set \
         PEER_ALLOW_UNAUTHENTICATED=true to accept that risk deliberately."
    );
}

impl AppEnv {
    pub fn create(env_vars: &HashMap<String, String>) -> Self {
        // The hostname is stable for a pod's lifetime and unique within a deployment,
        // which is all the replica identity has to be.
        let replica_id = read_optional(env_vars, "REPLICA_ID")
            .or_else(|| read_optional(env_vars, "HOSTNAME"))
            .unwrap_or_else(|| "replica".to_string());

        let peer_endpoint = read_or_default(env_vars, "PEER_ENDPOINT", "");
        let peer_shared_secret = read_or_default(env_vars, "PEER_SHARED_SECRET", "");
        require_a_decision_about_peer_authentication(
            &peer_endpoint,
            &peer_shared_secret,
            read_or_default(env_vars, "PEER_ALLOW_UNAUTHENTICATED", "false") == "true",
        );

        Self {
            app_name: read_or_default(env_vars, "APPNAME", "traefik-ratelimit-store"),
            replica_id,
            listen_address: read_or_default(env_vars, "LISTEN_ADDRESS", "0.0.0.0:6379"),
            peer_listen_address: read_or_default(env_vars, "PEER_LISTEN_ADDRESS", "0.0.0.0:8080"),
            peer_endpoint,
            peer_publish_interval: read_duration_millis(env_vars, "PEER_PUBLISH_INTERVAL_MS", 150),
            peer_staleness_limit: read_duration_millis(env_vars, "PEER_STALENESS_LIMIT_MS", 1_000),
            peer_request_timeout: read_duration_millis(env_vars, "PEER_REQUEST_TIMEOUT_MS", 50),
            peer_max_keys_per_report: read_usize(env_vars, "PEER_MAX_KEYS_PER_REPORT", 10_000),
            peer_shared_secret,
            store: StoreConfig {
                shard_count: read_usize(env_vars, "STORE_SHARD_COUNT", 16),
                capacity_per_shard: read_usize(env_vars, "STORE_CAPACITY_PER_SHARD", 65_536),
                sweep_interval: read_duration_millis(env_vars, "STORE_SWEEP_INTERVAL_MS", 1_000),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_apply_when_nothing_is_configured() {
        let env = AppEnv::create(&HashMap::new());

        assert_eq!(env.listen_address, "0.0.0.0:6379");
        assert_eq!(env.peer_publish_interval, Duration::from_millis(150));
        assert_eq!(env.store.shard_count, 16);
        assert!(env.peer_endpoint.is_empty());
    }

    #[test]
    fn a_blank_value_counts_as_unset() {
        // An unset variable injected by an orchestrator arrives as an empty string.
        let env_vars = HashMap::from([("LISTEN_ADDRESS".to_string(), "   ".to_string())]);

        let env = AppEnv::create(&env_vars);

        assert_eq!(env.listen_address, "0.0.0.0:6379");
    }

    #[test]
    fn replica_id_falls_back_to_the_hostname() {
        let env_vars = HashMap::from([("HOSTNAME".to_string(), "store-7c9d-abcde".to_string())]);

        let env = AppEnv::create(&env_vars);

        assert_eq!(env.replica_id, "store-7c9d-abcde");
    }

    #[test]
    fn a_report_is_capped_by_default() {
        let env = AppEnv::create(&HashMap::new());

        assert_eq!(env.peer_max_keys_per_report, 10_000);
    }

    #[test]
    fn the_peer_endpoint_is_unauthenticated_unless_a_secret_is_given() {
        let env = AppEnv::create(&HashMap::new());

        assert!(env.peer_shared_secret.is_empty());
    }

    #[test]
    fn a_lone_replica_needs_no_secret() {
        // No mesh, no endpoint to protect.
        let env = AppEnv::create(&HashMap::new());

        assert!(env.peer_endpoint.is_empty());
    }

    #[test]
    #[should_panic(expected = "PEER_SHARED_SECRET")]
    fn a_meshed_replica_refuses_to_start_without_a_decision() {
        let env_vars = HashMap::from([("PEER_ENDPOINT".to_string(), "peers.svc:8080".to_string())]);

        AppEnv::create(&env_vars);
    }

    #[test]
    fn a_meshed_replica_starts_with_a_secret() {
        let env_vars = HashMap::from([
            ("PEER_ENDPOINT".to_string(), "peers.svc:8080".to_string()),
            ("PEER_SHARED_SECRET".to_string(), "shared".to_string()),
        ]);

        let env = AppEnv::create(&env_vars);

        assert_eq!(env.peer_shared_secret, "shared");
    }

    #[test]
    fn the_risk_can_be_accepted_deliberately() {
        // The escape hatch exists so local development does not need a secret, and so the
        // choice is recorded in configuration rather than made by omission.
        let env_vars = HashMap::from([
            ("PEER_ENDPOINT".to_string(), "peers.svc:8080".to_string()),
            ("PEER_ALLOW_UNAUTHENTICATED".to_string(), "true".to_string()),
        ]);

        let env = AppEnv::create(&env_vars);

        assert!(env.peer_shared_secret.is_empty());
    }

    #[test]
    fn explicit_values_are_taken() {
        let env_vars = HashMap::from([
            ("PEER_ENDPOINT".to_string(), "peers.svc:8080".to_string()),
            ("PEER_SHARED_SECRET".to_string(), "shared".to_string()),
            ("PEER_PUBLISH_INTERVAL_MS".to_string(), "250".to_string()),
            ("STORE_SHARD_COUNT".to_string(), "32".to_string()),
        ]);

        let env = AppEnv::create(&env_vars);

        assert_eq!(env.peer_endpoint, "peers.svc:8080");
        assert_eq!(env.peer_publish_interval, Duration::from_millis(250));
        assert_eq!(env.store.shard_count, 32);
    }
}
