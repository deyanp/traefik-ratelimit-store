//! Startup configuration.
//!
//! Built once from a map the caller supplies, never by reading the process environment
//! directly — so tests construct the same shape by hand and exercise the same factory.

use std::collections::HashMap;
use std::time::Duration;

use crate::memory_budget;
use crate::store::StoreConfig;

/// Everything the binary needs to run, resolved at startup.
#[derive(Clone, Debug)]
pub struct AppEnv {
    pub app_name: String,
    /// A stable identity for this replica, used to discard its own peer reports.
    pub replica_id: String,
    /// Address the rate-limit protocol listener binds to.
    pub listen_address: String,
    /// Most protocol connections served at once; one beyond that is refused.
    pub max_connections: usize,
    /// A protocol connection that sends nothing for this long is closed.
    pub connection_idle_timeout: Duration,
    /// How long to keep serving after readiness starts failing on termination.
    pub drain_period: Duration,
    /// Address the peer endpoint binds to.
    pub peer_listen_address: String,
    /// Either a DNS name resolving to every peer, or a comma-separated list of addresses.
    /// Empty means this replica counts alone.
    pub peer_endpoint: String,
    /// How often this replica publishes its admissions to peers.
    pub peer_publish_interval: Duration,
    /// How long a single delivery may take before it is abandoned.
    pub peer_request_timeout: Duration,
    /// Most keys a single report carries, busiest first; also the most this replica will
    /// accept in one inbound report.
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

/// Refuses a value that would make a timer or a bound meaningless.
///
/// Each of these would otherwise fail later, inside a background task, where the failure
/// is silent: a zero interval panics the timer that drives sweeping or publishing, and a
/// zero shard count or ceiling panics the first request. Startup is where this belongs.
fn require_positive(name: &str, value: u128) {
    if value == 0 {
        panic!("Environment variable {name} must be greater than zero!");
    }
}

/// Refuses to start a meshed replica whose peer endpoint anyone can write to.
///
/// An unauthenticated endpoint is a rate-limit bypass rather than a nuisance: a report is
/// folded into this replica's buckets exactly like a peer's, so a stranger who claims a
/// timestamp far in the future refills a drained bucket, and one who claims admissions
/// throttles a key at will. Do that to each replica and the limit is whatever the
/// stranger says it is.
///
/// Requiring a secret by default would be no safer — an unset secret would reject every
/// report, and every replica would count alone, reaching over-admission by a different
/// route. So neither default is safe, and the only safe thing is to make the operator
/// choose.
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
         endpoint lets anyone refill or drain any bucket on this replica. Set \
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

        // The entry ceiling is derived from the memory budget rather than configured
        // beside it, because the two are one decision and setting them apart is how a
        // store gets killed while its own ceiling still reports headroom.
        let shard_count = read_usize(env_vars, "STORE_SHARD_COUNT", 16);
        require_positive("STORE_SHARD_COUNT", shard_count as u128);
        let budget_mb = read_optional(env_vars, "STORE_MEMORY_BUDGET_MB")
            .map(|_| read_usize(env_vars, "STORE_MEMORY_BUDGET_MB", 0));
        if let Some(budget_mb) = budget_mb {
            require_positive("STORE_MEMORY_BUDGET_MB", budget_mb as u128);
        }
        let budget_bytes = memory_budget::resolve_budget_bytes(budget_mb);
        let capacity_per_shard = read_optional(env_vars, "STORE_CAPACITY_PER_SHARD")
            .map(|_| read_usize(env_vars, "STORE_CAPACITY_PER_SHARD", 0))
            .unwrap_or_else(|| memory_budget::derive_entries_per_shard(budget_bytes, shard_count));
        require_positive("STORE_CAPACITY_PER_SHARD", capacity_per_shard as u128);

        let sweep_interval = read_duration_millis(env_vars, "STORE_SWEEP_INTERVAL_MS", 1_000);
        require_positive("STORE_SWEEP_INTERVAL_MS", sweep_interval.as_millis());

        let peer_publish_interval = read_duration_millis(env_vars, "PEER_PUBLISH_INTERVAL_MS", 150);
        require_positive(
            "PEER_PUBLISH_INTERVAL_MS",
            peer_publish_interval.as_millis(),
        );
        let peer_request_timeout = read_duration_millis(env_vars, "PEER_REQUEST_TIMEOUT_MS", 50);
        require_positive("PEER_REQUEST_TIMEOUT_MS", peer_request_timeout.as_millis());
        // A round of publishing waits for every delivery to finish or time out, so a
        // timeout at or beyond the interval would make the publisher skip intervals.
        if peer_request_timeout >= peer_publish_interval {
            panic!(
                "PEER_REQUEST_TIMEOUT_MS must be less than PEER_PUBLISH_INTERVAL_MS, \
                 otherwise one slow peer delays every report!"
            );
        }
        let peer_max_keys_per_report = read_usize(env_vars, "PEER_MAX_KEYS_PER_REPORT", 10_000);
        require_positive("PEER_MAX_KEYS_PER_REPORT", peer_max_keys_per_report as u128);

        let max_connections = read_usize(env_vars, "MAX_CONNECTIONS", 4_096);
        require_positive("MAX_CONNECTIONS", max_connections as u128);
        // The caller's own idle limit is thirty minutes; closing sooner would make it
        // reconnect for nothing.
        let connection_idle_timeout =
            read_duration_millis(env_vars, "CONNECTION_IDLE_TIMEOUT_MS", 30 * 60 * 1_000);
        require_positive(
            "CONNECTION_IDLE_TIMEOUT_MS",
            connection_idle_timeout.as_millis(),
        );
        let drain_period = read_duration_millis(env_vars, "DRAIN_PERIOD_MS", 5_000);

        Self {
            app_name: read_or_default(env_vars, "APPNAME", "traefik-ratelimit-store"),
            replica_id,
            listen_address: read_or_default(env_vars, "LISTEN_ADDRESS", "0.0.0.0:6379"),
            max_connections,
            connection_idle_timeout,
            drain_period,
            peer_listen_address: read_or_default(env_vars, "PEER_LISTEN_ADDRESS", "0.0.0.0:8080"),
            peer_endpoint,
            peer_publish_interval,
            peer_request_timeout,
            peer_max_keys_per_report,
            peer_shared_secret,
            store: StoreConfig {
                shard_count,
                capacity_per_shard,
                sweep_interval,
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
        assert_eq!(env.max_connections, 4_096);
        assert_eq!(env.connection_idle_timeout, Duration::from_secs(30 * 60));
        assert_eq!(env.drain_period, Duration::from_secs(5));
        assert_eq!(env.peer_publish_interval, Duration::from_millis(150));
        assert_eq!(env.store.shard_count, 16);
        assert!(env.peer_endpoint.is_empty());
        // Derived rather than a fixed default.
        assert!(env.store.capacity_per_shard >= 1_024);
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
    fn the_entry_ceiling_is_derived_from_the_budget() {
        let env_vars = HashMap::from([("STORE_MEMORY_BUDGET_MB".to_string(), "128".to_string())]);

        let env = AppEnv::create(&env_vars);

        // Whatever it derives must fit inside the budget it derived from — the property
        // that was violated when the two were configured separately.
        let allocated =
            memory_budget::compute_table_bytes(env.store.capacity_per_shard, env.store.shard_count);
        assert!(allocated <= 64 * 1024 * 1024, "{allocated} bytes");
        assert!(env.store.capacity_per_shard > 1_000);
    }

    #[test]
    fn an_explicit_ceiling_still_wins() {
        let env_vars = HashMap::from([
            ("STORE_MEMORY_BUDGET_MB".to_string(), "128".to_string()),
            ("STORE_CAPACITY_PER_SHARD".to_string(), "500".to_string()),
        ]);

        let env = AppEnv::create(&env_vars);

        assert_eq!(env.store.capacity_per_shard, 500);
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

    #[test]
    #[should_panic(expected = "STORE_SHARD_COUNT must be greater than zero")]
    fn a_zero_shard_count_is_refused_at_startup() {
        AppEnv::create(&HashMap::from([(
            "STORE_SHARD_COUNT".to_string(),
            "0".to_string(),
        )]));
    }

    #[test]
    #[should_panic(expected = "STORE_CAPACITY_PER_SHARD must be greater than zero")]
    fn a_zero_ceiling_is_refused_at_startup() {
        AppEnv::create(&HashMap::from([(
            "STORE_CAPACITY_PER_SHARD".to_string(),
            "0".to_string(),
        )]));
    }

    #[test]
    #[should_panic(expected = "PEER_PUBLISH_INTERVAL_MS must be greater than zero")]
    fn a_zero_publish_interval_is_refused_at_startup() {
        AppEnv::create(&HashMap::from([(
            "PEER_PUBLISH_INTERVAL_MS".to_string(),
            "0".to_string(),
        )]));
    }

    #[test]
    #[should_panic(expected = "STORE_SWEEP_INTERVAL_MS must be greater than zero")]
    fn a_zero_sweep_interval_is_refused_at_startup() {
        AppEnv::create(&HashMap::from([(
            "STORE_SWEEP_INTERVAL_MS".to_string(),
            "0".to_string(),
        )]));
    }

    #[test]
    #[should_panic(expected = "PEER_MAX_KEYS_PER_REPORT must be greater than zero")]
    fn a_zero_report_cap_is_refused_at_startup() {
        AppEnv::create(&HashMap::from([(
            "PEER_MAX_KEYS_PER_REPORT".to_string(),
            "0".to_string(),
        )]));
    }

    #[test]
    #[should_panic(expected = "PEER_REQUEST_TIMEOUT_MS must be less than PEER_PUBLISH_INTERVAL_MS")]
    fn a_delivery_timeout_longer_than_the_interval_is_refused_at_startup() {
        AppEnv::create(&HashMap::from([
            ("PEER_PUBLISH_INTERVAL_MS".to_string(), "100".to_string()),
            ("PEER_REQUEST_TIMEOUT_MS".to_string(), "100".to_string()),
        ]));
    }

    #[test]
    #[should_panic(expected = "STORE_MEMORY_BUDGET_MB must be a whole number")]
    fn a_malformed_budget_is_refused_rather_than_ignored() {
        // Silently falling back to the cgroup limit would hide a typo in the one setting
        // that decides whether the process fits its container.
        AppEnv::create(&HashMap::from([(
            "STORE_MEMORY_BUDGET_MB".to_string(),
            "128Mi".to_string(),
        )]));
    }
}
