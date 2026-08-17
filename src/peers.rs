//! What peers tell each other, and what a replica does with it.
//!
//! Replicas share **consumption**, not bucket state. Bucket state does not merge — taking
//! the newest or the largest of two `(last, tokens)` pairs discards one replica's
//! increments, which is precisely the over-admission the sharing exists to prevent.
//! Counts add, so they do merge.
//!
//! No merge logic is needed even so. A replica only ever publishes its own consumption,
//! so each entry in the table is written by exactly one author and arriving reports
//! overwrite rather than combine. The summing happens at read time, on the request path.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::store::KeyHash;

/// One replica's account of what it has taken recently.
///
/// Keys travel as hexadecimal because a map key must be a string in JSON; the value is
/// tokens consumed within the trailing window, not since startup, so a restarted replica
/// needs no recovery and no monotonicity.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerReport {
    pub replica_id: String,
    pub keys: HashMap<String, u32>,
}

impl PeerReport {
    pub fn new(replica_id: String, consumption: &HashMap<KeyHash, u32>) -> Self {
        Self {
            replica_id,
            keys: consumption
                .iter()
                .map(|(key, count)| (key.to_hex(), *count))
                .collect(),
        }
    }
}

/// A report as held by the receiver, with the moment it arrived.
#[derive(Clone, Debug)]
struct StoredReport {
    /// Stamped by the receiver, never by the sender.
    ///
    /// Staleness is therefore measured on one clock, so peers need no clock
    /// synchronisation and a skewed sender cannot make its report look fresh.
    received_at: Instant,
    keys: HashMap<KeyHash, u32>,
}

/// What every peer has recently reported.
#[derive(Debug, Default)]
pub struct PeerTable {
    /// Keyed by replica id, and each replica writes only its own entry — so this is
    /// last-write-wins per author with no contention and nothing to reconcile.
    reports: RwLock<HashMap<String, StoredReport>>,
    staleness_limit: Duration,
}

impl PeerTable {
    pub fn new(staleness_limit: Duration) -> Self {
        Self {
            reports: RwLock::new(HashMap::new()),
            staleness_limit,
        }
    }

    /// Records a report, ignoring this replica's own echo.
    ///
    /// A replica may publish to itself — one loopback request per interval is cheaper than
    /// the machinery to avoid it — and recognises the echo by its own id.
    pub fn record(&self, own_replica_id: &str, report: PeerReport, received_at: Instant) {
        if report.replica_id == own_replica_id {
            return;
        }

        let keys = report
            .keys
            .iter()
            .filter_map(|(hex, count)| KeyHash::from_hex(hex).map(|key| (key, *count)))
            .collect();

        self.reports
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(report.replica_id, StoredReport { received_at, keys });
    }

    /// Tokens every fresh peer reports having taken for `key`.
    ///
    /// A peer that has stopped reporting ages out of the window and stops counting, which
    /// is what makes a lost peer degrade into "this replica counts alone" rather than into
    /// a wrong answer.
    pub fn consumed_for(&self, key: KeyHash, now: Instant) -> u32 {
        let reports = self.reports.read().unwrap_or_else(|p| p.into_inner());

        reports
            .values()
            .filter(|report| now.duration_since(report.received_at) < self.staleness_limit)
            .filter_map(|report| report.keys.get(&key))
            .sum()
    }

    /// Drops peers that have not reported within the window.
    pub fn evict_stale(&self, now: Instant) {
        self.reports
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|_, report| now.duration_since(report.received_at) < self.staleness_limit);
    }

    /// How many peers are currently counted.
    pub fn fresh_peer_count(&self, now: Instant) -> usize {
        self.reports
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .filter(|report| now.duration_since(report.received_at) < self.staleness_limit)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_millis(1000);

    fn report_of(replica: &str, key: KeyHash, count: u32) -> PeerReport {
        PeerReport {
            replica_id: replica.to_string(),
            keys: HashMap::from([(key.to_hex(), count)]),
        }
    }

    #[test]
    fn counts_from_several_peers_add_up() {
        let now = Instant::now();
        let table = PeerTable::new(WINDOW);
        let key = KeyHash::from_key(b"rate:mw:client");

        table.record("self", report_of("peer-a", key, 5), now);
        table.record("self", report_of("peer-b", key, 3), now);

        assert_eq!(table.consumed_for(key, now), 8);
    }

    #[test]
    fn a_replicas_own_echo_is_discarded() {
        let now = Instant::now();
        let table = PeerTable::new(WINDOW);
        let key = KeyHash::from_key(b"rate:mw:client");

        table.record("self", report_of("self", key, 9), now);

        assert_eq!(table.consumed_for(key, now), 0);
        assert_eq!(table.fresh_peer_count(now), 0);
    }

    #[test]
    fn a_newer_report_replaces_the_previous_one_from_that_peer() {
        let now = Instant::now();
        let table = PeerTable::new(WINDOW);
        let key = KeyHash::from_key(b"rate:mw:client");

        table.record("self", report_of("peer-a", key, 5), now);
        table.record("self", report_of("peer-a", key, 2), now);

        // Replaced, not accumulated: each report is that peer's whole current view.
        assert_eq!(table.consumed_for(key, now), 2);
    }

    #[test]
    fn a_silent_peer_stops_counting() {
        let start = Instant::now();
        let table = PeerTable::new(WINDOW);
        let key = KeyHash::from_key(b"rate:mw:client");

        table.record("self", report_of("peer-a", key, 5), start);

        assert_eq!(
            table.consumed_for(key, start + Duration::from_millis(500)),
            5
        );
        assert_eq!(
            table.consumed_for(key, start + Duration::from_millis(1500)),
            0
        );
    }

    #[test]
    fn an_unreported_key_counts_as_zero() {
        let now = Instant::now();
        let table = PeerTable::new(WINDOW);

        table.record(
            "self",
            report_of("peer-a", KeyHash::from_key(b"one"), 5),
            now,
        );

        assert_eq!(table.consumed_for(KeyHash::from_key(b"other"), now), 0);
    }

    #[test]
    fn stale_peers_are_evicted() {
        let start = Instant::now();
        let table = PeerTable::new(WINDOW);
        table.record(
            "self",
            report_of("peer-a", KeyHash::from_key(b"one"), 5),
            start,
        );

        table.evict_stale(start + Duration::from_millis(1500));

        assert_eq!(
            table.fresh_peer_count(start + Duration::from_millis(1500)),
            0
        );
    }

    #[test]
    fn a_report_survives_a_round_trip_through_json() {
        let key = KeyHash::from_key(b"rate:mw:client");
        let report = report_of("peer-a", key, 7);

        let decoded: PeerReport =
            serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();

        assert_eq!(decoded, report);
    }
}
