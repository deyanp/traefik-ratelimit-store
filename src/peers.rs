//! What peers tell each other.
//!
//! Replicas share **admissions**, not bucket state. Bucket state does not merge — taking
//! the newest or the largest of two `(last, tokens)` pairs discards one replica's
//! increments, which is precisely the over-admission the sharing exists to prevent.
//! Admissions are tokens taken from the one logical bucket, so a peer can debit them from
//! its own copy exactly as if the requests had arrived there. That keeps every replica's
//! level tracking the shared one, and it is what makes the limit hold under sustained
//! traffic rather than only across a single burst.
//!
//! Each report carries what this replica admitted *since its last report*, so applying a
//! report once is the whole protocol: there is no table to reconcile and nothing to age
//! out. A report that is lost costs its peers that interval's admissions, which is the
//! same one-interval error the exchange cadence already allows.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::store::{KeyHash, KeyReport, PeerAdmissions};

/// Longest replica id accepted on the wire. Hostnames are far shorter; anything longer is
/// not a peer.
const MAX_REPLICA_ID_LENGTH: usize = 256;

/// Longest lifetime a peer may ask this replica to hold an entry for. The caller derives
/// lifetimes of a few seconds from the rate; a day is far beyond anything legitimate.
const MAX_REPORTED_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// One key's admissions, as they travel.
///
/// The key is hexadecimal because it is a digest, not a string. `last`, `limit` and
/// `burst` let the receiver fold the admissions into its own bucket as of the moment they
/// happened, even for a key it has not seen itself.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct KeyAdmissions {
    pub key: String,
    pub admitted: u32,
    pub last: f64,
    pub limit: f64,
    pub burst: f64,
    pub ttl_ms: u64,
}

/// One replica's account of what it admitted since its previous report.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PeerReport {
    pub replica_id: String,
    pub keys: Vec<KeyAdmissions>,
}

impl PeerReport {
    pub fn new(replica_id: String, lines: &[KeyReport]) -> Self {
        Self {
            replica_id,
            keys: lines
                .iter()
                .map(|line| KeyAdmissions {
                    key: line.key.to_hex(),
                    admitted: line.admitted,
                    last: line.last,
                    limit: line.limit,
                    burst: line.burst,
                    ttl_ms: line.ttl.as_millis().try_into().unwrap_or(u64::MAX),
                })
                .collect(),
        }
    }
}

/// Why a report was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReportError {
    /// The replica id is empty or longer than any hostname.
    InvalidReplicaId,
    /// More keys than this replica is prepared to fold from one report.
    TooManyKeys(usize),
}

impl std::fmt::Display for ReportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidReplicaId => write!(f, "replica id is empty or too long"),
            Self::TooManyKeys(count) => write!(f, "report carries {count} keys, over the limit"),
        }
    }
}

/// Turns a received report into the admissions to fold, dropping lines that cannot be
/// applied safely.
///
/// Returns `Ok(empty)` for this replica's own echo — a replica may publish to itself,
/// since one loopback request per interval is cheaper than the machinery to avoid it, and
/// it recognises the echo by its own id.
///
/// A line with a non-finite number, a non-positive `limit`, or a malformed key is skipped
/// rather than failing the report: one bad line should not discard its neighbours, and a
/// skipped line only makes this replica more permissive for that key by one report.
pub fn decode_report(
    report: PeerReport,
    own_replica_id: &str,
    max_keys: usize,
) -> Result<Vec<(KeyHash, PeerAdmissions)>, ReportError> {
    if report.replica_id.is_empty() || report.replica_id.len() > MAX_REPLICA_ID_LENGTH {
        return Err(ReportError::InvalidReplicaId);
    }
    if report.keys.len() > max_keys {
        return Err(ReportError::TooManyKeys(report.keys.len()));
    }
    if report.replica_id == own_replica_id {
        return Ok(Vec::new());
    }

    Ok(report
        .keys
        .iter()
        .filter_map(|line| {
            let key = KeyHash::from_hex(&line.key)?;
            let usable = line.admitted > 0
                && line.last.is_finite()
                && line.limit.is_finite()
                && line.limit > 0.0
                && line.burst.is_finite();
            usable.then_some((
                key,
                PeerAdmissions {
                    admitted: line.admitted,
                    last: line.last,
                    limit: line.limit,
                    burst: line.burst,
                    ttl: Duration::from_millis(line.ttl_ms).min(MAX_REPORTED_TTL),
                },
            ))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: f64 = 3.0 / 1_000_000.0;

    fn line(key: KeyHash, admitted: u32) -> KeyAdmissions {
        KeyAdmissions {
            key: key.to_hex(),
            admitted,
            last: 1_787_000_000_000_000.0,
            limit: LIMIT,
            burst: 10.0,
            ttl_ms: 2_000,
        }
    }

    fn report_of(replica: &str, lines: Vec<KeyAdmissions>) -> PeerReport {
        PeerReport {
            replica_id: replica.to_string(),
            keys: lines,
        }
    }

    #[test]
    fn a_report_decodes_into_admissions_to_fold() {
        let key = KeyHash::from_key(b"rate:mw:client");

        let decoded = decode_report(report_of("peer-a", vec![line(key, 5)]), "self", 100).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, key);
        assert_eq!(decoded[0].1.admitted, 5);
        assert_eq!(decoded[0].1.limit, LIMIT);
        assert_eq!(decoded[0].1.burst, 10.0);
        assert_eq!(decoded[0].1.ttl, Duration::from_secs(2));
    }

    #[test]
    fn a_replicas_own_echo_is_discarded() {
        let key = KeyHash::from_key(b"rate:mw:client");

        let decoded = decode_report(report_of("self", vec![line(key, 9)]), "self", 100).unwrap();

        assert!(decoded.is_empty());
    }

    #[test]
    fn an_oversized_report_is_refused() {
        let key = KeyHash::from_key(b"rate:mw:client");
        let lines = (0..3).map(|_| line(key, 1)).collect();

        assert_eq!(
            decode_report(report_of("peer-a", lines), "self", 2),
            Err(ReportError::TooManyKeys(3))
        );
    }

    #[test]
    fn an_implausible_replica_id_is_refused() {
        assert_eq!(
            decode_report(report_of("", vec![]), "self", 100),
            Err(ReportError::InvalidReplicaId)
        );
        assert_eq!(
            decode_report(report_of(&"x".repeat(300), vec![]), "self", 100),
            Err(ReportError::InvalidReplicaId)
        );
    }

    #[test]
    fn lines_that_cannot_be_folded_safely_are_skipped() {
        let key = KeyHash::from_key(b"rate:mw:client");
        let mut zero_limit = line(key, 1);
        zero_limit.limit = 0.0;
        let mut non_finite = line(key, 1);
        non_finite.burst = f64::INFINITY;
        let mut bad_key = line(key, 1);
        bad_key.key = "not-hex".to_string();
        let mut nothing_admitted = line(key, 0);
        nothing_admitted.admitted = 0;

        let decoded = decode_report(
            report_of(
                "peer-a",
                vec![
                    zero_limit,
                    non_finite,
                    bad_key,
                    nothing_admitted,
                    line(key, 2),
                ],
            ),
            "self",
            100,
        )
        .unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].1.admitted, 2);
    }

    #[test]
    fn a_reported_lifetime_is_capped() {
        let key = KeyHash::from_key(b"rate:mw:client");
        let mut forever = line(key, 1);
        forever.ttl_ms = u64::MAX;

        let decoded = decode_report(report_of("peer-a", vec![forever]), "self", 100).unwrap();

        assert_eq!(decoded[0].1.ttl, MAX_REPORTED_TTL);
    }

    #[test]
    fn a_report_survives_a_round_trip_through_json() {
        let key = KeyHash::from_key(b"rate:mw:client");
        let report = report_of("peer-a", vec![line(key, 7)]);

        let decoded: PeerReport =
            serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();

        assert_eq!(decoded, report);
    }
}
