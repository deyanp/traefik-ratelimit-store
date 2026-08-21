//! What peers tell each other, as bytes on the wire.
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
//!
//! The encoding is a fixed binary record, not JSON: under heavy load a report carries
//! thousands of keys several times a second to every peer, and at that volume the
//! difference is real — 48 bytes per key against ~150 of JSON, and a decode that is a
//! bounds check and a copy instead of a parse. One version byte leads, so a mismatched
//! replica fails loudly instead of misreading:
//!
//! ```text
//! [version u8] [replica_id_len u8] [replica_id bytes]
//! then per key, little-endian, 48 bytes:
//!   key      u128   16    admitted u32   4    last  f64  8
//!   limit    f64     8    burst    f64   8    ttl   u32  4  (milliseconds)
//! ```

use std::time::Duration;

use crate::store::{KeyHash, KeyReport, PeerAdmissions};

/// Refused on receipt if it does not match; bumped when the record layout changes.
pub const FORMAT_VERSION: u8 = 1;

/// One key's admissions on the wire.
pub const RECORD_BYTES: usize = 48;

/// Longest replica id the length prefix can carry. Startup refuses a longer one, so
/// encoding never has to.
pub const MAX_REPLICA_ID_LENGTH: usize = 255;

/// Longest lifetime a peer may ask this replica to hold an entry for. The caller derives
/// lifetimes of a few seconds from the rate; a day is far beyond anything legitimate.
const MAX_REPORTED_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Why a report was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReportError {
    /// The first byte names a format this replica does not speak.
    UnknownVersion(u8),
    /// The bytes do not parse as a report: truncated header, an id that is not UTF-8, or
    /// a body that is not a whole number of records.
    Malformed,
    /// More keys than this replica is prepared to fold from one report.
    TooManyKeys(usize),
}

impl std::fmt::Display for ReportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownVersion(version) => write!(f, "unknown report format version {version}"),
            Self::Malformed => write!(f, "report does not parse"),
            Self::TooManyKeys(count) => write!(f, "report carries {count} keys, over the limit"),
        }
    }
}

/// Renders a report: this replica's identity and what it admitted since the last one.
///
/// The id's length is validated at startup, so encoding cannot fail.
pub fn encode_report(replica_id: &str, lines: &[KeyReport]) -> Vec<u8> {
    debug_assert!(
        !replica_id.is_empty() && replica_id.len() <= MAX_REPLICA_ID_LENGTH,
        "replica id length is validated at startup"
    );

    let mut out = Vec::with_capacity(2 + replica_id.len() + lines.len() * RECORD_BYTES);
    out.push(FORMAT_VERSION);
    out.push(replica_id.len() as u8);
    out.extend_from_slice(replica_id.as_bytes());

    for line in lines {
        out.extend_from_slice(&line.key.to_bytes());
        out.extend_from_slice(&line.admitted.to_le_bytes());
        out.extend_from_slice(&line.last.to_le_bytes());
        out.extend_from_slice(&line.limit.to_le_bytes());
        out.extend_from_slice(&line.burst.to_le_bytes());
        let ttl_ms: u32 = line.ttl.as_millis().try_into().unwrap_or(u32::MAX);
        out.extend_from_slice(&ttl_ms.to_le_bytes());
    }

    out
}

/// Reads a fixed-width little-endian field out of a record, advancing the cursor.
///
/// Infallible by construction: records are exact multiples of [`RECORD_BYTES`] and the
/// field widths sum to it, which the compiler cannot see but the tests pin.
fn take<const N: usize>(record: &[u8], cursor: &mut usize) -> [u8; N] {
    let mut field = [0u8; N];
    field.copy_from_slice(&record[*cursor..*cursor + N]);
    *cursor += N;
    field
}

/// Turns a received report into the admissions to fold, dropping lines that cannot be
/// applied safely.
///
/// Returns `Ok(empty)` for this replica's own echo — a replica may publish to itself,
/// since one loopback request per interval is cheaper than the machinery to avoid it, and
/// it recognises the echo by its own id.
///
/// A record with a non-finite number, a non-positive `limit`, or nothing admitted is
/// skipped rather than failing the report: one bad line should not discard its
/// neighbours, and a skipped line only makes this replica more permissive for that key by
/// one report.
pub fn decode_report(
    bytes: &[u8],
    own_replica_id: &str,
    max_keys: usize,
) -> Result<Vec<(KeyHash, PeerAdmissions)>, ReportError> {
    let [version, id_length, rest @ ..] = bytes else {
        return Err(ReportError::Malformed);
    };
    if *version != FORMAT_VERSION {
        return Err(ReportError::UnknownVersion(*version));
    }
    let id_length = usize::from(*id_length);
    if id_length == 0 || rest.len() < id_length {
        return Err(ReportError::Malformed);
    }

    let (id, records) = rest.split_at(id_length);
    let Ok(id) = std::str::from_utf8(id) else {
        return Err(ReportError::Malformed);
    };
    if !records.len().is_multiple_of(RECORD_BYTES) {
        return Err(ReportError::Malformed);
    }
    let count = records.len() / RECORD_BYTES;
    if count > max_keys {
        return Err(ReportError::TooManyKeys(count));
    }
    if id == own_replica_id {
        return Ok(Vec::new());
    }

    let mut admissions = Vec::with_capacity(count);
    // The length was checked to be an exact multiple, so the remainder is empty.
    let (records, _) = records.as_chunks::<RECORD_BYTES>();
    for record in records {
        let mut cursor = 0;
        let key = KeyHash::from_bytes(take::<16>(record, &mut cursor));
        let admitted = u32::from_le_bytes(take::<4>(record, &mut cursor));
        let last = f64::from_le_bytes(take::<8>(record, &mut cursor));
        let limit = f64::from_le_bytes(take::<8>(record, &mut cursor));
        let burst = f64::from_le_bytes(take::<8>(record, &mut cursor));
        let ttl_ms = u32::from_le_bytes(take::<4>(record, &mut cursor));
        debug_assert_eq!(cursor, RECORD_BYTES);

        let usable = admitted > 0
            && last.is_finite()
            && limit.is_finite()
            && limit > 0.0
            && burst.is_finite();
        if !usable {
            continue;
        }

        admissions.push((
            key,
            PeerAdmissions {
                admitted,
                last,
                limit,
                burst,
                ttl: Duration::from_millis(u64::from(ttl_ms)).min(MAX_REPORTED_TTL),
            },
        ));
    }

    Ok(admissions)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: f64 = 3.0 / 1_000_000.0;

    fn line(key: KeyHash, admitted: u32) -> KeyReport {
        KeyReport {
            key,
            admitted,
            last: 1_787_000_000_000_000.0,
            limit: LIMIT,
            burst: 10.0,
            ttl: Duration::from_secs(2),
        }
    }

    #[test]
    fn a_record_costs_exactly_its_declared_width() {
        let key = KeyHash::from_key(b"rate:mw:client");

        let encoded = encode_report("peer-a", &[line(key, 5), line(key, 6)]);

        assert_eq!(encoded.len(), 2 + "peer-a".len() + 2 * RECORD_BYTES);
    }

    #[test]
    fn a_report_survives_the_round_trip() {
        let key = KeyHash::from_key(b"rate:mw:client");

        let decoded =
            decode_report(&encode_report("peer-a", &[line(key, 5)]), "self", 100).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, key);
        assert_eq!(decoded[0].1.admitted, 5);
        assert_eq!(decoded[0].1.last, 1_787_000_000_000_000.0);
        assert_eq!(decoded[0].1.limit, LIMIT);
        assert_eq!(decoded[0].1.burst, 10.0);
        assert_eq!(decoded[0].1.ttl, Duration::from_secs(2));
    }

    #[test]
    fn a_replicas_own_echo_is_discarded() {
        let key = KeyHash::from_key(b"rate:mw:client");

        let decoded = decode_report(&encode_report("self", &[line(key, 9)]), "self", 100).unwrap();

        assert!(decoded.is_empty());
    }

    #[test]
    fn an_oversized_report_is_refused() {
        let key = KeyHash::from_key(b"rate:mw:client");
        let lines = [line(key, 1), line(key, 1), line(key, 1)];

        assert_eq!(
            decode_report(&encode_report("peer-a", &lines), "self", 2),
            Err(ReportError::TooManyKeys(3))
        );
    }

    #[test]
    fn a_foreign_version_is_refused_not_misread() {
        let key = KeyHash::from_key(b"rate:mw:client");
        let mut encoded = encode_report("peer-a", &[line(key, 1)]);
        encoded[0] = 2;

        assert_eq!(
            decode_report(&encoded, "self", 100),
            Err(ReportError::UnknownVersion(2))
        );
    }

    #[test]
    fn bytes_that_do_not_parse_are_refused() {
        let key = KeyHash::from_key(b"rate:mw:client");
        let whole = encode_report("peer-a", &[line(key, 1)]);

        // Empty, truncated header, truncated id, and a torn record.
        assert_eq!(decode_report(&[], "self", 100), Err(ReportError::Malformed));
        assert_eq!(
            decode_report(&[FORMAT_VERSION], "self", 100),
            Err(ReportError::Malformed)
        );
        assert_eq!(
            decode_report(&[FORMAT_VERSION, 10, b'x'], "self", 100),
            Err(ReportError::Malformed)
        );
        assert_eq!(
            decode_report(&whole[..whole.len() - 1], "self", 100),
            Err(ReportError::Malformed)
        );
    }

    #[test]
    fn records_that_cannot_be_folded_safely_are_skipped() {
        let key = KeyHash::from_key(b"rate:mw:client");
        let mut zero_limit = line(key, 1);
        zero_limit.limit = 0.0;
        let mut non_finite = line(key, 1);
        non_finite.burst = f64::INFINITY;
        let nothing_admitted = line(key, 0);

        let decoded = decode_report(
            &encode_report(
                "peer-a",
                &[zero_limit, non_finite, nothing_admitted, line(key, 2)],
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
        forever.ttl = Duration::from_secs(u64::MAX);

        let decoded = decode_report(&encode_report("peer-a", &[forever]), "self", 100).unwrap();

        assert_eq!(decoded[0].1.ttl, MAX_REPORTED_TTL);
    }
}
