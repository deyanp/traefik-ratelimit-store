//! The bucket store: sharded, expiring, bounded.
//!
//! Three properties matter and none is optional. The read-modify-write for one key is
//! atomic, because a token bucket that interleaves is not a rate limit. Expired entries
//! are reclaimed on a timer rather than only when the map is full, which is the defect
//! this store exists to avoid. And reclamation does not depend on the key being touched
//! again — an idle store must shrink, not merely stop growing.

use std::collections::HashMap;
use std::collections::hash_map::Entry as MapEntry;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use sha1::{Digest, Sha1};

use crate::bucket::{self, BucketOutcome, BucketParams, BucketState};

/// The digest of a rate-limit key.
///
/// Keys are hashed on arrival so the caller's raw source — which may be a credential —
/// never exists at rest. 128 bits makes a collision unreachable at any plausible key count.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct KeyHash(u128);

impl KeyHash {
    pub fn from_key(raw: &[u8]) -> Self {
        let digest = Sha1::digest(raw);
        let mut truncated = [0u8; 16];
        truncated.copy_from_slice(&digest[..16]);
        Self(u128::from_be_bytes(truncated))
    }

    fn shard_index(&self, shard_count: usize) -> usize {
        (self.0 % shard_count as u128) as usize
    }

    /// The wire form, because a JSON map key must be a string.
    pub fn to_hex(self) -> String {
        format!("{:032x}", self.0)
    }

    pub fn from_hex(text: &str) -> Option<Self> {
        u128::from_str_radix(text, 16).ok().map(Self)
    }
}

/// How the store bounds itself.
#[derive(Clone, Copy, Debug)]
pub struct StoreConfig {
    /// Number of independently locked shards.
    pub shard_count: usize,
    /// Hard entry ceiling per shard. A backstop, not the mechanism.
    ///
    /// Derived from the memory budget at startup (see `memory_budget`), because the
    /// ceiling and the container's memory limit are one decision: an entry costs a few
    /// hundred bytes once the map has grown, so a ceiling above the limit means the
    /// process is killed before the trim that exists to prevent that ever runs.
    pub capacity_per_shard: usize,
    /// How often the background sweeper reclaims expired entries.
    pub sweep_interval: Duration,
}

impl Default for StoreConfig {
    /// Sized for tests and examples. The binary derives `capacity_per_shard` from the
    /// memory budget instead of taking this.
    fn default() -> Self {
        Self {
            shard_count: 16,
            capacity_per_shard: 8_192,
            sweep_interval: Duration::from_secs(1),
        }
    }
}

/// One key's stored state, its lifetime, and what this replica still owes its peers.
#[derive(Clone, Copy, Debug)]
struct Entry {
    state: BucketState,
    expires_at: Instant,
    /// The configuration the last request for this key carried. Sent with the next report
    /// so a peer can fold this replica's admissions into its own bucket before it has
    /// seen a request for the key itself. Never used for a local decision.
    limit: f64,
    burst: f64,
    /// Admissions not yet reported to peers. Reset when a report carries them.
    unpublished: u32,
}

/// This replica's admissions for one key since the last report, with what a peer needs
/// to fold them into its own bucket.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct KeyReport {
    pub key: KeyHash,
    pub admitted: u32,
    /// Timestamp of the last admission, in the caller's microseconds.
    pub last: f64,
    pub limit: f64,
    pub burst: f64,
    /// How long the entry has left to live here, so the peer's copy expires alongside it.
    pub ttl: Duration,
}

/// Admissions a peer made for one key, as received.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PeerAdmissions {
    pub admitted: u32,
    pub last: f64,
    pub limit: f64,
    pub burst: f64,
    pub ttl: Duration,
}

#[derive(Debug)]
struct Shard {
    entries: HashMap<KeyHash, Entry>,
}

impl Shard {
    /// Drops every entry whose lifetime has elapsed.
    ///
    /// A full scan is correct here because the resident set is bounded by the traffic of
    /// the last couple of seconds, not by uptime — which is the point of sweeping at all.
    fn sweep_expired(&mut self, now: Instant) {
        self.entries.retain(|_, entry| entry.expires_at > now);
    }

    /// Last-resort trim when a shard is at capacity with live entries.
    ///
    /// Drops the least recently active tenth. This should never run: reaching it means
    /// distinct keys are arriving faster than they expire, which is a capacity or a
    /// traffic anomaly rather than normal operation.
    ///
    /// Selects the cut point rather than sorting. The shard's mutex is held throughout, so
    /// every request hashing here waits for it — and at capacity a full sort costs about
    /// two milliseconds, sixteen times the normal p99. Partitioning is linear and needs
    /// only the boundary, since which particular entries go is not important.
    fn trim_least_recently_active(&mut self) {
        let mut activity: Vec<f64> = self
            .entries
            .values()
            .map(|entry| entry.state.last)
            .collect();

        let drop_count = (activity.len() / 10 + 1).min(activity.len());
        let (_, cut, _) =
            activity.select_nth_unstable_by(drop_count - 1, |left, right| left.total_cmp(right));
        let threshold = *cut;

        self.entries.retain(|_, entry| entry.state.last > threshold);
    }

    /// Makes room for one more key when the shard is at its ceiling.
    ///
    /// Expired entries go first; only if that frees nothing does the trim run. Returns
    /// whether the trim ran, so the caller can report it.
    fn make_room(&mut self, capacity: usize, now: Instant) -> bool {
        if self.entries.len() < capacity {
            return false;
        }

        self.sweep_expired(now);
        if self.entries.len() < capacity {
            return false;
        }

        self.trim_least_recently_active();
        true
    }
}

/// The store the connection handlers share.
#[derive(Debug)]
pub struct BucketStore {
    shards: Vec<Mutex<Shard>>,
    config: StoreConfig,
}

impl BucketStore {
    pub fn new(config: StoreConfig) -> Self {
        let shards = (0..config.shard_count)
            .map(|_| {
                Mutex::new(Shard {
                    entries: HashMap::new(),
                })
            })
            .collect();

        Self { shards, config }
    }

    pub fn config(&self) -> StoreConfig {
        self.config
    }

    fn lock_shard(&self, key: KeyHash) -> std::sync::MutexGuard<'_, Shard> {
        self.shards[key.shard_index(self.config.shard_count)]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn warn_at_capacity(&self, remaining: usize) {
        // The one condition an operator cannot infer from anything else. Distinct keys are
        // arriving faster than they expire, so the store is shedding the least recently
        // active — which means some sources are being admitted against a fresh bucket. It
        // is naturally throttled: a trim frees a tenth of a shard, so this cannot fire
        // again until that tenth is refilled.
        let (event_id, event_name) = crate::log_events::STORE_AT_CAPACITY;
        tracing::warn!(
            event_id,
            event_name,
            ceiling = self.config.capacity_per_shard,
            remaining,
            "shard at capacity; shedding least recently active keys"
        );
    }

    /// Applies one request to `key`, atomically, and stores the result.
    ///
    /// The caller supplies both clocks deliberately: `params.now` is the caller's own
    /// timestamp and drives the bucket arithmetic, while `now` is this process's
    /// monotonic clock and drives expiry — so a skewed caller cannot influence eviction.
    pub fn apply_request(
        &self,
        key: KeyHash,
        params: &BucketParams,
        ttl: Duration,
        now: Instant,
    ) -> BucketOutcome {
        let mut shard = self.lock_shard(key);

        // One probe for the common case. The ceiling is checked only for a key the shard
        // does not hold, which is the only insert that can grow it.
        let (previous, unpublished) = match shard.entries.get(&key) {
            // An entry the sweeper has not reached yet must still read as absent.
            Some(entry) if entry.expires_at > now => (Some(entry.state), entry.unpublished),
            Some(_) => (None, 0),
            None => {
                if shard.make_room(self.config.capacity_per_shard, now) {
                    self.warn_at_capacity(shard.entries.len());
                }
                (None, 0)
            }
        };

        let outcome = bucket::apply_request(previous, params);

        // A caller told to wait longer than it will accept is rejected, and a rejected
        // caller consumed nothing — so only an admission counts toward what peers are told.
        let admitted = outcome.wait <= params.max_delay;

        shard.entries.insert(
            key,
            Entry {
                state: outcome.state,
                expires_at: now + ttl,
                limit: params.limit,
                burst: params.burst,
                unpublished: unpublished.saturating_add(u32::from(admitted)),
            },
        );

        outcome
    }

    /// Folds admissions a peer reports for `key` into this replica's own bucket.
    ///
    /// Applied once, at receipt, as a debit at the moment the peer says they happened.
    /// Nothing is remembered about the peer afterwards: the level now simply reflects
    /// those admissions, exactly as it would had they been made here.
    pub fn apply_peer_admissions(&self, key: KeyHash, peer: PeerAdmissions, now: Instant) {
        let mut shard = self.lock_shard(key);

        let live = shard
            .entries
            .get(&key)
            .filter(|entry| entry.expires_at > now)
            .copied();

        if live.is_none() && shard.make_room(self.config.capacity_per_shard, now) {
            self.warn_at_capacity(shard.entries.len());
        }

        // A key this replica has seen is folded with the configuration it saw itself; the
        // peer's copy is needed only for a key it has not.
        let (limit, burst) = match live {
            Some(entry) => (entry.limit, entry.burst),
            None => (peer.limit, peer.burst),
        };
        let state = bucket::apply_peer_admissions(
            live.map(|entry| entry.state),
            f64::from(peer.admitted),
            peer.last,
            limit,
            burst,
        );

        let expires_at = match live {
            Some(entry) => entry.expires_at.max(now + peer.ttl),
            None => now + peer.ttl,
        };

        match shard.entries.entry(key) {
            MapEntry::Occupied(mut occupied) if live.is_some() => {
                let entry = occupied.get_mut();
                entry.state = state;
                entry.expires_at = expires_at;
            }
            MapEntry::Occupied(mut occupied) => {
                occupied.insert(Entry {
                    state,
                    expires_at,
                    limit: peer.limit,
                    burst: peer.burst,
                    unpublished: 0,
                });
            }
            MapEntry::Vacant(vacant) => {
                vacant.insert(Entry {
                    state,
                    expires_at,
                    limit: peer.limit,
                    burst: peer.burst,
                    unpublished: 0,
                });
            }
        }
    }

    /// What this replica has admitted since the last report, per key, and marks it sent.
    ///
    /// Each shard is locked once, for a read and a reset per entry; the selection below
    /// runs with no lock held.
    ///
    /// Truncated to the `limit` keys with the most unreported admissions. A report carries
    /// one entry per key touched since the last one, so a wide keyspace would otherwise
    /// produce a body of several megabytes to every peer several times a second. Nothing is
    /// lost by the cap: a key left out keeps its count and goes in a later report, so the
    /// cap only delays the keys where sharing decides the least.
    pub fn collect_report(&self, limit: usize, now: Instant) -> Vec<KeyReport> {
        let mut report = Vec::new();

        for shard in &self.shards {
            let mut shard = shard
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            for (key, entry) in shard.entries.iter_mut() {
                if entry.unpublished == 0 || entry.expires_at <= now {
                    continue;
                }
                report.push(KeyReport {
                    key: *key,
                    admitted: entry.unpublished,
                    last: entry.state.last,
                    limit: entry.limit,
                    burst: entry.burst,
                    ttl: entry.expires_at - now,
                });
                entry.unpublished = 0;
            }
        }

        if report.len() > limit {
            // Keep the busiest; hand the rest their counts back for a later report.
            report.select_nth_unstable_by(limit.saturating_sub(1), |left, right| {
                right.admitted.cmp(&left.admitted)
            });
            for deferred in report.drain(limit..) {
                if let Some(entry) = self.lock_shard(deferred.key).entries.get_mut(&deferred.key) {
                    entry.unpublished = entry.unpublished.saturating_add(deferred.admitted);
                }
            }
        }

        report
    }

    /// Reclaims expired entries across every shard.
    ///
    /// Driven by a timer rather than by traffic, so a store that goes idle — or whose
    /// traffic is skewed onto a few shards — still gives its memory back.
    pub fn sweep_expired(&self, now: Instant) {
        for shard in &self.shards {
            shard
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .sweep_expired(now);
        }
    }

    /// Total entries held, including any expired but not yet reclaimed.
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .entries
                    .len()
            })
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: f64 = 3.0 / 1_000_000.0;
    const BURST: f64 = 10.0;
    const REALISTIC_NOW: f64 = 1_787_000_000_000_000.0;
    const TTL: Duration = Duration::from_secs(2);

    fn params_at(now: f64) -> BucketParams {
        BucketParams {
            limit: LIMIT,
            burst: BURST,
            now,
            max_delay: 166_666.0,
        }
    }

    fn test_config() -> StoreConfig {
        StoreConfig {
            shard_count: 4,
            capacity_per_shard: 8,
            sweep_interval: Duration::from_secs(1),
        }
    }

    #[test]
    fn consecutive_requests_share_one_bucket() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());
        let key = KeyHash::from_key(b"rate:mw:client");

        let first = store.apply_request(key, &params_at(REALISTIC_NOW), TTL, start);
        let second = store.apply_request(key, &params_at(REALISTIC_NOW), TTL, start);

        assert_eq!(first.state.tokens, BURST - 1.0);
        assert_eq!(second.state.tokens, BURST - 2.0);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn distinct_keys_do_not_share_a_bucket() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());

        store.apply_request(
            KeyHash::from_key(b"rate:mw:a"),
            &params_at(REALISTIC_NOW),
            TTL,
            start,
        );
        let other = store.apply_request(
            KeyHash::from_key(b"rate:mw:b"),
            &params_at(REALISTIC_NOW),
            TTL,
            start,
        );

        assert_eq!(other.state.tokens, BURST - 1.0);
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn an_expired_entry_reads_as_absent_before_the_sweeper_reaches_it() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());
        let key = KeyHash::from_key(b"rate:mw:client");

        store.apply_request(key, &params_at(REALISTIC_NOW), TTL, start);
        let after_expiry = store.apply_request(
            key,
            &params_at(REALISTIC_NOW),
            TTL,
            start + Duration::from_secs(3),
        );

        // A full bucket again, because the previous state is no longer valid.
        assert_eq!(after_expiry.state.tokens, BURST - 1.0);
    }

    #[test]
    fn an_idle_store_gives_its_memory_back() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());

        for index in 0..6u32 {
            store.apply_request(
                KeyHash::from_key(format!("rate:mw:{index}").as_bytes()),
                &params_at(REALISTIC_NOW),
                TTL,
                start,
            );
        }
        assert_eq!(store.len(), 6);

        // No further traffic at all: nothing looks these keys up, no shard is touched, and
        // capacity is nowhere near. Only the timer-driven sweep can reclaim them.
        store.sweep_expired(start + Duration::from_secs(5));

        assert_eq!(store.len(), 0);
    }

    #[test]
    fn sweeping_reaches_every_shard_not_only_the_busy_ones() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());

        // Enough distinct keys to be spread over all four shards.
        for index in 0..40u32 {
            store.apply_request(
                KeyHash::from_key(format!("rate:mw:{index}").as_bytes()),
                &params_at(REALISTIC_NOW),
                TTL,
                start,
            );
        }

        store.sweep_expired(start + Duration::from_secs(5));

        assert_eq!(store.len(), 0);
    }

    #[test]
    fn live_entries_survive_a_sweep() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());

        store.apply_request(
            KeyHash::from_key(b"rate:mw:short"),
            &params_at(REALISTIC_NOW),
            Duration::from_secs(2),
            start,
        );
        store.apply_request(
            KeyHash::from_key(b"rate:mw:long"),
            &params_at(REALISTIC_NOW),
            Duration::from_secs(60),
            start,
        );

        store.sweep_expired(start + Duration::from_secs(5));

        assert_eq!(store.len(), 1);
    }

    #[test]
    fn capacity_is_never_exceeded_by_live_entries() {
        let start = Instant::now();
        let config = test_config();
        let store = BucketStore::new(config);

        // Far more distinct keys than the shards can hold, all of them live.
        for index in 0..500u32 {
            store.apply_request(
                KeyHash::from_key(format!("rate:mw:{index}").as_bytes()),
                &params_at(REALISTIC_NOW),
                Duration::from_secs(60),
                start,
            );
        }

        assert!(store.len() <= config.shard_count * config.capacity_per_shard);
    }

    #[test]
    fn admissions_are_reported_to_peers_once() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());
        let key = KeyHash::from_key(b"rate:mw:client");

        for _ in 0..3 {
            store.apply_request(key, &params_at(REALISTIC_NOW), TTL, start);
        }

        let report = store.collect_report(usize::MAX, start);
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].key, key);
        assert_eq!(report[0].admitted, 3);
        assert_eq!(report[0].last, REALISTIC_NOW);
        assert_eq!(report[0].limit, LIMIT);
        assert_eq!(report[0].burst, BURST);
        assert_eq!(report[0].ttl, TTL);

        // Carried once: the next report says nothing about these three.
        assert!(store.collect_report(usize::MAX, start).is_empty());
    }

    #[test]
    fn a_rejected_request_consumes_nothing() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());
        let key = KeyHash::from_key(b"rate:mw:client");

        // Drain the burst, then keep asking. Once the wait exceeds max_delay the caller is
        // turned away, and a caller that was turned away took nothing to tell peers about.
        for _ in 0..40 {
            store.apply_request(key, &params_at(REALISTIC_NOW), TTL, start);
        }

        let admitted = store.collect_report(usize::MAX, start)[0].admitted;
        assert_eq!(admitted, BURST as u32, "only the burst was admitted");
    }

    #[test]
    fn an_expired_entry_is_not_reported() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());
        let key = KeyHash::from_key(b"rate:mw:client");

        store.apply_request(key, &params_at(REALISTIC_NOW), TTL, start);

        assert!(
            store
                .collect_report(usize::MAX, start + Duration::from_secs(3))
                .is_empty()
        );
    }

    #[test]
    fn peer_admissions_are_debited_from_the_local_bucket() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());
        let key = KeyHash::from_key(b"rate:mw:client");

        store.apply_request(key, &params_at(REALISTIC_NOW), TTL, start);
        store.apply_peer_admissions(
            key,
            PeerAdmissions {
                admitted: 4,
                last: REALISTIC_NOW,
                limit: LIMIT,
                burst: BURST,
                ttl: TTL,
            },
            start,
        );

        let outcome = store.apply_request(key, &params_at(REALISTIC_NOW), TTL, start);

        // One taken here, four by the peer, one by this request.
        assert_eq!(outcome.state.tokens, BURST - 6.0);
        // What peers did is not re-reported to them.
        assert_eq!(store.collect_report(usize::MAX, start)[0].admitted, 2);
    }

    #[test]
    fn peer_admissions_create_the_entry_for_an_unseen_key() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());
        let key = KeyHash::from_key(b"rate:mw:client");

        store.apply_peer_admissions(
            key,
            PeerAdmissions {
                admitted: 9,
                last: REALISTIC_NOW,
                limit: LIMIT,
                burst: BURST,
                ttl: TTL,
            },
            start,
        );
        assert_eq!(store.len(), 1);

        // The peer took nine of ten; this request takes the last one.
        let outcome = store.apply_request(key, &params_at(REALISTIC_NOW), TTL, start);
        assert_eq!(outcome.state.tokens, 0.0);
        assert_eq!(outcome.wait, 0.0);
    }

    #[test]
    fn an_entry_created_by_peers_expires_with_the_peers_ttl() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());
        let key = KeyHash::from_key(b"rate:mw:client");

        store.apply_peer_admissions(
            key,
            PeerAdmissions {
                admitted: 9,
                last: REALISTIC_NOW,
                limit: LIMIT,
                burst: BURST,
                ttl: Duration::from_secs(2),
            },
            start,
        );

        store.sweep_expired(start + Duration::from_secs(3));

        assert_eq!(store.len(), 0);
    }

    #[test]
    fn peer_admissions_extend_an_existing_entrys_lifetime() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());
        let key = KeyHash::from_key(b"rate:mw:client");

        store.apply_request(
            key,
            &params_at(REALISTIC_NOW),
            Duration::from_secs(2),
            start,
        );
        store.apply_peer_admissions(
            key,
            PeerAdmissions {
                admitted: 1,
                last: REALISTIC_NOW,
                limit: LIMIT,
                burst: BURST,
                ttl: Duration::from_secs(60),
            },
            start,
        );

        store.sweep_expired(start + Duration::from_secs(5));

        assert_eq!(store.len(), 1);
    }

    #[test]
    fn a_key_hash_survives_its_wire_form() {
        let key = KeyHash::from_key(b"rate:mw:client");

        assert_eq!(KeyHash::from_hex(&key.to_hex()), Some(key));
    }

    #[test]
    fn concurrent_requests_on_one_key_admit_exactly_the_burst() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // The property the whole store rests on. Every other test drives it from one
        // thread, which cannot distinguish a correct lock from no lock at all: a bucket
        // whose read-modify-write interleaves is not a rate limit, it is a suggestion.
        let start = Instant::now();
        let store = Arc::new(BucketStore::new(StoreConfig::default()));
        let key = KeyHash::from_key(b"rate:mw:contended");
        let admitted = Arc::new(AtomicUsize::new(0));
        let max_delay = 166_666.0;

        let threads: Vec<_> = (0..16)
            .map(|_| {
                let store = store.clone();
                let admitted = admitted.clone();
                std::thread::spawn(move || {
                    for _ in 0..50 {
                        let outcome =
                            store.apply_request(key, &params_at(REALISTIC_NOW), TTL, start);
                        if outcome.wait <= max_delay {
                            admitted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();

        for thread in threads {
            thread.join().expect("no thread may panic");
        }

        // 800 requests at one instant against a burst of 10. Exactly the burst is
        // admitted; anything more means increments were lost between threads.
        assert_eq!(admitted.load(Ordering::Relaxed), BURST as usize);
    }

    #[test]
    fn the_capacity_trim_keeps_the_most_recently_active() {
        let start = Instant::now();
        let config = StoreConfig {
            shard_count: 1,
            capacity_per_shard: 100,
            sweep_interval: Duration::from_secs(1),
        };
        let store = BucketStore::new(config);

        // Distinct activity timestamps, so oldest and newest are unambiguous.
        for index in 0..100u32 {
            store.apply_request(
                KeyHash::from_key(format!("k{index}").as_bytes()),
                &params_at(REALISTIC_NOW + f64::from(index)),
                Duration::from_secs(600),
                start,
            );
        }

        // The insert that tips the shard over its capacity triggers the trim.
        store.apply_request(
            KeyHash::from_key(b"newest"),
            &params_at(REALISTIC_NOW + 1000.0),
            Duration::from_secs(600),
            start,
        );

        // The oldest went; the newest stayed. Which particular entries were dropped is not
        // important, only that recency decided it.
        assert!(store.len() < 100, "the trim must have dropped something");
        assert_eq!(
            store
                .apply_request(
                    KeyHash::from_key(b"k0"),
                    &params_at(REALISTIC_NOW + 1001.0),
                    Duration::from_secs(600),
                    start,
                )
                .state
                .tokens,
            BURST - 1.0,
            "the least recently active entry should have been dropped"
        );
    }

    #[test]
    fn the_capacity_trim_leaves_room_to_insert() {
        let start = Instant::now();
        let config = StoreConfig {
            shard_count: 1,
            capacity_per_shard: 50,
            sweep_interval: Duration::from_secs(1),
        };
        let store = BucketStore::new(config);

        // Sustained overflow: far more distinct live keys than the shard can hold.
        for index in 0..500u32 {
            store.apply_request(
                KeyHash::from_key(format!("k{index}").as_bytes()),
                &params_at(REALISTIC_NOW + f64::from(index)),
                Duration::from_secs(600),
                start,
            );
        }

        // Every insert still succeeded and the ceiling held throughout.
        assert!(store.len() <= 50);
        assert!(!store.is_empty(), "trimming must not empty the shard");
    }

    #[test]
    fn a_report_is_truncated_to_the_busiest_keys_and_the_rest_wait_their_turn() {
        let start = Instant::now();
        let store = BucketStore::new(StoreConfig::default());

        // Ten keys, each taken a different number of times.
        for index in 1..=10u32 {
            let key = KeyHash::from_key(format!("k{index}").as_bytes());
            for _ in 0..index {
                store.apply_request(key, &params_at(REALISTIC_NOW), TTL, start);
            }
        }

        let capped = store.collect_report(3, start);
        assert_eq!(capped.len(), 3, "got {} keys", capped.len());

        // What goes first is what matters most: the keys this replica has taken most for,
        // since those are the ones a peer's level is furthest from.
        let smallest_sent = capped.iter().map(|line| line.admitted).min().unwrap();
        assert!(
            smallest_sent >= 8,
            "the busiest keys should go first, sent a key with only {smallest_sent}"
        );

        // Nothing was lost: the deferred keys go in the next report, counts intact.
        let rest = store.collect_report(usize::MAX, start);
        assert_eq!(rest.len(), 7);
        assert_eq!(
            rest.iter().map(|line| line.admitted).sum::<u32>(),
            1 + 2 + 3 + 4 + 5 + 6 + 7
        );
        assert!(store.collect_report(usize::MAX, start).is_empty());
    }

    #[test]
    fn a_flood_of_new_keys_evicts_the_quiet_and_keeps_the_busy() {
        // What happens when the ceiling is reached and distinct keys keep arriving. The
        // ceiling bounds memory, so something has to go; the question is what.
        let start = Instant::now();
        let config = StoreConfig {
            shard_count: 1,
            capacity_per_shard: 200,
            sweep_interval: Duration::from_secs(1),
        };
        let store = BucketStore::new(config);
        let busy = KeyHash::from_key(b"rate:mw:busy");

        // A steady client, interleaved with a flood of keys seen once each.
        for index in 0..5_000u32 {
            let now = REALISTIC_NOW + f64::from(index) * 1_000.0;
            store.apply_request(busy, &params_at(now), TTL, start);
            store.apply_request(
                KeyHash::from_key(format!("rate:mw:flood-{index}").as_bytes()),
                &params_at(now),
                TTL,
                start,
            );
        }

        // Memory stayed bounded through 5,000 distinct arrivals.
        assert!(store.len() <= 200, "held {} entries", store.len());

        // And the steady client kept its bucket. Eviction is by least-recent activity, so
        // the flood evicts itself: a source seen once is the least recently active thing
        // in the shard, while a source still sending is the last thing to go. The client
        // worth limiting stays limited; the ones dropped were nowhere near their limit.
        let outcome =
            store.apply_request(busy, &params_at(REALISTIC_NOW + 5_000_000.0), TTL, start);
        assert!(
            outcome.state.tokens < BURST - 1.0,
            "the busy key was evicted and got a fresh bucket: {}",
            outcome.state.tokens
        );
    }

    #[test]
    fn hashing_is_stable_and_distinguishes_keys() {
        assert_eq!(KeyHash::from_key(b"same"), KeyHash::from_key(b"same"));
        assert_ne!(KeyHash::from_key(b"one"), KeyHash::from_key(b"two"));
    }
}
