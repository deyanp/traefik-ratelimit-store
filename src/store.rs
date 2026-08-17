//! The bucket store: sharded, expiring, bounded.
//!
//! Three properties matter and none is optional. The read-modify-write for one key is
//! atomic, because a token bucket that interleaves is not a rate limit. Expired entries
//! are reclaimed on a timer rather than only when the map is full, which is the defect
//! this store exists to avoid. And reclamation does not depend on the key being touched
//! again — an idle store must shrink, not merely stop growing.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Slots in an entry's consumption ring.
///
/// The ring spans `SLOT_COUNT` publish intervals, which must cover the staleness window a
/// peer applies to the report — otherwise a replica would under-report what it has taken.
const SLOT_COUNT: usize = 8;

/// How the store bounds itself.
#[derive(Clone, Copy, Debug)]
pub struct StoreConfig {
    /// Number of independently locked shards.
    pub shard_count: usize,
    /// Hard entry ceiling per shard. A backstop, not the mechanism.
    pub capacity_per_shard: usize,
    /// How often the background sweeper reclaims expired entries.
    pub sweep_interval: Duration,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            shard_count: 16,
            capacity_per_shard: 65_536,
            sweep_interval: Duration::from_secs(1),
        }
    }
}

/// One key's stored state, its lifetime, and what this replica has taken for it.
#[derive(Clone, Copy, Debug)]
struct Entry {
    state: BucketState,
    expires_at: Instant,
    /// Admissions per publish interval, oldest overwritten as the ring turns.
    slots: [u32; SLOT_COUNT],
    /// The tick the ring was last advanced to. Advancing is lazy, so an idle key costs
    /// nothing until it is touched or collected.
    slot_tick: u64,
}

impl Entry {
    fn advance_ring(&mut self, tick: u64) {
        let elapsed = tick.saturating_sub(self.slot_tick);

        if elapsed >= SLOT_COUNT as u64 {
            self.slots = [0; SLOT_COUNT];
        } else {
            for step in 1..=elapsed {
                self.slots[((self.slot_tick + step) as usize) % SLOT_COUNT] = 0;
            }
        }

        self.slot_tick = tick;
    }

    fn record_admission(&mut self, tick: u64) {
        self.slots[(tick as usize) % SLOT_COUNT] += 1;
    }

    fn consumed_in_window(&self) -> u32 {
        self.slots.iter().sum()
    }
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
    fn trim_least_recently_active(&mut self) {
        let mut activity: Vec<(KeyHash, f64)> = self
            .entries
            .iter()
            .map(|(key, entry)| (*key, entry.state.last))
            .collect();
        activity.sort_by(|left, right| left.1.total_cmp(&right.1));

        let drop_count = activity.len() / 10 + 1;
        for (key, _) in activity.into_iter().take(drop_count) {
            self.entries.remove(&key);
        }
    }
}

/// The store the connection handlers share.
#[derive(Debug)]
pub struct BucketStore {
    shards: Vec<Mutex<Shard>>,
    config: StoreConfig,
    /// Advanced once per publish interval; drives every entry's consumption ring.
    tick: AtomicU64,
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

        Self {
            shards,
            config,
            tick: AtomicU64::new(0),
        }
    }

    pub fn config(&self) -> StoreConfig {
        self.config
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
        let tick = self.tick.load(Ordering::Relaxed);
        let shard = &self.shards[key.shard_index(self.config.shard_count)];
        let mut shard = shard
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // An entry the sweeper has not reached yet must still read as absent.
        let existing = shard
            .entries
            .get(&key)
            .filter(|entry| entry.expires_at > now)
            .copied();
        let previous = existing.map(|entry| entry.state);

        let outcome = bucket::apply_request(previous, params);

        // A caller told to wait longer than it will accept is rejected, and a rejected
        // caller consumed nothing — so only an admission counts toward what peers are told.
        let admitted = outcome.wait <= params.max_delay;

        let (slots, slot_tick) = existing
            .map(|entry| (entry.slots, entry.slot_tick))
            .unwrap_or(([0; SLOT_COUNT], tick));
        let mut carried = Entry {
            state: outcome.state,
            expires_at: now + ttl,
            slots,
            slot_tick,
        };
        carried.advance_ring(tick);
        if admitted {
            carried.record_admission(tick);
        }

        if !shard.entries.contains_key(&key)
            && shard.entries.len() >= self.config.capacity_per_shard
        {
            shard.sweep_expired(now);
            if shard.entries.len() >= self.config.capacity_per_shard {
                shard.trim_least_recently_active();
            }
        }

        shard.entries.insert(key, carried);

        outcome
    }

    /// Advances the consumption ring by one publish interval and returns the tick used.
    pub fn advance_tick(&self) -> u64 {
        self.tick.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// What this replica has taken, per key, within the trailing window.
    ///
    /// Walks every shard once, which is the same walk the publish loop needs anyway.
    pub fn collect_consumption(&self) -> HashMap<KeyHash, u32> {
        let tick = self.tick.load(Ordering::Relaxed);
        let mut consumption = HashMap::new();

        for shard in &self.shards {
            let mut shard = shard
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            for (key, entry) in shard.entries.iter_mut() {
                entry.advance_ring(tick);
                let consumed = entry.consumed_in_window();
                if consumed > 0 {
                    consumption.insert(*key, consumed);
                }
            }
        }

        consumption
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
            peer_consumed: 0.0,
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
    fn admissions_are_counted_for_peers() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());
        let key = KeyHash::from_key(b"rate:mw:client");

        for _ in 0..3 {
            store.apply_request(key, &params_at(REALISTIC_NOW), TTL, start);
        }

        assert_eq!(store.collect_consumption().get(&key), Some(&3));
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

        let consumed = *store.collect_consumption().get(&key).unwrap();
        assert!(
            consumed < 40,
            "rejected requests must not be counted, got {consumed}"
        );
    }

    #[test]
    fn consumption_ages_out_of_the_window() {
        let start = Instant::now();
        let store = BucketStore::new(test_config());
        let key = KeyHash::from_key(b"rate:mw:client");

        store.apply_request(
            key,
            &params_at(REALISTIC_NOW),
            Duration::from_secs(60),
            start,
        );
        assert_eq!(store.collect_consumption().get(&key), Some(&1));

        // A full turn of the ring, which is what a peer's staleness window spans.
        for _ in 0..SLOT_COUNT {
            store.advance_tick();
        }

        assert_eq!(store.collect_consumption().get(&key), None);
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
    fn hashing_is_stable_and_distinguishes_keys() {
        assert_eq!(KeyHash::from_key(b"same"), KeyHash::from_key(b"same"));
        assert_ne!(KeyHash::from_key(b"one"), KeyHash::from_key(b"two"));
    }
}
