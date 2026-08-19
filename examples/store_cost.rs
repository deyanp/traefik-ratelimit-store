//! Measures the store's own operations, without a socket in the way.
//!
//! `latency` measures what a request costs end to end; this measures where that time goes
//! and, more importantly, what the rare operations cost. Both matter for a different
//! reason: the proxy has no fail-open switch, so any operation that can stall the request
//! path is a correctness risk once it approaches the read timeout.
//!
//! An example rather than a test, because timings assert more about the machine than the
//! code. The correctness of what is measured here is covered by unit tests.
//!
//! Usage: cargo run --release --example store_cost

use std::time::{Duration, Instant};

use traefik_ratelimit_store::bucket::BucketParams;
use traefik_ratelimit_store::store::{BucketStore, KeyHash, StoreConfig};

const REALISTIC_NOW: f64 = 1_787_000_000_000_000.0;
const TTL: Duration = Duration::from_secs(600);

/// Room for every key these measurements insert. The default ceiling is sized for tests;
/// measuring against it would time the capacity trim rather than the operation.
fn roomy_config() -> StoreConfig {
    StoreConfig {
        capacity_per_shard: 1 << 20,
        ..StoreConfig::default()
    }
}

fn params(now: f64) -> BucketParams {
    BucketParams {
        limit: 3.0 / 1_000_000.0,
        burst: 10.0,
        now,
        max_delay: 166_666.0,
    }
}

fn report(label: &str, duration: Duration, operations: usize) {
    let each = duration.as_nanos() as f64 / operations as f64;
    println!("{label:<34} {each:>9.0}ns/op   ({operations} ops in {duration:?})");
}

/// The hot path: one key, repeatedly, which is what a busy client looks like.
fn measure_apply_request(store: &BucketStore, start: Instant) {
    let key = KeyHash::from_key(b"rate:bench:hot");
    let operations = 200_000;

    let began = Instant::now();
    for index in 0..operations {
        store.apply_request(key, &params(REALISTIC_NOW + index as f64), TTL, start);
    }
    report("apply_request, one key", began.elapsed(), operations);
}

/// The same, across a wide keyspace, so hashing and map growth are included.
fn measure_apply_request_wide(store: &BucketStore, start: Instant) {
    let operations = 200_000;

    let began = Instant::now();
    for index in 0..operations {
        let key = KeyHash::from_key(format!("rate:bench:{index}").as_bytes());
        store.apply_request(key, &params(REALISTIC_NOW + index as f64), TTL, start);
    }
    report("apply_request, distinct keys", began.elapsed(), operations);
}

/// The timer-driven reclaim, over a store holding a realistic resident set.
fn measure_sweep(start: Instant) {
    let store = BucketStore::new(roomy_config());
    let count = 200_000;
    for index in 0..count {
        store.apply_request(
            KeyHash::from_key(format!("rate:bench:{index}").as_bytes()),
            &params(REALISTIC_NOW + index as f64),
            Duration::from_secs(2),
            start,
        );
    }

    let began = Instant::now();
    store.sweep_expired(start + Duration::from_secs(5));
    println!(
        "{:<34} {:>9?}   ({count} entries reclaimed)",
        "sweep_expired, whole store",
        began.elapsed()
    );
}

/// The rare one, and the only operation that can stall the request path.
///
/// A shard at capacity trims on insert while holding its own mutex, so every request
/// hashing to that shard waits for it.
fn measure_capacity_trim(start: Instant) {
    let capacity = 65_536;
    let config = StoreConfig {
        shard_count: 1,
        capacity_per_shard: capacity,
        sweep_interval: Duration::from_secs(1),
    };
    let store = BucketStore::new(config);

    for index in 0..capacity {
        store.apply_request(
            KeyHash::from_key(format!("k{index}").as_bytes()),
            &params(REALISTIC_NOW + index as f64),
            TTL,
            start,
        );
    }

    // The insert that tips the shard over is the one that trims.
    let began = Instant::now();
    store.apply_request(
        KeyHash::from_key(b"overflow"),
        &params(REALISTIC_NOW),
        TTL,
        start,
    );
    println!(
        "{:<34} {:>9?}   (one shard of {capacity}, holding its mutex)",
        "capacity trim, worst case",
        began.elapsed()
    );
}

fn main() {
    let start = Instant::now();
    let store = BucketStore::new(roomy_config());

    measure_apply_request(&store, start);
    measure_apply_request_wide(&store, start);
    measure_sweep(start);
    measure_capacity_trim(start);
}
