//! Measures what a distinct rate-limit key costs, in memory and in peer bandwidth.
//!
//! Hashing the key gives a fixed size and non-reversibility; it does not reduce how many
//! entries the store holds. One distinct source is one entry either way, so cardinality is
//! what has to be sized against the container's memory limit — and the ceiling that is
//! supposed to prevent an overrun has to sit below that limit, not above it.
//!
//! Usage: cargo run --release --example memory_per_key

use std::time::{Duration, Instant};
use traefik_ratelimit_store::bucket::BucketParams;
use traefik_ratelimit_store::store::{BucketStore, KeyHash, StoreConfig};

fn rss_kb() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0)
}

fn main() {
    let start = Instant::now();
    // Room for the last milestone — and not more: the tables are allocated for the
    // ceiling up front, so what this measures is the shipped story for a store sized to
    // hold a million keys.
    let store = BucketStore::new(StoreConfig {
        capacity_per_shard: 64 * 1024,
        ..StoreConfig::default()
    });
    let params = BucketParams {
        limit: 3.0 / 1_000_000.0,
        burst: 10.0,
        now: 1_787_000_000_000_000.0,
        max_delay: 166_666.0,
    };
    let base = rss_kb();
    println!("baseline: {base}KB");
    for milestone in [100_000u32, 250_000, 500_000, 1_000_000] {
        while store.len() < milestone as usize {
            let i = store.len() as u32;
            store.apply_request(
                KeyHash::from_key(format!("rate:mw:client-{i}").as_bytes()),
                &params,
                Duration::from_secs(600),
                start,
            );
            if store.len() as u32 == i {
                break;
            }
        }
        let now = rss_kb();
        println!(
            "{:>9} keys: {:>7}KB total, {:>4} bytes/key",
            store.len(),
            now,
            ((now - base) * 1024) / store.len().max(1) as u64
        );
    }
    // Report size: one 48-byte record per key touched since the last report.
    let lines = store.collect_report(usize::MAX, start);
    println!(
        "peer report for {} keys: {} bytes",
        lines.len(),
        traefik_ratelimit_store::peers::encode_report("r", &lines).len()
    );
}
