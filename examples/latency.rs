//! Measures what the store costs a request.
//!
//! Drives a running store over TCP with the same wire traffic the proxy sends, and reports
//! the distribution. The number that matters is the tail: the proxy answers any store error
//! with a 500 and has a tight read timeout, so a p99 approaching that timeout is a
//! correctness problem rather than a performance one.
//!
//! An example rather than a test: it needs a running store, and a timing assertion in CI
//! measures the runner more than the code.
//!
//! Usage: cargo run --release --example latency -- [address] [connections] [requests-each]

use std::env;
use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The reference script, addressed by digest exactly as the proxy addresses it.
const SCRIPT: &str = traefik_ratelimit_store::script::PINNED_SCRIPT;

fn bulk(value: &str) -> String {
    format!("${}\r\n{}\r\n", value.len(), value)
}

/// One evaluation command, as the client would frame it.
fn evaluation(command: &str, script_or_digest: &str, key: &str, now: i64) -> String {
    let args = [
        command,
        script_or_digest,
        "1",
        key,
        "0.0001",
        "1000000",
        "2",
        &now.to_string(),
        "500000",
    ];

    let mut out = format!("*{}\r\n", args.len());
    for arg in args {
        out.push_str(&bulk(arg));
    }
    out
}

fn micros_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is before the epoch")
        .as_micros() as i64
}

/// Runs one connection's share and returns its latencies in microseconds.
async fn measure_connection(address: String, key: String, requests: usize) -> Vec<u64> {
    let mut stream = TcpStream::connect(&address)
        .await
        .expect("the store must be listening");
    // The real client sets this, and without it Nagle batches these small writes — which
    // would make this measure the harness rather than the store.
    stream.set_nodelay(true).expect("nodelay");
    let mut buffer = [0u8; 4096];

    // Register the script once, exactly as the client does on its first call.
    stream
        .write_all(evaluation("EVAL", SCRIPT, &key, micros_now()).as_bytes())
        .await
        .expect("write");
    stream.read(&mut buffer).await.expect("read");

    let digest = traefik_ratelimit_store::script::compute_digest(SCRIPT);
    let mut latencies = Vec::with_capacity(requests);

    for _ in 0..requests {
        let command = evaluation("EVALSHA", &digest, &key, micros_now());

        let started = Instant::now();
        stream.write_all(command.as_bytes()).await.expect("write");
        stream.read(&mut buffer).await.expect("read");
        latencies.push(started.elapsed().as_micros() as u64);
    }

    latencies
}

fn percentile(sorted: &[u64], fraction: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() as f64 - 1.0) * fraction).round() as usize;
    sorted[index]
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let address = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:16379".to_string());
    let connections: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(16);
    let each: usize = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(2_000);

    println!("{connections} connections x {each} requests against {address}");

    let started = Instant::now();
    let mut handles = Vec::with_capacity(connections);
    for index in 0..connections {
        // Distinct keys, so this measures the store rather than lock contention on one
        // shard. Contention is measured separately, by the concurrency unit test.
        let key = format!("rate:bench:client-{index}");
        handles.push(tokio::spawn(measure_connection(
            address.clone(),
            key,
            each,
        )));
    }

    let mut latencies = Vec::with_capacity(connections * each);
    for handle in handles {
        latencies.extend(handle.await.expect("a connection task panicked"));
    }
    let elapsed = started.elapsed();

    latencies.sort_unstable();
    let total = latencies.len();
    let throughput = total as f64 / elapsed.as_secs_f64();

    println!();
    println!("requests:   {total}");
    println!("throughput: {throughput:.0}/s");
    println!("p50:        {}us", percentile(&latencies, 0.50));
    println!("p90:        {}us", percentile(&latencies, 0.90));
    println!("p99:        {}us", percentile(&latencies, 0.99));
    println!("p999:       {}us", percentile(&latencies, 0.999));
    println!("max:        {}us", latencies[total - 1]);
}
