//! Liveness and readiness.
//!
//! The two answer different questions and must not share an endpoint. Liveness asks
//! whether the process should be restarted; readiness asks whether it should be sent
//! traffic. A draining replica is emphatically alive, and a liveness probe that fails
//! during a drain kills the pod it was meant to let finish.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A `PING` in the wire form the protocol listener parses.
const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";
const PONG: &[u8] = b"+PONG\r\n";

/// How long the self-check waits before deciding the listener is not answering.
const SELF_CHECK_TIMEOUT: Duration = Duration::from_millis(500);

/// How long a self-check result is reused. The probe is unauthenticated, and each check
/// opens a connection to the protocol port; answering from a recent result means a caller
/// hammering the probe cannot turn it into connections against the listener.
const SELF_CHECK_REUSE: Duration = Duration::from_secs(1);

/// Whether this replica is willing to receive traffic.
///
/// Set false on shutdown so the orchestrator withdraws the replica from its endpoints
/// *before* the listener closes. Without that ordering the grace period buys nothing:
/// connections keep arriving until the socket disappears underneath them.
#[derive(Debug)]
pub struct Health {
    serving: AtomicBool,
    /// The protocol listener's own address, so the check exercises the port that carries
    /// production traffic rather than the one serving the probe.
    protocol_address: String,
    /// The last self-check's verdict and when it was reached.
    last_check: Mutex<Option<(Instant, bool)>>,
}

impl Health {
    pub fn new(protocol_address: String) -> Self {
        Self {
            serving: AtomicBool::new(true),
            protocol_address: to_loopback_form(&protocol_address),
            last_check: Mutex::new(None),
        }
    }

    /// Stops advertising readiness. Idempotent.
    pub fn begin_draining(&self) {
        self.serving.store(false, Ordering::SeqCst);
    }

    pub fn is_draining(&self) -> bool {
        !self.serving.load(Ordering::SeqCst)
    }

    /// Whether the protocol listener answers.
    ///
    /// Deliberately does not touch the store. A probe that acquires production locks can
    /// cause the stall it is looking for, and a store wedged badly enough to matter fails
    /// this check anyway by never completing it. A verdict younger than
    /// [`SELF_CHECK_REUSE`] is returned as is.
    pub async fn protocol_listener_answers(&self) -> bool {
        let now = Instant::now();
        if let Some((checked_at, answered)) =
            *self.last_check.lock().unwrap_or_else(|p| p.into_inner())
            && now.duration_since(checked_at) < SELF_CHECK_REUSE
        {
            return answered;
        }

        let attempt = async {
            let mut stream = TcpStream::connect(&self.protocol_address).await.ok()?;
            stream.write_all(PING).await.ok()?;

            let mut reply = [0u8; PONG.len()];
            stream.read_exact(&mut reply).await.ok()?;

            (reply == PONG).then_some(())
        };

        let answered = tokio::time::timeout(SELF_CHECK_TIMEOUT, attempt)
            .await
            .ok()
            .flatten()
            .is_some();
        *self.last_check.lock().unwrap_or_else(|p| p.into_inner()) = Some((now, answered));
        answered
    }
}

/// Rewrites a wildcard bind address into something connectable from this process.
///
/// A listener bound to `0.0.0.0:6379` is reachable at `127.0.0.1:6379`, but connecting to
/// the wildcard itself is not portable.
fn to_loopback_form(address: &str) -> String {
    match address.rsplit_once(':') {
        Some((host, port)) if host.is_empty() || host == "0.0.0.0" || host == "*" => {
            format!("127.0.0.1:{port}")
        }
        Some(("[::]", port)) => format!("[::1]:{port}"),
        _ => address.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wildcard_bind_address_becomes_loopback() {
        assert_eq!(to_loopback_form("0.0.0.0:6379"), "127.0.0.1:6379");
        assert_eq!(to_loopback_form(":6379"), "127.0.0.1:6379");
        assert_eq!(to_loopback_form("[::]:6379"), "[::1]:6379");
    }

    #[test]
    fn an_explicit_address_is_left_alone() {
        assert_eq!(to_loopback_form("10.0.0.1:6379"), "10.0.0.1:6379");
        assert_eq!(to_loopback_form("127.0.0.1:16379"), "127.0.0.1:16379");
    }

    #[test]
    fn a_replica_starts_willing_to_serve() {
        let health = Health::new("0.0.0.0:6379".to_string());

        assert!(!health.is_draining());
    }

    #[test]
    fn draining_is_idempotent() {
        let health = Health::new("0.0.0.0:6379".to_string());

        health.begin_draining();
        health.begin_draining();

        assert!(health.is_draining());
    }

    #[tokio::test]
    async fn the_self_check_fails_when_nothing_is_listening() {
        // Port 1 is privileged and nothing binds it, so this exercises the failure path
        // without depending on what else happens to be running.
        let health = Health::new("127.0.0.1:1".to_string());

        assert!(!health.protocol_listener_answers().await);
    }

    #[tokio::test]
    async fn a_recent_verdict_is_reused_rather_than_rechecked() {
        // A listener that appears after the first check is not seen until the verdict
        // ages out — which is the point: one connection to the listener per second, however
        // often the probe is called.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        drop(listener);
        let health = Health::new(address.clone());
        assert!(!health.protocol_listener_answers().await);

        let listener = tokio::net::TcpListener::bind(&address).await.unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut buffer = [0u8; 64];
                let _ = stream.read(&mut buffer).await;
                let _ = stream.write_all(PONG).await;
            }
        });

        assert!(
            !health.protocol_listener_answers().await,
            "reused within the window"
        );
        tokio::time::sleep(SELF_CHECK_REUSE + Duration::from_millis(50)).await;
        assert!(
            health.protocol_listener_answers().await,
            "rechecked after it"
        );
    }
}
