//! Liveness and readiness.
//!
//! The two answer different questions and must not share an endpoint. Liveness asks
//! whether the process should be restarted; readiness asks whether it should be sent
//! traffic. A draining replica is emphatically alive, and a liveness probe that fails
//! during a drain kills the pod it was meant to let finish.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A `PING` in the wire form the protocol listener parses.
const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";
const PONG: &[u8] = b"+PONG\r\n";

/// How long the self-check waits before deciding the listener is not answering.
const SELF_CHECK_TIMEOUT: Duration = Duration::from_millis(500);

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
}

impl Health {
    pub fn new(protocol_address: String) -> Self {
        Self {
            serving: AtomicBool::new(true),
            protocol_address: loopback_form(&protocol_address),
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
    /// this check anyway by never completing it.
    pub async fn protocol_listener_answers(&self) -> bool {
        let attempt = async {
            let mut stream = TcpStream::connect(&self.protocol_address).await.ok()?;
            stream.write_all(PING).await.ok()?;

            let mut reply = [0u8; PONG.len()];
            stream.read_exact(&mut reply).await.ok()?;

            (reply == PONG).then_some(())
        };

        tokio::time::timeout(SELF_CHECK_TIMEOUT, attempt)
            .await
            .ok()
            .flatten()
            .is_some()
    }
}

/// Rewrites a wildcard bind address into something connectable from this process.
///
/// A listener bound to `0.0.0.0:6379` is reachable at `127.0.0.1:6379`, but connecting to
/// the wildcard itself is not portable.
fn loopback_form(address: &str) -> String {
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
        assert_eq!(loopback_form("0.0.0.0:6379"), "127.0.0.1:6379");
        assert_eq!(loopback_form(":6379"), "127.0.0.1:6379");
        assert_eq!(loopback_form("[::]:6379"), "[::1]:6379");
    }

    #[test]
    fn an_explicit_address_is_left_alone() {
        assert_eq!(loopback_form("10.0.0.1:6379"), "10.0.0.1:6379");
        assert_eq!(loopback_form("127.0.0.1:16379"), "127.0.0.1:16379");
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
}
