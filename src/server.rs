//! The listener and per-connection loop.
//!
//! One task per connection, each owning its read buffer. Commands are answered in
//! arrival order, which is what a pipelining client expects.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::commands;
use crate::errors::{ProtocolError, TechnicalError};
use crate::log_events;
use crate::resp::{self, ParseOutcome};
use crate::script::ScriptRegistry;
use crate::store::BucketStore;

/// Read chunk size. Commands are small; the script source on the one-off EVAL is the
/// largest thing that arrives and fits comfortably.
const READ_CHUNK: usize = 8 * 1024;

/// Refuses a buffer that has grown past anything a legitimate pipeline would need,
/// so a client that never completes a command cannot consume memory indefinitely.
const MAX_BUFFERED_REQUEST: usize = 1024 * 1024;

/// How long the accept loop pauses after a failure that is not about one connection.
///
/// Running out of descriptors is the usual cause, and spinning on it would only burn the
/// CPU that the connections already open need; the pause lets some of them finish.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(250);

/// Serves one connection until it closes or its framing is lost.
async fn serve_connection(
    store: Arc<BucketStore>,
    scripts: Arc<ScriptRegistry>,
    mut stream: TcpStream,
) -> Result<(), ProtocolError> {
    let mut buffer: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    let mut chunk = vec![0u8; READ_CHUNK];
    let mut out: Vec<u8> = Vec::with_capacity(READ_CHUNK);

    loop {
        let read = match stream.read(&mut chunk).await {
            Ok(0) => return Ok(()),
            Ok(read) => read,
            // A transport failure is the client going away; nothing to answer.
            Err(_) => return Ok(()),
        };
        buffer.extend_from_slice(&chunk[..read]);

        if buffer.len() > MAX_BUFFERED_REQUEST {
            return Err(ProtocolError::LengthTooLarge(buffer.len() as i64));
        }

        out.clear();
        let mut consumed_total = 0;
        loop {
            match resp::parse_command(&buffer[consumed_total..])? {
                ParseOutcome::Incomplete => break,
                ParseOutcome::Complete { args, consumed } => {
                    let reply = commands::dispatch(&store, &scripts, &args, Instant::now());
                    resp::encode_reply(&reply, &mut out);
                    consumed_total += consumed;
                }
            }
        }
        buffer.drain(..consumed_total);

        if !out.is_empty() && stream.write_all(&out).await.is_err() {
            return Ok(());
        }
    }
}

/// Whether an accept failure concerned only the connection being accepted.
///
/// These are the peer going away between the kernel queueing it and this process taking
/// it; the next accept is unaffected.
fn is_connection_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
    )
}

/// Accepts connections until the process ends.
///
/// Only binding can fail. An accept that fails — the descriptor limit reached, a client
/// gone before it was taken — is logged and retried: every one of those is transient, and
/// exiting on it would trade a connection that could not be opened for every connection
/// that already was.
pub async fn run(
    store: Arc<BucketStore>,
    scripts: Arc<ScriptRegistry>,
    listen_address: &str,
) -> Result<(), TechnicalError> {
    let listener = TcpListener::bind(listen_address)
        .await
        .map_err(|e| TechnicalError(format!("failed to bind {listen_address}: {e}")))?;

    let (event_id, event_name) = log_events::STORE_START;
    tracing::info!(
        event_id,
        event_name,
        address = listen_address,
        "accepting rate-limit connections"
    );

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) if is_connection_error(&error) => continue,
            Err(error) => {
                let (event_id, event_name) = log_events::ACCEPT_FAILED;
                tracing::warn!(
                    event_id,
                    event_name,
                    error = %error,
                    "could not accept a connection; retrying"
                );
                tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
                continue;
            }
        };

        // Small commands with immediate replies; batching them costs more than it saves.
        let _ = stream.set_nodelay(true);

        let store = store.clone();
        let scripts = scripts.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_connection(store, scripts, stream).await {
                let (event_id, event_name) = log_events::CONNECTION_PROTOCOL_ERROR;
                tracing::warn!(event_id, event_name, %peer, error = %error, "closing connection");
            }
        });
    }
}
