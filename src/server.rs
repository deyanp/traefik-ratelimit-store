//! The listener and per-connection loop.
//!
//! One task per connection, each owning its read buffer. Commands are answered in
//! arrival order, which is what a pipelining client expects.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::Semaphore;

use crate::commands;
use crate::errors::{ProtocolError, TechnicalError};
use crate::log_events;
use crate::resp::{self, ParseOutcome};
use crate::script::ScriptRegistry;
use crate::store::BucketStore;

/// Initial read buffer. An evaluation is a couple of hundred bytes; the one-off `EVAL`
/// carrying the script source is the largest thing that arrives and grows it once.
const INITIAL_READ_BUFFER: usize = 2 * 1024;

/// Spare capacity kept ahead of the read cursor, and the step by which it grows.
const READ_HEADROOM: usize = 512;
const READ_GROWTH: usize = 4 * 1024;

/// Initial reply buffer. A pipelined batch of evaluations is a few dozen bytes each.
const INITIAL_WRITE_BUFFER: usize = 256;

/// Refuses a buffer that has grown past anything a legitimate pipeline would need,
/// so a client that never completes a command cannot consume memory indefinitely.
const MAX_BUFFERED_REQUEST: usize = 1024 * 1024;

/// Pending connections the kernel queues before refusing. A proxy fleet restarting opens
/// its pools all at once, and a connection refused there is a request failed.
const LISTEN_BACKLOG: u32 = 4096;

/// How long the accept loop pauses after a failure that is not about one connection.
///
/// Running out of descriptors is the usual cause, and spinning on it would only burn the
/// CPU that the connections already open need; the pause lets some of them finish.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(250);

/// Serves one connection until it closes, goes idle, or its framing is lost.
async fn serve_connection(
    store: Arc<BucketStore>,
    scripts: Arc<ScriptRegistry>,
    mut stream: TcpStream,
    idle_timeout: Duration,
) -> Result<(), ProtocolError> {
    let mut buffer: Vec<u8> = Vec::with_capacity(INITIAL_READ_BUFFER);
    let mut out: Vec<u8> = Vec::with_capacity(INITIAL_WRITE_BUFFER);
    // The buffer length below which the parser has said another attempt cannot
    // succeed, so a client sending a long argument byte by byte is not re-parsed per byte.
    let mut needed = 0;

    loop {
        if buffer.capacity() - buffer.len() < READ_HEADROOM {
            buffer.reserve(READ_GROWTH);
        }
        let read = match tokio::time::timeout(idle_timeout, stream.read_buf(&mut buffer)).await {
            Ok(Ok(0)) => return Ok(()),
            Ok(Ok(read)) => read,
            // A transport failure is the client going away; nothing to answer. An idle
            // client is closed the same way — it holds a descriptor and a buffer for nothing.
            Ok(Err(_)) | Err(_) => return Ok(()),
        };
        debug_assert!(read > 0);

        if buffer.len() > MAX_BUFFERED_REQUEST {
            return Err(ProtocolError::LengthTooLarge(buffer.len() as i64));
        }
        if buffer.len() < needed {
            continue;
        }

        out.clear();
        let mut consumed_total = 0;
        let mut close_after = false;
        let parse_error = loop {
            match resp::parse_command(&buffer[consumed_total..]) {
                Ok(ParseOutcome::Incomplete { at_least }) => {
                    needed = consumed_total + at_least;
                    break None;
                }
                Ok(ParseOutcome::Complete { args, consumed }) => {
                    let answer = commands::dispatch(&store, &scripts, &args, Instant::now());
                    resp::encode_reply(&answer.reply, &mut out);
                    consumed_total += consumed;
                    if answer.close_after {
                        close_after = true;
                        break None;
                    }
                }
                Err(error) => break Some(error),
            }
        };
        buffer.drain(..consumed_total);
        needed = needed.saturating_sub(consumed_total);

        // Replies to the commands already answered go out before anything else happens
        // to the connection, including closing it on the error that followed them.
        if !out.is_empty() && stream.write_all(&out).await.is_err() {
            return Ok(());
        }
        if let Some(error) = parse_error {
            return Err(error);
        }
        if close_after {
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

/// Binds the listener with a backlog sized for a fleet reconnecting at once.
async fn bind_listener(listen_address: &str) -> Result<TcpListener, TechnicalError> {
    let address = tokio::net::lookup_host(listen_address)
        .await
        .map_err(|e| TechnicalError(format!("cannot resolve {listen_address}: {e}")))?
        .next()
        .ok_or_else(|| TechnicalError(format!("{listen_address} resolves to nothing")))?;

    let socket = if address.is_ipv4() {
        TcpSocket::new_v4()
    } else {
        TcpSocket::new_v6()
    }
    .map_err(|e| TechnicalError(format!("cannot create a socket for {listen_address}: {e}")))?;

    // A restarted process must be able to rebind while the previous one's connections
    // are still in TIME_WAIT.
    socket.set_reuseaddr(true).map_err(|e| {
        TechnicalError(format!(
            "cannot configure the socket for {listen_address}: {e}"
        ))
    })?;
    socket
        .bind(address)
        .map_err(|e| TechnicalError(format!("failed to bind {listen_address}: {e}")))?;
    socket
        .listen(LISTEN_BACKLOG)
        .map_err(|e| TechnicalError(format!("failed to listen on {listen_address}: {e}")))
}

/// Accepts connections until the process ends.
///
/// Only binding can fail. An accept that fails — the descriptor limit reached, a client
/// gone before it was taken — is logged and retried: every one of those is transient, and
/// exiting on it would trade a connection that could not be opened for every connection
/// that already was.
///
/// At most `max_connections` are served at once; one beyond that is closed unanswered,
/// which the client reads as a refused connection and retries elsewhere. Each connection
/// is closed after `idle_timeout` without a byte.
pub async fn run(
    store: Arc<BucketStore>,
    scripts: Arc<ScriptRegistry>,
    listen_address: &str,
    max_connections: usize,
    idle_timeout: Duration,
) -> Result<(), TechnicalError> {
    let listener = bind_listener(listen_address).await?;

    let (event_id, event_name) = log_events::STORE_START;
    tracing::info!(
        event_id,
        event_name,
        address = listen_address,
        max_connections,
        "accepting rate-limit connections"
    );

    let connection_slots = Arc::new(Semaphore::new(max_connections));
    let at_capacity = AtomicBool::new(false);

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

        // Held by the connection's task and released when it ends. Reported when the
        // ceiling is first reached and when it clears, not per refused connection.
        let Ok(slot) = connection_slots.clone().try_acquire_owned() else {
            if !at_capacity.swap(true, Ordering::Relaxed) {
                let (event_id, event_name) = log_events::CONNECTIONS_AT_CAPACITY;
                tracing::warn!(
                    event_id,
                    event_name,
                    max_connections,
                    "connection ceiling reached; refusing new connections until one closes"
                );
            }
            drop(stream);
            continue;
        };
        if at_capacity.swap(false, Ordering::Relaxed) {
            tracing::info!("connection ceiling cleared");
        }

        // Small commands with immediate replies; batching them costs more than it saves.
        let _ = stream.set_nodelay(true);

        let store = store.clone();
        let scripts = scripts.clone();
        tokio::spawn(async move {
            let _slot = slot;
            if let Err(error) = serve_connection(store, scripts, stream, idle_timeout).await {
                let (event_id, event_name) = log_events::CONNECTION_PROTOCOL_ERROR;
                tracing::warn!(event_id, event_name, %peer, error = %error, "closing connection");
            }
        });
    }
}
