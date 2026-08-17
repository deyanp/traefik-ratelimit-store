//! The listener and per-connection loop.
//!
//! One task per connection, each owning its read buffer. Commands are answered in
//! arrival order, which is what a pipelining client expects.

use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::commands;
use crate::errors::{ProtocolError, TechnicalError};
use crate::log_events;
use crate::peers::PeerTable;
use crate::resp::{self, ParseOutcome};
use crate::script::ScriptRegistry;
use crate::store::BucketStore;

/// Read chunk size. Commands are small; the script source on the one-off EVAL is the
/// largest thing that arrives and fits comfortably.
const READ_CHUNK: usize = 8 * 1024;

/// Refuses a buffer that has grown past anything a legitimate pipeline would need,
/// so a client that never completes a command cannot consume memory indefinitely.
const MAX_BUFFERED_REQUEST: usize = 1024 * 1024;

/// Serves one connection until it closes or its framing is lost.
async fn serve_connection(
    store: Arc<BucketStore>,
    peers: Arc<PeerTable>,
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
                    let reply = commands::dispatch(&store, &peers, &scripts, &args, Instant::now());
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

/// Accepts connections until the process ends.
pub async fn run(
    store: Arc<BucketStore>,
    peers: Arc<PeerTable>,
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
        let (stream, peer) = listener
            .accept()
            .await
            .map_err(|e| TechnicalError(format!("failed to accept a connection: {e}")))?;

        // Small commands with immediate replies; batching them costs more than it saves.
        let _ = stream.set_nodelay(true);

        let store = store.clone();
        let peers = peers.clone();
        let scripts = scripts.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_connection(store, peers, scripts, stream).await {
                let (event_id, event_name) = log_events::CONNECTION_PROTOCOL_ERROR;
                tracing::warn!(event_id, event_name, %peer, error = %error, "closing connection");
            }
        });
    }
}
