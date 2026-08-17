//! Command dispatch.
//!
//! The surface is deliberately tiny — the client exercises six commands and nothing else
//! is implemented. Anything unrecognised is answered with an error rather than closing
//! the connection, because the client treats an error reply as data and carries on.

use std::time::{Duration, Instant};

use crate::bucket::BucketParams;
use crate::log_events;
use crate::peers::PeerTable;
use crate::resp::Reply;
use crate::script::{RegistrationOutcome, ScriptRegistry};
use crate::store::{BucketStore, KeyHash};

/// The number of key/argument slots the bucket evaluation expects after the command name.
const EVALUATION_ARGUMENT_COUNT: usize = 8;

/// Formats a number the way the caller's parser expects.
///
/// Shortest round-trip representation, which its float parser accepts; non-finite values
/// are impossible here but would be unparseable, so they are pinned to zero.
fn format_number(value: f64) -> String {
    if value.is_finite() {
        value.to_string()
    } else {
        "0".to_string()
    }
}

fn parse_number(raw: &[u8]) -> Option<f64> {
    std::str::from_utf8(raw)
        .ok()?
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite())
}

/// Uppercases an ASCII command token without allocating for the common case.
fn command_name_of(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw).to_ascii_uppercase()
}

fn error_reply(message: &str) -> Reply {
    Reply::Error(format!("ERR {message}"))
}

/// Runs the bucket evaluation described by `args`, which start at the key.
///
/// Layout is `<numkeys> <key> <limit> <burst> <ttl> <now> <max_delay>`.
fn evaluate_bucket(
    store: &BucketStore,
    peers: &PeerTable,
    args: &[Vec<u8>],
    now: Instant,
) -> Reply {
    let Some(key_count) = parse_number(&args[0]) else {
        return error_reply("numkeys is not a number");
    };
    if key_count != 1.0 {
        return error_reply("this store evaluates exactly one key per call");
    }

    let key = KeyHash::from_key(&args[1]);
    // What other replicas have already taken for this key counts against this decision.
    let peer_consumed = f64::from(peers.consumed_for(key, now));

    let (Some(limit), Some(burst), Some(ttl), Some(caller_now), Some(max_delay)) = (
        parse_number(&args[2]),
        parse_number(&args[3]),
        parse_number(&args[4]),
        parse_number(&args[5]),
        parse_number(&args[6]),
    ) else {
        return error_reply("bucket arguments must be numbers");
    };

    if limit <= 0.0 {
        return error_reply("limit must be positive");
    }
    // The caller derives the lifetime from the rate; a non-positive one would mean an
    // entry that is expired the moment it is written.
    let ttl = Duration::from_secs(ttl.max(1.0) as u64);

    let params = BucketParams {
        limit,
        burst,
        now: caller_now,
        max_delay,
        peer_consumed,
    };
    let outcome = store.apply_request(key, &params, ttl, now);

    // The caller reads element 0 as a boolean and element 1 as a float; element 2 is
    // returned for symmetry with the script and is not read.
    Reply::Array(vec![
        Reply::Bulk("true".to_string()),
        Reply::Bulk(format_number(outcome.wait)),
        Reply::Bulk(format_number(outcome.state.tokens)),
    ])
}

/// Answers one command.
pub fn dispatch(
    store: &BucketStore,
    peers: &PeerTable,
    scripts: &ScriptRegistry,
    args: &[Vec<u8>],
    now: Instant,
) -> Reply {
    let Some(name) = args.first() else {
        return error_reply("empty command");
    };

    match command_name_of(name).as_str() {
        // Answering HELLO with an error is the supported way to stay on RESP2: the client
        // reads a command error as "this server predates HELLO" and continues.
        "HELLO" => Reply::Error("ERR unknown command 'HELLO'".to_string()),

        // The client discards these results, so an error is as good as a status.
        "CLIENT" => Reply::Error("ERR unknown command 'CLIENT'".to_string()),

        "PING" => Reply::Simple("PONG"),
        "AUTH" | "SELECT" => Reply::Simple("OK"),
        "QUIT" => Reply::Simple("OK"),

        "EVALSHA" => {
            if args.len() < EVALUATION_ARGUMENT_COUNT + 1 {
                return error_reply("wrong number of arguments for 'evalsha'");
            }
            let digest = String::from_utf8_lossy(&args[1]).to_string();
            if !scripts.is_known(&digest) {
                // Provokes the client into resending the source, which is what lets the
                // store verify the caller's algorithm before serving its digest.
                return Reply::Error("NOSCRIPT No matching script. Please use EVAL.".to_string());
            }
            evaluate_bucket(store, peers, &args[2..], now)
        }

        "EVAL" => {
            if args.len() < EVALUATION_ARGUMENT_COUNT + 1 {
                return error_reply("wrong number of arguments for 'eval'");
            }
            let source = String::from_utf8_lossy(&args[1]).to_string();
            if scripts.register_source(&source) == RegistrationOutcome::Diverged {
                let (event_id, event_name) = log_events::SCRIPT_DIVERGED;
                tracing::error!(
                    event_id,
                    event_name,
                    digest = %crate::script::compute_digest(&source),
                    "caller script differs from the pinned text; bucket semantics may have drifted"
                );
            }
            evaluate_bucket(store, peers, &args[2..], now)
        }

        other => Reply::Error(format!("ERR unknown command '{other}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::script::PINNED_SCRIPT;
    use crate::store::StoreConfig;

    const REALISTIC_NOW: &str = "1787000000000000";

    fn args_of(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|part| part.as_bytes().to_vec()).collect()
    }

    fn evaluation_args(command: &str, script_or_digest: &str) -> Vec<Vec<u8>> {
        args_of(&[
            command,
            script_or_digest,
            "1",
            "rate:mw:client",
            "0.000003",
            "10",
            "2",
            REALISTIC_NOW,
            "166666",
        ])
    }

    fn fixtures() -> (BucketStore, PeerTable, ScriptRegistry, Instant) {
        let now = Instant::now();
        (
            BucketStore::new(StoreConfig::default()),
            PeerTable::new(Duration::from_secs(1)),
            ScriptRegistry::new(),
            now,
        )
    }

    #[test]
    fn hello_is_refused_so_the_client_stays_on_resp2() {
        let (store, peers, scripts, now) = fixtures();

        let reply = dispatch(&store, &peers, &scripts, &args_of(&["HELLO", "3"]), now);

        assert!(matches!(reply, Reply::Error(message) if message.contains("HELLO")));
    }

    #[test]
    fn ping_is_answered() {
        let (store, peers, scripts, now) = fixtures();

        assert_eq!(
            dispatch(&store, &peers, &scripts, &args_of(&["PING"]), now),
            Reply::Simple("PONG")
        );
    }

    #[test]
    fn an_unknown_digest_asks_the_client_to_send_the_source() {
        let (store, peers, scripts, now) = fixtures();

        let reply = dispatch(
            &store,
            &peers,
            &scripts,
            &evaluation_args("EVALSHA", "0".repeat(40).as_str()),
            now,
        );

        assert!(matches!(reply, Reply::Error(message) if message.starts_with("NOSCRIPT")));
    }

    #[test]
    fn eval_registers_the_source_and_serves_the_bucket() {
        let (store, peers, scripts, now) = fixtures();

        let reply = dispatch(
            &store,
            &peers,
            &scripts,
            &evaluation_args("EVAL", PINNED_SCRIPT),
            now,
        );

        let Reply::Array(items) = reply else {
            panic!("expected the three-element script reply");
        };
        assert_eq!(items[0], Reply::Bulk("true".to_string()));
        assert_eq!(items[1], Reply::Bulk("0".to_string()));
        assert_eq!(items[2], Reply::Bulk("9".to_string()));
    }

    #[test]
    fn the_digest_is_served_directly_once_registered() {
        let (store, peers, scripts, now) = fixtures();
        dispatch(
            &store,
            &peers,
            &scripts,
            &evaluation_args("EVAL", PINNED_SCRIPT),
            now,
        );

        let reply = dispatch(
            &store,
            &peers,
            &scripts,
            &evaluation_args("EVALSHA", &crate::script::compute_digest(PINNED_SCRIPT)),
            now,
        );

        let Reply::Array(items) = reply else {
            panic!("expected the three-element script reply");
        };
        // The second request against the same key consumes another token.
        assert_eq!(items[2], Reply::Bulk("8".to_string()));
    }

    #[test]
    fn an_unknown_command_is_an_error_not_a_disconnect() {
        let (store, peers, scripts, now) = fixtures();

        let reply = dispatch(
            &store,
            &peers,
            &scripts,
            &args_of(&["SUBSCRIBE", "channel"]),
            now,
        );

        assert!(matches!(reply, Reply::Error(message) if message.contains("SUBSCRIBE")));
    }

    #[test]
    fn non_numeric_bucket_arguments_are_rejected() {
        let (store, peers, scripts, now) = fixtures();
        let mut args = evaluation_args("EVAL", PINNED_SCRIPT);
        args[4] = b"not-a-number".to_vec();

        let reply = dispatch(&store, &peers, &scripts, &args, now);

        assert!(matches!(reply, Reply::Error(message) if message.contains("must be numbers")));
    }
}
