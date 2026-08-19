//! Command dispatch.
//!
//! The surface is deliberately tiny — the client exercises six commands and nothing else
//! is implemented. Anything unrecognised is answered with an error rather than closing
//! the connection, because the client treats an error reply as data and carries on.

use std::time::{Duration, Instant};

use crate::bucket::BucketParams;
use crate::log_events;
use crate::resp::Reply;
use crate::script::{RegistrationOutcome, ScriptRegistry};
use crate::store::{BucketStore, KeyHash};

/// The number of key/argument slots the bucket evaluation expects after the command name.
const EVALUATION_ARGUMENT_COUNT: usize = 8;

/// Longest lifetime a caller may give an entry. The caller derives a few seconds from the
/// rate; the cap exists so an arbitrary value cannot pin memory or overflow the clock.
const MAX_TTL_SECONDS: f64 = 24.0 * 60.0 * 60.0;

/// Formats a number the way the caller's parser expects.
///
/// Shortest round-trip representation, which its float parser accepts. A non-finite wait
/// can only come from a degenerate `limit`; it is rendered as the largest finite value so
/// the caller still reads "longer than any acceptable delay" and rejects, rather than a
/// zero that would admit.
fn format_number(value: f64) -> String {
    if value.is_finite() {
        value.to_string()
    } else if value.is_sign_negative() {
        f64::MIN.to_string()
    } else {
        f64::MAX.to_string()
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

fn error_reply(message: &str) -> Reply {
    Reply::Error(format!("ERR {message}"))
}

/// Runs the bucket evaluation described by `args`, which start at the key.
///
/// Layout is `<numkeys> <key> <limit> <burst> <ttl> <now> <max_delay>`.
fn evaluate_bucket(store: &BucketStore, args: &[Vec<u8>], now: Instant) -> Reply {
    let Some(key_count) = parse_number(&args[0]) else {
        return error_reply("numkeys is not a number");
    };
    if key_count != 1.0 {
        return error_reply("this store evaluates exactly one key per call");
    }

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
    // entry that is expired the moment it is written, and an unbounded one could not be
    // added to the clock.
    let ttl = Duration::from_secs(ttl.clamp(1.0, MAX_TTL_SECONDS) as u64);

    let key = KeyHash::from_key(&args[1]);
    let params = BucketParams {
        limit,
        burst,
        now: caller_now,
        max_delay,
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
    scripts: &ScriptRegistry,
    args: &[Vec<u8>],
    now: Instant,
) -> Reply {
    let Some(name) = args.first() else {
        return error_reply("empty command");
    };

    // Command names are ASCII and case-insensitive; matched in place, no copy.
    let is = |candidate: &[u8]| name.eq_ignore_ascii_case(candidate);

    if is(b"HELLO") {
        // Answering HELLO with an error is the supported way to stay on RESP2: the client
        // reads a command error as "this server predates HELLO" and continues.
        return Reply::Error("ERR unknown command 'HELLO'".to_string());
    }
    if is(b"CLIENT") {
        // The client discards these results, so an error is as good as a status.
        return Reply::Error("ERR unknown command 'CLIENT'".to_string());
    }
    if is(b"PING") {
        return Reply::Simple("PONG");
    }
    if is(b"AUTH") || is(b"SELECT") || is(b"QUIT") {
        return Reply::Simple("OK");
    }

    if is(b"EVALSHA") {
        if args.len() < EVALUATION_ARGUMENT_COUNT + 1 {
            return error_reply("wrong number of arguments for 'evalsha'");
        }
        if !scripts.is_known(&args[1]) {
            // Provokes the client into resending the source, which is what lets the
            // store verify the caller's algorithm before serving its digest.
            return Reply::Error("NOSCRIPT No matching script. Please use EVAL.".to_string());
        }
        return evaluate_bucket(store, &args[2..], now);
    }

    if is(b"EVAL") {
        if args.len() < EVALUATION_ARGUMENT_COUNT + 1 {
            return error_reply("wrong number of arguments for 'eval'");
        }
        match scripts.register_source(&args[1]) {
            RegistrationOutcome::Matched(_)
            | RegistrationOutcome::Diverged { first_seen: false } => {}
            RegistrationOutcome::Diverged { first_seen: true } => {
                let (event_id, event_name) = log_events::SCRIPT_DIVERGED;
                tracing::error!(
                    event_id,
                    event_name,
                    digest = %crate::script::compute_digest(&args[1]),
                    "caller script differs from the pinned text; bucket semantics may have drifted"
                );
            }
            RegistrationOutcome::Unregistered => {
                let (event_id, event_name) = log_events::SCRIPT_REGISTRY_FULL;
                tracing::warn!(
                    event_id,
                    event_name,
                    digest = %crate::script::compute_digest(&args[1]),
                    "another unrecognised script; served, but not remembered"
                );
            }
        }
        return evaluate_bucket(store, &args[2..], now);
    }

    Reply::Error(format!(
        "ERR unknown command '{}'",
        String::from_utf8_lossy(name).to_ascii_uppercase()
    ))
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

    fn fixtures() -> (BucketStore, ScriptRegistry, Instant) {
        let now = Instant::now();
        (
            BucketStore::new(StoreConfig::default()),
            ScriptRegistry::new(),
            now,
        )
    }

    #[test]
    fn hello_is_refused_so_the_client_stays_on_resp2() {
        let (store, scripts, now) = fixtures();

        let reply = dispatch(&store, &scripts, &args_of(&["HELLO", "3"]), now);

        assert!(matches!(reply, Reply::Error(message) if message.contains("HELLO")));
    }

    #[test]
    fn ping_is_answered() {
        let (store, scripts, now) = fixtures();

        assert_eq!(
            dispatch(&store, &scripts, &args_of(&["PING"]), now),
            Reply::Simple("PONG")
        );
    }

    #[test]
    fn an_unknown_digest_asks_the_client_to_send_the_source() {
        let (store, scripts, now) = fixtures();

        let reply = dispatch(
            &store,
            &scripts,
            &evaluation_args("EVALSHA", "0".repeat(40).as_str()),
            now,
        );

        assert!(matches!(reply, Reply::Error(message) if message.starts_with("NOSCRIPT")));
    }

    #[test]
    fn eval_registers_the_source_and_serves_the_bucket() {
        let (store, scripts, now) = fixtures();

        let reply = dispatch(
            &store,
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
        let (store, scripts, now) = fixtures();
        dispatch(
            &store,
            &scripts,
            &evaluation_args("EVAL", PINNED_SCRIPT),
            now,
        );

        let reply = dispatch(
            &store,
            &scripts,
            &evaluation_args(
                "EVALSHA",
                &crate::script::compute_digest(PINNED_SCRIPT.as_bytes()),
            ),
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
        let (store, scripts, now) = fixtures();

        let reply = dispatch(&store, &scripts, &args_of(&["SUBSCRIBE", "channel"]), now);

        assert!(matches!(reply, Reply::Error(message) if message.contains("SUBSCRIBE")));
    }

    #[test]
    fn non_numeric_bucket_arguments_are_rejected() {
        let (store, scripts, now) = fixtures();
        let mut args = evaluation_args("EVAL", PINNED_SCRIPT);
        args[4] = b"not-a-number".to_vec();

        let reply = dispatch(&store, &scripts, &args, now);

        assert!(matches!(reply, Reply::Error(message) if message.contains("must be numbers")));
    }

    #[test]
    fn an_absurd_ttl_is_clamped_rather_than_overflowing_the_clock() {
        let (store, scripts, now) = fixtures();
        let mut args = evaluation_args("EVAL", PINNED_SCRIPT);
        args[6] = b"1e30".to_vec();

        let reply = dispatch(&store, &scripts, &args, now);

        assert!(matches!(reply, Reply::Array(_)), "got {reply:?}");
    }

    #[test]
    fn command_names_are_matched_case_insensitively() {
        let (store, scripts, now) = fixtures();

        assert_eq!(
            dispatch(&store, &scripts, &args_of(&["ping"]), now),
            Reply::Simple("PONG")
        );
    }

    #[test]
    fn a_non_finite_wait_is_rendered_as_a_rejection_not_an_admission() {
        // Only a degenerate limit can produce it, but the caller must then read "wait
        // longer than anything acceptable", never zero.
        assert_eq!(format_number(f64::INFINITY), f64::MAX.to_string());
        assert_eq!(format_number(0.0), "0");
    }
}
