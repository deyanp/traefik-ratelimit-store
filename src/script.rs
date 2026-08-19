//! The script registry.
//!
//! The caller addresses the bucket algorithm by the SHA-1 of a Lua script it never
//! expects to have to send. This store does not execute Lua — it implements the same
//! arithmetic natively — so the SHA is only an identifier, and the script source is only
//! ever compared, never run.
//!
//! Registration is self-validating. An unrecognised digest is answered with `NOSCRIPT`,
//! which makes the caller resend the full source; that source is compared against the
//! text pinned below. A match registers the digest. A mismatch means the caller changed
//! its algorithm in an upgrade, and is reported so the divergence is noticed rather than
//! silently served with stale semantics.

use std::collections::HashSet;
use std::sync::{LazyLock, RwLock};

use sha1::{Digest, Sha1};

/// The script text this store's arithmetic reproduces, as published in v3.7.1.
///
/// Compared byte for byte; never executed.
pub const PINNED_SCRIPT: &str = r#"
local key = KEYS[1]
local limit, burst, ttl, t, max_delay = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]),
    tonumber(ARGV[5])

local bucket = {
    limit = limit,
    burst = burst,
    tokens = 0,
    last = 0
}

local rl_source = redis.call('hgetall', key)

if table.maxn(rl_source) == 4 then
    -- Get bucket state from redis
    bucket.last = tonumber(rl_source[2])
    bucket.tokens = tonumber(rl_source[4])
end

local last = bucket.last
if t < last then
    last = t
end

local elapsed = t - last
local delta = bucket.limit * elapsed
local tokens = bucket.tokens + delta
tokens = math.min(tokens, bucket.burst)
tokens = tokens - 1

local wait_duration = 0
if tokens < 0 then
    wait_duration = (tokens * -1) / bucket.limit
    if wait_duration > max_delay then
        tokens = tokens + 1
        tokens = math.min(tokens, burst)
    end
end

redis.call('hset', key, 'last', t, 'tokens', tokens)
redis.call('expire', key, ttl)

return {tostring(true), tostring(wait_duration),tostring(tokens)}"#;

/// The same algorithm as published later on the v3.7 branch.
///
/// It differs from [`PINNED_SCRIPT`] by one space before the final `tostring(tokens)`,
/// which is semantically nothing and a completely different digest. This is why the
/// pinned set has more than one member: two patch releases of one minor version disagree
/// on the text, so a fleet part-way through an upgrade sends both.
pub const PINNED_SCRIPT_SPACED_RETURN: &str = r#"
local key = KEYS[1]
local limit, burst, ttl, t, max_delay = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]),
    tonumber(ARGV[5])

local bucket = {
    limit = limit,
    burst = burst,
    tokens = 0,
    last = 0
}

local rl_source = redis.call('hgetall', key)

if table.maxn(rl_source) == 4 then
    -- Get bucket state from redis
    bucket.last = tonumber(rl_source[2])
    bucket.tokens = tonumber(rl_source[4])
end

local last = bucket.last
if t < last then
    last = t
end

local elapsed = t - last
local delta = bucket.limit * elapsed
local tokens = bucket.tokens + delta
tokens = math.min(tokens, bucket.burst)
tokens = tokens - 1

local wait_duration = 0
if tokens < 0 then
    wait_duration = (tokens * -1) / bucket.limit
    if wait_duration > max_delay then
        tokens = tokens + 1
        tokens = math.min(tokens, burst)
    end
end

redis.call('hset', key, 'last', t, 'tokens', tokens)
redis.call('expire', key, ttl)

return {tostring(true), tostring(wait_duration), tostring(tokens)}"#;

/// Every script text known to express the arithmetic this store implements.
///
/// Membership is exact-match on purpose. Normalising whitespace before comparing would
/// make the set tolerant of the harmless differences seen so far, but it would also make
/// it tolerant of a real change to the algorithm — which is the one thing the comparison
/// exists to catch. Adding a revision here is a deliberate act that says someone read the
/// diff.
pub const KNOWN_SCRIPTS: &[(&str, &str)] = &[
    ("v3.7.1", PINNED_SCRIPT),
    ("v3.7-branch", PINNED_SCRIPT_SPACED_RETURN),
];

/// The digest of [`PINNED_SCRIPT`], computed once.
pub static PINNED_SCRIPT_DIGEST: LazyLock<String> =
    LazyLock::new(|| compute_digest(PINNED_SCRIPT.as_bytes()));

/// How many digests of *unrecognised* sources the registry will remember.
///
/// A fleet part-way through an upgrade sends a handful of distinct texts at most. The
/// bound exists because the protocol port is open to anything the network admits, and an
/// unbounded set would let a caller grow this process's memory one `EVAL` at a time.
pub const MAX_DIVERGED_SCRIPTS: usize = 16;

/// What happened when a caller sent script source for evaluation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegistrationOutcome {
    /// The source matches a known text; its digest is now recognised.
    Matched(&'static str),
    /// The source matches nothing known. The request is still served with this store's
    /// arithmetic, because refusing would turn a caller upgrade into an outage — but the
    /// semantics may have drifted and the operator needs to know. `first_seen` is true
    /// the first time this particular text arrives, so the operator is told once per text
    /// rather than once per request.
    Diverged { first_seen: bool },
    /// The source matches nothing known and the registry is full, so its digest is not
    /// remembered: the request is served, and the next `EVALSHA` for it will be asked for
    /// the source again.
    Unregistered,
}

/// Returns the lowercase hexadecimal SHA-1 of `source`.
///
/// Hashed as bytes, exactly as the caller computes it; decoding the source as text first
/// would change the digest of anything that is not valid UTF-8.
pub fn compute_digest(source: &[u8]) -> String {
    hex::encode(Sha1::digest(source))
}

#[derive(Debug, Default)]
struct Registrations {
    known: HashSet<String>,
    /// How many of `known` are unrecognised texts, held against [`MAX_DIVERGED_SCRIPTS`].
    diverged: usize,
}

/// The digests this store answers to.
#[derive(Debug, Default)]
pub struct ScriptRegistry {
    registrations: RwLock<Registrations>,
}

impl ScriptRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `digest` has been registered by a prior evaluation.
    ///
    /// Deliberately empty at startup, so the first request provokes the source exchange
    /// that validates the caller's algorithm against the pinned one.
    pub fn is_known(&self, digest: &[u8]) -> bool {
        let Ok(digest) = std::str::from_utf8(digest) else {
            return false;
        };

        self.registrations
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .known
            .contains(digest)
    }

    /// Registers the digest of `source` and reports whether it matched a known text.
    pub fn register_source(&self, source: &[u8]) -> RegistrationOutcome {
        let digest = compute_digest(source);
        let matched = KNOWN_SCRIPTS
            .iter()
            .find(|(_, known)| known.as_bytes() == source)
            .map(|(revision, _)| *revision);

        let mut registrations = self
            .registrations
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        match matched {
            Some(revision) => {
                registrations.known.insert(digest);
                RegistrationOutcome::Matched(revision)
            }
            None if registrations.known.contains(&digest) => {
                RegistrationOutcome::Diverged { first_seen: false }
            }
            None if registrations.diverged >= MAX_DIVERGED_SCRIPTS => {
                RegistrationOutcome::Unregistered
            }
            None => {
                registrations.known.insert(digest);
                registrations.diverged += 1;
                RegistrationOutcome::Diverged { first_seen: true }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_is_lowercase_hex_of_forty_characters() {
        let digest = compute_digest(b"anything");

        assert_eq!(digest.len(), 40);
        assert!(
            digest
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn nothing_is_known_before_a_source_arrives() {
        let registry = ScriptRegistry::new();

        assert!(!registry.is_known(PINNED_SCRIPT_DIGEST.as_bytes()));
    }

    #[test]
    fn the_pinned_source_registers_and_matches() {
        let registry = ScriptRegistry::new();

        let outcome = registry.register_source(PINNED_SCRIPT.as_bytes());

        assert_eq!(outcome, RegistrationOutcome::Matched("v3.7.1"));
        assert!(registry.is_known(PINNED_SCRIPT_DIGEST.as_bytes()));
    }

    #[test]
    fn every_known_revision_is_accepted() {
        for (revision, source) in KNOWN_SCRIPTS {
            let registry = ScriptRegistry::new();

            assert_eq!(
                registry.register_source(source.as_bytes()),
                RegistrationOutcome::Matched(revision),
                "revision {revision} should be recognised"
            );
        }
    }

    #[test]
    fn known_revisions_have_distinct_digests() {
        // The reason the set exists: semantically identical texts hash differently, so a
        // single pinned copy would reject callers running a neighbouring patch release.
        let digests: std::collections::HashSet<String> = KNOWN_SCRIPTS
            .iter()
            .map(|(_, source)| compute_digest(source.as_bytes()))
            .collect();

        assert_eq!(digests.len(), KNOWN_SCRIPTS.len());
    }

    #[test]
    fn a_changed_source_registers_but_reports_divergence() {
        let registry = ScriptRegistry::new();
        let altered = format!("{PINNED_SCRIPT}\n-- upstream changed this");

        let outcome = registry.register_source(altered.as_bytes());

        assert_eq!(outcome, RegistrationOutcome::Diverged { first_seen: true });
        // Still served: an upgrade must not become an outage.
        assert!(registry.is_known(compute_digest(altered.as_bytes()).as_bytes()));
        // And reported once, not once per request.
        assert_eq!(
            registry.register_source(altered.as_bytes()),
            RegistrationOutcome::Diverged { first_seen: false }
        );
    }

    #[test]
    fn the_registry_does_not_grow_without_bound() {
        // The protocol port is open to anything the network admits, so a caller that sends
        // a new text with every EVAL must not be able to grow this process one digest at a
        // time. Beyond the bound the request is still served; the digest is just forgotten.
        let registry = ScriptRegistry::new();

        for index in 0..MAX_DIVERGED_SCRIPTS {
            let source = format!("return {index}");
            assert_eq!(
                registry.register_source(source.as_bytes()),
                RegistrationOutcome::Diverged { first_seen: true }
            );
        }

        let one_more = b"return 'one more'";
        assert_eq!(
            registry.register_source(one_more),
            RegistrationOutcome::Unregistered
        );
        assert!(!registry.is_known(compute_digest(one_more).as_bytes()));

        // A known text is always registered, full or not.
        assert_eq!(
            registry.register_source(PINNED_SCRIPT.as_bytes()),
            RegistrationOutcome::Matched("v3.7.1")
        );
    }

    #[test]
    fn the_digest_is_over_the_raw_bytes() {
        // The caller hashes the bytes it sends; anything else would never match its digest.
        let raw: &[u8] = &[0xff, 0xfe, b'x'];

        assert_eq!(compute_digest(raw), hex::encode(Sha1::digest(raw)));
    }
}
