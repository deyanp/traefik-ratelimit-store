//! Differential conformance against the reference implementation.
//!
//! Every other test asserts what this store does. This one asserts that what it does
//! matches the Lua the caller actually ships, by executing that Lua and diffing the
//! results over a generated corpus.
//!
//! The interpreter is a dev-dependency and never reaches the binary — implementing the
//! arithmetic natively is the whole design, and this is what proves the native version
//! earned it. Lua 5.1 specifically: the script calls `table.maxn`, removed in 5.2.
//!
//! The script text executed here is the `PINNED_SCRIPT` constant from `script.rs`, so a
//! bad transcription of the reference into this repository fails the test rather than
//! lurking behind a digest comparison that only ever compares it to itself.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use mlua::{Lua, Value, Variadic};

use crate::bucket::{self, BucketParams, BucketState};
use crate::script::PINNED_SCRIPT;

/// What the reference returns for one request.
#[derive(Clone, Copy, Debug, PartialEq)]
struct ReferenceOutcome {
    wait: f64,
    tokens: f64,
}

/// The reference implementation, executing the real script against a stubbed store.
struct Oracle {
    lua: Lua,
    /// Field values exactly as the reference would hold them: strings, because that is
    /// what a store round-trip produces and it is lossy in a way that matters here.
    entries: Rc<RefCell<HashMap<String, (String, String)>>>,
}

impl Oracle {
    fn new() -> Self {
        let lua = Lua::new();
        let entries: Rc<RefCell<HashMap<String, (String, String)>>> =
            Rc::new(RefCell::new(HashMap::new()));

        let redis = lua.create_table().expect("create redis table");
        let call_entries = entries.clone();

        let call = lua
            .create_function(move |lua, args: Variadic<Value>| {
                let verb = lua
                    .coerce_string(args[0].clone())?
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default();
                let key = lua
                    .coerce_string(args[1].clone())?
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default();

                match verb.as_str() {
                    // Returns the flat field/value sequence the script indexes positionally.
                    "hgetall" => {
                        let table = lua.create_table()?;
                        if let Some((last, tokens)) = call_entries.borrow().get(&key) {
                            table.set(1, "last")?;
                            table.set(2, last.clone())?;
                            table.set(3, "tokens")?;
                            table.set(4, tokens.clone())?;
                        }
                        Ok(Value::Table(table))
                    }
                    // The reference stores strings, so the numbers are stringified on the
                    // way in exactly as the interpreter would do it. That conversion keeps
                    // 14 significant digits, which is why the assertions below compare
                    // near-equality rather than bit equality.
                    "hset" => {
                        let last = lua
                            .coerce_string(args[3].clone())?
                            .map(|s| s.to_string_lossy().to_string())
                            .unwrap_or_default();
                        let tokens = lua
                            .coerce_string(args[5].clone())?
                            .map(|s| s.to_string_lossy().to_string())
                            .unwrap_or_default();
                        call_entries.borrow_mut().insert(key, (last, tokens));
                        Ok(Value::Boolean(true))
                    }
                    // Lifetime is this store's concern, not the arithmetic's.
                    "expire" => Ok(Value::Boolean(true)),
                    other => Err(mlua::Error::runtime(format!("unexpected verb {other}"))),
                }
            })
            .expect("create redis.call");

        redis.set("call", call).expect("set redis.call");
        lua.globals().set("redis", redis).expect("set redis");

        Self { lua, entries }
    }

    /// Runs one request through the reference.
    fn apply_request(&self, key: &str, params: &BucketParams) -> ReferenceOutcome {
        let globals = self.lua.globals();
        globals
            .set("KEYS", vec![key.to_string()])
            .expect("set KEYS");
        globals
            .set(
                "ARGV",
                vec![
                    format_argument(params.limit),
                    format_argument(params.burst),
                    "2".to_string(),
                    format_argument(params.now),
                    format_argument(params.max_delay),
                ],
            )
            .expect("set ARGV");

        let returned: mlua::Table = self
            .lua
            .load(PINNED_SCRIPT)
            .eval()
            .expect("the reference script must run");

        let wait: String = returned.get(2).expect("wait");
        let tokens: String = returned.get(3).expect("tokens");

        ReferenceOutcome {
            wait: wait.parse().expect("wait is a number"),
            tokens: tokens.parse().expect("tokens is a number"),
        }
    }

    /// The state the reference is holding, as this store would represent it.
    fn stored_state(&self, key: &str) -> Option<BucketState> {
        self.entries
            .borrow()
            .get(key)
            .map(|(last, tokens)| BucketState {
                last: last.parse().expect("stored last is a number"),
                tokens: tokens.parse().expect("stored tokens is a number"),
            })
    }
}

/// Renders an argument the way the caller sends it: a decimal string, full precision.
fn format_argument(value: f64) -> String {
    format!("{value:?}")
}

/// The reference loses precision on every store round-trip, keeping 14 significant
/// digits; this store does not. Equality is therefore near-equality, relative to
/// magnitude, and a gap wider than this is a real disagreement rather than rounding.
fn agrees(actual: f64, expected: f64) -> bool {
    let scale = actual.abs().max(expected.abs()).max(1.0);
    (actual - expected).abs() <= scale * 1e-9
}

/// A deterministic sequence, so a failure reproduces exactly.
struct Corpus(u64);

impl Corpus {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    /// A gap in microseconds, occasionally zero and occasionally backwards.
    fn next_gap(&mut self) -> f64 {
        match self.next() % 10 {
            0 => 0.0,
            1 => -((self.next() % 500_000) as f64),
            _ => (self.next() % 2_000_000) as f64,
        }
    }
}

/// The parameter sets to sweep, each as the caller would derive it from a configuration.
fn parameter_sets() -> Vec<(f64, f64, f64)> {
    // (requests per second, burst, max_delay in microseconds)
    vec![
        (3.0, 10.0, 166_666.0),
        (1.0, 1.0, 500_000.0),
        (15.0, 5.0, 33_333.0),
        (100.0, 200.0, 5_000.0),
        (0.5, 2.0, 500_000.0),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    const START: f64 = 1_787_000_000_000_000.0;

    fn params(rate: f64, burst: f64, max_delay: f64, now: f64) -> BucketParams {
        BucketParams {
            limit: rate / 1_000_000.0,
            burst,
            now,
            max_delay,
            // The reference knows nothing of peers, so the differential is scoped to a
            // single replica. Mesh accuracy is a separate, statistical property.
            peer_consumed: 0.0,
        }
    }

    #[test]
    fn each_step_matches_the_reference() {
        for (rate, burst, max_delay) in parameter_sets() {
            let oracle = Oracle::new();
            let mut corpus = Corpus(0x5EED);
            let mut now = START;

            for step in 0..200 {
                now += corpus.next_gap();
                let step_params = params(rate, burst, max_delay, now);

                // Both implementations are given the reference's own prior state, so each
                // step is compared in isolation and nothing accumulates between them.
                let previous = oracle.stored_state("k");
                let expected = oracle.apply_request("k", &step_params);
                let actual = bucket::apply_request(previous, &step_params);

                assert!(
                    agrees(actual.wait, expected.wait),
                    "rate {rate} burst {burst} step {step}: wait {} vs reference {}",
                    actual.wait,
                    expected.wait
                );
                assert!(
                    agrees(actual.state.tokens, expected.tokens),
                    "rate {rate} burst {burst} step {step}: tokens {} vs reference {}",
                    actual.state.tokens,
                    expected.tokens
                );
            }
        }
    }

    #[test]
    fn the_admit_or_reject_decision_never_differs() {
        // The number the caller acts on is the delay, compared against max_delay. A
        // disagreement in the last digits is tolerable; a disagreement about which side of
        // that threshold a request falls on is not, because it changes the response.
        for (rate, burst, max_delay) in parameter_sets() {
            let oracle = Oracle::new();
            let mut corpus = Corpus(0xC0FFEE);
            let mut now = START;

            for step in 0..200 {
                now += corpus.next_gap();
                let step_params = params(rate, burst, max_delay, now);

                let previous = oracle.stored_state("k");
                let expected = oracle.apply_request("k", &step_params);
                let actual = bucket::apply_request(previous, &step_params);

                assert_eq!(
                    actual.wait <= max_delay,
                    expected.wait <= max_delay,
                    "rate {rate} burst {burst} step {step}: admission differs ({} vs {})",
                    actual.wait,
                    expected.wait
                );
            }
        }
    }

    #[test]
    fn divergence_stays_within_the_references_own_precision() {
        // The per-step test hands both implementations the same prior state, which isolates
        // the arithmetic. This one lets each carry its own, which is how they actually run —
        // and they cannot agree exactly, because the reference is the less precise of the
        // two.
        //
        // The reference stores the bucket as strings, and the interpreter stringifies with
        // 14 significant digits. A microsecond timestamp today has sixteen, so the stored
        // `last` is truncated to roughly 100us of granularity: the reference cannot recall
        // when the previous request happened more precisely than that, and its refill is
        // wrong by up to `limit * granularity` tokens as a result.
        //
        // That error is re-derived from the stored value each step rather than compounding,
        // so the correct assertion is that this store never drifts further from the
        // reference than the reference's own precision explains.
        for (rate, burst, max_delay) in parameter_sets() {
            let oracle = Oracle::new();
            let mut corpus = Corpus(0xBEEF);
            let mut now = START;
            let mut carried: Option<BucketState> = None;

            for step in 0..500 {
                now += corpus.next_gap();
                let step_params = params(rate, burst, max_delay, now);

                let expected = oracle.apply_request("k", &step_params);
                let actual = bucket::apply_request(carried, &step_params);
                carried = Some(actual.state);

                // A thousandth of the configured burst. The mechanism is described above;
                // the per-step error is about `limit * now * 1e-13`, but across an
                // independent run it accumulates a little, because this store carries its
                // own state and never re-reads the reference's rounded one. Chasing that
                // with a fitted constant would assert nothing, so the bound asserted here
                // is the one that matters operationally: the gap stays far too small to
                // change any admission decision.
                let tolerance = burst * 1e-3;

                assert!(
                    (actual.state.tokens - expected.tokens).abs() <= tolerance,
                    "rate {rate} burst {burst} step {step}: drifted to {} against reference {} \
                     (gap {:e}, a thousandth of burst is {:e})",
                    actual.state.tokens,
                    expected.tokens,
                    (actual.state.tokens - expected.tokens).abs(),
                    tolerance
                );
            }
        }
    }

    #[test]
    fn the_reference_cannot_represent_a_microsecond_timestamp() {
        // Records the reason the test above tolerates what it does. If a future release
        // stores the bucket some other way, this fails and the tolerance is revisited
        // rather than silently over-permitting a real divergence.
        let oracle = Oracle::new();
        let step_params = params(3.0, 10.0, 166_666.0, START + 12_345.0);

        oracle.apply_request("k", &step_params);
        let stored = oracle
            .stored_state("k")
            .expect("the reference stored the bucket");

        assert_ne!(
            stored.last, step_params.now,
            "the reference is expected to lose precision storing the timestamp"
        );
        // Within one granularity unit: lossy, but only in the last digits.
        assert!((stored.last - step_params.now).abs() < step_params.now * 1e-12);
    }

    #[test]
    fn an_absent_key_matches_the_reference() {
        // The surprising case, and the one an implementation is most likely to get wrong:
        // a missing entry reads as last = 0, so the elapsed time is the whole timestamp
        // and the bucket arrives full rather than empty.
        for (rate, burst, max_delay) in parameter_sets() {
            let oracle = Oracle::new();
            let step_params = params(rate, burst, max_delay, START);

            let expected = oracle.apply_request("fresh", &step_params);
            let actual = bucket::apply_request(None, &step_params);

            assert!(agrees(actual.state.tokens, expected.tokens));
            assert!(agrees(actual.wait, expected.wait));
            assert!(agrees(actual.state.tokens, burst - 1.0));
        }
    }

    #[test]
    fn the_pinned_script_is_the_one_being_executed() {
        // Guards the premise of every assertion above: if PINNED_SCRIPT were mistranscribed
        // it would still hash consistently and the registry would still accept it, but the
        // oracle would be validating this store against the wrong reference.
        let oracle = Oracle::new();

        let outcome = oracle.apply_request("k", &params(3.0, 10.0, 166_666.0, START));

        assert_eq!(outcome.tokens, 9.0);
        assert_eq!(outcome.wait, 0.0);
    }
}
