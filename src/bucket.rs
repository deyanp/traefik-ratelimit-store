//! Token-bucket arithmetic.
//!
//! This module is the innermost layer: it knows nothing about the wire protocol, the
//! store, or peers. It reproduces the semantics the proxy expects from its rate-limit
//! backend, so every value is `f64` — the reference implementation computes in double
//! precision and the two must agree exactly.

/// What the store holds for one rate-limit key between requests.
///
/// `tokens` is deliberately allowed to go negative: when a caller is admitted with a
/// tolerable wait, the debt is carried here rather than clamped away.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BucketState {
    /// Timestamp of the last admitted request, in microseconds.
    pub last: f64,
    /// Tokens remaining after that request.
    pub tokens: f64,
}

/// The per-request parameters, supplied by the caller on every call.
///
/// None of these are stored: a configuration change therefore takes effect on the next
/// request, and the stored state self-corrects within one TTL.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BucketParams {
    /// Refill rate in tokens per microsecond.
    pub limit: f64,
    /// Bucket capacity in tokens.
    pub burst: f64,
    /// Time of this request, in microseconds.
    pub now: f64,
    /// The longest wait the caller will accept, in microseconds.
    pub max_delay: f64,
}

/// The result of admitting one request against a bucket.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BucketOutcome {
    /// The state to store back under the key.
    pub state: BucketState,
    /// How long the caller should wait, in microseconds. Zero means no wait.
    pub wait: f64,
}

/// Applies one request to a bucket, returning the new state and the caller's wait.
///
/// An absent bucket is not an error, and it is not an empty one either: the missing
/// state reads as `last = 0`, so the elapsed time is the whole timestamp and the refill
/// saturates at `burst`. A key that has expired or was never seen therefore admits its
/// request against a full bucket, which is what makes a lost counter harmless.
pub fn apply_request(previous: Option<BucketState>, params: &BucketParams) -> BucketOutcome {
    let BucketState {
        last,
        tokens: stored,
    } = previous.unwrap_or(BucketState {
        last: 0.0,
        tokens: 0.0,
    });

    // A clock that has gone backwards would otherwise credit tokens for negative elapsed time.
    let last = if params.now < last { params.now } else { last };

    let elapsed = params.now - last;
    let refilled = (stored + params.limit * elapsed).min(params.burst);

    let mut tokens = refilled - 1.0;

    let mut wait = 0.0;
    if tokens < 0.0 {
        wait = -tokens / params.limit;
        if wait > params.max_delay {
            // The caller will be rejected rather than made to wait, so the token it could
            // not use is returned to the bucket.
            tokens = (tokens + 1.0).min(params.burst);
        }
    }

    BucketOutcome {
        state: BucketState {
            last: params.now,
            tokens,
        },
        wait,
    }
}

/// Folds admissions another replica made into this bucket, as of the moment they happened.
///
/// A peer's admission is a token taken from the same logical bucket, so it is debited
/// here exactly as a local request would have been: refill up to `at`, then subtract.
/// Debiting the stored state — rather than offsetting each later decision — is what makes
/// the limit hold under sustained traffic: every replica's level then tracks the one
/// shared level, and each refills it at the configured rate only once.
///
/// `limit` and `burst` are the peer's view of the configuration, carried with the report
/// because the debit has to be folded before this replica necessarily sees a request for
/// the key itself. They are used for this fold only and never stored. An absent bucket is
/// taken as full at `at`, which is what a key this replica has not seen would have held.
pub fn apply_peer_admissions(
    previous: Option<BucketState>,
    admitted: f64,
    at: f64,
    limit: f64,
    burst: f64,
) -> BucketState {
    match previous {
        None => BucketState {
            last: at,
            tokens: burst - admitted,
        },
        // Admissions that predate the last local request are debited as of that request:
        // the level carried forward from it simply was `admitted` lower than stored.
        Some(state) if at <= state.last => BucketState {
            last: state.last,
            tokens: state.tokens - admitted,
        },
        Some(state) => {
            let refilled = (state.tokens + limit * (at - state.last)).min(burst);
            BucketState {
                last: at,
                tokens: refilled - admitted,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 3 requests per second expressed the way the caller sends it: tokens per microsecond.
    const LIMIT: f64 = 3.0 / 1_000_000.0;
    const BURST: f64 = 10.0;
    const MAX_DELAY: f64 = 166_666.0;

    /// A realistic wall-clock timestamp in microseconds. The caller always sends one, and
    /// the absent-key behaviour depends on it: a toy timestamp near the epoch produces a
    /// proportionally smaller refill instead of saturating at burst.
    const REALISTIC_NOW: f64 = 1_787_000_000_000_000.0;

    fn params_at(now: f64) -> BucketParams {
        BucketParams {
            limit: LIMIT,
            burst: BURST,
            now,
            max_delay: MAX_DELAY,
        }
    }

    fn is_close(actual: f64, expected: f64) -> bool {
        (actual - expected).abs() < 1e-9
    }

    #[test]
    fn absent_bucket_admits_against_a_full_bucket() {
        // A missing key reads as last = 0, so the elapsed time is the whole timestamp and
        // the refill saturates at burst. Losing a counter therefore admits the request
        // rather than rejecting it.
        let outcome = apply_request(None, &params_at(REALISTIC_NOW));

        assert_eq!(outcome.wait, 0.0);
        assert_eq!(outcome.state.tokens, BURST - 1.0);
        assert_eq!(outcome.state.last, REALISTIC_NOW);
    }

    #[test]
    fn refill_is_capped_at_burst() {
        let previous = BucketState {
            last: 0.0,
            tokens: 0.0,
        };
        // An hour of refill would far exceed burst.
        let outcome = apply_request(Some(previous), &params_at(3_600_000_000.0));

        assert_eq!(outcome.state.tokens, BURST - 1.0);
        assert_eq!(outcome.wait, 0.0);
    }

    #[test]
    fn debt_is_carried_when_the_wait_is_tolerable() {
        let previous = BucketState {
            last: 1_000_000.0,
            tokens: 0.0,
        };
        // 200ms of refill at 3/s is 0.6 tokens, so this request goes 0.4 into debt.
        // Waiting for 0.4 tokens takes 133ms, inside the 166ms max_delay.
        let outcome = apply_request(Some(previous), &params_at(1_200_000.0));

        assert!(is_close(outcome.state.tokens, -0.4));
        assert!(outcome.wait > 0.0 && outcome.wait <= MAX_DELAY);
    }

    #[test]
    fn token_is_refunded_when_the_wait_exceeds_max_delay() {
        let previous = BucketState {
            last: 1_000_000.0,
            tokens: 0.0,
        };
        // 100ms of refill is 0.3 tokens, leaving a 0.7 deficit that would take 233ms to
        // clear — beyond max_delay, so the caller is rejected and its token returned.
        let outcome = apply_request(Some(previous), &params_at(1_100_000.0));

        assert!(outcome.wait > MAX_DELAY);
        // The refund restores the bucket to what the refill alone had made it, so the
        // rejected caller consumes nothing.
        assert!(is_close(outcome.state.tokens, 0.3));
    }

    #[test]
    fn peer_admissions_debit_the_stored_level() {
        let previous = BucketState {
            last: 1_000_000.0,
            tokens: 5.0,
        };

        // A peer admitted four requests at the same instant. They came out of the same
        // logical bucket, so the stored level drops by four — and stays dropped.
        let state = apply_peer_admissions(Some(previous), 4.0, 1_000_000.0, LIMIT, BURST);
        assert_eq!(state.tokens, 1.0);

        let outcome = apply_request(Some(state), &params_at(1_000_000.0));
        assert_eq!(outcome.wait, 0.0);
        assert_eq!(outcome.state.tokens, 0.0);
    }

    #[test]
    fn peer_admissions_are_refilled_up_to_when_they_happened() {
        let previous = BucketState {
            last: 1_000_000.0,
            tokens: 0.0,
        };

        // Two seconds later a peer takes three tokens. By then the bucket had refilled six,
        // so the level after the debit is three, not minus three.
        let state = apply_peer_admissions(Some(previous), 3.0, 3_000_000.0, LIMIT, BURST);

        assert!(is_close(state.tokens, 3.0));
        assert_eq!(state.last, 3_000_000.0);
    }

    #[test]
    fn peer_admissions_before_the_last_local_request_debit_that_request() {
        let previous = BucketState {
            last: 2_000_000.0,
            tokens: 5.0,
        };

        // The peer's admissions predate the state this replica holds, so they are applied
        // to it directly: the level this replica carried was two lower than it thought.
        let state = apply_peer_admissions(Some(previous), 2.0, 1_000_000.0, LIMIT, BURST);

        assert_eq!(state.tokens, 3.0);
        assert_eq!(state.last, 2_000_000.0);
    }

    #[test]
    fn peer_admissions_against_an_unseen_key_start_from_a_full_bucket() {
        let state = apply_peer_admissions(None, 4.0, REALISTIC_NOW, LIMIT, BURST);

        assert_eq!(state.tokens, BURST - 4.0);
        assert_eq!(state.last, REALISTIC_NOW);
    }

    #[test]
    fn peer_admissions_can_exhaust_a_locally_full_bucket() {
        let previous = BucketState {
            last: 1_000_000.0,
            tokens: 10.0,
        };

        let state = apply_peer_admissions(Some(previous), 10.0, 1_000_000.0, LIMIT, BURST);
        let outcome = apply_request(Some(state), &params_at(1_000_000.0));

        // Peers have taken everything the bucket held, so the caller must wait.
        assert!(outcome.wait > 0.0);
    }

    #[test]
    fn clock_going_backwards_does_not_credit_tokens() {
        let previous = BucketState {
            last: 2_000_000.0,
            tokens: 5.0,
        };
        let outcome = apply_request(Some(previous), &params_at(1_000_000.0));

        // Elapsed is clamped to zero, so exactly one token is consumed and no refill occurs.
        assert_eq!(outcome.state.tokens, 4.0);
        assert_eq!(outcome.wait, 0.0);
    }
}
