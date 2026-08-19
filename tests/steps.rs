//! Mesh accuracy, driven through the production path.
//!
//! Replicas are real `BucketStore` instances, and an exchange is the same
//! `collect_report` → `PeerReport::new` → `decode_report` → `apply_peer_admissions`
//! sequence the publisher and the peer endpoint perform. Only the HTTP transport is left
//! out, which is the one seam a test is entitled to replace — everything that decides an
//! outcome is production code.

use std::time::{Duration, Instant};

use cucumber::{given, then, when};

use traefik_ratelimit_store::bucket::BucketParams;
use traefik_ratelimit_store::peers::{decode_report, encode_report};
use traefik_ratelimit_store::store::{BucketStore, KeyHash, StoreConfig};

/// The caller's clock at the start of every scenario, in microseconds. Scenarios that
/// freeze time send exactly this with every request, so no refill occurs and the budget
/// under test is exactly the configured burst.
const START: f64 = 1_787_000_000_000_000.0;

/// The key every request in a scenario competes for.
const CLIENT_KEY: &[u8] = b"rate:mw-shared:203.0.113.9";

const TTL: Duration = Duration::from_secs(600);

/// How often replicas exchange, in requests served by a single replica.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Exchange {
    Never,
    EveryRequests(usize),
}

struct Replica {
    id: String,
    store: BucketStore,
}

#[derive(cucumber::World)]
#[world(init = Self::new)]
pub struct World {
    rate_per_second: f64,
    burst: f64,
    max_delay: f64,
    replicas: Vec<Replica>,
    exchange: Exchange,
    admitted: usize,
}

impl std::fmt::Debug for World {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("World")
            .field("replicas", &self.replicas.len())
            .field("exchange", &self.exchange)
            .field("admitted", &self.admitted)
            .finish()
    }
}

impl World {
    fn new() -> Self {
        Self {
            rate_per_second: 0.0,
            burst: 0.0,
            max_delay: 0.0,
            replicas: Vec::new(),
            exchange: Exchange::EveryRequests(1),
            admitted: 0,
        }
    }

    fn params_at(&self, now: f64) -> BucketParams {
        BucketParams {
            limit: self.rate_per_second / 1_000_000.0,
            burst: self.burst,
            now,
            max_delay: self.max_delay,
        }
    }

    /// One replica tells every other what it admitted — the same bytes the publisher
    /// renders, decoded the same way the peer endpoint decodes them.
    fn exchange_from(&self, index: usize, at: Instant) {
        let source = &self.replicas[index];
        let body = encode_report(&source.id, &source.store.collect_report(usize::MAX, at));

        for (other, replica) in self.replicas.iter().enumerate() {
            if other == index {
                continue;
            }
            let admissions = decode_report(&body, &replica.id, usize::MAX)
                .expect("a report built by the store must decode");
            for (key, peer) in admissions {
                replica.store.apply_peer_admissions(key, peer, at);
            }
        }
    }

    /// Drives `requests` — each `(caller time, replica index)` — through the replicas,
    /// exchanging as the scenario configured.
    fn drive(&mut self, requests: impl Iterator<Item = (f64, usize)>) {
        let key = KeyHash::from_key(CLIENT_KEY);
        let at = Instant::now();
        let mut served_by_replica = vec![0usize; self.replicas.len()];

        for (now, index) in requests {
            let params = self.params_at(now);
            let outcome = self.replicas[index]
                .store
                .apply_request(key, &params, TTL, at);

            if outcome.wait <= params.max_delay {
                self.admitted += 1;
            }

            served_by_replica[index] += 1;
            if let Exchange::EveryRequests(every) = self.exchange
                && served_by_replica[index].is_multiple_of(every)
            {
                self.exchange_from(index, at);
            }
        }
    }
}

#[given(regex = r"^a rate limit of (\d+) requests per (\d+) seconds with a burst of (\d+)$")]
async fn given_rate_limit(world: &mut World, average: f64, period: f64, burst: f64) {
    world.rate_per_second = average / period;
    world.burst = burst;
    // The caller derives the delay it will tolerate from the rate, and this mirrors that.
    world.max_delay = if world.rate_per_second >= 1.0 {
        1_000_000.0 / (world.rate_per_second * 2.0)
    } else {
        500_000.0
    };
}

#[given(regex = r"^(\d+) replicas?$")]
async fn given_replicas(world: &mut World, count: usize) {
    world.replicas = (0..count)
        .map(|index| Replica {
            id: format!("replica-{index}"),
            store: BucketStore::new(StoreConfig::default()),
        })
        .collect();
}

#[given("counters are exchanged after every request")]
async fn given_exchange_every_request(world: &mut World) {
    world.exchange = Exchange::EveryRequests(1);
}

#[given(regex = r"^counters are exchanged after every (\d+) requests$")]
async fn given_exchange_every_n(world: &mut World, every: usize) {
    world.exchange = Exchange::EveryRequests(every);
}

#[given("counters are never exchanged")]
async fn given_no_exchange(world: &mut World) {
    world.exchange = Exchange::Never;
}

#[when(regex = r"^(\d+) requests for the same client arrive at once$")]
async fn when_requests_arrive_at_once(world: &mut World, count: usize) {
    let replica_count = world.replicas.len();
    // Round robin, which is what a load balancer spreading connections produces. Time
    // is frozen, so the only budget is the burst.
    world.drive((0..count).map(|request| (START, request % replica_count)));
}

#[when(regex = r"^(\d+) requests per second for the same client arrive for (\d+) seconds$")]
async fn when_sustained_spread(world: &mut World, per_second: usize, seconds: usize) {
    let replica_count = world.replicas.len();
    let step = 1_000_000.0 / per_second as f64;
    world.drive(
        (0..per_second * seconds)
            .map(|request| (START + request as f64 * step, request % replica_count)),
    );
}

#[when(
    regex = r"^(\d+) requests per second for the same client arrive for (\d+) seconds, all at one replica$"
)]
async fn when_sustained_concentrated(world: &mut World, per_second: usize, seconds: usize) {
    let step = 1_000_000.0 / per_second as f64;
    world.drive((0..per_second * seconds).map(|request| (START + request as f64 * step, 0)));
}

#[when(
    regex = r"^(\d+) requests per second for the same client arrive for (\d+) seconds, moving to the next replica every (\d+) seconds$"
)]
async fn when_sustained_shifting(
    world: &mut World,
    per_second: usize,
    seconds: usize,
    move_every: usize,
) {
    let replica_count = world.replicas.len();
    let step = 1_000_000.0 / per_second as f64;
    world.drive((0..per_second * seconds).map(|request| {
        let elapsed = request as f64 * step;
        let replica = (elapsed / (move_every as f64 * 1_000_000.0)) as usize % replica_count;
        (START + elapsed, replica)
    }));
}

#[then(regex = r"^(\d+) requests are admitted$")]
async fn then_exactly_admitted(world: &mut World, expected: usize) {
    assert_eq!(world.admitted, expected);
}

#[then(regex = r"^at most (\d+) requests are admitted$")]
async fn then_at_most_admitted(world: &mut World, ceiling: usize) {
    assert!(
        world.admitted <= ceiling,
        "{} admitted, ceiling {ceiling}",
        world.admitted
    );
}

#[then(regex = r"^at least (\d+) requests are admitted$")]
async fn then_at_least_admitted(world: &mut World, floor: usize) {
    assert!(
        world.admitted >= floor,
        "{} admitted, floor {floor}",
        world.admitted
    );
}
