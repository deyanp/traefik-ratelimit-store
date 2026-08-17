//! Mesh accuracy, driven through the production path.
//!
//! Replicas are real `BucketStore` and `PeerTable` instances, and a broadcast is the same
//! `collect_consumption` → `PeerReport::new` → `record` sequence the publisher performs.
//! Only the HTTP transport is left out, which is the one seam a test is entitled to
//! replace — everything that decides an outcome is production code.

use std::time::{Duration, Instant};

use cucumber::{given, then, when};

use traefik_ratelimit_store::bucket::BucketParams;
use traefik_ratelimit_store::peers::{PeerReport, PeerTable};
use traefik_ratelimit_store::store::{BucketStore, KeyHash, StoreConfig};

/// Every request in a scenario carries the same timestamp, so no refill occurs and the
/// budget under test is exactly the configured burst.
const FROZEN_NOW: f64 = 1_787_000_000_000_000.0;

/// The key every request in a scenario competes for.
const CLIENT_KEY: &[u8] = b"rate:mw-shared:203.0.113.9";

const TTL: Duration = Duration::from_secs(60);

/// How often replicas broadcast, in requests served by a single replica.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Broadcast {
    Never,
    EveryRequests(usize),
}

struct Replica {
    id: String,
    store: BucketStore,
    peers: PeerTable,
}

#[derive(cucumber::World)]
#[world(init = Self::new)]
pub struct World {
    rate_per_second: f64,
    burst: f64,
    max_delay: f64,
    replicas: Vec<Replica>,
    broadcast: Broadcast,
    /// Age applied to peer reports, so a scenario can make peers look silent.
    peer_silence: Duration,
    admitted: usize,
}

impl std::fmt::Debug for World {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("World")
            .field("replicas", &self.replicas.len())
            .field("broadcast", &self.broadcast)
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
            broadcast: Broadcast::EveryRequests(1),
            peer_silence: Duration::ZERO,
            admitted: 0,
        }
    }

    fn params(&self) -> BucketParams {
        BucketParams {
            limit: self.rate_per_second / 1_000_000.0,
            burst: self.burst,
            now: FROZEN_NOW,
            max_delay: self.max_delay,
            peer_consumed: 0.0,
        }
    }

    /// One replica tells every other what it has consumed, exactly as the publisher does.
    fn broadcast_from(&self, index: usize, at: Instant) {
        let source = &self.replicas[index];
        let report = PeerReport::new(
            source.id.clone(),
            &source.store.collect_consumption(usize::MAX),
        );

        for (other, replica) in self.replicas.iter().enumerate() {
            if other != index {
                replica.peers.record(&replica.id, report.clone(), at);
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
    // Generous, so a peer only ages out when a scenario says it should.
    let staleness_limit = Duration::from_secs(2);

    world.replicas = (0..count)
        .map(|index| Replica {
            id: format!("replica-{index}"),
            store: BucketStore::new(StoreConfig::default()),
            peers: PeerTable::new(staleness_limit),
        })
        .collect();
}

#[given("counters are exchanged after every request")]
async fn given_broadcast_every_request(world: &mut World) {
    world.broadcast = Broadcast::EveryRequests(1);
}

#[given(regex = r"^counters are exchanged after every (\d+) requests$")]
async fn given_broadcast_every_n(world: &mut World, every: usize) {
    world.broadcast = Broadcast::EveryRequests(every);
}

#[given("counters are never exchanged")]
async fn given_no_broadcast(world: &mut World) {
    world.broadcast = Broadcast::Never;
}

#[given(regex = r"^the peers have been silent for (\d+) seconds$")]
async fn given_peers_silent(world: &mut World, seconds: u64) {
    world.peer_silence = Duration::from_secs(seconds);
}

#[when(regex = r"^(\d+) requests for the same client arrive$")]
async fn when_requests_arrive(world: &mut World, count: usize) {
    let key = KeyHash::from_key(CLIENT_KEY);
    let params = world.params();
    let now = Instant::now();
    let replica_count = world.replicas.len();
    let mut served_by_replica = vec![0usize; replica_count];

    for request in 0..count {
        // Round robin, which is what a load balancer spreading connections produces.
        let index = request % replica_count;

        let peer_consumed = {
            let replica = &world.replicas[index];
            // Reports are read at a moment shifted by the scenario's silence, so a scenario
            // can age its peers out without waiting.
            f64::from(replica.peers.consumed_for(key, now + world.peer_silence))
        };

        let outcome = world.replicas[index].store.apply_request(
            key,
            &BucketParams {
                peer_consumed,
                ..params
            },
            TTL,
            now,
        );

        if outcome.wait <= params.max_delay {
            world.admitted += 1;
        }

        served_by_replica[index] += 1;

        if let Broadcast::EveryRequests(every) = world.broadcast
            && served_by_replica[index].is_multiple_of(every)
        {
            world.broadcast_from(index, now);
        }
    }
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
