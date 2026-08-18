# traefik-ratelimit-store

A rate-limit counter store that speaks just enough of the Redis wire protocol to back
Traefik's `rateLimit` middleware, so limits can be shared across proxy replicas without
running Redis or Valkey.

## Why it exists

Traefik's in-memory rate limiter keeps one bucket map per *router*, not per middleware,
and reclaims expired entries only once a map is full. On a large routing table that means
memory that grows all day and effective limits many times looser than configured.

Both are reported upstream ([#13704], [#13706]). What upstream does not fix is that the
in-memory limiter is per-pod: run Traefik as a DaemonSet and the effective limit is
multiplied by the node count. Traefik's only answer to cross-pod state is "configure
Redis", and this is that Redis-shaped thing without the Redis.

Dividing the configured limit by the node count is not a substitute. A load balancer
hashes the connection and HTTP keep-alive pins a client to one proxy pod, so one client is
served by one pod. Divide by N and a normal client is throttled at 1/N of the intended
rate, while a source spread across nodes still gets N times too much.

## What it is not

It is not a Redis server, and it does not execute Lua. Traefik ships its bucket algorithm
as a Lua script and addresses it by SHA-1; this store implements the same arithmetic
natively and treats the digest as an identifier. The script source is compared, never run.

Six commands are implemented. Everything else answers with an error.

| Command | Behaviour |
|---|---|
| `HELLO` | Refused, which is how the client falls back to RESP2 |
| `CLIENT` | Refused; the client discards the result |
| `AUTH`, `SELECT` | `+OK` |
| `PING` | `+PONG` |
| `EVALSHA` | Serves a known digest, otherwise `NOSCRIPT` |
| `EVAL` | Validates the source, registers its digest, serves |

## How the script check works

The registry starts empty, so the first `EVALSHA` is answered `NOSCRIPT`. The client then
resends the whole script source, which is compared against the texts in
`src/script.rs::KNOWN_SCRIPTS`. A match registers the digest and every later call is served
directly. A miss is still served — refusing would turn a proxy upgrade into an outage — but
it logs `ScriptDiverged` so an algorithm change is noticed rather than silently served with
stale semantics.

The set has more than one member for a reason. Between Traefik v3.7.1 and later v3.7 the
script gained one space before the final `tostring(tokens)`: semantically nothing, entirely
different digest. A fleet part-way through an upgrade sends both. Comparison stays
exact-match — normalising whitespace would also make it blind to a real change, which is
the one thing the check exists to catch.

## How replicas share counters

Replicas broadcast **consumption**, not bucket state. Bucket state does not merge: taking
the newest or largest of two `(last, tokens)` pairs discards one replica's increments,
which is the over-admission the sharing exists to prevent. Counts add, so they merge.

Every replica sends its own report to every peer each interval — a full mesh, not a gossip
protocol. At a handful of replicas that is simpler and one hop rather than several rounds.
Because a replica only ever publishes its own consumption, each entry in the peer table has
exactly one author, so arriving reports overwrite rather than combine and there is no merge
logic at all.

A peer that stops reporting ages out of the staleness window and stops counting, so losing
one degrades to "count alone" rather than to a wrong answer.

**Accuracy.** Overshoot is bounded by `(N−1) × rate × interval`. At three replicas, three
requests per second and a 150ms interval that is under one request. The bound scales with
the configured rate, so a high-rate middleware needs a shorter interval.

## Configuration

Everything has a working default; nothing is required.

| Variable | Default | Meaning |
|---|---|---|
| `LISTEN_ADDRESS` | `0.0.0.0:6379` | Where the protocol listener binds |
| `PEER_LISTEN_ADDRESS` | `0.0.0.0:8080` | Where the peer endpoint binds |
| `PEER_ENDPOINT` | *(empty)* | DNS name resolving to all peers, or a comma-separated list. Empty means this replica counts alone |
| `PEER_PUBLISH_INTERVAL_MS` | `150` | How often consumption is published to peers |
| `PEER_STALENESS_LIMIT_MS` | `1000` | How long a peer's report stays usable |
| `PEER_REQUEST_TIMEOUT_MS` | `50` | How long a single delivery may take before it is abandoned |
| `PEER_MAX_KEYS_PER_REPORT` | `10000` | Most keys a report carries, busiest first |
| `PEER_SHARED_SECRET` | *(empty)* | Bearer token peers must present. Required once `PEER_ENDPOINT` is set |
| `PEER_ALLOW_UNAUTHENTICATED` | `false` | Accept an unauthenticated peer endpoint deliberately |
| `STORE_MEMORY_BUDGET_MB` | *(the cgroup limit)* | Budget the entry ceiling is derived from |
| `STORE_SHARD_COUNT` | `16` | Independently locked shards |
| `STORE_CAPACITY_PER_SHARD` | *(derived)* | Override the derived ceiling. Rarely needed |
| `STORE_SWEEP_INTERVAL_MS` | `1000` | How often expired entries are reclaimed |
| `REPLICA_ID` | `$HOSTNAME` | Identity used to discard this replica's own peer reports |

Nothing here is Kubernetes-specific: peer discovery is a DNS name or a static list, and the
replica identity is a string. Under Kubernetes that DNS name is a headless Service.

## Configuring Traefik against it

```yaml
apiVersion: traefik.io/v1alpha1
kind: Middleware
metadata:
  name: rate-limits-api
spec:
  rateLimit:
    average: 30
    burst: 10
    period: 10s
    redis:
      endpoints: ["traefik-ratelimit-store.default.svc.cluster.local:6379"]
      db: 0
      poolSize: 2
      maxActiveConns: 5
      dialTimeout: 200ms
      readTimeout: 200ms
      writeTimeout: 200ms
```

Two of those are not tuning.

**Exactly one endpoint.** Traefik builds a cluster client whenever more than one address is
configured, and that client immediately asks for `CLUSTER SLOTS`. Point it at a single
ClusterIP Service and let the Service spread the connections.

**Tight timeouts.** Traefik answers any store error with HTTP 500 and has no fail-open
switch. Left at the defaults — 5s dial, 3s read, three retries — an unreachable store makes
each request hang about twenty seconds before failing. At 200ms it is under two.

## Health

Two endpoints on the peer port, answering different questions.

| Path | Probe | Meaning |
|---|---|---|
| `/health` | liveness | The process exists. Deliberately trivial |
| `/readiness` | readiness | The **protocol** listener answers a `PING`, and this replica is not draining |

Readiness checks the port Traefik uses, not the port serving the probe: a replica whose
protocol listener has stopped must leave the rotation even though its HTTP endpoint is
healthy.

They must not share an endpoint. A draining replica is emphatically alive, and a liveness
probe that fails during a drain kills the pod it was meant to let finish.

On `SIGTERM` the process fails readiness first, keeps serving for a drain period, then
exits — so the orchestrator withdraws the replica before its listener closes.

## Cost

Measured on a laptop. Reproduce with the examples below.

**End to end, over TCP** (`cargo run --release --example latency`):

| concurrency | throughput | p50 | p99 |
|---|---|---|---|
| 1 | 24k/s | 34us | 129us |
| 16 | 68k/s | 226us | 328us |
| 100 | 52k/s | 1.6ms | 2.6ms |
| 500 | 50k/s | 6.4ms | 16.7ms |
| 1500 | 47k/s | 26.3ms | 37.6ms |

Read that as a saturation curve, not a cost curve. Throughput plateaus around 50k/s from a
hundred connections on, after which latency is queue wait rather than service time —
Little's Law predicts 1500/47000 = 32ms against 26ms measured. The client shares the
machine with the store here, so the plateau is the *pair* saturating, not the store alone.

Concurrency in that table means requests in flight, not connections open. A pool of
idle connections costs memory and nothing else; only concurrent requests queue. With
`maxActiveConns: 5` the proxy bounds its own in-flight requests per middleware instance,
which keeps the left of this table the operative part — and is also what makes pool
exhaustion, rather than store latency, the thing to watch.

At 1500 concurrent connections the store held peak RSS of **29.8MB** against the manifest's
128Mi limit, about 18KB per connection, and completed all 75,000 requests without error.

**The store's own operations** (`cargo run --release --example store_cost`):

| Operation | Cost |
|---|---|
| `apply_request`, one key | 36ns |
| `apply_request`, distinct keys | 184ns |
| `sweep_expired`, 200k entries | 730us |
| capacity trim, worst case | 558us |

The store contributes tens of nanoseconds to a request that costs tens of microseconds end
to end — almost all of the measured latency is socket and scheduling, not this code. The
16-connection column is queueing on a machine with far fewer cores than connections:
throughput rises while latency does, which is what saturation looks like.

The number that matters is the tail against the 200ms read timeout. At low concurrency the
p99 has several hundred times' headroom; at 1500 concurrent requests it is closer to five
times. The proxy has no fail-open switch, so a store slow enough to hit that timeout
produces a 500 rather than a slow request — which is why the headroom, not the median,
is the figure to track.

The capacity trim is the only operation that can stall the request path, because it holds a
shard's mutex. It selects a cut point rather than sorting; sorting cost 2ms, sixteen times
the normal p99. It should never run at all — reaching it means distinct keys are arriving
faster than they expire.

The measurements read whole replies rather than stopping at the first `read`. That is not
pedantry: an earlier version stopped early, so leftover bytes were attributed to the
following request, and the distribution was wrong in both directions at once.

## Memory

Entries are reclaimed on a timer rather than when a map fills, and the sweep covers every
shard whether or not it saw traffic — so an idle store shrinks instead of merely not
growing. With Traefik's computed TTL of one to two seconds, the resident set is the sources
active in the last couple of seconds: proportional to concurrent traffic, not to uptime.

Rate-limit keys are hashed on arrival, so a caller's raw source — which may be a
credential — never exists at rest.

**Cardinality is the thing to size against, and hashing does not reduce it.** Hashing gives
a fixed 16 bytes and non-reversibility; one distinct source is still one entry. It does make
each entry smaller: the key the proxy sends is `rate:<middleware>:<source>`, around 54 bytes
for a realistic middleware name, so a 16-byte hash beats storing it raw — which would also
cost a heap allocation and a pointer to chase on every lookup. Measured with
`cargo run --release --example memory_per_key`:

| active keys | store RSS | bytes/key | peer report |
|---|---|---|---|
| 100,000 | 17.5MB | 161 | 3.7MB |
| 250,000 | 84MB | 337 | 9.2MB |
| 500,000 | 184MB | 373 | 18.5MB |
| 1,000,000 | 383MB | 390 | 37MB |

**So the ceiling is derived, not configured.** The entry count and the memory limit are one
decision, and an earlier version of this store set them apart — a 128Mi limit beside a
ceiling of a million entries, about 383MB, so the process would have been killed at roughly
350k keys while its own backstop still reported headroom.

There is now nothing to keep in step. The store reads the container's memory limit from
cgroup — a kernel facility, so it works the same under Kubernetes, Docker and systemd — and
sizes itself, giving entries half the budget and leaving the rest for connection buffers.
`STORE_MEMORY_BUDGET_MB` overrides the discovered limit, `STORE_CAPACITY_PER_SHARD`
overrides the result, and neither is normally needed.

It says what it chose, at startup:

```
entry ceiling sized against the memory budget
  shards=16 entries_per_shard=10485 total_entries=167760 approximate_bytes=67104000
```

| memory limit | entries held | of which |
|---|---|---|
| 128Mi | 167,760 | ~62MB |
| 256Mi | 335,536 | ~125MB |
| 512Mi | 671,088 | ~250MB |

If your distinct-source count exceeds the middle column, raise the container limit; the
ceiling follows on its own.

**And if it is exceeded anyway?** The ceiling holds and the least recently active entries
are shed. The flood evicts itself: a source seen once is the least recently active thing in
the shard, while a source still sending is the last thing to go — so the client worth
limiting stays limited, and the entries dropped belong to sources nowhere near their limit,
which are re-admitted against a fresh bucket. Memory stays bounded, and the cost is
amortized because a trim frees a tenth of a shard and cannot recur until that tenth
refills.

It logs `StoreAtCapacity` when this happens. **Alarm on it** — it is the one condition you
cannot infer from anything else: memory looks fine, latency looks fine, and quietly some
sources are being admitted against fresh buckets.

Peer reports are capped at the busiest `PEER_MAX_KEYS_PER_REPORT` keys, because a report
carries one entry per active key and a wide keyspace would otherwise mean megabytes to
every peer several times a second. Truncating by consumption is what makes the cap safe: a
key taken once contributes one token to a peer's decision, so dropping it risks one extra
admission, while the keys where sharing decides anything are the ones kept.

## Security

The peer endpoint accepts a report from anyone the network allows unless
`PEER_SHARED_SECRET` is set, in which case a matching `Authorization: Bearer` header is
required and the comparison is constant-time. Running without one logs
`PeerEndpointUnauthenticated` at startup.

**Set it.** An unauthenticated endpoint is a rate-limit bypass, not merely a nuisance.
Reports are keyed by replica id and overwrite, so a stranger who reaches the endpoint and
knows a replica's id — a pod name — can send an empty report in its name and erase that
replica's consumption from every peer's view. Repeat for each replica and every one of them
believes it is alone, which is N times the configured limit: exactly the failure this store
exists to prevent.

Demonstrated: a peer honestly reporting ten tokens taken produces a 333ms delay for the
next request; a forged empty report in that peer's name drops it to zero.

A NetworkPolicy is the other half, not a substitute — k3s and k3d ship flannel, which does
not enforce NetworkPolicy at all, and on managed clusters it holds only where the CNI
enforces it.

Neither default is safe, so there is no default. A replica with `PEER_ENDPOINT` set and no
`PEER_SHARED_SECRET` **refuses to start**, naming both ways out. Requiring a secret that
has not been configured would be no better than omitting one: every report would be
rejected, every peer would age out, and the same N-times-looser limit would arrive by a
different route. The only safe thing is to make the operator choose, so
`PEER_ALLOW_UNAUTHENTICATED=true` records the decision in configuration rather than letting
it happen by omission.

The shipped manifest reads the secret from a Kubernetes Secret:

```sh
kubectl create secret generic traefik-ratelimit-store \
    --from-literal=peer-shared-secret="$(openssl rand -hex 32)"
```

Request bodies are capped, the container runs as non-root on a read-only root filesystem
with all capabilities dropped, and the image carries no shell.

## Tests

```sh
cargo test                        # units, differential conformance, mesh scenarios
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

Seven layers, each covering something the others cannot.

**Unit tests** — the arithmetic, the protocol framing, the store's expiry and capacity
behaviour, the peer table, health. Includes a concurrency test driving one key from
sixteen threads, because every other test is sequential and a sequential test cannot
distinguish a correct lock from no lock at all.

**Differential conformance** — executes the reference Lua in a test-only interpreter and
diffs it against the native implementation over a generated corpus. This is the only thing
that proves the reimplementation is faithful. It runs the `PINNED_SCRIPT` constant, so a
bad transcription into this repository fails the test rather than lurking behind a digest
comparison that only ever compares it to itself.

**Mesh scenarios** (`tests/mesh_accuracy.feature`) — cucumber, covering what sharing
achieves and what it costs between broadcasts. Two replicas sharing admit what one would;
two replicas that never exchange admit twice as much.

**`conformance/end-to-end.sh`** — real Traefik with a `rateLimit` middleware pointed at
this store, via the file provider. Exercises Traefik's middleware, its Redis client, this
store's protocol handling and the arithmetic together.

**`conformance/k3d/run.sh`** — a throwaway k3d cluster, three store replicas behind real
Traefik reading a `Middleware` CRD. The only test covering the Kubernetes CRD provider,
peer discovery through a headless Service, and the manifests in `deploy/` as written.
Three replicas is what makes the result meaningful: without shared counters a burst of five
would admit up to fifteen.

```
Store replicas:   3
Peer addresses:   10.42.0.5 10.42.2.2 10.42.1.2
admitted (200): 6
refused  (429): 14
PASSED: 3 replicas enforced one shared limit behind real Traefik
```

The cluster is deleted afterwards and the kubectl context is never switched. Pass
`--keepCluster true` to inspect it.

**Alarm on three log events.** No metrics are implemented; the store emits structured logs
and nothing else, which is the largest gap remaining before production. `ScriptDiverged`
means a proxy upgrade changed the algorithm. `StoreAtCapacity` means keys are being shed.
`PeerEndpointUnauthenticated` means the mesh is writable by anything the network allows.

**`conformance/soak.sh`** — sustained load with connection churn, watching for leaks. Every
other measurement holds its connections open, which is the shape that hides a leak in
connection handling. Over 800,000 requests and 4,000 connections opened and closed, resident
memory plateaued at 6.8MB by the tenth round and descriptors stayed flat at 15.

```
requests served:  800000
connections made: 4000
rss first/final:  6240KB / 6864KB
fds final:        15
PASSED: memory and descriptors plateaued under sustained churn
```

**`conformance/k3d/rolling_update.sh`** — replaces every store replica while traffic flows.
The proxy answers any store error with a 500, so a rollout that drops connections shows up
as 500s rather than as a blip. Needs a cluster from `run.sh --keepCluster true`.

```
 590 429
 310 200
served: 900   dropped: 0
PASSED: every replica was replaced without dropping a request
```

That passes because of the drain: a replica fails readiness on SIGTERM, keeps serving for
its drain period, then exits — so the orchestrator withdraws it before the listener closes
and the proxy retries the resulting EOF onto a healthy replica.

**`conformance/probe.go.txt`** — drives a running store with go-redis configured exactly as
the rate limiter configures it. Useful for answering a protocol question without standing
up the whole chain. Copy it into a Traefik checkout as `conformance-probe/main.go`.

> On Docker Desktop, the end-to-end script keeps its generated configuration inside the
> repository rather than in a temporary directory. Docker shares only a configured set of
> host paths and `/tmp` is usually not among them; a bind mount from an unshared path
> silently produces an empty directory instead of the file, and Traefik then serves 404
> with nothing in its log to explain why.

## Deployment

`deploy/traefik-ratelimit-store.yaml` — three replicas, two Services (a ClusterIP for the
proxy, a headless one for peer discovery), a PodDisruptionBudget and topology spread.

```sh
docker build -t traefik-ratelimit-store:0.1.0 .
kubectl apply -f deploy/traefik-ratelimit-store.yaml
```

## Status

Built and tested; **not deployed**. `DESIGN.md` carries the full rationale, including the
decision record and the conditions under which this should not be built at all.

Deployment is gated on one upstream change. Traefik has no `denyOnError` for its rate-limit
middleware, so any store error becomes a 500 — [PR #13529] adds it. Until that lands,
adopting this, or any external store, means accepting fail-closed on the request path.

[#13704]: https://github.com/traefik/traefik/issues/13704
[#13706]: https://github.com/traefik/traefik/issues/13706
[PR #13529]: https://github.com/traefik/traefik/pull/13529
