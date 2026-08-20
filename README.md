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

Eight command names are answered. Everything else answers with an error.

| Command | Behaviour |
|---|---|
| `HELLO` | Refused, which is how the client falls back to RESP2 |
| `CLIENT` | Refused; the client discards the result |
| `AUTH`, `SELECT` | `+OK` |
| `PING` | `+PONG` |
| `QUIT` | `+OK`, then the connection is closed |
| `EVALSHA` | Serves a known digest, otherwise `NOSCRIPT` |
| `EVAL` | Validates the source, registers its digest, serves |

## How limits are scoped

The key the proxy sends is `rate:<qualifiedMiddlewareName>:<source>` — the middleware's
namespaced name plus the client (the IP, under `sourceCriterion.ipStrategy`). The router
is deliberately not part of it, so **scope follows the Middleware object, not the route**:

- One Middleware referenced by every route = one budget per client across all of them —
  a client spending its rate on one API has nothing left for the others.
- Separate budgets per service = separate Middleware CRDs. Only the *name* separates
  buckets; two middlewares with identical settings still count apart.
- Chained rate-limit middlewares each keep their own bucket; the stricter one bites first.

The configuration (`average`, `burst`, `period`) is not part of the key either — it arrives
with every request, so editing a Middleware takes effect on the next request against the
same counters. Note the contrast with Traefik's in-memory limiter, which keeps one bucket
map per *router*: there, one shared middleware on fourteen routes and N proxy pods is
14 × N budgets per client. Through the store it is exactly one.

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

Replicas share **admissions**, not bucket state. Bucket state does not merge: taking the
newest or largest of two `(last, tokens)` pairs discards one replica's increments, which is
the over-admission the sharing exists to prevent. An admission is a token taken from the
one logical bucket, so a peer can debit it from its own copy exactly as if the request had
arrived there.

Every replica sends its own report to every peer each interval — a full mesh, not a gossip
protocol. At a handful of replicas that is simpler and one hop rather than several rounds.
A report carries, per key, what this replica admitted *since its last report*, when, and
the `limit`/`burst` the caller sent, so the receiver folds it into its bucket as of that
moment — refill up to then, subtract, done. Applying a report once is the whole protocol:
there is no table to reconcile, no merge, and nothing to age out.

On the wire a report is a version byte, the replica id, and one fixed 48-byte record per
key — sized for heavy load, where a report carries thousands of keys several times a
second to every peer. Measured at 10,000 keys: 480KB (JSON spelled the same report at
~1.5MB), encoded in 27µs, decoded in 34µs — a bounds check and a copy, no parser — and
folded into the receiving store in 371µs. Collecting the report from a store with 200,000
keys touched in one interval costs 5.2ms on the sending side, once per interval, off the
request path.

Debiting the stored level — rather than offsetting each later decision by what peers
reported recently — is what makes the limit hold under **sustained** traffic, not only
across a burst. A decision-time offset leaves every replica refilling its own bucket at the
full configured rate, and once each level sits at the raised threshold, every refilled
token is spent locally: the deployment admits N times the rate while looking correct for
the first burst. The cucumber suite pins both regimes.

A peer that stops reporting costs nothing but its own future admissions: what it debited
before stays debited, and what it admits from then on is unknown until it reports again.
Losing one degrades to "count alone for that peer" rather than to a wrong answer.

**Accuracy.** Under sustained traffic the deployment admits the configured rate, however
the traffic is spread: `tests/mesh_accuracy.feature` drives 20 requests per second for 60
seconds at `30 per 10s, burst 10` across three replicas and admits 190–192 (burst plus
rate × time is 190), whether the traffic is spread evenly, concentrated on one replica, or
moves between them. A burst arriving faster than one exchange interval can be admitted up
to once per replica before the first reports land, so the instantaneous overshoot is bounded
by `(N−1) × burst` per key, and at steady state by `(N−1) × rate × interval` — under one
request at three replicas, three per second and 150ms.

## Configuration

Everything has a working default; nothing is required.

| Variable | Default | Meaning |
|---|---|---|
| `LISTEN_ADDRESS` | `0.0.0.0:6379` | Where the protocol listener binds |
| `MAX_CONNECTIONS` | `4096` | Protocol connections served at once; one more is refused |
| `CONNECTION_IDLE_TIMEOUT_MS` | `1800000` | A protocol connection silent for this long is closed (the client's own idle limit is 30 minutes) |
| `DRAIN_PERIOD_MS` | `5000` | How long to keep serving after readiness starts failing on `SIGTERM`; keep it above the readiness probe's period × threshold and below the termination grace period |
| `TOKIO_WORKER_THREADS` | *(cores)* | Runtime worker threads, read by the runtime itself; the shipped manifest sets 4 |
| `PEER_LISTEN_ADDRESS` | `0.0.0.0:8080` | Where the peer endpoint binds |
| `PEER_ENDPOINT` | *(empty)* | DNS name resolving to all peers, or a comma-separated list. Empty means this replica counts alone |
| `PEER_PUBLISH_INTERVAL_MS` | `150` | How often admissions are published to peers |
| `PEER_REQUEST_TIMEOUT_MS` | `50` | How long a single delivery may take before it is abandoned. Must be shorter than the interval |
| `PEER_MAX_KEYS_PER_REPORT` | `10000` | Most keys a report carries, busiest first; also the most accepted in one inbound report |
| `PEER_SHARED_SECRET` | *(empty)* | Bearer token peers must present. Required once `PEER_ENDPOINT` is set |
| `PEER_ALLOW_UNAUTHENTICATED` | `false` | Accept an unauthenticated peer endpoint deliberately |
| `STORE_MEMORY_BUDGET_MB` | *(the cgroup limit)* | Budget the entry ceiling is derived from |
| `STORE_SHARD_COUNT` | `16` | Independently locked shards |
| `STORE_CAPACITY_PER_SHARD` | *(derived)* | Override the derived ceiling. Rarely needed |
| `STORE_SWEEP_INTERVAL_MS` | `1000` | How often expired entries are reclaimed |
| `REPLICA_ID` | `$HOSTNAME` | Identity used to discard this replica's own peer reports |

Nothing here is Kubernetes-specific: peer discovery is a DNS name or a static list, and the
replica identity is a string. Under Kubernetes that DNS name is a headless Service.

A zero interval, shard count, ceiling or report cap, a delivery timeout at or beyond the
publish interval, or a malformed number anywhere **refuses to start** with a message naming
the variable. Each of those used to fail later, inside a background task, where nothing
would have noticed.

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
| 1 | 27k/s | 33us | 67us |
| 16 | 73k/s | 215us | 290us |
| 100 | 71k/s | 1.4ms | 1.9ms |
| 500 | 62k/s | 7.2ms | 9.8ms |
| 1500 | 66k/s | 21.7ms | 24.1ms |

Read that as a saturation curve, not a cost curve. Throughput plateaus around 65-70k/s from
sixteen connections on, after which latency is queue wait rather than service time —
Little's Law predicts 1500/66000 = 23ms against 21.7ms measured. The client shares the
machine with the store here, so the plateau is the *pair* saturating, not the store alone.

Concurrency in that table means requests in flight, not connections open. A pool of
idle connections costs memory and nothing else; only concurrent requests queue. With
`maxActiveConns: 5` the proxy bounds its own in-flight requests per middleware instance,
which keeps the left of this table the operative part — and is also what makes pool
exhaustion, rather than store latency, the thing to watch.

At 1500 concurrent connections the store held peak RSS of **22.9MB** against the manifest's
128Mi limit, about 15KB per connection, and completed all 750,000 requests without error.
Connections start with a 2KB read buffer that grows only if a command needs it, and one
that goes silent for `CONNECTION_IDLE_TIMEOUT_MS` is closed, so idle pools stop costing
even that.

**The store's own operations** (`cargo run --release --example store_cost`):

| Operation | Cost |
|---|---|
| `apply_request`, one key | 18ns |
| `apply_request`, distinct keys | 142ns |
| `sweep_expired`, 200k entries | 716us |
| capacity trim, worst case | 554us |
| `collect_report`, 200k touched keys capped to 10k | 4.7ms |
| encode / decode a 10k-key report | 28us / 29us |
| fold a 10k-key report into the receiving store | 368us |

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
cost a heap allocation and a pointer to chase on every lookup. An entry is a 16-byte key,
a 56-byte record and one control byte in an 81-byte table slot, and each shard's table is
allocated for its ceiling up front — so a store's memory is decided by its ceiling, not by
its moment-to-moment key count. Measured on Linux with
`cargo run --release --example memory_per_key`, on a store sized to hold a million keys
(two million slots, 163MB of tables):

| filled | store RSS | peer report if every key is touched |
|---|---|---|
| 100,000 | 150MB | 4.8MB |
| 250,000 | 163MB | 12MB |
| 1,000,000 | 163.5MB | 48MB |

The RSS is the table allocation, nearly in full from the first hundred thousand keys:
hashing scatters entries, so almost every page is touched early. What the budget buys is
therefore known at startup — `table_bytes`, logged — and nothing grows afterwards.

**So the ceiling is derived, not configured.** The entry count and the memory limit are one
decision, and an earlier version of this store set them apart — a 128Mi limit beside a
ceiling of a million entries, so the process would have been killed at a few hundred
thousand keys while its own backstop still reported headroom.

There is now nothing to keep in step. The store reads the container's memory limit from
cgroup — a kernel facility, so it works the same under Kubernetes, Docker and systemd — and
sizes itself: half the budget goes to the shard tables, the rest to connection buffers and
the allocator. Each shard's table is allocated once, at the largest power of two of slots
that fits its share, and filled to the seven-eighths the map allows — so the figure planned
for is the figure allocated, and no table ever grows under a shard's lock while requests
wait. `STORE_MEMORY_BUDGET_MB` overrides the discovered limit (the shipped manifest feeds it
from the container's own limit through the Downward API), `STORE_CAPACITY_PER_SHARD`
overrides the result, and neither is normally needed.

It says what it chose, at startup:

```
entry ceiling sized against the memory budget
  shards=16 entries_per_shard=28672 total_entries=458752 table_bytes=42467328
```

| memory limit | entries held | tables |
|---|---|---|
| 128Mi | 458,752 | 40.5MB |
| 256Mi | 917,504 | 81MB |
| 512Mi | 1,835,008 | 162MB |

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
carries one entry per key touched since the last one and a wide keyspace would otherwise
mean megabytes to every peer several times a second. Nothing is lost by the cap: a key left
out keeps its count and goes in a later report, so the cap only delays the keys where
sharing decides the least.

## Security

The peer endpoint accepts a report from anyone the network allows unless
`PEER_SHARED_SECRET` is set, in which case a matching `Authorization: Bearer` header is
required and the comparison is constant-time. Running without one logs
`PeerEndpointUnauthenticated` at startup.

**Set it.** An unauthenticated endpoint is a rate-limit bypass, not merely a nuisance. A
report is folded into this replica's buckets exactly like a peer's, so a stranger who
reaches the endpoint can claim admissions for a key and throttle it, or claim a timestamp
far in the future and refill a drained bucket. Do that to each replica and the limit is
whatever the stranger says it is.

A NetworkPolicy is the other half, not a substitute, and the shipped manifest carries one:
the protocol port admits only pods labelled as Traefik, the peer port only the store's own
replicas (and the kubelet, for probes). The protocol port needs it most — `AUTH` is
accepted unconditionally, so anything that can reach 6379 can drive any bucket. Whether it
holds depends on the cluster: k3s enforces it through its embedded policy controller
(verified live — an unlabelled pod is refused before the store sees a packet), and managed
clusters enforce it where their CNI does. Confirm on yours rather than assuming.

Neither default is safe, so there is no default. A replica with `PEER_ENDPOINT` set and no
`PEER_SHARED_SECRET` **refuses to start**, naming both ways out. Requiring a secret that
has not been configured would be no better than omitting one: every report would be
rejected, every replica would count alone, and the same N-times-looser limit would arrive
by a different route. The only safe thing is to make the operator choose, so
`PEER_ALLOW_UNAUTHENTICATED=true` records the decision in configuration rather than letting
it happen by omission.

The shipped manifest reads the secret from a Kubernetes Secret:

```sh
kubectl create secret generic traefik-ratelimit-store \
    --from-literal=peer-shared-secret="$(openssl rand -hex 32)"
```

Request bodies are capped and authorisation is checked before a body is parsed; the script
registry remembers at most sixteen unrecognised texts, so a stranger on the protocol port
cannot grow the process one `EVAL` at a time; the container runs as non-root on a
read-only root filesystem with all capabilities dropped, and the image carries no shell.

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

**Alarm on these log events.** No metrics are implemented; the store emits structured logs
and nothing else, which is the largest gap remaining before production. `ScriptDiverged`
means a proxy upgrade changed the algorithm. `StoreAtCapacity` means keys are being shed.
`PeerPublishRejected` means peers refuse this replica's reports — a secret that differs
between replicas — and the mesh is silently counting alone. `BackgroundTaskStopped` means
the sweeper, publisher or peer endpoint died and the process is exiting so it is replaced.
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

**`conformance/Dockerfile.loadgen`** — the latency example packaged to run as a pod, for
load-testing a deployed store over the cluster's own network. Its header is the runbook,
including the NetworkPolicy exception the load pod needs. Measured against three replicas
on a shared 2-CPU node: 10M requests at 192k requests/s, p50 1.8ms, p99 27.8ms, ~310m CPU
and 8MiB per replica, nothing dropped.

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

`deploy/traefik-ratelimit-store.yaml` — three replicas rolled in place (no surge pod, so a
rollout needs no spare scheduling slot), two Services (a ClusterIP for the proxy, a
headless one for peer discovery), a PodDisruptionBudget, topology spread, and the
NetworkPolicy for both ports.

```sh
# Without the secret the replicas refuse to start, deliberately (see Security).
kubectl create secret generic traefik-ratelimit-store \
    --from-literal=peer-shared-secret="$(openssl rand -hex 32)"
kubectl apply -f deploy/traefik-ratelimit-store.yaml
```

The manifest names the published image, so there is nothing to build first. To run your
own build instead, tag it with that same name before applying — `IfNotPresent` prefers
whatever is already on the node, so no registry is involved:

```sh
docker build -t ghcr.io/deyanp/traefik-ratelimit-store:0.1.0 .
```

## Releases

Tagging is what publishes. `.github/workflows/release.yml` builds the image, scans it
with the gate CI already applies, pushes it to GHCR, and writes the GitHub Release.

```sh
git tag -a v0.1.0 -m "v0.1.0"
git push origin v0.1.0
```

The tag must match the version in `Cargo.toml` and in `deploy/traefik-ratelimit-store.yaml`.
The workflow checks that first and stops before building anything if the three disagree.
A fixable critical finding in the scan stops the release with nothing pushed.

Images are `ghcr.io/deyanp/traefik-ratelimit-store:<version>` — one tag, one image, never
moved, which is what makes `imagePullPolicy: IfNotPresent` safe above.

## Status

Built, tested, and running in a development cluster behind real Traefik — three replicas
enforcing one shared limit, load-tested in place at 10M requests (192k requests/s, p99
27.8ms, nothing dropped). **Not in production.** `DESIGN.md` carries the full rationale,
including the decision record and the conditions under which this should not be built at
all.

Production deployment is gated on one upstream change. Traefik has no `denyOnError` for its rate-limit
middleware, so any store error becomes a 500 — [PR #13529] adds it. Until that lands,
adopting this, or any external store, means accepting fail-closed on the request path.

[#13704]: https://github.com/traefik/traefik/issues/13704
[#13706]: https://github.com/traefik/traefik/issues/13706
[PR #13529]: https://github.com/traefik/traefik/pull/13529
