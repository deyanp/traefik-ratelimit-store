# traefik-ratelimit-store

A rate-limit counter store that speaks just enough of the Redis wire protocol to back
Traefik's `rateLimit` middleware, so limits can be shared across proxy replicas without
running Redis or Valkey.

## Why it exists

Traefik's in-memory rate limiter keeps one bucket map per *router*, not per middleware,
and reclaims expired entries only once a map is full. On a large routing table that means
memory that grows all day and effective limits many times looser than configured.

Those are being fixed upstream. What upstream does not fix is that the in-memory limiter
is per-pod: run Traefik as a DaemonSet and the effective limit is multiplied by the node
count. Traefik's only answer to cross-pod state is "configure Redis", and this is that
Redis-shaped thing without the Redis.

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

## Configuration

Everything has a working default; nothing is required.

| Variable | Default | Meaning |
|---|---|---|
| `LISTEN_ADDRESS` | `0.0.0.0:6379` | Where the protocol listener binds |
| `PEER_LISTEN_ADDRESS` | `0.0.0.0:8080` | Where the peer endpoint binds |
| `PEER_ENDPOINT` | *(empty)* | DNS name resolving to all peers, or a comma-separated list. Empty means this replica counts alone |
| `PEER_PUBLISH_INTERVAL_MS` | `150` | How often consumption is published to peers |
| `PEER_STALENESS_LIMIT_MS` | `1000` | How long a peer's report stays usable |
| `STORE_SHARD_COUNT` | `16` | Independently locked shards |
| `STORE_CAPACITY_PER_SHARD` | `65536` | Entry ceiling per shard — a backstop, not the mechanism |
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

## Cost

Measured on a laptop with `cargo run --release --example latency`, against a store on
loopback, driving the same wire traffic the proxy sends.

| | 1 connection | 16 connections |
|---|---|---|
| p50 | 34us | 226us |
| p99 | 129us | 328us |
| p99.9 | 157us | 398us |
| worst | 177us | 572us |
| throughput | 24k/s | 68k/s |

The single-connection column is what the store costs a request. The 16-connection column
is dominated by queueing on a machine with far fewer cores than connections, not by the
store — throughput rises while latency does, which is what saturation looks like.

Both are far under the 200ms read timeout the proxy should be configured with — the p99
has roughly six hundred times' headroom. That margin is the point of measuring, since the
proxy has no fail-open switch: a store slow enough to hit the timeout produces a 500, not
a slow request.

The measurement reads whole replies rather than stopping at the first `read`. That is not
pedantry: an earlier version stopped early, so leftover bytes were attributed to the
following request, and the distribution was wrong in both directions at once.

## Memory

Entries are reclaimed on a timer rather than when a map fills, and the sweep covers every
shard whether or not it saw traffic — so an idle store shrinks instead of merely not
growing. With Traefik's computed TTL of one to two seconds, the resident set is the sources
active in the last couple of seconds: proportional to concurrent traffic, not to uptime.

Rate-limit keys are hashed on arrival, so a caller's raw source — which may be a
credential — never exists at rest.

## Tests

```sh
cargo test          # unit tests
cargo clippy --all-targets
cargo fmt --check
```

Three layers cover three different things.

**`cargo test` — arithmetic and behaviour.** The unit tests include a differential suite
that executes the reference Lua in a test-only interpreter and diffs it against the native
implementation over a generated corpus. The cucumber scenarios cover mesh accuracy across
replicas. Neither needs any infrastructure.

**`conformance/end-to-end.sh` — the whole chain.** Stands up real Traefik with a
`rateLimit` middleware pointed at this store, plus a backend, and checks that a burst is
admitted and the rest are refused. This is the only test that exercises Traefik's
middleware, its Redis client, this store's protocol handling and the arithmetic together.

```
admitted (200): 5
refused  (429): 15
PASSED: real Traefik enforced a shared limit through this store
```

**`conformance/k3d/run.sh` — the production shape.** Stands up a throwaway k3d cluster and
proves three store replicas enforce one shared limit behind real Traefik. This is the only
test that covers the Kubernetes CRD provider, which builds the middleware from a `Middleware`
resource rather than from a configuration file, and peer discovery through a headless
Service rather than a hardcoded list. It applies the manifests in `deploy/` as written.

Three replicas is what makes the result meaningful: without shared counters a burst of five
would admit up to fifteen.

```
Store replicas:   3
Peer addresses:   10.42.1.3 10.42.2.2 10.42.0.3
admitted (200): 6
refused  (429): 14
PASSED: 3 replicas enforced one shared limit behind real Traefik
```

The cluster is deleted afterwards and the kubectl context is never switched, so an existing
session is left alone. Pass `--keepCluster true` to inspect it.

**`conformance/probe.go.txt` — the client's own view.** Drives a running store with
go-redis configured exactly as the rate limiter configures it. Copy it into a Traefik
checkout as `conformance-probe/main.go`, start the store on `127.0.0.1:16379`, and `go run
./conformance-probe`. Useful when a protocol question needs answering without standing up
the whole chain.

> A note for anyone on Docker Desktop: the end-to-end script keeps its generated
> configuration inside the repository rather than in a temporary directory, because Docker
> shares only a configured set of host paths and `/tmp` is usually not among them. A bind
> mount from an unshared path silently produces an empty directory instead of the file, and
> Traefik then serves 404 with nothing in its log to explain why. The script checks the
> configuration actually reached the container and says so if it did not.

## Status

Designed and being built; not deployed. `DESIGN.md` carries the full rationale, including
the peer model and the conditions under which this should not be built at all.

One upstream dependency is worth knowing about: Traefik has no `denyOnError` for its
rate-limit middleware, so any store error becomes a 500. [PR #13529] adds it. Until that
lands, adopting this — or any external store — means accepting fail-closed on the request
path.

[PR #13529]: https://github.com/traefik/traefik/pull/13529
