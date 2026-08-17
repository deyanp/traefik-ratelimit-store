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

`conformance/probe.go.txt` drives a running store with the real go-redis client, configured
exactly as Traefik's rate limiter configures it. Copy it into a Traefik checkout as
`conformance-probe/main.go`, start the store on `127.0.0.1:16379`, and `go run
./conformance-probe`. It proves the handshake, the `NOSCRIPT`/`EVAL` exchange and the reply
shape against the real client rather than against an assumption about it.

## Status

Designed and being built; not deployed. `DESIGN.md` carries the full rationale, including
the peer model and the conditions under which this should not be built at all.

One upstream dependency is worth knowing about: Traefik has no `denyOnError` for its
rate-limit middleware, so any store error becomes a 500. [PR #13529] adds it. Until that
lands, adopting this — or any external store — means accepting fail-closed on the request
path.

[PR #13529]: https://github.com/traefik/traefik/pull/13529
