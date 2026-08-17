# Rate-limit shim — design and decisions

A Rust pod that speaks just enough of the Redis wire protocol to back Traefik's
`rateLimit` middleware, giving cluster-wide rate-limit counters without running Redis or
Valkey.

Verified against **traefik/traefik v3.7.1** (and `v3.7` at `f762508`, `master` at
`b51bd71`) and **redis/go-redis v9.7.0**. Every code reference below was read, not
assumed.

**Status: designed, not approved to build.** See [When not to build this](#when-not-to-build-this).

---

## 1. Why this exists

Traefik's in-memory rate limiter keeps one bucket map per *router*, not per middleware,
and never reclaims expired entries until the map is full. On a large routing table that
produces unbounded memory growth and effective limits many times looser than configured.

Two upstream patches address that (§12). Neither closes the remaining gap: **the
in-memory limiter is per-pod**, and Traefik runs as a DaemonSet, so the effective limit
is multiplied by node count. Traefik OSS's only answer to cross-pod state is "configure
Redis".

Dividing the configured limit by node count does **not** work as a substitute. The
LoadBalancer hashes the 5-tuple and HTTP keep-alive pins a client's connection to one
node, so a single client is served by a single Traefik pod. Divide by N and a normal
client is throttled at 1/N of the intended rate, while a source spread across nodes still
gets N×. No divisor is correct for both.

---

## 2. Decisions

| # | Decision | Status |
|---|---|---|
| 1 | RESP2 over TCP, six commands, no RESP3 | Settled |
| 2 | Native Rust token bucket, no Lua interpreter | Settled |
| 3 | Self-validating script registry via `NOSCRIPT` → `EVAL` | Settled |
| 4 | Sharded mutex maps, bounded sweep expiry | Settled |
| 5 | Hash the key on receipt | Settled |
| 6 | Count sharing over a full mesh, not demand-share allocation | Settled |
| 7 | N replicas, no hash ring, no forwarding hop | Settled |
| 8 | Single endpoint in Traefik config (never a list) | Settled |
| 9 | Timeouts pinned at 200 ms; pools pinned small | Settled |
| 10 | Repo `deyanp/traefik-ratelimit-store` | Settled |
| 11 | Whether to build at all | Blocked on §12 |

---

## 3. Transport and protocol

The protocol is dictated by Traefik, which talks through `go-redis`. It is **RESP2 over a
plain TCP socket** — not HTTP, not gRPC. The shim's job is to be indistinguishable from
Redis for the commands below.

### 3.1 Commands

| Command | When | Reply |
|---|---|---|
| `HELLO 3` | Once per connection | `-ERR unknown command` |
| `CLIENT SETINFO` | Twice per connection, pipelined | Anything — result is discarded |
| `AUTH` | Only if a password is configured | `+OK` |
| `SELECT` | Only if `db` > 0 | `+OK` — or just set `db: 0` |
| `EVALSHA` | Every rate-limited request | 3-element array, or `-NOSCRIPT` |
| `EVAL` | Only after a `NOSCRIPT` reply | 3-element array |
| `PING` | Never sent by go-redis | `+PONG` — add it for probes |

Everything else: `-ERR unknown command`.

`SCRIPT LOAD`, `SCRIPT EXISTS` and `Del` appear in Traefik's `Rediser` interface but are
never called on this path — `Script.Run` goes straight to `EVALSHA`. Do not implement
them.

### 3.2 RESP3 is avoidable

go-redis defaults to protocol 3 and opens with `HELLO 3`. But `initConn` (`redis.go:300-308`)
only aborts when the error is *not* a Redis error — a plain `-ERR …` is read as "this
server predates HELLO" and the connection continues on RESP2. Answering HELLO with an
error is the supported path, not a workaround, and it removes RESP3 from the build.

`CLIENT SETINFO` results are discarded with `_, _ = p.Exec(ctx)` (`redis.go:355-357`), so
erroring on them is safe.

### 3.3 One endpoint, never a list

`NewUniversalClient` returns a `ClusterClient` whenever more than one address is
configured (`universal.go:246`), and that client immediately demands `CLUSTER SLOTS`.
Traefik must be given exactly one address — a ClusterIP Service in front of the replicas.

---

## 4. Script handling

`Script.Run` is optimistic: `EVALSHA` first, and only on a reply whose error starts with
`NOSCRIPT` does it retry with `EVAL` carrying the full source. That yields a
self-validating registry for free:

1. First `EVALSHA` after startup arrives with an unknown SHA → reply `-NOSCRIPT No matching script.`
2. go-redis retries with `EVAL` and the complete ~1.2 KB source.
3. Compare it against the known texts from `pkg/middlewares/ratelimiter/lua.go`.
   - Match → register the SHA, serve every subsequent `EVALSHA` natively.
   - Mismatch → Traefik changed the algorithm in an upgrade. Serve anyway, log and alarm.

### 4.1 Pin a set, not a single text

Between v3.7.1 and later v3.7 the script gained one space before the final
`tostring(tokens)` — semantically nothing, entirely different digest. The accessor changed
too (`AllowTokenBucketScript` became `LoadTokenBucketScript`, a `sync.OnceValues` so Hub
can override it), which is a reminder that this file is actively edited.

So a single pinned copy is wrong on arrival: two patch releases of one minor version
disagree, and a fleet part-way through an upgrade sends both digests. `KNOWN_SCRIPTS` is a
set, one entry per upstream revision, each named.

Comparison stays **exact-match**. Normalising whitespace before comparing would absorb the
harmless differences seen so far, and would equally absorb a real change to the arithmetic
— which is the only thing the comparison exists to catch. Adding a revision to the set is a
deliberate act that records that somebody read the diff.

A divergence is **served, not refused**. Refusing would convert a routine proxy upgrade
into an outage, which is a far worse failure than briefly running semantics that may have
drifted.

This turns silent semantic drift into a detectable event, which is the reason to
implement the bucket natively rather than embed a Lua interpreter.

**The shim never executes Lua.** It handles an opaque SHA on the wire plus one string
comparison at startup.

---

## 5. Bucket semantics

Arguments, in order, from `evaluateScript`:

| ARGV | Meaning | Unit |
|---|---|---|
| 1 — `limit` | `rate / 1_000_000` | tokens per microsecond |
| 2 — `burst` | bucket capacity | tokens |
| 3 — `ttl` | key lifetime | whole seconds |
| 4 — `t` | `time.Now().UnixMicro()` | microseconds |
| 5 — `max_delay` | longest acceptable wait | microseconds |

Every value is `f64` — Lua 5.1 numbers are doubles and matching them is the point.

```rust
let (mut last, stored_tokens) = store.get(key).unwrap_or((0.0, 0.0));
if t < last { last = t; }                    // clock-went-backwards guard

let elapsed = t - last;
let mut tokens = (stored_tokens + limit * elapsed).min(burst) - 1.0;

let mut wait = 0.0;
if tokens < 0.0 {
    wait = -tokens / limit;                  // microseconds
    if wait > max_delay {
        tokens = (tokens + 1.0).min(burst);  // refund; caller will be rejected
    }
}

store.set(key, (t, tokens), ttl_seconds);
reply(["true", &fmt(wait), &fmt(tokens)])
```

### 5.1 Details that bite

- **Tokens go negative and stay stored.** When the wait is acceptable the debt is carried
  in the stored value. Do not clamp at zero.
- **Element 0 is always the literal `"true"`.** The script has no false branch, which
  makes the `!ok` path in Traefik's `Allow` dead code on this route. Return it anyway.
- **Reply elements must be bulk strings.** Traefik does `values[0].(string)` then
  `ParseBool` / `ParseFloat`; integer replies panic the type assertion.
- **The script reads `HGETALL` positionally** — `rl_source[2]` is `last`, `rl_source[4]`
  is `tokens`. Store a struct and never expose a generic `HGETALL`, and the hazard
  disappears.
- **Number formatting.** Lua's `tostring` emits `%.14g`; Rust's `Display` for `f64` emits
  shortest-round-trip. Go's `ParseFloat` accepts both. Just never emit `inf` or `NaN`.

### 5.2 The clock comes from the client

Traefik stamps `t` per request, so the shim needs no clock synchronisation for the bucket
maths — only for expiry. The flip side is that skew between nodes feeds directly into the
bucket, absorbed only by the backwards-clock guard.

---

## 6. Store

### 6.1 Atomicity

Redis executes scripts atomically because it is single-threaded. The read-modify-write
per key must be atomic here too. Shard the keyspace across 16–32 mutex-guarded maps and
hash the key to a shard; a lock held for a handful of float operations will not contend
meaningfully.

### 6.2 Expiry

Store an expiry instant per entry, sweep on a timer roughly every second, and keep a hard
capacity with LRU eviction as a backstop that should never fire. With Traefik's computed
TTL of 1–2 seconds the resident set is *sources active in the last two seconds* —
proportional to concurrent traffic, not to uptime. Single-digit megabytes at any
plausible volume.

This is precisely what Traefik's own `ttlmap` gets wrong, and doing it right is half the
reason the shim exists.

### 6.3 Scoping

The key is `rate:<qualifiedMiddlewareName>:<source>` — `mw.go:301` passes the qualified
name into `ratelimiter.New`, and the router does not appear.

| Situation | Counters | Why |
|---|---|---|
| Two different Middleware CRDs | Separate | Different names, regardless of config |
| Same name, two namespaces | Separate | Qualified names differ |
| One CRD, 102 routers | Shared | One name — this is the fix |

The configuration is **not** part of the key. `limit`, `burst`, `ttl` and `max_delay`
arrive as per-request arguments; only `(last, tokens)` is stored. A config edit therefore
takes effect on the next request and the carried token count self-corrects within one
TTL.

### 6.4 Hash the key on receipt

Traefik sends the key already formed. Hashing it before storing means the raw source
never exists at rest — which matters if the source criterion is ever a bearer token
again, and costs nothing.

---

## 7. Distributed model

### 7.1 Count sharing, not demand-share allocation

Two families exist in the prior art:

- **Share the budget** — [Doorman](https://github.com/youtube/doorman) (Google/YouTube).
  Nodes report *demand*; an algorithm assigns allocations (`PROPORTIONAL_SHARE`, O(n);
  `FAIR_SHARE`, O(n²)); enforcement is then purely local. Converges after one refresh
  cycle.
- **Share the count** — [caddy-ratelimit](https://github.com/mholt/caddy-ratelimit).
  Each instance publishes its own consumption and reads its peers'. Per request, sum
  peers' counts plus your own and compare against the limit. No allocation, no
  convergence.

**We take count sharing**, because it is correct in both traffic regimes while
demand-share is correct in only one.

A key's traffic can be spread across replicas or concentrated on one. Both occur here.
Spread is in fact the normal case: each Traefik pod holds ~251 connection pools all
pointing at the shim's ClusterIP Service, and kube-proxy balances per connection, so
requests for one key from one Traefik pod already reach different replicas. Client
hopping between Traefik pods — keep-alive connections closing, parallel connections,
pods rolling — adds to that rather than causing it. Concentration still happens whenever
a single client sustains one connection.

| Regime | Count sharing | Demand-share allocation |
|---|---|---|
| Spread | Correct, error bounded by one interval | Correct, converges to fair shares |
| Concentrated | Correct immediately — peers report ~0, so the node enforces the full limit | Throttles the client to 1/N until the next refresh |

Doorman is built for many clients with sustained spread demand against a shared resource,
where a refresh cycle of latency is irrelevant. Ours is bursty and short-lived per key.
Count sharing is right in both regimes and is far simpler.

### 7.2 Payload

Each interval, every replica publishes only the keys it touched:

```
replica_id, timestamp_micros,
keys: { key_hash -> tokens_consumed_in_trailing_window }
```

Per request:

```
total   = own_consumed(K) + Σ fresh_peers.consumed(K)
available = refill_since_epoch(K) − total
allow if available >= 1
```

Four decisions inside that:

- **Consumption, not bucket state.** `(last, tokens)` does not merge — max-merging loses
  concurrent increments and would allow up to N×. Consumption is additive; it does.
- **Staleness guard.** Ignore any peer whose timestamp is older than the window. Copied
  from Caddy, and it is what makes a missed or delayed peer report safe rather than
  silently wrong.
- **Full state per interval, not deltas.** Self-healing: a dropped message costs one
  interval instead of leaving permanent drift. Only active keys are included.
- **Interval 100–250 ms.** Caddy's 5 s default is uselessly coarse against `period: 10s`
  with `burst: 10`. At 3 replicas that is 6 messages per interval.

### 7.3 Transport — full-mesh broadcast, not gossip

"Gossip" is the wrong word for what this needs. Epidemic protocols contact a random
subset of peers and rely on transitive spread over O(log N) rounds, which exists because
N² is prohibitive at large N. At three replicas full mesh is simpler *and* lower latency
— one hop instead of several rounds.

**Every pod sends the same message to every other pod, every interval.** Per interval a
pod resolves the headless Service, builds one payload of its own consumption, and POSTs
that identical payload to each peer. Total messages per interval is `N × (N−1)` — six at
three replicas, a few hundred bytes each.

**Discovery is pluggable, and nothing here is Kubernetes-specific.** One environment
variable takes either a DNS name to resolve or a comma-separated static list of peers, so
the binary runs unchanged under Compose or on a bare host. Under Kubernetes that variable
holds a headless Service name — no API access, no RBAC, no watch.

**Replicas identify by `replica_id`, not by IP.** Each process takes a stable id at
startup (hostname is fine), and a report carrying your own id is ignored on arrival. That
makes self-exclusion free and removes any need for the downward API, which is the last
Kubernetes-specific dependency the design would otherwise have.

A Service with `clusterIP: None` returns A records for every ready pod:

```
traefik-ratelimit-store-peers.default.svc.cluster.local
  → 10.244.1.5
  → 10.244.2.7
  → 10.244.3.9
```

Two Services are required:

| Service | Type | Port | Client |
|---|---|---|---|
| `traefik-ratelimit-store` | ClusterIP | 6379 (RESP) | Traefik |
| `traefik-ratelimit-store-peers` | Headless | 8080 (HTTP) | the replicas themselves |

Re-resolve every interval rather than caching — CoreDNS is node-local and this keeps
membership current as replicas come and go. No self-exclusion is needed at resolve time:
a replica may POST to itself, and the inbound report is discarded because it carries the
receiver's own `replica_id`. One loopback request per interval is cheaper than the
machinery required to avoid it.

Caddy routes through shared storage instead, but only because Caddy already has a storage
abstraction. A hop through storage would add latency for nothing here.

### 7.4 No merge logic is required

Each pod broadcasts only *its own* consumption, so every peer's slot is owned exclusively
by that peer:

```
peers: HashMap<replica_id, PeerReport>       // overwrite on arrival
PeerReport = { timestamp, keys: HashMap<key_hash, consumed> }
```

The exclusivity is over the **cell** `(replica_id, key)`, not over the key. The same key
routinely appears in several reports:

```
peers[A] = { ts: …, keys: { K: 5 } }
peers[B] = { ts: …, keys: { K: 3 } }
total(K) = 8
```

What never happens is two writers touching `peers[A].keys[K]`. Last-write-wins per
replica, summation at read time on the request path. The structure is a G-Counter, but
trivially so — there is no contention and therefore no CRDT merge to implement.

Contrast with broadcasting bucket state: if A reported `tokens=5` and B `tokens=3` for the
*same* logical cell, the receiver would have to choose — max, min, average — and every
choice loses information. That is the merge problem, and per-replica consumption is what
avoids it.

A restarted pod that loses its counts simply reports its post-restart consumption. Because
the payload is *consumption in a trailing window* rather than a cumulative total, that is
correct with no monotonicity requirement — a further reason windowed beats cumulative.

### 7.5 Publish-loop rules

- **Never retry a failed POST.** Retrying stale state is worse than sending fresh state
  one interval later. Drop it; the peer ages out of the staleness window on its own.
- **Publish concurrently**, so one slow peer cannot delay the others.
- **Tight per-peer timeout**, around 50 ms, so a hung peer cannot stall the loop.
- **Fire-and-forget.** The response carries nothing; do not wait on it.

Switch to real gossip only if replica count passes roughly 15–20, where `N × (N−1)`
starts to matter. At three it does not.

### 7.6 Accuracy

Overshoot is bounded by `(N−1) × rate × interval`. Because spread is the normal case
(§7.1), this applies routinely rather than only under attack — and it scales with the
**configured rate**, so the interval must be sized against the highest limit in use:

| Configured limit | N=3, interval 150 ms | Overshoot |
|---|---|---|
| 3 rps (`average: 30 / period: 10s`) | 2 × 3 × 0.15 | 0.9 requests |
| 100 rps | 2 × 100 × 0.15 | 30 requests |

At current limits that is under one request. If a high-rate middleware is ever added, the
interval has to come down with it, or the overshoot becomes a material fraction of the
budget. Write this constraint into the config validation rather than discovering it
later.

Caddy leaves a `TODO` about extrapolating a peer's count from how stale its report is.
Worth knowing; not worth building in v1.

---

## 8. Topology

Rate-limit counters are **disposable** — losing one means a client gets a fresh budget
for one window. That removes consensus, failover and persistent volumes from the design,
which is why this is easier than a Valkey cluster rather than harder.

| Topology | Multiplier fixed | Memory bounded | Cross-node | Single pod loss |
|---|---|---|---|---|
| Today — in-process maps | No — 251 maps | No | No | N/A, cannot fail |
| DaemonSet + `internalTrafficPolicy: Local` | Yes — 1/node | Yes | No | That node 500s |
| Deployment, 1 replica | Yes | Yes | Exact | Everything 500s |
| Deployment, N replicas + hash ring | Yes | Yes | Exact | No 500s if fail-soft |
| **Deployment, N replicas + count sharing** | **Yes** | **Yes** | **Approximate** | **Invisible** |

Three replicas, anti-affinity, a PodDisruptionBudget. No StatefulSet, no PVC.

Pod loss is invisible because survivors already hold their own counts and the dead peer
simply ages out of the staleness window. Connection breaks are absorbed by go-redis,
whose `shouldRetry` returns true for `io.EOF`, `io.ErrUnexpectedEOF` and non-timeout
network errors including connection-refused.

**A hung pod is worse than a dead one.** `context.DeadlineExceeded` is explicitly *not*
retried, and timeouts retry only conditionally. Design for fast failure: a readiness
probe that genuinely fails, low timeouts, a bounded accept queue, and a `preStop` drain
so terminations close connections rather than stall them.

---

## 9. Failure semantics

**Traefik fails closed and there is no override.** Any error reaching Traefik returns
HTTP 500 for that request (`rate_limiter.go:154`). Traefik OSS has no `denyOnError` — that
knob exists only on the Hub `distributedRateLimit` middleware.

This is the single largest risk in adopting any external store, and it is being addressed
upstream (§12).

Default go-redis behaviour makes it much worse than it first appears: 5 s dial timeout,
3 s read timeout, and **3 retries** with backoff growing from 8 ms to 512 ms — and
Traefik's CRD exposes the timeouts but *not* `maxRetries`.

| Setting | Values | Time to fail one request |
|---|---|---|
| Untouched | dial 5 s, read 3 s | ~20 s of hang, then 500 |
| Pinned | dial/read/write 200 ms | ~1.8 s, then 500 |

Pinning the timeouts is a correctness requirement, not tuning.

`BuildMiddlewareChain` has no instance cache, so each of the ~251 router-level middleware
instances calls `NewUniversalClient` separately — **251 connection pools per Traefik pod,
per node**, with a default `PoolSize` of 10 × GOMAXPROCS. Pools fill lazily, so this stays
invisible until a burst.

```yaml
redis:
  endpoints: ["traefik-ratelimit-store.default.svc.cluster.local:6379"]
  db: 0
  poolSize: 2
  maxActiveConns: 5
  dialTimeout: 200ms
  readTimeout: 200ms
  writeTimeout: 200ms
```

~1,255 connections per node worst case. Check that against the shim's accept limit.

**Limit the blast radius**: keep most routes on the in-process IP-keyed limiter and put
only the routes that genuinely need cross-node budgets on the shim.

---

## 10. Observability

A dedicated store makes per-key rate-limit metrics possible for the first time — which
client hit which limit, how often, per middleware. Traefik does not expose that
granularity. Readiness probe only, matching the F# services.

---

## 11. Testing

Three layers, each proving something the others cannot.

### 11.1 Protocol conformance — against the real client

`conformance/probe.go.txt` drives a running store with go-redis, configured exactly as the
rate limiter configures it. This is the only way to prove the handshake, the
`NOSCRIPT`/`EVAL` exchange and the reply shape against the real client rather than against
a reading of its source. It proves the *protocol*; it says nothing about the arithmetic
beyond the handful of requests it makes.

### 11.2 Differential conformance — the load-bearing test

Run the real Lua and the Rust implementation over a generated corpus of
`(limit, burst, ttl, t, max_delay)` sequences and assert identical replies. Nothing else
proves semantic equivalence, and it doubles as the regression gate when the upstream script
changes.

**The oracle is `mlua` as a dev-dependency, not a Redis-compatible server.** Something has
to *execute* the Lua; a server would only be acting as an interpreter, and embedding one in
the test does the same job without a container, runs in CI anywhere, and is fast enough for
thousands of sequences instead of a handful. The script uses three verbs — `hgetall`,
`hset`, `expire` — stubbed against a map.

Two properties make this better than a server oracle rather than merely cheaper. It
executes the `PINNED_SCRIPT` constant from `script.rs`, so a bad transcription of the
script into this repo fails the test instead of lurking. And it keeps the oracle in the
same process, so a failing case is debuggable.

This does not reopen §2 decision 2. That decision is **no Lua interpreter in the shipped
binary**, which is why the arithmetic is native. A `[dev-dependencies]` oracle never
reaches production, and is precisely the tool that proves the native version is faithful.

**Use realistic timestamps.** An absent key reads as `last = 0`, so with a toy timestamp
near the epoch the refill is proportional rather than saturating at `burst` — a regime
production never sees. A corpus built around `now ≈ 1.78e15` exercises the real one.

### 11.3 Mesh accuracy — statistical, not differential

Multi-replica behaviour is approximate by design, so it needs its own test: drive known
load across N replicas and assert the total lands within `(N−1) × rate × interval`. Do not
try to make the differential test cover both — scope that one to a single replica.

Cucumber features for behaviour, per the house Rust testing conventions.

---

## 12. Upstream dependencies

| Item | What | State |
|---|---|---|
| Issue A + PR | Reclaim expired buckets as the map is used | Written, verified, not filed |
| Issue B | Per-router vs per-middleware scoping, opt-in field | Drafted, not filed |
| [PR #13529](https://github.com/traefik/traefik/pull/13529) | `denyOnError` for the Redis limiter | Open upstream, `status/2-needs-review` |

**#13529 is the one that matters most.** It converts a shim outage from "500s on every
rate-limited route" into "temporarily unlimited", which for abuse protection is
unambiguously the better failure. Without it, adopting any external store means accepting
fail-closed on the critical path.

---

## 13. Repository

**`github.com/deyanp/traefik-ratelimit-store`**

Sits outside `platform-r`, which is the port of the monorepo's business services; this is
ingress infrastructure, same reasoning that puts `forward-auth-traefik-service-r` outside
it too.

Four reasons for the name:

- **`store` is Traefik's own vocabulary** for this role — the Hub `distributedRateLimit`
  middleware configures it as `store: { redis: {…} }`. The name explains the component to
  anyone arriving from the Traefik docs.
- **Leading with `traefik`** groups it with other Traefik work and matches how anyone
  searches. The existing `forward-auth-traefik-service-r` buries the product mid-name,
  which sorts and reads worse.
- **`ratelimit` matches Traefik's middleware name** (`rateLimit`), so it is what people
  grep for.
- **Zero collisions** — `traefik-ratelimit-store` returns no hits across GitHub, against
  10 for `traefik-ratelimit` and 7 for `traefik-rate-limit`.

No `-r` suffix and **no `k8s` in the name**. The house Rust marker is dropped here because
the repo has no F# counterpart to disambiguate from. `k8s` is omitted for two reasons: it
would be true of every repo in this account and therefore distinguishes nothing, and it is
not actually accurate — see §7.3, the design takes only DNS and a replica identity from
its environment, with no Kubernetes API access, RBAC, watches or CRDs. Traefik itself runs
under Docker and the file provider, and this store should not exclude those by name.

| Thing | Name |
|---|---|
| Repo | `github.com/deyanp/traefik-ratelimit-store` |
| Local clone | `~/gitp/traefik-ratelimit-store` |
| Crate / binary | `traefik-ratelimit-store` |
| Deployment | `traefik-ratelimit-store` |
| ClusterIP Service (RESP, 6379) | `traefik-ratelimit-store` |
| Headless Service (peers, 8080) | `traefik-ratelimit-store-peers` |

Which makes the Traefik configuration self-documenting:

```yaml
redis:
  endpoints: ["traefik-ratelimit-store.default.svc.cluster.local:6379"]
```

---

## 14. Effort

| Component | Rough size |
|---|---|
| RESP2 codec + connection loop | ~200 LOC |
| Handshake quirks | ~60 LOC |
| Bucket + sharded store + sweeper | ~200 LOC |
| Script registry + self-validation | ~80 LOC |
| Gossip: publish, fetch, merge, staleness | ~250 LOC |
| Probes + metrics | ~80 LOC |
| Manifests | ~120 lines YAML |

Roughly 850 LOC and 5–7 days including the conformance harness. No `thiserror`, no
`anyhow`, hand-rolled error enums, newtypes over the key and token types.

---

## When not to build this

Two changes already in flight attack the same problem far more cheaply, and they compose:
IP-based keys cut the per-entry cost from ~1.75 KB to ~275 B, and the reload CronJob caps
accumulation per interval.

**Build it if** peak Traefik RSS still exceeds ~300 Mi after both land and the CronJob is
already at six hours — meaning the growth is real rather than an artefact of key size.

**Or build it if** Issue B is rejected upstream. Without B the divisor is not node count
but `nodes × routers-per-middleware`, a different number per middleware that changes
whenever a route is added. That arithmetic is unmaintainable and rots silently. The shim
makes you immune to that outcome.

**Otherwise don't.** It trades a bounded memory problem for an unbounded availability one,
and that trade only pays when the memory problem survives the cheap fixes.
