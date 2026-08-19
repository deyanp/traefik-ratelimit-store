# Rate-limit shim — design and decisions

A Rust pod that speaks just enough of the Redis wire protocol to back Traefik's
`rateLimit` middleware, giving cluster-wide rate-limit counters without running Redis or
Valkey.

Verified against **traefik/traefik v3.7.1** (and `v3.7` at `f762508`, `master` at
`b51bd71`) and **redis/go-redis v9.7.0**. Every code reference below was read, not
assumed.

**Status: built, tested, not deployed.** Deployment is gated on one upstream change
(§12). What the design got wrong is recorded in §15 rather than quietly corrected — the
mistakes are the part worth reading.

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
| 11 | Whether to build at all | Built |
| 12 | Readiness answers for the protocol port; liveness is separate | Settled (§8.1) |
| 13 | Drain on SIGTERM before the listener closes | Settled (§8.1) |
| 14 | Peer endpoint authenticated, and refuses to start undecided | Settled (§9.1) |
| 15 | Entry ceiling derived from the memory budget, not configured | Settled (§6.5) |

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

### 6.5 The ceiling is derived, not configured

The entry ceiling and the container's memory limit are one decision. An earlier version of
this design set them apart — a 128Mi limit beside a ceiling of a million entries — and a
million entries is well over a hundred megabytes, so the process would have been killed at
a few hundred thousand keys while its own backstop still reported headroom. A backstop
above the limit it protects is decoration.

So the store reads the container's limit from cgroup (a kernel facility, so it behaves the
same under Kubernetes, Docker and systemd), gives the shard tables half of it, and derives
the per-shard ceiling. Each table is allocated once, at the largest power of two of slots
that fits its share — 81 bytes a slot: the 16-byte key, the 56-byte entry, one control
byte — and the ceiling is the seven-eighths of that the map fills before it would grow. The
figure planned for is therefore the figure allocated, and no table ever grows under a
shard's lock while requests wait. It logs what it chose. `STORE_MEMORY_BUDGET_MB` overrides
the discovered limit (the shipped manifest feeds it from the container's own limit through
the Downward API, so there is nothing to keep in step); `STORE_CAPACITY_PER_SHARD` overrides
the result; neither is normally needed.

| memory limit | entries held | tables |
|---|---|---|
| 128Mi | 458,752 | 40.5MB |
| 256Mi | 917,504 | 81MB |
| 512Mi | 1,835,008 | 162MB |

The first cut planned for 400 bytes an entry, a figure measured on macOS, whose allocator
keeps the freed old tables of a growing map resident. Linux measures 170 with growing tables
and exactly the slot size with pre-sized ones (§15).

### 6.6 What happens at the ceiling

Distinct keys keep arriving; the ceiling holds; something has to go. The insert that finds
a shard full sweeps expired entries first, and if that frees nothing, drops the least
recently active tenth.

**The flood evicts itself.** A source seen once is the least recently active thing in the
shard, while a source still sending is the last thing to go. So the client worth limiting
stays limited, and the entries dropped belong to sources that were nowhere near their limit
— they are re-admitted against a fresh bucket, which changes nothing for them.

The cost is amortized: a trim frees a tenth of a shard, so the O(n) work cannot recur until
that tenth is refilled. It emits `StoreAtCapacity`, which is the one condition an operator
cannot infer from anything else — memory looks fine, latency looks fine, and quietly some
sources are being admitted against fresh buckets. Alarm on it.

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
demand-share is correct in only one — with one correction to Caddy's shape, recorded in
§7.2 and §15: the count a peer reports is *debited from the local bucket*, not compared
against the limit at decision time. Caddy's per-request comparison works because Caddy's
limiter *is* a sliding-window count; a token bucket that refills independently on every
replica needs the peers' admissions folded into its level, or each replica refills the
whole rate for itself.

A key's traffic can be spread across replicas or concentrated on one. Both occur here.
Spread is in fact the normal case: each Traefik pod holds ~251 connection pools all
pointing at the shim's ClusterIP Service, and kube-proxy balances per connection, so
requests for one key from one Traefik pod already reach different replicas. Client
hopping between Traefik pods — keep-alive connections closing, parallel connections,
pods rolling — adds to that rather than causing it. Concentration still happens whenever
a single client sustains one connection.

| Regime | Admission sharing | Demand-share allocation |
|---|---|---|
| Spread | Correct, error bounded by one interval | Correct, converges to fair shares |
| Concentrated | Correct immediately — peers report nothing, so the node enforces the full limit | Throttles the client to 1/N until the next refresh |
| Moving between replicas | Correct — every replica's level already reflects the others' admissions | Re-converges after a refresh cycle |

Doorman is built for many clients with sustained spread demand against a shared resource,
where a refresh cycle of latency is irrelevant. Ours is bursty and short-lived per key.
Count sharing is right in both regimes and is far simpler.

### 7.2 Payload

Each interval, every replica publishes what it admitted since its previous report, for the
keys it touched. On the wire that is one version byte, the replica id length-prefixed, and
a fixed 48-byte record per key, little-endian:

```
[version u8] [id_len u8] [replica_id]
per key: key u128 | admitted u32 | last f64 | limit f64 | burst f64 | ttl_ms u32
```

On receipt, per line, the receiver folds the admissions into its own bucket as of `last`:

```
level = min(stored + limit × (last − stored.last), burst) − admitted
```

— refill up to the moment the peer admitted them, then subtract, exactly as a local
request would have done. A key the receiver has not seen is taken as full at `last`. The
`limit`/`burst` travel with the line so a key only peers have touched can be folded at all;
a key the receiver has seen is folded with the configuration it saw itself. Per request,
nothing about peers is consulted: the bucket already reflects them.

Four decisions inside that:

- **Admissions, not bucket state.** `(last, tokens)` does not merge — max-merging loses
  concurrent increments and would allow up to N×. Admissions are tokens taken from one
  logical bucket; debiting them everywhere keeps every copy tracking that bucket.
- **Debit the level, do not offset the decision.** The first version subtracted peers'
  recent consumption from each decision and left the stored level alone. That shares the
  burst and nothing else: every replica still refills at the full rate, and under sustained
  spread traffic the deployment admits N × rate while the burst tests pass (§15). Folding
  the admissions into the level is what makes the rate shared.
- **Deltas since the last report, applied once.** There is no peer table, no staleness
  window and no reconciliation: a report is folded on arrival and forgotten. A lost report
  costs its peers one interval of one replica's admissions — the same one-interval error
  the cadence already allows — and a silent peer costs nothing but its own future
  admissions.
- **Interval 100–250 ms.** Caddy's 5 s default is uselessly coarse against `period: 10s`
  with `burst: 10`. At 3 replicas that is 6 messages per interval.

### 7.3 Transport — full-mesh broadcast, not gossip

"Gossip" is the wrong word for what this needs. Epidemic protocols contact a random
subset of peers and rely on transitive spread over O(log N) rounds, which exists because
N² is prohibitive at large N. At three replicas full mesh is simpler *and* lower latency
— one hop instead of several rounds.

**Every pod sends the same message to every other pod, every interval.** Per interval a
pod resolves the headless Service, builds one payload of its own admissions, and POSTs
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

Each pod broadcasts only *its own* admissions, and the receiver folds them straight into
its bucket. There is no per-peer table at all: the sum that Caddy computes at read time
across `peers[A].keys[K]`, `peers[B].keys[K]` happens here once, at receipt, as a debit —
and because the debit is applied to the level rather than kept beside it, the same key
appearing in several reports needs no reconciliation. The structure is still a G-Counter,
only materialised into the bucket instead of stored next to it.

Contrast with broadcasting bucket state: if A reported `tokens=5` and B `tokens=3` for the
*same* logical cell, the receiver would have to choose — max, min, average — and every
choice loses information. That is the merge problem, and per-replica admissions are what
avoid it.

A restarted pod reports only what it admits after restarting, which is all a peer needs.
Nothing is cumulative, so there is no monotonicity requirement and no epoch to carry.

### 7.5 Publish-loop rules

- **Never retry a failed POST.** A retry would deliver an old report after a fresher one
  is already due. Drop it; the peer is behind by one interval of this replica's admissions
  and no more.
- **Publish concurrently**, so one slow peer cannot delay the others.
- **Tight per-peer timeout**, around 50 ms, so a hung peer cannot stall the loop.
- **Fire-and-forget, but read the status.** The response carries no data, but a `401` or
  `413` means the peer is discarding every report — a secret that differs between replicas,
  a report cap raised past the body limit — and a mesh that silently counts alone is the
  failure this store exists to prevent. Any non-success is a rejection and is logged when
  it starts and when it clears; "no peer answered" likewise.
- **Resolve asynchronously, once per interval.** The resolver must never hold a runtime
  worker, because the same workers serve the protocol connections; a slow CoreDNS would
  otherwise stall requests into the proxy's 200 ms read timeout.

Switch to real gossip only if replica count passes roughly 15–20, where `N × (N−1)`
starts to matter. At three it does not.

### 7.6 Accuracy

Two regimes, two bounds.

**Sustained traffic admits the configured rate, however it is spread.** Every replica's
level is debited by every admission in the deployment and refilled once by time, so the
deployment behaves as one bucket up to the delay of one exchange. The cucumber suite drives
20 requests per second for 60 seconds at `30 per 10s, burst 10` across three replicas and
admits 190–192 — burst plus rate × time is 190 — spread evenly, concentrated on one
replica, or moving between them.

**A burst can be admitted once per replica before the first reports land.** Requests that
arrive within one interval of each other are decided before any peer hears of them, so the
instantaneous overshoot is bounded by `(N−1) × burst` per key, and at steady state by
`(N−1) × rate × interval`:

| Configured limit | N=3, interval 150 ms | Steady-state overshoot |
|---|---|---|
| 3 rps (`average: 30 / period: 10s`) | 2 × 3 × 0.15 | 0.9 requests |
| 100 rps | 2 × 100 × 0.15 | 30 requests |

The store cannot validate this at startup — it does not know the middlewares' rates, which
arrive per request — so it is a sizing rule for the operator: if a high-rate middleware is
added, the interval comes down with it.

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

Pod loss is invisible because every survivor's buckets already reflect what the dead peer
admitted, and nothing further is expected of it. Connection breaks are absorbed by go-redis,
whose `shouldRetry` returns true for `io.EOF`, `io.ErrUnexpectedEOF` and non-timeout
network errors including connection-refused.

**A hung pod is worse than a dead one.** `context.DeadlineExceeded` is explicitly *not*
retried, and timeouts retry only conditionally. Design for fast failure: a readiness
probe that genuinely fails, low timeouts, a bounded accept queue, and a drain on `SIGTERM`
so terminations close connections rather than stall them.

---

### 8.1 Health, and draining

Two probes, answering different questions, and they must not share an endpoint. Liveness
asks whether to restart the process; readiness asks whether to send it traffic. A draining
replica is emphatically alive, and a liveness probe that fails during a drain kills the pod
it was meant to let finish.

`/readiness` answers **for the protocol port**, by connecting to it and expecting a `PONG`.
A probe that only proves it can answer its own probe proves nothing: a replica whose RESP
listener has stopped while its HTTP endpoint still answers must leave the rotation.

It deliberately does not touch the store. A probe that acquires production locks can cause
the stall it is looking for, and a store wedged badly enough to matter fails this check
anyway by never completing it.

On `SIGTERM` the process fails readiness first, keeps serving for a drain period
(`DRAIN_PERIOD_MS`, five seconds by default), then exits — so the orchestrator withdraws
the replica before its listener closes. Without that ordering the grace period buys
nothing: connections keep arriving until the socket disappears underneath them. Verified by
replacing every replica under load without dropping a request (§11.5).

Two mechanisms withdraw a replica, and it is worth being precise about which does what.
When a pod is *deleted* — a rollout, a scale-down, a node drain — the orchestrator removes
it from the Service's endpoints the moment it enters `Terminating`, regardless of any
probe; the drain then only has to outlast the propagation to every node's proxy rules,
which is what the five seconds are for. The failing readiness probe is the net for the
other cases — a process told to stop by something other than a deletion — and it needs
`period × threshold` to be noticed, so the shipped probe runs every two seconds with a
threshold of two, inside the drain. The termination grace period must in turn exceed the
drain, or the kill arrives first.

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

### 9.1 The peer endpoint is a bypass if left open

This was documented backwards at first, and the correction matters. A report is folded into
the receiver's buckets exactly like a peer's, so a stranger who reaches the endpoint can
claim admissions for a key and throttle it, or claim a timestamp far in the future and
refill a drained bucket — the level refills up to the claimed moment before the debit. Do
that to each replica and the limit is whatever the stranger says it is.

A NetworkPolicy is the other half rather than a substitute. The shipped manifest carries
one, and it matters more for the protocol port: `AUTH` is accepted unconditionally, so
anything that can reach 6379 can drive any bucket directly. An earlier revision of this
section claimed k3s does not enforce NetworkPolicy because it ships flannel; that confused
the CNI with the policy controller — k3s enforces through its embedded controller, proven
live when a load-test pod without the permitted label was refused before the store saw a
packet. Enforcement is still the cluster's property, not this manifest's: confirm it where
it matters.

**Neither default is safe, so there is no default.** Requiring a secret that has not been
configured would reject every report, leave every replica counting alone, and reach the
same over-admission by another route. A replica with `PEER_ENDPOINT` set and no
`PEER_SHARED_SECRET` therefore refuses to start, naming both ways out;
`PEER_ALLOW_UNAUTHENTICATED=true` records the decision in configuration rather than letting
it happen by omission.

The receiver checks the secret before it parses a body, caps what one report may carry at
the same `PEER_MAX_KEYS_PER_REPORT` the sender applies, and skips any line it cannot fold
safely — a non-finite number, a non-positive limit, a malformed key.

What the secret does not do is tell replicas apart: it proves membership of the mesh, not
identity within it. Any holder of the secret — or one compromised replica — can send a
report in any shape, and the receiver cannot distinguish it from an honest peer's. Signed
per-replica reports would close that; nothing in the current threat model, where the
replicas are one deployment sharing one Secret, asks for it. Recorded so the boundary is
known, not because it is planned.

---

## 10. Observability

A dedicated store makes per-key rate-limit metrics possible for the first time — which
client hit which limit, how often, per middleware. Traefik does not expose that
granularity.

**None are implemented.** The store emits structured log events and nothing else, and that
is the largest remaining gap before production. These are worth alarming on, because none
can be inferred from anything else:

| Event | Means |
|---|---|
| `ScriptDiverged` | The proxy changed its algorithm in an upgrade |
| `StoreAtCapacity` | Keys are being shed; some sources get fresh buckets |
| `PeerEndpointUnauthenticated` | The mesh is writable by anything the network allows |
| `PeerPublishRejected` | Peers refuse this replica's reports — the mesh is silently counting alone |
| `PeerPublishFailed` / `PeerDiscoveryEmpty` | No peer can be reached, or the endpoint resolves to nothing |
| `BackgroundTaskStopped` | The sweeper, publisher or peer endpoint died; the process is exiting |
| `AcceptFailed` | Connections cannot be accepted, usually the descriptor limit |
| `ConnectionsAtCapacity` | The connection ceiling was reached; new connections are refused |

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
load across N replicas and assert the total. Two shapes, because they fail differently: a
burst at one instant, where the bound is the exchange interval, and sustained traffic over
a minute — spread, concentrated, and moving between replicas — where the total must be
burst plus rate × time. The first version passed the burst scenarios and admitted three
times the rate under the sustained ones (§15). Do not try to make the differential test
cover both — scope that one to a single replica.

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
| Mesh: publish, receive, fold | ~250 LOC |
| Probes + metrics | ~80 LOC |
| Manifests | ~120 lines YAML |

Roughly 850 LOC and 5–7 days including the conformance harness. No `thiserror`, no
`anyhow`, hand-rolled error enums, newtypes over the key and token types.

**What it actually took: roughly 3,000 lines including tests.** The estimate counted the
happy path and missed that most of the work is the parts that only matter when something is
wrong — health and draining, the capacity backstop, the memory budget, peer authentication,
and seven layers of test harness. The protocol and the arithmetic, which the estimate was
mostly about, came in close to the guess.

---

## 15. What building it changed

The design above is mostly what got built. These are the places it was wrong, kept because
the corrections are more useful than a clean document.

**An absent key yields a *full* bucket, not an empty one.** The missing state reads as
`last = 0`, so the elapsed time is the whole timestamp and the refill saturates at burst —
a lost counter admits its request rather than rejecting it. The first test written for this
asserted the opposite and passed only because it used a toy timestamp near the epoch, where
the refill is proportional instead of saturating.

**One pinned script text was wrong on arrival.** Two patch releases of the same minor
version disagree on it (§4.1), found by diffing the branch against the tag while building
the conformance probe.

**The reference is less precise than this store.** It keeps the bucket as strings, and Lua
stringifies with fourteen significant digits, so the stored timestamp is truncated to about
180µs of granularity. The two therefore cannot agree exactly, and the differential test
asserts they stay within a thousandth of the configured burst rather than to the bit.

**Sweeping only ran when a shard was touched.** An idle or traffic-skewed store would have
held expired entries indefinitely — Traefik's own bug in milder form. Reclamation is now
timer-driven across every shard.

**Ring rotation was a side effect of publishing**, so a replica running alone never rotated
its consumption counters. Nothing read them, which is the only reason nothing broke.

**Count sharing shared the burst and nothing else.** The first version subtracted peers'
recent consumption from each *decision* and left each replica's stored level alone — Caddy's
shape, which is right for Caddy because Caddy's limiter is a sliding-window count. A token
bucket that refills independently on every replica is not: once each level sat at the
raised threshold, every refilled token was spent locally, and under sustained spread
traffic three replicas admitted 552 requests in a minute against an ideal of 190 — N × rate,
with every burst scenario green. Found in review, by simulation, not by a test; the test
suite only froze time. Admissions are now *debited from the level* at the moment they
happened, which makes every replica's copy track the one shared bucket; the sustained
scenarios are in the suite, and the windowed ring, the peer table and the staleness window
all went with the old model.

**The capacity ceiling sat above the memory limit** (§6.5), which made the backstop
decoration: the process would have been killed before it ever trimmed.

**The peer endpoint was documented as a throttling risk** when it is a bypass (§9.1).

**The memory figure was measured on the wrong operating system.** 400 bytes an entry came
from macOS, whose allocator keeps a growing map's freed old tables resident; Linux, where
the container runs, measures 170 with growing tables and exactly the 81-byte slot with
pre-sized ones. The first ceiling was 2.7× too cautious — a safe mistake, but the tables
are now allocated at their final size so the planned figure and the allocated one are the
same number, and the README says which platform to measure on.

**Four things could kill a background task silently, and nothing watched.** A zero
interval panicked the timer, a zero ceiling or report cap panicked the first insert or
publish, a TTL large enough overflowed the clock, and an accept error — the descriptor limit
— exited the process. Each is now refused at startup or handled where it happens, and the
sweeper, publisher and peer endpoint are supervised: the first to stop ends the process.

Two of those were found by a question rather than by a test — whether peer authentication
was necessary, and whether hashing was worth it given the key size. Both questions were
built on something this document had asserted wrongly.

---

## When to deploy this

It is built. Whether to deploy it is a different question with a harder answer.

Two cheaper changes attack the same problem and compose: IP-based keys cut the per-entry
cost, and a reload CronJob caps accumulation per interval. Between them, reaching the
ceiling on any single map needs that many distinct client addresses through one router
inside one reload interval. **Measure before deploying.**

**Deploy if** peak proxy memory still exceeds its budget after both land, or if a route
genuinely needs one budget across all nodes — which, because keep-alive pins a client to
one pod, is more often than the arithmetic suggests.

**Only after re-tuning the limits.** Effective limits today are
`configured x nodes x routers-per-middleware`; pointing a middleware at this store makes
them `configured` exactly, a tightening of up to 102x. That is the breaking change of
[#13706] arriving by choice rather than by upgrade, and production has never once run under
the configured values, so nobody knows the real demand.

**Gradually.** One low-traffic middleware first. The blast radius of anything still wrong
is then one route rather than all of them, and it is the only honest way to learn the
demand figure the point above depends on.

[#13706]: https://github.com/traefik/traefik/issues/13706
