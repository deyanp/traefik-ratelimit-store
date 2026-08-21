# Manual checks against a running deployment

The scripts beside this file build their own throwaway environment and tear it down again.
These checks are the opposite: they run against a store already deployed in a cluster you
keep, and answer the question that a passing CI run does not — whether *this* deployment,
with *this* `Middleware`, is enforcing the limit you configured.

Everything here is curl through the proxy. The store publishes no counter introspection
endpoint, deliberately: the counters are observable only in what the proxy admits and
refuses, and that is also the only thing that matters. Each check reads a bucket by
spending it.

## What you need

- The store deployed (`deploy/traefik-ratelimit-store.yaml`), and a `Middleware` whose
  `rateLimit` carries a `redis` block pointing at the store's ClusterIP Service.
- Two routes that both reference that middleware, reachable from wherever you run curl.
- The middleware's `average`, `burst` and `period`.

Fill in your own values:

```sh
BASE=https://api.example.test:8443    # the proxy entrypoint
ROUTE_A=$BASE/service-a/some/path     # a path on a route carrying the middleware
ROUTE_B=$BASE/service-b/some/path     # a path on a DIFFERENT route carrying the SAME middleware
AVERAGE=100                           # from the Middleware
BURST=200
```

Paths that answer 404 or 405 are fine, and are usually the better choice. The rate-limit
middleware runs before the backend, so any status other than 429 means *admitted*, and a
path the backend rejects immediately costs it nothing while you spend thousands of
requests against it.

## The helper

```sh
burst() { for i in $(seq $1); do printf 'url = "%s"\noutput = "/dev/null"\n' "$2"; done \
          | curl -sk -w '%{http_code}\n' -K - | sort | uniq -c; }
```

`burst <n> <url>` sends n requests as fast as one keep-alive connection carries them, and
tallies the statuses.

Three details in it matter:

- **`-K -` feeds curl a config on stdin** rather than repeating the URL as arguments. With
  several URLs on the command line `-o /dev/null` applies to the first one only, and the
  remaining response bodies land in your tally.
- **One connection, kept open.** Reconnecting per request adds a TLS handshake — tens of
  milliseconds each, during which the bucket refills faster than the loop can spend it.
  A parallel loop of separate curl processes is *slower* at draining a bucket than one
  sequential connection, which is the opposite of what it looks like.
- **`-k`** because a dev cluster usually serves a self-signed certificate. Drop it if yours
  does not.

Leave a few seconds between checks. Every check spends the same bucket, so one that starts
against a drained bucket measures the check before it.

## 1. The bucket has a ceiling

```sh
burst $(( BURST * 2 )) $ROUTE_A
```

Admitted should be about `BURST + elapsed × AVERAGE`, where `elapsed` is how long the burst
took — no bucket is purely static, and at these rates the refill during the burst itself is
a visible part of the total. With `burst=200`, `average=100`/s and 400 requests taking
0.67s: 267 admitted, 133 refused.

That arithmetic *is* the test. What the other outcomes mean:

| Result | Meaning |
|---|---|
| Every request admitted | The middleware is not on this route, or the route is not the one you think |
| Admitted ≈ `BURST` + refill | Correct |
| Admitted ≈ replicas × `BURST` | The replicas are not sharing counters — check for `PeerPublishRejected` in their logs, and that every replica has the same `PEER_SHARED_SECRET` |
| Every request refused with 500 | The proxy cannot reach the store; check the NetworkPolicy and the Service name in the `redis` block |

## 2. It refills at `average`, and no faster

```sh
burst $(( BURST * 2 )) $ROUTE_A
sleep 1
burst $(( AVERAGE + AVERAGE / 2 )) $ROUTE_A
```

The second burst should admit about `AVERAGE` (one second of refill) plus whatever the
burst itself takes. Measured at 100/s: 122 of 150.

This is the check that separates a working refill from a bucket that simply resets. A
limiter that refilled the whole burst allowance at once would admit all 150.

## 3. Under the limit, nothing is refused

```sh
for s in 1 2 3 4; do burst $(( AVERAGE / 2 )) $ROUTE_A; sleep 0.5; done
```

Zero 429s, across all four rounds. Sustained traffic at half the configured rate must never
be refused: a store that returned tokens slightly too slowly, or a mesh that double-counted
its own admissions, would show up here as a handful of 429s in the later rounds and nowhere
else.

## 4. The key is the middleware, not the route

```sh
burst $(( BURST * 2 )) $ROUTE_A    # drain, on the first route
burst 20 $ROUTE_B                  # immediately, on the second
```

The second route should be refused almost everything — it inherits the drained bucket,
admitting only what refilled in the gap (5 of 20, in one measurement).

This is the check that distinguishes the store from the proxy's in-memory limiter. Traefik
keeps one bucket map per *router*, so in-memory the second route would have its own
untouched allowance; through the store the key is `rate:<middleware>:<source>` and both
routes spend one budget. If both routes admit a full burst, the middleware is not
redis-backed — the proxy fell back to memory, which it does silently.

## 5. The key is also the source

While the first client is drained, a request from a different source address should be
admitted in full:

```sh
kubectl run curlcheck --rm -i --restart=Never --image=curlimages/curl:latest --command -- sh -c '
for i in $(seq 50); do printf "url = \"http://<proxy-service>/service-a/some/path\"\noutput = \"/dev/null\"\n"; done \
| curl -sk -H "Host: api.example.test" -w "%{http_code}\n" -K - | sort | uniq -c'
```

All 50 admitted, while the host running the earlier checks is still at zero.

Mind what the proxy actually sees as the source. Under `sourceCriterion.ipStrategy` with no
`depth`, that is the connecting address — which behind a cloud load balancer, a k3d
serverlb or any hop that does not preserve the client address is *one* address for all
external traffic, and therefore one bucket for all of it. If this check admits nothing, the
pod and your shell are sharing a source address as far as the proxy is concerned.

## 6. Chained middlewares count apart

If a route carries two rate-limit middlewares, each keeps its own bucket and the stricter
one bites first. On a route carrying both the shared middleware and a tighter one at
`average=5, burst=20`, a burst of 60 admits 20.

Useful as a sanity check on which limit is actually in force: if the tight middleware is on
the route, no burst against it can ever reveal the wider limit.

## 7. The replicas share one budget

Check 1 already proves it, provided the store runs more than one replica. The proxy's
connection pool fans out across every replica behind the ClusterIP, so a burst is decided
by all of them; admitting one bucket's worth means they agreed on one bucket. Without
sharing, N replicas admit up to N × `burst`.

To see that the burst really did reach every replica rather than landing on one:

```sh
kubectl logs -l app=traefik-ratelimit-store --prefix --tail=20 \
    | grep -E "ScriptRegistered|ScriptDiverged|PeerPublishRejected|StoreAtCapacity"
```

`ScriptRegistered` from every replica, timestamped inside your burst, is that evidence —
each of them served the script for itself. `PeerPublishRejected` means the mesh is not
sharing and the result of check 1 is meaningless; `ScriptDiverged` means the proxy sent a
script this store does not recognise, so the arithmetic being enforced may not be the
proxy's.

## Store-side and proxy-side confirmation

The store's own endpoints, on the peer port:

```sh
STORE_POD=$(kubectl get pod -l app=traefik-ratelimit-store -o jsonpath='{.items[0].metadata.name}')
kubectl port-forward pod/$STORE_POD 18080:8080 &

curl -s -w ' [%{http_code}]\n' http://127.0.0.1:18080/health       # alive [200]
curl -s -w ' [%{http_code}]\n' http://127.0.0.1:18080/readiness    # ready [200]
```

`/readiness` connects to the RESP port to answer, so a `ready` here means the port carrying
production traffic answered a `PING` within the last second — not merely that the process
is up.

And, if the proxy's API is exposed, that it is configured against the store at all:

```sh
kubectl port-forward pod/<proxy-pod> 19000:8080 &
curl -s http://127.0.0.1:19000/api/http/middlewares/<namespace>-<middleware>@kubernetescrd
```

Look for `"status":"enabled"` and a `redis` block naming the store's Service. A middleware
that failed to parse is reported here with its error, and is otherwise invisible: the route
keeps serving, limited only by whatever the proxy fell back to.
