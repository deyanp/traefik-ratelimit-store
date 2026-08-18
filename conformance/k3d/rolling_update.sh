#!/usr/bin/env bash
# Rolls every store replica while traffic is flowing, and checks nothing was dropped.
#
# The proxy answers any store error with a 500 and has no fail-open switch, so a rollout
# that drops connections is visible as 500s rather than as a blip. 429 is the expected
# outcome for most requests here — the point is that none of them are 500.
#
# What makes this pass is the drain: on SIGTERM a replica fails readiness first, keeps
# serving for its drain period, then exits, so the orchestrator withdraws it before the
# listener closes and the proxy retries the resulting EOF onto a healthy replica.
#
# Needs a cluster from run.sh --keepCluster true.
#
# Usage: $0 [--clusterName <name>] [--requests <n>]

set -euo pipefail

clusterName=rlstore-e2e
requests=900

while [ $# -gt 0 ]; do
    case "$1" in
        --clusterName) clusterName="$2"; shift 2 ;;
        --requests)    requests="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

CONTEXT="k3d-$clusterName"
LOCAL_PORT=18100

cleanup() { kill %1 > /dev/null 2>&1 || true; }
trap cleanup EXIT

if ! kubectl --context "$CONTEXT" get deploy/traefik-ratelimit-store > /dev/null 2>&1; then
    echo "No store found in $CONTEXT."
    echo "Run: ./conformance/k3d/run.sh --keepCluster true"
    exit 1
fi

kubectl --context "$CONTEXT" port-forward svc/traefik "$LOCAL_PORT:80" > /dev/null 2>&1 &
sleep 4

echo "Sending $requests requests while the replicas roll..."
( for _ in $(seq 1 "$requests"); do
      curl -s -o /dev/null -m 3 -w '%{http_code}\n' "http://127.0.0.1:$LOCAL_PORT/"
      sleep 0.05
  done > /tmp/rolling-update-statuses.txt ) &
LOAD=$!

sleep 6
kubectl --context "$CONTEXT" rollout restart deploy/traefik-ratelimit-store > /dev/null
kubectl --context "$CONTEXT" rollout status deploy/traefik-ratelimit-store --timeout=180s > /dev/null
wait $LOAD

echo ""
sort /tmp/rolling-update-statuses.txt | uniq -c | sort -rn

served=$(grep -cE '^(200|429)$' /tmp/rolling-update-statuses.txt || true)
dropped=$(grep -cvE '^(200|429)$' /tmp/rolling-update-statuses.txt || true)

echo ""
echo "served:  $served"
echo "dropped: $dropped"
echo ""
if [ "$dropped" -ne 0 ]; then
    echo "FAILED: $dropped requests were neither served nor rate limited — the rollout dropped them"
    exit 1
fi
echo "PASSED: every replica was replaced without dropping a request"
