#!/usr/bin/env bash
# Stands up a throwaway k3d cluster and proves the store enforces a shared limit behind
# real Traefik, configured the way production configures it.
#
# This covers what the Docker-only test cannot:
#
#   - the Kubernetes CRD provider, which builds the middleware from a Middleware resource
#     rather than reading a dynamic configuration file — a different code path,
#     with its own defaulting
#   - peer discovery through a headless Service, resolving one address per ready replica
#     instead of a hardcoded list
#   - the manifests in deploy/, applied as written
#   - several store replicas behind a ClusterIP, so the proxy's connections fan out across
#     them exactly as they do in production. This is what makes the result meaningful: with
#     three replicas and no counter sharing, a burst of five would admit up to fifteen.
#
# Usage: $0 [--clusterName <name>] [--traefikVersion <tag>] [--keepCluster <true|false>]

set -euo pipefail

clusterName=rlstore-e2e
traefikVersion=v3.7.10
keepCluster=false

while [ $# -gt 0 ]; do
    case "$1" in
        --clusterName)    clusterName="$2"; shift 2 ;;
        --traefikVersion) traefikVersion="$2"; shift 2 ;;
        --keepCluster)    keepCluster="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
CONTEXT="k3d-$clusterName"
IMAGE=traefik-ratelimit-store:ci
LOCAL_PORT=18100

cleanup() {
    kill %1 > /dev/null 2>&1 || true
    if [ "$keepCluster" != "true" ]; then
        echo "Deleting the cluster..."
        k3d cluster delete "$clusterName" > /dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

echo "Building the store image..."
docker build -q -t "$IMAGE" "$ROOT" > /dev/null

echo "Creating cluster $clusterName..."
# Three nodes so the DaemonSet gives three proxy pods and the store spreads across all of
# them. The bundled ingress is disabled: this test supplies its own, at a pinned version.
# The context is deliberately not switched, so an existing kubectl session is left alone.
k3d cluster create "$clusterName" \
    --agents 2 \
    --k3s-arg "--disable=traefik@server:*" \
    --kubeconfig-switch-context=false \
    --wait > /dev/null

echo "Importing the store image..."
k3d image import "$IMAGE" -c "$clusterName" > /dev/null

echo "Applying Traefik CRDs..."
kubectl --context "$CONTEXT" apply --server-side \
    -f "https://raw.githubusercontent.com/traefik/traefik/$traefikVersion/integration/fixtures/k8s/01-traefik-crd.yml" > /dev/null

echo "Deploying the store from deploy/..."
# The manifest requires a peer secret, so the cluster gets one. Generated per run rather
# than hardcoded, so the test exercises the same path production does.
kubectl --context "$CONTEXT" create secret generic traefik-ratelimit-store \
    --from-literal=peer-shared-secret="$(openssl rand -hex 32)" > /dev/null

sed "s|image: traefik-ratelimit-store:.*|image: $IMAGE|" "$ROOT/deploy/traefik-ratelimit-store.yaml" \
    | kubectl --context "$CONTEXT" apply -f - > /dev/null

echo "Deploying Traefik and the workload..."
sed "s|image: traefik:.*|image: traefik:$traefikVersion|" "$HERE/traefik.yaml" \
    | kubectl --context "$CONTEXT" apply -f - > /dev/null
kubectl --context "$CONTEXT" apply -f "$HERE/workload.yaml" > /dev/null

echo "Waiting for everything to become ready..."
kubectl --context "$CONTEXT" rollout status deploy/traefik-ratelimit-store --timeout=180s > /dev/null
kubectl --context "$CONTEXT" rollout status ds/traefik --timeout=180s > /dev/null
kubectl --context "$CONTEXT" rollout status deploy/whoami --timeout=180s > /dev/null

replicas=$(kubectl --context "$CONTEXT" get pods -l app=traefik-ratelimit-store --no-headers | wc -l | tr -d ' ')
peers=$(kubectl --context "$CONTEXT" get endpointslices \
    -l kubernetes.io/service-name=traefik-ratelimit-store-peers \
    -o jsonpath='{.items[*].endpoints[*].addresses[*]}')

echo ""
echo "Store replicas:   $replicas"
echo "Peer addresses:   $peers"

if [ -z "$peers" ]; then
    echo "FAILED: the headless Service resolved no peers, so the replicas cannot share counters"
    exit 1
fi

# Established after the rollouts, so it cannot bind to a pod that is about to be replaced.
kubectl --context "$CONTEXT" port-forward svc/traefik "$LOCAL_PORT:80" > /dev/null 2>&1 &
sleep 4

echo ""
echo "Sending 20 requests..."
admitted=0
refused=0
other=0
for _ in $(seq 1 20); do
    status=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$LOCAL_PORT/")
    case "$status" in
        200) admitted=$((admitted + 1)) ;;
        429) refused=$((refused + 1)) ;;
        *)   other=$((other + 1)); echo "  unexpected status $status" ;;
    esac
done

echo ""
echo "admitted (200): $admitted"
echo "refused  (429): $refused"
echo "other:          $other"

# burst is 5. Anything approaching replicas x burst means the counters are not shared.
ceiling=$((5 + replicas))

echo ""
if [ "$other" -ne 0 ]; then
    echo "FAILED: unexpected statuses, so the store did not answer cleanly"
    exit 1
fi
if [ "$refused" -eq 0 ]; then
    echo "FAILED: nothing was refused, so the limit was not enforced"
    exit 1
fi
if [ "$admitted" -gt "$ceiling" ]; then
    echo "FAILED: $admitted admitted against a burst of 5 across $replicas replicas —"
    echo "        the counters are not being shared"
    exit 1
fi
echo "PASSED: $replicas replicas enforced one shared limit behind real Traefik"
