#!/usr/bin/env bash
# Drives real Traefik, configured with a rateLimit middleware pointed at this store, and
# checks that a burst is admitted and the rest are refused.
#
# This is the only test that exercises the whole chain: Traefik's middleware, its Redis
# client, this store's protocol handling, and the bucket arithmetic. Everything else
# either stubs the client or drives the store directly.
#
# Usage: $0 [--traefikVersion <tag>] [--requests <n>]

set -euo pipefail

traefikVersion=v3.7.10
requests=20

while [ $# -gt 0 ]; do
    case "$1" in
        --traefikVersion) traefikVersion="$2"; shift 2 ;;
        --requests)       requests="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

NETWORK=rlstore-e2e
IMAGE=traefik-ratelimit-store:ci

# Deliberately inside the repository rather than a temporary directory. Docker Desktop
# shares only a configured set of host paths, and neither /tmp nor mktemp's default
# location is normally among them — a bind mount from there silently produces an EMPTY
# DIRECTORY instead of the file, and Traefik then serves 404 with nothing in its log to
# explain why. The repository is under the user's home, which is shared by default.
WORKDIR="$(cd "$(dirname "$0")/.." && pwd)/.e2e"
rm -rf "$WORKDIR"
mkdir -p "$WORKDIR"

cleanup() {
    docker rm -f rlstore-traefik rlstore-whoami rlstore-store > /dev/null 2>&1 || true
    docker network rm "$NETWORK" > /dev/null 2>&1 || true
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

echo "Building the store image..."
docker build -q -t "$IMAGE" . > /dev/null

echo "Creating the network..."
docker network create "$NETWORK" > /dev/null

echo "Starting the store..."
docker run -d --name rlstore-store --network "$NETWORK" --network-alias store "$IMAGE" > /dev/null

echo "Starting a backend..."
docker run -d --name rlstore-whoami --network "$NETWORK" --network-alias whoami traefik/whoami > /dev/null

# burst 5 at 3 requests per second: a rapid burst should see about five admitted and the
# remainder refused, because almost no time passes for the bucket to refill.
cat > "$WORKDIR/dynamic.yml" <<'YAML'
http:
  routers:
    whoami:
      rule: "PathPrefix(`/`)"
      entryPoints: [web]
      middlewares: [ratelimit]
      service: whoami
  services:
    whoami:
      loadBalancer:
        servers:
          - url: "http://whoami:80"
  middlewares:
    ratelimit:
      rateLimit:
        average: 30
        period: 10s
        burst: 5
        redis:
          endpoints: ["store:6379"]
          db: 0
          poolSize: 2
          maxActiveConns: 5
          dialTimeout: 200ms
          readTimeout: 200ms
          writeTimeout: 200ms
YAML

echo "Starting Traefik $traefikVersion..."
# The directory is mounted, not the file: a directory mount fails loudly when the host
# path is unavailable, where a file mount fails silently by inventing a directory.
docker run -d --name rlstore-traefik --network "$NETWORK" -p 18000:80 \
    -v "$WORKDIR:/etc/traefik/conf:ro" \
    "traefik:$traefikVersion" \
    --entrypoints.web.address=:80 \
    --providers.file.directory=/etc/traefik/conf \
    --log.level=INFO > /dev/null

echo "Confirming Traefik can see its configuration..."
if ! docker exec rlstore-traefik ls /etc/traefik/conf/dynamic.yml > /dev/null 2>&1; then
    echo "FAILED: the configuration did not reach the container."
    echo "Add $WORKDIR to Docker's shared file paths and retry."
    exit 1
fi

echo "Waiting for the route to come up..."
for _ in $(seq 1 30); do
    if [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:18000/)" != "000" ]; then
        break
    fi
    sleep 1
done

echo ""
echo "Sending $requests requests..."
admitted=0
refused=0
other=0
for _ in $(seq 1 "$requests"); do
    status=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:18000/)
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
echo ""
echo "Store log:"
docker logs rlstore-store 2>&1 | tail -5

echo ""
if [ "$other" -ne 0 ]; then
    echo "FAILED: unexpected statuses, so the store did not answer cleanly"
    exit 1
fi
if [ "$admitted" -eq 0 ]; then
    echo "FAILED: nothing was admitted"
    exit 1
fi
if [ "$refused" -eq 0 ]; then
    echo "FAILED: nothing was refused, so the limit was not enforced"
    exit 1
fi
echo "PASSED: real Traefik enforced a shared limit through this store"
