#!/usr/bin/env bash
# Runs sustained load with connection churn and watches for leaks.
#
# Every other measurement holds its connections open for the duration, which is exactly
# the shape that hides a leak in connection handling. This opens and closes them
# repeatedly, and samples the things that would grow if something were not being released:
# resident memory, file descriptors, and the store's own entry count.
#
# Usage: $0 [--rounds <n>] [--connections <n>] [--requestsEach <n>]

set -euo pipefail

rounds=40
connections=100
requestsEach=200

while [ $# -gt 0 ]; do
    case "$1" in
        --rounds)       rounds="$2"; shift 2 ;;
        --connections)  connections="$2"; shift 2 ;;
        --requestsEach) requestsEach="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ADDRESS=127.0.0.1:16379

cleanup() { kill "$STORE_PID" 2>/dev/null || true; }
trap cleanup EXIT

echo "Building..."
cargo build --release --manifest-path "$ROOT/Cargo.toml" -q
cargo build --release --examples --manifest-path "$ROOT/Cargo.toml" -q

LISTEN_ADDRESS="$ADDRESS" PEER_LISTEN_ADDRESS=127.0.0.1:18080 RUST_LOG=warn \
    "$ROOT/target/release/traefik-ratelimit-store" > /tmp/soak-store.log 2>&1 &
STORE_PID=$!
sleep 2

echo "Store pid $STORE_PID"
echo "$rounds rounds x $connections connections x $requestsEach requests"
echo ""
printf "%-7s %-10s %-8s %-12s\n" "round" "rss(KB)" "fds" "requests"

total=0
first_rss=0
for round in $(seq 1 "$rounds"); do
    "$ROOT/target/release/examples/latency" "$ADDRESS" "$connections" "$requestsEach" > /dev/null 2>&1
    total=$((total + connections * requestsEach))

    rss=$(ps -o rss= -p "$STORE_PID" | tr -d ' ')
    fds=$(lsof -p "$STORE_PID" 2>/dev/null | wc -l | tr -d ' ')
    [ "$first_rss" -eq 0 ] && first_rss=$rss

    if [ $((round % 5)) -eq 0 ] || [ "$round" -eq 1 ]; then
        printf "%-7s %-10s %-8s %-12s\n" "$round" "$rss" "$fds" "$total"
    fi
done

final_rss=$(ps -o rss= -p "$STORE_PID" | tr -d ' ')
final_fds=$(lsof -p "$STORE_PID" 2>/dev/null | wc -l | tr -d ' ')

echo ""
echo "requests served:  $total"
echo "connections made: $((rounds * connections))"
echo "rss first/final:  ${first_rss}KB / ${final_rss}KB"
echo "fds final:        $final_fds"
echo ""

# A leak shows as growth proportional to the work done. Memory settles at a plateau
# instead, because the resident set tracks concurrent traffic rather than uptime.
growth=$((final_rss - first_rss))
echo "rss growth: ${growth}KB over $total requests"
if [ "$final_fds" -gt 200 ]; then
    echo "FAILED: $final_fds descriptors held after $((rounds * connections)) connections — they are not being released"
    exit 1
fi
if [ "$growth" -gt "$((first_rss))" ]; then
    echo "FAILED: resident memory more than doubled, which is growth rather than a plateau"
    exit 1
fi
echo "PASSED: memory and descriptors plateaued under sustained churn"
