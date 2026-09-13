#!/usr/bin/env bash
#
# Runs the CDC consumer (consumer/index_users.py).
# Assumes bash setup.sh has already been run once from the repo root.
#
# Usage:
#   bash run/consumer.sh                 # single instance (default)
#   bash run/consumer.sh --instances 3    # 3 instances, same Kafka
#                                         # consumer group - Kafka
#                                         # splits the topic's
#                                         # partitions across them.
#                                         # usersdb.public.users has
#                                         # 3 partitions, so 3 is the
#                                         # natural ceiling here.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

INSTANCES=1

if [ "${1:-}" = "--instances" ]; then
    INSTANCES="${2:?--instances requires a number}"
fi

if [ ! -d ".venv" ]; then
    echo "No .venv found. Run 'bash setup.sh' from the repo root first." >&2
    exit 1
fi

if [ ! -f ".env" ]; then
    echo "No .env found. Run 'bash setup.sh' from the repo root first." >&2
    exit 1
fi

# shellcheck disable=SC1091
source .venv/bin/activate

set -a
# shellcheck disable=SC1091
source .env
set +a

if [ "$INSTANCES" -eq 1 ]; then
    exec python consumer/index_users.py
fi

mkdir -p logs

pids=()

cleanup() {
    echo "Stopping $INSTANCES consumer instances..."
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait
}
trap cleanup INT TERM

for i in $(seq 0 $((INSTANCES - 1))); do
    CONSUMER_INSTANCE_ID="$i" python consumer/index_users.py \
        > "logs/consumer-$i.log" 2>&1 &
    pids+=("$!")
    echo "Started consumer instance $i (pid $!) -> logs/consumer-$i.log"
done

wait
