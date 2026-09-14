#!/usr/bin/env bash
#
# One-time project setup: creates a shared virtualenv, installs
# dependencies, and scaffolds .env if it doesn't exist yet.
#
# Usage:
#   bash setup.sh
#
# After this, the scripts under setup/ (consumer.sh, read-benchmarking.sh,
# write-benchmarking.sh) activate this venv and load .env themselves, so
# they can be run directly without re-activating anything by hand.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [ ! -d ".venv" ]; then
    echo "Creating virtual environment in .venv ..."
    python3.14 -m venv .venv
else
    echo ".venv already exists, reusing it."
fi

# shellcheck disable=SC1091
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt

if [ ! -f ".env" ]; then
    echo "Creating .env template ..."
    cat > .env <<'EOF'
# MySQL source (usersdb on mysql-source, host port 3306) - the write
# benchmark's user (SELECT/UPDATE on users only)
MYSQL_SOURCE_HOST=localhost
MYSQL_SOURCE_PORT=3306
MYSQL_SOURCE_DB=usersdb
MYSQL_SOURCE_USER=writer
MYSQL_SOURCE_PASSWORD=writerpass

# MySQL source admin - only needed by scripts/seed_source.py
MYSQL_SOURCE_ADMIN_USER=root
MYSQL_SOURCE_ADMIN_PASSWORD=rootpassword

# MySQL destination (searchdb on mysql-dest, host port 3307) - used
# by consumer/index_users.py and search/query.py
MYSQL_DEST_HOST=localhost
MYSQL_DEST_PORT=3307
MYSQL_DEST_DB=searchdb
MYSQL_DEST_USER=consumer
MYSQL_DEST_PASSWORD=consumerpass

# Kafka (defaults match kafka/docker-compose.yml; override if different)
KAFKA_BOOTSTRAP_SERVERS=localhost:9094
KAFKA_TOPIC=usersdb.usersdb.users
KAFKA_GROUP_ID=users-indexer

# Shared stage-latency log: consumer/index_users.py appends to it,
# benchmarking/write-benchmarking.py reads it back for its report.
CDC_TIMINGS_LOG=logs/cdc_timings.jsonl
EOF
    echo "Created .env with empty values — fill in real credentials before running anything."
else
    echo ".env already exists, leaving it untouched."
fi

echo
echo "Setup complete."
echo "Next: fill in .env, make sure kafka/docker-compose.yml is up and the"
echo "Debezium connector is registered, then run one of:"
echo "  bash run/consumer.sh"
echo "  bash run/read-benchmarking.sh"
echo "  bash run/write-benchmarking.sh"
