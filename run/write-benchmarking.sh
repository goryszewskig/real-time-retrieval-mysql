#!/usr/bin/env bash
#
# Runs the write/CDC replication benchmark
# (benchmarking/write-benchmarking.py).
#
# Assumes bash setup.sh has already been run once from the repo root,
# and that the "users" table in Postgres is already seeded with the
# rows in data/jobseekers_10000.csv (e.g. via the same load that fed
# the Debezium initial snapshot).
#
# Any arguments are forwarded to write-benchmarking.py, e.g.:
#   bash run/write-benchmarking.sh --rate 1 --duration 1

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

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

# write-benchmarking.py's --csv default ("users.csv") doesn't match
# this repo's actual dataset path, so it's passed explicitly here.
# Anything you pass to this script (--rate, --duration, ...) is
# forwarded after it and overrides/extends these defaults.
python benchmarking/write-benchmarking.py --csv data/jobseekers_10000.csv "$@"
