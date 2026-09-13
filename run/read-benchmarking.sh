#!/usr/bin/env bash
#
# Runs the read/hybrid-search benchmark
# (benchmarking/read-benchmarking.py).
#
# Assumes bash setup.sh has already been run once from the repo root.
#
# NOTE: benchmarking/read-benchmarking.py does `from query import query`,
# which only resolves if search/ (where query.py lives) is on
# PYTHONPATH. That's not the case by default, so this script adds it
# rather than relying on the script being run from inside search/.
#
# Any arguments are forwarded to read-benchmarking.py, e.g.:
#   bash run/read-benchmarking.sh --rate 1 --duration 1

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

PYTHONPATH="$ROOT_DIR/search${PYTHONPATH:+:$PYTHONPATH}" \
    python benchmarking/read-benchmarking.py "$@"
