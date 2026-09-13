#!/usr/bin/env python3

import argparse
import csv
import json
import os
import random
import statistics
import sys
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import psycopg2
from opensearchpy import OpenSearch
from qdrant_client import QdrantClient

from load_generator import run_open_loop


# ============================================================
# Configuration
# ============================================================

DEFAULT_RATE = 100
DEFAULT_DURATION = 60
DEFAULT_CONCURRENCY = 20

OPENSEARCH_INDEX = "users"
QDRANT_COLLECTION = "users_semantic"

POLL_INTERVAL_SECONDS = 0.01
REPLICATION_TIMEOUT_SECONDS = 30

# Shared with consumer/index_users.py, which appends one JSON
# line per processed message here. Must match its default/env var
# so both processes agree on the path without extra config.
CDC_TIMINGS_LOG = os.getenv(
    "CDC_TIMINGS_LOG",
    "logs/cdc_timings.jsonl",
)


# ============================================================
# Arguments
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmark PostgreSQL write and CDC replication latency."
    )

    parser.add_argument(
        "--rate",
        type=int,
        default=DEFAULT_RATE,
        help=f"Updates per second. Default: {DEFAULT_RATE}",
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION,
        help=f"Benchmark duration in seconds. Default: {DEFAULT_DURATION}",
    )

    parser.add_argument(
        "--csv",
        default="users.csv",
        help="Original 10K users CSV. Default: users.csv",
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=(
            "Max concurrent in-flight updates. Default: "
            f"{DEFAULT_CONCURRENCY}. Raise this if the achieved "
            "rate falls short of --rate."
        ),
    )

    return parser.parse_args()


# ============================================================
# Environment
# ============================================================

def get_env(name):
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}"
        )

    return value


# PostgreSQL
PG_HOST = get_env("PGHOST")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DATABASE = get_env("PGDATABASE")
PG_USER = get_env("PGWRITEUSER")
PG_PASSWORD = get_env("PGWRITEPASSWORD")


# OpenSearch
OPENSEARCH_HOST = get_env("OPENSEARCH_HOST")
OPENSEARCH_USERNAME = get_env("OPENSEARCH_USERNAME")
OPENSEARCH_PASSWORD = get_env("OPENSEARCH_PASSWORD")


# Qdrant
QDRANT_URL = get_env("QDRANT_URL")
QDRANT_API_KEY = get_env("QDRANT_API_KEY")


# ============================================================
# Clients
# ============================================================

def create_postgres_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode="require",
    )


opensearch = OpenSearch(
    hosts=[
        {
            "host": OPENSEARCH_HOST,
            "port": 443,
        }
    ],
    http_auth=(
        OPENSEARCH_USERNAME,
        OPENSEARCH_PASSWORD,
    ),
    use_ssl=True,
    verify_certs=True,
)

qdrant = QdrantClient(
    url=QDRANT_URL,
    api_key=QDRANT_API_KEY,
)


# ============================================================
# CSV
# ============================================================

def load_csv(path):
    """
    Load the original 10K dataset.

    The CSV is used as the value pool for generating updates.
    """

    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        raise RuntimeError(
            f"No rows found in CSV: {path}"
        )

    print(f"Loaded {len(rows):,} rows from {path}")

    return rows


# ============================================================
# Benchmark state
# ============================================================

class BenchmarkState:

    def __init__(self):
        self.lock = threading.Lock()

        self.total_updates = 0
        self.completed_updates = 0
        self.write_errors = 0

        self.db_latencies_ms = []

        self.opensearch_replication_ms = []
        self.qdrant_replication_ms = []

        self.opensearch_timeouts = 0
        self.qdrant_timeouts = 0

        self.pending = {}

        # All target ids touched this run, independent of self.pending
        # (which entries get removed from once replication is observed).
        # Used to match records back out of the consumer's timings log.
        self.all_target_ids = []

        # Stage breakdown, populated from the consumer's timings log
        # after the run (see read_new_timing_records / main()).
        self.db_to_debezium_ms = []
        self.kafka_to_consumer_ms = []
        self.opensearch_network_ms = []
        self.opensearch_server_ms = []
        self.qdrant_network_ms = []
        self.qdrant_server_ms = []

        self.stop_verifier = False


state = BenchmarkState()


# ============================================================
# Helpers
# ============================================================

def percentile(values, percentile):
    if not values:
        return None

    values = sorted(values)

    index = (len(values) - 1) * percentile / 100

    lower = int(index)
    upper = min(lower + 1, len(values) - 1)

    if lower == upper:
        return values[lower]

    weight = index - lower

    return (
        values[lower] * (1 - weight)
        + values[upper] * weight
    )


def format_metric(value):
    if value is None:
        return "N/A"

    return f"{value:.2f} ms"


def now_utc():
    return datetime.now(timezone.utc)


# ============================================================
# PostgreSQL update
# ============================================================

def perform_update(cursor, target_id, source_row):
    """
    Update one PostgreSQL user using values selected from
    the original 10K CSV.

    There is no database trigger on this table, so updated_at
    is set explicitly here rather than relying on one.

    RETURNING updated_at gives us the exact timestamp that
    PostgreSQL committed for this update.
    """

    start = time.perf_counter()

    cursor.execute(
        """
        UPDATE users
        SET
            name = %s,
            email = %s,
            company = %s,
            job_title = %s,
            location = %s,
            skills = %s,
            bio = %s,
            experience = %s,
            updated_at = NOW()
        WHERE id = %s
        RETURNING id, updated_at
        """,
        (
            source_row.get("name"),
            source_row.get("email"),
            source_row.get("company"),
            source_row.get("job_title"),
            source_row.get("location"),
            source_row.get("skills"),
            source_row.get("bio"),
            int(source_row["experience"])
            if source_row.get("experience")
            else None,
            target_id,
        ),
    )

    result = cursor.fetchone()

    if not result:
        raise RuntimeError(
            f"User {target_id} was not found"
        )

    # Commit is deliberately included in DB latency.
    cursor.connection.commit()

    end = time.perf_counter()

    db_latency_ms = (end - start) * 1000

    return result[0], result[1], db_latency_ms


# ============================================================
# PostgreSQL writer
# ============================================================

# One (connection, cursor) pair per worker thread - psycopg2
# connections aren't safe to share across threads. Populated by
# _init_pg_worker, which run_open_loop's ThreadPoolExecutor calls
# once per worker thread before it processes any tasks.
_pg_worker = threading.local()


def _init_pg_worker():
    connection = create_postgres_connection()
    _pg_worker.connection = connection
    _pg_worker.cursor = connection.cursor()


def run_writer(csv_rows, rate, duration, concurrency):

    print()
    print("Starting write benchmark")
    print(f"Rate:        {rate:,} updates/sec")
    print(f"Duration:    {duration:,} seconds")
    print(f"Concurrency: {concurrency:,}")
    print(f"Workload:    100% UPDATE")
    print()

    def _one_update():
        # Pick an existing target user.
        target_row = random.choice(csv_rows)
        target_id = int(target_row["id"])

        # Pick another row as the source of new values.
        source_row = random.choice(csv_rows)

        return perform_update(
            _pg_worker.cursor,
            target_id,
            source_row,
        )

    def _on_success(result):
        user_id, updated_at, db_latency_ms = result

        # Convert PostgreSQL timestamp into an epoch timestamp.
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)

        update_time = updated_at.timestamp()

        with state.lock:

            state.total_updates += 1

            state.db_latencies_ms.append(db_latency_ms)

            state.pending[user_id] = {
                "updated_at": update_time,
                "created_at": time.perf_counter(),
            }

            state.all_target_ids.append(user_id)

    def _on_error(exc):
        with state.lock:
            state.write_errors += 1

        print(
            f"[ERROR] Update failed: {exc}",
            file=sys.stderr,
        )

    dispatched, actual_duration = run_open_loop(
        rate=rate,
        duration=duration,
        task_fn=_one_update,
        on_success=_on_success,
        on_error=_on_error,
        max_concurrency=concurrency,
        worker_init=_init_pg_worker,
    )

    print()
    print(
        f"Writer finished: "
        f"{dispatched:,} dispatched, "
        f"{state.total_updates:,} completed, "
        f"{state.write_errors:,} errors"
    )

    return dispatched, actual_duration


# ============================================================
# Timestamp parsing
# ============================================================

def parse_debezium_timestamp(value):
    """
    users.updated_at is a Postgres "timestamp without time zone"
    column (confirmed via \\d users). Debezium's default wire
    representation for that type is epoch MICROSECONDS as a
    number (io.debezium.time.MicroTimestamp) - not an ISO string.

    Some older, separately-seeded documents (loaded outside this
    CDC pipeline, before it existed) still carry ISO-8601 strings,
    so both forms are handled here. Returns epoch seconds, or
    None if the value can't be parsed.
    """

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        return value / 1_000_000

    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
        except ValueError:
            return None

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return parsed.timestamp()

    return None


# ============================================================
# OpenSearch verification
# ============================================================

def check_opensearch(user_id, expected_updated_at):
    try:

        response = opensearch.get(
            index=OPENSEARCH_INDEX,
            id=str(user_id),
        )

        source = response.get("_source", {})

        actual_updated_at = source.get("updated_at")

        if actual_updated_at is None:
            return False

        actual_epoch = parse_debezium_timestamp(actual_updated_at)

        if actual_epoch is None:
            return False

        return actual_epoch >= expected_updated_at

    except Exception:
        return False


# ============================================================
# Qdrant verification
# ============================================================

def check_qdrant(user_id, expected_updated_at):
    try:

        points = qdrant.retrieve(
            collection_name=QDRANT_COLLECTION,
            ids=[int(user_id)],
            with_payload=True,
        )

        if not points:
            return False

        payload = points[0].payload or {}

        actual_updated_at = payload.get(
            "updated_at"
        )

        if actual_updated_at is None:
            return False

        actual_epoch = parse_debezium_timestamp(actual_updated_at)

        if actual_epoch is None:
            return False

        return actual_epoch >= expected_updated_at

    except Exception:
        return False


# ============================================================
# Stage-latency breakdown (from the consumer's timings log)
# ============================================================

def get_timings_log_offset():
    """
    Current size of the consumer's timings log, so a later read
    can skip straight to lines appended during this benchmark run.
    """

    try:
        return os.path.getsize(CDC_TIMINGS_LOG)
    except OSError:
        return 0


def collect_stage_timings(start_offset, target_ids):
    """
    Read new lines appended to the consumer's timings log since
    start_offset, keep only records for ids this run touched, and
    fold their stage values into the matching state.* lists.
    """

    target_ids = set(target_ids)

    try:
        with open(CDC_TIMINGS_LOG, "r", encoding="utf-8") as f:
            f.seek(start_offset)
            new_lines = f.readlines()
    except OSError:
        return

    for line in new_lines:
        line = line.strip()

        if not line:
            continue

        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        if record.get("user_id") not in target_ids:
            continue

        for field, bucket in (
            ("db_to_debezium_ms", state.db_to_debezium_ms),
            ("kafka_to_consumer_ms", state.kafka_to_consumer_ms),
            ("opensearch_network_ms", state.opensearch_network_ms),
            ("opensearch_server_ms", state.opensearch_server_ms),
            ("qdrant_network_ms", state.qdrant_network_ms),
            ("qdrant_server_ms", state.qdrant_server_ms),
        ):
            value = record.get(field)

            if value is not None:
                bucket.append(value)


# ============================================================
# Replication verifier
# ============================================================

def run_verifier():

    print("Replication verifier started")

    while True:

        with state.lock:

            if (
                state.stop_verifier
                and not state.pending
            ):
                break

            pending_items = list(
                state.pending.items()
            )

        now = time.perf_counter()

        for user_id, item in pending_items:

            expected_updated_at = item["updated_at"]
            started_at = item["created_at"]

            elapsed = now - started_at

            # -----------------------------------------------
            # OpenSearch
            # -----------------------------------------------

            if not item.get("opensearch_done"):

                if check_opensearch(
                    user_id,
                    expected_updated_at,
                ):

                    latency_ms = elapsed * 1000

                    with state.lock:

                        state.opensearch_replication_ms.append(
                            latency_ms
                        )

                        state.pending[user_id][
                            "opensearch_done"
                        ] = True

                        print(
                            f"[OpenSearch] "
                            f"user={user_id} "
                            f"replication={latency_ms:.2f} ms"
                        )

            # -----------------------------------------------
            # Qdrant
            # -----------------------------------------------

            if not item.get("qdrant_done"):

                if check_qdrant(
                    user_id,
                    expected_updated_at,
                ):

                    latency_ms = elapsed * 1000

                    with state.lock:

                        state.qdrant_replication_ms.append(
                            latency_ms
                        )

                        state.pending[user_id][
                            "qdrant_done"
                        ] = True

                        print(
                            f"[Qdrant] "
                            f"user={user_id} "
                            f"replication={latency_ms:.2f} ms"
                        )

            # -----------------------------------------------
            # Remove completed update
            # -----------------------------------------------

            with state.lock:

                item = state.pending.get(
                    user_id
                )

                if not item:
                    continue

                if (
                    item.get("opensearch_done")
                    and item.get("qdrant_done")
                ):
                    del state.pending[user_id]

        time.sleep(POLL_INTERVAL_SECONDS)

    print("Replication verifier stopped")


# ============================================================
# Reporting
# ============================================================

def print_report(name, values):

    print()
    print(name)
    print("-" * len(name))

    if not values:

        print("No measurements")

        return

    print(
        f"count: {len(values):,}"
    )

    print(
        f"min:   {min(values):.2f} ms"
    )

    print(
        f"mean:  {statistics.mean(values):.2f} ms"
    )

    print(
        f"p50:   {percentile(values, 50):.2f} ms"
    )

    print(
        f"p95:   {percentile(values, 95):.2f} ms"
    )

    print(
        f"p99:   {percentile(values, 99):.2f} ms"
    )

    print(
        f"max:   {max(values):.2f} ms"
    )


def print_final_report(target_rate, dispatched, duration):

    print()
    print("=" * 65)
    print("WRITE / CDC BENCHMARK REPORT")
    print("=" * 65)

    print(
        f"Duration:          {duration:.2f} seconds"
    )

    print(
        f"Requested rate:    {target_rate:,} updates/sec"
    )

    print(
        f"Dispatched:        {dispatched:,} "
        f"({dispatched / duration:.2f} updates/sec)"
    )

    print(
        f"Completed:         {state.total_updates:,} "
        f"({state.total_updates / duration:.2f} updates/sec)"
    )

    print(
        f"Errors:            {state.write_errors:,}"
    )

    if dispatched > state.total_updates + state.write_errors:
        print(
            "NOTE: dispatched > completed + errors - some updates "
            "were still in flight when the run ended."
        )

    if dispatched / duration < target_rate * 0.9:
        print(
            "NOTE: dispatched rate fell short of the requested "
            "rate - --concurrency is too low for how long each "
            "update actually takes; raise it to dispatch faster."
        )

    print_report(
        "PostgreSQL write latency",
        state.db_latencies_ms,
    )

    print_report(
        "PostgreSQL -> OpenSearch replication latency",
        state.opensearch_replication_ms,
    )

    print_report(
        "PostgreSQL -> Qdrant replication latency",
        state.qdrant_replication_ms,
    )

    print()
    print("=" * 65)
    print("STAGE-BY-STAGE LATENCY BREAKDOWN")
    print(
        "(from the consumer's own instrumentation - requires "
        "consumer/index_users.py to be running the version that "
        "writes to CDC_TIMINGS_LOG)"
    )
    print("=" * 65)

    print_report(
        "Postgres commit -> Debezium capture",
        state.db_to_debezium_ms,
    )

    print_report(
        "Debezium -> consumer receive (Kafka)",
        state.kafka_to_consumer_ms,
    )

    print_report(
        "OpenSearch: network overhead",
        state.opensearch_network_ms,
    )

    print_report(
        "OpenSearch: server-side indexing (took)",
        state.opensearch_server_ms,
    )

    print_report(
        "Qdrant: network overhead",
        state.qdrant_network_ms,
    )

    print_report(
        "Qdrant: server-side embed + HNSW (time)",
        state.qdrant_server_ms,
    )

    print()
    print("Replication coverage")
    print("--------------------")

    print(
        f"OpenSearch observed: "
        f"{len(state.opensearch_replication_ms):,} / "
        f"{state.total_updates:,}"
    )

    print(
        f"Qdrant observed:     "
        f"{len(state.qdrant_replication_ms):,} / "
        f"{state.total_updates:,}"
    )

    print("=" * 65)


# ============================================================
# Main
# ============================================================

def main():

    args = parse_args()

    if args.rate <= 0:
        raise ValueError(
            "--rate must be greater than 0"
        )

    if args.duration <= 0:
        raise ValueError(
            "--duration must be greater than 0"
        )

    if args.concurrency <= 0:
        raise ValueError(
            "--concurrency must be greater than 0"
        )

    csv_rows = load_csv(args.csv)

    # Snapshot the timings log's current size so the later read
    # only picks up lines the consumer appends during this run.
    timings_log_start_offset = get_timings_log_offset()

    # Start verifier first so that very fast CDC events
    # are not missed.
    verifier = threading.Thread(
        target=run_verifier,
        daemon=True,
    )

    verifier.start()

    dispatched = 0
    actual_duration = args.duration

    try:

        dispatched, actual_duration = run_writer(
            csv_rows,
            args.rate,
            args.duration,
            args.concurrency,
        )

        # Give the CDC pipeline time to catch up.
        print()
        print(
            "Waiting for CDC pipeline to catch up..."
        )

        deadline = (
            time.perf_counter()
            + REPLICATION_TIMEOUT_SECONDS
        )

        while time.perf_counter() < deadline:

            with state.lock:

                if not state.pending:
                    break

            time.sleep(0.1)

    finally:

        with state.lock:
            state.stop_verifier = True

        verifier.join(timeout=5)

    collect_stage_timings(
        timings_log_start_offset,
        state.all_target_ids,
    )

    print_final_report(args.rate, dispatched, actual_duration)


if __name__ == "__main__":
    main()