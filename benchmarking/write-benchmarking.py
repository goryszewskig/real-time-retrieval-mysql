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
from datetime import datetime, timezone

import pymysql

from load_generator import run_open_loop


# ============================================================
# Configuration
# ============================================================

DEFAULT_RATE = 100
DEFAULT_DURATION = 60
DEFAULT_CONCURRENCY = 20

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
        description="Benchmark MySQL write and CDC replication latency."
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
        default="data/jobseekers_10000.csv",
        help="Original 10K users CSV. Default: data/jobseekers_10000.csv",
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


# MySQL source (usersdb) - the benchmark's write user
MYSQL_SOURCE_HOST = os.getenv("MYSQL_SOURCE_HOST", "localhost")
MYSQL_SOURCE_PORT = int(os.getenv("MYSQL_SOURCE_PORT", "3306"))
MYSQL_SOURCE_DB = os.getenv("MYSQL_SOURCE_DB", "usersdb")
MYSQL_SOURCE_USER = get_env("MYSQL_SOURCE_USER")
MYSQL_SOURCE_PASSWORD = get_env("MYSQL_SOURCE_PASSWORD")

# MySQL destination (searchdb) - polled by the replication verifier
MYSQL_DEST_HOST = os.getenv("MYSQL_DEST_HOST", "localhost")
MYSQL_DEST_PORT = int(os.getenv("MYSQL_DEST_PORT", "3307"))
MYSQL_DEST_DB = os.getenv("MYSQL_DEST_DB", "searchdb")
MYSQL_DEST_USER = get_env("MYSQL_DEST_USER")
MYSQL_DEST_PASSWORD = get_env("MYSQL_DEST_PASSWORD")


# ============================================================
# Connections
# ============================================================

def create_source_connection():
    return pymysql.connect(
        host=MYSQL_SOURCE_HOST,
        port=MYSQL_SOURCE_PORT,
        user=MYSQL_SOURCE_USER,
        password=MYSQL_SOURCE_PASSWORD,
        database=MYSQL_SOURCE_DB,
        autocommit=False,
    )


def create_dest_connection():
    return pymysql.connect(
        host=MYSQL_DEST_HOST,
        port=MYSQL_DEST_PORT,
        user=MYSQL_DEST_USER,
        password=MYSQL_DEST_PASSWORD,
        database=MYSQL_DEST_DB,
        autocommit=True,
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

        self.dest_replication_ms = []
        self.dest_timeouts = 0

        self.pending = {}

        # All target ids touched this run, independent of self.pending
        # (which entries get removed from once replication is observed).
        # Used to match records back out of the consumer's timings log.
        self.all_target_ids = []

        # Stage breakdown, populated from the consumer's timings log
        # after the run (see collect_stage_timings / main()).
        self.db_to_debezium_ms = []
        self.kafka_to_consumer_ms = []
        self.mysql_total_ms = []

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
# MySQL source update
# ============================================================

def perform_update(connection, target_id, source_row):
    """
    Update one MySQL source user using values selected from
    the original 10K CSV.

    There is no database trigger on this table, so updated_at
    is set explicitly here rather than relying on one.

    MySQL 8.0 has no RETURNING clause, so the committed
    updated_at is read back with a follow-up SELECT on the
    same connection/transaction. NOW(6) keeps microsecond
    precision so the value can be matched exactly downstream.
    """

    start = time.perf_counter()

    with connection.cursor() as cursor:

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
                updated_at = NOW(6)
            WHERE id = %s
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

        if cursor.rowcount == 0:
            raise RuntimeError(
                f"User {target_id} was not found"
            )

        cursor.execute(
            "SELECT id, updated_at FROM users WHERE id = %s",
            (target_id,),
        )

        result = cursor.fetchone()

    # Commit is deliberately included in DB latency.
    connection.commit()

    end = time.perf_counter()

    db_latency_ms = (end - start) * 1000

    return result[0], result[1], db_latency_ms


# ============================================================
# MySQL writer
# ============================================================

# One connection per worker thread - PyMySQL connections aren't
# safe to share across threads. Populated by _init_mysql_worker,
# which run_open_loop's ThreadPoolExecutor calls once per worker
# thread before it processes any tasks.
_mysql_worker = threading.local()


def _init_mysql_worker():
    _mysql_worker.connection = create_source_connection()


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
            _mysql_worker.connection,
            target_id,
            source_row,
        )

    def _on_success(result):
        user_id, updated_at, db_latency_ms = result

        # Convert the MySQL DATETIME(6) into an epoch timestamp.
        # The compose MySQL servers run in UTC.
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
        worker_init=_init_mysql_worker,
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
# MySQL destination verification
# ============================================================

def check_dest(connection, user_id, expected_updated_at):
    """
    True once the destination row's updated_at matches (or has
    passed) the timestamp the source committed. Both sides store
    the same DATETIME(6) value, so this is an exact comparison of
    epoch timestamps.
    """

    try:

        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT updated_at FROM users WHERE id = %s",
                (user_id,),
            )
            row = cursor.fetchone()

        if not row or row[0] is None:
            return False

        actual = row[0]

        if isinstance(actual, datetime):

            if actual.tzinfo is None:
                actual = actual.replace(tzinfo=timezone.utc)

            actual_epoch = actual.timestamp()

        else:
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
            ("mysql_total_ms", state.mysql_total_ms),
        ):
            value = record.get(field)

            if value is not None:
                bucket.append(value)


# ============================================================
# Replication verifier
# ============================================================

def run_verifier():

    print("Replication verifier started")

    # Own connection: the verifier runs on its own thread.
    connection = create_dest_connection()

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
            # MySQL destination
            # -----------------------------------------------

            if not item.get("dest_done"):

                if check_dest(
                    connection,
                    user_id,
                    expected_updated_at,
                ):

                    latency_ms = elapsed * 1000

                    with state.lock:

                        state.dest_replication_ms.append(
                            latency_ms
                        )

                        state.pending[user_id][
                            "dest_done"
                        ] = True

                        print(
                            f"[MySQL dest] "
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

                if item.get("dest_done"):
                    del state.pending[user_id]

        time.sleep(POLL_INTERVAL_SECONDS)

    connection.close()

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
        "MySQL source write latency",
        state.db_latencies_ms,
    )

    print_report(
        "MySQL source -> MySQL dest replication latency",
        state.dest_replication_ms,
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
        "MySQL source commit -> Debezium capture",
        state.db_to_debezium_ms,
    )

    print_report(
        "Debezium -> consumer receive (Kafka)",
        state.kafka_to_consumer_ms,
    )

    print_report(
        "MySQL dest: upsert (network + server)",
        state.mysql_total_ms,
    )

    print()
    print("Replication coverage")
    print("--------------------")

    print(
        f"MySQL dest observed: "
        f"{len(state.dest_replication_ms):,} / "
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
