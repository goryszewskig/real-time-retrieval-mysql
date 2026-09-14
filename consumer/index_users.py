import json
import os
import sys
import time

import pymysql
from kafka import KafkaConsumer


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9094",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "usersdb.usersdb.users",
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "users-indexer",
)

# Cosmetic only - all instances share KAFKA_GROUP_ID, which is what
# Kafka actually uses to split the topic's partitions across them.
# This just makes concurrently-running instances distinguishable in
# logs and in broker-side client listings.
CONSUMER_INSTANCE_ID = os.getenv("CONSUMER_INSTANCE_ID", "0")

# Destination MySQL (searchdb on mysql-dest).
MYSQL_DEST_HOST = os.getenv("MYSQL_DEST_HOST", "localhost")
MYSQL_DEST_PORT = int(os.getenv("MYSQL_DEST_PORT", "3307"))
MYSQL_DEST_DB = os.getenv("MYSQL_DEST_DB", "searchdb")
MYSQL_DEST_USER = os.environ["MYSQL_DEST_USER"]
MYSQL_DEST_PASSWORD = os.environ["MYSQL_DEST_PASSWORD"]

CDC_TIMINGS_LOG = os.getenv(
    "CDC_TIMINGS_LOG",
    "logs/cdc_timings.jsonl",
)


# ============================================================
# Destination MySQL connection
#
# Only one message is processed at a time in this process, so a
# single connection is enough (PyMySQL connections are not
# thread-safe anyway). autocommit=True: every upsert/delete is its
# own transaction, committed before we commit the Kafka offset.
# ============================================================

def create_mysql_connection():
    return pymysql.connect(
        host=MYSQL_DEST_HOST,
        port=MYSQL_DEST_PORT,
        user=MYSQL_DEST_USER,
        password=MYSQL_DEST_PASSWORD,
        database=MYSQL_DEST_DB,
        autocommit=True,
    )


mysql_conn = create_mysql_connection()


def reconnect():
    global mysql_conn

    try:
        mysql_conn.close()
    except Exception:
        pass

    mysql_conn = create_mysql_connection()


# ============================================================
# Kafka
# ============================================================

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=KAFKA_GROUP_ID,
    client_id=f"users-indexer-{CONSUMER_INSTANCE_ID}",

    # Debezium JSON payload
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),

    # Start from beginning when this consumer group
    # has no committed offset.
    auto_offset_reset="earliest",

    # We explicitly commit only after indexing succeeds.
    enable_auto_commit=False,
)


# ============================================================
# Helpers
# ============================================================

UPSERT_SQL = """
    INSERT INTO users (
        id, name, email, company, job_title, location,
        skills, bio, experience, created_at, updated_at
    )
    VALUES (
        %(id)s, %(name)s, %(email)s, %(company)s, %(job_title)s,
        %(location)s, %(skills)s, %(bio)s, %(experience)s,
        %(created_at)s, %(updated_at)s
    ) AS new
    ON DUPLICATE KEY UPDATE
        name = new.name,
        email = new.email,
        company = new.company,
        job_title = new.job_title,
        location = new.location,
        skills = new.skills,
        bio = new.bio,
        experience = new.experience,
        created_at = new.created_at,
        updated_at = new.updated_at
"""


def micros_to_datetime(epoch_micros):
    """
    Debezium's wire representation for a MySQL DATETIME(6) column is
    epoch MICROSECONDS as a number (io.debezium.time.MicroTimestamp).
    Convert it to a datetime for PyMySQL.
    """

    if epoch_micros is None:
        return None

    from datetime import datetime, timezone

    return datetime.fromtimestamp(
        epoch_micros / 1_000_000,
        tz=timezone.utc,
    ).replace(tzinfo=None)


def index_mysql(user):
    """
    Upsert the source row into the destination MySQL users table.

    The source users.id is deliberately used as the primary key, so
    the upsert is idempotent - safe under at-least-once redelivery.
    """

    document_id = user["id"]

    row = {
        "id": document_id,
        "name": user.get("name"),
        "email": user.get("email"),
        "company": user.get("company"),
        "job_title": user.get("job_title"),
        "location": user.get("location"),
        "skills": user.get("skills"),
        "bio": user.get("bio"),
        "experience": user.get("experience"),
        "created_at": micros_to_datetime(user.get("created_at")),
        "updated_at": micros_to_datetime(user.get("updated_at")),
    }

    total_start = time.perf_counter()

    try:
        with mysql_conn.cursor() as cursor:
            cursor.execute(UPSERT_SQL, row)
    except pymysql.OperationalError:
        # Dropped connection - reconnect once and retry.
        reconnect()

        with mysql_conn.cursor() as cursor:
            cursor.execute(UPSERT_SQL, row)

    total_ms = (time.perf_counter() - total_start) * 1000

    return {
        "total_ms": total_ms,
    }


def log_timing(record):
    """
    Append one stage-latency record (JSON line) to the shared
    timings log that benchmarking/write-benchmarking.py reads
    back after a run to build its stage-by-stage report.
    """

    try:
        directory = os.path.dirname(CDC_TIMINGS_LOG)

        if directory:
            os.makedirs(directory, exist_ok=True)

        with open(CDC_TIMINGS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    except Exception as exc:
        print(
            f"[WARN] Failed to write timings log: {exc}",
            file=sys.stderr,
        )


def delete_user(user_id):
    """
    Delete the user from the destination table.
    """

    try:
        with mysql_conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM users WHERE id = %s",
                (user_id,),
            )
    except pymysql.OperationalError:
        reconnect()

        with mysql_conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM users WHERE id = %s",
                (user_id,),
            )

    print(
        f"[DELETE] user={user_id}"
    )


# ============================================================
# Debezium event processing
# ============================================================

def process_event(event):
    """
    Debezium MySQL event structure:

    {
        "before": {...},
        "after": {...},
        "source": {...},
        "op": "c"
    }

    op:
        c = INSERT
        u = UPDATE
        d = DELETE
        r = READ (initial snapshot)
    """

    if not event:
        return

    received_at_ms = time.time() * 1000

    payload = event.get("payload")
    source_ts_ms = payload.get("source", {}).get("ts_ms")
    event_ts_ms = payload.get("ts_ms")

    db_to_debezium_ms = (
        (event_ts_ms - source_ts_ms)
        if source_ts_ms and event_ts_ms
        else None
    )

    kafka_to_consumer_ms = (
        (received_at_ms - event_ts_ms)
        if event_ts_ms
        else None
    )

    operation = payload.get("op")

    if operation in ("c", "u", "r"):
        user = payload.get("after")

        if not user:
            print(
                f"[WARN] {operation} event has no 'after'"
            )
            return

        mysql_timing = index_mysql(user)

        log_timing(
            {
                "user_id": user.get("id"),
                "op": operation,
                "db_to_debezium_ms": db_to_debezium_ms,
                "kafka_to_consumer_ms": kafka_to_consumer_ms,
                "mysql_total_ms": mysql_timing["total_ms"],
                "processed_at_ms": received_at_ms,
            }
        )

        return

    if operation == "d":
        before = payload.get("before")

        if not before:
            print(
                "[WARN] delete event has no 'before'"
            )
            return

        delete_user(before["id"])

        return

    print(
        f"[WARN] Unknown Debezium operation: {operation}"
    )


# ============================================================
# Main consumer loop
# ============================================================

def main():

    print(
        f"[{CONSUMER_INSTANCE_ID}] Listening to Kafka topic: "
        f"{KAFKA_TOPIC}"
    )

    processed = 0
    started_at = time.perf_counter()

    for message in consumer:

        try:

            event = message.value

            process_event(event)

            # Commit only AFTER the destination write succeeded.
            # Async: a synchronous commit per message costs a broker
            # round trip and was the main throughput bottleneck
            # (~25 ms/message). At-least-once is preserved: if the
            # process dies before the async commit lands, the
            # messages are simply redelivered, and the idempotent
            # upsert makes that safe.
            consumer.commit_async()

            processed += 1

            if processed % 500 == 0:
                rate = processed / (
                    time.perf_counter() - started_at
                )
                print(
                    f"[{CONSUMER_INSTANCE_ID}] processed "
                    f"{processed:,} messages "
                    f"({rate:.0f} msg/sec)"
                )

        except Exception as exc:

            print(
                f"[{CONSUMER_INSTANCE_ID}] [ERROR] Failed processing "
                f"partition={message.partition} "
                f"offset={message.offset}: {exc}",
                file=sys.stderr,
            )

            # IMPORTANT:
            # Do not commit the Kafka offset.
            #
            # Kafka will redeliver the event after restart
            # / rebalance.
            #
            # This gives us at-least-once processing.

            raise


if __name__ == "__main__":
    main()
