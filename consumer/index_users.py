import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, wait

import httpx
from kafka import KafkaConsumer
from opensearchpy import OpenSearch
from qdrant_client import QdrantClient, models


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "usersdb.public.users",
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

OPENSEARCH_HOST = os.environ["OPENSEARCH_HOST"]
OPENSEARCH_USERNAME = os.environ["OPENSEARCH_USERNAME"]
OPENSEARCH_PASSWORD = os.environ["OPENSEARCH_PASSWORD"]

OPENSEARCH_INDEX = "users"

QDRANT_URL = os.environ["QDRANT_URL"]
QDRANT_API_KEY = os.environ["QDRANT_API_KEY"]
QDRANT_COLLECTION = "users_semantic"

EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

CDC_TIMINGS_LOG = os.getenv(
    "CDC_TIMINGS_LOG",
    "logs/cdc_timings.jsonl",
)


# ============================================================
# Clients
# ============================================================

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
    cloud_inference=True,
)


# ============================================================
# Qdrant server-side timing capture
#
# The high-level qdrant-client response objects don't expose the
# raw REST envelope's "time" field (server-side embedding + HNSW
# time, in seconds), so it's captured here via an httpx response
# hook instead of hand-rolling the request ourselves.
# ============================================================

_qdrant_last_server_ms = {"value": None}

_original_httpx_send = httpx.Client.send


def _capture_qdrant_timing(self, request, *args, **kwargs):
    response = _original_httpx_send(self, request, *args, **kwargs)

    if "/points" in str(request.url):
        try:
            body = json.loads(response.read())
            _qdrant_last_server_ms["value"] = (
                body.get("time", 0) * 1000
            )
        except Exception:
            _qdrant_last_server_ms["value"] = None

    return response


httpx.Client.send = _capture_qdrant_timing


# OpenSearch and Qdrant writes are independent of each other, so
# they run concurrently rather than one after another. Only one
# message is processed at a time, so at most one opensearch call
# and one qdrant call are ever in flight together (never two of
# the same kind at once) - see index_qdrant's use of
# _qdrant_last_server_ms above for why that matters.
_write_executor = ThreadPoolExecutor(
    max_workers=2,
    thread_name_prefix="cdc-write",
)


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

def build_search_text(user):
    """
    Build the semantic representation of a user.

    Keep this consistent with the original Qdrant
    indexing script.
    """

    fields = [
        user.get("name"),
        user.get("job_title"),
        user.get("company"),
        user.get("location"),
        user.get("skills"),
        user.get("bio"),
    ]

    return " ".join(
        str(value)
        for value in fields
        if value
    )


def index_opensearch(user):
    """
    Upsert the PostgreSQL row into OpenSearch.

    PostgreSQL users.id is deliberately used as the
    OpenSearch document ID.

    Uses the Bulk API (for a single document) rather than the
    plain Index API, because only Bulk responses include a
    "took" field (server-side indexing time, ms) - the signal
    used to split network overhead from actual indexing time.
    """

    document_id = str(user["id"])

    total_start = time.perf_counter()

    print(OPENSEARCH_INDEX, document_id, user)

    response = opensearch.bulk(
        body=[
            {
                "index": {
                    "_index": OPENSEARCH_INDEX,
                    "_id": document_id,
                }
            },
            user,
        ],
        refresh=False,
    )

    total_ms = (time.perf_counter() - total_start) * 1000
    server_ms = response.get("took")

    print(
        f"[OpenSearch] upserted user={document_id}"
    )

    return {
        "total_ms": total_ms,
        "server_ms": server_ms,
        "network_ms": (
            (total_ms - server_ms)
            if server_ms is not None
            else None
        ),
    }


def index_qdrant(user):
    """
    Generate the embedding and upsert the point into Qdrant.

    PostgreSQL users.id is used as the Qdrant point ID.
    """

    document_id = int(user["id"])

    text = build_search_text(user)

    payload = {
        "id": document_id,
        "name": user.get("name"),
        "email": user.get("email"),
        "company": user.get("company"),
        "job_title": user.get("job_title"),
        "location": user.get("location"),
        "skills": user.get("skills"),
        "bio": user.get("bio"),
        "experience": user.get("experience"),
        "created_at": user.get("created_at"),
        "updated_at": user.get("updated_at"),
    }

    _qdrant_last_server_ms["value"] = None

    total_start = time.perf_counter()

    qdrant.upsert(
        collection_name=QDRANT_COLLECTION,
        points=[
            models.PointStruct(
                id=document_id,
                vector=models.Document(
                    text=text,
                    model=EMBEDDING_MODEL,
                ),
                payload=payload,
            )
        ],
    )

    total_ms = (time.perf_counter() - total_start) * 1000
    server_ms = _qdrant_last_server_ms["value"]

    print(
        f"[Qdrant] upserted user={document_id}"
    )

    return {
        "total_ms": total_ms,
        "server_ms": server_ms,
        "network_ms": (
            (total_ms - server_ms)
            if server_ms is not None
            else None
        ),
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
    Delete the user from both derived indexes.
    """

    document_id = str(user_id)

    opensearch.delete(
        index=OPENSEARCH_INDEX,
        id=document_id,
        ignore=[404],
        refresh=False,
    )

    qdrant.delete(
        collection_name=QDRANT_COLLECTION,
        points_selector=models.PointIdsList(
            points=[int(user_id)]
        ),
    )

    print(
        f"[DELETE] user={document_id}"
    )


# ============================================================
# Debezium event processing
# ============================================================

def process_event(event):
    """
    Debezium PostgreSQL event structure:

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
    print(operation)

    if operation in ("c", "u", "r"):
        user = payload.get("after")

        if not user:
            print(
                f"[WARN] {operation} event has no 'after'"
            )
            return

        opensearch_future = _write_executor.submit(
            index_opensearch, user
        )
        qdrant_future = _write_executor.submit(
            index_qdrant, user
        )

        wait([opensearch_future, qdrant_future])

        opensearch_timing = opensearch_future.result()
        qdrant_timing = qdrant_future.result()

        log_timing(
            {
                "user_id": user.get("id"),
                "op": operation,
                "db_to_debezium_ms": db_to_debezium_ms,
                "kafka_to_consumer_ms": kafka_to_consumer_ms,
                "opensearch_total_ms": opensearch_timing["total_ms"],
                "opensearch_server_ms": opensearch_timing["server_ms"],
                "opensearch_network_ms": opensearch_timing["network_ms"],
                "qdrant_total_ms": qdrant_timing["total_ms"],
                "qdrant_server_ms": qdrant_timing["server_ms"],
                "qdrant_network_ms": qdrant_timing["network_ms"],
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

    for message in consumer:

        print(
            "\n--------------------------------------------------"
        )

        print(
            f"[{CONSUMER_INSTANCE_ID}] Kafka "
            f"partition={message.partition} "
            f"offset={message.offset}"
        )

        try:

            event = message.value

            process_event(event)

            # Commit ONLY after both indexes succeeded.
            consumer.commit()

            print(
                f"[{CONSUMER_INSTANCE_ID}] [Kafka] "
                f"committed offset={message.offset}"
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
