#!/usr/bin/env python3

import os
import threading
import time

import pymysql


# ============================================================
# Configuration
# ============================================================

TOP_K_FINAL = 10

FULLTEXT_COLUMNS = (
    "job_title, skills, bio, company, location"
)


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


MYSQL_DEST_HOST = os.getenv("MYSQL_DEST_HOST", "localhost")
MYSQL_DEST_PORT = int(os.getenv("MYSQL_DEST_PORT", "3307"))
MYSQL_DEST_DB = os.getenv("MYSQL_DEST_DB", "searchdb")
MYSQL_DEST_USER = get_env("MYSQL_DEST_USER")
MYSQL_DEST_PASSWORD = get_env("MYSQL_DEST_PASSWORD")


# ============================================================
# Connection
#
# PyMySQL connections are not thread-safe, and the read benchmark
# calls query() concurrently from a worker pool - so each thread
# gets its own connection via thread-local storage.
# ============================================================

_thread_local = threading.local()


def get_connection():
    connection = getattr(_thread_local, "connection", None)

    if connection is None:
        connection = pymysql.connect(
            host=MYSQL_DEST_HOST,
            port=MYSQL_DEST_PORT,
            user=MYSQL_DEST_USER,
            password=MYSQL_DEST_PASSWORD,
            database=MYSQL_DEST_DB,
            autocommit=True,
        )
        _thread_local.connection = connection
    else:
        try:
            connection.ping()
        except Exception:
            connection = pymysql.connect(
                host=MYSQL_DEST_HOST,
                port=MYSQL_DEST_PORT,
                user=MYSQL_DEST_USER,
                password=MYSQL_DEST_PASSWORD,
                database=MYSQL_DEST_DB,
                autocommit=True,
            )
            _thread_local.connection = connection

    return connection


# ============================================================
# FULLTEXT search
# ============================================================

def search_fulltext(query_text, limit=TOP_K_FINAL):
    """
    Execute MySQL FULLTEXT retrieval (natural-language mode) over
    job_title, skills, bio, company, location.

    Returns the top `limit` documents ranked by relevance score.
    """

    start = time.perf_counter()

    connection = get_connection()

    with connection.cursor(
        pymysql.cursors.DictCursor
    ) as cursor:

        cursor.execute(
            f"""
            SELECT
                id, name, email, company, job_title, location,
                skills, bio, experience,
                MATCH({FULLTEXT_COLUMNS})
                    AGAINST (%s IN NATURAL LANGUAGE MODE) AS score
            FROM users
            WHERE MATCH({FULLTEXT_COLUMNS})
                AGAINST (%s IN NATURAL LANGUAGE MODE)
            ORDER BY score DESC
            LIMIT %s
            """,
            (query_text, query_text, limit),
        )

        rows = cursor.fetchall()

    latency_ms = (
        time.perf_counter() - start
    ) * 1000

    results = []

    for rank, row in enumerate(rows, start=1):

        results.append(
            {
                "id": str(row["id"]),
                "rank": rank,
                "score": float(row["score"]),
                "source": row,
            }
        )

    return results, latency_ms


# ============================================================
# Query
# ============================================================

def query(query_text):
    """
    Execute a FULLTEXT query against the destination MySQL and
    return the top TOP_K_FINAL results with latency information.

    The return shape mirrors the original hybrid pipeline (latency
    dict + results list) so benchmarking/read-benchmarking.py keeps
    working unchanged; the semantic/RRF stages no longer exist, so
    their latency fields are reported as 0.
    """

    fulltext_results, fulltext_latency_ms = (
        search_fulltext(query_text)
    )

    # Same item shape the original RRF fusion produced, so the CLI
    # printer and benchmarks keep working.
    final_results = [
        {
            "id": result["id"],
            "rrf_score": result["score"],
            "retrieved_by": "fulltext",
            "bm25_rank": result["rank"],
            "semantic_rank": None,
            "source": result["source"],
        }
        for result in fulltext_results
    ]

    return {
        "query": query_text,

        "fulltext_results": fulltext_results,

        "hybrid_results": final_results,

        "latency": {
            "bm25_ms": fulltext_latency_ms,
            "semantic_ms": 0.0,
            "rrf_ms": 0.0,
            "total_ms": fulltext_latency_ms,
        },

        "overlap": {
            "count": 0,
            "percentage": 0.0,
        },
    }


# ============================================================
# CLI
# ============================================================

def print_results(result):

    print()
    print("=" * 80)
    print(
        f"QUERY: {result['query']}"
    )
    print("=" * 80)

    latency = result["latency"]

    print()
    print("Latency")
    print("-" * 80)

    print(
        f"FULLTEXT:   {latency['bm25_ms']:.2f} ms"
    )

    print()
    print(
        f"Top {TOP_K_FINAL} results"
    )
    print("-" * 80)

    for rank, item in enumerate(
        result["hybrid_results"],
        start=1,
    ):

        print(
            f"{rank:2d}. "
            f"id={item['id']} "
            f"score={item['rrf_score']:.6f}"
        )

        source = item.get("source")

        if source:

            print(
                f"    {source.get('name')} | "
                f"{source.get('job_title')} | "
                f"{source.get('company')}"
            )


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(
        description="MySQL FULLTEXT search over the users table."
    )

    parser.add_argument(
        "query",
        nargs="?",
        default="backend engineer kafka",
        help="Search query.",
    )

    args = parser.parse_args()

    result = query(args.query)

    print_results(result)
