#!/usr/bin/env python3

import argparse
import random
import statistics
import threading

from query import query
from load_generator import run_open_loop


# ============================================================
# Defaults
# ============================================================

DEFAULT_RATE = 10
DEFAULT_DURATION = 60
DEFAULT_CONCURRENCY = 20

DEFAULT_QUERIES = [
    "backend engineer kafka",
    "python distributed systems",
    "senior software engineer aws",
    "machine learning engineer",
    "data engineer spark kafka",
    "backend developer postgres",
    "cloud engineer kubernetes",
    "software engineer microservices",
    "java backend engineer",
    "python api developer",
]


# ============================================================
# Arguments
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Benchmark MySQL FULLTEXT search."
        )
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=DEFAULT_RATE,
        help=(
            f"Queries per second. "
            f"Default: {DEFAULT_RATE}"
        ),
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION,
        help=(
            f"Benchmark duration in seconds. "
            f"Default: {DEFAULT_DURATION}"
        ),
    )

    parser.add_argument(
        "--queries",
        default=None,
        help=(
            "Optional file containing queries, "
            "one query per line."
        ),
    )

    parser.add_argument(
        "--warmup",
        type=int,
        default=10,
        help=(
            "Number of warmup queries before measurement. "
            "Default: 10"
        ),
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=(
            "Max concurrent in-flight queries. Default: "
            f"{DEFAULT_CONCURRENCY}. Raise this if the achieved "
            "rate falls short of --rate."
        ),
    )

    return parser.parse_args()


# ============================================================
# Query loading
# ============================================================

def load_queries(path):

    if path is None:
        return DEFAULT_QUERIES

    with open(
        path,
        "r",
        encoding="utf-8",
    ) as f:

        queries = [
            line.strip()
            for line in f
            if line.strip()
        ]

    if not queries:
        raise RuntimeError(
            f"No queries found in {path}"
        )

    return queries


# ============================================================
# Statistics
# ============================================================

def percentile(values, percentile):

    if not values:
        return None

    values = sorted(values)

    index = (
        (len(values) - 1)
        * percentile
        / 100
    )

    lower = int(index)
    upper = min(
        lower + 1,
        len(values) - 1,
    )

    if lower == upper:
        return values[lower]

    weight = index - lower

    return (
        values[lower] * (1 - weight)
        + values[upper] * weight
    )


def print_metric(name, values):

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


# ============================================================
# Benchmark
# ============================================================

def run_benchmark(
    queries,
    rate,
    duration,
    warmup,
    concurrency,
):

    # --------------------------------------------------------
    # Warmup
    # --------------------------------------------------------

    print()
    print(
        f"Running {warmup} warmup queries..."
    )

    for i in range(warmup):

        query_text = random.choice(queries)

        try:
            query(query_text)
        except Exception as exc:
            print(
                f"[WARN] Warmup query failed: {exc}"
            )

    print("Warmup complete.")

    # --------------------------------------------------------
    # Measurement state
    #
    # search/query.py keeps one PyMySQL connection per worker
    # thread (thread-local), safe to call concurrently - like
    # write-benchmarking.py's per-thread connections, no extra
    # setup is needed here. These lists are appended to from
    # worker threads though, so every access is guarded by
    # state_lock.
    # --------------------------------------------------------

    state_lock = threading.Lock()

    fulltext_latencies = []
    total_latencies = []

    errors = 0

    print()
    print("Starting read benchmark")
    print(
        f"Rate:        {rate:.2f} queries/sec"
    )
    print(
        f"Duration:    {duration} seconds"
    )
    print(
        f"Concurrency: {concurrency}"
    )
    print(
        f"Queries:     {len(queries)}"
    )
    print(
        "Retrieval:   MySQL FULLTEXT (natural language)"
    )
    print()

    # --------------------------------------------------------
    # Main benchmark loop (open-loop: dispatched on schedule by
    # run_open_loop, not gated on the previous query completing)
    # --------------------------------------------------------

    def _one_query():
        query_text = random.choice(queries)
        return query_text, query(query_text)

    def _on_success(result):
        query_text, result_dict = result

        latency = result_dict["latency"]

        with state_lock:
            fulltext_latencies.append(latency["bm25_ms"])
            total_latencies.append(latency["total_ms"])

    def _on_error(exc):
        nonlocal errors

        with state_lock:
            errors += 1

        print(
            f"[ERROR] Query failed: {exc}"
        )

    dispatched, actual_duration = run_open_loop(
        rate=rate,
        duration=duration,
        task_fn=_one_query,
        on_success=_on_success,
        on_error=_on_error,
        max_concurrency=concurrency,
    )

    return {
        "duration": actual_duration,
        "dispatched": dispatched,
        "queries": len(total_latencies),
        "errors": errors,
        "fulltext": fulltext_latencies,
        "total": total_latencies,
    }


# ============================================================
# Report
# ============================================================

def print_report(
    result,
    target_rate,
):

    duration = result["duration"]
    dispatched = result["dispatched"]
    queries = result["queries"]
    errors = result["errors"]

    dispatched_rate = (
        dispatched / duration
        if duration > 0
        else 0
    )

    completed_rate = (
        queries / duration
        if duration > 0
        else 0
    )

    print()
    print("=" * 70)
    print("READ / FULLTEXT SEARCH BENCHMARK")
    print("=" * 70)

    print(
        f"Requested rate:    {target_rate:.2f} queries/sec"
    )

    print(
        f"Dispatched:        {dispatched:,} "
        f"({dispatched_rate:.2f} queries/sec)"
    )

    print(
        f"Completed:         {queries:,} "
        f"({completed_rate:.2f} queries/sec)"
    )

    print(
        f"Duration:          {duration:.2f} seconds"
    )

    print(
        f"Errors:            {errors:,}"
    )

    if dispatched_rate < target_rate * 0.9:
        print(
            "NOTE: dispatched rate fell short of the requested "
            "rate - --concurrency is too low for how long each "
            "query actually takes; raise it to dispatch faster."
        )

    print_metric(
        "MySQL FULLTEXT latency",
        result["fulltext"],
    )

    print_metric(
        "End-to-end query latency",
        result["total"],
    )

    print()
    print("=" * 70)


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

    queries = load_queries(
        args.queries
    )

    result = run_benchmark(
        queries=queries,
        rate=args.rate,
        duration=args.duration,
        warmup=args.warmup,
        concurrency=args.concurrency,
    )

    print_report(
        result,
        target_rate=args.rate,
    )


if __name__ == "__main__":
    main()