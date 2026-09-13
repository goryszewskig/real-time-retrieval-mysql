#!/usr/bin/env python3

import os
import time

from opensearchpy import OpenSearch
from qdrant_client import QdrantClient, models


# ============================================================
# Configuration
# ============================================================

OPENSEARCH_INDEX = "users"
QDRANT_COLLECTION = "users_semantic"

TOP_K_RETRIEVAL = 50
TOP_K_FINAL = 10

BM25_WEIGHT = 0.6
SEMANTIC_WEIGHT = 0.4

RRF_K = 60

EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


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


OPENSEARCH_HOST = get_env("OPENSEARCH_HOST")
OPENSEARCH_USERNAME = get_env("OPENSEARCH_USERNAME")
OPENSEARCH_PASSWORD = get_env("OPENSEARCH_PASSWORD")

QDRANT_URL = get_env("QDRANT_URL")
QDRANT_API_KEY = get_env("QDRANT_API_KEY")


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
# BM25
# ============================================================

def search_opensearch(query):
    """
    Execute lexical BM25 retrieval.

    Returns the top TOP_K_RETRIEVAL documents ranked by BM25.
    """

    start = time.perf_counter()

    response = opensearch.search(
        index=OPENSEARCH_INDEX,
        body={
            "size": TOP_K_RETRIEVAL,

            "_source": [
                "id",
                "name",
                "job_title",
                "company",
                "location",
                "skills",
                "bio",
                "experience",
            ],

            "query": {
                "multi_match": {
                    "query": query,
                    "fields": [
                        "job_title",
                        "skills",
                        "bio",
                        "company",
                        "location",
                    ],
                }
            },
        },
    )

    latency_ms = (
        time.perf_counter() - start
    ) * 1000

    results = []

    for rank, hit in enumerate(
        response["hits"]["hits"],
        start=1,
    ):

        source = hit["_source"]

        results.append(
            {
                "id": str(source["id"]),
                "rank": rank,
                "bm25_score": hit["_score"],
                "source": source,
            }
        )

    return results, latency_ms


# ============================================================
# Semantic search
# ============================================================

def search_qdrant(query):
    """
    Execute semantic vector retrieval using Qdrant Cloud
    Inference.

    Returns the top TOP_K_RETRIEVAL documents ranked by
    cosine similarity.
    """

    start = time.perf_counter()

    response = qdrant.query_points(
        collection_name=QDRANT_COLLECTION,

        query=models.Document(
            text=query,
            model=EMBEDDING_MODEL,
        ),

        limit=TOP_K_RETRIEVAL,

        with_payload=True,
    )

    latency_ms = (
        time.perf_counter() - start
    ) * 1000

    results = []

    for rank, point in enumerate(
        response.points,
        start=1,
    ):

        results.append(
            {
                "id": str(point.id),
                "rank": rank,
                "semantic_score": point.score,
                "payload": point.payload,
            }
        )

    return results, latency_ms


# ============================================================
# Weighted Reciprocal Rank Fusion
# ============================================================

def weighted_rrf(
    opensearch_results,
    qdrant_results,
    bm25_weight=BM25_WEIGHT,
    semantic_weight=SEMANTIC_WEIGHT,
    k=RRF_K,
):
    """
    Weighted Reciprocal Rank Fusion.

    score(d) =
        bm25_weight    * 1 / (k + bm25_rank)
      + semantic_weight * 1 / (k + semantic_rank)

    If a document appears in only one retrieval result,
    it receives only that retriever's contribution.
    """

    scores = {}

    # --------------------------------------------------------
    # BM25 contribution
    # --------------------------------------------------------

    for result in opensearch_results:

        doc_id = result["id"]

        rrf_score = (
            1.0
            / (k + result["rank"])
        )

        if doc_id not in scores:

            scores[doc_id] = {
                "id": doc_id,
                "bm25_rank": None,
                "semantic_rank": None,
                "bm25_rrf": 0.0,
                "semantic_rrf": 0.0,
                "source": result["source"],
            }

        scores[doc_id]["bm25_rank"] = (
            result["rank"]
        )

        scores[doc_id]["bm25_rrf"] = (
            bm25_weight * rrf_score
        )

    # --------------------------------------------------------
    # Semantic contribution
    # --------------------------------------------------------

    for result in qdrant_results:

        doc_id = result["id"]

        rrf_score = (
            1.0
            / (k + result["rank"])
        )

        if doc_id not in scores:

            scores[doc_id] = {
                "id": doc_id,
                "bm25_rank": None,
                "semantic_rank": None,
                "bm25_rrf": 0.0,
                "semantic_rrf": 0.0,
                "payload": result["payload"],
            }

        scores[doc_id]["semantic_rank"] = (
            result["rank"]
        )

        scores[doc_id]["semantic_rrf"] = (
            semantic_weight * rrf_score
        )

        # Prefer OpenSearch's source if available.
        if "source" not in scores[doc_id]:
            scores[doc_id]["payload"] = (
                result["payload"]
            )

    # --------------------------------------------------------
    # Final score
    # --------------------------------------------------------

    for result in scores.values():

        result["rrf_score"] = (
            result["bm25_rrf"]
            + result["semantic_rrf"]
        )

        if (
            result["bm25_rank"] is not None
            and result["semantic_rank"] is not None
        ):
            result["retrieved_by"] = "both"

        elif result["bm25_rank"] is not None:
            result["retrieved_by"] = "bm25"

        else:
            result["retrieved_by"] = "semantic"

    return sorted(
        scores.values(),
        key=lambda x: x["rrf_score"],
        reverse=True,
    )


# ============================================================
# Hybrid query
# ============================================================

def query(query_text):
    """
    Execute:

        Query
          ├── BM25 → top 50
          └── Semantic → top 50
                    ↓
               Weighted RRF
                    ↓
                 top 10

    Returns the final results and latency information.
    """

    # --------------------------------------------------------
    # BM25
    # --------------------------------------------------------

    bm25_results, bm25_latency_ms = (
        search_opensearch(query_text)
    )

    # --------------------------------------------------------
    # Semantic
    # --------------------------------------------------------

    semantic_results, semantic_latency_ms = (
        search_qdrant(query_text)
    )

    # --------------------------------------------------------
    # RRF
    # --------------------------------------------------------

    rrf_start = time.perf_counter()

    hybrid_results = weighted_rrf(
        bm25_results,
        semantic_results,
        bm25_weight=BM25_WEIGHT,
        semantic_weight=SEMANTIC_WEIGHT,
        k=RRF_K,
    )

    rrf_latency_ms = (
        time.perf_counter() - rrf_start
    ) * 1000

    # --------------------------------------------------------
    # Top K
    # --------------------------------------------------------

    final_results = hybrid_results[:TOP_K_FINAL]

    total_latency_ms = (
        bm25_latency_ms
        + semantic_latency_ms
        + rrf_latency_ms
    )

    # --------------------------------------------------------
    # Retrieval overlap
    # --------------------------------------------------------

    bm25_ids = {
        result["id"]
        for result in bm25_results
    }

    semantic_ids = {
        result["id"]
        for result in semantic_results
    }

    overlap = (
        bm25_ids & semantic_ids
    )

    overlap_count = len(overlap)

    overlap_percentage = (
        overlap_count
        / TOP_K_RETRIEVAL
        * 100
    )

    return {
        "query": query_text,

        "bm25_results": bm25_results,
        "semantic_results": semantic_results,

        "hybrid_results": final_results,

        "latency": {
            "bm25_ms": bm25_latency_ms,
            "semantic_ms": semantic_latency_ms,
            "rrf_ms": rrf_latency_ms,
            "total_ms": total_latency_ms,
        },

        "overlap": {
            "count": overlap_count,
            "percentage": overlap_percentage,
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
        f"BM25:       {latency['bm25_ms']:.2f} ms"
    )

    print(
        f"Semantic:   {latency['semantic_ms']:.2f} ms"
    )

    print(
        f"RRF:        {latency['rrf_ms']:.2f} ms"
    )

    print(
        f"Total:      {latency['total_ms']:.2f} ms"
    )

    overlap = result["overlap"]

    print()
    print("Retriever overlap")
    print("-" * 80)

    print(
        f"Top-{TOP_K_RETRIEVAL} overlap: "
        f"{overlap['count']} "
        f"({overlap['percentage']:.1f}%)"
    )

    print()
    print(
        f"Top {TOP_K_FINAL} hybrid results"
    )
    print("-" * 80)

    for rank, item in enumerate(
        result["hybrid_results"],
        start=1,
    ):

        print(
            f"{rank:2d}. "
            f"id={item['id']} "
            f"rrf={item['rrf_score']:.6f} "
            f"source={item['retrieved_by']} "
            f"bm25_rank={item['bm25_rank']} "
            f"semantic_rank={item['semantic_rank']}"
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
        description="Hybrid BM25 + semantic search using weighted RRF."
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