# CDC-driven hybrid search demo

A demo pipeline showing near-real-time indexing from PostgreSQL into
OpenSearch (lexical/BM25) and Qdrant (semantic/vector) via Debezium + Kafka,
with a hybrid-search query layer and load-testing tools for both the
write/CDC path and the read/search path. See [docs/architecture.md](docs/architecture.md)
for the full design and load-tested findings.

## Prerequisites

- Docker (for Kafka + Debezium)
- Python 3
- An OpenSearch domain
- A Qdrant instance (Cloud or self-hosted)
- A PostgreSQL database with logical replication enabled

## Setup

1. **One-time environment setup** — creates a virtualenv, installs
   dependencies, and scaffolds `.env`:

   ```bash
   bash setup.sh
   ```

2. **Fill in `.env`** with real credentials (never commit real values):

   | Variable | Purpose |
   |---|---|
   | `OPENSEARCH_HOST`, `OPENSEARCH_USERNAME`, `OPENSEARCH_PASSWORD` | OpenSearch domain |
   | `QDRANT_URL`, `QDRANT_API_KEY` | Qdrant instance |
   | `PGHOST`, `PGPORT`, `PGDATABASE` | PostgreSQL connection |
   | `PGUSER`, `PGPASSWORD` | Debezium's replication role (read-only) |
   | `PGWRITEUSER`, `PGWRITEPASSWORD` | A separate role with `UPDATE` on `users`, used only by the write benchmark |
   | `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID` | Kafka connection |
   | `CDC_TIMINGS_LOG` | Path to the consumer's stage-latency log (default `logs/cdc_timings.jsonl`) |

3. **Start Kafka + Debezium**:

   ```bash
   docker compose -f kafka/docker-compose.yml up -d
   ```

4. **Register the Debezium connector**:

   ```bash
   curl -X POST -H "Content-Type: application/json" \
     --data @connector/users-connector.json \
     http://localhost:8083/connectors
   ```

5. **Run the consumer**:

   ```bash
   bash run/consumer.sh                  # single instance
   bash run/consumer.sh --instances 3    # one per Kafka partition
   ```

6. **Run the benchmarks**:

   ```bash
   bash run/write-benchmarking.sh --rate 10 --duration 60 --concurrency 20
   bash run/read-benchmarking.sh  --rate 10 --duration 60 --concurrency 20
   ```

   See `docs/write-benchmark.md` and `docs/read-benchmarking.md` for example
   reports (100 updates/sec and 100 queries/sec runs).

7. **Try an ad-hoc hybrid search**:

   ```bash
   python search/query.py "backend engineer kafka"
   ```
