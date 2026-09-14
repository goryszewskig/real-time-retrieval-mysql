# CDC-driven FULLTEXT search demo (MySQL variant)

> This is the MySQL variant of the original PostgreSQL → OpenSearch/Qdrant
> pipeline ([real-time-retrieval](https://github.com/goryszewskig/real-time-retrieval)).
> Source and destination are both MySQL 8.0.

A demo pipeline showing near-real-time replication from a MySQL 8.0 source
database into a separate MySQL 8.0 destination database via Debezium + Kafka,
with a FULLTEXT search query layer and load-testing tools for both the
write/CDC path and the read/search path. See [docs/architecture.md](docs/architecture.md)
for the full design and load-tested findings.

## Prerequisites

- Docker (for Kafka + Debezium + both MySQL 8.0 instances)
- Python 3

Everything else runs locally in docker-compose: no external databases,
search engines, or cloud services are needed.

## Setup

1. **One-time environment setup** — creates a virtualenv, installs
   dependencies, and scaffolds `.env`:

   ```bash
   bash setup.sh
   ```

2. **Fill in `.env`** — the scaffolded defaults already match the
   docker-compose services, so this works out of the box unless you
   changed credentials:

   | Variable | Purpose |
   |---|---|
   | `MYSQL_SOURCE_HOST`, `MYSQL_SOURCE_PORT`, `MYSQL_SOURCE_DB` | Source MySQL (usersdb, default `localhost:3306`) |
   | `MYSQL_SOURCE_USER`, `MYSQL_SOURCE_PASSWORD` | Role with `UPDATE` on `users`, used only by the write benchmark |
   | `MYSQL_SOURCE_ADMIN_USER`, `MYSQL_SOURCE_ADMIN_PASSWORD` | Admin role, used only by the seed script |
   | `MYSQL_DEST_HOST`, `MYSQL_DEST_PORT`, `MYSQL_DEST_DB` | Destination MySQL (searchdb, default `localhost:3307`) |
   | `MYSQL_DEST_USER`, `MYSQL_DEST_PASSWORD` | Consumer/query role |
   | `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID` | Kafka connection |
   | `CDC_TIMINGS_LOG` | Path to the consumer's stage-latency log (default `logs/cdc_timings.jsonl`) |

3. **Start Kafka, Debezium, and both MySQL instances**:

   ```bash
   docker compose -f kafka/docker-compose.yml up -d
   ```

   On first start, MySQL runs the init SQL in `kafka/mysql/source-init/`
   and `kafka/mysql/dest-init/` (creates the databases, tables, FULLTEXT
   index, and users).

4. **Seed the source database**:

   ```bash
   python scripts/seed_source.py
   ```

5. **Register the Debezium connector**:

   ```bash
   curl -X POST -H "Content-Type: application/json" \
     --data @kafka/connector/users-connector.json \
     http://localhost:8083/connectors
   ```

6. **Run the consumer**:

   ```bash
   bash run/consumer.sh                  # single instance
   bash run/consumer.sh --instances 3    # one per Kafka partition
   ```

7. **Run the benchmarks**:

   ```bash
   bash run/write-benchmarking.sh --rate 10 --duration 60 --concurrency 20
   bash run/read-benchmarking.sh  --rate 10 --duration 60 --concurrency 20
   ```

8. **Try an ad-hoc FULLTEXT search**:

   ```bash
   python search/query.py "backend engineer kafka"
   ```
