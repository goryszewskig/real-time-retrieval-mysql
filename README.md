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

## Operating Kafka (DevOps guide)

Everything below uses the compose stack's real names: broker container
`cdc-kafka` (bootstrap `localhost:9092` inside the container, `localhost:9094`
from the host), Connect REST on `http://localhost:8083`, CDC topic
`usersdb.usersdb.users`, consumer group `users-indexer`.

### Daily health checks

```bash
# 1. Are all four containers up and healthy?
docker compose -f kafka/docker-compose.yml ps

# 2. Is the connector running? (state should be RUNNING for connector AND task)
curl -s http://localhost:8083/connectors/users-cdc/status

# 3. Is the consumer keeping up? (LAG should trend to 0)
docker exec cdc-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group users-indexer

# 4. Broker alive and topics present?
docker exec cdc-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

### Common scenarios

**Watch CDC events live** (debug what Debezium is actually publishing):

```bash
docker exec -it cdc-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic usersdb.usersdb.users --from-beginning --max-messages 5
```

**Connector is FAILED or UNASSIGNED** — get the error trace, then restart:

```bash
curl -s http://localhost:8083/connectors/users-cdc/status | python -m json.tool
curl -X POST http://localhost:8083/connectors/users-cdc/restart
```

Typical causes: MySQL source restarted (binlog position moved), wrong
credentials, or the source wasn't healthy when the connector registered.

**Re-run the initial snapshot** (destination drifted, or you reseeded the
source). Debezium stores its binlog offset inside Kafka, so the connector
must be deleted *and* Kafka state wiped, otherwise it resumes where it
left off and silently skips the snapshot:

```bash
curl -X DELETE http://localhost:8083/connectors/users-cdc
docker compose -f kafka/docker-compose.yml stop debezium kafka
docker rm cdc-kafka cdc-debezium
docker volume rm kafka_kafka-data
docker compose -f kafka/docker-compose.yml up -d kafka debezium
curl -X POST -H "Content-Type: application/json" \
  --data @kafka/connector/users-connector.json \
  http://localhost:8083/connectors
```

Then truncate the destination and restart the consumer (its group offsets
lived in the wiped Kafka, so it re-reads from `earliest` automatically).

**Scale the consumer** — the topic has 3 partitions, so up to 3 instances
help; a 4th sits idle:

```bash
bash run/consumer.sh --instances 3
```

**Restart order matters**: `mysql-source` → `kafka` → `debezium` → host
consumer. `docker compose up -d` already enforces the container part via
`depends_on`/healthchecks; only restart the host consumer last.

### Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `curl :8083/connectors` connection refused | Connect still booting (it takes ~30-60s) | Wait, watch `docker logs -f cdc-debezium` |
| Connector RUNNING but no events in Kafka | Registered *after* the writes happened, or offsets from a previous life | Re-register with a wiped Kafka (snapshot procedure above) |
| Consumer: `NoBrokersAvailable` | Host client pointing at `kafka:9092` (unresolvable from host) | Use `localhost:9094` in `KAFKA_BOOTSTRAP_SERVERS` |
| Consumer lag grows forever | Fewer consumer instances than partitions, or consumer crashed | Check process; scale to `--instances 3`; check `logs/consumer*.log` |
| Debezium task FAILED with binlog error | `mysql-source` was recreated (`down -v` or new volume) | Wipe Kafka + re-register (offset points at a binlog file that no longer exists) |
| Destination row count != source | Consumer crashed mid-stream, or manual writes to destination | Restart consumer (at-least-once + idempotent upserts self-heal); re-snapshot if drift is large |
| Port 3306/3307/8083/9094 already in use | Another local MySQL/Kafka project | `docker ps` to find it; change the host-side port mapping in compose |
| Container name conflict on `up` | Stale container from another project (`mysql-source`, `debezium`, ...) | Compose uses `cdc-*` names to avoid this; remove/renamed the *other* project's container if you hit it anyway |

### Full teardown / reset

```bash
# Stop everything, keep data volumes:
docker compose -f kafka/docker-compose.yml down

# Stop everything AND wipe all data (Kafka, both MySQLs) - clean slate:
docker compose -f kafka/docker-compose.yml down -v
```

After `down -v`, follow Setup steps 3-6 again (MySQL init SQL re-runs on
fresh volumes, then reseed, then re-register the connector).
