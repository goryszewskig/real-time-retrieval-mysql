# Repo Analysis — real-time-retrieval (MySQL variant)

Analyzed: 2026-09-13 (original); updated 2026-09-14 after the MySQL migration.

> **2026-09-14 update:** the pipeline was migrated end-to-end to MySQL 8.0 and
> published as a separate repo (`real-time-retrieval-mysql`). Source: MySQL 8.0
> binlog via Debezium `MySqlConnector` (`snapshot.mode: initial`). Destination:
> a second MySQL 8.0 instance with a FULLTEXT index (replacing OpenSearch +
> Qdrant; MySQL 8.0 has no vector type, so the semantic leg was dropped).
> Query layer: `MATCH ... AGAINST ... IN NATURAL LANGUAGE MODE`. Benchmarks now
> use PyMySQL (`UPDATE` + follow-up `SELECT` instead of Postgres `RETURNING`).
> New: `kafka/mysql/{source-init,dest-init}/01-init.sql` (schema, users,
> FULLTEXT index) and `scripts/seed_source.py` (CSV loader). Verified E2E on
> 2026-09-14: 10K-row snapshot replicated, UPDATE/DELETE CDC round-trips,
> FULLTEXT query, and both benchmarks (write: 51/51 replicated with stage
> breakdown; read: 51/51 queries, 0 errors).

## Original analysis (PostgreSQL → OpenSearch + Qdrant)

Single commit (`62eb47c`); no AGENTS.md, no tests, no CI.

## What this is

A **demo/learning pipeline for CDC-driven near-real-time hybrid search**. Every
change to a PostgreSQL `users` table (10K synthetic job-seeker rows in
`data/jobseekers_10000.csv`) is captured by Debezium from the WAL, streamed
through Kafka, and applied by a Python consumer to **two search backends** —
OpenSearch (lexical/BM25) and Qdrant (semantic vectors, server-side embeddings
via `all-MiniLM-L6-v2` + Qdrant Cloud Inference). A query layer
(`search/query.py`) fuses both with weighted Reciprocal Rank Fusion
(BM25 0.6 / semantic 0.4, k=60, top-50 → top-10). Both paths have open-loop
load benchmarks with detailed percentile reports (`docs/write-benchmark.md`,
`docs/read-benchmarking.md`).

## Structure

| Path | Role |
|---|---|
| `kafka/docker-compose.yml` | Single-node Kafka 4.0 (KRaft) + Debezium Connect 3.3. Dual listeners: `kafka:9092` internal, `localhost:9094` for host clients. 3 default partitions. |
| `kafka/connector/users-connector.json` | Debezium Postgres connector config (`pgoutput`, `no_data` snapshot, table `public.users`). Credentials intentionally blank. |
| `consumer/index_users.py` | The core service. Kafka consumer group `users-indexer`; at-least-once (offset committed only after both index writes succeed; idempotent upserts make redelivery safe). OpenSearch + Qdrant writes run concurrently on a 2-thread pool. Emits one JSONL stage-latency record per message to `logs/cdc_timings.jsonl`. |
| `search/query.py` | Hybrid retrieval: BM25 (multi_match over job_title/skills/bio/company/location) + Qdrant semantic, fused by weighted RRF. CLI entry point. |
| `benchmarking/load_generator.py` | Shared **open-loop** dispatcher: fixed-schedule issuing decoupled from call latency, semaphore-bounded concurrency, optional per-thread init. |
| `benchmarking/write-benchmarking.py` | Randomized UPDATE load against Postgres (per-thread psycopg2 conns, `updated_at = NOW()` inline — no DB trigger), plus a background verifier thread that polls both indexes until each update is observed, and folds in the consumer's timing log for a stage-by-stage breakdown. |
| `benchmarking/read-benchmarking.py` | Hybrid-query load with warmup, shared connection-pooled clients, percentile reporting incl. BM25/semantic overlap stats. |
| `setup.sh`, `run/*.sh` | venv + `.env` scaffolding; `run/consumer.sh --instances N` scales the consumer via Kafka group rebalancing (ceiling = 3 partitions). |

## Notable engineering quality

- **Deliberate measurement design.** Bulk API used (for single docs) purely to
  get OpenSearch's `took` field; an httpx monkey-patch captures Qdrant's raw
  REST `time` field — both to split network vs. server-side latency per stage.
- **Honest, load-tested limitations documented** in `docs/architecture.md`
  (findings from real 100 ops/sec runs, not speculation): consumer ceiling
  ~1–3 updates/sec per instance (Kafka backlog at 100 ups), OpenSearch
  timeouts above ~20 concurrent queries, no consumer retry/backoff (one
  transient error kills the process), minor Debezium↔Postgres clock skew, and
  a known-unfixed bug where `search/query.py` calls the two backends
  *sequentially* instead of concurrently.
- **Past pitfalls recorded**: an earlier closed-loop benchmark silently capped
  achievable rate; a DB-trigger assumption made replication checks pass on
  stale timestamps. Both fixed and documented.
- Comments throughout explain *why*, not *what* — unusually good
  context-preservation for a demo repo.

## Gaps / risks

- **No tests** despite `pytest` being in `requirements.txt`; no CI.
- **No retry/backoff or dead-lettering** in the consumer — a single transient
  error crashes it (documented, unfixed). Restart is manual.
- Sequential BM25→Qdrant calls in `query()` inflate latency and understate
  read-path pressure (documented, unfixed).
- Shell scripts (`setup.sh`, `run/*.sh`) are bash-only and use
  `.venv/bin/activate` — they won't work in native Windows PowerShell
  (this repo lives on `M:\` on Windows; Git Bash/WSL required).
- Docs reference `connector/users-connector.json`; actual path is
  `kafka/connector/users-connector.json` — README's curl command is stale.
- Docs mention `snapshot.mode: initial`; the actual connector config uses
  `no_data`.
- Benchmarks verify freshness by polling with `updated_at` comparisons, so
  verification granularity is limited by timestamp precision and clock skew.
- `.env`-based secrets; connector JSON has empty credential placeholders that
  must be filled by hand before registration.
