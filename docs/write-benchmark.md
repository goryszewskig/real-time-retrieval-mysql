=================================================================
WRITE / CDC BENCHMARK REPORT
=================================================================
Duration:          300.58 seconds
Requested rate:    100 updates/sec
Dispatched:        29,997 (99.80 updates/sec)
Completed:         29,997 (99.80 updates/sec)
Errors:            0

PostgreSQL write latency
------------------------
count: 29,997
p95:   632.92 ms
p99:   762.62 ms
max:   1134.83 ms

PostgreSQL -> OpenSearch replication latency
--------------------------------------------
count: 574
p95:   64922.41 ms
p99:   65431.88 ms
max:   65496.73 ms

PostgreSQL -> Qdrant replication latency
----------------------------------------
count: 574
p95:   64922.41 ms
p99:   65431.88 ms
max:   65496.73 ms

=================================================================
STAGE-BY-STAGE LATENCY BREAKDOWN
=================================================================

Postgres commit -> Debezium capture
-----------------------------------
count: 2,460
p95:   10063.00 ms
p99:   10906.02 ms
max:   11169.00 ms

Debezium -> consumer receive (Kafka)
------------------------------------
count: 2,460
p95:   292322.17 ms
p99:   302706.23 ms
max:   310250.76 ms

OpenSearch: network overhead
----------------------------
count: 2,460
p95:   447.09 ms
p99:   543.66 ms
max:   1253.18 ms

OpenSearch: server-side indexing (took)
---------------------------------------
count: 2,460
p95:   7.00 ms
p99:   14.00 ms
max:   126.00 ms

Qdrant: network overhead
------------------------
count: 2,460
p95:   197.22 ms
p99:   261.87 ms
max:   738.92 ms

Qdrant: server-side embed + HNSW (time)
---------------------------------------
count: 2,460
p95:   187.68 ms
p99:   192.16 ms
max:   568.34 ms

Replication coverage
--------------------
OpenSearch observed: 574 / 29,997
Qdrant observed:     574 / 29,997

=================================================================
NOTE ON SAMPLE SIZE (CDC-side sections only)
=================================================================
Postgres write latency (29,997 samples) and the requested-vs-
achieved rate are full-population and reliable.

The replication-latency and stage-breakdown sections are NOT: at
100 updates/sec sustained for 5 minutes, the 3 consumer instances
(the CDC pipeline's real throughput ceiling, established at
~1-3 updates/sec/instance in earlier runs) fall far behind - a
backlog builds for the entire run. Only whichever updates the
consumers had actually reached by the time this benchmark's fixed
post-run observation window closed are represented here (574 of
29,997 replication-confirmed; 2,460 of 29,997 in the stage log).
Later-dispatched updates, still queued in Kafka when this report
was generated, contribute zero data points here - not because they
failed, but because the pipeline hadn't gotten to them yet. Treat
the CDC-side p95/p99/max above as a lower bound sampled from the
front of a large, still-draining backlog, not the true steady-state
distribution at this rate.
=================================================================
