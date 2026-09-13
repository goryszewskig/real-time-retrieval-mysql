=================================================================
READ / HYBRID SEARCH BENCHMARK REPORT
=================================================================
Duration:          122.68 seconds
Requested rate:    100.00 queries/sec
Dispatched:        1,469 (11.97 queries/sec)
Completed:         1,219 (9.94 queries/sec)
Errors:            250

OpenSearch / BM25 latency (total round-trip, network + server)
----------------------------------------------------------------
count: 1,219
p95:   6998.25 ms
p99:   7634.46 ms
max:   8209.14 ms

Qdrant / semantic latency (total round-trip, network + server)
----------------------------------------------------------------
count: 1,219
p95:   630.21 ms
p99:   2107.44 ms
max:   4140.56 ms

RRF computation latency
------------------------
count: 1,219
p95:   0.24 ms
p99:   0.29 ms
max:   1.58 ms

End-to-end hybrid latency
--------------------------
count: 1,219
p95:   7616.90 ms
p99:   8517.81 ms
max:   10454.31 ms

BM25 / semantic top-50 overlap
-------------------------------
count: 1,219
p95:   40.00 ms
p99:   40.00 ms
max:   40.00 ms

=================================================================
INTERPRETATION
=================================================================
Requested vs achieved rate is the headline finding here, and it's
not a tooling problem: dispatched only reached ~12/sec against a
100/sec request, with 250 read-timeout errors (~17% of dispatched
attempts). Concurrency was set to 50 for this run; that made things
WORSE, not better, because the bottleneck is OpenSearch's own
capacity to serve concurrent BM25 queries, not insufficient
dispatch concurrency - raising concurrency further pushes more
simultaneous load at an already-saturated cluster. This matches
(and extends) the same finding from an earlier, smaller run at
concurrency=20, where OpenSearch also degraded sharply under
concurrent load while Qdrant stayed comparatively healthy - true
again here (Qdrant p95 630ms vs OpenSearch p95 6998ms).

Practical read: at whatever concurrency this cluster can actually
absorb (untested here, but well under 20 based on prior results),
BM25 latency is fast (low hundreds of ms). Past that point,
latency and error rate both blow out together. If sustaining 100
qps end-to-end is a real requirement, that requires the OpenSearch
domain itself to be resized (a cluster capacity/cost decision, not
a code fix) - not something addressed by this benchmarking tool.

Unlike the write-side report, no network-vs-server split is given
here: search/query.py's BM25/semantic latencies are total client
round-trip time only - that split was purpose-built for the
consumer's writes (via OpenSearch's bulk `took` and Qdrant's raw
`time` field) and was never added to the read path. The numbers
above are what's actually measured today, not an approximation of
a split that doesn't exist yet.
=================================================================
