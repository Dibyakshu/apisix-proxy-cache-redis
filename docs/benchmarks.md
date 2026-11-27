# Benchmarks

## Method

- **Tool:** `wrk`, 1 thread, 50 connections (200 in scenario D), 10 seconds
  per run. Script: `tests/bench.py`, run by `tests/run.sh all`.
- **Gateway:** nginx + lua-nginx-module with **one worker**, running either
  this plugin or **APISIX's own `proxy-cache` plugin (memory strategy,
  unmodified)** for comparison. A separate nginx serves as the backend.
- **Redis:** 7.0 on the same machine: a single instance, a master + replica
  + 3 Sentinels, and a 3-master cluster.
- **Machine:** 2 vCPUs shared by wrk, the gateway, the backend and every
  Redis process. Absolute numbers are therefore low, and everything runs
  over localhost, so network round trips are almost free. Compare the
  columns, not the absolute values. On real hardware, with Redis on its own
  host, expect higher throughput, and expect the one-round-trip design to
  matter more.
- **Variance:** across repeated runs, 5–10% for throughput. p99 is noisier.
  One run of 1 KB Sentinel hits showed a p99 of 226 ms; six reruns showed
  7–10 ms and the cause could not be reproduced, so treat isolated p99
  outliers on this shared 2-core machine with caution.

## Results (requests per second; p50 / p99 latency)

### A. Cache hits, backend instant

| Size | no cache | APISIX memory cache | Redis single | Redis Sentinel | Redis Cluster |
|---|---|---|---|---|---|
| 1 KB | 41,320 | 21,990 (2.2 / 5.0 ms) | 12,790 (3.7 / 6.9 ms) | 12,510 (3.8 / 6.9 ms) | 10,090 (4.5 / 13.6 ms) |
| 100 KB | 8,860 | 2,850 (16.6 / 25.8 ms) | **6,910** (6.6 / 18.5 ms) | **6,310** (7.7 / 18.1 ms) | **6,380** (7.2 / 19.4 ms) |
| 1 MB | 838 | 305 (157 / 209 ms) | **760** (60 / 158 ms) | **784** (60 / 134 ms) | **812** (57 / 136 ms) |

- **Large responses:** 2–2.7x faster than the memory cache. The built-in
  memory cache JSON-encodes and decodes the whole body on every write and
  hit; this plugin stores and returns raw bytes.
- **Small responses:** the extra Redis round trip and protocol work
  dominate.
- **"no cache" isn't a fair comparison when the backend is instant,** as it
  is here. In practice the backend is what you're caching, so see B.

### B. Cache hits, backend takes 20 ms, 1 KB

| no cache | memory | Redis single | Redis Sentinel | Redis Cluster |
|---|---|---|---|---|
| 2,360 (21.0 ms p50) | 18,580 | 11,710 | 11,280 | 10,170 |

### C. Every request a miss (each response written), 1 KB

| memory | Redis single | Redis Sentinel | Redis Cluster | single + `purge_prefix_depth: 1` | single + `purge_prefix_depth: 2` |
|---|---|---|---|---|---|
| 12,940 | 9,320 | 9,510 | 8,260 | 9,230 | 8,820 |

Recording each response for prefix purge costs about 1% at depth 1 and
about 5% at depth 2.

### D. Cache hits, 200 connections, 1 KB

| memory | Redis single | Redis Sentinel | Redis Cluster |
|---|---|---|---|
| 21,840 (p99 53 ms) | 11,270 (p99 37 ms) | 11,360 (p99 36 ms) | 9,370 (p99 45 ms) |

### G. Stampede: hot URL, `cache_ttl: 1`, backend takes 50 ms, 10 s of load

| Setting | req/s | p99 | Upstream calls |
|---|---|---|---|
| none | 11,570 | 46.9 ms | 500 |
| `cache_lock` | 11,120 | 76.9 ms | **10** |
| `stale_while_updating: 30` | 11,610 | **11.1 ms** | **10** |
| both | 11,590 | 11.0 ms | **10** |

10 is the ideal: one upstream call per expiry. `cache_lock` alone makes
waiting requests wait for the fetch, which is why its p99 is higher.

### P. Purge by prefix: 10,000 entries under one prefix

| Redis single | Redis Sentinel | Redis Cluster |
|---|---|---|
| 132–174 ms | 161–179 ms | 195–257 ms |

## Failure tests

All run under continuous load from 8 client threads against 2 gateway
workers, with every request checked.

### Redis Cluster: master killed (3 masters + 3 replicas)

| | Before the kill | During promotion (about 3 s) | After promotion |
|---|---|---|---|
| Requests served | 100% | **100%** | 100% |
| Cache | HIT | only the dead master's keys MISS | **100% HIT** |
| p99 | 6–8 ms | 8–11 ms | 6–11 ms |
| Worst request | 12 ms | 131 ms | 22 ms |

### Redis Sentinel: master killed, then restarted (master + replica + 3 Sentinels)

| | Before the kill | During failover (about 3 s) | After failover |
|---|---|---|---|
| Requests served | 100% | **100%** (MISS) | 100% |
| Cache | HIT | MISS | **100% HIT** on the new master |
| p99 | 6–7 ms | 8–12 ms | 6–10 ms |
| Worst request | 16 ms | 18 ms | 33 ms |

The restarted old master was turned into a replica by Sentinel, and the
plugin kept writing to the new master the whole time.

### Redis unavailable

| Event | Effect |
|---|---|
| Connection refused | No measurable cost per request (p99 4 ms). |
| Hung (accepts TCP, never replies) | During the outage, p50 2.3 ms and p99 about 5 ms. At onset, only in-flight requests wait one `timeout` (200 ms). |
| Recovery | Automatic: the first successful probe re-enables caching. |

### Soak

- **v0.2:** 7 minutes, 5.44 million mixed requests.
- **v0.3:** 4 minutes, 1.97 million requests mixing single and Sentinel,
  Vary, URL and prefix purges, `cache_lock` and `stale_while_updating`.
- **Both:** zero errors, flat worker memory (about 13 MB), stable Redis
  connection counts.
