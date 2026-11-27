# Changelog

## 0.3.1

- Docker smoke test: set `stream_plugins: []` and only fail on errors from
  this plugin (APISIX's stream `syslog` plugin logged an unrelated startup
  error with the minimal plugin list).
- README: note about the `syslog` / `prometheus-cache` startup message.

## 0.3.0

- **Redis Sentinel support:** `policy: "redis-sentinel"`.
  - Masters are discovered through the Sentinels and cached per worker for
    up to 10 s.
  - The plugin re-asks after a connection error or a `READONLY` reply (a
    demoted master) and asks the last Sentinel that answered first.
- **Purge by URL prefix:** route option `purge_prefix_depth` and
  `PURGE /prefix/*`.
  - Entries are recorded per host and path prefix in sorted sets that
    expire with their entries.
  - Purging is batched, across all consumers and Vary variants. It returns
    `{"purged": n}`.
- **Redis Cluster failover:** for 10 s after a cluster error, writes also go
  through the fast-failing single-command path. During a master failover
  p99 now stays at 8–11 ms (0.2.0: 23–34 ms).
- **A single failed cluster node** no longer aborts the writes queued for
  other nodes in the same batch.
- **Testing:**
  - the test kit runs every functional check on single, Sentinel and
    Cluster;
  - new failover test that kills the Sentinel-managed master under load;
  - the Docker smoke test covers Sentinel and prefix purge;
  - GitHub Actions CI.
- **Docs:** the documented Redis ACL now lists the commands prefix purge
  runs inside its scripts.

## 0.2.0

- **Faster:**
  - one Redis round trip per lookup (plain pipelined `GET` and `PTTL`);
  - response bodies stored raw instead of JSON-escaped, so large hits are
    2–3x faster than the built-in memory cache;
  - writes go through a bounded per-worker queue with pipelined batches.
- **Stampede protection:** `cache_lock` and `stale_while_updating`.
- **Freshness from Redis's clock (`PTTL`):** clock differences between
  APISIX nodes no longer matter.
- **Hashed keys:** fixed-size `{sha1}` keys; the value carries an MD5 of the
  cache key; no URL or consumer name is stored in Redis.
- **Redis Cluster failover:**
  - the plugin refreshes the cluster's key-to-node map after a master is
    marked unhealthy (the bundled client doesn't);
  - a single failed node no longer disables caching for every key;
  - reads switch to a fast-failing path for 10 s after cluster errors.
- **Outage handling:** single-probe recovery after a backoff, rate-limited
  error logs, no buffering of bodies while Redis is known to be down.
- **Fixes:**
  - the cache key no longer depends on APISIX's `$upstream_cache_key`
    variable, which only exists when the built-in proxy-cache is enabled;
  - Redis connections are closed when `AUTH` fails;
  - write drainers survive unexpected errors.
- **Logging:** new `$proxy_cache_redis_status` variable.

## 0.1.0

- First version: APISIX's proxy-cache memory strategy, stored in Redis.
  Single Redis and Redis Cluster, chosen per route with `policy`.
