# apisix-proxy-cache-redis

A response cache plugin for [Apache APISIX](https://apisix.apache.org/) that
stores cached responses in **Redis**. It supports a single server, **Redis
Sentinel** and **Redis Cluster**, so every APISIX node shares one cache.

It behaves like APISIX's built-in `proxy-cache` plugin with
`cache_strategy: "memory"`, whose caching rules are carried over line for
line. On top of that it adds the things a shared production cache needs:
- stampede protection;
- purge by URL prefix;
- fail-open behaviour when Redis is down;
- clean failover on Sentinel and Cluster.

```
client ──► APISIX node 1 ──┐
client ──► APISIX node 2 ──┼──► Redis (single │ Sentinel │ Cluster)   one shared cache
client ──► APISIX node N ──┘         │
                                     └─ MISS ──► your upstream
```

## Contents

- [Features](#features)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
- [How it works](#how-it-works)
- [Performance](#performance)
- [Production checklist](#production-checklist)
- [Operations](#operations)
- [Compatibility](#compatibility)
- [Testing](#testing)
- [Limitations](#limitations)
- [Differences from the built-in proxy-cache](#differences-from-the-built-in-proxy-cache)
- [License](#license)

## Features

- **Same caching rules as APISIX's memory cache:** cache key and consumer
  isolation, `Vary` variants, `Cache-Control` and `Expires`, `PURGE`, TTL,
  `Set-Cookie` safety, size cap, and the `Apisix-Cache-Status` header.
- **Three Redis modes, chosen per route:** `redis`, `redis-sentinel` and
  `redis-cluster`. Connection details live only in `config.yaml`.
- **Fast:** one Redis round trip per lookup, and response bodies are stored
  as raw bytes. Large responses are served **2–2.5x faster than the built-in
  memory cache** ([benchmarks](docs/benchmarks.md)).
- **Stampede protection, opt-in:**
  - `cache_lock`: only one request per URL goes to the upstream on a miss.
  - `stale_while_updating`: the expired copy is served instantly while one
    request refreshes it.
- **Purge by URL prefix:** `PURGE /products/*` removes everything cached
  under `/products`, for every consumer and every Vary variant.
- **Fail-open:**
  - if Redis is down, slow or misconfigured, requests are still served from
    the upstream;
  - a worker that sees a failure skips Redis briefly, then sends a single
    probe;
  - error logs are rate-limited.
- **Failover-tested:** a killed Sentinel master and a killed Cluster master
  both recovered automatically under load, with every request served.
- **Safe keys:** keys are fixed-size hashes, so URLs and consumer names never
  appear in Redis. Hash collisions are detected, and a collision can never
  serve the wrong response.
- **Observable:** the `$proxy_cache_redis_status` variable is available to
  logger plugins.

## Quick start

**1. Copy the plugin** to every APISIX node (or into your APISIX image):

```shell
git clone https://github.com/<you>/apisix-proxy-cache-redis.git /opt/apisix-plugins/apisix-proxy-cache-redis
```

You only need the `apisix/` folder. It contains `apisix/plugins/proxy-cache-redis.lua`
and `apisix/plugins/proxy-cache-redis/`.

**2. Edit `conf/config.yaml`.** See [`conf/config.yaml.example`](conf/config.yaml.example)
for every option.

```yaml
apisix:
  extra_lua_path: "/opt/apisix-plugins/apisix-proxy-cache-redis/?.lua"

plugins:                     # keep your existing list and add this one
  - proxy-cache-redis

plugin_attr:
  proxy-cache-redis:
    redis:                   # and/or redis-sentinel, redis-cluster
      host: 10.0.0.5
      port: 6379
      password: ${{REDIS_PASSWORD}}
```

> ⚠️ **Setting `plugins:` replaces APISIX's built-in plugin list.** Start
> from the default list in `apisix/cli/config.lua` for your APISIX version
> and add `proxy-cache-redis` to it, or every other plugin will be disabled.
>
> Alternatively, copy the two items under `apisix/plugins/` straight into
> APISIX's own `apisix/plugins/` directory and skip `extra_lua_path`. Note
> that an APISIX upgrade can overwrite that directory.

**3. Reload APISIX:** `apisix reload`, or restart the container.

**4. Enable it on a route:**

```shell
curl "http://127.0.0.1:9180/apisix/admin/routes/1" -X PUT -H "X-API-KEY: $ADMIN_KEY" -d '{
  "uri": "/api/products/*",
  "plugins": {
    "proxy-cache-redis": {
      "policy": "redis",
      "cache_ttl": 60
    }
  },
  "upstream": { "type": "roundrobin", "nodes": { "backend:8080": 1 } }
}'
```

```shell
curl -i http://127.0.0.1:9080/api/products/1    # Apisix-Cache-Status: MISS
curl -i http://127.0.0.1:9080/api/products/1    # Apisix-Cache-Status: HIT
```

**5. Before production,** run the [smoke test in the official APISIX image](#testing)
and go through the [production checklist](#production-checklist).

## Configuration

### `config.yaml`: `plugin_attr.proxy-cache-redis`

| Field | Default | Description |
|---|---|---|
| `key_prefix` | `apisix:proxy-cache:` | Prefix of every Redis key. Must not contain `{` or `}`. |
| `expired_retention` | `60` | Seconds an expired entry stays in Redis. During that time it is reported as `EXPIRED` (not `MISS`), `PURGE` returns 200, and `stale_while_updating` can serve it. |
| `failure_backoff` | `1` | After a connection error or timeout, the worker skips Redis for this many seconds, then a single request probes it. `0` disables this (not recommended). |
| `write_concurrency` | `4` | Background write drainers per worker. |
| `max_pending_writes` | `10000` | Per-worker write queue limit, in items. Writes beyond it are dropped and counted; the response is still served. |
| `max_pending_bytes` | `67108864` | The same limit in bytes (64 MiB per worker). |
| `redis` | none | Single Redis. See below. |
| `redis-sentinel` | none | Redis Sentinel. See below. |
| `redis-cluster` | none | Redis Cluster. See below. |

Define only the sections your routes use. A route whose `policy` has no
section is rejected when you save it, with a clear error.

**`redis`**

| Field | Default |
|---|---|
| `host` | required |
| `port` | `6379` |
| `username` | none |
| `password` | none |
| `database` | `0` |
| `timeout` (ms) | `200` |
| `ssl` | `false` |
| `ssl_verify` | `false` |
| `server_name` | none |
| `keepalive_timeout` (ms) | `10000` |
| `keepalive_pool` | `100` |

**`redis-sentinel`**

| Field | Default | Notes |
|---|---|---|
| `sentinels` | required | List of `"host:port"`. |
| `master_name` | required | |
| `sentinel_username` | none | Only if your Sentinels require AUTH. |
| `sentinel_password` | none | Only if your Sentinels require AUTH. |
| `username`, `password` | none | For the Redis master. |
| `database`, `timeout`, `ssl`, `ssl_verify`, `server_name`, `keepalive_timeout`, `keepalive_pool` | as for `redis` | `ssl` applies to both the Sentinels and the master. |

**`redis-cluster`**

| Field | Default | Notes |
|---|---|---|
| `nodes` | required | List of `"host:port"`. |
| `name` | required | Any unique name for this cluster. |
| `password` | none | |
| `timeout` (ms) | `200` | |
| `ssl`, `ssl_verify` | `false` | |
| `keepalive_timeout`, `keepalive_pool` | as for `redis` | |

Cluster mode also needs this in `config.yaml`:

```yaml
nginx_config:
  http:
    custom_lua_shared_dict:
      plugin-proxy-cache-redis-cluster-slot-lock: 1m
```

Passwords can come from environment variables, for example
`password: ${{REDIS_PASSWORD}}`.

### Route options

| Field | Default | Description |
|---|---|---|
| `policy` | `"redis"` | `redis`, `redis-sentinel` or `redis-cluster`. |
| `cache_ttl` | `300` | Seconds. Used when `cache_control` is false. |
| `cache_key` | `["$host", "$request_uri"]` | Variables and strings joined into the cache key. |
| `cache_method` | `["GET", "HEAD"]` | Methods that may be cached (`GET`, `POST`, `HEAD`). |
| `cache_http_status` | `[200, 301, 404]` | Upstream status codes that may be cached. |
| `cache_control` | `false` | Follow HTTP `Cache-Control` and `Expires` semantics (TTL from the response; request `max-age`/`min-fresh`/`no-cache`/`only-if-cached` honoured). |
| `cache_bypass` | none | Variables. If any of them is non-empty and not `"0"`, skip the cache lookup. |
| `no_cache` | none | Variables. If any of them is non-empty and not `"0"`, don't store the response. |
| `hide_cache_headers` | `false` | Remove `Expires` and `Cache-Control` from responses. |
| `consumer_isolation` | `true` | Give every authenticated consumer its own cache. **Keep this on** for anything user-specific. |
| `cache_set_cookie` | `false` | Allow caching responses that carry `Set-Cookie`. |
| `max_resp_body_size` | `1048576` | Bytes. Responses this size or larger are not cached. |
| `cache_lock` | `false` | On a miss, only one request per URL goes upstream; the others wait for its response. |
| `cache_lock_timeout` | `5` | Seconds a request waits for another request's response (1–60). |
| `stale_while_updating` | `0` | For this many seconds after expiry, serve the expired copy (status `UPDATING`) while one request refreshes it. Must be ≤ `expired_retention`. |
| `purge_prefix_depth` | `0` | 0–8. Record every cached response under its first N URL path levels, so `PURGE /a/b/*` works for prefixes up to N levels deep. |

## Usage

**Purge one URL:**

```shell
curl -X PURGE http://127.0.0.1:9080/api/products/1
# 200 when it was cached (all Vary variants are removed), 404 when it wasn't
```

**Purge by prefix.** This needs `purge_prefix_depth`; with `2`, both
`/api/*` and `/api/products/*` work.

```shell
curl -X PURGE 'http://127.0.0.1:9080/api/products/*'
# 200 {"purged":1532}   or   404 {"purged":0}
```

- It removes every cached response under the prefix for that host, across
  **all consumers and all Vary variants**.
- Prefixes deeper than `purge_prefix_depth`, and `PURGE /*` (the whole
  host), return 400.
- Only responses cached after the option was enabled are recorded.

> ⚠️ **Anyone who can send `PURGE` to a route can empty its cache**, exactly
> as with APISIX's built-in `proxy-cache`. Restrict it, for example with a
> separate route for `methods: ["PURGE"]` protected by `key-auth` +
> `consumer-restriction` or `ip-restriction`.

**Log the cache status:** `$proxy_cache_redis_status` works in the
`log_format` of APISIX logger plugins (`http-logger`, `kafka-logger`, and so
on).

**Cache status values:**

| `Apisix-Cache-Status` | Meaning |
|---|---|
| `HIT` | Served from Redis. |
| `MISS` | Not in the cache, or Redis is unavailable (fail-open); fetched from upstream. |
| `EXPIRED` | Was cached but expired; fetched from upstream. |
| `STALE` | Cached copy is too old for the request's `Cache-Control`; fetched from upstream. |
| `BYPASS` | `cache_bypass` matched, or the request said `no-cache`/`no-store`. |
| `UPDATING` | Expired copy served while another request refreshes it (`stale_while_updating`). |

## How it works

- **Keys.** Each URL is stored as `<prefix>{sha1(cache key)}`, plus `:i`
  (Vary index), `:v:<sig>` (one per Vary variant) and `:l` (stampede lock).
  - All of a URL's keys share one Redis Cluster slot, so their updates run
    as atomic Redis scripts.
  - Values carry an MD5 of the cache key and are served only if both hashes
    match.
- **Values.** Each value is a small JSON header block followed by the raw
  body bytes, with no JSON escaping of the body.
- **Expiry uses Redis's clock only.** The physical TTL is
  `ttl + expired_retention`, and freshness and `Age` come from `PTTL`. Clock
  differences between APISIX nodes don't matter.
- **Reads:** one round trip (pipelined `GET` and `PTTL`). URLs that use Vary
  need a second one.
- **Writes** can't happen while nginx is streaming the response, so they go
  into a bounded per-worker queue. A few background drainers send them in
  batches as atomic scripts, and the queue is flushed on reload.
- **Failures:**
  - fail-open everywhere, with a per-worker backoff and a single recovery
    probe;
  - cluster mode is per node: one dead node only affects its own keys, and
    the plugin refreshes the cluster's key-to-node map after a failover
    (the bundled cluster client doesn't);
  - in Sentinel mode the master address is cached for up to 10 seconds,
    re-asked after any connection error or `READONLY` reply, and the
    Sentinel that last answered is asked first.
- **Prefix purge.** Each recorded prefix is a sorted set of entry keys, one
  per host and path prefix, scored by expiry time.
  - Expired members are trimmed on every write, and the set itself expires
    with its last entry.
  - A purge deletes the members in batches of 500, one pipeline per batch.

## Performance

These are excerpts; full results and method are in
[docs/benchmarks.md](docs/benchmarks.md). They were measured with `wrk` on a
2-core machine shared by the gateway (one nginx worker), the backend, Redis
and wrk, and compared against APISIX's own `proxy-cache` memory plugin in
the same gateway. Absolute numbers will be higher on real hardware.

**Cache hits, requests per second:**

| Response size | APISIX memory cache | Redis single | Redis Sentinel | Redis Cluster |
|---|---|---|---|---|
| 1 KB | 21,990 | 12,790 | 12,510 | 10,090 |
| 100 KB | 2,850 | **6,910** | **6,310** | **6,380** |
| 1 MB | 305 | **760** | **784** | **812** |

**Stampede test:** a hot URL with a 1 s TTL, a 50 ms backend, 10 s of load.

| Setting | Upstream calls |
|---|---|
| no protection | 500 |
| `cache_lock` | **10** |
| `stale_while_updating` | **10** (p99 46 ms → 11 ms) |

**Prefix purge of 10,000 entries:** single 132 ms, Sentinel 161 ms,
Cluster 195 ms.

**Failover:**
- **Sentinel master killed under load:** every request served (65,947 of
  65,947), worst request 18 ms, cache back at 100% HITs on the new master
  about 4 s after the kill.
- **Cluster master killed under load:** every request served; only the dead
  node's keys missed during promotion (p99 8–11 ms, worst request 131 ms);
  100% HITs right after promotion.

## Production checklist

1. **Run `docker/smoke.sh`** (needs Docker) with your APISIX version. It
   runs the plugin inside the official APISIX image against single Redis,
   Sentinel and Cluster. GitHub Actions runs it on every push.
2. **Set Redis `maxmemory` and `maxmemory-policy allkeys-lru`.** Use
   `volatile-lru` if this Redis also holds data that must not be evicted;
   every cache key has a TTL. With `noeviction`, a full Redis stops caching
   (requests are still served).
3. **Keep `consumer_isolation: true`** on any route whose responses depend
   on who is asking.
4. **Restrict `PURGE`**; see [Usage](#usage).
5. **Start with one read-heavy, non-personalised route** and a short
   `cache_ttl` (30–60 s). Watch the APISIX error log, Redis memory and CPU,
   and `$proxy_cache_redis_status`. Then expand route by route.
6. **Rollback:** remove the plugin from the route. It takes effect
   immediately.

## Operations

- **Minimal Redis ACL** (single or Sentinel master). Redis checks the
  commands run inside scripts too:
  `ACL SETUSER cache on >secret ~apisix:proxy-cache:* +get +set +del +exists +pttl +eval +evalsha +select +zadd +zrange +zrem +zremrangebyscore +pexpire +time`
  - drop `+zadd +zrange +zrem +zremrangebyscore +pexpire +time` if no route
    uses `purge_prefix_depth`;
  - drop `+select` if `database` is 0;
  - Redis Cluster also needs `+cluster|slots +cluster|nodes`;
  - Sentinels need `+sentinel|get-master-addr-by-name` for the
    `sentinel_username`.
- **TLS:** set `ssl: true`, and `ssl_verify: true` with a trusted CA in
  production.
- **Cluster health tracking:** the optional `redis_cluster_health` shared
  dict, which APISIX declares when `limit-count`, `limit-req` or
  `limit-conn` is enabled, turns on per-node fast-fail. Don't declare it
  yourself if one of those plugins is enabled.
- **Clear everything:**
  `redis-cli --scan --pattern 'apisix:proxy-cache:*' | xargs -r redis-cli del`
  (on every node of a cluster).
- **Don't enable `proxy-cache` and `proxy-cache-redis` on the same route.**
- **Upgrading APISIX:**
  - `apisix/plugins/proxy-cache-redis/handler.lua` names the upstream commit
    it was derived from; diff
    `apisix/plugins/proxy-cache/memory_handler.lua` against it and port any
    changes;
  - re-run `tests/run.sh failover` when APISIX updates
    `lua-resty-redis-cluster`.

## Compatibility

- **Built and tested against** APISIX master
  `22ce60adae3c51177d32225ace62cdeabbac8706` (2026-11-26). The smoke test
  targets `apache/apisix:3.21.0-debian`.
- **APISIX APIs used:** `core.response.hold_body_chunk`,
  `core.ctx.register_var`, `apisix.utils.rediscluster`,
  `apisix.plugins.proxy-cache.util`. These exist throughout APISIX 3.x. On
  versions where `hold_body_chunk` has no size argument, large responses are
  fully buffered before being skipped.
- **Redis:** tested with Redis 7.0. Redis ≥ 5 should work, since the scripts
  rely on effects replication for `TIME`. Valkey is expected to work but is
  untested.

## Testing

| What | How | Where it runs |
|---|---|---|
| Functional suite, about 270 checks on single, Sentinel and Cluster (every behaviour of APISIX's `memory.t`, plus everything above), ACL, TLS, outages | `tests/run.sh` | nginx + lua-nginx-module with APISIX's own Redis-cluster helper, proxy-cache util and jsonschema |
| + Sentinel and Cluster master killed under load | `tests/run.sh failover` | same |
| + benchmark against the built-in memory cache | `tests/run.sh all` | same, needs `wrk` |
| Smoke test in the **official APISIX image** | `docker/smoke.sh` | Docker |
| All of the above except the benchmark | GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) | every push |

See [tests/README.md](tests/README.md) for requirements.

## Limitations

- **Small responses** are about 40% slower than the in-process memory cache:
  that's the network hop. Large responses are faster.
- **Writes are best-effort.** They're dropped when the queue is full or
  Redis is unavailable, and lost if a worker crashes. Responses are always
  served.
- **`cache_lock` waiters** hold their connection for up to
  `cache_lock_timeout`. The lock is per URL, so Vary variants of one URL
  wait on each other briefly.
- **`UPDATING` (stale) responses** ignore the request's own
  `Cache-Control: max-age` and `min-fresh`.
- **Prefix purge:**
  - it needs `purge_prefix_depth` and only covers responses cached after it
    was enabled;
  - it can't purge a whole host;
  - every write updates one sorted set per recorded level, so a single very
    hot first-level prefix becomes a hot Redis key.
- **Per-worker state:** the backoff, the Sentinel master cache and the write
  queue are per nginx worker.
- **Never cached** (same as the built-in cache): `HEAD` responses,
  `Vary: *`, `private`/`no-store`/`no-cache` responses, `Set-Cookie` unless
  `cache_set_cookie` is on, and responses of `max_resp_body_size` or larger.

## Differences from the built-in proxy-cache

| | memory (built-in) | proxy-cache-redis |
|---|---|---|
| Where entries live | per APISIX node | shared by all nodes; survives APISIX restarts |
| Storage unavailable | can't happen | fail-open (`MISS`), with backoff; `PURGE` returns `503` |
| `max_resp_body_size` default | 64 MiB | 1 MiB |
| Stampede protection | none | `cache_lock`, `stale_while_updating` |
| Purge by prefix | no | `purge_prefix_depth` |
| `cache_zone`, `cache_strategy` | yes | not applicable (`policy` instead) |

## License

Apache License 2.0; see [LICENSE](LICENSE). Parts are derived from Apache
APISIX; see [NOTICE](NOTICE).
