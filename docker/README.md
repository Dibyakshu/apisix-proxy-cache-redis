# Smoke test in the official APISIX image

```shell
docker/smoke.sh                                  # apache/apisix:3.21.0-debian
APISIX_IMAGE_TAG=<your version>-debian docker/smoke.sh
KEEP=1 docker/smoke.sh                           # leave the containers running
```

This runs APISIX in standalone mode (no etcd) with the plugin mounted
through `extra_lua_path`, alongside single Redis, Redis Sentinel, a 3-node
Redis Cluster and an nginx backend. It checks:

- on all three policies:
  - MISS then HIT with an identical body;
  - PURGE (200, and 404 for a URL that was never cached);
  - Vary variants;
  - `Cache-Control: no-store` is respected;
- one-request refresh of an expired entry (`cache_lock` +
  `stale_while_updating`);
- purge by prefix;
- no plugin errors in the APISIX log.

It needs Docker with the compose plugin, curl, and a free port 9080 (or set
`APISIX_PORT`). The containers are removed afterwards. If anything fails,
the script prints the last 40 lines of the APISIX log.

GitHub Actions runs this on every push (`.github/workflows/ci.yml`).
