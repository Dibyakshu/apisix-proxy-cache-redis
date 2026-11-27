# Test kit

```shell
# Ubuntu/Debian
sudo apt install nginx-core libnginx-mod-http-lua lua-resty-core \
                 lua-resty-lrucache lua-cjson redis-server wrk git python3 openssl
tests/run.sh            # functional suite, ~4 minutes
tests/run.sh failover   # + kill a Sentinel master and a Cluster master under load
tests/run.sh all        # + benchmark against the built-in memory cache (~8 more minutes)
```

`run.sh` doesn't need root. It does the following:

1. Fetches APISIX at the pinned commit and the libraries APISIX ships at
   their pinned versions, into a temporary folder.
2. Starts real Redis servers:
   - single on `6390`;
   - password + ACL user on `6391`;
   - TLS on `6392`;
   - Sentinel: master `6400`, replica `6401`, Sentinels `26379-26381`;
   - a 3-node cluster on `7000-7002`;
   - a 6-node cluster with replicas on `7100-7105`, for the failover test.
3. Starts nginx: a gateway with 2 workers on `1984`, and a test backend on
   `1980`.
4. Runs the tests.
5. Stops everything and deletes the temporary folder. Use `KEEP=1` to keep
   it; logs are in `<workdir>/logs/`.

Those ports must be free. If your nginx modules are not in
`/usr/lib/nginx/modules`, set `NGINX_MODULES=/path`.

| File | What it does |
|---|---|
| `run_tests.py` | About 270 checks: every behaviour of APISIX's `t/plugin/proxy-cache/memory.t` on single, Sentinel and Cluster; stampede lock; stale-while-updating; prefix purge (including 1,500-entry purges); write-queue limits; collision safety; Redis-clock expiry; ACL; wrong password (no leaked connections); TLS; refused and hung Redis; recovery; Sentinel manual failover; Sentinels down. |
| `failover.py` | Kills a Redis Cluster master under load, then reports served requests, cache hits and latency per second. |
| `sentinel_failover.py` | Kills the Sentinel-managed master under load, restarts it, and checks it rejoins as a replica. |
| `bench.py` | wrk benchmark: no cache, APISIX's unmodified memory cache, and Redis single, Sentinel and Cluster. |
| `lua/runner.lua` | Calls the plugin the way APISIX's plugin runner does, and includes the test backend. The header `X-Plugin: memory` runs APISIX's built-in proxy-cache instead, for comparison. |
| `stubs/apisix/*.lua` | Minimal stand-ins for `apisix.core` and the config lookup. `hold_body_chunk` and `set_header` are copied from APISIX. |

To test inside the official APISIX image instead, use `../docker/smoke.sh`.
