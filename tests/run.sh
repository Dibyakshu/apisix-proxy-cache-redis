#!/usr/bin/env bash
# End-to-end test kit for proxy-cache-redis.
#
#   ./run.sh            functional suite (~270 checks) on single Redis,
#                       Redis Sentinel and Redis Cluster, plus ACL, TLS and
#                       outage cases -- ~4 minutes
#   ./run.sh failover   + kill a Redis Cluster master and a Sentinel-managed
#                       master under load -- ~1 more minute
#   ./run.sh all        + load benchmark against APISIX's built-in memory
#                       cache (needs wrk) -- ~8 more minutes
#
# Starts real Redis servers and nginx + lua-nginx-module, loads the plugin
# with APISIX's own rediscluster helper and proxy-cache util at the pinned
# commit, runs the tests, then stops everything and deletes the work dir.
#
#   single      :6390          password + ACL user :6391     TLS :6392
#   sentinel    master :6400, replica :6401, sentinels :26379-26381
#   cluster     :7000-7002     cluster with replicas :7100-7105 (failover)
#   nginx       gateway :1984  test backend :1980
#
# Needs: git, python3, openssl, redis-server/redis-cli (built with TLS, as
#        Debian/Ubuntu's are), nginx with lua-nginx-module, wrk for `all`.
#   Ubuntu: apt install nginx-core libnginx-mod-http-lua lua-resty-core \
#                       lua-resty-lrucache lua-cjson redis-server wrk
# Usage: [KEEP=1] [NGINX_MODULES=/path] ./run.sh [failover|all]
set -euo pipefail

MODE="${1:-functional}"
ROOT="$(cd "$(dirname "$0")" && pwd)"
PLUGIN="$(cd "$ROOT/.." && pwd)"
WORK="$(mktemp -d /tmp/pcr-test.XXXXXX)"
# mktemp creates the dir as 0700; nginx workers may run as another user
# and must be able to read the libraries in it
chmod 755 "$WORK"
APISIX_SHA=22ce60adae3c51177d32225ace62cdeabbac8706
NGINX_MODULES="${NGINX_MODULES:-/usr/lib/nginx/modules}"
PORTS="6390 6391 6400 6401 26379 26380 26381 7000 7001 7002 7100 7101 7102 7103 7104 7105"

cleanup() {
    nginx -p "$WORK" -c "$WORK/gateway.conf" -s stop 2>/dev/null || true
    nginx -p "$WORK" -c "$WORK/upstream.conf" -s stop 2>/dev/null || true
    for p in $PORTS; do
        redis-cli -p "$p" -a adminpw --no-auth-warning shutdown nosave >/dev/null 2>&1 || true
        redis-cli -p "$p" shutdown nosave >/dev/null 2>&1 || true
    done
    redis-cli --tls --insecure -p 6392 shutdown nosave >/dev/null 2>&1 || true
    if [ "${KEEP:-0}" = "1" ]; then echo "work dir kept: $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

clone() {  # repo dir ref
    git -c advice.detachedHead=false clone -q --depth 1 --branch "$3" "$1" "$WORK/src/$2"
}

wait_for() {  # description, command...
    local what=$1; shift
    for _ in $(seq 1 60); do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 0.5
    done
    echo "timed out waiting for $what" >&2
    return 1
}

echo "== fetching libraries"
mkdir -p "$WORK"/{src,libs/resty,libs/net,logs,redis/sentinel,tls}
git init -q "$WORK/apisix"
git -C "$WORK/apisix" fetch -q --depth 1 https://github.com/apache/apisix.git "$APISIX_SHA"
git -C "$WORK/apisix" -c advice.detachedHead=false checkout -q FETCH_HEAD
clone https://github.com/openresty/lua-resty-redis.git redis v0.34
# v0.09: newer releases need OpenResty's patched lua-resty-core (distro
# nginx-lua lacks it); APISIX itself runs on OpenResty
clone https://github.com/openresty/lua-resty-lock.git lock v0.09
clone https://github.com/openresty/lua-resty-string.git string v0.19
# the fork APISIX ships (rockspec: lua-resty-redis-cluster = 1.3.5-0)
clone https://github.com/api7/lua-resty-redis-cluster.git cluster v1.3.5
clone https://github.com/api7/jsonschema.git jsonschema v0.9.13
clone https://github.com/golgote/neturl.git neturl v1.1-1
cp "$WORK"/src/redis/lib/resty/redis.lua "$WORK"/src/lock/lib/resty/lock.lua \
   "$WORK"/src/string/lib/resty/string.lua "$WORK"/src/string/lib/resty/random.lua \
   "$WORK"/src/cluster/lib/resty/rediscluster.lua "$WORK"/src/cluster/lib/resty/xmodem.lua \
   "$WORK/libs/resty/"
cp -r "$WORK"/src/jsonschema/lib/* "$WORK/libs/"
cp "$WORK"/src/neturl/lib/net/url.lua "$WORK/libs/net/"

echo "== starting redis"
R() { redis-server --daemonize yes --save "" --appendonly no "$@"; }
R --port 6390 --dir "$WORK/redis" --logfile "$WORK/redis/6390.log"
# password + ACL user limited to the commands and keys the plugin needs
R --port 6391 --requirepass adminpw --dir "$WORK/redis" --logfile "$WORK/redis/6391.log"
wait_for "redis 6391" redis-cli -p 6391 -a adminpw --no-auth-warning ping
redis-cli -p 6391 -a adminpw --no-auth-warning ACL SETUSER cache on '>s3cret' '~test:pc:*' \
    +get +set +del +exists +pttl +eval +evalsha +select \
    +zadd +zrange +zrem +zremrangebyscore +pexpire +time >/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$WORK/tls/key.pem" \
    -out "$WORK/tls/cert.pem" -days 1 -subj "/CN=localhost" 2>/dev/null
R --port 0 --tls-port 6392 --tls-cert-file "$WORK/tls/cert.pem" \
    --tls-key-file "$WORK/tls/key.pem" --tls-ca-cert-file "$WORK/tls/cert.pem" \
    --tls-auth-clients no --dir "$WORK/redis" --logfile "$WORK/redis/6392.log"

# Sentinel: master 6400, replica 6401, three sentinels
R --port 6400 --dir "$WORK/redis/sentinel" --logfile "$WORK/redis/sentinel/6400.log"
R --port 6401 --replicaof 127.0.0.1 6400 --dir "$WORK/redis/sentinel" \
    --logfile "$WORK/redis/sentinel/6401.log"
for p in 26379 26380 26381; do
    cat > "$WORK/redis/sentinel/sentinel-$p.conf" <<EOF
port $p
daemonize yes
logfile $WORK/redis/sentinel/sentinel-$p.log
dir $WORK/redis/sentinel
sentinel monitor mymaster 127.0.0.1 6400 2
sentinel down-after-milliseconds mymaster 1000
sentinel failover-timeout mymaster 3000
sentinel parallel-syncs mymaster 1
EOF
    redis-server "$WORK/redis/sentinel/sentinel-$p.conf" --sentinel
done

for p in 7000 7001 7002 7100 7101 7102 7103 7104 7105; do
    mkdir -p "$WORK/redis/$p"
    R --port "$p" --cluster-enabled yes --cluster-config-file "nodes-$p.conf" \
        --cluster-node-timeout 2000 --dir "$WORK/redis/$p" --logfile "$WORK/redis/$p.log"
done
wait_for "cluster nodes" redis-cli -p 7105 ping
redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
    --cluster-replicas 0 --cluster-yes >/dev/null
redis-cli --cluster create 127.0.0.1:7100 127.0.0.1:7101 127.0.0.1:7102 \
    127.0.0.1:7103 127.0.0.1:7104 127.0.0.1:7105 --cluster-replicas 1 --cluster-yes >/dev/null
cluster_ok() { redis-cli -p "$1" cluster info | grep -q "cluster_state:ok"; }
wait_for "cluster 7000" cluster_ok 7000
wait_for "cluster 7100" cluster_ok 7100
sentinels_ready() {
    [ "$(redis-cli -p 26379 sentinel master mymaster | grep -A1 '^num-other-sentinels$' | tail -1)" = 2 ]
}
wait_for "sentinels" sentinels_ready

render() {  # template workers -> file
    sed -e "s#__ROOT__#$ROOT#g; s#__PLUGIN__#$PLUGIN#g; s#__WORK__#$WORK#g; \
            s#__WORKERS__#$2#g; s#/usr/lib/nginx/modules#$NGINX_MODULES#g" "$1"
}

start_gateway() {  # workers
    render "$ROOT/gateway.conf.template" "$1" > "$WORK/gateway.conf"
    nginx -p "$WORK" -c "$WORK/gateway.conf"
    wait_for "gateway" curl -sf -o /dev/null "http://127.0.0.1:1984/admin?stats=1"
}

echo "== starting nginx"
render "$ROOT/upstream.conf.template" 1 > "$WORK/upstream.conf"
nginx -p "$WORK" -c "$WORK/upstream.conf"
start_gateway 2

cd "$ROOT"
export PCR_WORK="$WORK"
echo "== functional tests (2 nginx workers)"
python3 run_tests.py

if [ "$MODE" = "failover" ] || [ "$MODE" = "all" ]; then
    echo "== Redis Cluster failover: kill a master under load"
    python3 failover.py
    echo "== Redis Sentinel failover: kill the master under load"
    python3 sentinel_failover.py
fi

if [ "$MODE" = "all" ]; then
    echo "== benchmark (1 gateway worker, 50 connections, 10 s per run)"
    nginx -p "$WORK" -c "$WORK/gateway.conf" -s stop
    sleep 1
    for p in 6390 7000 7001 7002; do redis-cli -p "$p" flushall >/dev/null; done
    start_gateway 1
    BENCH_OUT="$WORK/bench.json" python3 bench.py
fi
