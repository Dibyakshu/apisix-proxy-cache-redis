#!/usr/bin/env bash
# Smoke test of proxy-cache-redis inside the official APISIX image.
#
#   ./smoke.sh                 uses apache/apisix:3.21.0-debian
#   APISIX_IMAGE_TAG=3.x.y-debian ./smoke.sh
#   KEEP=1 ./smoke.sh          leave the containers running afterwards
#
# Needs docker with the compose plugin, and curl. Port 9080 must be free
# (or set APISIX_PORT).
set -u
cd "$(dirname "$0")"

PORT="${APISIX_PORT:-9080}"
BASE="http://127.0.0.1:$PORT"
PASS=0
FAIL=0
RUN=$RANDOM$RANDOM

cleanup() {
    if [ "${KEEP:-0}" != "1" ]; then
        docker compose down -v >/dev/null 2>&1
    fi
}
trap cleanup EXIT

check() {  # name, condition result (0 = ok), detail
    if [ "$2" -eq 0 ]; then
        PASS=$((PASS + 1)); echo "PASS $1"
    else
        FAIL=$((FAIL + 1)); echo "FAIL $1  -> $3"
    fi
}

# request METHOD PATH [extra curl args] -> sets STATUS, CODE, BODY
request() {
    local method=$1 path=$2
    shift 2
    local hdr
    hdr=$(mktemp)
    BODY=$(curl -s -X "$method" -D "$hdr" "$@" "$BASE$path")
    CODE=$(head -1 "$hdr" | awk '{print $2}')
    STATUS=$(grep -i '^apisix-cache-status:' "$hdr" | awk '{print $2}' | tr -d '\r')
    rm -f "$hdr"
}

echo "== starting APISIX ${APISIX_IMAGE_TAG:-3.21.0-debian}, Redis, Sentinel, Cluster, backend"
docker compose up -d --quiet-pull || exit 1

for _ in $(seq 1 90); do
    if curl -s -o /dev/null "$BASE/single/ready"; then break; fi
    sleep 1
done

for pol in single sentinel cluster; do
    p="/$pol/$RUN"

    request GET "$p/a"; s1=$STATUS b1=$BODY
    sleep 0.5
    request GET "$p/a"
    check "[$pol] MISS then HIT with the same body" \
        "$([ "$s1" = MISS ] && [ "$STATUS" = HIT ] && [ "$BODY" = "$b1" ]; echo $?)" \
        "$s1/$STATUS"

    request PURGE "$p/a"
    check "[$pol] PURGE -> 200" "$([ "$CODE" = 200 ]; echo $?)" "$CODE"
    request GET "$p/a"
    check "[$pol] after PURGE -> MISS" "$([ "$STATUS" = MISS ]; echo $?)" "$STATUS"
    request PURGE "$p/never-cached"
    check "[$pol] PURGE of uncached URL -> 404" "$([ "$CODE" = 404 ]; echo $?)" "$CODE"

    request GET "$p/vary/x" -H "Accept-Language: en"
    request GET "$p/vary/x" -H "Accept-Language: fr"
    sleep 0.5
    request GET "$p/vary/x" -H "Accept-Language: en"; se=$STATUS be=$BODY
    request GET "$p/vary/x" -H "Accept-Language: fr"; sf=$STATUS bf=$BODY
    check "[$pol] Vary: each language cached separately" \
        "$([ "$se" = HIT ] && [ "$sf" = HIT ] && [[ "$be" == *-en ]] && [[ "$bf" == *-fr ]]; echo $?)" \
        "$se/$sf"

    request GET "$p/nocache/x"; sleep 0.3; request GET "$p/nocache/x"
    check "[$pol] Cache-Control: no-store response not cached" \
        "$([ "$STATUS" = MISS ]; echo $?)" "$STATUS"
done

p="/protected/$RUN"
request GET "$p/a"; sleep 0.4
request GET "$p/a"; b1=$BODY
sleep 1.5
request GET "$p/a"; s2=$STATUS
sleep 0.5
request GET "$p/a"
check "[protected] expired entry refreshed by one request, then HIT with new body" \
    "$([ "$s2" = EXPIRED ] && [ "$STATUS" = HIT ] && [ "$BODY" != "$b1" ]; echo $?)" \
    "$s2/$STATUS"

p="/prefix/$RUN"
request GET "$p/a"; request GET "$p/b"; request GET "/prefix/other$RUN/c"
sleep 0.5
request PURGE "$p/*"
check "[prefix] PURGE /prefix/<run>/* purges 2 entries" \
    "$([ "$CODE" = 200 ] && [[ "$BODY" == *'"purged":2'* ]]; echo $?)" "$CODE $BODY"
request GET "$p/a"; sa=$STATUS
request GET "/prefix/other$RUN/c"; sc=$STATUS
check "[prefix] purged entries MISS, other prefix still HIT" \
    "$([ "$sa" = MISS ] && [ "$sc" = HIT ]; echo $?)" "$sa/$sc"

errors=$(docker compose logs apisix 2>&1 | grep -E "runtime error|attempt to|\[error\].*proxy-cache-redis|failed to load plugin" | head -5)
check "no plugin errors in the APISIX log" "$([ -z "$errors" ]; echo $?)" "$errors"

echo
echo "$PASS passed, $FAIL failed"
if [ "$FAIL" -ne 0 ]; then
    echo "--- APISIX log (last 40 lines)"
    docker compose logs --tail 40 apisix
    exit 1
fi
