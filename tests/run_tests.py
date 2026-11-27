#!/usr/bin/env python3
"""End-to-end tests for proxy-cache-redis against real Redis.

Part 1 ports every behaviour of apisix t/plugin/proxy-cache/memory.t and
runs it on single Redis and on Redis Cluster. Part 2 covers what the Redis
version adds: stampede lock, stale-while-updating, write queue limits,
hashed keys / collision safety, Redis-clock expiry, status variable, auth,
ACL, TLS, outage behaviour."""
import hashlib, http.client, json, os, subprocess, sys, time, threading, urllib.parse

GW = ("127.0.0.1", int(os.environ.get("GW_PORT", "1984")))
HOST = "127.0.0.1"
WRITE_WAIT = 0.2           # async write queue
EXTRA = os.environ.get("EXTRA", "1") == "1"   # ACL/TLS/hung (need extra servers)
results = []


def req(path, conf=None, method="GET", headers=None, timeout=10):
    c = http.client.HTTPConnection(*GW, timeout=timeout)
    h = dict(headers or {})
    if conf is not None:
        h["X-Conf"] = json.dumps(conf, separators=(",", ":"))
    c.request(method, path, headers=h)
    r = c.getresponse()
    body = r.read()
    hd = {k.lower(): v for k, v in r.getheaders()}
    c.close()
    return r.status, hd, body


def admin(**kw):
    c = http.client.HTTPConnection(*GW, timeout=5)
    c.request("GET", "/admin?" + urllib.parse.urlencode(kw))
    r = c.getresponse().read().decode().strip()
    c.close()
    time.sleep(0.05)
    return r


def rcli(port, *args, cluster=None):
    cmd = ["redis-cli", "-p", str(port)]
    if cluster if cluster is not None else port >= 7000:
        cmd.append("-c")
    return subprocess.run(cmd + list(args), capture_output=True, text=True).stdout.strip()


def ekey(uri, consumer=None):
    base = HOST + uri
    if consumer:
        base = consumer + "\x01" + base
    return "test:pc:{" + hashlib.sha1(base.encode()).hexdigest() + "}"


def sentinel_master_port():
    out = subprocess.run(["redis-cli", "-p", "26379", "sentinel", "get-master-addr-by-name",
                          "mymaster"], capture_output=True, text=True).stdout.split()
    return int(out[1]) if len(out) == 2 else 6400


def port_of(policy):
    if policy == "redis":
        return 6390
    if policy == "redis-sentinel":
        return sentinel_master_port()
    return 7000


def single_ports(policy):
    return (port_of(policy),) if policy != "redis-cluster" else (7000, 7001, 7002)


def keys_for(policy, uri):
    ek = ekey(uri)
    out = "\n".join(subprocess.run(["redis-cli", "-p", str(p), "--scan", "--pattern",
                                    ek.replace("{", "\\{").replace("}", "\\}") + "*"],
                                   capture_output=True, text=True).stdout
                    for p in single_ports(policy))
    return [k for k in out.splitlines() if k]


def all_keys(policy):
    ports = single_ports(policy)
    ks = []
    for p in ports:
        ks += [k for k in subprocess.run(["redis-cli", "-p", str(p), "--scan", "--pattern", "test:pc:*"],
                                          capture_output=True, text=True).stdout.splitlines() if k]
    return ks


def parse_meta(raw):
    nl = raw.index("\n")
    n = int(raw[:nl])
    return json.loads(raw[nl + 1:nl + 1 + n])


def flush():
    rcli(6390, "flushall")
    rcli(sentinel_master_port(), "flushall")
    for p in (7000, 7001, 7002):
        subprocess.run(["redis-cli", "-p", str(p), "flushall"], capture_output=True)


def status(r):
    return r[1].get("apisix-cache-status")


def count(r):
    return int(r[1].get("x-count", "0"))


def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  -> {detail}"), flush=True)


def W():
    time.sleep(WRITE_WAIT)


def parallel(fn, n):
    out = [None] * n
    def run(i):
        out[i] = fn(i)
    ths = [threading.Thread(target=run, args=(i,)) for i in range(n)]
    [t.start() for t in ths]; [t.join() for t in ths]
    return out


# --------------------------------------------------------------------------
# Part 1: upstream memory.t behaviours, per policy
# --------------------------------------------------------------------------
def functional(policy):
    P = f"[{policy}] "
    base = {"policy": policy}
    pre = f"/{policy}"
    port = port_of(policy)

    def C(**kw):
        d = dict(base); d.update(kw); return d

    u = pre + "/hello"
    r1 = req(u, C()); W()
    r2 = req(u, C())
    check(P + "first request MISS", status(r1) == "MISS", status(r1))
    check(P + "second request HIT with same body", status(r2) == "HIT" and r2[2] == r1[2], (status(r2), r2[2], r1[2]))
    check(P + "HIT has Age header", "age" in r2[1], r2[1])
    check(P + "HIT keeps upstream headers", r2[1].get("x-upstream") == "yes", r2[1])
    check(P + "status variable matches header", r2[1].get("x-status-var") == "HIT", r2[1])
    raw = rcli(port, "get", ekey(u))
    check(P + "stored value does not contain the URL (key id is a hash)",
          raw and "/hello" not in raw and "body-" in raw, raw[:120])

    cb = C(cache_bypass=["$arg_bypass"])
    req(pre + "/bp", cb); W()
    r = req(pre + "/bp?bypass=1", cb)
    check(P + "cache_bypass -> BYPASS", status(r) == "BYPASS", status(r))

    r = req(u, C(), method="PURGE")
    check(P + "PURGE existing -> 200", r[0] == 200, r[0])
    r = req(u, C())
    check(P + "after PURGE -> MISS", status(r) == "MISS", status(r))
    W()
    req(u, C(), method="PURGE")
    r = req(u, C(), method="PURGE")
    check(P + "PURGE missing -> 404", r[0] == 404, r[0])

    nc = C(no_cache=["$arg_nocache"])
    req(pre + "/nc?nocache=1", nc); W()
    r = req(pre + "/nc?nocache=1", nc)
    check(P + "no_cache response not stored", status(r) == "MISS", status(r))

    req(pre + "/nf?status=404", C()); W()
    r = req(pre + "/nf?status=404", C())
    check(P + "404 cached by default", status(r) == "HIT" and r[0] == 404, (status(r), r[0]))
    req(pre + "/err?status=500", C()); W()
    r = req(pre + "/err?status=500", C())
    check(P + "500 not cached", status(r) == "MISS", status(r))

    req(pre + "/head", C(), method="HEAD"); W()
    r = req(pre + "/head", C(), method="HEAD")
    check(P + "HEAD not stored (matches upstream)", status(r) == "MISS", status(r))
    req(pre + "/post", C(), method="POST"); W()
    r = req(pre + "/post", C(), method="POST")
    check(P + "POST not in cache_method -> MISS", status(r) == "MISS", status(r))

    req(pre + "/hch?cc=max-age%3D60", C(hide_cache_headers=True)); W()
    r = req(pre + "/hch?cc=max-age%3D60", C(hide_cache_headers=True))
    check(P + "hide_cache_headers hides Cache-Control on HIT",
          status(r) == "HIT" and r[1].get("cache-control", "") == "", r[1])
    req(pre + "/shch?cc=max-age%3D60", C()); W()
    r = req(pre + "/shch?cc=max-age%3D60", C())
    check(P + "Cache-Control kept when hide_cache_headers=false",
          status(r) == "HIT" and r[1].get("cache-control") == "max-age=60", r[1])

    t1 = C(cache_ttl=1)
    req(pre + "/ttl", t1); W()
    r = req(pre + "/ttl", t1)
    check(P + "ttl=1 HIT", status(r) == "HIT", status(r))
    time.sleep(1.3)
    r = req(pre + "/ttl", t1)
    check(P + "after ttl -> EXPIRED (not MISS)", status(r) == "EXPIRED", status(r))
    W()
    r = req(pre + "/ttl", t1)
    check(P + "refetched after EXPIRED -> HIT", status(r) == "HIT", status(r))

    cc = C(cache_control=True)
    u2 = pre + "/cc?cc=max-age%3D300"
    r = req(u2, cc, headers={"Cache-Control": "max-age=60"})
    check(P + "cache_control MISS", status(r) == "MISS", status(r))
    time.sleep(1.2)
    r = req(u2, cc, headers={"Cache-Control": "max-age=1"})
    check(P + "request max-age=1 -> STALE", status(r) == "STALE", status(r))
    r = req(u2, cc, headers={"Cache-Control": "min-fresh=600"})
    check(P + "request min-fresh -> STALE", status(r) == "STALE", status(r))
    r = req(u2, cc)
    check(P + "cache_control plain request HIT", status(r) == "HIT", status(r))
    r = req(u2, cc, headers={"Cache-Control": "no-store"})
    check(P + "request no-store -> BYPASS", status(r) == "BYPASS", status(r))
    r = req(u2, cc, headers={"Cache-Control": "no-cache"})
    check(P + "request no-cache -> BYPASS", status(r) == "BYPASS", status(r))
    for d in ("private", "no-store", "no-cache"):
        x = pre + "/resp-" + d + "?cc=" + d
        req(x, C()); W()
        r = req(x, C())
        check(P + f"response Cache-Control: {d} never cached", status(r) == "MISS", status(r))
    r = req(pre + "/oic", cc, headers={"Cache-Control": "only-if-cached"})
    check(P + "only-if-cached miss -> 504", r[0] == 504, r[0])
    x = pre + "/exp?expires_in=300"
    req(x, cc); W()
    r = req(x, cc)
    check(P + "cache_control: Expires header gives TTL", status(r) == "HIT", status(r))
    x = pre + "/zero?cc=max-age%3D0"
    req(x, cc); W()
    r = req(x, cc)
    check(P + "cache_control: max-age=0 not cached", status(r) == "MISS", status(r))

    req(pre + "/ck?cookie=1", C()); W()
    r = req(pre + "/ck?cookie=1", C())
    check(P + "Set-Cookie not cached by default", status(r) == "MISS", status(r))
    req(pre + "/ck2?cookie=1", C(cache_set_cookie=True)); W()
    r = req(pre + "/ck2?cookie=1", C(cache_set_cookie=True))
    check(P + "Set-Cookie cached with cache_set_cookie=true", status(r) == "HIT", status(r))

    req(pre + "/cons", C(), headers={"X-Consumer": "alice"}); W()
    r_a = req(pre + "/cons", C(), headers={"X-Consumer": "alice"})
    r_b = req(pre + "/cons", C(), headers={"X-Consumer": "bob"})
    check(P + "consumer_isolation: alice HIT, bob MISS",
          status(r_a) == "HIT" and status(r_b) == "MISS", (status(r_a), status(r_b)))
    check(P + "consumer name not readable in Redis keys or values",
          not any("alice" in k for k in all_keys(policy))
          and "alice" not in rcli(port, "get", ekey("/" + policy + "/cons", "alice")), "")

    v = pre + "/vary?vary=Accept-Language"
    en, fr = {"Accept-Language": "en"}, {"Accept-Language": "fr"}
    req(v, C(), headers=en); W()
    req(v, C(), headers=fr); W()
    re_, rf = req(v, C(), headers=en), req(v, C(), headers=fr)
    check(P + "Vary: separate variants HIT with own bodies",
          status(re_) == "HIT" and status(rf) == "HIT" and b"-en" in re_[2] and b"-fr" in rf[2],
          (status(re_), status(rf), re_[2], rf[2]))
    r = req(v, C(), headers={"Accept-Language": "de"})
    check(P + "Vary: new header value -> MISS", status(r) == "MISS", status(r))
    req(pre + "/vstar?vary=*", C()); W()
    r = req(pre + "/vstar?vary=*", C())
    check(P + "Vary: * never cached", status(r) == "MISS", status(r))
    m1 = pre + "/vm?vary=Accept-Language,%20X-A"
    req(m1, C(), headers={"Accept-Language": "en", "X-A": "1"}); W()
    r = req(m1, C(), headers={"Accept-Language": "en", "X-A": "1"})
    r2 = req(m1, C(), headers={"Accept-Language": "en", "X-A": "2"})
    check(P + "Vary: multiple headers", status(r) == "HIT" and status(r2) == "MISS", (status(r), status(r2)))
    r = req(v, C(), method="PURGE")
    check(P + "PURGE with variants -> 200", r[0] == 200, r[0])
    re_ = req(v, C(), headers=en)
    rf = req(v, C(), headers=fr)
    check(P + "PURGE cleared every variant", status(re_) == "MISS" and status(rf) == "MISS",
          (status(re_), status(rf)))
    W()
    check(P + "Vary keys live in one cluster slot (same hash tag)",
          len({k.split("}")[0] for k in keys_for(policy, v)}) == 1, keys_for(policy, v))

    sw = pre + "/switch"
    ck = C(cache_key=["$host", "$uri"])
    req(sw + "?vary=Accept-Language", ck, headers=en); W()
    before = keys_for(policy, sw)
    req(sw, ck, headers={"Accept-Language": "de"}); W()
    after = keys_for(policy, sw)
    check(P + "Vary->no-Vary: old index and variants flushed",
          any(":i" in k for k in before) and after == [ekey(sw)], (before, after))

    e1 = C(cache_ttl=1)
    req(pre + "/pexp", e1); W()
    time.sleep(1.3)
    r = req(pre + "/pexp", e1, method="PURGE")
    check(P + "PURGE expired entry -> 200", r[0] == 200, r[0])

    b = pre + "/bin?binary=1"
    r1 = req(b, C()); W()
    r2 = req(b, C())
    check(P + "binary body (NUL, newlines, JSON) round-trips byte-exact",
          status(r2) == "HIT" and r1[2] == r2[2], (r1[2], r2[2]))

    req(pre + "/big?size=5000", C(max_resp_body_size=1000)); W()
    r = req(pre + "/big?size=5000", C(max_resp_body_size=1000))
    check(P + "body over max_resp_body_size not cached", status(r) == "MISS" and len(r[2]) > 5000,
          (status(r), len(r[2])))
    r1 = req(pre + "/big2?size=200000", C()); W()
    r2 = req(pre + "/big2?size=200000", C())
    check(P + "200KB multi-chunk body cached intact", status(r2) == "HIT" and r1[2] == r2[2],
          (status(r2), len(r2[2])))
    raw = rcli(port, "strlen", ekey(pre + "/big2?size=200000"))
    check(P + "body stored raw (no JSON escaping overhead)",
          raw.isdigit() and int(raw) < 200000 + 600, raw)

    req(pre + "/rttl", C(cache_ttl=10)); W()
    pttl = int(rcli(port, "pttl", ekey(pre + "/rttl")) or -1)
    check(P + "Redis TTL = cache_ttl + expired_retention (~70s)", 69000 <= pttl <= 70100, pttl)

    long_uri = pre + "/long?" + "q=" + "x" * 6000
    req(long_uri, C()); W()
    r = req(long_uri, C())
    k = ekey(long_uri)
    check(P + "6KB URL: HIT, key stays fixed-size",
          status(r) == "HIT" and len(k) < 64 and rcli(port, "exists", k) == "1", (status(r), len(k)))

    cu = pre + "/conc?vary=X-V"
    parallel(lambda i: req(cu, C(), headers={"X-V": str(i)}), 20)
    time.sleep(0.5)
    ok = all(status(req(cu, C(), headers={"X-V": str(i)})) == "HIT" for i in range(20))
    raw = rcli(port, "get", ekey(cu) + ":i")
    nvar = len(parse_meta(raw)["variants"]) if raw else -1
    check(P + "20 concurrent writers: index lists all 20 variants, all HIT", ok and nvar == 20, (ok, nvar))
    r = req(cu, C(), method="PURGE")
    check(P + "PURGE removes all 20 concurrent variants", r[0] == 200 and not keys_for(policy, cu),
          keys_for(policy, cu))

    mv = pre + "/maxv?vary=X-V"
    for i in range(66):
        req(mv, C(), headers={"X-V": str(i)}); time.sleep(0.01)
    W()
    r0 = req(mv, C(), headers={"X-V": "0"})
    r65 = req(mv, C(), headers={"X-V": "65"})
    nvk = len([k for k in keys_for(policy, mv) if ":v:" in k])
    check(P + "MAX_VARIANTS=64 FIFO eviction", status(r0) == "MISS" and status(r65) == "HIT" and nvk == 64,
          (status(r0), status(r65), nvk))


# --------------------------------------------------------------------------
# Part 2: what the Redis version adds
# --------------------------------------------------------------------------
def redis_features(policy):
    P = f"[{policy}] "
    pre = f"/f-{policy}"
    port = port_of(policy)

    def C(**kw):
        d = {"policy": policy}; d.update(kw); return d

    u = pre + "/collide"
    k = ekey(u)
    meta = json.dumps({"k": "some/other/key", "v": 4, "status": 200, "headers": {}, "ttl": 300, "rt": 60000})
    rcli(port, "set", k, f"{len(meta)}\n{meta}EVIL", "px", "300000")
    r = req(u, C())
    check(P + "value for another cache key (hash collision) is not served",
          status(r) == "MISS" and b"EVIL" not in r[2], (status(r), r[2]))
    W()
    r = req(u, C())
    check(P + "...and is overwritten by the real response", status(r) == "HIT" and b"EVIL" not in r[2],
          (status(r), r[2]))

    u = pre + "/clock"
    req(u, C(cache_ttl=100)); W()
    rcli(port, "pexpire", ekey(u), str(100000 + 60000 - 40000))   # 40 s "old"
    r = req(u, C(cache_ttl=100))
    age = int(r[1].get("age", "-1"))
    check(P + "Age comes from Redis PTTL (no node clock)", status(r) == "HIT" and 39 <= age <= 41, (status(r), age))
    rcli(port, "pexpire", ekey(u), "30000")   # inside retention -> logically expired
    r = req(u, C(cache_ttl=100))
    check(P + "expiry decided by Redis PTTL", status(r) == "EXPIRED", status(r))

    u = pre + "/swu?delay_ms=300"
    conf = C(cache_ttl=1, stale_while_updating=30)
    first = req(u, conf); W()
    time.sleep(1.3)
    rs = parallel(lambda i: req(u, conf), 10)
    sts = sorted(status(r) for r in rs)
    fetched = max(count(r) for r in rs)
    check(P + "stale_while_updating: 1 refresh, 9 served stale (UPDATING)",
          sts.count("UPDATING") == 9 and sts.count("EXPIRED") == 1, sts)
    check(P + "stale_while_updating: only one upstream call",
          fetched == count(first) + 1, (count(first), fetched))
    upd = [r for r in rs if status(r) == "UPDATING"]
    check(P + "stale responses did not wait for the upstream",
          all(b"body-%d" % count(first) in r[2] for r in upd), [r[2][:12] for r in upd])
    W()
    r = req(u, conf)
    check(P + "after refresh: HIT with new body", status(r) == "HIT" and count(r) == fetched, (status(r), count(r)))

    u = pre + "/lock?delay_ms=300"
    conf = C(cache_lock=True)
    rs = parallel(lambda i: req(u, conf), 20)
    sts = sorted(status(r) for r in rs)
    fetches = len({count(r) for r in rs})
    check(P + "cache_lock: 20 concurrent misses -> 1 upstream call",
          fetches == 1 and sts.count("MISS") == 1 and sts.count("HIT") == 19, (fetches, sts))
    check(P + "cache_lock: waiters got the full body", len({r[2] for r in rs}) == 1, "")
    check(P + "cache_lock: no lock left behind", rcli(port, "exists", ekey(u) + ":l") == "0", "")

    u = pre + "/lock500?status=500&delay_ms=300"
    t0 = time.time()
    rs = parallel(lambda i: req(u, conf), 8)
    dt = time.time() - t0
    check(P + "cache_lock + uncacheable response: waiters released quickly (not 5s timeout)",
          dt < 3 and all(r[0] == 500 for r in rs), f"{dt:.2f}s")

    r = req(pre + "/lock-other", conf)
    check(P + "cache_lock: other URLs unaffected", status(r) == "MISS", status(r))

    rcli(port, "set", ekey(pre + "/pl") + ":l", "x", "px", "60000")
    req(pre + "/pl", C(), method="PURGE")
    check(P + "PURGE clears a stuck lock", rcli(port, "exists", ekey(pre + "/pl") + ":l") == "0", "")


def prefix_purge_tests(policy):
    P = f"[{policy}] "
    pre = f"/pp-{policy}"
    port = port_of(policy)
    conf = {"policy": policy, "purge_prefix_depth": 2}

    urls = [pre + "/a/1", pre + "/a/2", pre + "/b/1"]
    for u in urls:
        req(u, conf)
    req(pre + "/a/v?vary=Accept-Language", conf, headers={"Accept-Language": "en"})
    req(pre + "/a/v?vary=Accept-Language", conf, headers={"Accept-Language": "fr"})
    req(pre + "/a/1", conf, headers={"X-Consumer": "alice"})
    req(pre + "/a/deep/x/y", conf)
    time.sleep(0.4)
    ok = all(status(req(u, conf)) == "HIT" for u in urls)
    check(P + "prefix purge: entries cached first", ok, "")

    zkey = "test:pc:pfx:{" + hashlib.sha1((HOST + "\0" + pre + "/a").encode()).hexdigest() + "}"
    zp = int(rcli(port, "pttl", zkey) or -2) if policy != "redis-cluster" else \
        int(rcli(7000, "pttl", zkey) or -2)
    check(P + "prefix index expires with its entries (PTTL <= ttl + retention)",
          0 < zp <= 360000, zp)

    r = req(pre + "/a/*", conf, method="PURGE")
    body = json.loads(r[2] or b"{}")
    check(P + "PURGE /a/* -> 200 with count (2 URLs + Vary URL + alice's copy + deep URL)",
          r[0] == 200 and body.get("purged") == 5, (r[0], r[2]))
    sts = [status(req(pre + "/a/1", conf)), status(req(pre + "/a/2", conf)),
           status(req(pre + "/a/v?vary=Accept-Language", conf, headers={"Accept-Language": "fr"})),
           status(req(pre + "/a/1", conf, headers={"X-Consumer": "alice"})),
           status(req(pre + "/a/deep/x/y", conf))]
    check(P + "everything under /a gone (incl. Vary variants, other consumers, deeper paths)",
          sts == ["MISS"] * 5, sts)
    r = req(pre + "/b/1", conf)
    check(P + "sibling prefix /b untouched", status(r) == "HIT", status(r))

    r = req("/*", conf, method="PURGE")
    check(P + "PURGE /* (whole host) rejected with 400", r[0] == 400 and b"error_msg" in r[2], (r[0], r[2]))
    r = req(pre + "/a/deep/x/*", conf, method="PURGE")
    check(P + "PURGE deeper than purge_prefix_depth rejected with 400", r[0] == 400, (r[0], r[2]))
    r = req(pre + "/zzz/*", conf, method="PURGE")
    check(P + "PURGE prefix with nothing cached -> 404 {purged:0}",
          r[0] == 404 and json.loads(r[2]).get("purged") == 0, (r[0], r[2]))

    # first level purges everything below it, across many entries (batches)
    big = pre[1:]
    time.sleep(0.4)
    n = 1500
    def fill(i):
        req(f"{pre}/bulk/{i % 7}/{i}", conf)
    for start in range(0, n, 100):
        parallel(lambda j: fill(start + j), 100)
    time.sleep(1.0)
    cached = sum(1 for i in range(0, n, 150) if status(req(f"{pre}/bulk/{i % 7}/{i}", conf)) == "HIT")
    t = time.time()
    r = req(pre + "/bulk/*", conf, method="PURGE", timeout=60)
    dt = time.time() - t
    purged = json.loads(r[2] or b"{}").get("purged")
    check(P + f"PURGE /bulk/* removes all {n} entries in batches",
          r[0] == 200 and purged == n and cached == 10, (r[0], purged, cached, f"{dt:.2f}s"))
    left = sum(1 for i in range(0, n, 50) if rcli(port if policy != "redis-cluster" else 7000,
                                                   "exists", ekey(f"{pre}/bulk/{i % 7}/{i}")) == "1")
    check(P + f"prefix purge of {n} entries took {dt:.2f}s and left nothing behind", left == 0, left)

    # without purge_prefix_depth a PURGE of ".../*" is an ordinary exact purge
    r = req(pre + "/b/*", {"policy": policy}, method="PURGE")
    check(P + "routes without purge_prefix_depth: PURGE /x/* is an exact-URL purge (404)",
          r[0] == 404, r[0])


def sentinel_tests():
    P = "[redis-sentinel] "
    admin(attr="sentinel_partly_dead")
    req("/sp/a", {"policy": "redis-sentinel"}); W()
    r = req("/sp/a", {"policy": "redis-sentinel"})
    check(P + "first sentinel down, second answers: HIT", status(r) == "HIT", status(r))

    admin(attr="sentinel_dead")
    t = time.time()
    rs = [req("/sd/a", {"policy": "redis-sentinel"}) for _ in range(10)]
    dt = time.time() - t
    check(P + "all sentinels down: requests served (fail-open) and fast",
          all(r[0] == 200 and status(r) == "MISS" for r in rs) and dt < 1.5, f"{dt:.2f}s")

    admin(attr="sentinel_bad_name")
    r = req("/sb/a", {"policy": "redis-sentinel"})
    check(P + "unknown master name: request served (MISS)", r[0] == 200 and status(r) == "MISS", r[0])

    # manual failover: old master stays up and becomes a replica -> writes
    # there fail with READONLY; the plugin must re-ask Sentinel
    admin(attr="both")
    conf = {"policy": "redis-sentinel"}
    before = sentinel_master_port()
    subprocess.run(["redis-cli", "-p", "26379", "sentinel", "failover", "mymaster"], capture_output=True)
    for _ in range(40):
        time.sleep(0.25)
        if sentinel_master_port() != before:
            break
    after = sentinel_master_port()
    time.sleep(1.0)
    hits = 0
    for i in range(6):
        u = f"/smf/{i}"
        req(u, conf); time.sleep(0.25)
        if status(req(u, conf)) == "HIT":
            hits += 1
    check(P + f"manual failover {before}->{after}: plugin follows the new master",
          after != before and hits >= 5, (before, after, hits))


def config_tests():
    admin(attr="both")
    check("config: valid route accepted", admin(check=json.dumps({"policy": "redis"})) == "valid")
    check("config: default policy is redis", admin(check=json.dumps({})) == "valid")
    r = admin(check=json.dumps({"policy": "memcached"}))
    check("config: unknown policy rejected", r.startswith("invalid"), r)
    r = admin(check=json.dumps({"cache_key": ["$request_method"]}))
    check("config: $request_method in cache_key rejected", "unsupported" in r, r)
    r = admin(check=json.dumps({"cache_lock_timeout": 0}))
    check("config: cache_lock_timeout out of range rejected", r.startswith("invalid"), r)
    r = admin(check=json.dumps({"policy": "redis-sentinel"}))
    check("config: sentinel policy accepted", r == "valid", r)
    r = admin(check=json.dumps({"purge_prefix_depth": 9}))
    check("config: purge_prefix_depth > 8 rejected", r.startswith("invalid"), r)
    r = admin(check=json.dumps({"stale_while_updating": 120}))
    check("config: stale_while_updating > expired_retention rejected with clear msg",
          "exceeds expired_retention" in r, r)

    admin(attr="single_only")
    check("config: single-only + policy redis OK", admin(check=json.dumps({"policy": "redis"})) == "valid")
    r = admin(check=json.dumps({"policy": "redis-cluster"}))
    check("config: single-only + policy redis-cluster rejected with clear msg",
          "redis-cluster" in r and "not configured" in r, r)
    admin(attr="cluster_only")
    check("config: cluster-only + policy redis-cluster OK",
          admin(check=json.dumps({"policy": "redis-cluster"})) == "valid")
    r = admin(check=json.dumps({"policy": "redis"}))
    check("config: cluster-only + policy redis rejected", "not configured" in r, r)
    admin(attr="none")
    r = admin(check=json.dumps({"policy": "redis"}))
    check("config: no plugin_attr -> rejected", "not configured in config.yaml" in r, r)
    admin(attr="bad_prefix")
    r = admin(check=json.dumps({"policy": "redis"}))
    check("config: key_prefix with braces rejected", "invalid plugin_attr" in r, r)
    admin(attr="unknown_field")
    r = admin(check=json.dumps({"policy": "redis"}))
    check("config: typo in config.yaml section rejected", "invalid plugin_attr" in r, r)

    admin(attr="single_only"); flush()
    req("/so/a", {"policy": "redis"}); W()
    r = req("/so/a", {"policy": "redis"})
    check("config: single-only setup serves HITs", status(r) == "HIT", status(r))
    admin(attr="both")


def queue_tests():
    admin(attr="tiny_queue")   # 1 drainer, 2 pending items max
    rs = parallel(lambda i: req(f"/q/{i}?delay_ms=5", {"policy": "redis"}), 60)
    time.sleep(0.5)
    stored = len([k for k in all_keys("redis")])
    check("write queue full: every request still served", all(r[0] == 200 for r in rs), "")
    check("write queue full: excess writes dropped, not queued without bound",
          0 < stored < 60, stored)
    admin(attr="both")


def security_tests():
    if not EXTRA:
        return
    admin(attr="acl")
    rc = lambda *a: subprocess.run(["redis-cli", "-p", "6391", "-a", "adminpw", "--no-auth-warning", "-n", "2"] + list(a),
                                   capture_output=True, text=True).stdout.strip()
    rc("flushdb")
    req("/acl/a", {"policy": "redis"}); W()
    r = req("/acl/a", {"policy": "redis"})
    check("ACL user (only the documented commands) + db 2: HIT",
          status(r) == "HIT" and rc("exists", ekey("/acl/a")) == "1", (status(r), rc("exists", ekey("/acl/a"))))
    pconf = {"policy": "redis", "purge_prefix_depth": 1}
    req("/aclp/a", pconf); req("/aclp/b", pconf); W()
    r = req("/aclp/*", pconf, method="PURGE")
    check("ACL user: prefix index + purge work with the documented commands",
          r[0] == 200 and json.loads(r[2]).get("purged") == 2, (r[0], r[2]))

    admin(attr="bad_password")
    clients = lambda: int(subprocess.run(["redis-cli", "-p", "6391", "-a", "adminpw", "--no-auth-warning",
                                          "info", "clients"], capture_output=True, text=True).stdout
                          .split("connected_clients:")[1].split()[0])
    before = clients()
    rs = [req(f"/bad/{i}", {"policy": "redis"}) for i in range(30)]
    time.sleep(0.3)
    after = clients()
    check("wrong password: requests still served", all(r[0] == 200 and status(r) == "MISS" for r in rs), "")
    check("wrong password: no leaked Redis connections", after - before <= 1, (before, after))

    admin(attr="tls")
    req("/tls/a", {"policy": "redis"}); W()
    r = req("/tls/a", {"policy": "redis"})
    check("TLS Redis: HIT", status(r) == "HIT", status(r))
    admin(attr="both")


def outage_tests():
    admin(attr="dead")
    t = time.time(); r1 = req("/dead/a", {"policy": "redis"}); d1 = time.time() - t
    check("outage (refused): request still served (fail-open)",
          r1[0] == 200 and status(r1) == "MISS" and r1[2].startswith(b"body-"), (r1[0], status(r1)))
    t = time.time()
    for _ in range(10):
        req("/dead/a", {"policy": "redis"})
    check("outage (refused): following requests don't wait", time.time() - t < 0.5, f"{time.time()-t:.3f}s")
    r = req("/dead/a", {"policy": "redis"}, method="PURGE")
    check("outage: PURGE reports 503", r[0] == 503, r[0])

    if EXTRA:
        hung = subprocess.Popen([sys.executable, "-c",
            "import socket,time;s=socket.socket();s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);"
            "s.bind(('127.0.0.1',6398));s.listen(4096);time.sleep(120)"])
        time.sleep(0.3)
        admin(attr="hung")
        for w in range(4):           # trip the backoff in both workers
            req("/hung/warm", {"policy": "redis"})
        time.sleep(0.05)
        lat = []
        for i in range(200):
            t = time.time(); r = req("/hung/a", {"policy": "redis"}); lat.append(time.time() - t)
            time.sleep(0.01)
        lat.sort()
        slow = len([x for x in lat if x > 0.15])
        check("outage (hung Redis): <= 1 probe per worker per backoff second waits",
              slow <= 8 and lat[len(lat) // 2] < 0.02, f"slow={slow} p50={lat[len(lat)//2]*1000:.1f}ms")
        hung.kill()
        rs_ = subprocess.Popen(["redis-server", "--port", "6398", "--save", "", "--appendonly", "no"],
                               stdout=subprocess.DEVNULL)
        time.sleep(1.5)
        for _ in range(4):
            req("/hung/rec", {"policy": "redis"}); time.sleep(0.2)
        r = req("/hung/rec", {"policy": "redis"})
        check("outage: recovers automatically when Redis is back", status(r) == "HIT", status(r))
        rs_.kill()
    admin(attr="both")


if __name__ == "__main__":
    flush()
    config_tests()
    for pol in ("redis", "redis-sentinel", "redis-cluster"):
        flush()
        functional(pol)
        redis_features(pol)
        prefix_purge_tests(pol)
    flush()
    sentinel_tests()
    flush()
    queue_tests()
    security_tests()
    outage_tests()
    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    sys.exit(1 if failed else 0)
