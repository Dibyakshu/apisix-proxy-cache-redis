#!/usr/bin/env python3
"""Load benchmark: no cache vs upstream proxy-cache (memory) vs
proxy-cache-redis (single / sentinel / cluster), using wrk. 1 gateway worker."""
import http.client, json, os, re, subprocess, sys, time

DUR = os.environ.get("BENCH_DUR", "10s")
CONNS = 50
OUT = []


def to_ms(v):
    m = re.match(r"([\d.]+)(us|ms|s)", v)
    n, u = float(m.group(1)), m.group(2)
    return n / 1000 if u == "us" else n * 1000 if u == "s" else n


def wrk(path, plugin, conf=None, conns=CONNS, script=None, dur=DUR, method=None):
    cmd = ["wrk", "-t1", f"-c{conns}", f"-d{dur}", "--latency", "-H", f"X-Plugin: {plugin}"]
    if conf is not None:
        cmd += ["-H", "X-Conf: " + json.dumps(conf, separators=(",", ":"))]
    if script:
        cmd += ["-s", script]
    cmd.append("http://127.0.0.1:1984" + path)
    out = subprocess.run(cmd, capture_output=True, text=True).stdout
    rps = float(re.search(r"Requests/sec:\s+([\d.]+)", out).group(1))
    lat = re.search(r"Latency Distribution\s+50%\s+(\S+)\s+75%\s+(\S+)\s+90%\s+(\S+)\s+99%\s+(\S+)", out)
    non2xx = re.search(r"Non-2xx or 3xx responses:\s+(\d+)", out)
    errs = re.search(r"Socket errors:.*", out)
    return dict(rps=rps, p50=to_ms(lat.group(1)), p99=to_ms(lat.group(4)),
                non2xx=int(non2xx.group(1)) if non2xx else 0,
                errors=errs.group(0) if errs else "")


def get(path, plugin, conf=None, method="GET"):
    h = {"X-Plugin": plugin}
    if conf is not None:
        h["X-Conf"] = json.dumps(conf, separators=(",", ":"))
    c = http.client.HTTPConnection("127.0.0.1", 1984, timeout=60)
    c.request(method, path, headers=h)
    r = c.getresponse(); body = r.read()
    out = (r.getheader("Apisix-Cache-Status"), int(r.getheader("X-Count") or 0), r.status, body)
    c.close()
    return out


def warm(path, plugin, conf=None):
    for _ in range(2):
        st = get(path, plugin, conf)[0]
        time.sleep(0.3)
    return st


def admin(attr):
    c = http.client.HTTPConnection("127.0.0.1", 1984); c.request("GET", "/admin?attr=" + attr)
    c.getresponse().read(); c.close(); time.sleep(0.1)


def row(scenario, mode, res, note=""):
    OUT.append(dict(scenario=scenario, mode=mode, note=note, **res))
    print(f"{scenario:<40} {mode:<22} {res['rps']:>8.0f} rps  p50 {res['p50']:>7.2f} ms  "
          f"p99 {res['p99']:>7.2f} ms  non2xx={res['non2xx']} {res['errors']} {note}", flush=True)


MODES = [
    ("none", "none", None),
    ("memory", "memory", {}),
    ("redis-single", "redis", {"policy": "redis", "max_resp_body_size": 2097152}),
    ("redis-sentinel", "redis", {"policy": "redis-sentinel", "max_resp_body_size": 2097152}),
    ("redis-cluster", "redis", {"policy": "redis-cluster", "max_resp_body_size": 2097152}),
]


def miss_script(name, extra=""):
    path = f"/tmp/pcr-miss_{name}.lua"
    open(path, "w").write(
        'local n = 0\nrequest = function()\n  n = n + 1\n'
        f'  return wrk.format(nil, "/c{int(time.time())}/{name}/" .. (n % 97) .. "/" .. n .. "?size=1024")\nend\n')
    return path


def suite(only=None):
    admin("both")
    if only in (None, "A"):
        for size, label in ((1024, "1KB"), (102400, "100KB"), (1048576, "1MB")):
            for name, plugin, conf in MODES:
                path = f"/a/{name}/{label}?size={size}"
                st = warm(path, plugin, conf)
                row(f"A cache hit, {label}", name, wrk(path, plugin, conf), f"[{st or '-'}]")
    if only in (None, "B"):
        for name, plugin, conf in MODES:
            path = f"/b/{name}?size=1024&delay_ms=20"
            st = warm(path, plugin, conf)
            row("B hit, backend takes 20ms, 1KB", name, wrk(path, plugin, conf), f"[{st or '-'}]")
    if only in (None, "C"):
        for name, plugin, conf in MODES:
            script = miss_script(name)
            row("C all MISS (write path), 1KB", name, wrk("/", plugin, conf, script=script))
            os.remove(script)
        for depth in (1, 2):
            conf = {"policy": "redis", "purge_prefix_depth": depth}
            script = miss_script(f"pfx{depth}")
            row("C all MISS (write path), 1KB", f"redis-single+prefix{depth}",
                wrk("/", "redis", conf, script=script))
            os.remove(script)
    if only in (None, "D"):
        for name, plugin, conf in MODES:
            path = f"/d/{name}?size=1024"
            warm(path, plugin, conf)
            row("D hit, 200 connections, 1KB", name, wrk(path, plugin, conf, conns=200))
    if only in (None, "G"):
        for label, extra in (("plain", {}), ("cache_lock", {"cache_lock": True}),
                             ("stale_while_updating", {"stale_while_updating": 30}),
                             ("both", {"cache_lock": True, "stale_while_updating": 30})):
            path = f"/g/{label}?size=1024&delay_ms=50"
            conf = dict({"policy": "redis", "cache_ttl": 1}, **extra)
            warm(path, "redis", conf)
            before = get(path, "none")[1]
            res = wrk(path, "redis", conf)
            calls = get(path, "none")[1] - before - 1
            row("G stampede: ttl 1s, backend 50ms", label, res, f"upstream calls: {calls}")
    if only in (None, "P"):
        # time to purge N entries by prefix
        for pol in ("redis", "redis-sentinel", "redis-cluster"):
            conf = {"policy": pol, "purge_prefix_depth": 1}
            n = 10000
            script = f"/tmp/pcr-fill_{pol}.lua"
            open(script, "w").write(
                'local n = 0\nrequest = function()\n  n = n + 1\n'
                f'  if n > {n} then return wrk.format(nil, "/pfill-{pol}/x?size=64") end\n'
                f'  return wrk.format(nil, "/pfill-{pol}/" .. n .. "?size=512")\nend\n')
            wrk("/", "redis", conf, script=script, dur="4s")
            os.remove(script)
            time.sleep(1)
            t = time.time()
            st, _, code, body = get(f"/pfill-{pol}/*", "redis", conf, method="PURGE")
            dt = time.time() - t
            print(f"P purge by prefix                         {pol:<22} {body.decode()} in {dt*1000:.0f} ms",
                  flush=True)
            OUT.append(dict(scenario="P purge by prefix", mode=pol, body=body.decode(), ms=dt * 1000))


if __name__ == "__main__":
    suite(sys.argv[1] if len(sys.argv) > 1 else None)
    json.dump(OUT, open(os.environ.get("BENCH_OUT", "/tmp/pcr-bench.json"), "w"), indent=1)
