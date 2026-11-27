#!/usr/bin/env python3
"""Kill the Redis master behind Sentinel under load; report per second how
many requests were served / cached and the latency; restart the old master
(it rejoins as a replica) and check writes keep going to the new master."""
import collections, http.client, os, subprocess, sys, threading, time

GW = ("127.0.0.1", 1984)
CONF = '{"policy":"redis-sentinel"}'


def req(path):
    c = http.client.HTTPConnection(*GW, timeout=10)
    c.request("GET", path, headers={"X-Conf": CONF})
    r = c.getresponse(); r.read()
    s = r.getheader("Apisix-Cache-Status"); c.close()
    return r.status, s


def master():
    out = subprocess.run(["redis-cli", "-p", "26379", "sentinel", "get-master-addr-by-name",
                          "mymaster"], capture_output=True, text=True).stdout.split()
    return int(out[1])


paths = [f"/sfo/{i}" for i in range(200)]
for p in paths:
    req(p)
time.sleep(0.5)
stats = collections.defaultdict(collections.Counter)
lat = collections.defaultdict(list)
stop = False
t0 = time.time()


def worker():
    i = 0
    while not stop:
        p = paths[i % 200]; i += 1
        a = time.time()
        try:
            st, s = req(p)
        except Exception as e:
            st, s = "EXC", type(e).__name__
        sec = int(time.time() - t0)
        stats[sec][(st, s)] += 1
        lat[sec].append(time.time() - a)


ths = [threading.Thread(target=worker) for _ in range(8)]
[t.start() for t in ths]
time.sleep(3)
old = master()
port = str(old)
print(f"killing master {old} at t=3s")
subprocess.run(["redis-cli", "-p", port, "shutdown", "nosave"], capture_output=True)
time.sleep(9)
new = master()
print(f"sentinel now reports master {new}; restarting old master {old} at t=12s")
work = os.environ.get("PCR_WORK", "/tmp")
subprocess.run(["redis-server", "--port", port, "--daemonize", "yes", "--save", "",
                "--appendonly", "no", "--dir", work, "--logfile", f"{work}/restarted-{port}.log"],
               capture_output=True)
time.sleep(7)
stop = True
[t.join() for t in ths]

for sec in sorted(stats):
    l = sorted(lat[sec])
    print(f"t={sec:2d}s", dict(stats[sec]),
          f"p50={l[len(l)//2]*1000:.1f}ms p99={l[int(len(l)*.99)]*1000:.1f}ms max={l[-1]*1000:.0f}ms")

# Sentinel converts a returning old master into a replica within seconds
role = "?"
for _ in range(60):
    role = subprocess.run(["redis-cli", "-p", port, "role"], capture_output=True,
                          text=True).stdout.split()[0]
    if role == "slave":
        break
    time.sleep(0.5)
served = sum(n for c in stats.values() for (st, _), n in c.items() if st == 200)
total = sum(n for c in stats.values() for n in c.values())
late = collections.Counter()
[late.update(stats[s]) for s in stats if s >= 15]
late_hit = late[(200, "HIT")] / max(1, sum(late.values()))
checks = [
    (served == total, f"every request served ({served}/{total})"),
    (new != old, f"sentinel promoted a new master ({old} -> {new})"),
    (late_hit > 0.95, f"cache recovered on the new master ({late_hit:.1%} HIT from t=15s)"),
    (role == "slave", f"restarted old master rejoined as replica (role={role})"),
]
ok = True
for c, msg in checks:
    print(("PASS " if c else "FAIL ") + "sentinel failover: " + msg)
    ok = ok and c
sys.exit(0 if ok else 1)
