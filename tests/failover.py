#!/usr/bin/env python3
"""Kill a Redis Cluster master (6-node cluster with replicas, ports
7100-7105) under load; report per second how many requests were served and
cached, with latency, and check that the cache recovers after the replica
is promoted."""
import collections, http.client, subprocess, sys, threading, time

GW = ("127.0.0.1", 1984)


def req(path):
    c = http.client.HTTPConnection(*GW, timeout=10)
    c.request("GET", path, headers={"X-Conf": '{"policy":"redis-cluster"}'})
    r = c.getresponse(); r.read()
    s = r.getheader("Apisix-Cache-Status"); c.close()
    return r.status, s


def admin(attr):
    c = http.client.HTTPConnection(*GW)
    c.request("GET", "/admin?attr=" + attr)
    c.getresponse().read(); c.close()
    time.sleep(0.1)


admin("failover")
for p in (7100, 7101, 7102):
    subprocess.run(["redis-cli", "-p", str(p), "flushall"], capture_output=True)
paths = [f"/fo/{i}" for i in range(200)]
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
print("killing cluster master 7100 at t=3s")
subprocess.run(["redis-cli", "-p", "7100", "shutdown", "nosave"], capture_output=True)
time.sleep(14)
stop = True
[t.join() for t in ths]
admin("both")

for sec in sorted(stats):
    l = sorted(lat[sec])
    print(f"t={sec:2d}s", dict(stats[sec]),
          f"p50={l[len(l)//2]*1000:.1f}ms p99={l[int(len(l)*.99)]*1000:.1f}ms max={l[-1]*1000:.0f}ms")

served = sum(n for c in stats.values() for (st, _), n in c.items() if st == 200)
total = sum(n for c in stats.values() for n in c.values())
late = collections.Counter()
[late.update(stats[s]) for s in stats if s >= 9]
late_hit = late[(200, "HIT")] / max(1, sum(late.values()))
ok1, ok2 = served == total, late_hit > 0.95
print(("PASS" if ok1 else "FAIL") + f" cluster failover: every request served ({served}/{total})")
print(("PASS" if ok2 else "FAIL") + f" cluster failover: cache recovered after promotion "
      f"({late_hit:.1%} HIT from t=9s)")
sys.exit(0 if ok1 and ok2 else 1)
