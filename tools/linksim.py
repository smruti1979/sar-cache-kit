#!/usr/bin/env python3
"""linksim: simulate link loss, cache staleness and outbox drain order.

Scenario `cache`  A pull-through cache at the Incident Command Post (ICP) serves
                  read requests for a data item (for example weather or hazard
                  feeds) whose origin sits across an unreliable link. Four
                  policies are compared on the same request stream and the same
                  outages:
                    naive           strict TTL, no stale serving, no coalescing,
                                    cold start
                    stale_if_error  serve flagged stale data when refresh fails
                    coalesced       + one in-flight fetch per key, TTL jitter
                    full            + stale-while-revalidate, prefetch of hot
                                    keys, pre-seeded cache (warm start)
Scenario `drain`  After a field-to-ICP link outage, how long until the ICP sees
                  fresh positions? Compares FIFO, strict priority classes, and
                  priority with latest-wins position coalescing.

TTLs and staleness limits come from configs/cache-policy.toml. Every number in
the defaults is an assumption: change it from the command line.
Standard library only (Python 3.11+ for tomllib). Runs are seeded and repeatable.
"""
import argparse
import bisect
import collections
import math
import random
import sys
import tomllib

# ------------------------------------------------------------------ helpers --


def load_policy(path, data_name):
    with open(path, "rb") as f:
        pol = tomllib.load(f)
    item = pol["data"][data_name]
    cls = dict(pol["classes"][item["class"]])
    cls.update({k: v for k, v in item.items() if k in ("ttl_s", "max_stale_s")})
    cls["jitter"] = pol["defaults"]["ttl_jitter"]
    cls["data"] = data_name
    return cls


def parse_outages(spec):
    out = []
    for part in spec.split(","):
        part = part.strip()
        if part:
            a, b = part.split("-")
            out.append((int(a), int(b)))
    return sorted(out)


class Link:
    def __init__(self, outages):
        self.outages = outages
        self.starts = [a for a, _ in outages]

    def up(self, t):
        i = bisect.bisect_right(self.starts, t) - 1
        return not (i >= 0 and self.outages[i][0] <= t < self.outages[i][1])


def poisson(rng, lam):
    if lam > 30:
        return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def pct(sorted_vals, q):
    if not sorted_vals:
        return 0
    return sorted_vals[min(len(sorted_vals) - 1, int(q * len(sorted_vals)))]


# ------------------------------------------------------------ cache scenario --
POLICIES = {
    "naive":          dict(sie=False, coalesce=False, jitter=False, swr=False, prefetch=False, warm=False),
    "stale_if_error": dict(sie=True,  coalesce=False, jitter=False, swr=False, prefetch=False, warm=False),
    "coalesced":      dict(sie=True,  coalesce=True,  jitter=True,  swr=False, prefetch=False, warm=False),
    "full":           dict(sie=True,  coalesce=True,  jitter=True,  swr=True,  prefetch=True,  warm=True),
}


def run_cache(cls, args, link, name):
    pol = POLICIES[name]
    rng = random.Random(args.seed)            # same request stream for every policy
    krng = random.Random(args.seed + 1)
    n, dur = args.keys, args.duration
    ttl, max_stale = cls["ttl_s"], cls["max_stale_s"]
    jit = [krng.uniform(-1, 1) * cls["jitter"] if pol["jitter"] else 0.0 for _ in range(n)]
    ttl_k = [ttl * (1 + j) for j in jit]
    weights = [1.0 / (r + 1) ** args.zipf for r in range(n)]
    cum, acc = [], 0.0
    for w in weights:
        acc += w
        cum.append(acc)
    fetched = [0 if pol["warm"] else None for _ in range(n)]
    inflight = {}                             # key -> (done_t, ok)
    last_req = [-10 ** 9] * n
    fresh = [0] * dur
    stale = [0] * dur
    fail = [0] * dur
    origin = [0] * dur
    waited = 0
    ages = []
    dups = 0
    lat = args.latency
    prefetch_frac = cls.get("prefetch_frac", 0.8)

    for t in range(dur):
        for k, (done, ok) in list(inflight.items()):
            if done <= t:
                if ok:
                    fetched[k] = done
                del inflight[k]
        up = link.up(t)
        cap = args.cap if up else 0

        for _ in range(poisson(rng, args.rate)):
            k = bisect.bisect_left(cum, rng.random() * acc)
            last_req[k] = t
            f = fetched[k]
            age = None if f is None else t - f
            if age is not None and age <= ttl_k[k]:
                fresh[t] += 1
                ages.append(age)
                continue
            have_stale = pol["sie"] and age is not None and age <= max_stale
            will_fetch = False
            if k in inflight and pol["coalesce"]:
                will_fetch = inflight[k][1]
            elif up and cap > 0:
                cap -= 1
                origin[t] += 1
                if k in inflight:
                    dups += 1
                ok = link.up(t + lat)
                inflight[k] = (t + lat, ok)
                will_fetch = ok
            if will_fetch:
                if pol["swr"] and have_stale:
                    stale[t] += 1
                    ages.append(age)
                else:
                    fresh[t] += 1
                    ages.append(0)
                    waited += 1
            elif have_stale:
                stale[t] += 1
                ages.append(age)
            else:
                fail[t] += 1

        if pol["prefetch"] and up and cap > 1 and t % 5 == 0:
            cands = [(t - fetched[k], k) for k in range(n)
                     if fetched[k] is not None and k not in inflight
                     and t - last_req[k] < ttl
                     and t - fetched[k] >= prefetch_frac * ttl_k[k]]
            cands.sort(reverse=True)
            for _, k in cands[: cap // 2]:
                cap -= 1
                origin[t] += 1
                inflight[k] = (t + lat, link.up(t + lat))

    total = sum(fresh) + sum(stale) + sum(fail)
    ages.sort()
    rec = []
    for _, end in link.outages:
        for t in range(end, dur - 10):
            f10 = sum(fresh[t:t + 10])
            tot10 = f10 + sum(stale[t:t + 10]) + sum(fail[t:t + 10])
            if tot10 and f10 >= 0.95 * tot10:
                rec.append(t - end)
                break
    return {
        "policy": name,
        "availability_%": 100.0 * (total - sum(fail)) / total,
        "fresh_%": 100.0 * sum(fresh) / total,
        "stale_flagged_%": 100.0 * sum(stale) / total,
        "failed_%": 100.0 * sum(fail) / total,
        "waited_%": 100.0 * waited / total,
        "age_p50_s": pct(ages, 0.50),
        "age_p95_s": pct(ages, 0.95),
        "age_max_s": ages[-1] if ages else 0,
        "origin_requests": sum(origin),
        "duplicate_fetches": dups,
        "peak_origin_per_s": max(origin),
        "recovery_s": round(sum(rec) / len(rec)) if rec and len(rec) == len(link.outages) else None,
    }


def print_cache_table(cls, args, link, results):
    down = sum(b - a for a, b in link.outages)
    print(f"\nScenario: cache | data item: {cls['data']} | ttl {cls['ttl_s']} s | "
          f"max_stale {cls['max_stale_s']} s")
    print(f"duration {args.duration} s, link down {down} s ({100 * down / args.duration:.0f}%), "
          f"outages {link.outages}")
    print(f"{args.rate:g} req/s over {args.keys} keys (Zipf {args.zipf}), "
          f"fetch latency {args.latency} s, link cap {args.cap} fetches/s\n")
    cols = [("policy", "{:<15}"), ("availability_%", "{:>9.2f}"), ("fresh_%", "{:>8.2f}"),
            ("stale_flagged_%", "{:>9.2f}"), ("failed_%", "{:>8.2f}"), ("waited_%", "{:>8.2f}"), ("age_p95_s", "{:>7}"),
            ("age_max_s", "{:>7}"), ("origin_requests", "{:>8}"),
            ("duplicate_fetches", "{:>6}"), ("peak_origin_per_s", "{:>6}"), ("recovery_s", "{:>8}")]
    heads = ["policy", "avail %", "fresh %", "stale-fl %", "fail %", "waited %", "p95 age", "max age",
             "origin", "dups", "peak/s", "recov s"]
    print(" ".join(h.ljust(15) if i == 0 else h.rjust(9) for i, h in enumerate(heads)))
    for r in results:
        row = []
        for i, (key, fmt) in enumerate(cols):
            v = r[key]
            if v is None:
                v = "-"
                row.append(str(v).rjust(9))
            else:
                row.append(fmt.format(v).rjust(9) if i else fmt.format(v))
        print(" ".join(row))


# ------------------------------------------------------------ drain scenario --
def run_drain(args, mode):
    o0, o1 = args.outage_start, args.outage_end
    horizon = args.horizon
    dur = o1 + horizon
    B = args.bandwidth_kbps * 125                 # bytes per second
    assets = args.assets
    drones = args.drones
    delivered = [-10 ** 9] * assets
    fifo = collections.deque()
    prio = {1: collections.deque(), 2: collections.deque(),
            3: collections.deque(), 4: collections.deque()}
    latest = {}                                   # asset -> msg (latest-wins mode)
    backlog_at_reconnect = 0
    series = []
    st = {"qb": 0}
    qb_series = []
    gen_bytes = args.pos_bytes * assets

    def enqueue(m):
        st["qb"] += m[4]
        if mode == "fifo":
            fifo.append(m)
        elif mode == "priority":
            prio[m[2]].append(m)
        else:
            if m[2] == 1:
                old = latest.get(m[1])
                if old is not None:
                    old[2] = 4                    # older fix becomes track history
                    prio[4].append(old)
                latest[m[1]] = m
            else:
                prio[m[2]].append(m)

    def send(m):
        if m[2] == 1 or (m[2] == 4 and m[5] == 1):
            delivered[m[1]] = max(delivered[m[1]], m[0])

    for t in range(dur):
        # message: [gen_t, asset, class, size, remaining, original_class]
        for a in range(assets):
            enqueue([t, a, 1, args.pos_bytes, args.pos_bytes, 1])
        for d in range(drones):
            if (t + d) % args.thumb_every == 0:
                enqueue([t, d, 2, args.thumb_bytes, args.thumb_bytes, 2])
            if (t + 3 * d) % args.clip_every == 0:
                enqueue([t, d, 3, args.clip_bytes, args.clip_bytes, 3])
        enqueue([t, 0, 4, args.log_bytes, args.log_bytes, 4])
        if t == o1:
            backlog_at_reconnect = st["qb"]
        if not (o0 <= t < o1):
            budget = B
            if mode == "fifo":
                while budget > 0 and fifo:
                    m = fifo[0]
                    s = min(budget, m[4])
                    m[4] -= s
                    budget -= s
                    st["qb"] -= s
                    if m[4] == 0:
                        send(fifo.popleft())
            else:
                for cls in (1, 2, 3, 4):
                    if cls == 1 and mode == "latest":
                        for a in sorted(latest, key=lambda a: latest[a][0]):
                            m = latest[a]
                            s = min(budget, m[4])
                            m[4] -= s
                            budget -= s
                            st["qb"] -= s
                            if m[4] == 0:
                                send(m)
                                del latest[a]
                            if budget <= 0:
                                break
                        continue
                    q = prio[cls]
                    while budget > 0 and q:
                        m = q[0]
                        s = min(budget, m[4])
                        m[4] -= s
                        budget -= s
                        st["qb"] -= s
                        if m[4] == 0:
                            send(q.popleft())
        if t >= o1:
            series.append((t, max(t - d for d in delivered)))
            qb_series.append((t, st["qb"]))

    ttf = None
    for i in range(len(series)):
        if all(a <= args.fresh_s for _, a in series[i:]):
            ttf = series[i][0] - o1
            break
    steady = gen_bytes + args.log_bytes + (drones * args.thumb_bytes / args.thumb_every) \
        + (drones * args.clip_bytes / args.clip_every)
    drain = None
    for t, q in qb_series:
        if q <= 5 * steady:
            drain = t - o1
            break
    age60 = dict(series).get(o1 + 60)
    return {
        "mode": mode,
        "backlog_MB": backlog_at_reconnect / 1e6,
        "time_to_fresh_positions_s": ttf,
        "time_to_drain_s": drain,
        "position_age_at_60s": age60,
        "steady_load_KBps": steady / 1000,
        "link_KBps": B / 1000,
    }


def print_drain(args, results):
    print(f"\nScenario: drain | outage {args.outage_start}-{args.outage_end} s "
          f"({args.outage_end - args.outage_start} s), link {args.bandwidth_kbps} kbit/s, "
          f"{args.assets} assets, {args.drones} drones")
    r0 = results[0]
    print(f"steady load {r0['steady_load_KBps']:.0f} KB/s of {r0['link_KBps']:.0f} KB/s link; "
          f"fresh = position age <= {args.fresh_s} s\n")
    print(f"{'mode':<10} {'backlog MB':>11} {'fresh positions after (s)':>26} "
          f"{'position age at +60 s':>22} {'backlog drained after (s)':>26}")
    for r in results:
        ttf = "never (in horizon)" if r["time_to_fresh_positions_s"] is None \
            else r["time_to_fresh_positions_s"]
        print(f"{r['mode']:<10} {r['backlog_MB']:>11.1f} {str(ttf):>26} "
              f"{r['position_age_at_60s']:>22} {str(r['time_to_drain_s']):>26}")


# ---------------------------------------------------------------------- main --
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="scenario", required=True)

    c = sub.add_parser("cache")
    c.add_argument("--policy-file", default="configs/cache-policy.toml")
    c.add_argument("--data", default="weather_forecast")
    c.add_argument("--duration", type=int, default=14400)
    c.add_argument("--outages", default="3600-4500,7200-9900,11000-12200")
    c.add_argument("--rate", type=float, default=20.0, help="requests per second")
    c.add_argument("--keys", type=int, default=400)
    c.add_argument("--zipf", type=float, default=1.1)
    c.add_argument("--latency", type=int, default=2, help="origin fetch latency in seconds")
    c.add_argument("--cap", type=int, default=8, help="max origin fetches per second over the link")
    c.add_argument("--seed", type=int, default=7)
    c.add_argument("--policies", default="naive,stale_if_error,coalesced,full")

    d = sub.add_parser("drain")
    d.add_argument("--outage-start", type=int, default=600)
    d.add_argument("--outage-end", type=int, default=1200)
    d.add_argument("--horizon", type=int, default=3600)
    d.add_argument("--bandwidth-kbps", type=int, default=2000)
    d.add_argument("--assets", type=int, default=30)
    d.add_argument("--drones", type=int, default=10)
    d.add_argument("--pos-bytes", type=int, default=200)
    d.add_argument("--thumb-bytes", type=int, default=30000)
    d.add_argument("--thumb-every", type=int, default=5)
    d.add_argument("--clip-bytes", type=int, default=500000)
    d.add_argument("--clip-every", type=int, default=60)
    d.add_argument("--log-bytes", type=int, default=10000)
    d.add_argument("--fresh-s", type=int, default=15)

    args = ap.parse_args(argv)
    if args.scenario == "cache":
        cls = load_policy(args.policy_file, args.data)
        link = Link(parse_outages(args.outages))
        results = [run_cache(cls, args, link, p) for p in args.policies.split(",")]
        print_cache_table(cls, args, link, results)
    else:
        results = [run_drain(args, m) for m in ("fifo", "priority", "latest")]
        print_drain(args, results)


if __name__ == "__main__":
    main()
