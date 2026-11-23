"""Live end-to-end TurboTest tests on a running fullstack server.

For each network condition (applied with tc/netem on the server container's
egress, i.e. the download direction), the harness runs real ndt7 downloads:

  * baseline : plain ndt7 test, no TurboTest            -> reference T and bytes
  * shadow   : turbotest=shadow, full-length test; the service evaluates every
               epsilon on the same traffic               -> exact per-test metrics
  * enforce  : turbotest=enforce for each epsilon        -> real early stops

Metrics (paper §5.1), per shadow test and epsilon:
  T        = full-test throughput reported by ndt7: server TCPInfo
             BytesAcked * 8 / ElapsedTime of the last server measurement
  T_early  = TurboTest predicted throughput at the stop point
  rel err  = |T - T_early| / T
  data transferred = bytes acked in the first tau 100 ms rows / bytes acked over
             all rows, from the same tcp-info snapshots the service used
             (table_1:<uuid>); a test that is never stopped transfers 100%.
For enforce tests: real duration and bytes, compared with the baseline
tests of the same condition.

Usage (see README): python -m tools.live_eval --client ../../ndt7-client-go-snl/ndt7-client ...
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time

import numpy as np
import redis

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from turbotest.features import COL, Snapshot, StreamingFeatureBuilder  # noqa: E402

EPSILONS = [5, 10, 15, 20, 25, 30, 35]

# name: (rate, delay, loss, jitter). rate None = no shaping (local link).
CONDITIONS = {
    "local": (None, None, None, None),
    "10M_40ms": ("10mbit", "40ms", None, None),
    "25M_20ms": ("25mbit", "20ms", None, None),
    "50M_60ms": ("50mbit", "60ms", None, None),
    "100M_20ms": ("100mbit", "20ms", None, None),
    "200M_30ms": ("200mbit", "30ms", None, None),
    "500M_10ms": ("500mbit", "10ms", None, None),
    "50M_40ms_loss1": ("50mbit", "40ms", "1%", None),
    "20M_80ms_jitter": ("20mbit", "80ms", None, "20ms"),
}


def sh(cmd, check=True):
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def set_condition(container, dev, name):
    rate, delay, loss, jitter = CONDITIONS[name]
    sh(["docker", "exec", container, "tc", "qdisc", "del", "dev", dev, "root"], check=False)
    if rate is None:
        return "none"
    # netem limit must hold a bandwidth-delay product of packets.
    args = ["docker", "exec", container, "tc", "qdisc", "add", "dev", dev, "root", "netem",
            "rate", rate, "delay", delay]
    if jitter:
        args += [jitter, "distribution", "normal"]
    if loss:
        args += ["loss", loss]
    args += ["limit", "100000"]
    sh(args)
    return sh(["docker", "exec", container, "tc", "qdisc", "show", "dev", dev]).stdout.strip()


def run_client(client, server, mode=None, eps=None, timeout=40):
    cmd = [client, "-server", server, "-scheme", "ws", "-upload=false", "-format", "json",
           "-client-name", "turbotest-live"]
    if mode:
        cmd += ["-turbotest", mode]
        if eps:
            cmd += ["-turbotest-epsilon", str(eps)]
    t0 = time.time()
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    wall = time.time() - t0
    out = {"wall_s": wall, "uuid": None, "server_last": None, "client_last": None,
           "turbotest": None, "summary": None, "rc": p.returncode, "stderr": p.stderr[-500:]}
    for line in p.stdout.splitlines():
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        if "Download" in ev and isinstance(ev.get("Download"), dict) and "Throughput" in ev["Download"]:
            out["summary"] = ev
            continue
        v = ev.get("Value") or {}
        if not isinstance(v, dict):
            continue
        ci = v.get("ConnectionInfo")
        if ci and ci.get("UUID"):
            out["uuid"] = ci["UUID"]
        if v.get("TurboTest"):
            out["turbotest"] = v["TurboTest"]
        if v.get("Origin") == "server" and v.get("TCPInfo"):
            out["server_last"] = v["TCPInfo"]
        if v.get("Origin") == "client" and v.get("AppInfo"):
            out["client_last"] = v["AppInfo"]
    return out


def fetch_redis(r, uuid):
    rows = [x.decode() for x in r.lrange(f"table_1:{uuid}", 0, -1)]
    res = {k.decode(): v.decode() for k, v in r.hgetall(f"turbotest:result:{uuid}").items()}
    shadow = {int(k): json.loads(v) for k, v in r.hgetall(f"turbotest:shadow:{uuid}").items()}
    return rows, res, shadow


def bins_from_rows(rows):
    b = StreamingFeatureBuilder()
    for row in rows:
        b.add(Snapshot.parse(row))
    b.flush()
    return b


def full_throughput(server_last):
    if not server_last or not server_last.get("ElapsedTime"):
        return None
    return server_last["BytesAcked"] * 8 / server_last["ElapsedTime"]  # bit/us == Mbit/s


def wait_result(r, uuid, key="turbotest:shadow:", n=len(EPSILONS), timeout=2.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if r.hlen(key + uuid) >= n:
            return
        time.sleep(0.05)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", required=True)
    ap.add_argument("--server", default="localhost:18080")
    ap.add_argument("--redis", default="127.0.0.1:16379")
    ap.add_argument("--container", default="ndt-fullstack")
    ap.add_argument("--dev", default="eth0")
    ap.add_argument("--conditions", default=",".join(CONDITIONS))
    ap.add_argument("--n-baseline", type=int, default=3)
    ap.add_argument("--n-shadow", type=int, default=6)
    ap.add_argument("--n-enforce", type=int, default=2, help="per epsilon")
    ap.add_argument("--epsilons", default=",".join(map(str, EPSILONS)))
    ap.add_argument("--out", default="results/live")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    host, port = a.redis.rsplit(":", 1)
    r = redis.Redis(host=host, port=int(port))
    eps_list = [int(e) for e in a.epsilons.split(",")]
    raw_path = os.path.join(a.out, "live_tests.jsonl")
    raw = open(raw_path, "a")

    for cond in a.conditions.split(","):
        qd = set_condition(a.container, a.dev, cond)
        print(f"\n=== condition {cond}: {qd}", flush=True)
        time.sleep(1)
        plan = [("baseline", None)] * a.n_baseline + [("shadow", 15)] * a.n_shadow
        plan += [("enforce", e) for e in eps_list for _ in range(a.n_enforce)]
        for kind, eps in plan:
            mode = None if kind == "baseline" else kind
            res = run_client(a.client, a.server, mode, eps)
            rec = {"condition": cond, "kind": kind, "epsilon": eps, "time": time.time(), **res}
            uuid = res["uuid"]
            if uuid:
                if kind == "shadow":
                    wait_result(r, uuid)
                elif kind == "enforce":
                    wait_result(r, uuid, key="turbotest:result:", n=1)
                rows, tt_res, shadow = fetch_redis(r, uuid)
                b = bins_from_rows(rows) if rows else None
                rec["n_snapshots"] = len(rows)
                rec["n_rows"] = b.n_rows if b else 0
                if b and b.n_rows:
                    pkt = np.nan_to_num(b.matrix()[:, COL["sum_packet_length"]])
                    rec["prefix_bytes"] = np.cumsum(pkt).tolist()
                rec["service_result"] = tt_res
                rec["shadow"] = shadow
            rec["T_mbps"] = full_throughput(res["server_last"])
            raw.write(json.dumps(rec) + "\n")
            raw.flush()
            tt = res.get("turbotest") or {}
            print(f"  {kind:8s} eps={eps!s:4s} T={rec['T_mbps'] or 0:8.2f} Mbit/s wall={res['wall_s']:5.2f}s "
                  f"rows={rec.get('n_rows', 0):3d} tt={tt.get('Decision', '-')}"
                  f"{' pred=%.2f' % tt['PredictedMbps'] if tt.get('PredictedMbps') else ''}"
                  f"{' tau=%d' % tt['Tau'] if tt.get('Tau') else ''} uuid={uuid}", flush=True)
            time.sleep(0.5)
    set_condition(a.container, a.dev, "local")
    raw.close()
    print(f"\nraw results: {raw_path}\nnow run: python -m tools.live_report {raw_path}")


if __name__ == "__main__":
    main()
