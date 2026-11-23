"""Summarize live_eval.py results (accuracy, data saving, stop time per epsilon).

Byte counts come from the tcp-info snapshots of each test (table_1:<uuid>,
"prefix_bytes" in the records): the last snapshot is taken when the connection
closes, so its count includes everything the test sent. The last server
measurement (server_last) is sent before the end of the test and misses the
last few hundred ms, so it is not used for bytes.

Shadow tests give the paper metric: bytes up to the decision point tau.
Enforce tests give two numbers, both relative to the baseline tests of the
same condition:
  data at tau        bytes acked in the first tau 100 ms rows
  data actually sent bytes acked until the connection closed; the difference
                     is what the stop path costs (decision delay, flag poll,
                     and the data already queued in the socket send buffer).

Usage: python -m tools.live_report results/live/live_tests.jsonl [--json out.json]
"""

import argparse
import json
import statistics
from collections import defaultdict

import numpy as np


def med(x):
    return float(statistics.median(x)) if x else float("nan")


def load(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def shadow_metrics(rec, eps):
    """Per-test paper metrics for one epsilon from a shadow test."""
    T = rec.get("T_mbps")
    pref = rec.get("prefix_bytes")
    sh = (rec.get("shadow") or {}).get(str(eps)) or (rec.get("shadow") or {}).get(eps)
    if not T or not pref or sh is None:
        return None
    total = pref[-1]
    if sh.get("decision") == "stop" and sh.get("tau", 0) <= len(pref):
        tau = sh["tau"]
        return {"rel_err": abs(sh["predicted_mbps"] - T) / T, "b_early": pref[tau - 1], "b": total,
                "stop_s": tau / 10, "stopped": True}
    return {"rel_err": 0.0, "b_early": total, "b": total, "stop_s": len(pref) / 10, "stopped": False}


def total_bytes(rec):
    """Bytes acked over the whole test, from the final tcp-info snapshot."""
    pref = rec.get("prefix_bytes")
    return pref[-1] if pref else None


def enforce_bytes(rec):
    """(bytes at tau, bytes actually sent) of one enforce test, or None."""
    pref = rec.get("prefix_bytes")
    if not pref:
        return None
    sr = rec.get("service_result") or {}
    total = pref[-1]
    if sr.get("status") == "stop" and 0 < int(sr.get("tau", 0)) <= len(pref):
        return pref[int(sr["tau"]) - 1], total
    return total, total


def stop_delays_ms(rec):
    """(end of bin tau -> decision written, end of bin tau -> ndt-server saw the flag)."""
    sr = rec.get("service_result") or {}
    tt = rec.get("turbotest") or {}
    if sr.get("status") != "stop" or not sr.get("decided_at_ns") or not sr.get("first_snapshot_ns"):
        return None
    stop_ms = float(sr["stop_after_ms"])
    dec = (int(sr["decided_at_ns"]) - int(sr["first_snapshot_ns"])) / 1e6 - stop_ms
    seen = tt["FlagSeenMs"] - stop_ms if tt.get("FlagSeenMs") else None
    return dec, seen


def summarize(recs):
    out = {"conditions": {}, "overall_shadow": {}, "overall_enforce": {}}
    conds = list(dict.fromkeys(r["condition"] for r in recs))
    eps_all = sorted({int(e) for r in recs if r["kind"] == "shadow" for e in (r.get("shadow") or {})})
    per_eps_all = defaultdict(list)
    enf_all = defaultdict(list)  # eps -> [(b_tau, b_sent, b_ref, delays)]
    for c in conds:
        rs = [r for r in recs if r["condition"] == c and r.get("rc") == 0]
        base = [r for r in rs if r["kind"] == "baseline" and r.get("T_mbps")]
        shad = [r for r in rs if r["kind"] == "shadow" and r.get("T_mbps")]
        enf = [r for r in rs if r["kind"] == "enforce"]
        T_ref = med([r["T_mbps"] for r in base])
        B_ref = med([b for b in (total_bytes(r) for r in base) if b])
        cs = {
            "baseline": {"n": len(base), "T_mbps": T_ref, "bytes": B_ref,
                         "duration_s": med([r["wall_s"] for r in base])},
            "shadow_T_mbps": med([r["T_mbps"] for r in shad]),
            "shadow": {}, "enforce": {},
        }
        for e in eps_all:
            ms = [m for m in (shadow_metrics(r, e) for r in shad) if m]
            per_eps_all[e] += ms
            if ms:
                cs["shadow"][e] = {
                    "n": len(ms),
                    "median_rel_err_pct": med([m["rel_err"] for m in ms]) * 100,
                    "data_transferred_pct": sum(m["b_early"] for m in ms) / sum(m["b"] for m in ms) * 100,
                    "stopped_pct": np.mean([m["stopped"] for m in ms]) * 100,
                    "median_stop_s": med([m["stop_s"] for m in ms]),
                }
        for e in sorted({r["epsilon"] for r in enf}):
            es = [r for r in enf if r["epsilon"] == e]
            tts = [r.get("turbotest") or {} for r in es]
            early = [r for r, t in zip(es, tts) if t.get("TerminatedEarly")]
            preds = [t["PredictedMbps"] for t in tts if t.get("TerminatedEarly")]
            eb = [x for x in (enforce_bytes(r) for r in es) if x]
            dl = [x for x in (stop_delays_ms(r) for r in es) if x]
            if B_ref == B_ref:
                enf_all[e] += [(bt, bs, B_ref) for bt, bs in eb]
                out.setdefault("_delays", []).extend(dl)
            cs["enforce"][e] = {
                "n": len(es),
                "terminated_pct": len(early) / len(es) * 100 if es else 0,
                "median_duration_s": med([r["wall_s"] for r in es]),
                "median_flag_seen_s": med([t["FlagSeenMs"] / 1000 for t in tts if t.get("FlagSeenMs")]),
                "median_flag_latency_ms": med([t["FlagLatencyMs"] for t in tts if t.get("FlagLatencyMs")]),
                "median_rel_err_vs_baseline_pct": med([abs(p - T_ref) / T_ref * 100 for p in preds]) if T_ref == T_ref else None,
                "data_at_tau_pct": sum(b for b, _ in eb) / (len(eb) * B_ref) * 100 if eb and B_ref == B_ref else None,
                "data_sent_pct": sum(b for _, b in eb) / (len(eb) * B_ref) * 100 if eb and B_ref == B_ref else None,
                "median_decision_delay_ms": med([d for d, _ in dl]),
            }
        out["conditions"][c] = cs
    for e, ms in sorted(per_eps_all.items()):
        out["overall_shadow"][e] = {
            "n": len(ms),
            "median_rel_err_pct": med([m["rel_err"] for m in ms]) * 100,
            "data_transferred_pct": sum(m["b_early"] for m in ms) / sum(m["b"] for m in ms) * 100,
            "stopped_pct": float(np.mean([m["stopped"] for m in ms]) * 100),
            "median_stop_s": med([m["stop_s"] for m in ms]),
        }
    for e, v in sorted(enf_all.items()):
        ref = sum(b for _, _, b in v)
        at, sent = sum(x[0] for x in v) / ref * 100, sum(x[1] for x in v) / ref * 100
        out["overall_enforce"][e] = {"n": len(v), "data_at_tau_pct": at, "data_sent_pct": sent,
                                     "lost_pct_points": sent - at}
    dl = out.pop("_delays", [])
    q = lambda a, p: float(np.percentile(a, p)) if a else float("nan")  # noqa: E731
    dec = [d for d, _ in dl]
    seen = [s for _, s in dl if s is not None]
    out["stop_delays_ms"] = {
        "n": len(dl),
        "bin_end_to_decision": {"median": q(dec, 50), "p90": q(dec, 90), "max": q(dec, 100)},
        "bin_end_to_server_flag": {"median": q(seen, 50), "p90": q(seen, 90), "max": q(seen, 100)},
    }
    return out


def print_report(s):
    print("Per-epsilon, all conditions (shadow tests, same traffic for every epsilon):")
    print(f"{'eps':>4} {'n':>4} {'med err %':>10} {'data xfer %':>12} {'stopped %':>10} {'med stop s':>11}")
    for e, v in s["overall_shadow"].items():
        print(f"{e:>4} {v['n']:>4} {v['median_rel_err_pct']:>10.1f} {v['data_transferred_pct']:>12.1f} "
              f"{v['stopped_pct']:>10.0f} {v['median_stop_s']:>11.2f}")
    if s["overall_enforce"]:
        print("\nPer-epsilon, all conditions (enforce tests: real stops; % of the baseline bytes):")
        print(f"{'eps':>4} {'n':>4} {'data at tau %':>14} {'data sent %':>12} {'lost (points)':>14}")
        for e, v in s["overall_enforce"].items():
            print(f"{e:>4} {v['n']:>4} {v['data_at_tau_pct']:>14.1f} {v['data_sent_pct']:>12.1f} "
                  f"{v['lost_pct_points']:>14.1f}")
        d = s["stop_delays_ms"]
        a, b = d["bin_end_to_decision"], d["bin_end_to_server_flag"]
        print(f"Stop delays over {d['n']} stops (ms): end of bin tau -> decision written: median {a['median']:.1f}, "
              f"p90 {a['p90']:.1f}, max {a['max']:.1f}; -> ndt-server saw the flag: median {b['median']:.1f}, "
              f"p90 {b['p90']:.1f}, max {b['max']:.1f}")
    for c, cs in s["conditions"].items():
        b = cs["baseline"]
        print(f"\n== {c}: baseline n={b['n']} T={b['T_mbps']:.1f} Mbit/s bytes={b['bytes'] / 1e6:.1f} MB "
              f"duration={b['duration_s']:.1f}s | shadow T={cs['shadow_T_mbps']:.1f}")
        print(f"   {'eps':>4} | shadow: {'err%':>6} {'xfer%':>6} {'stop s':>6} | enforce: {'n':>2} {'early%':>6} "
              f"{'dur s':>6} {'flag s':>6} {'lat ms':>6} {'err%':>6} {'@tau%':>6} {'sent%':>6}")
        for e in sorted(set(cs["shadow"]) | set(cs["enforce"])):
            sv = cs["shadow"].get(e, {})
            ev = cs["enforce"].get(e, {})
            f = lambda d, k, fmt="6.1f": (format(d[k], fmt) if d.get(k) is not None and d.get(k) == d.get(k) else "   -  ")  # noqa: E731
            print(f"   {e:>4} |         {f(sv, 'median_rel_err_pct')} {f(sv, 'data_transferred_pct')} "
                  f"{f(sv, 'median_stop_s', '6.2f')} |          {ev.get('n', 0):>2} {f(ev, 'terminated_pct', '6.0f')} "
                  f"{f(ev, 'median_duration_s', '6.2f')} {f(ev, 'median_flag_seen_s', '6.2f')} "
                  f"{f(ev, 'median_flag_latency_ms')} {f(ev, 'median_rel_err_vs_baseline_pct')} "
                  f"{f(ev, 'data_at_tau_pct')} {f(ev, 'data_sent_pct')}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--json")
    a = ap.parse_args()
    s = summarize(load(a.path))
    print_report(s)
    if a.json:
        with open(a.json, "w") as f:
            json.dump(s, f, indent=1)


if __name__ == "__main__":
    main()
