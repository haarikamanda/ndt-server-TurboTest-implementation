"""Offline accuracy / data-saving evaluation of TurboTest on the 1M M-Lab test set.

Replays each test's 100 ms feature rows through the same code the real-time
service runs (regressor_input / classifier_input / ONNX classifier) and stops
at the first tau in 5..95 whose classifier probability is >= 0.5.

Metrics follow the paper (§5.1):
  * relative error  E = |T - T_early| / T, T = ReturnedThroughputMbps (oracle file)
  * data transferred = sum_i B_early_i / sum_i B_i (cumulative, operator view),
    B_i = TotalBytesAcked, B_early_i = B_i * (1 - data_saving_i / 100),
    data_saving from the 100 ms byte series (same formula as the regressor script).
  * a test that is never stopped runs to completion: E = 0, B_early = B.

Two Stage-1 sources:
  A  "reference": regressor predictions from 2025_09_13_..._regressor_output.csv
     (the ones the classifier was trained/tested with)
  B  "deployed":  predictions of the shipped regressor (xg_regressor_5.pkl)

Usage:
  python -m tools.offline_eval --model-dir ../../model_shared --n 100000 --out results/
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from turbotest.features import COL, features_from_dataframe  # noqa: E402
from turbotest.models import (  # noqa: E402
    CLASSIFIER_TAUS, REGRESSOR_TAUS, Classifier, Regressor, classifier_input, export_onnx,
    normalize_classifier_input, regressor_input,
)

TIERS = ["0-25", "25-100", "100-200", "200-400", "400+"]

_G = {}


def _init(model_dir, ref_preds, epsilons):
    _G["reg"] = Regressor(model_dir)
    _G["clf"] = {e: Classifier(model_dir, e, backend="onnx", threads=1) for e in epsilons}
    norm = os.path.join(model_dir, "transformer_classifier", "saved_normalization_values")
    _G["min"] = np.load(os.path.join(norm, "min_vals.npy")).astype(np.float32)
    _G["rng"] = np.load(os.path.join(norm, "ranges.npy")).astype(np.float32)
    _G["dir"] = os.path.join(model_dir, "input_data", "tcpinfo_features_09_12_2025")
    _G["ref"] = ref_preds


def _prepare(fn):
    """Per test: regressor preds (B), classifier inputs for A and B, data saving per tau."""
    try:
        m = features_from_dataframe(pd.read_csv(os.path.join(_G["dir"], fn)))
    except Exception:
        return None
    n = m.shape[0]
    taus = [t for t in REGRESSOR_TAUS if n > t]
    if not taus:
        return None
    xs = np.stack([regressor_input(m, t) for t in taus])
    pred_b = dict(zip(taus, _G["reg"].predict_bps_batch(xs).astype(np.float64)))
    pred_a = _G["ref"].get(fn, {})

    sum_pkt = m[:, COL["sum_packet_length"]]
    total = float(np.nansum(sum_pkt))
    prefix = np.cumsum(np.nan_to_num(sum_pkt))
    ds = {t: (100.0 * (total - prefix[t - 1]) / total if total > 0 else 0.0) for t in taus}

    out = {"file": fn, "n_rows": n, "ds": ds, "pred_a": {t: pred_a[t] for t in taus if t in pred_a},
           "pred_b": pred_b, "probs": {}}
    for src, preds in (("A", pred_a), ("B", pred_b)):
        ts, xs = [], []
        for t in CLASSIFIER_TAUS:
            if n >= t and all(k in preds for k in range(5, t + 1, 5)):
                ts.append(t)
                xs.append(normalize_classifier_input(classifier_input(m, t, preds), _G["min"], _G["rng"]))
        for e, clf in _G["clf"].items():
            out["probs"][(e, src)] = dict(zip(ts, clf.prob_batch(np.stack(xs)).tolist())) if xs else {}
    return out


def load_ref_preds(path, files):
    t0 = time.time()
    keep = set(files)
    ref = {}
    for chunk in pd.read_csv(path, usecols=["file", "tau", "predicted_throughput"], chunksize=2_000_000):
        chunk = chunk[chunk["file"].isin(keep)]
        for f, t, p in zip(chunk["file"].values, chunk["tau"].values, chunk["predicted_throughput"].values):
            ref.setdefault(f, {})[int(t)] = float(p)
    print(f"loaded reference regressor preds for {len(ref)} tests in {time.time() - t0:.0f}s", flush=True)
    return ref


def load_bytes(gt_dir):
    frames = []
    for f in sorted(os.listdir(gt_dir)):
        if f.endswith(".csv"):
            frames.append(pd.read_csv(os.path.join(gt_dir, f), usecols=["UUID", "TotalBytesAcked", "ReturnedThroughputMbps"]))
    g = pd.concat(frames).drop_duplicates("UUID")
    return g.set_index("UUID")


def summarize(df, label):
    res = {"label": label, "n_tests": int(len(df))}
    res["median_rel_err_pct"] = float(np.median(df["rel_err"]) * 100)
    res["p75_rel_err_pct"] = float(np.percentile(df["rel_err"], 75) * 100)
    res["p90_rel_err_pct"] = float(np.percentile(df["rel_err"], 90) * 100)
    res["within_20pct"] = float(np.mean(df["rel_err"] <= 0.20) * 100)
    res["data_transferred_pct"] = float(df["b_early"].sum() / df["bytes"].sum() * 100)
    res["data_saving_pct"] = 100 - res["data_transferred_pct"]
    res["terminated_pct"] = float(df["terminated"].mean() * 100)
    res["median_stop_s"] = float(np.median(df["tau_stop"]) / 10)
    res["mean_stop_s"] = float(np.mean(df["tau_stop"]) / 10)
    res["by_tier"] = {}
    for tier in TIERS:
        g = df[df["speed_tier"] == tier]
        if len(g):
            res["by_tier"][tier] = {
                "n": int(len(g)),
                "median_rel_err_pct": float(np.median(g["rel_err"]) * 100),
                "data_transferred_pct": float(g["b_early"].sum() / g["bytes"].sum() * 100),
                "terminated_pct": float(g["terminated"].mean() * 100),
            }
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-dir", required=True)
    ap.add_argument("--n", type=int, default=100_000, help="tests to sample (0 = all)")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--epsilons", default="15")
    ap.add_argument("--threshold", type=float, default=0.5)
    ap.add_argument("--out", default="results")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    md = a.model_dir

    oracle = pd.read_csv(os.path.join(md, "transformer_classifier", "oracle_labels", "xgboost_1_million.csv"),
                         usecols=["file", "tau", "correct_throughput", "speed_tier"])
    oracle = oracle.rename(columns={"tau": "tau_oracle"})
    feat_dir = os.path.join(md, "input_data", "tcpinfo_features_09_12_2025")
    present = set(os.listdir(feat_dir))
    oracle = oracle[oracle["file"].isin(present)]
    if a.n and a.n < len(oracle):
        oracle = oracle.sample(n=a.n, random_state=a.seed)
    files = oracle["file"].tolist()
    print(f"evaluating {len(files)} tests", flush=True)

    gt = load_bytes(os.path.join(md, "xgboost_regressor", "ground_truth", "m_lab_csvs_July_7th"))
    oracle["uuid"] = oracle["file"].str.replace(".00000_features.csv", "", regex=False)
    oracle = oracle.join(gt, on="uuid")
    missing = oracle["TotalBytesAcked"].isna().mean() * 100
    print(f"ground-truth bytes missing for {missing:.2f}% of tests (dropped)", flush=True)
    oracle = oracle.dropna(subset=["TotalBytesAcked"])
    files = oracle["file"].tolist()

    ref = load_ref_preds(os.path.join(md, "transformer_classifier", "regressor_outputs",
                                      "2025_09_13_1_million_dataset_regressor_output.csv"), files)

    epsilons = [int(e) for e in a.epsilons.split(",")]
    for e in epsilons:  # export ONNX once, before the workers start
        export_onnx(Classifier.checkpoint_path(md, e))
    t0 = time.time()
    prepared = []
    # spawn, not fork: the parent may have initialized torch/OpenMP threads
    # (ONNX export), and forking after that can deadlock the workers.
    import multiprocessing as mp
    with ProcessPoolExecutor(a.workers, mp_context=mp.get_context("spawn"), initializer=_init,
                             initargs=(md, ref, epsilons)) as ex:
        for i, r in enumerate(ex.map(_prepare, files, chunksize=64)):
            if r is not None:
                prepared.append(r)
            if (i + 1) % 20000 == 0:
                print(f"  prepared {i + 1}/{len(files)} ({time.time() - t0:.0f}s)", flush=True)
    print(f"prepared {len(prepared)} tests in {time.time() - t0:.0f}s", flush=True)

    meta = oracle.set_index("file")
    summaries = []
    for eps in epsilons:
        for src in ("A", "B"):
            first = {}
            for i, r in enumerate(prepared):
                hits = [t for t, p in r["probs"][(eps, src)].items() if p >= a.threshold]
                if hits:
                    first[i] = min(hits)
            rows = []
            for i, r in enumerate(prepared):
                mrow = meta.loc[r["file"]]
                T = float(mrow["correct_throughput"])  # Mbit/s
                B = float(mrow["TotalBytesAcked"])
                preds = r["pred_a"] if src == "A" else r["pred_b"]
                if i in first and first[i] in preds:
                    t = first[i]
                    est = preds[t] / 1e6
                    err = abs(est - T) / T if T > 0 else 0.0
                    b_early = B * (1 - r["ds"][t] / 100)
                    rows.append((r["file"], mrow["speed_tier"], T, est, err, B, b_early, t, True))
                else:
                    rows.append((r["file"], mrow["speed_tier"], T, T, 0.0, B, B, 100, False))
            df = pd.DataFrame(rows, columns=["file", "speed_tier", "true_mbps", "est_mbps", "rel_err",
                                             "bytes", "b_early", "tau_stop", "terminated"])
            label = f"eps{eps}_{'reference' if src == 'A' else 'deployed'}"
            df.to_csv(os.path.join(a.out, f"offline_{label}.csv.gz"), index=False)
            s = summarize(df, label)
            summaries.append(s)
            print(json.dumps(s, indent=1), flush=True)
    with open(os.path.join(a.out, "offline_summary.json"), "w") as f:
        json.dump(summaries, f, indent=1)


if __name__ == "__main__":
    main()
