"""Check that the online TurboTest code reproduces the offline pipeline.

Checks, for a random sample of tests from one preprocessed arrow chunk:
  1. Stage 1 inputs/outputs vs 2025_09_13_1_million_dataset_regressor_output.csv
  2. Stage 2 raw 1300-value inputs vs the preprocessed arrow file (column X)
  3. ONNX Runtime vs PyTorch classifier outputs

Usage:
  python -m tools.parity_check --model-dir ../../model_shared --chunk 6 --n 300
"""

import argparse
import os
import random
import subprocess
import sys
import time

import numpy as np
import pandas as pd
import pyarrow.feather as feather

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from turbotest.features import features_from_dataframe  # noqa: E402
from turbotest.models import (  # noqa: E402
    CLASSIFIER_TAUS, REGRESSOR_TAUS, Classifier, Regressor, classifier_input,
    normalize_classifier_input, regressor_input,
)


def arrow_rows(tbl, files):
    df = tbl.select(["filename", "tau_curr"]).to_pandas()
    df["row"] = np.arange(len(df))
    df = df[df["filename"].isin(files)]
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-dir", required=True)
    ap.add_argument("--chunk", type=int, default=6)
    ap.add_argument("--n", type=int, default=300)
    ap.add_argument("--seed", type=int, default=0)
    a = ap.parse_args()

    md = a.model_dir
    feat_dir = os.path.join(md, "input_data", "tcpinfo_features_09_12_2025")
    arrow = os.path.join(md, "transformer_classifier", "preprocessed_arrow_files",
                         f"2025_11_15_1_million_dataset_test_feather_{a.chunk}.arrow")
    reg_csv = os.path.join(md, "transformer_classifier", "regressor_outputs",
                           "2025_09_13_1_million_dataset_regressor_output.csv")

    tbl = feather.read_table(arrow)
    names = sorted(set(tbl.column("filename").to_pylist()))
    random.Random(a.seed).shuffle(names)
    files = names[: a.n]
    print(f"chunk {a.chunk}: {len(names)} tests, sampling {len(files)}")

    # Offline regressor predictions for the sample (grep is much faster than pandas on 2.6 GB).
    pat = os.path.join(os.path.dirname(arrow), f".parity_patterns_{os.getpid()}")
    with open(pat, "w") as f:
        f.write("\n".join(files))
    out = subprocess.run(["grep", "-F", "-f", pat, reg_csv], capture_output=True, text=True).stdout
    os.remove(pat)
    from io import StringIO

    reg_df = pd.read_csv(StringIO(out), header=None,
                         names=["file", "tau", "relative_error", "predicted_throughput",
                                "correct_throughput", "data_saving", "CountryCode", "speed_tier"])
    reg_off = {(r.file, int(r.tau)): float(r.predicted_throughput) for r in reg_df.itertuples()}

    regressor = Regressor(md)
    clf_torch = Classifier(md, 15, backend="torch")
    clf_onnx = Classifier(md, 15, backend="onnx")

    rows = arrow_rows(tbl, set(files))
    X = tbl.column("X")

    reg_err, n_reg, x_maxdiff, n_x, p_diff, n_x_exact = [], 0, 0.0, 0, 0.0, 0
    t_reg, t_clf = [], []
    for fn in files:
        m = features_from_dataframe(pd.read_csv(os.path.join(feat_dir, fn)))
        n = m.shape[0]
        mine = {}
        for tau in REGRESSOR_TAUS:
            if n <= tau:  # offline: `if length <= tau: continue`
                continue
            t0 = time.perf_counter()
            mine[tau] = regressor.predict_bps(regressor_input(m, tau))
            t_reg.append(time.perf_counter() - t0)
            if (fn, tau) in reg_off:
                off = reg_off[(fn, tau)]
                reg_err.append(abs(mine[tau] - off) / max(abs(off), 1.0))
                n_reg += 1
        for r in rows[rows["filename"] == fn].itertuples():
            tau = int(r.tau_curr)
            offline_vec = np.asarray(X[r.row].as_py(), dtype=np.float32)
            preds = {t: reg_off[(fn, t)] for t in range(5, tau + 1, 5)}
            vec = classifier_input(m, tau, preds)
            d = float(np.max(np.abs(vec - offline_vec) / np.maximum(1.0, np.abs(offline_vec))))
            x_maxdiff = max(x_maxdiff, d)
            n_x += 1
            n_x_exact += int(np.array_equal(vec, offline_vec))
            xin = normalize_classifier_input(vec, clf_torch.min_vals, clf_torch.ranges)[None]
            pt = float(clf_torch.prob_batch(xin)[0])
            t0 = time.perf_counter()
            po = float(clf_onnx.prob_batch(xin)[0])
            t_clf.append(time.perf_counter() - t0)
            p_diff = max(p_diff, abs(pt - po))

    reg_err = np.array(reg_err)
    print(f"[stage1] {n_reg} (test,tau) pairs: max rel diff {reg_err.max():.2e}, "
          f"exact(<1e-6) {np.mean(reg_err < 1e-6) * 100:.2f}%")
    print(f"[stage2 input] {n_x} vectors: exact {n_x_exact / n_x * 100:.2f}%, max rel diff {x_maxdiff:.2e}")
    print(f"[stage2 onnx vs torch] max |dp| = {p_diff:.2e}")
    print(f"[latency] regressor median {np.median(t_reg) * 1e3:.3f} ms p99 {np.percentile(t_reg, 99) * 1e3:.3f} ms; "
          f"classifier(onnx) median {np.median(t_clf) * 1e3:.3f} ms p99 {np.percentile(t_clf, 99) * 1e3:.3f} ms")
    ok = reg_err.max() < 1e-5 and x_maxdiff < 1e-5 and p_diff < 1e-4
    print("PARITY OK" if ok else "PARITY MISMATCH")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
